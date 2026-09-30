Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.UnsubmittedSuccessorMigrationTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{ExecutionFence, ExecutionSupervisor, ManagedResponsibility, ResponsibilityGraph}
  alias SymphonyElixir.ManagedResponsibility.Manifest
  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.WorkPackageClaim.{Journal, UnsubmittedSuccessor}

  @projection_id "workpkg_4446a7d851764ecf9bf62bfbae26d1cc"
  @grant_fields ~w(id parent_delegation_id role actor_id scope authority budget expires_at_ms expected_deliverable expected_evidence return_to_parent)a

  setup do
    # Path.expand normalizes the drive letter on Windows, which the hardened
    # managed-ledger path check requires for its canonical-path comparison.
    root = Path.expand(Path.dirname(Workflow.workflow_file_path()))
    workspace_root = Path.join(root, "workspaces")
    File.mkdir_p!(workspace_root)
    now = System.system_time(:millisecond)

    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: workspace_root,
      codex_max_total_tokens: 500_000
    )

    {:ok, graph, _} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), now)

    old_payload =
      update_in(Fixture.payload(now)["entries"], fn [first | rest] ->
        first =
          first
          |> put_in(["accountable", "scope", "work_package_id"], "linear:HGS-1")
          |> put_in(["responsible", "scope", "work_package_id"], "linear:HGS-1")

        [first | rest]
      end)

    {:ok, base_manifest} = ManagedResponsibility.decode(old_payload, Fixture.context(), now)
    journal_path = Path.join(root, "claims.json")
    :ok = Journal.save(journal_path, Journal.new())

    runtime = %{
      managed_delegations: base_manifest,
      managed_project_profile_id: "profile-test",
      journal_path: journal_path
    }

    state = %Orchestrator.State{
      execution_fence: ExecutionFence.new(),
      responsibility_graph: graph,
      work_package_runtime: runtime
    }

    state = Fixture.initialize_budget(state)
    issue = Fixture.issue(1)
    {:ok, admitted, _, _, _, _} = Orchestrator.admit_execution_for_test(state, issue, nil)

    {:ok,
     %{
       root: root,
       now: System.system_time(:millisecond),
       issue: issue,
       runtime: admitted.work_package_runtime,
       state: admitted,
       fence: admitted.execution_fence,
       graph: admitted.responsibility_graph,
       execution: admitted.execution_fence.executions[issue.id]
     }}
  end

  test "signed successor retires only the exact untouched pair and graph-first replay completes after restart", context do
    {runtime, entry} = successor(context)

    {:ok, candidate_fence, candidate_graph, :retired} =
      UnsubmittedSuccessor.prepare(runtime, context.fence, context.graph, entry, context.now)

    old = context.runtime.managed_delegations.entries |> Enum.find(&(&1.issue_id == context.issue.id))
    assert candidate_graph.delegations[old.accountable.id].status == :revoked
    assert candidate_graph.delegations[old.responsible.id].status == :revoked
    assert candidate_graph.delegations[old.responsible.id].runtime_lease == nil
    assert candidate_graph.delegations[old.responsible.id].terminal_evidence["type"] == "unsubmitted_successor"
    assert candidate_fence.executions[context.issue.id].status == :retired

    assert {:ok, admitted_graph} =
             ManagedResponsibility.admit(
               candidate_graph,
               runtime.managed_delegations,
               context.issue,
               context.now + 1
             )

    successor_entry = Enum.find(runtime.managed_delegations.entries, &(&1.issue_id == context.issue.id))
    assert admitted_graph.delegations[successor_entry.accountable.id].status == :active
    assert admitted_graph.delegations[successor_entry.responsible.id].status == :active
    assert admitted_graph.delegations[old.responsible.id].status == :revoked

    graph_path = Path.join(context.root, "successor-graph.json")
    fence_path = Path.join(context.root, "successor-fence.json")
    :ok = GraphPersistence.save(graph_path, candidate_graph)

    # The permitted graph-first intermediate must still hold dispatch closed.
    {:ok, graph_after_crash} = GraphPersistence.load(graph_path)

    partial_state = %{
      context.state
      | execution_fence: context.fence,
        responsibility_graph: graph_after_crash,
        work_package_runtime: runtime
    }

    assert {:error, _} = Orchestrator.admit_execution_for_test(partial_state, context.issue, nil)

    # The original observations are now stale. Replay is authorized only by
    # the graph's exact persisted receipt and its original preparation time.
    assert {:ok, replay_fence, ^graph_after_crash, :retired} =
             UnsubmittedSuccessor.prepare(runtime, context.fence, graph_after_crash, entry, context.now + 120_000)

    :ok = ExecutionFence.Persistence.save(fence_path, replay_fence)
    {:ok, cold_fence} = ExecutionFence.Persistence.load(fence_path)
    {:ok, cold_graph} = GraphPersistence.load(graph_path)

    assert {:ok, ^cold_fence, ^cold_graph, :already_retired} =
             UnsubmittedSuccessor.prepare(runtime, cold_fence, cold_graph, entry, context.now + 120_001)

    assert :ok = ExecutionFence.validate(cold_fence)
    assert :ok = ResponsibilityGraph.validate(cold_graph)
    assert cold_fence.executions[context.issue.id].retirement.type == "unsubmitted_successor"
  end

  test "claim, worker, scope, identity, stale observations and journal rows keep the transition closed", context do
    {runtime, entry} = successor(context)
    observation = entry.unsubmitted_observation

    contradictory_observations = [
      Map.put(observation, "provider_claimed_at", "2026-09-30T05:00:00Z"),
      Map.put(observation, "provider_claim_generation", 2),
      Map.put(observation, "kubernetes_job_issue_matches", 1),
      Map.put(observation, "kubernetes_pod_issue_matches", 1),
      Map.put(observation, "process_count", 1),
      Map.put(observation, "process_unit", "symphony-exec-wrong.scope"),
      Map.put(observation, "workspace_absent", false),
      Map.put(observation, "workspace_evidence_ref", "not-a-digest"),
      Map.put(observation, "provider_observed_at_ms", context.now - 60_001)
    ]

    for changed_observation <- contradictory_observations do
      changed_entry = Map.put(entry, :unsubmitted_observation, changed_observation)
      changed_manifest = replace_entry(runtime.managed_delegations, changed_entry)

      assert {:error, _} =
               UnsubmittedSuccessor.prepare(
                 %{runtime | managed_delegations: changed_manifest},
                 context.fence,
                 context.graph,
                 changed_entry,
                 context.now
               )
    end

    [session] = Map.keys(context.execution.leases)
    changed_worker = update_in(context.execution, [:leases, session, :head], fn _ -> "observed" end)
    changed_fence = put_in(context.fence, [:executions, context.issue.id], changed_worker)
    assert {:error, _} = UnsubmittedSuccessor.prepare(runtime, changed_fence, context.graph, entry, context.now)

    wrong_projection = put_in(entry, [:responsible, :scope, :work_package_id], "workpkg-other")
    assert {:error, _} = UnsubmittedSuccessor.prepare(runtime, context.fence, context.graph, wrong_projection, context.now)

    later_row = %{
      issue_id: context.issue.id,
      managed_project_profile_id: "profile-test",
      repository_ref: "openai/symphony",
      projection_id: @projection_id,
      reservation_id: "reservation-later",
      reservation_nonce: "nonce-later",
      scope_keys: ["issue_id"],
      runner_id: "runner-test",
      generation: 2,
      session_id: "session-later",
      process_id: "process-later",
      responsible_delegation_id: "responsible-later",
      execution_fence_token: "fence-later",
      runtime_lease_id: "lease-later"
    }

    {:ok, journal} = Journal.put(Journal.new(), "later-generation", later_row)
    :ok = Journal.save(runtime.journal_path, journal)

    assert {:error, :prior_claim_journal_row_exists} =
             UnsubmittedSuccessor.prepare(runtime, context.fence, context.graph, entry, context.now)
  end

  test "reconciles only the exact restart-blocked predecessor pair before retirement", context do
    {runtime, entry} = successor(context)
    old_entry = Enum.find(context.runtime.managed_delegations.entries, &(&1.issue_id == context.issue.id))

    blocked_graph =
      Enum.reduce([old_entry.accountable.id, old_entry.responsible.id], context.graph, fn id, graph ->
        update_in(graph, [:delegations, id], fn delegation ->
          %{delegation | status: :blocked, blocked_on: :restart_reconciliation}
        end)
      end)

    assert :ok = ResponsibilityGraph.validate(blocked_graph)

    assert {:ok, _fence, retired_graph, :retired} =
             UnsubmittedSuccessor.prepare(runtime, context.fence, blocked_graph, entry, context.now)

    assert Enum.count(retired_graph.events, &(&1.type == :reconciled)) == 2

    wrong_reason_graph =
      put_in(blocked_graph, [:delegations, old_entry.accountable.id, :blocked_on], :operator_hold)

    assert {:error, _reason} =
             UnsubmittedSuccessor.prepare(runtime, context.fence, wrong_reason_graph, entry, context.now)

    wrong_lease_graph =
      update_in(blocked_graph, [:delegations, old_entry.responsible.id, :runtime_lease], fn current ->
        %{current | process_id: "different-process"}
      end)

    assert {:error, _reason} =
             UnsubmittedSuccessor.prepare(runtime, context.fence, wrong_lease_graph, entry, context.now)
  end

  test "retires an expired restart-blocked pair without reactivating it", context do
    {runtime, entry} = successor(context)
    old_entry = Enum.find(context.runtime.managed_delegations.entries, &(&1.issue_id == context.issue.id))
    retire_at = context.now + 60_001

    blocked_graph =
      Enum.reduce([old_entry.accountable.id, old_entry.responsible.id], context.graph, fn id, graph ->
        update_in(graph, [:delegations, id], fn delegation ->
          %{delegation | status: :blocked, blocked_on: :restart_reconciliation}
        end)
      end)

    observation =
      Enum.reduce(~w(provider_observed_at_ms kubernetes_observed_at_ms process_observed_at_ms workspace_observed_at_ms), entry.unsubmitted_observation, fn key, acc ->
        Map.put(acc, key, retire_at)
      end)

    entry = Map.put(entry, :unsubmitted_observation, observation)
    runtime = %{runtime | managed_delegations: replace_entry(runtime.managed_delegations, entry)}

    assert {:ok, _fence, retired_graph, :retired} =
             UnsubmittedSuccessor.prepare(runtime, context.fence, blocked_graph, entry, retire_at)

    refute Enum.any?(retired_graph.events, &(&1.type == :reconciled))
    assert Enum.count(retired_graph.events, &(&1.type == :unsubmitted_successor_retired)) == 2
  end

  test "v2 decoder requires the complete successor extension and exact provider projection", context do
    {runtime, entry} = successor(context)
    raw_payload = successor_payload(context)

    missing_observation =
      update_in(raw_payload["entries"], fn [first | rest] ->
        [Map.delete(first, "unsubmitted_observation") | rest]
      end)

    assert {:error, _} = ManagedResponsibility.decode(missing_observation, Fixture.context(), context.now)

    wrong_projection =
      update_in(raw_payload["entries"], fn [first | rest] ->
        [put_in(first, ["responsible", "scope", "work_package_id"], "linear:HGS-1") | rest]
      end)

    wrong_state_path =
      update_in(raw_payload["entries"], fn [first | rest] ->
        observation = Map.put(first["unsubmitted_observation"], "journal_path", "/tmp/other/work-package.json")
        [Map.put(first, "unsubmitted_observation", observation) | rest]
      end)

    assert {:error, _} = ManagedResponsibility.decode(wrong_projection, Fixture.context(), context.now)
    assert {:error, _} = ManagedResponsibility.decode(wrong_state_path, Fixture.context(), context.now)
    assert entry.issue_id == context.issue.id
    assert runtime.managed_delegations.entries |> Enum.any?(&(&1 == entry))
  end

  describe "signature-verified successor manifest boundary" do
    @describetag skip: System.get_env("SYMPHONY_TEST_ROOT_MANIFEST_FILES") != "1"

    test "only a valid Ed25519 manifest load supplies provenance accepted by retirement", context do
      assert {:unix, :linux} = :os.type()
      assert {"0\n", 0} = System.cmd("id", ["-u"])

      root = Path.join(System.tmp_dir!(), "unsubmitted-successor-#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)
      on_exit(fn -> File.rm_rf!(root) end)

      path = Path.join(root, "manifest.json")
      payload = successor_payload(context)
      bytes = Jason.encode!(payload)
      File.write!(path, bytes)
      File.chmod!(path, 0o644)
      assert File.stat!(path).uid == 0

      {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519)

      env = %{
        "DAHLIA_MANAGED_DELEGATION_PATH" => path,
        "DAHLIA_MANAGED_DELEGATION_SHA256" => digest(bytes),
        "DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519" => sign_v2(bytes, private_key),
        "DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519" => hex(public_key),
        "SYMPHONY_POOL_KEY" => "test-pool",
        "DAHLIA_RUNNER_ID" => "runner-test",
        "SYMPHONY_REPOSITORY_REF" => "openai/symphony",
        "DAHLIA_MANAGED_PROJECT_PROFILE_ID" => "profile-test"
      }

      assert {:ok, loaded_manifest} = Manifest.load(env, context.now)
      assert loaded_manifest.source_sha256 == digest(bytes)
      assert loaded_manifest.signer_key_sha256 == digest(public_key)

      entry = Enum.find(loaded_manifest.entries, &(&1.issue_id == context.issue.id))
      runtime = %{context.runtime | managed_delegations: loaded_manifest}

      assert {:ok, _fence, _graph, :retired} =
               UnsubmittedSuccessor.prepare(runtime, context.fence, context.graph, entry, context.now)

      assert {:error, _reason} =
               Manifest.load(
                 Map.put(env, "DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519", String.duplicate("0", 128)),
                 context.now
               )
    end
  end

  defp successor(context) do
    payload = successor_payload(context)
    {:ok, manifest} = ManagedResponsibility.decode(payload, Fixture.context(), context.now)

    manifest =
      Map.merge(manifest, %{
        source_sha256: String.duplicate("a", 64),
        signer_key_sha256: String.duplicate("b", 64)
      })

    entry = Enum.find(manifest.entries, &(&1.issue_id == context.issue.id))
    {%{context.runtime | managed_delegations: manifest}, entry}
  end

  defp successor_payload(context) do
    old_entry = Enum.find(context.runtime.managed_delegations.entries, &(&1.issue_id == context.issue.id))
    old_accountable = context.graph.delegations[old_entry.accountable.id]
    old_responsible = context.graph.delegations[old_entry.responsible.id]
    session_id = context.execution.leases |> Map.keys() |> hd()

    prior = %{
      "issue_id" => context.issue.id,
      "generation" => context.execution.generation,
      "repository_ref" => context.execution.repository,
      "managed_project_profile_id" => "profile-test",
      "accountable_id" => old_accountable.id,
      "responsible_id" => old_responsible.id,
      "accountable_digest" => grant_digest(old_accountable),
      "responsible_digest" => grant_digest(old_responsible)
    }

    observation = %{
      "issue_id" => context.issue.id,
      "generation" => context.execution.generation,
      "repository_ref" => context.execution.repository,
      "managed_project_profile_id" => "profile-test",
      "journal_path" => "/srv/dahlia-runner-state/run/pools/test-pool/work-package.json",
      "execution_fence_path" => "/srv/dahlia-runner-state/workspaces/pools/test-pool/.symphony/execution-fence.json",
      "responsibility_graph_path" => "/srv/dahlia-runner-state/workspaces/pools/test-pool/.symphony/responsibility-graph.json",
      "provider_projection_id" => @projection_id,
      "provider_reservation_state" => "reserved",
      "provider_claimed_at" => nil,
      "provider_claim_generation" => nil,
      "provider_execution_fence_token" => nil,
      "provider_observed_at_ms" => context.now,
      "provider_evidence_ref" => String.duplicate("1", 64),
      "kubernetes_namespace" => "frigga",
      "kubernetes_job_issue_matches" => 0,
      "kubernetes_pod_issue_matches" => 0,
      "kubernetes_jobs_resource_version" => "40139359",
      "kubernetes_pods_resource_version" => "40139362",
      "kubernetes_jobs_evidence_ref" => String.duplicate("2", 64),
      "kubernetes_pods_evidence_ref" => String.duplicate("3", 64),
      "kubernetes_observed_at_ms" => context.now,
      "process_unit" => ExecutionSupervisor.unit_name(context.issue.id, context.execution.generation, session_id),
      "process_load_state" => "not-found",
      "process_active_state" => "inactive",
      "process_control_group" => nil,
      "process_main_pid" => nil,
      "process_count" => 0,
      "process_observed_at_ms" => context.now,
      "process_evidence_ref" => String.duplicate("4", 64),
      "workspace_absent" => true,
      "workspace_observed_at_ms" => context.now,
      "workspace_evidence_ref" => String.duplicate("5", 64)
    }

    payload = Fixture.payload(context.now + 120_000)

    entries =
      Enum.map(payload["entries"], &successor_entry(&1, context, prior, observation))

    Map.put(payload, "entries", entries)
  end

  defp successor_entry(raw, context, prior, observation) do
    if raw["issue_id"] == context.issue.id do
      expires_at = context.now + 120_000

      raw
      |> Map.update!("accountable", &successor_accountable(&1, expires_at))
      |> Map.update!("responsible", &successor_responsible(&1, expires_at))
      |> Map.put("prior_unsubmitted_authority", prior)
      |> Map.put("unsubmitted_observation", observation)
    else
      raw
    end
  end

  defp successor_accountable(grant, expires_at) do
    grant
    |> Map.merge(%{"id" => "accountable-1-successor", "expires_at_ms" => expires_at})
    |> put_in(["scope", "work_package_id"], @projection_id)
  end

  defp successor_responsible(grant, expires_at) do
    grant
    |> Map.merge(%{
      "id" => "responsible-1-successor",
      "parent_delegation_id" => "accountable-1-successor",
      "expires_at_ms" => expires_at
    })
    |> put_in(["scope", "work_package_id"], @projection_id)
  end

  defp replace_entry(manifest, entry) do
    entries = Enum.map(manifest.entries, fn current -> if current.issue_id == entry.issue_id, do: entry, else: current end)
    %{manifest | entries: entries}
  end

  defp grant_digest(grant) do
    grant
    |> Map.take(@grant_fields)
    |> :erlang.term_to_binary([:deterministic])
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
  defp hex(bytes), do: Base.encode16(bytes, case: :lower)

  defp sign_v2(bytes, private_key) do
    :crypto.sign(:eddsa, :none, "hypergrid.symphony.managed-delegation.v2\0" <> bytes, [private_key, :ed25519])
    |> hex()
  end
end
