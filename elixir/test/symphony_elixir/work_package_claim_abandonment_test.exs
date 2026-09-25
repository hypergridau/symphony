Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.WorkPackageClaimAbandonmentTest do
  use SymphonyElixir.TestSupport
  alias SymphonyElixir.{ExecutionFence, ManagedResponsibility, ResponsibilityGraph}
  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture
  alias SymphonyElixir.WorkPackageClaim.{Abandonment, Journal, Recovery}

  # Produced by the actual Dahlia recovery.contract.ts implementation, not this adapter.
  @provider_tuple_digest "deba8a5045fe3a09b87b8e4347c21afd145f5fbe9a8fcc219a14a93692b2d2ff"

  setup do
    root = Path.dirname(Workflow.workflow_file_path())
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", workspace_root: Path.join(root, "workspaces"), codex_max_total_tokens: 500_000)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    now = System.system_time(:millisecond)
    issue = Fixture.issue(1)
    {:ok, graph, _} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), now)
    {:ok, manifest} = ManagedResponsibility.decode(Fixture.payload(now), Fixture.context(), now)
    {:ok, graph} = ManagedResponsibility.admit(graph, manifest, issue, now)

    state = %Orchestrator.State{
      execution_fence: ExecutionFence.new(),
      responsibility_graph: graph,
      execution_fence_path: Config.execution_fence_state_path(),
      responsibility_graph_path: Config.responsibility_graph_state_path()
    }

    state =
      Enum.reduce(1..3, state, fn _, current ->
        {:ok, admitted, token, session, delegation, lease} = Orchestrator.admit_execution_for_test(current, issue, nil)
        {:ok, fence, _} = ExecutionFence.release(admitted.execution_fence, token, session, :spawn_failed)
        graph = admitted.responsibility_graph
        release_now = System.system_time(:millisecond)
        {:ok, released, _} = ResponsibilityGraph.release_runtime_lease(graph, delegation, lease, release_now)
        %{admitted | execution_fence: fence, responsibility_graph: released}
      end)

    :ok = ExecutionFence.Persistence.save(state.execution_fence_path, state.execution_fence)
    :ok = ResponsibilityGraph.Persistence.save(state.responsibility_graph_path, state.responsibility_graph)

    session = "worker:#{issue.id}:1"

    reservation = %{
      issue_id: issue.id,
      repository_ref: "openai/symphony",
      runner_id: "runner-test",
      managed_project_profile_id: "profile-test",
      projection_id: "package-1",
      reservation_id: "reservation-1",
      reservation_nonce: "test-recovery-nonce",
      generation: 1,
      session_id: session,
      process_id: session,
      responsible_delegation_id: "responsible-1",
      execution_fence_token: "#{issue.id}:1",
      runtime_lease_id: session,
      scope_keys: ["openai/symphony"]
    }

    {:ok, journal} = Journal.put(Journal.new(), Journal.reservation_key(issue.id, "profile-test", "openai/symphony"), reservation)
    journal_path = Path.join(root, "legacy-claims.json")
    :ok = Journal.save(journal_path, journal)
    directory = Path.join(root, "recovery-receipts")
    File.mkdir_p!(directory)
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519)

    runtime = %{
      base_url: "http://127.0.0.1:1",
      runner_token: "test-token",
      attestation_key: "test-key",
      runner_id: "runner-test",
      managed_project_profile_id: "profile-test",
      journal_path: journal_path,
      managed_delegations: manifest,
      claim_recovery: %{directory: directory, public_key: public_key}
    }

    expected = %{
      "projectionId" => "package-1",
      "reservationId" => "reservation-1",
      "workspaceId" => "ws-test",
      "companyId" => "company-test",
      "issueId" => issue.id,
      "runnerId" => "runner-test",
      "managedProjectProfileId" => "profile-test",
      "repositoryRef" => "openai/symphony",
      "scopeKeys" => ["openai/symphony"],
      "generation" => 1,
      "sessionId" => session,
      "processId" => session,
      "responsibleDelegationId" => "responsible-1",
      "executionFenceToken" => "#{issue.id}:1",
      "runtimeLeaseId" => session,
      "nonceHash" => hash(reservation.reservation_nonce)
    }

    receipt = %{
      "recoveryId" => "recovery-1",
      "projectionId" => "package-1",
      "fenceRevision" => "prepared-revision",
      "oldTupleDigest" => @provider_tuple_digest,
      "oldNonceHash" => expected["nonceHash"],
      "nextGenerationFloor" => 4,
      "confirmedAt" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "proofDigest" => String.duplicate("a", 64),
      "projectionState" => "queued",
      "reservationState" => "released",
      "executionCapacityState" => "released",
      "scopeState" => "released"
    }

    envelope = %{
      "contractVersion" => "work-package-pre-spawn-recovery.v1",
      "expected" => expected,
      "receipt" => receipt,
      "localGenerationMax" => 3,
      "journalSHA256" => hash(File.read!(journal_path)),
      "neverSpawned" => true
    }

    context = %{
      state: Fixture.initialize_budget(%{state | work_package_runtime: runtime}),
      runtime: runtime,
      issue: issue,
      envelope: envelope,
      path: Path.join(directory, issue.id <> ".json"),
      private_key: private_key,
      journal: journal
    }

    sign_envelope(context, envelope)
    context
  end

  test "a real orchestrator restart admits only the next generation and preserves old evidence", c do
    before = File.read!(c.runtime.journal_path)
    name = Module.concat(__MODULE__, "Restart#{System.unique_integer([:positive])}")
    child = Supervisor.child_spec({Orchestrator, name: name, work_package_runtime: c.runtime}, id: name)
    pid = start_supervised!(child)
    assert is_map(Orchestrator.responsibility_snapshot(pid))
    state = :sys.get_state(pid)
    assert :authorized = Abandonment.check(c.runtime, state.execution_fence, c.issue.id)
    assert {:new, _graph} = prepare_recovery(c.runtime, state, c.issue)
    stop_supervised!(name)
    pid = start_supervised!(child)
    state = :sys.get_state(pid)
    assert {:ok, advanced, %{generation: 4}, _, _, _} = Orchestrator.admit_execution_for_test(state, c.issue, nil)
    assert advanced.execution_fence.history == [state.execution_fence.executions[c.issue.id] | state.execution_fence.history]
    assert File.read!(c.runtime.journal_path) == before
    assert :missing = Abandonment.check(c.runtime, advanced.execution_fence, c.issue.id)
    assert {:error, :stale_generation} = ExecutionFence.authorize(advanced.execution_fence, %{issue_id: c.issue.id, generation: 3}, :commit)
  end

  test "tampered signatures, changed local journal and generation rollback are held", c do
    {_, unrelated_key} = :crypto.generate_key(:eddsa, :ed25519)
    sign_envelope(%{c | private_key: unrelated_key}, c.envelope)
    assert {:error, _} = Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)
    sign_envelope(c, Map.put(c.envelope, "localGenerationMax", 4))
    assert {:error, _} = Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)
    sign_envelope(c, c.envelope)
    File.write!(c.runtime.journal_path, File.read!(c.runtime.journal_path) <> "\n")
    assert {:error, :claim_abandonment_local_state_changed} = Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)
  end

  test "an existing issue workspace forbids pre-spawn recovery", c do
    execution = c.state.execution_fence.executions[c.issue.id]
    File.mkdir_p!(execution.worktree)
    assert {:error, _} = Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)
  end

  test "a prior worker observation forbids pre-spawn recovery", c do
    observed = put_in(c.state.execution_fence, [:history, Access.at(0), :leases, "worker:#{c.issue.id}:2", :head], "observed-head")
    assert {:error, _} = Abandonment.check(c.runtime, observed, c.issue.id)
  end

  test "only explicit signed never-spawned evidence is accepted", c do
    sign_envelope(c, Map.put(c.envelope, "neverSpawned", false))
    assert {:error, _} = Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)
    sign_envelope(c, put_in(c.envelope, ["receipt", "scopeState"], "held"))
    assert {:error, _} = Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)
    assert :missing = Abandonment.check(Map.delete(c.runtime, :claim_recovery), c.state.execution_fence, c.issue.id)
  end

  test "a real spawn marker contradicts even an otherwise valid signed envelope", c do
    [key] = Map.keys(c.journal.reservations)
    dispatch = %{phase: "spawn_started", attempts: 1, retry_at_ms: 0, authority_digest: String.duplicate("a", 64)}
    marked = put_in(c.journal, [:reservations, key, :dispatch], dispatch)
    :ok = Journal.save(c.runtime.journal_path, marked)
    sign_envelope(c, Map.put(c.envelope, "journalSHA256", hash(File.read!(c.runtime.journal_path))))
    assert {:error, :claim_abandonment_local_state_changed} = Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)
  end

  test "new journal scope IDs must match the signed provider claim", c do
    [key] = Map.keys(c.journal.reservations)

    scoped =
      c.journal
      |> put_in([:reservations, key, :workspace_id], "ws-test")
      |> put_in([:reservations, key, :company_id], "company-test")

    :ok = Journal.save(c.runtime.journal_path, scoped)
    sign_envelope(c, Map.put(c.envelope, "journalSHA256", hash(File.read!(c.runtime.journal_path))))
    assert :authorized = Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)

    changed = put_in(scoped, [:reservations, key, :company_id], "other-company")
    :ok = Journal.save(c.runtime.journal_path, changed)
    sign_envelope(c, Map.put(c.envelope, "journalSHA256", hash(File.read!(c.runtime.journal_path))))

    assert {:error, :claim_abandonment_local_state_changed} =
             Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)
  end

  test "recovery cannot replace the current native owner", c do
    changed = %{c.issue | assignee_id: "changed-owner"}
    assert {:error, _} = prepare_recovery(c.runtime, c.state, changed)
  end

  test "missing current execution or malformed workspace path is held", c do
    missing = put_in(c.state.execution_fence, [:executions], %{})
    assert {:error, _} = Abandonment.check(c.runtime, missing, c.issue.id)
    malformed = put_in(c.state.execution_fence, [:executions, c.issue.id, :worktree], 42)
    assert {:error, _} = Abandonment.check(c.runtime, malformed, c.issue.id)
  end

  test "a parent with the wrong responsibility role is held", c do
    parent = c.runtime.managed_delegations.entries |> hd() |> get_in([:accountable, :id])
    graph = put_in(c.state.responsibility_graph, [:delegations, parent, :role], :responsible)
    state = %{c.state | responsibility_graph: graph}
    assert {:error, _} = prepare_recovery(c.runtime, state, c.issue)
  end

  test "another reservation cannot ride along with a confirmed recovery", c do
    [reservation] = Map.values(c.journal.reservations)
    other = %{reservation | reservation_id: "other-reservation", generation: 2}
    key = Journal.reservation_key(c.issue.id, "profile-test", "openai/symphony", 2)
    {:ok, journal} = Journal.put(c.journal, key, other)
    :ok = Journal.save(c.runtime.journal_path, journal)
    sign_envelope(c, Map.put(c.envelope, "journalSHA256", hash(File.read!(c.runtime.journal_path))))
    assert {:error, :claim_abandonment_local_state_changed} = Abandonment.check(c.runtime, c.state.execution_fence, c.issue.id)
  end

  defp prepare_recovery(runtime, state, issue) do
    now = System.system_time(:millisecond)
    Recovery.prepare(runtime, state.execution_fence, state.responsibility_graph, issue, nil, now)
  end

  defp sign_envelope(c, payload) do
    bytes = Jason.encode!(payload)
    signature = :crypto.sign(:eddsa, :none, bytes, [c.private_key, :ed25519])
    File.write!(c.path, Jason.encode!(%{payload: Base.url_encode64(bytes, padding: false), signature: Base.url_encode64(signature, padding: false)}))
  end

  defp hash(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
