defmodule SymphonyElixir.RKE2Job.PreSpawnAbortControllerTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{ExecutionFence, ManagedAssignmentBundle, ResponsibilityGraph, WorkPackageClaim}
  alias SymphonyElixir.ManagedExecutor.ClaimBinding
  alias SymphonyElixir.RKE2Job.{PreSpawnAbortController, SuspendedController}
  alias SymphonyElixir.WorkPackageClaim.Journal

  @issue "abort-controller-1"
  @profile "profile-abort-controller"
  @repo "hypergridau/symphony"
  @allocation "rke2job:v1:fixture-abort-controller"
  @digest String.duplicate("b", 64)

  defmodule Preflight do
    def preflight_owned(_allocation, _assignment, _key, context) do
      {owner, result} = context.preflight_context
      send(owner, {:preflight, result})
      result
    end
  end

  defmodule Root do
    def verify_eligibility(request) do
      owner = :persistent_term.get({__MODULE__, :owner}, self())
      send(owner, {:root_eligibility, request})
      :persistent_term.get({__MODULE__, :result}, :ok)
    end
  end

  defmodule Activation do
    def activate_owned(allocation, _assignment, _key, owner) do
      send(owner, {:activated, allocation.id})
      {:ok, %{status: :active}}
    end
  end

  test "typed denial checkpoints an abort while ordinary preflight success stays suspended" do
    fixture = fixture()
    success = controller_context(fixture, :ok)

    assert {:ok, :ready} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, success)
    assert_receive {:preflight, :ok}
    refute_receive {:root_eligibility, _}
    assert {:ok, %{phase: "allocation_suspended", allocation_id: @allocation}} =
             WorkPackageClaim.handoff_allocation(fixture.input)

    denied = controller_context(fixture, {:denied, :codex_auth_slot_denied})
    root_result(self(), :ok)

    assert {:abort, reservation} =
             PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, denied)

    assert reservation.dispatch.phase == "abort_pending"
    assert reservation.dispatch.abort_reason == "codex_auth_slot_denied"
    assert reservation.dispatch.allocation_id == @allocation
    assert_receive {:preflight, {:denied, :codex_auth_slot_denied}}
    assert_receive {:root_eligibility, %{"operation" => "verify_pre_execution_abort_eligibility"}}
    assert {:held, :pre_spawn_abort_pending} =
             SuspendedController.resume(fixture.assignment, fixture.input, Map.put(denied, :adapter, Activation))

    refute_receive {:activated, _}
    refute_receive {:root_spawn, _}
  end

  test "held, malformed, and timeout preflights leave no abort checkpoint or result effect" do
    for result <- [{:held, :lease_uncertain}, {:error, :malformed_response}, {:error, :timeout}, {:denied, :other}] do
      fixture = fixture()
      context = controller_context(fixture, result)
      root_result(self(), :ok)

      assert {:held, _reason} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
      assert_receive {:preflight, ^result}
      refute_receive {:root_eligibility, _}
      assert {:ok, %{phase: "allocation_suspended", allocation_id: @allocation}} =
               WorkPackageClaim.handoff_allocation(fixture.input)
    end
  end

  test "missing root eligibility holds without writing abort state" do
    fixture = fixture()
    root_result(self(), {:error, :root_unavailable})

    assert {:held, :root_unavailable} =
             PreSpawnAbortController.before_resume(
               fixture.assignment,
               fixture.input,
               controller_context(fixture, {:denied, :codex_auth_slot_denied})
             )

    assert_receive {:root_eligibility, _}
    assert {:ok, %{phase: "allocation_suspended"}} = WorkPackageClaim.handoff_allocation(fixture.input)
  end

  test "abort survives journal reload and blocks spawn intent and controller resume" do
    fixture = fixture()
    root_result(self(), :ok)
    context = controller_context(fixture, {:denied, :codex_auth_slot_denied})

    assert {:abort, first} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
    assert {:ok, reloaded} = Journal.load(fixture.input.journal_path)
    key = Journal.reservation_key(@issue, @profile, @repo, 1)
    retained = reloaded.reservations[key]
    assert retained.dispatch == first.dispatch
    assert retained.dispatch.allocation_id == @allocation
    assert retained.dispatch.abort_reason == "codex_auth_slot_denied"
    assert {:error, :invalid_claim_dispatch_transition} =
             WorkPackageClaim.begin_suspended_spawn(fixture.input, @allocation)
    assert {:held, :pre_spawn_abort_pending} =
             SuspendedController.resume(fixture.assignment, fixture.input, Map.put(context, :adapter, Activation))
    refute_receive {:activated, _}

    assert {:abort, second} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
    assert second.dispatch == first.dispatch
    expected_result = %{
      assignment_digest: fixture.assignment.sha256,
      abort_reason: :codex_auth_slot_denied,
      outcome: :blocked,
      summary: SymphonyElixir.ManagedExecutor.Record.pre_execution_summary(:codex_auth_slot_denied),
      evidence_ref: "managed-executor:#{fixture.assignment.sha256}:codex_auth_slot_denied"
    }
    assert PreSpawnAbortController.result(fixture.assignment) == expected_result
    refute_receive {:preflight, _}
  end

  test "abort config is credential-free and tampering fails journal save and decode" do
    fixture = fixture()
    root_result(self(), :ok)

    assert {:abort, _} =
             PreSpawnAbortController.before_resume(
               fixture.assignment,
               fixture.input,
               controller_context(fixture, {:denied, :codex_auth_slot_denied})
             )

    {:ok, journal} = Journal.load(fixture.input.journal_path)
    key = Journal.reservation_key(@issue, @profile, @repo, 1)
    original = journal.reservations[key]
    tampered = put_in(original, [:dispatch, :abort_config, :runner_token], "must-not-persist")
    {:ok, bad_journal} = Journal.put(journal, key, tampered)
    assert {:error, :invalid_journal} = Journal.save(fixture.input.journal_path <> ".tampered", bad_journal)

    decoded = Jason.decode!(File.read!(fixture.input.journal_path))
    tampered_json = put_in(decoded, ["reservations", key, "dispatch", "abort_config", "runner_token"], "must-not-persist")
    assert {:error, _} = Journal.decode_bytes(Jason.encode!(tampered_json))
  end

  test "unsafe worker lease observations hold the abort" do
    fixture = fixture()
    root_result(self(), :ok)
    assert {:abort, reservation} =
             PreSpawnAbortController.before_resume(
               fixture.assignment,
               fixture.input,
               controller_context(fixture, {:denied, :codex_auth_slot_denied})
             )

    for change <- [
          &Map.put(&1, :head, "observed-head"),
          &Map.put(&1, :last_heartbeat_at, 10),
          &Map.put(&1, :supervisor_identity, "supervisor-1"),
          &Map.put(&1, :status, :released),
          &Map.put(&1, :release_reason, :operator_cancelled)
        ] do
      input = update_in(fixture.input, [:fence_state, :sessions, "worker-abort-1"], change)
      assert {:held, :pre_spawn_worker_state_unverified} =
               PreSpawnAbortController.unused_worker(input, fixture.assignment, reservation, false)
    end
  end

  test "abort cannot replace an already persisted spawn intent" do
    fixture = fixture()
    assert :ok = WorkPackageClaim.begin_suspended_spawn(fixture.input, @allocation)
    context = controller_context(fixture, {:denied, :codex_auth_slot_denied})

    assert {:ok, :ready} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
    refute_receive {:preflight, _}
    refute_receive {:root_eligibility, _}
    assert_receive {:root_spawn, "spawn_intent"}
    assert {:ok, %{phase: "spawn_started", allocation_id: @allocation}} =
             WorkPackageClaim.handoff_allocation(fixture.input)
    {:ok, journal} = Journal.load(fixture.input.journal_path)
    key = Journal.reservation_key(@issue, @profile, @repo, 1)
    assert {:error, :pre_spawn_abort_not_admissible} =
             SymphonyElixir.WorkPackageClaim.Dispatch.begin_suspended_abort(
               journal,
               key,
               fixture.input,
               @allocation,
               config(fixture.assignment)
             )
  end

  test "concurrent spawn and typed abort are serialized by the shared claim journal" do
    fixture = fixture()
    root_result(self(), :ok)
    context = controller_context(fixture, {:denied, :codex_auth_slot_denied})
    parent = self()
    spawn_task = Task.async(fn -> send(parent, :spawn_ready); receive do :go -> WorkPackageClaim.begin_suspended_spawn(fixture.input, @allocation) end end)
    abort_task = Task.async(fn -> send(parent, :abort_ready); receive do :go -> PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context) end end)
    assert_receive :spawn_ready
    assert_receive :abort_ready
    send(spawn_task.pid, :go)
    send(abort_task.pid, :go)
    spawn_result = Task.await(spawn_task, 5_000)
    abort_result = Task.await(abort_task, 5_000)
    {:ok, final} = Journal.load(fixture.input.journal_path)
    key = Journal.reservation_key(@issue, @profile, @repo, 1)
    phase = final.reservations[key].dispatch.phase

    assert phase in ["spawn_started", "abort_pending"]
    refute (spawn_result == :ok and match?({:abort, _}, abort_result))
    refute (phase == "abort_pending" and spawn_result == :ok)
  end

  defp fixture do
    path = Path.join(System.tmp_dir!(), "pre-spawn-abort-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(path); File.rm(path <> ".lock"); File.rm(path <> ".tampered") end)
    assignment = assignment()
    input = claim_input(path)
    request_fun = fn url, _options ->
      payload = if String.ends_with?(url, "/reservations/by-issue"), do: reservation_payload(), else: claim_result_payload()
      {:ok, %Req.Response{status: 200, body: %{"data" => payload}}}
    end

    assert {:ok, _claim} = WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end)
    assert :ok = WorkPackageClaim.record_suspended_allocation(input, %{id: @allocation, status: :ready})
    {:ok, journal} = Journal.load(path)
    key = Journal.reservation_key(@issue, @profile, @repo, 1)
    {:ok, binding} = ClaimBinding.from_journal(journal.reservations[key], assignment, "runner-abort-1")
    input = Map.put(input, :claim_binding, binding)
    %{input: input, assignment: assignment}
  end

  defp controller_context(fixture, preflight_result) do
    %{adapter: Preflight, config: config(fixture.assignment), root_abort_input_publisher: Root,
      abort_caller_test_ports: %{}, preflight_context: {self(), preflight_result}}
  end

  defp root_result(owner, result) do
    :persistent_term.put({Root, :owner}, owner)
    :persistent_term.put({Root, :result}, result)
  end

  defp config(assignment) do
    slot = %{slot_id: "slot-abort", lease_id: "lease-abort", claim_name: "claim-abort", claim_uid: "uid-abort",
      assignment_sha256: assignment.sha256, binding_sha256: @digest, seat: assignment.seat}
    %{namespace: "frigga", image: "registry.example/worker@sha256:" <> String.duplicate("a", 64), repository_id: "repo-id",
      auth_slot: slot, auth_slot_catalog: %{slot.slot_id => slot.claim_name}, assignment_binding_digest: @digest}
  end

  defp assignment do
    {:ok, assignment} = ManagedAssignmentBundle.build(%{objective: %{id: "objective-abort", identity: "objective-abort", content: "one task"},
      repository_ref: @repo, base_ref: "refs/remotes/origin/main", branch: "codex/pre-spawn-abort", seat: "runner-abort-1",
      lease: %{issue_id: @issue, repository: @repo, generation: 1, session_id: "worker-abort-1", process_id: "process-abort-1"},
      intent_ancestry: ["owner", "delegation-abort"], acceptance: %{deliverable: "abort regression", evidence: "synthetic"},
      context_secret_refs: [], platform: "linux-x86_64", environment_classification: "repository",
      environment_constraints: ["repository", "no-production-workload"], placement: :internal_beta, target_environment: :rke2})
    assignment
  end

  defp claim_input(path) do
    fence = ExecutionFence.new()
    {:ok, admitted, token} = ExecutionFence.admit(fence, %{issue_id: @issue, repository: @repo, branch: "codex/pre-spawn-abort", worktree: "tmp"}, 0)
    {:ok, fence_state, :registered} = ExecutionFence.register(admitted, token, :worker, %{session_id: "worker-abort-1", process_id: "process-abort-1",
      branch: "codex/pre-spawn-abort", worktree: "tmp", linear_state: "In Progress", pr_state: "none", head: "unobserved", last_heartbeat_at: 0}, 0)
    lease = %{issue_id: @issue, repository: @repo, generation: 1, session_id: "worker-abort-1", process_id: "process-abort-1"}
    scope = %{company_id: "hypergrid", objective_id: "objective-abort", initiative_id: "initiative", project_id: "project", work_package_id: "package",
      issue_id: @issue, repository: @repo, paths: [], modules: [], environments: ["local"], actions: [:read, :observe, :delegate, :reconcile, :edit, :commit, :push, :state_mutation, :cleanup, :review, :report]}
    authority = %{class: :routine_engineering, capabilities: scope.actions, environments: ["local"]}
    budget = %{model: "luna", effort: :high, max_tokens: 1000, max_children: 1}
    {:ok, owner_graph, _} = ResponsibilityGraph.delegate(ResponsibilityGraph.new(), delegation("owner", :accountable, scope, authority, budget), 0)
    {:ok, graph, _} = ResponsibilityGraph.delegate(owner_graph, delegation("delegation-abort", :responsible, scope, authority, budget, parent_delegation_id: "owner"), 0)
    {:ok, graph} = ResponsibilityGraph.bind_runtime_lease(graph, "delegation-abort", lease, 0)
    %{base_url: "http://provider.test", runner_token: "runner-token", attestation_key: "attestation-key", runner_id: "runner-abort-1", pool_key: "midgard",
      host_witness_fun: fn request ->
        if request["operation"] == "spawn_intent", do: send(self(), {:root_spawn, request["operation"]})
        {:ok, %{"ok" => true, "receipt" => %{"version" => 1, "sequence" => 1, "hash" => String.duplicate("a", 64), "replayed" => false}}}
      end,
      managed_project_profile_id: @profile, issue_id: @issue, issue_identifier: "HGS-TEST", repository_ref: @repo, fence_state: fence_state,
      responsibility_graph: graph, journal_path: path}
  end

  defp delegation(id, role, scope, authority, budget, extras \\ []) do
    Map.merge(%{id: id, parent_delegation_id: nil, role: role, actor_id: id, scope: scope, authority: authority, budget: budget, runtime_lease: nil,
      expires_at_ms: 2_000_000_000_000, expected_deliverable: "adapter", expected_evidence: "tests", return_to_parent: %{owner_id: "owner", contract: "evidence"}}, Map.new(extras))
  end

  defp reservation_payload do
    %{"projectionId" => "projection-abort", "reservationId" => "reservation-abort", "workspaceId" => "workspace-abort", "companyId" => "hypergrid",
      "reservationNonce" => "nonce-abort", "issueId" => @issue, "managedProjectProfileId" => @profile, "repositoryRef" => @repo,
      "scopeKeys" => ["repo:#{@repo}", "work:abort"]}
  end

  defp claim_result_payload do
    %{"projectionId" => "projection-abort", "projectionState" => "active", "mutationState" => "applied",
      "claimEvidence" => %{"responsibleDelegationId" => "delegation-abort", "executionFenceToken" => "#{@issue}:1", "runtimeLeaseId" => "worker-abort-1"}}
  end
end
