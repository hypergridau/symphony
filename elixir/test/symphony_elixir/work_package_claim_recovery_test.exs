Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.WorkPackageClaimRecoveryTest do
  use SymphonyElixir.TestSupport
  alias SymphonyElixir.{ExecutionFence, ManagedResponsibility, ResponsibilityGraph, WorkPackageClaim}
  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture
  alias SymphonyElixir.WorkPackageClaim.{Dispatch, Journal, Recovery, Unsubmitted}

  setup do
    root = Path.dirname(Workflow.workflow_file_path())
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", workspace_root: Path.join(root, "workspaces"), codex_max_total_tokens: 500_000)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    now = System.system_time(:millisecond)
    {:ok, graph, _} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), now)
    {:ok, manifest} = ManagedResponsibility.decode(Fixture.payload(now), Fixture.context(), now)

    runtime = %{
      managed_delegations: manifest,
      base_url: "http://127.0.0.1:1",
      runner_token: "test-token",
      attestation_key: "test-key",
      runner_id: "runner-test",
      managed_project_profile_id: "profile-test",
      journal_path: Path.join(root, "claims.json")
    }

    state = %Orchestrator.State{
      execution_fence: ExecutionFence.new(),
      responsibility_graph: graph,
      execution_fence_path: Config.execution_fence_state_path(),
      responsibility_graph_path: Config.responsibility_graph_state_path(),
      work_package_runtime: runtime,
      max_concurrent_agents: 1
    }

    state = Fixture.initialize_budget(state)
    issue = Fixture.issue(1)
    {:ok, state, token, session, delegation, lease} = Orchestrator.admit_execution_for_test(state, issue, nil)
    input = claim_input(state, issue)

    request = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue"),
        do: {:ok, %Req.Response{status: 200, body: reservation(issue.id)}},
        else: {:error, :response_lost_after_commit}
    end

    assert {:error, {:claim_indeterminate, _}} =
             WorkPackageClaim.claim(input,
               request_fun: request,
               now_fun: fn -> DateTime.from_unix!(now - 6_000, :millisecond) end
             )

    %{
      state: state,
      issue: issue,
      input: input,
      token: token,
      session: session,
      delegation: delegation,
      lease: lease,
      runtime: runtime
    }
  end

  test "real orchestrator restart recovers the same pre-spawn authority", context do
    name = Module.concat(__MODULE__, "Restart#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name, work_package_runtime: context.runtime})
    assert is_map(Orchestrator.responsibility_snapshot(pid))
    state = :sys.get_state(pid)
    assert state.running == %{}
    assert {:ok, recovered, token, session, delegation, lease} = Orchestrator.admit_execution_for_test(state, context.issue, nil)
    assert {token, session, delegation, lease} == {context.token, context.session, context.delegation, context.lease}
    assert recovered.execution_fence.history == context.state.execution_fence.history
    assert recovered.execution_fence.executions[context.issue.id].generation == 1
    assert recovered.responsibility_graph.delegations[delegation].runtime_lease == lease
  end

  test "pending authority uses its own slot and excludes fresh admission", context do
    assert Orchestrator.should_dispatch_issue_for_test(context.issue, context.state)
    refute Orchestrator.should_dispatch_issue_for_test(Fixture.issue(2), context.state)
    assert {:ok, _state, token, _, _, _} = Orchestrator.admit_execution_for_test(context.state, context.issue, nil)
    assert token == context.token
  end

  test "HTTP commit followed by a closed response is replayed after OTP restart", context do
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, reuseaddr: true, ip: {127, 0, 0, 1}])
    on_exit(fn -> :gen_tcp.close(listener) end)
    {:ok, {_, port}} = :inet.sockname(listener)
    parent = self()

    server =
      Task.async(fn ->
        {:ok, reserve_socket} = :gen_tcp.accept(listener, 5_000)
        read_request(reserve_socket)
        reply(reserve_socket, reservation(context.issue.id))
        {:ok, claim_socket} = :gen_tcp.accept(listener, 5_000)
        first = read_request(claim_socket)["attestation"]
        send(parent, {:provider_committed, first})
        :gen_tcp.close(claim_socket)
        {:ok, replay_socket} = :gen_tcp.accept(listener, 5_000)
        replay = read_request(replay_socket)["attestation"]
        assert Map.drop(replay, ["attestedAt", "signature"]) == Map.drop(first, ["attestedAt", "signature"])

        reply(replay_socket, %{
          "projectionId" => "package-1",
          "projectionState" => "active",
          "mutationState" => "applied",
          "claimEvidence" => Map.take(replay, ["responsibleDelegationId", "executionFenceToken", "runtimeLeaseId"])
        })
      end)

    runtime = %{context.runtime | base_url: "http://127.0.0.1:#{port}", journal_path: context.runtime.journal_path <> ".http"}
    state = Fixture.initialize_budget(%{context.state | work_package_runtime: runtime})
    input = claim_input(state, context.issue)

    assert {:error, {:claim_indeterminate, {:provider_request, _}}} =
             WorkPackageClaim.claim(input,
               now_fun: fn -> DateTime.add(DateTime.utc_now(), -6, :second) end
             )

    assert_receive {:provider_committed, committed}
    assert committed["generation"] == 1
    name = Module.concat(__MODULE__, "HttpRestart#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name, work_package_runtime: runtime})
    assert is_map(Orchestrator.responsibility_snapshot(pid))
    assert {:ok, recovered, token, _, _, _} = Orchestrator.admit_execution_for_test(:sys.get_state(pid), context.issue, nil)
    assert token == context.token
    assert {:ok, _} = WorkPackageClaim.claim(claim_input(recovered, context.issue))
    assert recovered.running == %{}
    assert :ok = Task.await(server, 5_000)
  end

  test "lost acknowledgement followed by replay requires one durable spawn boundary", context do
    response = fn _url, options ->
      payload = Keyword.fetch!(options, :json).attestation
      assert payload["generation"] == context.token.generation
      assert payload["sessionId"] == context.session

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "projectionId" => "package-1",
           "projectionState" => "active",
           "mutationState" => "applied",
           "claimEvidence" => %{"responsibleDelegationId" => context.delegation, "executionFenceToken" => "#{context.issue.id}:1", "runtimeLeaseId" => context.session}
         }
       }}
    end

    assert {:ok, _} = WorkPackageClaim.claim(context.input, request_fun: response)
    assert :ok = WorkPackageClaim.begin_spawn(context.input)
    assert {:error, _} = WorkPackageClaim.begin_spawn(context.input)
    assert {:error, :claim_spawn_already_attempted} = Orchestrator.admit_execution_for_test(context.state, context.issue, nil)
    assert context.state.running == %{}
  end

  test "missing and corrupt journals retain the fence instead of admitting another generation", context do
    File.rename!(context.runtime.journal_path, context.runtime.journal_path <> ".retained")
    assert {:error, :claim_recovery_journal_missing} = Orchestrator.admit_execution_for_test(context.state, context.issue, nil)
    File.write!(context.runtime.journal_path, "{broken")
    assert {:error, _} = Orchestrator.admit_execution_for_test(context.state, context.issue, nil)
    assert context.state.execution_fence.executions[context.issue.id].generation == 1
  end

  test "a surviving journal prevents fresh admission after loss of the fence", context do
    state = %{context.state | execution_fence: ExecutionFence.new()}
    assert {:error, :claim_exists_without_matching_fence} = Orchestrator.admit_execution_for_test(state, context.issue, nil)
  end

  test "journal read failures leave the real orchestrator fence unchanged during reconciliation", context do
    name = Module.concat(__MODULE__, "MissingJournal#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name, work_package_runtime: context.runtime})
    before = :sys.get_state(pid).execution_fence
    File.rename!(context.runtime.journal_path, context.runtime.journal_path <> ".retained")

    assert {:error, :claim_recovery_journal_missing} =
             Orchestrator.reconcile_execution_fence(pid, [], System.system_time(:millisecond), 300_000)

    assert :sys.get_state(pid).execution_fence == before
    File.write!(context.runtime.journal_path, "{broken")
    assert {:error, _} = Orchestrator.reconcile_execution_fence(pid, [], System.system_time(:millisecond), 300_000)
    assert :sys.get_state(pid).execution_fence == before
    assert Recovery.held?(before, context.issue.id)
  end

  test "the adapter cannot bless an unmarked legacy reservation with a new spawn marker", context do
    {:ok, journal} = Journal.load(context.runtime.journal_path)
    [key] = Map.keys(journal.reservations)
    {:ok, legacy} = Journal.put(journal, key, Map.delete(journal.reservations[key], :dispatch))
    assert :ok = Journal.save(context.runtime.journal_path, legacy)

    assert {:error, :legacy_claim_requires_reconciliation} =
             WorkPackageClaim.claim(context.input,
               request_fun: fn _url, _opts -> flunk("legacy authority must not cause an HTTP request") end
             )
  end

  test "generic missing-observation reconciliation preserves proven pre-spawn leases", context do
    now = System.system_time(:millisecond)
    {:ok, claims} = Recovery.unstarted_claims(context.runtime, context.state.execution_fence)
    before = context.state.execution_fence
    assert {:ok, fence, _summary} = ExecutionFence.reconcile_claim_sessions(before, [], claims, now, 300_000)
    assert fence.executions[context.issue.id].leases[context.session].status == :active
    assert {:ok, _, token, _, _, _} = Orchestrator.admit_execution_for_test(%{context.state | execution_fence: fence}, context.issue, nil)
    assert token == context.token

    observation =
      context.state.execution_fence.sessions[context.session]
      |> Map.put(:last_heartbeat_at, now)
      |> Map.put(:head, "actually-observed")

    assert {:ok, observed, _} = ExecutionFence.reconcile_claim_sessions(fence, [observation], claims, now, 300_000)
    assert {:error, _} = Orchestrator.admit_execution_for_test(%{context.state | execution_fence: observed}, context.issue, nil)

    stale = %{observation | generation: observation.generation + 1}
    assert {:ok, contradictory, _} = ExecutionFence.reconcile_claim_sessions(fence, [stale], claims, now, 300_000)
    assert contradictory.executions[context.issue.id].ownership == :contradictory
    assert {:error, _} = Orchestrator.admit_execution_for_test(%{context.state | execution_fence: contradictory}, context.issue, nil)
  end

  test "sixth confirmed attempt remains explicitly blocked across restart without resetting budget", context do
    {:ok, journal} = Journal.load(context.runtime.journal_path)
    [key] = Map.keys(journal.reservations)

    journal =
      Enum.reduce(2..6, journal, fn _attempt, previous ->
        due = DateTime.from_unix!(previous.reservations[key].dispatch.retry_at_ms, :millisecond)
        {:ok, next} = Dispatch.submit(previous, key, context.input, due)
        next
      end)

    {:ok, journal} = Dispatch.confirm(journal, key)
    assert :ok = Journal.save(context.runtime.journal_path, journal)
    name = Module.concat(__MODULE__, "Exhausted#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name, work_package_runtime: context.runtime})
    assert {:error, :claim_confirmed_revalidation_required} = Orchestrator.admit_execution_for_test(:sys.get_state(pid), context.issue, nil)
    assert {:ok, restored} = Journal.load(context.runtime.journal_path)
    assert restored.reservations[key].dispatch.attempts == 6
  end

  test "changed owner or expired authorization cannot recover a claim", context do
    assert {:error, _} = Orchestrator.admit_execution_for_test(context.state, %{context.issue | assignee_id: "different-owner"}, nil)
    manifest = context.runtime.managed_delegations
    changed = %{manifest | authority_ref: "different-authority"}
    state = %{context.state | work_package_runtime: %{context.runtime | managed_delegations: changed}}
    assert {:error, _} = Orchestrator.admit_execution_for_test(state, context.issue, nil)
    expired = put_in(context.state.responsibility_graph, [:delegations, context.delegation, :expires_at_ms], 1)
    assert {:error, _} = Orchestrator.admit_execution_for_test(%{context.state | responsibility_graph: expired}, context.issue, nil)
  end

  test "advanced generations and observed worker activity are never rolled back", context do
    {:ok, journal} = Journal.load(context.runtime.journal_path)
    [reservation] = Map.values(journal.reservations)
    assert {:error, _} = ExecutionFence.reconcile_unstarted_claim(context.state.execution_fence, %{reservation | generation: 2})
    fence = put_in(context.state.execution_fence, [:executions, context.issue.id, :leases, context.session, :head], "observed-head")
    assert {:error, _} = ExecutionFence.reconcile_unstarted_claim(fence, reservation)
    {:ok, journal} = Dispatch.begin_spawn(put_in(journal, [:reservations, hd(Map.keys(journal.reservations)), :dispatch, :phase], "confirmed"), hd(Map.keys(journal.reservations)), context.input)
    assert :ok = Journal.save(context.runtime.journal_path, journal)
    assert {:error, :claim_spawn_already_attempted} = Orchestrator.admit_execution_for_test(context.state, context.issue, nil)
  end

  test "cleaned failed attempt admits fresh authority after actual orchestrator restart", context do
    cleaned = cleaned_failed_attempt(context)
    assert :ok = ExecutionFence.Persistence.save(cleaned.execution_fence_path, cleaned.execution_fence)
    assert :ok = ResponsibilityGraph.Persistence.save(cleaned.responsibility_graph_path, cleaned.responsibility_graph)
    journal_before = File.read!(context.runtime.journal_path)
    name = Module.concat(__MODULE__, "FailedRestart#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name, work_package_runtime: context.runtime})
    restarted = :sys.get_state(pid)
    parent = restarted.responsibility_graph.delegations[context.delegation].parent_delegation_id
    assert restarted.responsibility_graph.delegations[parent].blocked_on == :restart_reconciliation

    assert {:ok, recovered, token, session, delegation, lease} =
             Orchestrator.admit_execution_for_test(restarted, context.issue, nil)

    assert token.generation == context.token.generation + 1
    refute session == context.session
    assert delegation == context.delegation
    assert lease.generation == token.generation
    assert recovered.responsibility_graph.delegations[parent].status == :active
    assert File.read!(context.runtime.journal_path) == journal_before
    assert Enum.any?(recovered.execution_fence.history, &(&1.generation == context.token.generation and &1.terminal.state == "Failed attempt"))
  end

  test "public revocation survives persistence and authorizes only its exact replacement", context do
    now = System.system_time(:millisecond)
    cleaned = cleaned_failed_attempt(context)
    before = cleaned.responsibility_graph
    parent = before.delegations[context.delegation].parent_delegation_id
    assert before.delegations[parent].expires_at_ms > now
    assert {:ok, ref} = Recovery.authority_revocation_ref(cleaned.execution_fence, before, context.token, context.delegation)
    assert {:ok, revoked, _} = ResponsibilityGraph.revoke(before, parent, ref, now)
    path = Path.join(Path.dirname(cleaned.responsibility_graph_path), "revoked-authority.json")
    assert :ok = ResponsibilityGraph.Persistence.save(path, revoked)
    assert {:ok, reloaded} = ResponsibilityGraph.Persistence.load(path)
    assert {:ok, ^ref} = Recovery.authority_revocation_ref(cleaned.execution_fence, reloaded, context.token, context.delegation)
    assert {:ok, manifest} = distinct_manifest(context.issue.id, now, "accountable-gen2", "responsible-gen2", ref)
    runtime = %{cleaned.work_package_runtime | managed_delegations: manifest}
    journal_before = File.read!(runtime.journal_path)

    assert {:new, candidate} = Recovery.prepare(runtime, cleaned.execution_fence, reloaded, context.issue, nil, now)
    assert candidate.delegations["responsible-gen2"].status == :active
    assert candidate.delegations["accountable-gen2"].actor_id == context.issue.assignee_id
    assert Map.take(candidate.delegations, Map.keys(reloaded.delegations)) == reloaded.delegations
    assert File.read!(runtime.journal_path) == journal_before

    for id <- [parent, context.delegation] do
      assert reloaded.delegations[id].status == :revoked

      assert Map.drop(reloaded.delegations[id], [:status, :terminal_reason, :terminal_evidence]) ==
               Map.drop(before.delegations[id], [:status, :terminal_reason, :terminal_evidence])
    end
  end

  test "revoked recovery refuses absent, wrong or changed retirement evidence and active authority", context do
    now = System.system_time(:millisecond)
    cleaned = cleaned_failed_attempt(context)
    graph = cleaned.responsibility_graph
    parent = graph.delegations[context.delegation].parent_delegation_id
    {:ok, ref} = Recovery.authority_revocation_ref(cleaned.execution_fence, graph, context.token, context.delegation)
    {:ok, revoked, _} = ResponsibilityGraph.revoke(graph, parent, ref, now)
    {:ok, valid} = distinct_manifest(context.issue.id, now, "accountable-gen2", "responsible-gen2", ref)
    {:ok, absent} = distinct_manifest(context.issue.id, now)
    {:ok, wrong} = distinct_manifest(context.issue.id, now, "accountable-gen2", "responsible-gen2", "sha256:" <> String.duplicate("f", 64))
    changed = update_in(revoked.delegations[parent].budget.max_tokens, &(&1 + 1))
    mixed = put_in(revoked.delegations[context.delegation].status, :active)

    invalid_candidates = [{revoked, absent}, {revoked, wrong}, {graph, valid}, {changed, valid}, {mixed, valid}]

    for {candidate_graph, manifest} <- invalid_candidates do
      runtime = %{cleaned.work_package_runtime | managed_delegations: manifest}

      assert {:error, :claim_abandonment_responsibility_changed} =
               Recovery.prepare(runtime, cleaned.execution_fence, candidate_graph, context.issue, nil, now)
    end

    assert {:error, _} = Recovery.authority_revocation_ref(cleaned.execution_fence, graph, %{context.token | generation: context.token.generation + 1}, context.delegation)
    assert {:error, _} = Recovery.authority_revocation_ref(context.state.execution_fence, graph, context.token, context.delegation)
  end

  test "real restart preserves expired authority while admitting a distinct manifest pair", context do
    now = System.system_time(:millisecond)
    cleaned = cleaned_failed_attempt(context)
    graph = expired_old_pair(cleaned.responsibility_graph, context.delegation, now)
    {:ok, manifest} = distinct_manifest(context.issue.id, now)
    runtime = %{cleaned.work_package_runtime | managed_delegations: manifest}
    :ok = ExecutionFence.Persistence.save(cleaned.execution_fence_path, cleaned.execution_fence)
    :ok = ResponsibilityGraph.Persistence.save(cleaned.responsibility_graph_path, graph)
    journal_before = File.read!(runtime.journal_path)
    name = Module.concat(__MODULE__, "DistinctRestart#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name, work_package_runtime: runtime})
    restarted = :sys.get_state(pid)

    assert {:ok, recovered, token, session, "responsible-gen2", lease} =
             Orchestrator.admit_execution_for_test(restarted, context.issue, nil)

    assert token.generation == context.token.generation + 1
    assert session != context.session
    assert lease.generation == token.generation
    assert recovered.responsibility_graph.delegations["responsible-gen2"].parent_delegation_id == "accountable-gen2"
    assert Map.take(recovered.responsibility_graph.delegations, Map.keys(graph.delegations)) == restarted.responsibility_graph.delegations
    assert Enum.take(recovered.responsibility_graph.events, -length(restarted.responsibility_graph.events)) == restarted.responsibility_graph.events
    assert :ok = ResponsibilityGraph.validate(recovered.responsibility_graph)
    assert File.read!(runtime.journal_path) == journal_before
    assert recovered.execution_fence.history == [restarted.execution_fence.executions[context.issue.id] | restarted.execution_fence.history]
    assert {:error, :stale_generation} = ExecutionFence.authorize(recovered.execution_fence, context.token, :commit)
  end

  test "real restart reconciles elapsed prior authority before distinct recovery", context do
    now = System.system_time(:millisecond)
    cleaned = cleaned_failed_attempt(context)
    graph = elapsed_old_pair(cleaned.responsibility_graph, context.delegation, now)
    parent = graph.delegations[context.delegation].parent_delegation_id
    assert Enum.all?([parent, context.delegation], &(graph.delegations[&1].status == :active))
    {:ok, manifest} = distinct_manifest(context.issue.id, now)
    runtime = %{cleaned.work_package_runtime | managed_delegations: manifest}
    :ok = ExecutionFence.Persistence.save(cleaned.execution_fence_path, cleaned.execution_fence)
    :ok = ResponsibilityGraph.Persistence.save(cleaned.responsibility_graph_path, graph)
    journal_before = File.read!(runtime.journal_path)
    name = Module.concat(__MODULE__, "ElapsedRestart#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name, work_package_runtime: runtime})
    restarted = :sys.get_state(pid)

    assert {:ok, recovered, token, _session, "responsible-gen2", _lease} =
             Orchestrator.admit_execution_for_test(restarted, context.issue, nil)

    for id <- [parent, context.delegation] do
      previous = restarted.responsibility_graph.delegations[id]
      current = recovered.responsibility_graph.delegations[id]
      assert current.status == :expired and current.terminal_reason == :lease_expired
      assert Map.drop(current, [:status, :terminal_reason]) == Map.drop(previous, [:status, :terminal_reason])
    end

    expiry_events =
      Enum.filter(recovered.responsibility_graph.events, fn event ->
        (Map.get(event, :type) || Map.get(event, "type")) in [:expired, "expired"]
      end)

    expired_ids = Enum.map(expiry_events, &(Map.get(&1, :delegation_id) || Map.get(&1, "delegation_id")))
    assert Enum.sort(expired_ids) == Enum.sort([parent, context.delegation])
    unrelated_ids = Map.keys(restarted.responsibility_graph.delegations) -- [parent, context.delegation]
    previous_unrelated = Map.take(restarted.responsibility_graph.delegations, unrelated_ids)
    assert Map.take(recovered.responsibility_graph.delegations, unrelated_ids) == previous_unrelated
    reconciliation_time = System.system_time(:millisecond)
    recovered_graph = recovered.responsibility_graph
    assert {:ok, same_graph, %{expired: []}} = ResponsibilityGraph.reconcile(recovered_graph, reconciliation_time)
    assert same_graph == recovered.responsibility_graph

    assert token.generation == context.token.generation + 1
    assert File.read!(runtime.journal_path) == journal_before
    assert Enum.take(recovered.responsibility_graph.events, -length(restarted.responsibility_graph.events)) == restarted.responsibility_graph.events
    assert recovered.execution_fence.history == [restarted.execution_fence.executions[context.issue.id] | restarted.execution_fence.history]
    assert {:error, :stale_generation} = ExecutionFence.authorize(recovered.execution_fence, context.token, :commit)
    assert :ok = ResponsibilityGraph.validate(recovered.responsibility_graph)
  end

  test "distinct recovery rejects partial authority, old leases, expired grants and changed ownership", context do
    now = System.system_time(:millisecond)
    cleaned = cleaned_failed_attempt(context)
    graph = expired_old_pair(cleaned.responsibility_graph, context.delegation, now)
    {:ok, manifest} = distinct_manifest(context.issue.id, now)
    entry = Enum.find(manifest.entries, &(&1.issue_id == context.issue.id))
    runtime = %{cleaned.work_package_runtime | managed_delegations: manifest}
    state = %{cleaned | work_package_runtime: runtime, responsibility_graph: graph}
    {:ok, partial, _} = ResponsibilityGraph.delegate(graph, entry.accountable, now)
    leased = put_in(graph, [:delegations, context.delegation, :runtime_lease], context.lease)
    parent = graph.delegations[context.delegation].parent_delegation_id
    accountable_leased = put_in(graph.delegations[parent].runtime_lease, context.lease)
    {:ok, expired_new_manifest} = distinct_manifest(context.issue.id, now - 120_000)
    {:ok, reused_parent} = distinct_manifest(context.issue.id, now, parent, "responsible-gen2")
    {:ok, reused_responsible} = distinct_manifest(context.issue.id, now, "accountable-gen2", context.delegation)
    missing = %{graph | delegations: Map.delete(graph.delegations, context.delegation)}
    journal_before = File.read!(runtime.journal_path)

    for {candidate, issue} <- [
          {%{state | responsibility_graph: cleaned.responsibility_graph}, context.issue},
          {%{state | responsibility_graph: partial}, context.issue},
          {%{state | responsibility_graph: leased}, context.issue},
          {%{state | responsibility_graph: accountable_leased}, context.issue},
          {%{state | responsibility_graph: missing}, context.issue},
          {put_in(state.work_package_runtime.managed_delegations, expired_new_manifest), context.issue},
          {put_in(state.work_package_runtime.managed_delegations, reused_parent), context.issue},
          {put_in(state.work_package_runtime.managed_delegations, reused_responsible), context.issue},
          {state, %{context.issue | assignee_id: "different-owner"}}
        ] do
      assert {:error, _} = Orchestrator.admit_execution_for_test(candidate, issue, nil)
      assert File.read!(runtime.journal_path) == journal_before
    end
  end

  test "same-grant failed retry verifies an expired never-submitted predecessor through runtime", context do
    now = System.system_time(:millisecond)
    cleaned = cleaned_failed_attempt(context)
    manifest = cleaned.work_package_runtime.managed_delegations

    entries =
      Enum.map(manifest.entries, fn entry ->
        if entry.issue_id == Fixture.issue(2).id do
          entry = put_in(entry.accountable.expires_at_ms, now + 1_000)
          put_in(entry.responsible.expires_at_ms, now + 1_000)
        else
          entry
        end
      end)

    state = put_in(cleaned.work_package_runtime.managed_delegations, %{manifest | entries: entries})
    manifest = state.work_package_runtime.managed_delegations
    {:ok, empty_graph, _} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), now)
    prior_state = %{state | execution_fence: ExecutionFence.new(), responsibility_graph: empty_graph}
    {:ok, predecessor, _, _, _, _} = Orchestrator.admit_execution_for_test(prior_state, Fixture.issue(2), nil)
    entry = Enum.find(manifest.entries, &(&1.issue_id == Fixture.issue(2).id))
    execution = predecessor.execution_fence.executions[entry.issue_id]
    File.mkdir_p!(Path.dirname(execution.worktree))
    assert File.lstat(execution.worktree) == {:error, :enoent}
    assert Map.take(predecessor.responsibility_graph.delegations[entry.accountable.id], Map.keys(entry.accountable)) == entry.accountable
    assert Map.take(predecessor.responsibility_graph.delegations[entry.responsible.id], Map.keys(entry.responsible)) == entry.responsible
    [worker] = Map.values(execution.leases)
    prior_fence = predecessor.execution_fence
    prior_token = %{issue_id: execution.issue_id, generation: execution.generation}
    assert {:ok, _} = ExecutionFence.release_unsubmitted_claim(prior_fence, prior_token, worker.session_id)
    {:ok, restart_graph} = ResponsibilityGraph.mark_unreconciled_after_restart(predecessor.responsibility_graph)
    predecessor = %{predecessor | responsibility_graph: restart_graph}
    observation = %{"issue_id" => entry.issue_id, "generation" => execution.generation, "provider_claim" => "absent", "active_process" => "absent", "evidence_ref" => "test:independent-absence"}
    runtime = predecessor.work_package_runtime
    prior_graph = predecessor.responsibility_graph
    {:ok, fence, graph} = Unsubmitted.retire_expired(runtime, prior_fence, prior_graph, entry, observation, now + 2_000)
    fence = %{cleaned.execution_fence | executions: Map.merge(cleaned.execution_fence.executions, fence.executions), sessions: Map.merge(cleaned.execution_fence.sessions, fence.sessions)}
    graph = %{cleaned.responsibility_graph | delegations: Map.merge(cleaned.responsibility_graph.delegations, graph.delegations), events: graph.events ++ cleaned.responsibility_graph.events}
    assert :ok = ExecutionFence.validate(fence)
    assert :ok = ResponsibilityGraph.validate(graph)
    retired = %{state | execution_fence: fence, responsibility_graph: graph}
    journal_before = File.read!(context.runtime.journal_path)
    assert {:ok, retried, token, _, _, _} = Orchestrator.admit_execution_for_test(retired, context.issue, nil)
    assert token.generation > context.token.generation
    assert retried.execution_fence.executions[entry.issue_id] == fence.executions[entry.issue_id]
    assert retried.responsibility_graph.delegations[entry.responsible.id] == graph.delegations[entry.responsible.id]
    assert File.read!(context.runtime.journal_path) == journal_before
    File.mkdir!(execution.worktree)
    assert {:error, :previous_repository_cleanup_required} = Orchestrator.admit_execution_for_test(retired, context.issue, nil)
    assert File.read!(context.runtime.journal_path) == journal_before
  end

  test "failed retry refuses changed responsibility and missing cleanup acknowledgement", context do
    cleaned = cleaned_failed_attempt(context)
    {:ok, graph} = ResponsibilityGraph.mark_unreconciled_after_restart(cleaned.responsibility_graph)
    state = %{cleaned | responsibility_graph: graph}
    parent = graph.delegations[context.delegation].parent_delegation_id

    for changed <- [
          put_in(graph, [:delegations, context.delegation, :runtime_lease], context.lease),
          put_in(graph, [:delegations, parent, :blocked_on], :external_decision),
          put_in(graph, [:delegations, parent, :runtime_lease], context.lease),
          put_in(graph, [:delegations, context.delegation, :expires_at_ms], 1)
        ] do
      assert {:error, _} = Orchestrator.admit_execution_for_test(%{state | responsibility_graph: changed}, context.issue, nil)
    end

    assert {:error, _} = Orchestrator.admit_execution_for_test(state, %{context.issue | assignee_id: "different-owner"}, nil)
    {:ok, journal} = Journal.load(context.runtime.journal_path)
    [key] = Map.keys(journal.reservations)
    {:ok, missing} = Journal.put(journal, key, Map.delete(journal.reservations[key], :cleanup_receipts))
    assert :ok = Journal.save(context.runtime.journal_path, missing)
    assert {:error, :claim_terminal_acknowledgement_required} = Orchestrator.admit_execution_for_test(state, context.issue, nil)
    assert graph.delegations[parent].blocked_on == :restart_reconciliation
  end

  test "queued historical issue releases only its new unsubmitted lease and leaves another task eligible", context do
    cleaned = cleaned_failed_attempt(context)
    journal_before = File.read!(context.runtime.journal_path)
    parent = self()

    request = fn url, options ->
      assert String.ends_with?(url, "/reservations/by-issue")
      send(parent, {:reservation_lookup, Keyword.fetch!(options, :json).issueId})
      {:ok, %Req.Response{status: 409, body: %{"error" => %{"code" => "work_package_reservation_not_reissuable"}}}}
    end

    {:ok, admitted, token, session, delegation, lease} = Orchestrator.admit_execution_for_test(cleaned, context.issue, nil)
    assert {:error, :reservation_not_ready} = WorkPackageClaim.claim(claim_input(admitted, context.issue), request_fun: request)

    entry = %{
      execution_token: token,
      execution_session_id: session,
      responsibility_delegation_id: delegation,
      responsibility_runtime_lease: lease
    }

    next = Orchestrator.handle_claim_failure_for_test(admitted, context.issue, :reservation_not_ready, entry)
    assert_receive {:reservation_lookup, first}
    assert first == context.issue.id
    assert next.running == %{}
    refute Recovery.held?(next.execution_fence, first)
    assert Orchestrator.should_dispatch_issue_for_test(Fixture.issue(2), next)
    assert File.read!(context.runtime.journal_path) == journal_before
    assert next.responsibility_graph.delegations[context.delegation].runtime_lease == nil
    assert Enum.all?(Map.values(next.execution_fence.executions[first].leases), &(&1.release_reason == :claim_not_submitted))
    File.mkdir_p!(Path.dirname(next.execution_fence.executions[first].worktree))
    assert {:ok, successor, _, _, "responsible-2", _} = Orchestrator.admit_execution_for_test(next, Fixture.issue(2), nil)
    assert successor.execution_fence.executions[first] == next.execution_fence.executions[first]
    assert successor.execution_fence.history == next.execution_fence.history
    assert File.read!(context.runtime.journal_path) == journal_before
  end

  test "real restart recovers an untouched unsubmitted generation without erasing historical claims", context do
    cleaned = cleaned_failed_attempt(context)
    {:ok, admitted, token, _, _, _} = Orchestrator.admit_execution_for_test(cleaned, context.issue, nil)
    assert token.generation == 2
    before = File.read!(context.runtime.journal_path)
    name = Module.concat(__MODULE__, "UnsubmittedRestart#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name, work_package_runtime: context.runtime})
    restarted = :sys.get_state(pid)
    assert restarted.execution_fence.executions[context.issue.id].ownership == :unknown
    assert {:ok, recovered, next_token, _, _, _} = Orchestrator.admit_execution_for_test(restarted, context.issue, nil)
    assert next_token.generation == token.generation + 1
    assert File.read!(context.runtime.journal_path) == before
    assert Enum.any?(recovered.execution_fence.history, &(&1.generation == admitted.execution_fence.executions[context.issue.id].generation))
    assert Enum.any?(recovered.execution_fence.history, &(&1.generation == 1 and &1.terminal.state == "Failed attempt"))
  end

  test "unsubmitted recovery completes graph-first and fence-first interrupted persistence", context do
    cleaned = cleaned_failed_attempt(context)
    {:ok, admitted, _, _, _, _} = Orchestrator.admit_execution_for_test(cleaned, context.issue, nil)
    execution = admitted.execution_fence.executions[context.issue.id]

    {:new, fence, graph} =
      Unsubmitted.prepare(
        context.runtime,
        admitted.execution_fence,
        admitted.responsibility_graph,
        execution,
        System.system_time(:millisecond)
      )

    for partial <- [%{admitted | responsibility_graph: graph}, %{admitted | execution_fence: fence}] do
      assert {:ok, recovered, token, _, _, _} = Orchestrator.admit_execution_for_test(partial, context.issue, nil)
      assert token.generation == 3
      assert recovered.responsibility_graph.delegations[context.delegation].runtime_lease.generation == 3
    end
  end

  test "unsubmitted proof rejects observed supervised released-stop and foreign responsibility state", context do
    cleaned = cleaned_failed_attempt(context)
    {:ok, admitted, _token, session, _, _} = Orchestrator.admit_execution_for_test(cleaned, context.issue, nil)
    path = [:executions, context.issue.id, :leases, session]

    for change <- [
          %{head: "observed"},
          %{last_heartbeat_at: 1},
          %{supervisor_identity: %{unexpected: true}},
          %{status: :released, release_reason: :orchestrator_stop},
          %{termination_required: true}
        ] do
      fence = update_in(admitted.execution_fence, path, &Map.merge(&1, change))
      assert {:error, _} = Orchestrator.admit_execution_for_test(%{admitted | execution_fence: fence}, context.issue, nil)
    end

    graph = put_in(admitted.responsibility_graph, [:delegations, context.delegation, :runtime_lease], context.lease)
    assert {:error, _} = Orchestrator.admit_execution_for_test(%{admitted | responsibility_graph: graph}, context.issue, nil)
    File.rename!(context.runtime.journal_path, context.runtime.journal_path <> ".unsubmitted-retained")
    assert {:error, :claim_recovery_journal_missing} = Orchestrator.admit_execution_for_test(admitted, context.issue, nil)
  end

  test "current and future claims remain held and a missing fence cannot reuse old history", context do
    assert Unsubmitted.claim_may_exist?(context.runtime, context.state.execution_fence, context.issue.id)

    assert :submitted =
             Unsubmitted.prepare(
               context.runtime,
               context.state.execution_fence,
               context.state.responsibility_graph,
               context.state.execution_fence.executions[context.issue.id],
               System.system_time(:millisecond)
             )

    cleaned = cleaned_failed_attempt(context)
    {:ok, admitted, _, _, _, _} = Orchestrator.admit_execution_for_test(cleaned, context.issue, nil)
    refute Unsubmitted.claim_may_exist?(context.runtime, admitted.execution_fence, context.issue.id)
    assert {:error, :claim_exists_without_matching_fence} = Orchestrator.admit_execution_for_test(%{admitted | execution_fence: ExecutionFence.new()}, context.issue, nil)
    {:ok, journal} = Journal.load(context.runtime.journal_path)
    [old] = Map.values(journal.reservations)
    future = %{old | generation: 3}
    key = Journal.reservation_key(old.issue_id, old.managed_project_profile_id, old.repository_ref, 3)
    {:ok, journal} = Journal.put(journal, key, future)
    assert :ok = Journal.save(context.runtime.journal_path, journal)
    assert Unsubmitted.claim_may_exist?(context.runtime, admitted.execution_fence, context.issue.id)
    assert {:error, _} = Orchestrator.admit_execution_for_test(admitted, context.issue, nil)
  end

  test "unrelated reservation conflicts retain the local claim slot", context do
    cleaned = cleaned_failed_attempt(context)
    {:ok, admitted, token, session, delegation, lease} = Orchestrator.admit_execution_for_test(cleaned, context.issue, nil)
    before = File.read!(context.runtime.journal_path)

    request = fn _url, _options ->
      {:ok, %Req.Response{status: 409, body: %{"error" => %{"code" => "different_conflict"}}}}
    end

    assert {:error, reason} = WorkPackageClaim.claim(claim_input(admitted, context.issue), request_fun: request)
    refute reason == :reservation_not_ready

    entry = %{
      execution_token: token,
      execution_session_id: session,
      responsibility_delegation_id: delegation,
      responsibility_runtime_lease: lease
    }

    retained = Orchestrator.handle_claim_failure_for_test(admitted, context.issue, reason, entry)
    assert Recovery.held?(retained.execution_fence, context.issue.id)
    assert retained.execution_fence == admitted.execution_fence
    assert File.read!(context.runtime.journal_path) == before
  end

  test "malformed and unreadable journals never prove an unsubmitted claim", context do
    cleaned = cleaned_failed_attempt(context)
    {:ok, admitted, _, _, _, _} = Orchestrator.admit_execution_for_test(cleaned, context.issue, nil)
    File.rename!(context.runtime.journal_path, context.runtime.journal_path <> ".preserved")
    bad = context.runtime.journal_path <> ".malformed"
    File.write!(bad, "{truncated")

    for path <- [bad, Path.dirname(bad)] do
      runtime = %{context.runtime | journal_path: path}
      assert Unsubmitted.claim_may_exist?(runtime, admitted.execution_fence, context.issue.id)

      assert {:error, _} =
               Orchestrator.admit_execution_for_test(%{admitted | work_package_runtime: runtime}, context.issue, nil)
    end

    assert File.read!(bad) == "{truncated"
  end

  test "same generation with a foreign profile cannot release current authority", context do
    cleaned = cleaned_failed_attempt(context)
    {:ok, admitted, token, _, _, _} = Orchestrator.admit_execution_for_test(cleaned, context.issue, nil)
    {:ok, journal} = Journal.load(context.runtime.journal_path)
    [old] = Map.values(journal.reservations)
    foreign = %{old | generation: token.generation, managed_project_profile_id: "foreign-profile"}

    key =
      Journal.reservation_key(
        old.issue_id,
        foreign.managed_project_profile_id,
        old.repository_ref,
        token.generation
      )

    {:ok, journal} = Journal.put(journal, key, foreign)
    assert :ok = Journal.save(context.runtime.journal_path, journal)
    before = File.read!(context.runtime.journal_path)
    assert Unsubmitted.claim_may_exist?(context.runtime, admitted.execution_fence, context.issue.id)

    assert {:error, :claim_recovery_identity_conflict} =
             Orchestrator.admit_execution_for_test(admitted, context.issue, nil)

    assert File.read!(context.runtime.journal_path) == before
  end

  defp expired_old_pair(graph, responsible_id, now) do
    graph = elapsed_old_pair(graph, responsible_id, now)
    parent_id = graph.delegations[responsible_id].parent_delegation_id
    {:ok, expired, %{expired: ids}} = ResponsibilityGraph.reconcile(graph, now)
    assert Enum.sort(ids) == Enum.sort([parent_id, responsible_id])
    assert :ok = ResponsibilityGraph.validate(expired)
    expired
  end

  defp elapsed_old_pair(graph, responsible_id, now) do
    parent_id = graph.delegations[responsible_id].parent_delegation_id

    Enum.reduce([parent_id, responsible_id], graph, fn id, current ->
      update_in(current.delegations[id], &Map.merge(&1, %{accepted_at_ms: now - 3, last_heartbeat_at: now - 2, expires_at_ms: now - 1}))
    end)
  end

  defp distinct_manifest(issue_id, now, accountable_id \\ "accountable-gen2", responsible_id \\ "responsible-gen2", revocation_ref \\ nil) do
    raw = Fixture.payload(now)

    entries =
      Enum.map(raw["entries"], fn entry ->
        if entry["issue_id"] == issue_id do
          updated =
            entry
            |> put_in(["accountable", "id"], accountable_id)
            |> put_in(["responsible", "id"], responsible_id)
            |> put_in(["responsible", "parent_delegation_id"], accountable_id)

          with_revocation_ref(updated, revocation_ref)
        else
          entry
        end
      end)

    ManagedResponsibility.decode(%{raw | "entries" => entries}, Fixture.context(), now)
  end

  defp with_revocation_ref(entry, ref) when is_binary(ref), do: Map.put(entry, "prior_authority_revocation_ref", ref)
  defp with_revocation_ref(entry, _ref), do: entry

  defp cleaned_failed_attempt(context) do
    now = System.system_time(:millisecond)
    head = String.duplicate("a", 40)
    evidence_ref = "sha256:" <> String.duplicate("b", 64)
    token = context.token
    {:ok, fence, :released} = ExecutionFence.release(context.state.execution_fence, token, context.session, :orchestrator_stop)
    evidence = %{session_id: context.session, process_id: context.lease.process_id, process_tree: :terminated, evidence_ref: evidence_ref, observed_at_ms: now}
    {:ok, fence, :confirmed} = ExecutionFence.confirm_termination(fence, token, context.session, evidence, now)
    {:ok, fence, :fenced} = ExecutionFence.FailedAttempt.record(fence, token, %{accepted_head: head, failure_evidence_ref: evidence_ref}, now)
    {:ok, fence, :prepared} = ExecutionFence.prepare_cleanup(fence, token, head, now, :failed)
    {:ok, fence} = ExecutionFence.record_cleanup_evidence(fence, token, head, evidence_ref, now)
    {:ok, fence, :cleaned} = ExecutionFence.cleanup(fence, token, head, now)
    {:ok, graph, _} = ResponsibilityGraph.release_runtime_lease(context.state.responsibility_graph, context.delegation, context.lease, now)
    {:ok, journal} = Journal.load(context.runtime.journal_path)
    [key] = Map.keys(journal.reservations)
    ack = %{reservation_state: "released", execution_capacity_state: "released", scope_state: "released", accepted_head: head}
    {:ok, journal} = Journal.put_cleanup_receipt(journal, key, "termination_confirmed", %{acknowledgement: ack})
    {:ok, journal} = Journal.put_cleanup_receipt(journal, key, "repository_cleanup_verified", %{acknowledgement: ack})
    assert :ok = Journal.save(context.runtime.journal_path, journal)
    %{context.state | execution_fence: fence, responsibility_graph: graph}
  end

  defp claim_input(state, issue) do
    Map.merge(state.work_package_runtime, %{
      issue_id: issue.id,
      issue_identifier: issue.identifier,
      repository_ref: "openai/symphony",
      fence_state: state.execution_fence,
      responsibility_graph: state.responsibility_graph
    })
  end

  defp reservation(issue_id) do
    %{
      "projectionId" => "package-1",
      "reservationId" => "reservation",
      "workspaceId" => "workspace-test",
      "companyId" => "company-test",
      "reservationNonce" => "test-private-nonce",
      "issueId" => issue_id,
      "managedProjectProfileId" => "profile-test",
      "repositoryRef" => "openai/symphony",
      "scopeKeys" => ["repo:openai/symphony"]
    }
  end

  defp read_request(socket, buffer \\ "") do
    case :binary.split(buffer, "\r\n\r\n") do
      [headers, body] ->
        [_, length] = Regex.run(~r/content-length:\s*(\d+)/i, headers)
        length = String.to_integer(length)

        if byte_size(body) >= length do
          Jason.decode!(binary_part(body, 0, length))
        else
          {:ok, data} = :gen_tcp.recv(socket, 0, 5_000)
          read_request(socket, buffer <> data)
        end

      [_headers] ->
        {:ok, data} = :gen_tcp.recv(socket, 0, 5_000)
        read_request(socket, buffer <> data)
    end
  end

  defp reply(socket, body) do
    encoded = Jason.encode!(body)
    :ok = :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: #{byte_size(encoded)}\r\nConnection: close\r\n\r\n" <> encoded)
    :gen_tcp.close(socket)
  end
end
