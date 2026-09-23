defmodule SymphonyElixir.WorkPackageClaimTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.Codex.ModelRouter
  alias SymphonyElixir.{ExecutionFence, Orchestrator, ResponsibilityGraph, WorkPackageClaim}
  alias SymphonyElixir.Tracker.Issue
  alias SymphonyElixir.WorkPackageClaim.{Journal, Recovery}

  @repository "hypergridau/symphony"
  @issue_id "issue-349"
  @profile "profile-349"

  @canonical_json_fixture ~s({"contractVersion":"work-package-runtime-attestation.v1","runnerId":"runner-349","managedProjectProfileId":"profile-349","reservationId":"reservation-349","reservationNonce":"nonce-349","issueId":"issue-349","generation":1,"sessionId":"worker-349","processId":"process-349","responsibleDelegationId":"delegation-349","executionFenceToken":"issue-349:1","runtimeLeaseId":"worker-349","repositoryRef":"hypergridau/symphony","scopeKeys":["repo:hypergridau/symphony","work:349"],"attestedAt":"2026-09-06T10:00:00.000Z"})

  test "managed final pause read precedes the durable spawn marker" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input, token: token, lease: lease} = authority_fixture(path)

    previous_pause_path = System.get_env("SYMPHONY_GLOBAL_PAUSE_FILE")
    pause_root = Path.join(System.tmp_dir!(), "symphony-spawn-order-#{System.unique_integer([:positive])}")
    File.mkdir_p!(pause_root)
    pause_path = Path.join(pause_root, "global-mutable-pause.state")
    File.write!(pause_path, "running\n")
    System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", pause_path)

    on_exit(fn ->
      if is_binary(previous_pause_path), do: System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", previous_pause_path), else: System.delete_env("SYMPHONY_GLOBAL_PAUSE_FILE")
      File.rm_rf(pause_root)
    end)

    claim_time = ~U[2026-09-06 10:00:00.000Z]

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue") do
        {:ok, response(%{"data" => reservation_payload()})}
      else
        {:ok, response(%{"data" => claim_result_payload()})}
      end
    end

    assert {:ok, _claim} = WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> claim_time end)
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "confirmed"

    runtime = Map.take(input, [:base_url, :runner_token, :attestation_key, :runner_id, :managed_project_profile_id, :journal_path])
    task_supervisor = start_supervised!({Task.Supervisor, max_children: 0})

    state = %Orchestrator.State{
      execution_fence: input.fence_state,
      responsibility_graph: input.responsibility_graph,
      work_package_runtime: runtime,
      task_supervisor: task_supervisor
    }

    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Spawn order", state: "Todo"}
    pause_pattern = {SymphonyElixir.GlobalPause, :paused?, 0}
    spawn_pattern = {WorkPackageClaim, :begin_spawn, 1}
    parent = self()
    tracer = spawn(fn -> forward_trace(parent) end)

    :erlang.trace_pattern(pause_pattern, true, [:local])
    :erlang.trace_pattern(spawn_pattern, true, [:local])
    :erlang.trace(self(), true, [:call, {:tracer, tracer}])

    after_spawn =
      try do
        Orchestrator.spawn_fenced_issue_for_test(state, issue, token, lease.session_id, "delegation-349", lease)
      after
        :erlang.trace(self(), false, [:call])
        :erlang.trace_pattern(pause_pattern, false, [:local])
        :erlang.trace_pattern(spawn_pattern, false, [:local])
        send(tracer, :stop)
      end

    assert_receive :trace_complete

    calls =
      Stream.repeatedly(fn ->
        receive do
          {:trace, pid, :call, {module, function, _args}} when pid == self() -> {module, function}
        after
          0 -> :done
        end
      end)
      |> Enum.take_while(&(&1 != :done))

    final_gate = {SymphonyElixir.GlobalPause, :paused?}
    durable_marker = {WorkPackageClaim, :begin_spawn}
    marker_index = Enum.find_index(calls, &(&1 == durable_marker))
    assert is_integer(marker_index), inspect({calls, after_spawn.blocked})
    assert final_gate in Enum.take(calls, marker_index)
    refute final_gate in Enum.drop(calls, marker_index + 1)

    assert after_spawn.running == %{}
    assert Task.Supervisor.children(task_supervisor) == []
    assert {:ok, journal_after_spawn} = Journal.load(path)
    [{_key, reservation_after_spawn}] = Map.to_list(journal_after_spawn.reservations)
    assert reservation_after_spawn.dispatch.phase == "spawn_started"
  end

  test "real orchestrator snapshot waits for a managed spawn already past its final gate" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input, token: token, lease: lease} = authority_fixture(path)

    previous_pause_path = System.get_env("SYMPHONY_GLOBAL_PAUSE_FILE")
    pause_root = Path.join(System.tmp_dir!(), "symphony-managed-barrier-#{System.unique_integer([:positive])}")
    File.mkdir_p!(pause_root)
    pause_path = Path.join(pause_root, "global-mutable-pause.state")
    transition_path = Path.join(pause_root, "global-mutable-pause.transition")
    File.write!(pause_path, "running\n")
    System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", pause_path)

    on_exit(fn ->
      if is_binary(previous_pause_path), do: System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", previous_pause_path), else: System.delete_env("SYMPHONY_GLOBAL_PAUSE_FILE")
      File.rm_rf(pause_root)
    end)

    previous_workflow_path = Application.get_env(:symphony_elixir, :workflow_file_path)
    previous_issues = Application.get_env(:symphony_elixir, :memory_tracker_issues)
    workflow_path = Path.join(pause_root, "WORKFLOW.md")

    SymphonyElixir.TestSupport.write_workflow_file!(workflow_path,
      tracker_kind: "memory",
      workspace_root: pause_root,
      poll_interval_ms: 60_000
    )

    SymphonyElixir.Workflow.set_workflow_file_path(workflow_path)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])

    on_exit(fn ->
      if is_binary(previous_workflow_path), do: SymphonyElixir.Workflow.set_workflow_file_path(previous_workflow_path), else: SymphonyElixir.Workflow.clear_workflow_file_path()
      if is_nil(previous_issues), do: Application.delete_env(:symphony_elixir, :memory_tracker_issues), else: Application.put_env(:symphony_elixir, :memory_tracker_issues, previous_issues)
    end)

    claim_time = ~U[2026-09-06 10:00:00.000Z]

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue") do
        {:ok, response(%{"data" => reservation_payload()})}
      else
        {:ok, response(%{"data" => claim_result_payload()})}
      end
    end

    assert {:ok, _claim} = WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> claim_time end)

    # The real supervisor's zero-child capacity makes release fail before an
    # AgentRunner closure can execute. Suspension holds the synchronous start.
    held_supervisor = start_supervised!({Task.Supervisor, max_children: 0})
    on_exit(fn -> if Process.alive?(held_supervisor), do: :sys.resume(held_supervisor) end)

    runtime = Map.take(input, [:base_url, :runner_token, :attestation_key, :runner_id, :managed_project_profile_id, :journal_path])

    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Managed barrier", state: "Todo"}
    name = Module.concat(__MODULE__, "ManagedBarrier#{System.unique_integer([:positive])}")
    {:ok, orchestrator} = Orchestrator.start_link(name: name)
    Process.unlink(orchestrator)
    on_exit(fn -> if Process.alive?(orchestrator), do: Process.exit(orchestrator, :kill) end)

    assert Enum.any?(1..200, fn _ ->
             case Orchestrator.snapshot(name, 1_000).startup_maintenance do
               %{status: "succeeded"} ->
                 true

               _ ->
                 Process.sleep(5)
                 false
             end
           end)

    assert :ok = :sys.suspend(held_supervisor)

    :sys.replace_state(orchestrator, fn state ->
      %{state | execution_fence: input.fence_state, responsibility_graph: input.responsibility_graph, work_package_runtime: runtime, task_supervisor: held_supervisor}
    end)

    dispatch =
      Task.async(fn ->
        :sys.replace_state(orchestrator, fn state ->
          Orchestrator.spawn_fenced_issue_for_test(state, issue, token, lease.session_id, "delegation-349", lease)
        end)
      end)

    assert Enum.any?(1..200, fn _ ->
             case Process.info(held_supervisor, :messages) do
               {:messages, messages} ->
                 if Enum.any?(messages, &match?({:"$gen_call", _, {:start_task, _, _, _}}, &1)),
                   do: true,
                   else:
                     (
                       Process.sleep(5)
                       false
                     )

               _ ->
                 false
             end
           end)

    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "spawn_started"

    epoch = String.duplicate("d", 32)
    File.write!(transition_path, "pausing:#{epoch}\n")
    File.write!(pause_path, "paused\n")
    snapshot = Task.async(fn -> Orchestrator.snapshot(name, 10_000) end)

    assert Enum.any?(1..200, fn _ ->
             case Process.info(orchestrator, :messages) do
               {:messages, messages} ->
                 if Enum.any?(messages, &match?({:"$gen_call", _, :snapshot}, &1)),
                   do: true,
                   else:
                     (
                       Process.sleep(5)
                       false
                     )

               _ ->
                 false
             end
           end)

    assert Task.yield(snapshot, 0) == nil
    assert :ok = :sys.resume(held_supervisor)
    assert %Orchestrator.State{running: running_state, blocked: blocked_state} = Task.await(dispatch, 5_000)
    assert running_state == %{}
    assert Map.has_key?(blocked_state, @issue_id)
    assert %{pause_gate: gate, running: running} = Task.await(snapshot, 5_000)
    assert gate.transition_epoch == epoch
    assert gate.paused?
    assert running == []
  end

  test "pause acknowledgement waits for a successful managed child start" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)

    pause_root = Path.join(System.tmp_dir!(), "symphony-successful-barrier-#{System.unique_integer([:positive])}")
    File.mkdir_p!(pause_root)
    pause_path = Path.join(pause_root, "global-mutable-pause.state")
    transition_path = Path.join(pause_root, "global-mutable-pause.transition")
    File.write!(pause_path, "running\n")
    previous_pause_path = System.get_env("SYMPHONY_GLOBAL_PAUSE_FILE")
    System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", pause_path)

    on_exit(fn ->
      if is_binary(previous_pause_path), do: System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", previous_pause_path), else: System.delete_env("SYMPHONY_GLOBAL_PAUSE_FILE")
      File.rm_rf(pause_root)
    end)

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue"),
        do: {:ok, response(%{"data" => reservation_payload()})},
        else: {:ok, response(%{"data" => claim_result_payload()})}
    end

    assert {:ok, _claim} = WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end)

    previous_workflow_path = Application.get_env(:symphony_elixir, :workflow_file_path)
    previous_issues = Application.get_env(:symphony_elixir, :memory_tracker_issues)
    workflow_path = Path.join(pause_root, "WORKFLOW.md")

    SymphonyElixir.TestSupport.write_workflow_file!(workflow_path,
      tracker_kind: "memory",
      workspace_root: pause_root,
      poll_interval_ms: 60_000
    )

    SymphonyElixir.Workflow.set_workflow_file_path(workflow_path)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])

    on_exit(fn ->
      if is_binary(previous_workflow_path), do: SymphonyElixir.Workflow.set_workflow_file_path(previous_workflow_path), else: SymphonyElixir.Workflow.clear_workflow_file_path()
      if is_nil(previous_issues), do: Application.delete_env(:symphony_elixir, :memory_tracker_issues), else: Application.put_env(:symphony_elixir, :memory_tracker_issues, previous_issues)
    end)

    held_supervisor = start_supervised!({Task.Supervisor, max_children: 1})
    on_exit(fn -> if Process.alive?(held_supervisor), do: :sys.resume(held_supervisor) end)
    runtime = Map.take(input, [:base_url, :runner_token, :attestation_key, :runner_id, :managed_project_profile_id, :journal_path])
    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Successful barrier", state: "Todo"}
    name = Module.concat(__MODULE__, "SuccessfulBarrier#{System.unique_integer([:positive])}")
    {:ok, orchestrator} = Orchestrator.start_link(name: name)
    Process.unlink(orchestrator)
    on_exit(fn -> if Process.alive?(orchestrator), do: Process.exit(orchestrator, :kill) end)

    assert Enum.any?(1..200, fn _ ->
             case Orchestrator.snapshot(name, 1_000).startup_maintenance do
               %{status: "succeeded"} ->
                 true

               _ ->
                 Process.sleep(5)
                 false
             end
           end)

    assert :ok = :sys.suspend(held_supervisor)

    :sys.replace_state(orchestrator, fn state ->
      %{state | execution_fence: input.fence_state, responsibility_graph: input.responsibility_graph, work_package_runtime: runtime, task_supervisor: held_supervisor}
    end)

    parent = self()

    worker = fn ->
      send(parent, {:managed_child_started, self()})

      receive do
        :stop -> :ok
      end
    end

    dispatch =
      Task.async(fn ->
        :sys.replace_state(orchestrator, fn state ->
          send(parent, {:managed_start_result, Orchestrator.start_claimed_worker_for_test(state, issue, worker)})
          state
        end)
      end)

    assert Enum.any?(1..200, fn _ ->
             case Process.info(held_supervisor, :messages) do
               {:messages, messages} ->
                 if Enum.any?(messages, &match?({:"$gen_call", _, {:start_task, _, _, _}}, &1)),
                   do: true,
                   else:
                     (
                       Process.sleep(5)
                       false
                     )

               _ ->
                 false
             end
           end)

    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "spawn_started"

    epoch = String.duplicate("e", 32)
    external_pause_setter!(pause_path, transition_path, epoch)
    snapshot = Task.async(fn -> Orchestrator.snapshot(name, 10_000) end)

    assert Enum.any?(1..200, fn _ ->
             case Process.info(orchestrator, :messages) do
               {:messages, messages} ->
                 if Enum.any?(messages, &match?({:"$gen_call", _, :snapshot}, &1)),
                   do: true,
                   else:
                     (
                       Process.sleep(5)
                       false
                     )

               _ ->
                 false
             end
           end)

    assert Task.yield(snapshot, 0) == nil
    assert :ok = :sys.resume(held_supervisor)
    # The callback sends this result only after start_child returns a live pid;
    # task-body scheduling may occur later and is not the pause boundary.
    assert_receive {:managed_start_result, {:ok, child}}, 5_000
    assert_receive {:managed_child_started, ^child}, 5_000
    assert child in Task.Supervisor.children(held_supervisor)
    assert %Orchestrator.State{} = Task.await(dispatch, 5_000)

    assert %{pause_gate: gate} = Task.await(snapshot, 5_000)
    assert gate.transition_epoch == epoch
    assert gate.paused?

    assert {:error, :global_pause} =
             Orchestrator.start_claimed_worker_for_test(
               :sys.get_state(orchestrator),
               issue,
               fn -> send(parent, :late_child_started) end
             )

    send(child, :stop)
    refute_receive :late_child_started, 50
  end

  test "a global pause after confirmed managed claim blocks the prospective spawn" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input, token: token, lease: lease} = authority_fixture(path)

    previous_pause_path = System.get_env("SYMPHONY_GLOBAL_PAUSE_FILE")
    pause_root = Path.join(System.tmp_dir!(), "symphony-claim-pause-#{System.unique_integer([:positive])}")
    File.mkdir_p!(pause_root)
    pause_path = Path.join(pause_root, "global-mutable-pause.state")
    System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", pause_path)
    File.write!(pause_path, "running\n")

    on_exit(fn ->
      if is_binary(previous_pause_path), do: System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", previous_pause_path), else: System.delete_env("SYMPHONY_GLOBAL_PAUSE_FILE")
      File.rm_rf(pause_root)
    end)

    runtime = Map.take(input, [:base_url, :runner_token, :attestation_key, :runner_id, :managed_project_profile_id, :journal_path])
    state = %Orchestrator.State{execution_fence: input.fence_state, responsibility_graph: input.responsibility_graph, work_package_runtime: runtime}
    task_supervisor = start_supervised!({Task.Supervisor, name: Module.concat(__MODULE__, "ClaimPause#{System.unique_integer([:positive])}")})
    state = %{state | task_supervisor: task_supervisor}
    children_before = Task.Supervisor.children(task_supervisor)
    Process.put(:claim_pause_requests, 0)
    claim_time = ~U[2026-09-06 10:00:00.000Z]

    request_fun = fn url, _options ->
      Process.put(:claim_pause_requests, Process.get(:claim_pause_requests) + 1)

      if String.ends_with?(url, "/reservations/by-issue") do
        {:ok, response(%{"data" => reservation_payload()})}
      else
        File.write!(pause_path, "paused\n")
        {:ok, response(%{"data" => claim_result_payload()})}
      end
    end

    assert {:ok, _claim} = WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> claim_time end)
    assert Process.get(:claim_pause_requests) == 2
    assert SymphonyElixir.GlobalPause.paused?()
    assert {:ok, journal} = Journal.load(path)
    journal_bytes_before_pause = File.read!(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "confirmed"

    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Pause race", state: "Todo"}

    after_pause =
      Orchestrator.spawn_fenced_issue_for_test(
        state,
        issue,
        token,
        lease.session_id,
        "delegation-349",
        lease
      )

    assert Task.Supervisor.children(task_supervisor) == children_before
    assert after_pause.running == %{}

    assert Map.has_key?(after_pause.blocked, @issue_id),
           "confirmed managed claim was left active without explicit blocking or reconciliation"

    assert %{error: error, execution_token: ^token} = after_pause.blocked[@issue_id]
    assert String.contains?(error, ":global_pause")
    assert MapSet.member?(after_pause.claimed, @issue_id)

    assert after_pause.execution_fence == input.fence_state
    assert after_pause.responsibility_graph == input.responsibility_graph
    assert File.read!(path) == journal_bytes_before_pause
    assert {:ok, journal_after_pause} = Journal.load(path)
    [{_key, reservation_after_pause}] = Map.to_list(journal_after_pause.reservations)
    assert journal_after_pause == journal
    assert reservation_after_pause.dispatch.phase == "confirmed"

    # Simulate restart reconciliation of the retained authority snapshots.
    # It deliberately makes the live execution unknown and its delegations
    # blocked; the confirmed journal must remain held until provider recovery
    # is eligible, not be replayed or replaced immediately.
    assert {:ok, restarted_fence} = ExecutionFence.mark_unreconciled_after_restart(after_pause.execution_fence)
    assert {:ok, restarted_graph} = ResponsibilityGraph.mark_unreconciled_after_restart(after_pause.responsibility_graph)
    assert restarted_fence.executions[@issue_id].ownership == :unknown
    assert restarted_graph.delegations["delegation-349"].status == :blocked
    assert restarted_graph.delegations["delegation-349"].blocked_on == :restart_reconciliation

    claim_time_ms = DateTime.to_unix(claim_time, :millisecond)

    assert Recovery.held?(restarted_fence, @issue_id)
    assert {:ok, [retained_claim]} = Recovery.unstarted_claims(runtime, restarted_fence)
    assert retained_claim.dispatch.phase == "confirmed"

    assert {:error, :claim_recovery_backoff} =
             Recovery.prepare(runtime, restarted_fence, restarted_graph, issue, nil, claim_time_ms)

    assert restarted_fence.executions[@issue_id].ownership == :unknown
    assert restarted_graph.delegations["delegation-349"].blocked_on == :restart_reconciliation
    assert {:ok, journal_after_restart} = Journal.load(path)
    assert journal_after_restart == journal
    assert File.read!(path) == journal_bytes_before_pause
    assert Process.get(:claim_pause_requests) == 2
    assert Task.Supervisor.children(task_supervisor) == children_before
  end

  test "claims a reservation and replays the same journaled tuple after restart" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input, fence_state: fence, graph: graph} = authority_fixture(path)
    parent = self()

    first_request = fn url, options ->
      send(parent, {:request, url, options})

      if String.ends_with?(url, "/reservations/by-issue") do
        {:ok, response(%{"data" => reservation_payload()})}
      else
        {:error, :lost_response}
      end
    end

    assert {:error, {:claim_indeterminate, {:provider_request, :lost_response}}} = WorkPackageClaim.claim(input, request_fun: first_request, now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end)
    assert_receive {:request, reservation_url, reservation_options}
    assert String.ends_with?(reservation_url, "/reservations/by-issue")
    assert Keyword.get(reservation_options, :json) == %{issueId: @issue_id, managedProjectProfileId: @profile, repositoryRef: @repository}
    assert_receive {:request, claim_url, claim_options}
    assert String.ends_with?(claim_url, "/projection-349/claim")

    claim_payload = Keyword.fetch!(claim_options, :json).attestation
    assert claim_payload["generation"] == 1
    assert claim_payload["executionFenceToken"] == "#{@issue_id}:1"
    assert claim_payload["scopeKeys"] == ["repo:#{@repository}", "work:349"]

    if match?({:unix, _}, :os.type()) do
      assert Bitwise.band(File.stat!(path).mode, 0o777) == 0o600
    end

    restarted_input = %{input | fence_state: fence, responsibility_graph: graph}

    second_request = fn url, options ->
      send(parent, {:replay_request, url, options})
      {:ok, response(%{"data" => claim_result_payload()})}
    end

    assert {:ok, second} = WorkPackageClaim.claim(restarted_input, request_fun: second_request, now_fun: fn -> ~U[2026-09-06 10:01:00.000Z] end)
    assert second.attestation.reservation_nonce == "nonce-349"
    assert_receive {:replay_request, replay_url, _replay_options}
    refute String.ends_with?(replay_url, "/reservations/by-issue")
  end

  test "failed Codex turn evidence is durable, deduplicated, and scoped to its reservation" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)

    request = fn url, _options ->
      payload = if String.ends_with?(url, "/reservations/by-issue"), do: reservation_payload(), else: claim_result_payload()
      {:ok, response(%{"data" => payload})}
    end

    assert {:ok, _} = WorkPackageClaim.claim(input, request_fun: request)
    assert {:ok, journal} = Journal.load(path)
    [{key, _reservation}] = Map.to_list(journal.reservations)
    assert {:ok, 0} = Journal.failed_worker_turn_count(journal, @issue_id, @profile, @repository)

    evidence = %{
      thread_id: "thread-349",
      turn_id: "turn-349",
      observed_at_ms: 1_790_000_000_000,
      payload_sha256: String.duplicate("a", 64)
    }

    assert {:ok, once} = Journal.put_failed_worker_turn(journal, key, "thread-349:turn-349", evidence)

    assert {:ok, ^once} =
             Journal.put_failed_worker_turn(once, key, "thread-349:turn-349", %{
               evidence
               | observed_at_ms: evidence.observed_at_ms + 1
             })

    assert {:error, :failed_worker_turn_conflict} =
             Journal.put_failed_worker_turn(once, key, "thread-349:turn-349", %{evidence | payload_sha256: String.duplicate("b", 64)})

    assert :ok = Journal.save(path, once)
    assert {:ok, reloaded} = Journal.load(path)
    assert {:ok, 1} = Journal.failed_worker_turn_count(reloaded, @issue_id, @profile, @repository)
    assert {:ok, 0} = Journal.failed_worker_turn_count(reloaded, @issue_id, "other-profile", @repository)
    assert {:ok, 0} = Journal.failed_worker_turn_count(reloaded, "other-issue", @profile, @repository)

    runtime = %{journal_path: path, managed_project_profile_id: @profile, repository_ref: @repository}

    assert {:ok, %{model: "gpt-6-luna", effort: "xhigh"}} =
             ModelRouter.resolve_managed_from_journal(%Issue{id: @issue_id, labels: []}, runtime)

    File.rm!(path)

    assert {:error, :managed_journal_missing} =
             ModelRouter.resolve_managed_from_journal(%Issue{id: @issue_id, labels: []}, runtime)

    assert {:ok, %{model: "gpt-6-luna", effort: "high"}} =
             ModelRouter.resolve_managed_from_journal(%Issue{id: @issue_id, labels: []}, Map.put(runtime, :allow_missing_initial, true))

    assert {:error, :invalid_failed_worker_turn} =
             Journal.put_failed_worker_turn(journal, key, "other-turn", evidence)
  end

  test "fails closed for expired authority and malformed provider reservation" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)

    reservation_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue") do
        {:ok, response(%{"data" => %{"projectionId" => "p", "reservationId" => "r"}})}
      else
        {:ok, response(%{"data" => %{}})}
      end
    end

    assert {:error, {:missing_reservation_field, "reservationNonce"}} =
             WorkPackageClaim.claim(input, request_fun: reservation_fun)

    expired_graph = put_in(input.responsibility_graph, [:delegations, "delegation-349", :expires_at_ms], 1)

    assert {:error, :runtime_lease_mismatch} =
             WorkPackageClaim.claim(
               %{input | responsibility_graph: expired_graph},
               request_fun: reservation_fun
             )

    assert {:ok, canonical} =
             WorkPackageClaim.canonical_json(attestation_for_test())

    assert canonical == @canonical_json_fixture

    assert {:ok, signature} = WorkPackageClaim.sign(attestation_for_test(), "attestation-key")
    assert signature == "eYASgI7yqAih8J9N1Noj0r8Ap8X3H3tVgi__MnwwQkg"
  end

  test "managed authority binds the exact work-package projection on first claim and replay" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)

    graph =
      update_in(input.responsibility_graph, [:delegations], fn delegations ->
        Map.new(delegations, fn {id, delegation} ->
          {id, put_in(delegation, [:scope, :work_package_id], "projection-349")}
        end)
      end)

    managed = input |> Map.put(:managed_delegations, %{}) |> Map.put(:responsibility_graph, graph)

    wrong_projection = fn url, _options ->
      assert String.ends_with?(url, "/reservations/by-issue")
      {:ok, response(%{"data" => Map.put(reservation_payload(), "projectionId", "wrong-projection")})}
    end

    assert {:error, :reservation_scope_mismatch} = WorkPackageClaim.claim(managed, request_fun: wrong_projection)
    refute File.exists?(path)

    valid_projection = fn url, _options ->
      payload = if String.ends_with?(url, "/reservations/by-issue"), do: reservation_payload(), else: claim_result_payload()
      {:ok, response(%{"data" => payload})}
    end

    assert {:ok, _} = WorkPackageClaim.claim(managed, request_fun: valid_projection)
    assert {:ok, journal} = Journal.load(path)
    [{key, reservation}] = Map.to_list(journal.reservations)
    {:ok, changed} = Journal.put(journal, key, %{reservation | projection_id: "wrong-projection"})
    assert :ok = Journal.save(path, changed)

    assert {:error, :reservation_authority_mismatch} =
             WorkPackageClaim.claim(managed, request_fun: fn _url, _options -> flunk("a mismatched replay must not send a request") end)
  end

  test "rejects corrupt journals, profile mismatches, malformed claims, and HTTP errors" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)

    File.write!(path, "{}")
    assert {:error, {:invalid_journal, _reason}} = WorkPackageClaim.claim(input)
    File.rm!(path)

    wrong_profile = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue") do
        {:ok, response(%{"data" => %{reservation_payload() | "managedProjectProfileId" => "profile-other"}})}
      else
        {:ok, response(%{"data" => claim_result_payload()})}
      end
    end

    assert {:error, :reservation_scope_mismatch} =
             WorkPackageClaim.claim(input, request_fun: wrong_profile)

    malformed_claim = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue") do
        {:ok, response(%{"data" => reservation_payload()})}
      else
        {:ok, response(%{"data" => %{}})}
      end
    end

    assert {:error, {:claim_indeterminate, :invalid_claim_result}} =
             WorkPackageClaim.claim(input, request_fun: malformed_claim)

    provider_error = fn _url, _options -> {:ok, response(%{"error" => "unavailable"}, 503)} end

    assert {:error, {:claim_indeterminate, {:provider_status, 503}}} =
             WorkPackageClaim.claim(input, request_fun: provider_error, now_fun: fn -> DateTime.add(DateTime.utc_now(), 60, :second) end)

    assert {:ok, journal} = Journal.load(path)
    assert map_size(journal.reservations) == 1
    [{key, reservation}] = Map.to_list(journal.reservations)
    {:ok, corrupted_journal} = Journal.put(journal, key, %{reservation | generation: 2})
    assert :ok = Journal.save(path, corrupted_journal)

    assert {:error, :reservation_authority_mismatch} =
             WorkPackageClaim.claim(input, request_fun: provider_error)
  end

  test "transient HTTP failures preserve the claim while authority conflicts block replay" do
    for status <- [408, 425, 429, 409] do
      path = temp_path()
      on_exit(fn -> File.rm_rf(path) end)
      %{input: input} = authority_fixture(path)
      now = ~U[2026-09-06 10:00:00Z]

      request = fn url, _options ->
        if String.ends_with?(url, "/reservations/by-issue"),
          do: {:ok, response(%{"data" => reservation_payload()})},
          else: {:ok, response(%{"error" => "retained fixture response"}, status)}
      end

      result = WorkPackageClaim.claim(input, request_fun: request, now_fun: fn -> now end)
      kind = if status == 409, do: :claim_blocked, else: :claim_indeterminate
      assert result == {:error, {kind, {:provider_status, status}}}
      assert {:ok, journal} = Journal.load(path)
      [reservation] = Map.values(journal.reservations)
      assert reservation.dispatch.phase == if(status == 409, do: "blocked", else: "submitted")

      if status != 409 do
        assert {:error, :claim_recovery_backoff} =
                 WorkPackageClaim.claim(input, request_fun: fn _, _ -> flunk("early retry sent HTTP") end, now_fun: fn -> now end)

        assert {:ok, replay} =
                 WorkPackageClaim.claim(input,
                   request_fun: fn url, options ->
                     refute String.ends_with?(url, "/reservations/by-issue")
                     assert Keyword.fetch!(options, :json).attestation["reservationNonce"] == reservation.reservation_nonce
                     {:ok, response(%{"data" => claim_result_payload()})}
                   end,
                   now_fun: fn -> DateTime.add(now, 5, :second) end
                 )

        assert replay.reservation.generation == reservation.generation
      end
    end
  end

  defp authority_fixture(path) do
    fence = ExecutionFence.new()

    {:ok, admitted, token} =
      ExecutionFence.admit(fence, %{issue_id: @issue_id, repository: @repository, branch: "hgs-349", worktree: "tmp"}, 0)

    {:ok, fence_state, :registered} =
      ExecutionFence.register(
        admitted,
        token,
        :worker,
        %{
          session_id: "worker-349",
          process_id: "process-349",
          branch: "hgs-349",
          worktree: "tmp",
          linear_state: "In Progress",
          pr_state: "none",
          head: "unobserved",
          last_heartbeat_at: 0
        },
        0
      )

    lease = %{
      issue_id: @issue_id,
      repository: @repository,
      generation: 1,
      session_id: "worker-349",
      process_id: "process-349"
    }

    scope = %{
      company_id: "hypergrid",
      objective_id: "objective",
      initiative_id: "initiative",
      project_id: "project",
      work_package_id: "package",
      issue_id: @issue_id,
      repository: @repository,
      paths: [],
      modules: [],
      environments: ["local"],
      actions: [
        :read,
        :observe,
        :delegate,
        :reconcile,
        :edit,
        :commit,
        :push,
        :state_mutation,
        :cleanup,
        :review,
        :report
      ]
    }

    authority = %{
      class: :routine_engineering,
      capabilities: scope.actions,
      environments: ["local"]
    }

    budget = %{model: "luna", effort: :high, max_tokens: 1000, max_children: 1}

    {:ok, owner_graph, _} =
      ResponsibilityGraph.delegate(
        ResponsibilityGraph.new(),
        delegation("owner", :accountable, scope, authority, budget),
        0
      )

    {:ok, child_graph, _} =
      ResponsibilityGraph.delegate(
        owner_graph,
        delegation(
          "delegation-349",
          :responsible,
          scope,
          authority,
          budget,
          parent_delegation_id: "owner"
        ),
        0
      )

    {:ok, graph} = ResponsibilityGraph.bind_runtime_lease(child_graph, "delegation-349", lease, 0)

    input = %{
      base_url: "http://provider.test",
      runner_token: "runner-token",
      attestation_key: "attestation-key",
      runner_id: "runner-349",
      managed_project_profile_id: @profile,
      issue_id: @issue_id,
      issue_identifier: "HGS-349",
      repository_ref: @repository,
      fence_state: fence_state,
      responsibility_graph: graph,
      journal_path: path
    }

    %{input: input, fence_state: fence_state, graph: graph, token: token, lease: lease}
  end

  defp delegation(id, role, scope, authority, budget, extras \\ []) do
    Map.merge(
      %{
        id: id,
        parent_delegation_id: nil,
        role: role,
        actor_id: id,
        scope: scope,
        authority: authority,
        budget: budget,
        runtime_lease: nil,
        expires_at_ms: 2_000_000_000_000,
        expected_deliverable: "adapter",
        expected_evidence: "tests",
        return_to_parent: %{owner_id: "owner", contract: "evidence"}
      },
      Map.new(extras)
    )
  end

  defp reservation_payload do
    %{
      "projectionId" => "projection-349",
      "reservationId" => "reservation-349",
      "reservationNonce" => "nonce-349",
      "issueId" => @issue_id,
      "managedProjectProfileId" => @profile,
      "repositoryRef" => @repository,
      "scopeKeys" => ["work:349", "repo:#{@repository}"]
    }
  end

  defp claim_result_payload do
    %{
      "projectionId" => "projection-349",
      "projectionState" => "active",
      "mutationState" => "applied",
      "claimEvidence" => %{
        "responsibleDelegationId" => "delegation-349",
        "executionFenceToken" => "#{@issue_id}:1",
        "runtimeLeaseId" => "worker-349"
      }
    }
  end

  defp attestation_for_test do
    %{
      contract_version: "work-package-runtime-attestation.v1",
      runner_id: "runner-349",
      managed_project_profile_id: @profile,
      reservation_id: "reservation-349",
      reservation_nonce: "nonce-349",
      issue_id: @issue_id,
      generation: 1,
      session_id: "worker-349",
      process_id: "process-349",
      responsible_delegation_id: "delegation-349",
      execution_fence_token: "#{@issue_id}:1",
      runtime_lease_id: "worker-349",
      repository_ref: @repository,
      scope_keys: ["work:349", "repo:#{@repository}"],
      attested_at: "2026-09-06T10:00:00.000Z"
    }
  end

  defp response(body, status \\ 200), do: %Req.Response{status: status, body: body}

  defp external_pause_setter!(pause_path, transition_path, epoch) do
    erlang = System.find_executable("erl")
    assert is_binary(erlang), "erl executable is required for the cross-process pause fixture"

    code = """
    PausePath = os:getenv("PAUSE_PATH"),
    TransitionPath = os:getenv("TRANSITION_PATH"),
    Epoch = os:getenv("PAUSE_EPOCH"),
    TemporaryPath = PausePath ++ ".tmp-" ++ Epoch,
    BackupPath = PausePath ++ ".previous-" ++ Epoch,
    ok = file:write_file(TransitionPath, ["pausing:", Epoch, "\\n"], [sync]),
    ok = file:write_file(TemporaryPath, <<"paused\\n">>, [sync]),
    case file:rename(TemporaryPath, PausePath) of
      ok -> ok;
      {error, eexist} ->
        ok = file:rename(PausePath, BackupPath),
        ok = file:rename(TemporaryPath, PausePath),
        ok = file:delete(BackupPath);
      {error, Reason} -> erlang:error({atomic_gate_replace_failed, Reason})
    end,
    halt().
    """

    assert {"", 0} =
             System.cmd(erlang, ["-noshell", "-eval", code],
               env: [
                 {"PAUSE_PATH", pause_path},
                 {"TRANSITION_PATH", transition_path},
                 {"PAUSE_EPOCH", epoch}
               ],
               stderr_to_stdout: true
             )
  end

  defp temp_path, do: Path.join(System.tmp_dir!(), "symphony-work-package-#{System.unique_integer([:positive])}.json")

  defp forward_trace(parent) do
    receive do
      :stop ->
        send(parent, :trace_complete)

      {:trace, _pid, :call, _call} = event ->
        send(parent, event)
        forward_trace(parent)
    end
  end
end
