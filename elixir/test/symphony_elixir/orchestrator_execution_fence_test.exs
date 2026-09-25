defmodule SymphonyElixir.OrchestratorExecutionFenceTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{ExecutionFence, ExecutionSupervisor, Orchestrator, WorkPackageCleanupReceipt}
  alias SymphonyElixir.ExecutionFence.Persistence
  alias SymphonyElixir.WorkPackageClaim.Journal

  test "managed failed-turn evidence requires the live worker, lease, and claimed reservation" do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", max_concurrent_agents: 1)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])

    issue_id = "HGS-failed-turn-evidence"
    repository = "openai/symphony"
    profile = "profile-failed-turn"
    journal_path = Path.join(System.tmp_dir!(), "symphony-failed-turn-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(journal_path) end)
    admission = %{issue_id: issue_id, repository: repository, branch: "codex/failed-turn", worktree: "/tmp/failed-turn"}
    {:ok, fence, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)

    session =
      Map.merge(admission, %{
        generation: token.generation,
        role: :worker,
        session_id: "worker-failed-turn",
        process_id: "process-failed-turn",
        linear_state: "In Progress",
        pr_state: "OPEN",
        head: "unobserved",
        last_heartbeat_at: 100
      })

    {:ok, fence, :registered} = ExecutionFence.register(fence, token, :worker, session, 100)
    key = Journal.reservation_key(issue_id, profile, repository, token.generation)

    reservation = %{
      issue_id: issue_id,
      managed_project_profile_id: profile,
      repository_ref: repository,
      projection_id: "projection-failed-turn",
      reservation_id: "reservation-failed-turn",
      reservation_nonce: "nonce-failed-turn",
      scope_keys: ["repo:#{repository}"],
      runner_id: "runner-failed-turn",
      generation: token.generation,
      session_id: session.session_id,
      process_id: session.process_id,
      responsible_delegation_id: "delegation-failed-turn",
      execution_fence_token: "#{issue_id}:#{token.generation}",
      runtime_lease_id: session.session_id
    }

    {:ok, journal} = Journal.put(Journal.new(), key, reservation)
    :ok = Journal.save(journal_path, journal)
    name = Module.concat(__MODULE__, "FailedTurn#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name})

    entry = %{
      pid: self(),
      execution_token: token,
      execution_session_id: session.session_id,
      responsibility_delegation_id: reservation.responsible_delegation_id,
      codex_session_identity: %{thread_id: "thread-failed-turn", turn_id: "turn-failed-turn"}
    }

    runtime = %{
      journal_path: journal_path,
      managed_project_profile_id: profile,
      managed_delegations: %{repository_ref: repository}
    }

    :sys.replace_state(pid, &%{
      &1
      | execution_fence: fence,
        running: %{issue_id => entry},
        work_package_runtime: runtime
    })

    update = %{
      event: :turn_failed,
      execution_token: token,
      execution_session_id: session.session_id,
      payload: %{
        "method" => "turn/failed",
        "params" => %{"threadId" => "thread-failed-turn", "turn" => %{"id" => "turn-failed-turn"}}
      }
    }

    assert :ok = GenServer.call(pid, {:managed_failed_turn, issue_id, update})
    assert {:ok, persisted} = Journal.load(journal_path)
    assert {:ok, 1} = Journal.failed_worker_turn_count(persisted, issue_id, profile, repository)
    bytes = File.read!(journal_path)
    assert :ok = GenServer.call(pid, {:managed_failed_turn, issue_id, update})
    assert File.read!(journal_path) == bytes

    for params <- [
          %{"threadId" => "stale-thread", "turn" => %{"id" => "turn-failed-turn"}},
          %{"threadId" => "thread-failed-turn", "turn" => %{"id" => "stale-turn"}},
          %{"turnId" => "turn-failed-turn"}
        ] do
      assert {:error, :managed_failed_turn_identity_mismatch} =
               GenServer.call(pid, {
                 :managed_failed_turn,
                 issue_id,
                 put_in(update, [:payload, "params"], params)
               })

      assert File.read!(journal_path) == bytes
    end

    assert {:error, :managed_failed_turn_identity_mismatch} =
             GenServer.call(pid, {:managed_failed_turn, issue_id, %{update | execution_session_id: "stale"}})

    wrong_sender = Task.async(fn -> GenServer.call(pid, {:managed_failed_turn, issue_id, update}) end)
    assert {:error, :managed_failed_turn_identity_mismatch} = Task.await(wrong_sender)

    {:ok, released_fence, :released} = ExecutionFence.release(fence, token, session.session_id)
    :sys.replace_state(pid, &%{&1 | execution_fence: released_fence})
    assert {:error, :managed_failed_turn_identity_mismatch} =
             GenServer.call(pid, {:managed_failed_turn, issue_id, update})
    assert File.read!(journal_path) == bytes
  end

  test "termination confirmation through the server persists only valid generation-bound evidence" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      max_concurrent_agents: 1
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    fence_path = Path.join(System.tmp_dir!(), "symphony-termination-api-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(fence_path) end)
    name = Module.concat(__MODULE__, "TerminationApi#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name})

    {:ok, fence, token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)
    {:ok, fence, :registered} = ExecutionFence.register(fence, token, :worker, session(), 100)
    {:ok, fence, :released} = ExecutionFence.release(fence, token, "worker-1", :orchestrator_stop)
    :sys.replace_state(pid, &%{&1 | execution_fence: fence, execution_fence_path: fence_path})

    evidence = %{
      session_id: "worker-1",
      process_id: "logical-process-1",
      process_tree: :terminated,
      evidence_ref: "termination-api-test",
      observed_at_ms: 110
    }

    assert {:ok, :confirmed} =
             Orchestrator.confirm_execution_termination(pid, token, "worker-1", evidence, 110)

    assert {:ok, persisted} = Persistence.load(fence_path)
    execution = persisted.executions["HGS-294"]
    assert execution.leases["worker-1"].termination_confirmed_at_ms == 110
    assert execution.ownership == :reconciled
    assert execution.termination_unconfirmed == false
    live_fence = :sys.get_state(pid).execution_fence
    assert live_fence.executions["HGS-294"].leases["worker-1"].termination_confirmed_at_ms == 110
    persisted_bytes = File.read!(fence_path)

    assert {:ok, :already_confirmed} =
             Orchestrator.confirm_execution_termination(pid, token, "worker-1", evidence, 111)

    assert File.read!(fence_path) == persisted_bytes

    for {rejected_token, rejected_evidence} <- [
          {token, %{evidence | process_id: "wrong-process"}},
          {%{token | generation: token.generation + 1}, evidence}
        ] do
      assert {:error, _reason} =
               Orchestrator.confirm_execution_termination(pid, rejected_token, "worker-1", rejected_evidence, 112)

      assert File.read!(fence_path) == persisted_bytes
      assert :sys.get_state(pid).execution_fence == live_fence
    end
  end

  test "an ordinary poll retries a failed cleanup receipt and permits the next generation" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      max_concurrent_agents: 1
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    on_exit(fn -> Application.delete_env(:symphony_elixir, :memory_tracker_issues) end)

    issue_id = "HGS-350-retry"
    repository = "openai/symphony"
    profile = "profile-350-retry"
    journal_path = Path.join(System.tmp_dir!(), "symphony-cleanup-retry-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(journal_path) end)

    admission = %{
      issue_id: issue_id,
      repository: repository,
      branch: "codex/hgs-350-retry",
      worktree: Path.join(System.tmp_dir!(), "symphony-hgs-350-retry")
    }

    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)

    session =
      Map.merge(admission, %{
        generation: 1,
        role: :worker,
        session_id: "worker-350-retry",
        process_id: "process-350-retry",
        linear_state: "In Progress",
        pr_state: "OPEN",
        head: "abc123",
        last_heartbeat_at: 100
      })

    {:ok, fence_state, :registered} = ExecutionFence.register(fence_state, token, :worker, session, 100)
    {:ok, fence_state, :released} = ExecutionFence.release(fence_state, token, session.session_id, :orchestrator_stop)

    evidence = %{
      session_id: session.session_id,
      process_id: session.process_id,
      process_tree: :terminated,
      evidence_ref: "process-tree-check-350-retry",
      observed_at_ms: 110
    }

    {:ok, fence_state, :confirmed} =
      ExecutionFence.confirm_termination(fence_state, token, session.session_id, evidence, 110)

    {:ok, fence_state, :fenced} =
      ExecutionFence.fence(fence_state, token, %{terminal_state: "Done", accepted_head: "abc123"}, 120)

    {:ok, fence_state, :prepared} = ExecutionFence.prepare_cleanup(fence_state, token, "abc123", 121)

    {:ok, fence_state} =
      ExecutionFence.record_cleanup_evidence(fence_state, token, "abc123", "sha256:cleanup-350-retry", 122)

    {:ok, fence_state, :cleaned} = ExecutionFence.cleanup(fence_state, token, "abc123", 123)

    reservation_key = Journal.reservation_key(issue_id, profile, repository, 1)

    reservation = %{
      issue_id: issue_id,
      managed_project_profile_id: profile,
      repository_ref: repository,
      projection_id: "projection-350-retry",
      reservation_id: "reservation-350-retry",
      reservation_nonce: "nonce-350-retry",
      scope_keys: ["repo:#{repository}", "work:350-retry"],
      runner_id: "runner-350-retry",
      generation: 1,
      session_id: session.session_id,
      process_id: session.process_id,
      responsible_delegation_id: "delegation-350-retry",
      execution_fence_token: "#{issue_id}:1",
      runtime_lease_id: session.session_id
    }

    {:ok, journal} = Journal.put(Journal.new(), reservation_key, reservation)
    assert :ok = Journal.save(journal_path, journal)

    input = %{
      base_url: "http://provider.test",
      runner_token: "runner-token",
      attestation_key: "attestation-key",
      runner_id: reservation.runner_id,
      managed_project_profile_id: profile,
      issue_id: issue_id,
      repository_ref: repository,
      fence_state: fence_state,
      journal_path: journal_path
    }

    termination_request = fn _url, options ->
      payload = Keyword.fetch!(options, :json)

      {:ok,
       provider_response(%{
         "projectionId" => reservation.projection_id,
         "reservationId" => reservation.reservation_id,
         "receiptId" => payload["receiptId"],
         "receiptKind" => "termination_confirmed",
         "executionCapacityState" => "released",
         "scopeState" => "held",
         "reservationState" => "claimed",
         "generation" => 1,
         "evidenceRef" => payload["evidenceRef"],
         "acceptedHead" => payload["acceptedHead"],
         "replayed" => false
       })}
    end

    assert {:ok, _termination_result} =
             WorkPackageCleanupReceipt.termination_confirmed(
               input,
               %{terminal_outcome: :completed, accepted_head: "abc123"},
               request_fun: termination_request,
               now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end
             )

    Process.put(:repository_receipt_attempts, 0)

    repository_request = fn _url, options ->
      attempt = Process.get(:repository_receipt_attempts) + 1
      Process.put(:repository_receipt_attempts, attempt)

      if attempt == 1 do
        {:error, :provider_temporarily_unavailable}
      else
        payload = Keyword.fetch!(options, :json)

        {:ok,
         provider_response(%{
           "projectionId" => reservation.projection_id,
           "reservationId" => reservation.reservation_id,
           "receiptId" => payload["receiptId"],
           "receiptKind" => "repository_cleanup_verified",
           "executionCapacityState" => "released",
           "scopeState" => "released",
           "reservationState" => "released",
           "generation" => 1,
           "evidenceRef" => payload["evidenceRef"],
           "acceptedHead" => payload["acceptedHead"],
           "replayed" => false
         })}
      end
    end

    runtime = %{
      base_url: input.base_url,
      runner_token: input.runner_token,
      attestation_key: input.attestation_key,
      runner_id: input.runner_id,
      managed_project_profile_id: input.managed_project_profile_id,
      journal_path: journal_path,
      request_fun: repository_request,
      now_fun: fn -> ~U[2026-09-06 10:01:00.000Z] end,
      cleanup_evidence_fun: fn _state, _token, _head -> {:ok, "sha256:cleanup-350-retry"} end
    }

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 0,
      execution_fence: fence_state,
      work_package_runtime: runtime,
      running: %{},
      blocked: %{},
      claimed: MapSet.new()
    }

    assert {:noreply, after_failed_poll} = Orchestrator.handle_info(:run_poll_cycle, state)
    assert Process.get(:repository_receipt_attempts) == 1
    assert {:ok, journal_after_failure} = Journal.load(journal_path)
    assert :missing = Journal.cleanup_receipt_ack(journal_after_failure, reservation_key, "repository_cleanup_verified")

    assert {:noreply, after_successful_poll} = Orchestrator.handle_info(:run_poll_cycle, after_failed_poll)
    assert Process.get(:repository_receipt_attempts) == 2
    assert {:ok, journal_after_success} = Journal.load(journal_path)

    assert {:ok, acknowledgement} =
             Journal.cleanup_receipt_ack(
               journal_after_success,
               reservation_key,
               "repository_cleanup_verified"
             )

    assert acknowledgement.scope_state == "released"
    assert acknowledgement.reservation_state == "released"
    assert {:ok, _next_fence, next_token} = ExecutionFence.admit(after_successful_poll.execution_fence, admission, 130)
    assert next_token.generation == 2
  end

  test "ordinary replay filters acknowledged history before applying its batch limit" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      max_concurrent_agents: 1
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])

    journal_path =
      Path.join(System.tmp_dir!(), "symphony-cleanup-starvation-#{System.unique_integer([:positive])}.json")

    on_exit(fn -> File.rm(journal_path) end)

    {fence_state, executions} =
      Enum.reduce(0..9, {ExecutionFence.new(), []}, fn index, {fence, acc} ->
        {next_fence, details} = replay_cleaned_execution(fence, index)
        {next_fence, [details | acc]}
      end)

    profile = "profile-350-starvation"

    journal =
      Enum.reduce(executions, Journal.new(), fn details, current_journal ->
        reservation = replay_reservation(details, profile)
        key = Journal.reservation_key(details.issue_id, profile, details.repository, 1)

        {:ok, current_journal} = Journal.put(current_journal, key, reservation)

        {:ok, current_journal} =
          Journal.put_cleanup_receipt(
            current_journal,
            key,
            "termination_confirmed",
            %{
              receipt_id: "termination-#{details.index}",
              receipt_kind: "termination_confirmed",
              generation: 1,
              evidence_ref: details.evidence_ref,
              accepted_head: details.head
            }
          )

        termination_ack = %{
          projection_id: reservation.projection_id,
          reservation_id: reservation.reservation_id,
          receipt_id: "termination-#{details.index}",
          receipt_kind: "termination_confirmed",
          execution_capacity_state: "released",
          scope_state: "released",
          reservation_state: "released",
          generation: 1,
          evidence_ref: details.evidence_ref,
          accepted_head: details.head,
          replayed: false
        }

        {:ok, current_journal} =
          Journal.put_cleanup_receipt_ack(
            current_journal,
            key,
            "termination_confirmed",
            termination_ack
          )

        if details.index < 9 do
          {:ok, current_journal} =
            Journal.put_cleanup_receipt(
              current_journal,
              key,
              "repository_cleanup_verified",
              %{
                receipt_id: "repository-#{details.index}",
                receipt_kind: "repository_cleanup_verified",
                generation: 1,
                evidence_ref: "sha256:cleanup-#{details.index}",
                accepted_head: details.head
              }
            )

          repository_ack = %{
            projection_id: reservation.projection_id,
            reservation_id: reservation.reservation_id,
            receipt_id: "repository-#{details.index}",
            receipt_kind: "repository_cleanup_verified",
            execution_capacity_state: "released",
            scope_state: "released",
            reservation_state: "released",
            generation: 1,
            evidence_ref: "sha256:cleanup-#{details.index}",
            accepted_head: details.head,
            replayed: false
          }

          {:ok, current_journal} =
            Journal.put_cleanup_receipt_ack(
              current_journal,
              key,
              "repository_cleanup_verified",
              repository_ack
            )

          current_journal
        else
          current_journal
        end
      end)

    assert :ok = Journal.save(journal_path, journal)
    Process.put(:starvation_receipt_calls, 0)

    request_fun = fn _url, options ->
      Process.put(:starvation_receipt_calls, Process.get(:starvation_receipt_calls) + 1)
      payload = Keyword.fetch!(options, :json)
      assert payload["receiptKind"] == "repository_cleanup_verified"

      {:ok,
       provider_response(%{
         "projectionId" => "projection-9",
         "reservationId" => "reservation-9",
         "receiptId" => payload["receiptId"],
         "receiptKind" => "repository_cleanup_verified",
         "executionCapacityState" => "released",
         "scopeState" => "released",
         "reservationState" => "released",
         "generation" => 1,
         "evidenceRef" => payload["evidenceRef"],
         "acceptedHead" => payload["acceptedHead"],
         "replayed" => false
       })}
    end

    runtime = %{
      base_url: "http://provider.test",
      runner_token: "runner-token",
      attestation_key: "attestation-key",
      runner_id: "runner-starvation",
      managed_project_profile_id: profile,
      journal_path: journal_path,
      request_fun: request_fun,
      now_fun: fn -> ~U[2026-09-06 10:10:00.000Z] end,
      cleanup_evidence_fun: fn _state, _token, _head -> {:ok, "sha256:cleanup-9"} end
    }

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 0,
      execution_fence: fence_state,
      work_package_runtime: runtime,
      running: %{},
      blocked: %{},
      claimed: MapSet.new()
    }

    assert {:noreply, _next_state} = Orchestrator.handle_info(:run_poll_cycle, state)
    assert Process.get(:starvation_receipt_calls) == 1
  end

  test "ordinary replay quarantines missing cleanup evidence instead of hot polling" do
    write_workflow_file!(Workflow.workflow_file_path(),
      tracker_kind: "memory",
      max_concurrent_agents: 1
    )

    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    on_exit(fn -> Application.delete_env(:symphony_elixir, :memory_tracker_issues) end)

    journal_path =
      Path.join(System.tmp_dir!(), "symphony-cleanup-quarantine-#{System.unique_integer([:positive])}.json")

    on_exit(fn -> File.rm(journal_path) end)

    {fence_state, details} = replay_cleaned_execution(ExecutionFence.new(), 99)
    profile = "profile-350-quarantine"
    reservation = replay_reservation(details, profile)
    key = Journal.reservation_key(details.issue_id, profile, details.repository, 1)
    {:ok, journal} = Journal.put(Journal.new(), key, reservation)

    {:ok, journal} =
      Journal.put_cleanup_receipt(journal, key, "termination_confirmed", %{
        receipt_id: "termination-quarantine",
        receipt_kind: "termination_confirmed",
        generation: 1,
        evidence_ref: details.evidence_ref,
        accepted_head: details.head
      })

    {:ok, journal} =
      Journal.put_cleanup_receipt_ack(journal, key, "termination_confirmed", %{
        projection_id: reservation.projection_id,
        reservation_id: reservation.reservation_id,
        receipt_id: "termination-quarantine",
        receipt_kind: "termination_confirmed",
        execution_capacity_state: "released",
        scope_state: "held",
        reservation_state: "claimed",
        generation: 1,
        evidence_ref: details.evidence_ref,
        accepted_head: details.head,
        replayed: false
      })

    assert :ok = Journal.save(journal_path, journal)
    Process.put(:cleanup_evidence_attempts, 0)

    runtime = %{
      base_url: "http://provider.test",
      runner_token: "runner-token",
      attestation_key: "attestation-key",
      runner_id: reservation.runner_id,
      managed_project_profile_id: profile,
      journal_path: journal_path,
      request_fun: fn _url, _options -> flunk("provider must not be called without cleanup evidence") end,
      cleanup_evidence_fun: fn _state, _token, _head ->
        Process.put(:cleanup_evidence_attempts, Process.get(:cleanup_evidence_attempts, 0) + 1)
        {:error, {:cleanup_manifest_unreadable, :enoent}}
      end
    }

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 0,
      execution_fence: fence_state,
      work_package_runtime: runtime
    }

    assert {:noreply, after_first_poll} = Orchestrator.handle_info(:run_poll_cycle, state)
    assert Process.get(:cleanup_evidence_attempts) == 1

    assert {:noreply, _after_second_poll} = Orchestrator.handle_info(:run_poll_cycle, after_first_poll)
    assert Process.get(:cleanup_evidence_attempts) == 1
  end

  test "restart reconciliation stops and confirms persisted supervisor ownership" do
    admission = admission()
    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)
    {:ok, fence_state, :registered} = ExecutionFence.register(fence_state, token, :worker, session(), 100)

    identity =
      ExecutionSupervisor.identity("HGS-294", 1, "worker-1", "logical-process-1", 100)
      |> Map.merge(%{control_group: "/user.slice/symphony.scope", launch_processes: [111], main_pid: 111})

    {:ok, fence_state} = ExecutionFence.record_supervisor(fence_state, token, "worker-1", identity)

    runner = fn _executable, args, _opts ->
      case args do
        ["--user", "show", "--property=LoadState,ActiveState,ControlGroup,MainPID", _unit] ->
          {"LoadState=loaded\nActiveState=active\nControlGroup=/user.slice/symphony.scope\nMainPID=111\n", 0}

        ["--user", "stop", _unit] ->
          {"", 0}

        ["--user", "show", "--property=ActiveState", _unit] ->
          {"ActiveState=inactive\n", 0}

        ["--user", "show", "--property=ControlGroup", _unit] ->
          {"ControlGroup=/user.slice/symphony.scope\n", 0}
      end
    end

    cgroup_reader = fn _path ->
      case Process.get(:restart_cgroup_reads, 0) do
        0 ->
          Process.put(:restart_cgroup_reads, 1)
          {:ok, [111]}

        _ ->
          {:ok, []}
      end
    end

    {:ok, reconciled} =
      Orchestrator.reconcile_persisted_supervisors_for_test(
        fence_state,
        command_runner: runner,
        cgroup_reader: cgroup_reader,
        now_ms: 200
      )

    lease = reconciled.executions["HGS-294"].leases["worker-1"]
    assert lease.status == :released
    assert lease.termination_confirmed_at_ms == 200
    assert reconciled.executions["HGS-294"].termination_unconfirmed == false
    assert reconciled.executions["HGS-294"].ownership == :reconciled
    assert {:ok, _next_state, next_token} = ExecutionFence.admit(reconciled, admission, 210)
    assert next_token.generation == 2
  end

  test "restart reconciliation accepts termination evidence observed after its initial clock snapshot" do
    admission = admission()
    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)
    {:ok, fence_state, :registered} = ExecutionFence.register(fence_state, token, :worker, session(), 100)

    identity =
      ExecutionSupervisor.identity("HGS-294", 1, "worker-1", "logical-process-1", 100)
      |> Map.merge(%{control_group: "/user.slice/symphony.scope", launch_processes: [111], main_pid: 111})

    {:ok, fence_state} = ExecutionFence.record_supervisor(fence_state, token, "worker-1", identity)

    runner = fn _executable, args, _opts ->
      case args do
        ["--user", "show", "--property=LoadState,ActiveState,ControlGroup,MainPID", _unit] ->
          {"LoadState=loaded\nActiveState=inactive\nControlGroup=/user.slice/symphony.scope\nMainPID=0\n", 0}

        ["--user", "show", "--property=ControlGroup", _unit] ->
          {"ControlGroup=/user.slice/symphony.scope\n", 0}
      end
    end

    {:ok, reconciled} =
      Orchestrator.reconcile_persisted_supervisors_for_test(
        fence_state,
        command_runner: runner,
        cgroup_reader: fn _path -> {:ok, []} end,
        termination_fun: fn identity, _opts ->
          {:ok,
           %{
             process_tree: :terminated,
             supervisor: :systemd_user,
             unit: identity.unit,
             session_id: identity.session_id,
             process_id: identity.process_id,
             pre_active_state: "active",
             pre_control_group: identity.control_group,
             pre_processes: identity.launch_processes,
             main_pid: identity.main_pid,
             active_state: "inactive",
             control_group: identity.control_group,
             remaining_processes: 0,
             observed_at_ms: 201,
             evidence_ref: "systemd:observed-after-restart-snapshot"
           }}
        end,
        now_ms: 200
      )

    lease = reconciled.executions["HGS-294"].leases["worker-1"]
    assert lease.termination_confirmed_at_ms == 201
    assert lease.termination_evidence.observed_at_ms == 201
  end

  test "orchestrator mutation guard follows the current generation snapshot" do
    admission = %{
      issue_id: "HGS-294",
      repository: "openai/symphony",
      branch: "codex/hgs-294",
      worktree: "C:/code/hypergrid.au/_worktrees/symphony-hgs-294"
    }

    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)
    state = %Orchestrator.State{execution_fence: fence_state}

    assert {:reply, {:ok, %{generation: 1, action: :state_mutation}}, ^state} =
             Orchestrator.handle_call(
               {:execution_fence_authorize, token, :state_mutation},
               {self(), make_ref()},
               state
             )

    {:ok, fenced_fence, :fenced} =
      ExecutionFence.fence(fence_state, token, %{terminal_state: "Done", accepted_head: "abc123"}, 110)

    fenced_state = %{state | execution_fence: fenced_fence}

    assert {:reply, {:error, :terminal_fenced}, ^fenced_state} =
             Orchestrator.handle_call(
               {:execution_fence_authorize, token, :state_mutation},
               {self(), make_ref()},
               fenced_state
             )
  end

  test "snapshot exposes sanitized execution and session ownership" do
    admission = admission()
    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)
    {:ok, fence_state, :registered} = ExecutionFence.register(fence_state, token, :worker, session(), 100)

    state = %Orchestrator.State{
      poll_interval_ms: 30_000,
      max_concurrent_agents: 1,
      next_poll_due_at_ms: 100,
      poll_check_in_progress: false,
      tick_timer_ref: nil,
      tick_token: nil,
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
      execution_fence: fence_state
    }

    assert {:reply, snapshot, _state} = Orchestrator.handle_call(:snapshot, {self(), make_ref()}, state)
    assert %{schema_version: 1, executions: [execution], sessions: [session], history: []} = snapshot.execution_fence
    assert execution.issue_id == "HGS-294"
    assert execution.status == :active
    assert execution.cleanup == :pending
    assert session.role == :worker
    assert session.session_id == "worker-1"
    assert session.process_id == "logical-process-1"
    refute Map.has_key?(session, :pid)
    refute Map.has_key?(session, :closure)
  end

  test "matching worker runtime information persists its exact head" do
    admission = admission()
    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)
    {:ok, fence_state, :registered} = ExecutionFence.register(fence_state, token, :worker, session(), 100)

    entry = %{
      execution_token: token,
      execution_session_id: "worker-1",
      worker_host: nil,
      workspace_path: admission.worktree,
      accepted_head: nil
    }

    state = %Orchestrator.State{
      execution_fence: fence_state,
      running: %{"HGS-294" => entry}
    }

    runtime_info = %{
      execution_token: token,
      execution_session_id: "worker-1",
      worker_host: nil,
      workspace_path: admission.worktree,
      head: "def456"
    }

    assert {:noreply, updated_state} =
             Orchestrator.handle_info(
               {:worker_runtime_info, "HGS-294", runtime_info},
               state
             )

    assert updated_state.running["HGS-294"].accepted_head == "def456"
    assert updated_state.execution_fence.sessions["worker-1"].head == "def456"
  end

  test "runtime information from another generation is ignored" do
    admission = admission()
    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)
    {:ok, fence_state, :registered} = ExecutionFence.register(fence_state, token, :worker, session(), 100)

    entry = %{execution_token: token, execution_session_id: "worker-1", accepted_head: nil}
    state = %Orchestrator.State{execution_fence: fence_state, running: %{"HGS-294" => entry}}

    stale_info = %{
      execution_token: %{issue_id: "HGS-294", generation: 99},
      execution_session_id: "worker-1",
      head: "def456"
    }

    assert {:noreply, ^state} =
             Orchestrator.handle_info({:worker_runtime_info, "HGS-294", stale_info}, state)
  end

  test "orchestrator persists one head-divergence triage record" do
    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-orchestrator-triage-#{System.unique_integer([:positive])}"
      )

    path = Path.join(root, "execution-fence.json")
    File.mkdir_p!(root)
    on_exit(fn -> File.rm_rf(root) end)

    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)
    {:ok, fence_state, :fenced} = ExecutionFence.fence(fence_state, token, %{terminal_state: "Done", accepted_head: "abc123"}, 110)
    state = %Orchestrator.State{execution_fence: fence_state, execution_fence_path: path}

    assert {:reply, {:ok, :recorded}, updated_state} =
             Orchestrator.handle_call(
               {:execution_fence_head_divergence, token, "abc123", "def456", 120},
               {self(), make_ref()},
               state
             )

    assert {:ok, persisted} = Persistence.load(path)
    assert [%{observed_head: "def456"}] = Map.values(persisted.triage_records)

    assert {:reply, {:ok, :already_recorded}, ^updated_state} =
             Orchestrator.handle_call(
               {:execution_fence_head_divergence, token, "abc123", "ghi789", 121},
               {self(), make_ref()},
               updated_state
             )
  end

  test "terminal reconciliation preserves a divergent workspace and persists triage" do
    workflow_root = Path.dirname(Workflow.workflow_file_path())
    workspace_root = Path.join(workflow_root, "workspace-root")
    workspace = Path.join(workspace_root, "workspace")
    state_path = Path.join(workflow_root, "execution-fence.json")
    issue_id = "HGS-294-divergence"
    issue_identifier = "MT-294-divergence"

    File.mkdir_p!(workspace)

    write_workflow_file!(Workflow.workflow_file_path(),
      workspace_root: "workspace-root",
      tracker_active_states: ["Todo"],
      tracker_terminal_states: ["Done"]
    )

    {_, 0} = System.cmd("git", ["init", "-q"], cd: workspace)
    File.write!(Path.join(workspace, "README.md"), "initial\n")
    {_, 0} = System.cmd("git", ["add", "."], cd: workspace)

    {_, 0} =
      System.cmd(
        "git",
        ["-c", "user.name=Symphony Test", "-c", "user.email=symphony@example.test", "commit", "-qm", "initial"],
        cd: workspace
      )

    {old_head, 0} = System.cmd("git", ["rev-parse", "HEAD"], cd: workspace)
    old_head = String.trim(old_head)

    File.write!(Path.join(workspace, "README.md"), "diverged\n")
    {_, 0} = System.cmd("git", ["add", "."], cd: workspace)

    {_, 0} =
      System.cmd(
        "git",
        ["-c", "user.name=Symphony Test", "-c", "user.email=symphony@example.test", "commit", "-qm", "diverged"],
        cd: workspace
      )

    {observed_head, 0} = System.cmd("git", ["rev-parse", "HEAD"], cd: workspace)
    observed_head = String.trim(observed_head)
    assert {:ok, ^observed_head} = Workspace.current_head(workspace)

    execution_attrs =
      admission()
      |> Map.put(:issue_id, issue_id)
      |> Map.put(:worktree, workspace)

    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), execution_attrs, 100)

    {:ok, fence_state, :registered} =
      ExecutionFence.register(
        fence_state,
        token,
        :worker,
        session()
        |> Map.put(:issue_id, issue_id)
        |> Map.put(:worktree, workspace),
        100
      )

    agent_pid =
      spawn(fn ->
        receive do
          :stop -> :ok
        end
      end)

    state = %Orchestrator.State{
      task_supervisor: SymphonyElixir.TaskSupervisor,
      execution_fence: fence_state,
      execution_fence_path: state_path,
      running: %{
        issue_id => %{
          pid: agent_pid,
          ref: nil,
          identifier: issue_identifier,
          issue: %Issue{id: issue_id, state: "Todo", identifier: issue_identifier},
          execution_token: token,
          execution_session_id: "worker-1",
          workspace_path: workspace,
          accepted_head: old_head,
          started_at: DateTime.utc_now()
        }
      },
      claimed: MapSet.new([issue_id]),
      codex_totals: %{input_tokens: 0, output_tokens: 0, total_tokens: 0, seconds_running: 0},
      retry_attempts: %{}
    }

    terminal_issue = %Issue{
      id: issue_id,
      identifier: issue_identifier,
      state: "Done",
      title: "Diverged",
      description: "Unexpected post-terminal delta",
      labels: []
    }

    updated_state = Orchestrator.reconcile_issue_states_for_test([terminal_issue], state)

    refute Map.has_key?(updated_state.running, issue_id)
    assert File.dir?(workspace)
    assert updated_state.execution_fence.executions[issue_id].cleanup == :pending
    assert [%{expected_head: ^old_head, observed_head: ^observed_head}] = Map.values(updated_state.execution_fence.triage_records)
    assert {:ok, persisted} = Persistence.load(state_path)
    assert persisted.triage_records == updated_state.execution_fence.triage_records
    assert persisted.executions[issue_id].cleanup == :pending
    assert length(Map.values(persisted.triage_records)) == 1
  end

  test "reconciliation call applies blocked ownership and returns its evidence" do
    admission = admission()
    {:ok, fence_state, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)
    {:ok, fence_state, :registered} = ExecutionFence.register(fence_state, token, :worker, session(), 100)
    state = %Orchestrator.State{execution_fence: fence_state}
    unknown = Map.put(session(), :session_id, "unknown-session")

    assert {:reply, {:ok, %{summary: %{status: :blocked, unknown: unknown_ids}, execution_fence: _}}, updated_state} =
             Orchestrator.handle_call(
               {:execution_fence_reconcile, [unknown], 101, 50},
               {self(), make_ref()},
               state
             )

    assert unknown_ids == ["unknown-session", "worker-1"]
    assert updated_state.execution_fence.executions["HGS-294"].ownership == :unknown
  end

  defp admission do
    %{
      issue_id: "HGS-294",
      repository: "openai/symphony",
      branch: "codex/hgs-294",
      worktree: "C:/code/hypergrid.au/_worktrees/symphony-hgs-294"
    }
  end

  defp session do
    Map.merge(admission(), %{
      generation: 1,
      role: :worker,
      session_id: "worker-1",
      process_id: "logical-process-1",
      linear_state: "In Progress",
      pr_state: "OPEN",
      head: "abc123",
      last_heartbeat_at: 100
    })
  end

  defp replay_cleaned_execution(fence, index) do
    issue_id = "HGS-350-starvation-#{index}"
    repository = "openai/symphony-#{index}"
    head = "head-#{index}"
    admission = %{issue_id: issue_id, repository: repository, branch: "codex/#{index}", worktree: "/tmp/#{issue_id}"}
    session_id = "worker-starvation-#{index}"

    {:ok, fence, token} = ExecutionFence.admit(fence, admission, 100)

    session = %{
      issue_id: issue_id,
      repository: repository,
      branch: admission.branch,
      worktree: admission.worktree,
      generation: 1,
      role: :worker,
      session_id: session_id,
      process_id: "process-starvation-#{index}",
      linear_state: "In Progress",
      pr_state: "OPEN",
      head: head,
      last_heartbeat_at: 100
    }

    {:ok, fence, :registered} = ExecutionFence.register(fence, token, :worker, session, 100)
    {:ok, fence, :released} = ExecutionFence.release(fence, token, session_id, :orchestrator_stop)

    evidence = %{
      session_id: session_id,
      process_id: session.process_id,
      process_tree: :terminated,
      evidence_ref: "process-tree-starvation-#{index}",
      observed_at_ms: 110
    }

    {:ok, fence, :confirmed} = ExecutionFence.confirm_termination(fence, token, session_id, evidence, 110)
    {:ok, fence, :fenced} = ExecutionFence.fence(fence, token, %{terminal_state: "Done", accepted_head: head}, 120)
    {:ok, fence, :prepared} = ExecutionFence.prepare_cleanup(fence, token, head, 121)
    {:ok, fence} = ExecutionFence.record_cleanup_evidence(fence, token, head, "sha256:cleanup-#{index}", 122)
    {:ok, fence, :cleaned} = ExecutionFence.cleanup(fence, token, head, 123)

    {fence,
     %{
       index: index,
       issue_id: issue_id,
       repository: repository,
       session_id: session_id,
       process_id: session.process_id,
       head: head,
       evidence_ref: evidence.evidence_ref,
       projection_id: "projection-#{index}",
       reservation_id: "reservation-#{index}"
     }}
  end

  defp replay_reservation(details, profile) do
    %{
      issue_id: details.issue_id,
      managed_project_profile_id: profile,
      repository_ref: details.repository,
      projection_id: details.projection_id,
      reservation_id: details.reservation_id,
      reservation_nonce: "nonce-#{details.index}",
      scope_keys: ["repo:#{details.repository}"],
      runner_id: "runner-starvation",
      generation: 1,
      session_id: details.session_id,
      process_id: details.process_id,
      responsible_delegation_id: "delegation-#{details.index}",
      execution_fence_token: "#{details.issue_id}:1",
      runtime_lease_id: details.session_id
    }
  end

  defp provider_response(data), do: %Req.Response{status: 200, body: %{"data" => data}}
end
