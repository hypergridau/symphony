defmodule SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter do
  alias SymphonyElixir.WorkPackageClaim.Journal

  def allocate_or_reconcile(_assignment, key, context) do
    if is_binary(context[:claim_journal_path]) do
      {:ok, journal} = Journal.load(context.claim_journal_path)
      [reservation] = Map.values(journal.reservations)
      send(context.test_pid, {:allocation_snapshot_observed, reservation.assignment_snapshot})
    end

    send(context.test_pid, {:allocation_requested, key})
    Map.get(context, :allocation_result, {:ok, %{id: "rke2job:v1:fixture-allocation", status: :ready}})
  end

  def activate_owned(allocation, _assignment, key, context) do
    {:ok, journal} = Journal.load(context.claim_journal_path)
    [reservation] = Map.values(journal.reservations)
    send(context.test_pid, {:activation_requested, allocation.id, key, reservation.dispatch.phase})
    {:ok, %{status: :active}}
  end
end

defmodule SymphonyElixir.RKE2Job.PollTerminalAdapter do
  def finalize_terminal_owned(allocation, assignment, key, context) do
    send(context.test_pid, {:poll_terminal_finalization, allocation.id, assignment.sha256, key})
    {:held, :terminal_result_pending}
  end
end

defmodule SymphonyElixir.RKE2Job.PollSlotGuard do
  def verify_claim_uid(_slot, _context), do: :ok
  def verify_bound(_slot, _assignment, _allocation, _context), do: :ok
end

defmodule SymphonyElixir.WorkPackageClaimTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.Codex.ModelRouter
  alias SymphonyElixir.{ExecutionFence, ManagedAssignmentBundle, Orchestrator, ResponsibilityGraph, WorkPackageClaim}
  alias SymphonyElixir.ManagedExecutor.ClaimBinding
  alias SymphonyElixir.RKE2Job.{HostAllocationContext, JobSpec, SuspendedController}
  alias SymphonyElixir.Tracker.Issue
  alias SymphonyElixir.WorkPackageClaim.{Journal, Recovery}

  @repository "hypergridau/symphony"
  @issue_id "issue-349"
  @profile "profile-349"

  @canonical_json_fixture ~s({"contractVersion":"work-package-runtime-attestation.v1","runnerId":"runner-349","managedProjectProfileId":"profile-349","reservationId":"reservation-349","reservationNonce":"nonce-349","issueId":"issue-349","generation":1,"sessionId":"worker-349","processId":"process-349","responsibleDelegationId":"delegation-349","executionFenceToken":"issue-349:1","runtimeLeaseId":"worker-349","repositoryRef":"hypergridau/symphony","scopeKeys":["repo:hypergridau/symphony","work:349"],"attestedAt":"2026-09-06T10:00:00.000Z"})

  test "root witness rejection prevents the provider claim request" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    parent = self()

    input = %{
      input
      | host_witness_fun: fn request ->
          send(parent, {:witness, request})
          {:error, :root_witness_unavailable}
        end
    }

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue") do
        {:ok, response(%{"data" => reservation_payload()})}
      else
        send(parent, :provider_claim_requested)
        {:ok, response(%{"data" => claim_result_payload()})}
      end
    end

    assert {:error, :root_witness_unavailable} =
             WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end)

    assert_receive {:witness, %{"operation" => "claim_intent", "claim" => claim}}
    assert claim["reservationId"] == "reservation-349"
    expected_nonce_hash = :crypto.hash(:sha256, "nonce-349") |> Base.encode16(case: :lower)
    assert claim["nonceHash"] == expected_nonce_hash
    refute_receive :provider_claim_requested
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "submitted"
  end

  test "paused managed claim retains its exact suspended allocation for controller recovery" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    parent = self()

    input = %{
      input
      | host_witness_fun: fn request ->
          send(parent, {:claim_witness, request["operation"]})
          replayed = request["replayOnly"] == true
          {:ok, %{"ok" => true, "receipt" => %{"version" => 1, "sequence" => 1, "hash" => String.duplicate("a", 64), "replayed" => replayed}}}
        end
    }

    request_fun = fn url, _options ->
      payload = if String.ends_with?(url, "/reservations/by-issue"), do: reservation_payload(), else: claim_result_payload()
      {:ok, response(%{"data" => payload})}
    end

    assert {:ok, _claim} =
             WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end)

    allocation_id = "rke2job:v1:fixture-allocation"
    assert :ok = WorkPackageClaim.record_suspended_allocation(input, %{id: allocation_id, status: :ready})
    assert :ok = WorkPackageClaim.begin_paused_recovery(input)
    assert {:ok, ^allocation_id} = WorkPackageClaim.suspended_allocation(input)
    assert {:error, :suspended_allocation_controller_required} = WorkPackageClaim.begin_spawn(input)
    assert {:ok, %{phase: "allocation_suspended", allocation_id: ^allocation_id}} = WorkPackageClaim.handoff_allocation(input)
    assert :ok = WorkPackageClaim.begin_suspended_spawn(input, allocation_id)
    assert {:ok, %{phase: "spawn_started", allocation_id: ^allocation_id}} = WorkPackageClaim.handoff_allocation(input)
    assert :ok = WorkPackageClaim.replay_spawn_intent(input)

    assert {:ok, journal} = Journal.load(path)
    [reservation] = Map.values(journal.reservations)
    assert reservation.dispatch.phase == "spawn_started"
    assert reservation.dispatch.allocation_id == allocation_id
    assert_receive {:claim_witness, "claim_intent"}
    assert_receive {:claim_witness, "claim_bound"}
    assert_receive {:claim_witness, "spawn_intent"}
    assert_receive {:claim_witness, "spawn_intent"}
  end

  test "suspended controller records allocation before intent, activates after root witness, and replays" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input, lease: lease} = authority_fixture(path)
    parent = self()

    input = %{
      input
      | host_witness_fun: fn request ->
          send(parent, {:root_witness, request["operation"], request["replayOnly"] == true})

          {:ok,
           %{
             "ok" => true,
             "receipt" => %{
               "version" => 1,
               "sequence" => 1,
               "hash" => String.duplicate("a", 64),
               "replayed" => request["replayOnly"] == true
             }
           }}
        end
    }

    request_fun = fn url, _options ->
      payload = if String.ends_with?(url, "/reservations/by-issue"), do: reservation_payload(), else: claim_result_payload()
      {:ok, response(%{"data" => payload})}
    end

    assert {:ok, claim} =
             WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end)

    assignment = suspended_assignment(lease)
    assert {:ok, binding} = ClaimBinding.from_claim(claim, assignment, input.runner_id)
    assert {:ok, ^binding} = ClaimBinding.from_journal(claim.reservation, assignment, input.runner_id)

    assert {:error, :provider_claim_invalid} =
             ClaimBinding.from_journal(%{claim.reservation | session_id: "foreign-session"}, assignment, input.runner_id)

    assert :ok = WorkPackageClaim.record_assignment_snapshot(input, assignment)
    assert :ok = WorkPackageClaim.record_assignment_snapshot(input, assignment)

    changed_attrs =
      assignment
      |> Map.drop([:schema_version, :sha256, :environment])
      |> Map.put(:objective, %{assignment.objective | content: "A different objective"})
      |> Map.merge(%{
        platform: assignment.environment.platform,
        environment_classification: assignment.environment.classification,
        environment_constraints: assignment.environment.constraints,
        placement: assignment.environment.placement,
        target_environment: assignment.environment.target_environment
      })

    assert {:ok, changed_assignment} = ManagedAssignmentBundle.build(changed_attrs)

    assert {:error, :assignment_snapshot_changed} =
             WorkPackageClaim.record_assignment_snapshot(input, changed_assignment)

    assert {:ok, foreign_assignment} =
             changed_attrs
             |> Map.put(:lease, %{assignment.lease | session_id: "foreign-session"})
             |> ManagedAssignmentBundle.build()

    assert {:ok, foreign_snapshot} = ManagedAssignmentBundle.snapshot(foreign_assignment)
    original_journal = File.read!(path)
    original_payload = Jason.decode!(original_journal)
    [reservation_key] = Map.keys(original_payload["reservations"])
    foreign_payload = put_in(original_payload, ["reservations", reservation_key, "assignment_snapshot"], foreign_snapshot)
    File.write!(path, Jason.encode!(foreign_payload))
    assert {:error, {:invalid_journal, _reason}} = Journal.load(path)
    File.write!(path, original_journal)

    context = %{
      adapter: SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter,
      claim_binding: binding,
      claim_journal_path: path,
      test_pid: self()
    }

    assert {:error, :suspended_controller_claim_binding_invalid} =
             SuspendedController.allocate(
               assignment,
               input,
               %{context | claim_binding: %{binding | reservation_id: "other-reservation"}}
             )

    refute_receive {:allocation_requested, _key}

    assert {:ok, %{id: allocation_id, status: :ready}} =
             SuspendedController.allocate(assignment, input, context)

    assert allocation_id == "rke2job:v1:fixture-allocation"
    assert_receive {:allocation_snapshot_observed, snapshot}
    assert {:ok, ^assignment} = ManagedAssignmentBundle.from_snapshot(snapshot)
    assert_receive {:allocation_requested, allocation_key}
    assert allocation_key == assignment.sha256 <> ":allocation"

    assert {:ok, journal} = Journal.load(path)
    [retained] = Map.values(journal.reservations)
    assert retained.assignment_snapshot == snapshot
    assert retained.dispatch.allocation_id == allocation_id
    allocated_journal = File.read!(path)
    allocated_payload = Jason.decode!(allocated_journal)

    assert {:ok, %{phase: "allocation_suspended", allocation_id: ^allocation_id}} =
             WorkPackageClaim.handoff_allocation(input)

    assert {:error, :suspended_allocation_not_admissible} =
             WorkPackageClaim.prepare_suspended_allocation(input)

    assert {:error, :suspended_allocation_not_admissible} =
             SuspendedController.allocate(assignment, input, context)

    refute_receive {:allocation_requested, _key}

    assert {:error, :suspended_controller_assignment_snapshot_changed} =
             SuspendedController.resume(changed_assignment, input, context)

    refute_receive {:root_witness, "spawn_intent", _replay}
    refute_receive {:activation_requested, _allocation_id, _key, _phase}

    missing_snapshot_payload = update_in(allocated_payload, ["reservations", reservation_key], &Map.delete(&1, "assignment_snapshot"))
    File.write!(path, Jason.encode!(missing_snapshot_payload))

    assert {:error, :suspended_controller_assignment_snapshot_changed} =
             SuspendedController.resume(assignment, input, context)

    refute_receive {:root_witness, "spawn_intent", _replay}
    File.write!(path, allocated_journal)

    assert {:ok, %{status: :active}} = SuspendedController.resume(assignment, input, context)
    assert_receive {:root_witness, "spawn_intent", false}
    assert_receive {:activation_requested, ^allocation_id, activation_key, "spawn_started"}
    assert activation_key == assignment.sha256 <> ":activate"

    assert {:ok, changed_snapshot} = ManagedAssignmentBundle.snapshot(changed_assignment)
    started_journal = File.read!(path)
    started_payload = Jason.decode!(started_journal)
    changed_payload = put_in(started_payload, ["reservations", reservation_key, "assignment_snapshot"], changed_snapshot)
    File.write!(path, Jason.encode!(changed_payload))

    assert {:error, :suspended_controller_assignment_snapshot_changed} =
             SuspendedController.resume(assignment, input, context)

    refute_receive {:root_witness, "spawn_intent", _replay}
    refute_receive {:activation_requested, _allocation_id, _key, _phase}
    File.write!(path, started_journal)

    assert {:ok, %{status: :active}} = SuspendedController.resume(assignment, input, context)
    assert_receive {:root_witness, "spawn_intent", true}
    assert_receive {:activation_requested, ^allocation_id, ^activation_key, "spawn_started"}

    payload = path |> File.read!() |> Jason.decode!()
    [key] = Map.keys(payload["reservations"])
    payload = put_in(payload, ["reservations", key, "assignment_snapshot"], Base.encode64("changed"))
    File.write!(path, Jason.encode!(payload))
    assert {:error, {:invalid_journal, :invalid_assignment_snapshot}} = Journal.load(path)
  end

  test "suspended controller rejects mismatched claim binding before allocation" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input, lease: lease} = authority_fixture(path)
    assignment = suspended_assignment(lease)

    context = %{
      adapter: SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter,
      claim_binding: %{issue_id: "different-issue"},
      test_pid: self()
    }

    assert {:error, :suspended_controller_claim_binding_invalid} =
             SuspendedController.allocate(assignment, input, context)

    refute_receive {:allocation_requested, _key}
    assert :missing = Journal.load(path)
  end

  test "rejected suspended activation intent retains its allocation in recovery pending" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue"),
        do: {:ok, response(%{"data" => reservation_payload()})},
        else: {:ok, response(%{"data" => claim_result_payload()})}
    end

    assert {:ok, _claim} =
             WorkPackageClaim.claim(input,
               request_fun: request_fun,
               now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end
             )

    allocation_id = "rke2job:v1:fixture-allocation"
    assert :ok = WorkPackageClaim.record_suspended_allocation(input, %{id: allocation_id, status: :ready})
    denied = %{input | host_witness_fun: fn _request -> {:error, :root_witness_unavailable} end}

    assert {:error, :root_witness_unavailable} = WorkPackageClaim.begin_suspended_spawn(denied, allocation_id)

    assert {:ok, journal} = Journal.load(path)
    [reservation] = Map.values(journal.reservations)
    assert reservation.dispatch.phase == "spawn_started"
    assert reservation.dispatch.allocation_id == allocation_id
    assert {:ok, %{phase: "spawn_started", allocation_id: ^allocation_id}} = WorkPackageClaim.handoff_allocation(input)
  end

  test "suspended handoff reads and writes fail closed when the claim journal or row is missing" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    allocation_id = "rke2job:v1:missing-row"

    assert {:error, :claim_journal_missing} = WorkPackageClaim.handoff_allocation(input)
    assert {:error, :claim_journal_missing} = WorkPackageClaim.begin_suspended_spawn(input, allocation_id)

    assert :ok = Journal.save(path, Journal.new())
    assert {:error, :claim_recovery_journal_missing} = WorkPackageClaim.handoff_allocation(input)
    assert {:error, :claim_recovery_journal_missing} = WorkPackageClaim.begin_suspended_spawn(input, allocation_id)
  end

  test "expired graph authority cannot submit a new provider claim" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    expiry = 1_000_000_000_000

    delegations =
      Enum.reduce(["owner", "delegation-349"], input.responsibility_graph.delegations, fn id, acc ->
        Map.update!(acc, id, &Map.put(&1, :expires_at_ms, expiry))
      end)

    input = %{input | responsibility_graph: %{input.responsibility_graph | delegations: delegations}}

    assert {:ok, %{expires_at_ms: ^expiry}} =
             ResponsibilityGraph.admission_delegation(input.responsibility_graph, @issue_id, "HGS-349", @repository)

    parent = self()

    request_fun = fn _url, _options ->
      send(parent, :provider_request)
      {:ok, response(%{"data" => %{}})}
    end

    assert {:error, :runtime_lease_mismatch} =
             WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: &DateTime.utc_now/0)

    refute_receive :provider_request
    assert :missing = Journal.load(path)
  end

  test "authority expiry after spawn_started is fenced before a spawn witness" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    parent = self()

    input = %{
      input
      | host_witness_fun: fn %{"operation" => operation} ->
          send(parent, {:spawn_fence_witness, operation})
          {:ok, %{"ok" => true, "receipt" => %{"version" => 1, "sequence" => 1, "hash" => String.duplicate("a", 64), "replayed" => false}}}
        end
    }

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue"),
        do: {:ok, response(%{"data" => reservation_payload()})},
        else: {:ok, response(%{"data" => claim_result_payload()})}
    end

    assert {:ok, _claim} =
             WorkPackageClaim.claim(input,
               request_fun: request_fun,
               now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end
             )

    assert_receive {:spawn_fence_witness, "claim_intent"}
    assert_receive {:spawn_fence_witness, "claim_bound"}
    expiry = input.responsibility_graph.delegations["delegation-349"].expires_at_ms
    before_expiry = DateTime.from_unix!(expiry - 1, :millisecond)
    after_expiry = DateTime.from_unix!(expiry + 1, :millisecond)
    times = :atomics.new(1, [])

    now_fun = fn ->
      case :atomics.add_get(times, 1, 1) do
        1 -> before_expiry
        _ -> after_expiry
      end
    end

    assert {:error, {:pre_spawn_recovery_pending, :runtime_lease_mismatch}} =
             WorkPackageClaim.begin_spawn(input, now_fun: now_fun)

    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
    refute_receive {:spawn_fence_witness, "spawn_intent"}
    assert {:error, :invalid_claim_dispatch_transition} = WorkPackageClaim.begin_paused_recovery(input)
  end

  test "spawn witness failure keeps the attempted reservation held" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue"),
        do: {:ok, response(%{"data" => reservation_payload()})},
        else: {:ok, response(%{"data" => claim_result_payload()})}
    end

    assert {:ok, _claim} =
             WorkPackageClaim.claim(input,
               request_fun: request_fun,
               now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end
             )

    input = %{
      input
      | host_witness_fun: fn %{"operation" => "spawn_intent"} ->
          {:error, :root_witness_unavailable}
        end
    }

    assert {:error, :root_witness_unavailable} = WorkPackageClaim.begin_spawn(input)
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "spawn_started"
    assert {:error, :invalid_claim_dispatch_transition} = WorkPackageClaim.begin_paused_recovery(input)
  end

  test "explicit root pause rejection demotes a never-witnessed spawn for abort recovery" do
    for reason <- ["global admission paused", "global pause transition active"] do
      path = temp_path()
      on_exit(fn -> File.rm_rf(path) end)
      %{input: input} = authority_fixture(path)

      request_fun = fn url, _options ->
        if String.ends_with?(url, "/reservations/by-issue"),
          do: {:ok, response(%{"data" => reservation_payload()})},
          else: {:ok, response(%{"data" => claim_result_payload()})}
      end

      assert {:ok, _claim} =
               WorkPackageClaim.claim(input,
                 request_fun: request_fun,
                 now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end
               )

      input = %{
        input
        | host_witness_fun: fn %{"operation" => "spawn_intent"} ->
            {:ok, %{"ok" => false, "error" => reason}}
          end
      }

      assert {:error, {:pre_spawn_recovery_pending, {:global_pause, ^reason}}} =
               WorkPackageClaim.begin_spawn(input)

      assert {:ok, journal} = Journal.load(path)
      [{_key, reservation}] = Map.to_list(journal.reservations)
      assert reservation.dispatch.phase == "recovery_pending"
      assert {:error, :invalid_claim_dispatch_transition} = WorkPackageClaim.begin_paused_recovery(input)
    end
  end

  test "uncertain spawn intent can recover only an exact existing root receipt" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    parent = self()

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue"),
        do: {:ok, response(%{"data" => reservation_payload()})},
        else: {:ok, response(%{"data" => claim_result_payload()})}
    end

    assert {:ok, _claim} =
             WorkPackageClaim.claim(input,
               request_fun: request_fun,
               now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end
             )

    assert {:error, :claim_spawn_replay_unavailable} = WorkPackageClaim.replay_spawn_intent(input)

    uncertain = %{input | host_witness_fun: fn _request -> {:error, :root_witness_unavailable} end}
    assert {:error, :root_witness_unavailable} = WorkPackageClaim.begin_spawn(uncertain)

    replay = %{
      input
      | host_witness_fun: fn request ->
          send(parent, {:spawn_replay, request})

          {:ok,
           %{
             "ok" => true,
             "receipt" => %{
               "version" => 1,
               "sequence" => 3,
               "hash" => String.duplicate("a", 64),
               "replayed" => true
             }
           }}
        end
    }

    assert :ok = WorkPackageClaim.replay_spawn_intent(replay)
    assert_receive {:spawn_replay, %{"operation" => "spawn_intent", "replayOnly" => true}}

    missing = %{
      replay
      | host_witness_fun: fn _request -> {:error, {:host_witness_rejected, "spawn intent replay missing"}} end
    }

    assert {:error, {:host_witness_rejected, "spawn intent replay missing"}} =
             WorkPackageClaim.replay_spawn_intent(missing)
  end

  test "claim response remains indeterminate when root cannot record its binding" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    parent = self()

    input = %{
      input
      | host_witness_fun: fn %{"operation" => operation} ->
          send(parent, {:witness_operation, operation})

          if operation == "claim_bound" do
            {:error, :root_witness_unavailable}
          else
            {:ok, %{"ok" => true, "receipt" => %{"version" => 1, "sequence" => 1, "hash" => String.duplicate("a", 64), "replayed" => false}}}
          end
        end
    }

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue") do
        {:ok, response(%{"data" => reservation_payload()})}
      else
        send(parent, :provider_claim_requested)
        {:ok, response(%{"data" => claim_result_payload()})}
      end
    end

    assert {:error, {:claim_indeterminate, :root_witness_unavailable}} =
             WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end)

    assert_receive {:witness_operation, "claim_intent"}
    assert_receive :provider_claim_requested
    assert_receive {:witness_operation, "claim_bound"}
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "submitted"
  end

  test "local claimed worker final pause read precedes the durable spawn marker" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    input = with_assignment_manifest(input, %Issue{id: @issue_id, identifier: "HGS-349", title: "Spawn order"})

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
    assert reservation.workspace_id == "workspace-349"
    assert reservation.company_id == "company-349"

    runtime =
      input
      |> Map.take([:base_url, :runner_token, :attestation_key, :runner_id, :managed_project_profile_id, :journal_path, :pool_key, :host_witness_fun, :managed_delegations])

    task_supervisor = start_supervised!({Task.Supervisor, max_children: 0})

    state = %Orchestrator.State{
      execution_fence: input.fence_state,
      responsibility_graph: input.responsibility_graph,
      work_package_runtime: runtime,
      task_supervisor: task_supervisor
    }

    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Spawn order", state: "Todo"}
    pause_pattern = {SymphonyElixir.GlobalPause, :paused?, 0}
    spawn_pattern = {WorkPackageClaim, :begin_spawn, 2}
    parent = self()
    tracer = spawn(fn -> forward_trace(parent) end)

    :erlang.trace_pattern(pause_pattern, true, [:local])
    :erlang.trace_pattern(spawn_pattern, true, [:local])
    :erlang.trace(self(), true, [:call, {:tracer, tracer}])

    start_result =
      try do
        Orchestrator.start_claimed_worker_for_test(state, issue, fn -> :ok end)
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
    assert is_integer(marker_index), inspect({calls, start_result})
    assert final_gate in Enum.take(calls, marker_index)
    refute final_gate in Enum.drop(calls, marker_index + 1)

    assert match?({:error, _reason}, start_result)
    assert Task.Supervisor.children(task_supervisor) == []
    assert {:ok, journal_after_spawn} = Journal.load(path)
    [{_key, reservation_after_spawn}] = Map.to_list(journal_after_spawn.reservations)
    assert reservation_after_spawn.dispatch.phase == "spawn_started"
  end

  test "real orchestrator snapshot waits for a local claimed spawn already past its final gate" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    input = with_assignment_manifest(input, %Issue{id: @issue_id, identifier: "HGS-349", title: "Managed barrier"})

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
      if is_binary(previous_workflow_path),
        do: SymphonyElixir.Workflow.set_workflow_file_path(previous_workflow_path),
        else: SymphonyElixir.Workflow.clear_workflow_file_path()

      if is_nil(previous_issues),
        do: Application.delete_env(:symphony_elixir, :memory_tracker_issues),
        else: Application.put_env(:symphony_elixir, :memory_tracker_issues, previous_issues)
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

    runtime =
      input
      |> Map.take([:base_url, :runner_token, :attestation_key, :runner_id, :managed_project_profile_id, :journal_path, :pool_key, :host_witness_fun, :managed_delegations])

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
      %{
        state
        | execution_fence: input.fence_state,
          responsibility_graph: input.responsibility_graph,
          work_package_runtime: runtime,
          task_supervisor: held_supervisor
      }
    end)

    parent = self()

    dispatch =
      Task.async(fn ->
        :sys.replace_state(orchestrator, fn state ->
          start_result = Orchestrator.start_claimed_worker_for_test(state, issue, fn -> :ok end)
          send(parent, {:managed_start_result, start_result})
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
    assert_receive {:managed_start_result, {:error, _reason}}, 5_000
    assert %Orchestrator.State{running: running_state} = Task.await(dispatch, 5_000)
    assert running_state == %{}
    assert %{pause_gate: gate, running: running} = Task.await(snapshot, 5_000)
    assert gate.transition_epoch == epoch
    assert gate.paused?
    assert running == []
  end

  test "pause acknowledgement waits for a successful managed child start" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    input = with_assignment_manifest(input, %Issue{id: @issue_id, identifier: "HGS-349", title: "Successful barrier"})

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
      if is_binary(previous_workflow_path),
        do: SymphonyElixir.Workflow.set_workflow_file_path(previous_workflow_path),
        else: SymphonyElixir.Workflow.clear_workflow_file_path()

      if is_nil(previous_issues),
        do: Application.delete_env(:symphony_elixir, :memory_tracker_issues),
        else: Application.put_env(:symphony_elixir, :memory_tracker_issues, previous_issues)
    end)

    held_supervisor = start_supervised!({Task.Supervisor, max_children: 1})
    on_exit(fn -> if Process.alive?(held_supervisor), do: :sys.resume(held_supervisor) end)

    runtime =
      input
      |> Map.take([:base_url, :runner_token, :attestation_key, :runner_id, :managed_project_profile_id, :journal_path, :pool_key, :host_witness_fun, :managed_delegations])

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
      %{
        state
        | execution_fence: input.fence_state,
          responsibility_graph: input.responsibility_graph,
          work_package_runtime: runtime,
          task_supervisor: held_supervisor
      }
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

    assert {:error, {:global_pause_recovery_fence_failed, :invalid_claim_dispatch_transition}} =
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
    input = with_assignment_manifest(input, %Issue{id: @issue_id, identifier: "HGS-349", title: "Pause race"})

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

    runtime =
      input
      |> Map.take([:base_url, :runner_token, :attestation_key, :runner_id, :managed_project_profile_id, :journal_path, :pool_key, :host_witness_fun, :managed_delegations])

    fence_path = path <> ".fence"
    graph_path = path <> ".graph"
    :ok = ExecutionFence.Persistence.save(fence_path, input.fence_state)
    :ok = ResponsibilityGraph.Persistence.save(graph_path, input.responsibility_graph)

    state = %Orchestrator.State{
      execution_fence: input.fence_state,
      responsibility_graph: input.responsibility_graph,
      work_package_runtime: runtime,
      execution_fence_path: fence_path,
      responsibility_graph_path: graph_path
    }

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

    assert after_pause.execution_fence.executions[@issue_id].leases[lease.session_id].status == :released
    assert after_pause.execution_fence.executions[@issue_id].leases[lease.session_id].release_reason == :spawn_failed
    assert after_pause.responsibility_graph.delegations["delegation-349"].runtime_lease == nil
    assert {:ok, persisted_fence} = ExecutionFence.Persistence.load(fence_path)
    assert persisted_fence.executions[@issue_id].leases[lease.session_id].status == :released
    assert persisted_fence.executions[@issue_id].leases[lease.session_id].release_reason == "spawn_failed"
    assert {:ok, persisted_graph} = ResponsibilityGraph.Persistence.load(graph_path)
    assert persisted_graph.delegations["delegation-349"].runtime_lease == nil
    assert persisted_graph.delegations["delegation-349"].status == :active
    assert {:ok, journal_after_pause} = Journal.load(path)
    [{_key, reservation_after_pause}] = Map.to_list(journal_after_pause.reservations)
    assert reservation_after_pause.dispatch.phase == "recovery_pending"
    assert {:error, :invalid_claim_dispatch_transition} = WorkPackageClaim.begin_spawn(input)

    # Simulate restart reconciliation of the retained authority snapshots.
    # It deliberately makes the live execution unknown and its delegations
    # blocked; the confirmed journal must remain held until provider recovery
    # is eligible, not be replayed or replaced immediately.
    assert {:ok, restarted_fence} = ExecutionFence.mark_unreconciled_after_restart(after_pause.execution_fence)

    assert {:ok, restarted_graph} =
             ResponsibilityGraph.mark_unreconciled_after_restart(after_pause.responsibility_graph)

    assert restarted_fence.executions[@issue_id].ownership == :reconciled
    assert restarted_graph.delegations["delegation-349"].status == :active

    claim_time_ms = DateTime.to_unix(claim_time, :millisecond)

    refute Recovery.held?(restarted_fence, @issue_id)
    assert {:ok, [retained_claim]} = Recovery.unstarted_claims(runtime, restarted_fence)
    assert retained_claim.dispatch.phase == "recovery_pending"

    assert {:error, :claim_reconciliation_required} =
             Recovery.prepare(runtime, restarted_fence, restarted_graph, issue, nil, claim_time_ms)

    assert restarted_fence.executions[@issue_id].ownership == :reconciled
    assert restarted_graph.delegations["delegation-349"].runtime_lease == nil
    assert {:ok, journal_after_restart} = Journal.load(path)
    assert journal_after_restart == journal_after_pause
    assert Process.get(:claim_pause_requests) == 2
    assert Task.Supervisor.children(task_supervisor) == children_before
  end

  test "post-claim objective drift journals recovery and releases the fenced worker lease" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    original_issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Signed objective", state: "Todo", assignee_id: "owner"}
    changed_issue = %{original_issue | title: "Changed after claim"}

    {after_preflight, runtime} = post_claim_revalidation_failure(path, original_issue, changed_issue)

    assert Map.has_key?(after_preflight.blocked, @issue_id)
    assert after_preflight.running == %{}
    assert after_preflight.execution_fence.executions[@issue_id].leases["worker-349"].status == :released
    assert after_preflight.execution_fence.executions[@issue_id].leases["worker-349"].release_reason == :spawn_failed
    assert after_preflight.responsibility_graph.delegations["delegation-349"].runtime_lease == nil

    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
    assert {:ok, [retained]} = Recovery.unstarted_claims(runtime, after_preflight.execution_fence)
    assert retained.dispatch.phase == "recovery_pending"
  end

  test "post-claim grant expiry journals recovery and does not start a worker" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Signed objective", state: "Todo", assignee_id: "owner"}

    {after_preflight, _runtime} =
      post_claim_revalidation_failure(path, issue, issue,
        manifest_expiry_ms: System.system_time(:millisecond) - 1,
        graph_expiry_ms: System.system_time(:millisecond) - 1
      )

    assert Map.has_key?(after_preflight.blocked, @issue_id)
    assert after_preflight.running == %{}
    assert after_preflight.execution_fence.executions[@issue_id].leases["worker-349"].status == :released
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
  end

  test "post-claim owner drift journals recovery instead of dispatching to a stale assignee" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Signed objective", state: "Todo", assignee_id: "owner"}
    reassigned_issue = %{issue | assignee_id: "new-owner"}

    {after_preflight, _runtime} = post_claim_revalidation_failure(path, issue, reassigned_issue)

    assert Map.has_key?(after_preflight.blocked, @issue_id)
    assert after_preflight.running == %{}
    assert after_preflight.execution_fence.executions[@issue_id].leases["worker-349"].status == :released
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
  end

  test "managed spawn rejects a missing provider claim before starting a worker" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Signed objective", state: "Todo", assignee_id: "owner", dispatchable: true}

    {after_preflight, _runtime} =
      post_claim_revalidation_failure(path, issue, issue, provider_claim_override: nil)

    assert after_preflight.running == %{}
    assert after_preflight.blocked[@issue_id].error =~ "provider_claim_binding_failed"
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
  end

  test "signed RKE2 assignment retains its claim without starting a persistent local worker" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)

    issue = %Issue{
      id: @issue_id,
      identifier: "HGS-349",
      title: "Disposable assignment",
      state: "Todo",
      assignee_id: "owner",
      dispatchable: true
    }

    {blocked, runtime} = post_claim_revalidation_failure(path, issue, issue)

    assert blocked.running == %{}
    assert blocked.blocked[@issue_id].error =~ "disposable_rke2_controller_unavailable"
    assert blocked.execution_fence.executions[@issue_id].leases["worker-349"].status == :released
    assert {:ok, [retained]} = Recovery.unstarted_claims(runtime, blocked.execution_fence)
    assert retained.dispatch.phase == "recovery_pending"
    assert retained.dispatch.allocation_id == nil
    assert retained.reservation_id == "reservation-349"
    assert retained.reservation_nonce == "nonce-349"
  end

  test "signed RKE2 dispatch journals the exact suspended allocation before any activation" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)

    issue = %Issue{
      id: @issue_id,
      identifier: "HGS-349",
      title: "Suspended disposable assignment",
      state: "Todo",
      assignee_id: "owner",
      dispatchable: true
    }

    context = %{
      adapter: SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter,
      test_pid: self(),
      claim_journal_path: path
    }

    {blocked, _runtime} =
      post_claim_revalidation_failure(path, issue, issue, disposable_rke2_context: context)

    assert_receive {:allocation_requested, allocation_key}
    assert String.ends_with?(allocation_key, ":allocation")
    refute_receive {:activation_requested, _, _, _}
    assert blocked.running == %{}
    assert blocked.blocked[@issue_id].error =~ "disposable_rke2_activation_unavailable"
    assert blocked.execution_fence.executions[@issue_id].leases["worker-349"].status == :active
    assert {:ok, journal} = Journal.load(path)
    [reservation] = Map.values(journal.reservations)
    assert reservation.dispatch.phase == "allocation_suspended"
    assert reservation.dispatch.allocation_id == "rke2job:v1:fixture-allocation"
  end

  test "owner drift retains a blocked disposable allocation for exact cleanup" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)

    issue = %Issue{
      id: @issue_id,
      identifier: "HGS-349",
      title: "Retained disposable assignment",
      state: "Todo",
      assignee_id: "owner",
      dispatchable: true
    }

    context = %{
      adapter: SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter,
      test_pid: self(),
      claim_journal_path: path
    }

    {blocked, _runtime} =
      post_claim_revalidation_failure(path, issue, issue, disposable_rke2_context: context)

    drifted = %{issue | assignee_id: "another-owner"}
    reconciled = Orchestrator.reconcile_blocked_issue_states_for_test([drifted], blocked)

    assert Map.has_key?(reconciled.blocked, @issue_id)
    assert MapSet.member?(reconciled.claimed, @issue_id)
    assert reconciled.blocked[@issue_id].issue.assignee_id == "another-owner"
    assert {:ok, journal} = Journal.load(path)
    [reservation] = Map.values(journal.reservations)
    assert reservation.dispatch.phase == "allocation_suspended"
    assert reservation.dispatch.allocation_id == "rke2job:v1:fixture-allocation"
    refute_receive {:activation_requested, _, _, _}

    File.rename!(path, path <> ".missing")
    on_exit(fn -> File.rm_rf(path <> ".missing") end)
    uncertain = Orchestrator.reconcile_blocked_issue_states_for_test([drifted], reconciled)
    assert Map.has_key?(uncertain.blocked, @issue_id)
    assert MapSet.member?(uncertain.claimed, @issue_id)
  end

  test "restart restores a retained disposable claim before terminal issue reconciliation" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)

    issue = %Issue{
      id: @issue_id,
      identifier: "HGS-349",
      title: "Restarted disposable assignment",
      state: "Todo",
      assignee_id: "owner",
      dispatchable: true
    }

    context = %{
      adapter: SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter,
      test_pid: self(),
      claim_journal_path: path
    }

    {blocked, _runtime} =
      post_claim_revalidation_failure(path, issue, issue, disposable_rke2_context: context)

    restarted = %{blocked | blocked: %{}, claimed: MapSet.new()}
    File.rename!(path, path <> ".retained")
    on_exit(fn -> File.rm_rf(path <> ".retained") end)

    missing = Orchestrator.restore_retained_disposable_claims_for_test(restarted)
    refute missing.retained_claim_journal_ready?
    refute Orchestrator.should_dispatch_issue_for_test(issue, missing)

    File.write!(path, "invalid journal")

    unavailable = Orchestrator.restore_retained_disposable_claims_for_test(missing)
    refute unavailable.retained_claim_journal_ready?
    refute Orchestrator.should_dispatch_issue_for_test(issue, unavailable)
    refute MapSet.member?(unavailable.claimed, @issue_id)

    File.rm!(path)
    File.rename!(path <> ".retained", path)

    lost_fence = %{restarted | execution_fence: %{restarted.execution_fence | executions: %{}}}
    unpaired = Orchestrator.restore_retained_disposable_claims_for_test(lost_fence)
    refute unpaired.retained_claim_journal_ready?
    refute Orchestrator.should_dispatch_issue_for_test(issue, unpaired)

    restored = Orchestrator.restore_retained_disposable_claims_for_test(unavailable)

    assert restored.retained_claim_journal_ready?
    assert MapSet.member?(restored.claimed, @issue_id)
    assert restored.blocked[@issue_id].execution_token == %{issue_id: @issue_id, generation: 1}
    assert restored.blocked[@issue_id].error =~ "rke2job:v1:fixture-allocation"
    assert Orchestrator.restore_retained_disposable_claims_for_test(restored).blocked == restored.blocked

    terminal = %{issue | state: "Done", dispatchable: false}
    terminal_held = Orchestrator.reconcile_blocked_issue_states_for_test([terminal], restored)
    assert MapSet.member?(terminal_held.claimed, @issue_id)
    assert terminal_held.blocked[@issue_id].issue.state == "Done"
    refute_receive {:activation_requested, _, _, _}
  end

  test "poll owns terminal reconciliation only for the exact started disposable Job" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)

    issue = %Issue{
      id: @issue_id,
      identifier: "HGS-349",
      title: "Started disposable assignment",
      state: "Todo",
      assignee_id: "owner",
      dispatchable: true
    }

    context = %{
      adapter: SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter,
      test_pid: self(),
      claim_journal_path: path
    }

    {blocked, _runtime} = post_claim_revalidation_failure(path, issue, issue, disposable_rke2_context: context)

    {:ok, journal} = Journal.load(path)
    [{key, reservation}] = Map.to_list(journal.reservations)
    {:ok, assignment} = ManagedAssignmentBundle.from_snapshot(reservation.assignment_snapshot)

    env = %{
      "SYMPHONY_RKE2_API_SERVER" => "https://10.0.14.10:6443",
      "SYMPHONY_RKE2_CREDENTIAL_ROOT" => "/etc/symphony/frigga-kubernetes",
      "SYMPHONY_RKE2_WORKER_IMAGE" => "ghcr.io/hypergridau/symphony-worker@sha256:" <> String.duplicate("a", 64),
      "SYMPHONY_RKE2_REPOSITORY_ID" => "123456789",
      "SYMPHONY_RKE2_AUTH_SLOT_ID" => "slot-one",
      "SYMPHONY_RKE2_AUTH_CLAIM_NAME" => "codex-oauth-slot-1",
      "SYMPHONY_RKE2_RESULT_JOURNAL_ROOT" => "/private/symphony/job-results"
    }

    {:ok, base} =
      HostAllocationContext.configuration(
        env,
        %{repository_ref: assignment.repository_ref},
        "https://provider.example",
        "host-token"
      )

    slot = %{
      slot_id: base.slot_id,
      claim_name: base.claim_name,
      claim_uid: "pvc-uid-one",
      lease_id: "12345678-1234-4123-8123-123456789abc",
      assignment_sha256: assignment.sha256,
      seat: assignment.seat
    }

    {:ok, expected} =
      JobSpec.compile(assignment, %{
        namespace: "frigga",
        image: base.image,
        repository_id: base.repository_id,
        auth_slot: slot,
        auth_slot_catalog: %{base.slot_id => base.claim_name}
      })

    name = expected["metadata"]["name"]
    uid = "job-uid-one"
    labels = %{"batch.kubernetes.io/controller-uid" => uid, "batch.kubernetes.io/job-name" => name}

    job =
      expected
      |> put_in(["metadata", "uid"], uid)
      |> put_in(["metadata", "labels"], Map.merge(expected["metadata"]["labels"], labels))
      |> put_in(["spec", "selector"], %{"matchLabels" => %{"batch.kubernetes.io/controller-uid" => uid}})
      |> put_in(
        ["spec", "template", "metadata", "labels"],
        Map.merge(expected["spec"]["template"]["metadata"]["labels"], labels)
      )
      |> put_in(["spec", "suspend"], false)

    allocation_id =
      "rke2job:v1:" <>
        Base.url_encode64(Jason.encode!([1, "frigga", name, uid, assignment.sha256]), padding: false)

    reservation = %{
      reservation
      | dispatch: %{reservation.dispatch | phase: "spawn_started", allocation_id: allocation_id}
    }

    {:ok, journal} = Journal.put(journal, key, reservation)
    :ok = Journal.save(path, journal)

    host_config =
      base
      |> Map.put(:slot_guard, SymphonyElixir.RKE2Job.PollSlotGuard)
      |> Map.put(:adapter, SymphonyElixir.RKE2Job.PollTerminalAdapter)
      |> Map.put(:test_pid, self())
      |> Map.put(:client_context_fun, fn _assignment, :allocate, _key, _config -> {:ok, %{synthetic: true}} end)
      |> Map.put(:job_read_fun, fn "frigga", ^name, %{synthetic: true} -> {:ok, Process.get(:poll_retained_job)} end)

    Process.put(:poll_retained_job, job)

    state = %{
      blocked
      | work_package_runtime: Map.put(blocked.work_package_runtime, :disposable_rke2_host_config, host_config)
    }

    restored = Orchestrator.restore_retained_disposable_claims_for_test(state)
    assert_receive {:poll_terminal_finalization, ^allocation_id, assignment_sha256, finalize_key}
    assert assignment_sha256 == assignment.sha256
    assert finalize_key == assignment.sha256 <> ":finalize"
    assert MapSet.member?(restored.claimed, @issue_id)

    Process.put(:poll_retained_job, put_in(job, ["metadata", "uid"], "replacement-uid"))
    assert MapSet.member?(Orchestrator.restore_retained_disposable_claims_for_test(restored).claimed, @issue_id)
    refute_receive {:poll_terminal_finalization, _, _, _}
  end

  test "uncertain RKE2 allocation keeps the local lease and provider claim" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)

    issue = %Issue{
      id: @issue_id,
      identifier: "HGS-349",
      title: "Uncertain disposable allocation",
      state: "Todo",
      assignee_id: "owner",
      dispatchable: true
    }

    context = %{
      adapter: SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter,
      test_pid: self(),
      allocation_result: {:held, :create_response_uncertain}
    }

    {blocked, _runtime} =
      post_claim_revalidation_failure(path, issue, issue, disposable_rke2_context: context)

    assert_receive {:allocation_requested, _key}
    refute_receive {:activation_requested, _, _, _}
    assert blocked.running == %{}
    assert blocked.blocked[@issue_id].error =~ "disposable_rke2_allocation_uncertain"
    assert blocked.execution_fence.executions[@issue_id].leases["worker-349"].status == :active
    assert {:ok, journal} = Journal.load(path)
    [reservation] = Map.values(journal.reservations)
    assert reservation.dispatch.phase == "confirmed"
    assert reservation.reservation_id == "reservation-349"
  end

  test "RKE2 replay retains a journaled allocation without calling the adapter again" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)

    issue = %Issue{
      id: @issue_id,
      identifier: "HGS-349",
      title: "Replayed disposable allocation",
      state: "Todo",
      assignee_id: "owner",
      dispatchable: true
    }

    allocation_id = "rke2job:v1:fixture-allocation"

    before_dispatch = fn input ->
      assert :ok = WorkPackageClaim.record_suspended_allocation(input, %{id: allocation_id, status: :ready})
    end

    context = %{adapter: SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter, test_pid: self()}

    {blocked, _runtime} =
      post_claim_revalidation_failure(path, issue, issue,
        before_dispatch_fun: before_dispatch,
        disposable_rke2_context: context
      )

    refute_receive {:allocation_requested, _key}
    refute_receive {:activation_requested, _, _, _}
    assert blocked.running == %{}
    assert blocked.blocked[@issue_id].error =~ allocation_id
    assert blocked.execution_fence.executions[@issue_id].leases["worker-349"].status == :active
    assert {:ok, journal} = Journal.load(path)
    [reservation] = Map.values(journal.reservations)
    assert reservation.dispatch.phase == "allocation_suspended"
    assert reservation.dispatch.allocation_id == allocation_id
  end

  test "signed dispatch composes trusted host context before suspended allocation" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)

    issue = %Issue{
      id: @issue_id,
      identifier: "HGS-349",
      title: "Host-configured disposable allocation",
      state: "Todo",
      assignee_id: "owner",
      dispatchable: true
    }

    env = %{
      "SYMPHONY_RKE2_API_SERVER" => "https://10.0.14.10:6443",
      "SYMPHONY_RKE2_CREDENTIAL_ROOT" => "/etc/symphony/frigga-kubernetes",
      "SYMPHONY_RKE2_WORKER_IMAGE" => "ghcr.io/hypergridau/symphony-worker@sha256:" <> String.duplicate("a", 64),
      "SYMPHONY_RKE2_REPOSITORY_ID" => "123456789",
      "SYMPHONY_RKE2_AUTH_SLOT_ID" => "slot-one",
      "SYMPHONY_RKE2_AUTH_CLAIM_NAME" => "codex-oauth-slot-1",
      "SYMPHONY_RKE2_RESULT_JOURNAL_ROOT" => "/private/symphony/job-results"
    }

    assert {:ok, config} =
             HostAllocationContext.configuration(env, %{repository_ref: @repository}, "https://provider.example", "host-token")

    caller = self()

    config =
      config
      |> Map.put(:adapter, SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter)
      |> Map.put(:test_pid, caller)
      |> Map.put(:client_context_fun, fn _, _, _, _ -> {:ok, %{synthetic: true}} end)
      |> Map.put(:pvc_read_fun, fn "frigga", "codex-oauth-slot-1", %{synthetic: true} ->
        {:ok,
         %{
           "apiVersion" => "v1",
           "kind" => "PersistentVolumeClaim",
           "metadata" => %{"namespace" => "frigga", "name" => "codex-oauth-slot-1", "uid" => "pvc-uid-one"},
           "status" => %{"phase" => "Bound"}
         }}
      end)
      |> Map.put(:post_fun, fn url, _opts ->
        send(caller, {:slot_reserved, url})

        {:ok,
         %Req.Response{
           status: 200,
           body: %{"data" => %{"slotId" => "slot-one", "claimName" => "codex-oauth-slot-1", "claimUid" => "pvc-uid-one", "leaseId" => "12345678-1234-4123-8123-123456789abc", "replayed" => false}}
         }}
      end)

    {blocked, _runtime} =
      post_claim_revalidation_failure(path, issue, issue, disposable_rke2_host_config: config)

    assert_receive {:slot_reserved, url}
    assert String.ends_with?(url, "/reservation-349/codex-auth-slots/reserve")
    assert_receive {:allocation_requested, _key}
    refute_receive {:activation_requested, _, _, _}
    assert blocked.blocked[@issue_id].error =~ "disposable_rke2_activation_unavailable"
    assert blocked.execution_fence.executions[@issue_id].leases["worker-349"].status == :active
    assert {:ok, journal} = Journal.load(path)
    [reservation] = Map.values(journal.reservations)
    assert reservation.dispatch.phase == "allocation_suspended"

    recovery_path = temp_path()
    on_exit(fn -> File.rm_rf(recovery_path) end)

    {recovery_blocked, _runtime} =
      post_claim_revalidation_failure(recovery_path, issue, issue,
        before_dispatch_fun: fn input -> assert :ok = WorkPackageClaim.begin_paused_recovery(input) end,
        disposable_rke2_host_config: config
      )

    assert recovery_blocked.blocked[@issue_id].error =~ "disposable_rke2_claim_not_admissible"
    refute_receive {:slot_reserved, _url}
    refute_receive {:allocation_requested, _key}

    drift_path = temp_path()
    on_exit(fn -> File.rm_rf(drift_path) end)

    {drift_blocked, _runtime} =
      post_claim_revalidation_failure(drift_path, issue, issue,
        before_dispatch_fun: fn input ->
          assert {:ok, journal} = Journal.load(input.journal_path)
          [{key, reservation}] = Map.to_list(journal.reservations)
          assert :ok = Journal.save(input.journal_path, %{journal | reservations: %{key => %{reservation | reservation_nonce: "changed-nonce"}}})
        end,
        disposable_rke2_host_config: config
      )

    assert drift_blocked.blocked[@issue_id].error =~ "disposable_rke2_claim_not_admissible"
    refute_receive {:slot_reserved, _url}
    refute_receive {:allocation_requested, _key}
  end

  test "paused RKE2 replay retains the exact suspended Job and its execution lease" do
    path = temp_path()
    pause_root = temp_path()
    File.mkdir_p!(pause_root)
    pause_path = Path.join(pause_root, "global-mutable-pause.state")
    File.write!(pause_path, "running\n")
    previous_pause_path = System.get_env("SYMPHONY_GLOBAL_PAUSE_FILE")
    System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", pause_path)

    on_exit(fn ->
      if is_binary(previous_pause_path),
        do: System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", previous_pause_path),
        else: System.delete_env("SYMPHONY_GLOBAL_PAUSE_FILE")

      File.rm_rf(path)
      File.rm_rf(pause_root)
    end)

    issue = %Issue{
      id: @issue_id,
      identifier: "HGS-349",
      title: "Suspended assignment",
      state: "Todo",
      assignee_id: "owner",
      dispatchable: true
    }

    allocation_id = "rke2job:v1:fixture-allocation"

    before_dispatch = fn input ->
      assert :ok = WorkPackageClaim.record_suspended_allocation(input, %{id: allocation_id, status: :ready})
      File.write!(pause_path, "paused\n")
    end

    context = %{adapter: SymphonyElixir.RKE2Job.SuspendedControllerFakeAdapter, test_pid: self()}

    {blocked, runtime} =
      post_claim_revalidation_failure(path, issue, issue,
        before_dispatch_fun: before_dispatch,
        disposable_rke2_context: context
      )

    assert blocked.running == %{}
    refute_receive {:allocation_requested, _key}
    assert blocked.blocked[@issue_id].error =~ "suspended_job_retained"
    assert blocked.execution_fence.executions[@issue_id].leases["worker-349"].status == :active
    assert {:ok, journal} = Journal.load(path)
    [retained] = Map.values(journal.reservations)
    assert retained.dispatch.phase == "allocation_suspended"
    assert retained.dispatch.allocation_id == allocation_id
    assert retained.reservation_id == "reservation-349"

    assert {:ok, restarted_fence} =
             ExecutionFence.mark_unreconciled_after_restart(blocked.execution_fence)

    assert {:ok, restarted_graph} =
             ResponsibilityGraph.mark_unreconciled_after_restart(blocked.responsibility_graph)

    assert {:error, :managed_responsibility_requires_enforcement} =
             Recovery.prepare(runtime, restarted_fence, restarted_graph, issue, nil, System.system_time(:millisecond))
  end

  test "managed spawn rejects a claim for another issue before starting a worker" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Signed objective", state: "Todo", assignee_id: "owner", dispatchable: true}
    wrong_issue_claim = fn claim -> put_in(claim, [:reservation, :issue_id], "another-issue") end

    {after_preflight, _runtime} =
      post_claim_revalidation_failure(path, issue, issue, provider_claim_override: wrong_issue_claim)

    assert after_preflight.running == %{}
    assert after_preflight.blocked[@issue_id].error =~ "provider_claim_binding_failed"
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
  end

  test "managed spawn rejects a claim for another runner before starting a worker" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Signed objective", state: "Todo", assignee_id: "owner", dispatchable: true}

    wrong_runner_claim = fn claim ->
      claim
      |> put_in([:reservation, :runner_id], "another-runner")
      |> put_in([:attestation, :runner_id], "another-runner")
    end

    {after_preflight, _runtime} =
      post_claim_revalidation_failure(path, issue, issue, provider_claim_override: wrong_runner_claim)

    assert after_preflight.running == %{}
    assert after_preflight.blocked[@issue_id].error =~ "provider_claim_binding_failed"
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
  end

  test "managed bundle rebuild failure after claim also closes replay before blocking" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Signed objective", state: "Todo", assignee_id: "owner"}

    {after_preflight, _runtime} =
      post_claim_revalidation_failure(path, issue, issue,
        manifest_expiry_ms: System.system_time(:millisecond) - 1,
        spawn_directly: true
      )

    assert Map.has_key?(after_preflight.blocked, @issue_id)
    assert after_preflight.running == %{}
    assert after_preflight.execution_fence.executions[@issue_id].leases["worker-349"].status == :released
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
  end

  test "an expired graph lease at the final claim fence enters recovery before worker start" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Signed objective", state: "Todo", assignee_id: "owner"}

    {after_preflight, _runtime} =
      post_claim_revalidation_failure(path, issue, issue,
        graph_expiry_ms: System.system_time(:millisecond) - 1,
        spawn_directly: true
      )

    assert Map.has_key?(after_preflight.blocked, @issue_id)
    assert after_preflight.running == %{}
    assert after_preflight.execution_fence.executions[@issue_id].leases["worker-349"].status == :released
    assert after_preflight.responsibility_graph.delegations["delegation-349"].runtime_lease == nil
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
  end

  test "expiry after local spawn_started fences the claim before a root witness" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    %{input: input} = authority_fixture(path)
    expiry = input.responsibility_graph.delegations["delegation-349"].expires_at_ms
    parent = self()

    input = %{
      input
      | host_witness_fun: fn request ->
          send(parent, {:root_witness, request["operation"]})

          {:ok,
           %{
             "ok" => true,
             "receipt" => %{
               "version" => 1,
               "sequence" => 1,
               "hash" => String.duplicate("a", 64),
               "replayed" => false
             }
           }}
        end
    }

    request_fun = fn url, _options ->
      payload =
        if String.ends_with?(url, "/reservations/by-issue"),
          do: reservation_payload(),
          else: claim_result_payload()

      {:ok, response(%{"data" => payload})}
    end

    assert {:ok, _claim} =
             WorkPackageClaim.claim(input,
               request_fun: request_fun,
               now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end
             )

    assert {:error, {:pre_spawn_recovery_pending, _reason}} =
             WorkPackageClaim.begin_spawn(input, now_fun: spawn_expiry_clock(expiry))

    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
    refute_receive {:root_witness, "spawn_intent"}
  end

  test "root pause denial after spawn_started starts no child and releases the local lease" do
    path = temp_path()
    on_exit(fn -> File.rm_rf(path) end)
    issue = %Issue{id: @issue_id, identifier: "HGS-349", title: "Signed objective", state: "Todo", assignee_id: "owner", dispatchable: true}

    {after_preflight, runtime} =
      post_claim_revalidation_failure(path, issue, issue,
        spawn_directly: true,
        witness_pause_rejection: "global admission paused"
      )

    assert after_preflight.running == %{}
    assert Map.has_key?(after_preflight.blocked, @issue_id)
    assert after_preflight.execution_fence.executions[@issue_id].leases["worker-349"].status == :released
    assert after_preflight.execution_fence.executions[@issue_id].leases["worker-349"].release_reason == :spawn_failed
    assert after_preflight.responsibility_graph.delegations["delegation-349"].runtime_lease == nil
    assert {:ok, journal} = Journal.load(path)
    [{_key, reservation}] = Map.to_list(journal.reservations)
    assert reservation.dispatch.phase == "recovery_pending"
    assert {:ok, [retained]} = Recovery.unstarted_claims(runtime, after_preflight.execution_fence)
    assert retained.dispatch.phase == "recovery_pending"
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

  defp suspended_assignment(lease) do
    {:ok, assignment} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-349", identity: "objective-349", content: "Run one bounded disposable assignment"},
        repository_ref: @repository,
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs349-disposable",
        seat: "runner-349",
        lease: lease,
        intent_ancestry: ["owner", "delegation-349"],
        acceptance: %{deliverable: "Disposable Job", evidence: "Exact claim and UID"},
        context_secret_refs: [],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    assignment
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
      pool_key: "midgard",
      host_witness_fun: fn _request ->
        {:ok, %{"ok" => true, "receipt" => %{"version" => 1, "sequence" => 1, "hash" => String.duplicate("a", 64), "replayed" => false}}}
      end,
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
      "workspaceId" => "workspace-349",
      "companyId" => "company-349",
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

  defp assignment_manifest(issue) do
    %{
      schema_version: 2,
      repository_ref: @repository,
      entries: [
        %{
          issue_id: issue.id,
          identifier: issue.identifier,
          owner_id: issue.assignee_id,
          accountable: %{expires_at_ms: 2_000_000_000_000},
          responsible: %{
            id: "delegation-349",
            expires_at_ms: 2_000_000_000_000,
            scope: %{objective_id: "objective", repository: @repository}
          },
          assignment_context: %{
            objective_id: "objective",
            objective_content: issue.title,
            base_ref: "refs/remotes/origin/main",
            platform: "linux-x86_64",
            environment_classification: "repository",
            environment_constraints: ["repository"],
            placement: :internal_beta,
            target_environment: :rke2
          }
        }
      ]
    }
  end

  defp with_assignment_manifest(input, issue) do
    graph =
      input.responsibility_graph
      |> put_in([:delegations, "owner", :scope, :work_package_id], "projection-349")
      |> put_in([:delegations, "delegation-349", :scope, :work_package_id], "projection-349")

    input
    |> Map.put(:managed_delegations, assignment_manifest(issue))
    |> Map.put(:responsibility_graph, graph)
  end

  defp post_claim_revalidation_failure(path, issue, refreshed_issue, opts \\ []) do
    %{input: input, token: token, lease: lease} = authority_fixture(path)
    input = with_assignment_manifest(input, issue)
    input = expire_assignment_manifest(input, Keyword.get(opts, :manifest_expiry_ms))

    runtime =
      input
      |> Map.take([:base_url, :runner_token, :attestation_key, :runner_id, :managed_project_profile_id, :journal_path, :pool_key, :host_witness_fun, :managed_delegations])
      |> Map.merge(Map.new(Keyword.take(opts, [:disposable_rke2_context, :disposable_rke2_host_config])))

    fence_path = path <> ".fence"
    graph_path = path <> ".graph"
    :ok = ExecutionFence.Persistence.save(fence_path, input.fence_state)
    :ok = ResponsibilityGraph.Persistence.save(graph_path, input.responsibility_graph)

    state = %Orchestrator.State{
      execution_fence: input.fence_state,
      responsibility_graph: input.responsibility_graph,
      work_package_runtime: runtime,
      execution_fence_path: fence_path,
      responsibility_graph_path: graph_path
    }

    request_fun = fn url, _options ->
      if String.ends_with?(url, "/reservations/by-issue"),
        do: {:ok, response(%{"data" => reservation_payload()})},
        else: {:ok, response(%{"data" => claim_result_payload()})}
    end

    assert {:ok, claim} = WorkPackageClaim.claim(input, request_fun: request_fun, now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end)

    if before_dispatch = Keyword.get(opts, :before_dispatch_fun), do: before_dispatch.(input)

    state = maybe_reject_spawn_witness(state, opts)

    state = expire_runtime_delegations(state, graph_path, Keyword.get(opts, :graph_expiry_ms))

    provider_claim =
      case Keyword.get(opts, :provider_claim_override, claim) do
        override when is_function(override, 1) -> override.(claim)
        override -> override
      end

    dispatch = %{
      attempt: nil,
      recipient: self(),
      worker_host: nil,
      token: token,
      session_id: lease.session_id,
      delegation_id: "delegation-349",
      runtime_lease: lease,
      provider_claim: provider_claim
    }

    after_preflight =
      if Keyword.get(opts, :spawn_directly, false) do
        Orchestrator.spawn_fenced_issue_for_test(
          state,
          issue,
          token,
          lease.session_id,
          "delegation-349",
          lease
        )
      else
        Orchestrator.spawn_claimed_issue_for_test(state, issue, dispatch, fn [@issue_id] ->
          {:ok, [refreshed_issue]}
        end)
      end

    {after_preflight, runtime}
  end

  defp expire_assignment_manifest(input, expiry) when is_integer(expiry) do
    [entry] = input.managed_delegations.entries

    manifest = %{
      input.managed_delegations
      | entries: [
          %{
            entry
            | accountable: %{entry.accountable | expires_at_ms: expiry},
              responsible: %{entry.responsible | expires_at_ms: expiry}
          }
        ]
    }

    %{input | managed_delegations: manifest}
  end

  defp expire_assignment_manifest(input, _expiry), do: input

  defp maybe_reject_spawn_witness(state, opts) do
    case Keyword.get(opts, :witness_pause_rejection) do
      reason when reason in ["global admission paused", "global pause transition active"] ->
        witness = fn %{"operation" => "spawn_intent"} ->
          {:ok, %{"ok" => false, "error" => reason}}
        end

        %{state | work_package_runtime: Map.put(state.work_package_runtime, :host_witness_fun, witness)}

      _ ->
        state
    end
  end

  defp spawn_expiry_clock(expiry) do
    times = :atomics.new(1, [])

    fn ->
      case :atomics.add_get(times, 1, 1) do
        1 -> DateTime.from_unix!(expiry - 1, :millisecond)
        _ -> DateTime.from_unix!(expiry + 1, :millisecond)
      end
    end
  end

  defp expire_runtime_delegations(state, _graph_path, nil), do: state

  defp expire_runtime_delegations(state, graph_path, expires_at_ms) when is_integer(expires_at_ms) do
    delegations =
      Enum.reduce(["owner", "delegation-349"], state.responsibility_graph.delegations, fn id, acc ->
        Map.update!(acc, id, &Map.put(&1, :expires_at_ms, expires_at_ms))
      end)

    graph = %{state.responsibility_graph | delegations: delegations}
    :ok = ResponsibilityGraph.Persistence.save(graph_path, graph)
    %{state | responsibility_graph: graph}
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
