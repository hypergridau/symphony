Code.require_file("../support/rke2_job_fake_client.exs", __DIR__)

defmodule SymphonyElixir.RKE2Job.PreSpawnAbortControllerTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.{ExecutionFence, ManagedAssignmentBundle, Orchestrator, ResponsibilityGraph, WorkPackageClaim}
  alias SymphonyElixir.ExecutionFence.Persistence
  alias SymphonyElixir.GlobalPause
  alias SymphonyElixir.ManagedExecutor.ClaimBinding
  alias SymphonyElixir.ManagedExecutor.Record
  alias SymphonyElixir.RKE2Job.{HostAllocationContext, JobSpec, PreSpawnAbortController, SuspendedController}
  alias SymphonyElixir.RKE2Job.ManagedExecutorAdapter
  alias SymphonyElixir.Tracker.Issue
  alias SymphonyElixir.WorkPackageClaim.{Dispatch, Journal, Recovery}

  @issue "11111111-2222-4333-8444-555555555599"
  @profile "profile-abort-controller"
  @repo "hypergridau/symphony"
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

    def publish(request) do
      owner = :persistent_term.get({__MODULE__, :owner}, self())
      send(owner, {:root_abort_inputs, request})
      record_lifecycle_event(:root_abort_inputs)
      :ok
    end

    def verify_disposal(request, expected) do
      owner = :persistent_term.get({__MODULE__, :owner}, self())
      send(owner, {:root_disposal_verification, request, expected})
      record_lifecycle_event(:root_disposal_verification)

      response =
        request
        |> Map.take(~w(claimSHA256 assignmentDigest allocationId resultReference))
        |> Map.merge(expected)
        |> Map.put("status", "pre-execution-abort-disposal-verified")
        |> Map.put("observedAt", DateTime.utc_now() |> DateTime.to_iso8601())

      {:ok, response}
    end

    defp record_lifecycle_event(event) do
      Process.put(:abort_lifecycle_events, Process.get(:abort_lifecycle_events, []) ++ [event])
    end
  end

  defmodule Activation do
    def activate_owned(allocation, _assignment, _key, owner) do
      send(owner, {:activated, allocation.id})
      {:ok, %{status: :active}}
    end
  end

  defmodule AllocationAdapter do
    alias SymphonyElixir.RKE2Job.JobSpec

    def allocate_or_reconcile(assignment, _key, context) do
      {:ok, expected} = JobSpec.compile(assignment, context.config)
      name = get_in(expected, ["metadata", "name"])
      digest = get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-sha256"])
      uid = "uid-" <> String.slice(name, -8, 8)
      encoded = Jason.encode!([1, "frigga", name, uid, digest]) |> Base.url_encode64(padding: false)
      {:ok, %{id: "rke2job:v1:" <> encoded, status: :ready}}
    end

    def activate_owned(_allocation, _assignment, _key, _owner), do: {:error, :unexpected_activation}
  end

  defmodule FakeClientContext do
    def client_context(_assignment, _operation, _key, context), do: {:ok, context}
  end

  defmodule FakeSlotGuard do
    def verify_claim_uid(_slot, _context) do
      send(:persistent_term.get({__MODULE__, :owner}), :slot_claim_uid_verified)
      :ok
    end

    def verify_bound(_slot, _assignment, _allocation, _context) do
      send(:persistent_term.get({__MODULE__, :owner}), :slot_binding_verified)
      :ok
    end
  end

  defmodule AbortClient do
    def create_job(_namespace, _job, _context), do: {:error, :unexpected}
    def list_pods(_namespace, _context), do: {:error, :unexpected}
    def activate_job(_namespace, _name, _uid, _version, _context), do: {:error, :unexpected}
    def delete_job(_namespace, _name, _uid, _context), do: {:error, :unexpected}

    def get_job(namespace, name, context) do
      send(:persistent_term.get({__MODULE__, :owner}), :abort_job_read)
      record_lifecycle_event(:worker_read)
      SymphonyElixir.RKE2JobFakeClient.get_job(namespace, name, context.agent)
    end

    def list_pods_snapshot(namespace, context) do
      record_lifecycle_event(:pod_snapshot)
      SymphonyElixir.RKE2JobFakeClient.list_pods_snapshot(namespace, context.agent)
    end

    def delete_suspended_job(namespace, name, uid, resource_version, context) do
      send(:persistent_term.get({__MODULE__, :owner}), :abort_job_delete)
      record_lifecycle_event(:worker_delete)
      SymphonyElixir.RKE2JobFakeClient.delete_suspended_job(namespace, name, uid, resource_version, context.agent)
    end

    defp record_lifecycle_event(event) do
      Process.put(:abort_lifecycle_events, Process.get(:abort_lifecycle_events, []) ++ [event])
    end
  end

  test "typed denial checkpoints an abort while ordinary preflight success stays suspended" do
    fixture = fixture()
    success = controller_context(fixture, :ok)

    assert {:ok, :ready} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, success)
    assert_receive {:preflight, :ok}
    refute_receive {:root_eligibility, _}
    allocation_id = fixture.allocation_id

    assert {:ok, %{phase: "allocation_suspended", allocation_id: ^allocation_id}} =
             WorkPackageClaim.handoff_allocation(fixture.input)

    denied = controller_context(fixture, {:denied, :codex_auth_slot_denied})
    root_result(self(), :ok)

    assert {:abort, reservation} =
             PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, denied)

    assert reservation.dispatch.phase == "abort_pending"
    assert reservation.dispatch.abort_reason == "codex_auth_slot_denied"
    assert reservation.dispatch.allocation_id == fixture.allocation_id
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

      assert {:ok, %{phase: "allocation_suspended", allocation_id: allocation_id}} =
               WorkPackageClaim.handoff_allocation(fixture.input)

      assert allocation_id == fixture.allocation_id
      assert File.ls!(fixture.result_root) == []
      refute_receive {:root_abort_inputs, _}
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
    assert File.ls!(fixture.result_root) == []
    refute_receive {:root_abort_inputs, _}
  end

  test "abort survives journal reload and blocks spawn intent and controller resume" do
    fixture = fixture()
    root_result(self(), :ok)
    context = controller_context(fixture, {:denied, :codex_auth_slot_denied})

    assert {:abort, first} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
    assert_receive {:preflight, {:denied, :codex_auth_slot_denied}}
    assert {:ok, reloaded} = Journal.load(fixture.input.journal_path)
    key = Journal.reservation_key(@issue, @profile, @repo, 1)
    retained = reloaded.reservations[key]
    assert retained.dispatch == first.dispatch
    assert retained.dispatch.allocation_id == fixture.allocation_id
    assert retained.dispatch.abort_reason == "codex_auth_slot_denied"

    assert {:error, :invalid_claim_dispatch_transition} =
             WorkPackageClaim.begin_suspended_spawn(fixture.input, fixture.allocation_id)

    assert {:held, :pre_spawn_abort_pending} =
             SuspendedController.resume(fixture.assignment, fixture.input, Map.put(context, :adapter, Activation))

    refute_receive {:activated, _}

    assert {:abort, second} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
    assert second.dispatch == first.dispatch

    expected_result = %{
      assignment_digest: fixture.assignment.sha256,
      abort_reason: :codex_auth_slot_denied,
      outcome: :blocked,
      summary: Record.pre_execution_summary(:codex_auth_slot_denied),
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

    tampered_json =
      put_in(decoded, ["reservations", key, "dispatch", "abort_config", "runner_token"], "must-not-persist")

    assert {:error, _} = Journal.decode_bytes(Jason.encode!(tampered_json))
  end

  test "typed denial publishes result, persists exact lease release, then prepares deletes and publishes root inputs" do
    fixture = fixture()
    root_result(self(), :ok)
    :persistent_term.put({FakeSlotGuard, :owner}, self())
    :persistent_term.put({AbortClient, :owner}, self())
    Process.put(:abort_lifecycle_events, [])

    {:ok, client_agent} =
      Agent.start_link(fn ->
        %{
          jobs: %{},
          pods: %{},
          creates: 0,
          deletes: [],
          create_error: nil,
          create_commit?: true,
          get_error: nil,
          delete_error: nil,
          delete_commit?: false,
          pod_list_resource_version: "pod-list-before-delete",
          event_sink: self()
        }
      end)

    {:ok, _job} = SymphonyElixir.RKE2JobFakeClient.create_job("frigga", expected_job(fixture.assignment), client_agent)
    kube_context_fun = fn _assignment, _operation, _key, _context -> {:ok, %{agent: client_agent}} end
    host = Map.put(fixture.host, :client_context_fun, kube_context_fun)
    context = controller_context(fixture, {:denied, :codex_auth_slot_denied})
    assert {:abort, reservation} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)

    assert :ok = PreSpawnAbortController.publish(reservation, fixture.assignment, context)
    assert :ok = PreSpawnAbortController.publish(reservation, fixture.assignment, context)
    assert :ok = Persistence.save(fixture.fence_path, fixture.input.fence_state)

    {:ok, released, _receipt} =
      ExecutionFence.release(
        fixture.input.fence_state,
        %{issue_id: @issue, generation: fixture.assignment.lease.generation},
        fixture.assignment.lease.session_id,
        :spawn_failed
      )

    assert :ok = Persistence.save(fixture.fence_path, released)
    assert {:ok, reread_fence} = Persistence.load(fixture.fence_path)
    assert Persistence.encode_bytes(reread_fence) == Persistence.encode_bytes(released)
    assert reread_fence.sessions[fixture.assignment.lease.session_id].release_reason == "spawn_failed"
    released_input = %{fixture.input | fence_state: reread_fence}
    assert :ok = PreSpawnAbortController.unused_worker(released_input, fixture.assignment, reservation, true)

    assert {:ok, host_context} =
             HostAllocationContext.reattach_abort(
               fixture.assignment,
               fixture.binding,
               reservation.dispatch,
               host
             )

    assert_receive :slot_claim_uid_verified
    assert_receive :slot_binding_verified

    adapter_context =
      host_context
      |> Map.merge(%{
        client: AbortClient,
        client_context_provider: FakeClientContext,
        client_context_provider_context: %{agent: client_agent},
        auth_slot_lease_guard: FakeSlotGuard,
        auth_slot_lease_guard_context: nil,
        abort_result_journal_root: fixture.result_root
      })

    post_fun = fn _url, options ->
      request = Jason.decode!(Keyword.fetch!(options, :body))
      send(self(), {:provider_prepare, request["prepareId"], request["allocationId"]})
      Process.put(:abort_lifecycle_events, Process.get(:abort_lifecycle_events, []) ++ [:provider_prepare])

      data = %{
        "prepareId" => request["prepareId"],
        "projectionId" => reservation.projection_id,
        "reservationId" => reservation.reservation_id,
        "preparedAt" => "2026-10-05T10:00:00Z",
        "replayed" => false
      }

      {:ok, %Req.Response{status: 200, body: %{"data" => data}}}
    end

    caller_ports = %{
      adapter_context: adapter_context,
      witness_input: released_input,
      reservation: reservation,
      provider_context: %{base_url: "https://provider.test", runner_token: "synthetic-runner-token"},
      journal_root: host.abort_journal_root,
      workspace_root: host.workspace_root,
      post_fun: post_fun,
      root_abort_input_publisher: Root
    }

    context = Map.put(context, :abort_caller_test_ports, caller_ports)

    cleanup_result =
      PreSpawnAbortController.cleanup(
        reservation,
        fixture.assignment,
        released_input,
        context,
        host
      )

    assert :ok = cleanup_result

    assert_receive :abort_job_read
    assert_receive {:provider_prepare, _prepare_id, allocation_id}
    assert allocation_id == fixture.allocation_id
    assert_receive {:root_prepare_intent, _prepare_id, request_hash}
    assert byte_size(request_hash) == 64
    assert_receive {:root_disposal_verification, disposal_request, disposal_expected}
    assert disposal_request["operation"] == "verify_pre_execution_abort_disposal"
    assert map_size(disposal_expected) == 3
    assert_receive :abort_job_delete
    assert_receive {:root_abort_inputs, %{"operation" => "publish_pre_execution_abort_inputs"}}
    events = Process.get(:abort_lifecycle_events)
    disposal_index = Enum.find_index(events, &(&1 == :root_disposal_verification))
    delete_index = Enum.find_index(events, &(&1 == :worker_delete))
    assert is_integer(disposal_index) and is_integer(delete_index)
    assert disposal_index < delete_index
    assert Agent.get(client_agent, &map_size(&1.jobs)) == 0
    assert :ok = PreSpawnAbortController.unused_worker(released_input, fixture.assignment, reservation, true)
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
      input =
        fixture.input
        |> update_in([:fence_state, :executions, @issue, :leases, "worker-abort-1"], change)
        |> update_in([:fence_state, :sessions, "worker-abort-1"], change)

      assert {:held, :pre_spawn_worker_state_unverified} =
               PreSpawnAbortController.unused_worker(input, fixture.assignment, reservation, false)
    end
  end

  test "abort cannot replace an already persisted spawn intent" do
    fixture = fixture()
    assert :ok = WorkPackageClaim.begin_suspended_spawn(fixture.input, fixture.allocation_id)
    context = controller_context(fixture, {:denied, :codex_auth_slot_denied})

    assert {:ok, :ready} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
    refute_receive {:preflight, _}
    refute_receive {:root_eligibility, _}
    assert_receive {:root_spawn, "spawn_intent"}

    assert {:ok, %{phase: "spawn_started", allocation_id: allocation_id}} =
             WorkPackageClaim.handoff_allocation(fixture.input)

    assert allocation_id == fixture.allocation_id
    {:ok, journal} = Journal.load(fixture.input.journal_path)
    key = Journal.reservation_key(@issue, @profile, @repo, 1)

    assert {:error, :pre_spawn_abort_not_admissible} =
             Dispatch.begin_suspended_abort(
               journal,
               key,
               fixture.input,
               fixture.allocation_id,
               config(fixture.assignment)
             )
  end

  test "restart preserves a persisted abort lease until root eligibility re-verifies ownership" do
    fixture = fixture()
    root_result(self(), :ok)
    context = controller_context(fixture, {:denied, :codex_auth_slot_denied})
    assert {:abort, _} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
    assert_receive {:root_eligibility, %{"operation" => "verify_pre_execution_abort_eligibility"}}

    assert :ok = Persistence.save(fixture.fence_path, fixture.input.fence_state)
    assert {:ok, persisted} = Persistence.load(fixture.fence_path)
    assert {:ok, restarted} = ExecutionFence.mark_unreconciled_after_restart(persisted)
    assert restarted.executions[@issue].ownership == :unknown
    runtime = %{journal_path: fixture.input.journal_path}
    assert {:ok, [reservation]} = Recovery.unstarted_claims(runtime, restarted)
    assert reservation.dispatch.phase == "abort_pending"

    assert {:ok, preserved, _summary} =
             ExecutionFence.reconcile_claim_sessions(
               restarted,
               [],
               [reservation],
               2_000_000_000_000,
               1
             )

    assert preserved.executions[@issue].ownership == :unknown
    assert preserved.executions[@issue].status == :active
    assert preserved.sessions[fixture.assignment.lease.session_id].status == :active

    restored_state =
      Orchestrator.restore_retained_disposable_claims_for_test(%Orchestrator.State{
        work_package_runtime: Map.merge(runtime, %{runner_id: "runner-abort-1", managed_project_profile_id: @profile}),
        execution_fence: preserved
      })

    assert Map.has_key?(restored_state.blocked, @issue)
    refute Map.has_key?(restored_state.running, @issue)

    reconciliation_input = %{fixture.input | fence_state: restarted}

    assert {:ok, candidate} =
             PreSpawnAbortController.reconcile_ownership(reservation, fixture.assignment, reconciliation_input, context)

    assert candidate.executions[@issue].ownership == :reconciled
    assert_receive {:root_eligibility, %{"operation" => "verify_pre_execution_abort_eligibility"}}

    unsafe =
      restarted
      |> update_in(
        [:executions, @issue, :leases, fixture.assignment.lease.session_id],
        &Map.put(&1, :head, "observed-head")
      )
      |> update_in([:sessions, fixture.assignment.lease.session_id], &Map.put(&1, :head, "observed-head"))

    root_result(self(), :ok)

    assert {:held, _} =
             PreSpawnAbortController.reconcile_ownership(
               reservation,
               fixture.assignment,
               %{fixture.input | fence_state: unsafe},
               context
             )

    refute_receive {:root_eligibility, _}
  end

  test "retained abort cleanup routing survives terminal, reassigned, and non-active issues while globally paused" do
    previous_pause_path = System.get_env("SYMPHONY_GLOBAL_PAUSE_FILE")
    pause_root = Path.join(System.tmp_dir!(), "pre-spawn-abort-pause-#{System.unique_integer([:positive])}")
    File.mkdir_p!(pause_root)
    pause_path = Path.join(pause_root, "global-mutable-pause.state")
    File.write!(pause_path, "paused\n")
    System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", pause_path)

    on_exit(fn ->
      if is_binary(previous_pause_path) do
        System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", previous_pause_path)
      else
        System.delete_env("SYMPHONY_GLOBAL_PAUSE_FILE")
      end

      File.rm_rf(pause_root)
    end)

    assert GlobalPause.snapshot().paused?

    for {state, assignee, missing?} <- [
          {"Done", "owner", false},
          {"In Progress", "new-owner", false},
          {"Backlog", "owner", false},
          {"In Progress", "owner", true}
        ] do
      fixture = fixture()
      root_result(self(), :ok)
      :persistent_term.put({FakeSlotGuard, :owner}, self())
      context = controller_context(fixture, {:denied, :codex_auth_slot_denied})
      assert {:abort, _} = PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
      assert :ok = Persistence.save(fixture.fence_path, fixture.input.fence_state)

      issue = %Issue{
        id: @issue,
        identifier: "HGS-ABORT",
        title: "retained abort",
        state: state,
        assignee_id: assignee,
        dispatchable: true
      }

      blocked_entry = %{
        issue: issue,
        identifier: issue.identifier,
        execution_token: %{issue_id: @issue, generation: 1},
        error: "previous hold"
      }

      runtime = %{
        journal_path: fixture.input.journal_path,
        runner_id: "runner-abort-1",
        managed_project_profile_id: @profile,
        disposable_rke2_host_config: Map.put(fixture.host, :abort_journal_root, nil)
      }

      orchestrator_state = %Orchestrator.State{
        work_package_runtime: runtime,
        execution_fence: fixture.input.fence_state,
        execution_fence_path: fixture.fence_path,
        blocked: %{@issue => blocked_entry},
        claimed: MapSet.new([@issue])
      }

      resumed =
        if missing?,
          do: Orchestrator.reconcile_missing_blocked_issue_for_test(orchestrator_state, @issue),
          else: Orchestrator.reconcile_blocked_issue_states_for_test([issue], orchestrator_state)

      assert_receive :slot_claim_uid_verified
      assert_receive :slot_binding_verified
      assert resumed.execution_fence.executions[@issue].leases[fixture.assignment.lease.session_id].status == :active
      assert resumed.blocked[@issue].error =~ "pre_spawn_abort_ownership_unverified"
      refute_receive :abort_job_read
      refute_receive :provider_prepare
      refute_receive :abort_job_delete
    end
  end

  test "concurrent spawn and typed abort are serialized by the shared claim journal" do
    fixture = fixture()
    root_result(self(), :ok)
    context = controller_context(fixture, {:denied, :codex_auth_slot_denied})
    parent = self()

    spawn_task =
      Task.async(fn ->
        send(parent, :spawn_ready)

        receive do
          :go -> WorkPackageClaim.begin_suspended_spawn(fixture.input, fixture.allocation_id)
        end
      end)

    abort_task =
      Task.async(fn ->
        send(parent, :abort_ready)

        receive do
          :go -> PreSpawnAbortController.before_resume(fixture.assignment, fixture.input, context)
        end
      end)

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
    refute spawn_result == :ok and match?({:abort, _}, abort_result)
    refute phase == "abort_pending" and spawn_result == :ok
  end

  defp fixture do
    path = Path.join(System.tmp_dir!(), "pre-spawn-abort-#{System.unique_integer([:positive])}.json")
    fence_path = path <> ".fence"
    result_root = Path.join(File.cwd!(), ".tmp-pre-spawn-abort-results-#{System.unique_integer([:positive])}")
    abort_root = Path.join(File.cwd!(), ".tmp-pre-spawn-abort-journal-#{System.unique_integer([:positive])}")
    workspace_root = Path.join(File.cwd!(), ".tmp-pre-spawn-abort-workspace-#{System.unique_integer([:positive])}")
    credential_root = Path.join(File.cwd!(), ".tmp-pre-spawn-abort-creds-#{System.unique_integer([:positive])}")
    Enum.each([result_root, abort_root, workspace_root, credential_root], &private_dir!/1)

    on_exit(fn ->
      File.rm(path)
      File.rm(path <> ".lock")
      File.rm(path <> ".tampered")
      File.rm(fence_path)
      File.rm_rf(result_root)
      File.rm_rf(abort_root)
      File.rm_rf(workspace_root)
      File.rm_rf(credential_root)
    end)

    assignment = assignment()
    input = claim_input(path)

    request_fun = fn url, _options ->
      payload =
        if String.ends_with?(url, "/reservations/by-issue"),
          do: reservation_payload(),
          else: claim_result_payload()

      {:ok, %Req.Response{status: 200, body: %{"data" => payload}}}
    end

    assert {:ok, claim} =
             WorkPackageClaim.claim(input,
               request_fun: request_fun,
               now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end
             )

    {:ok, binding} = ClaimBinding.from_claim(claim, assignment, "runner-abort-1")
    context = %{adapter: AllocationAdapter, config: config(assignment), claim_binding: binding}
    assert {:ok, %{id: allocation_id, status: :ready}} = SuspendedController.allocate(assignment, input, context)
    {:ok, journal} = Journal.load(path)
    key = Journal.reservation_key(@issue, @profile, @repo, 1)
    reservation = journal.reservations[key]
    assert reservation.assignment_snapshot == elem(ManagedAssignmentBundle.snapshot(assignment), 1)
    input = Map.put(input, :claim_binding, binding)
    host = host_config(assignment, result_root, abort_root, workspace_root, credential_root)

    %{
      input: input,
      assignment: assignment,
      binding: binding,
      allocation_id: allocation_id,
      host: host,
      result_root: result_root,
      fence_path: fence_path
    }
  end

  defp controller_context(fixture, preflight_result) do
    %{
      adapter: Preflight,
      config: config(fixture.assignment),
      claim_binding: fixture.binding,
      root_abort_input_publisher: Root,
      abort_result_journal_root: fixture.result_root,
      abort_caller_test_ports: %{},
      preflight_context: {self(), preflight_result}
    }
  end

  defp root_result(owner, result) do
    :persistent_term.put({Root, :owner}, owner)
    :persistent_term.put({Root, :result}, result)
  end

  defp config(assignment) do
    slot = %{
      slot_id: "slot-abort",
      lease_id: "12345678-1234-4123-8123-123456789abc",
      claim_name: "claim-abort",
      claim_uid: "uid-abort",
      assignment_sha256: assignment.sha256,
      binding_sha256: @digest,
      seat: assignment.seat
    }

    %{
      namespace: "frigga",
      image: "registry.example/worker@sha256:" <> String.duplicate("a", 64),
      repository_id: "123456789",
      auth_slot: slot,
      auth_slot_catalog: %{slot.slot_id => slot.claim_name},
      assignment_binding_digest: @digest
    }
  end

  defp assignment do
    {:ok, assignment} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-abort", identity: "objective-abort", content: "one task"},
        repository_ref: @repo,
        base_ref: "refs/remotes/origin/main",
        branch: "codex/pre-spawn-abort",
        seat: "runner-abort-1",
        lease: %{
          issue_id: @issue,
          repository: @repo,
          generation: 1,
          session_id: "worker-abort-1",
          process_id: "process-abort-1"
        },
        intent_ancestry: ["owner", "delegation-abort"],
        acceptance: %{deliverable: "abort regression", evidence: "synthetic"},
        context_secret_refs: [],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    assignment
  end

  defp claim_input(path) do
    fence = ExecutionFence.new()

    admission = %{
      issue_id: @issue,
      repository: @repo,
      branch: "codex/pre-spawn-abort",
      worktree: "tmp"
    }

    {:ok, admitted, token} = ExecutionFence.admit(fence, admission, 0)

    {:ok, fence_state, :registered} =
      ExecutionFence.register(
        admitted,
        token,
        :worker,
        %{
          session_id: "worker-abort-1",
          process_id: "process-abort-1",
          branch: "codex/pre-spawn-abort",
          worktree: "tmp",
          linear_state: "In Progress",
          pr_state: "none",
          head: "unobserved",
          last_heartbeat_at: 0
        },
        0
      )

    lease = %{
      issue_id: @issue,
      repository: @repo,
      generation: 1,
      session_id: "worker-abort-1",
      process_id: "process-abort-1"
    }

    scope = %{
      company_id: "hypergrid",
      objective_id: "objective-abort",
      initiative_id: "initiative",
      project_id: "project",
      work_package_id: "package",
      issue_id: @issue,
      repository: @repo,
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

    authority = %{class: :routine_engineering, capabilities: scope.actions, environments: ["local"]}
    budget = %{model: "luna", effort: :high, max_tokens: 1000, max_children: 1}
    owner_delegation = delegation("owner", :accountable, scope, authority, budget)
    {:ok, owner_graph, _} = ResponsibilityGraph.delegate(ResponsibilityGraph.new(), owner_delegation, 0)

    child_delegation =
      delegation("delegation-abort", :responsible, scope, authority, budget, parent_delegation_id: "owner")

    {:ok, graph, _} = ResponsibilityGraph.delegate(owner_graph, child_delegation, 0)
    {:ok, graph} = ResponsibilityGraph.bind_runtime_lease(graph, "delegation-abort", lease, 0)

    %{
      base_url: "https://provider.test",
      runner_token: "runner-token",
      attestation_key: "attestation-key",
      runner_id: "runner-abort-1",
      pool_key: "midgard",
      host_witness_fun: fn request ->
        if request["operation"] == "spawn_intent", do: send(self(), {:root_spawn, request["operation"]})

        receipt =
          if request["operation"] == "abort_prepare_intent" do
            intent = request["abortPrepare"]
            send(self(), {:root_prepare_intent, intent["prepareId"], intent["prepareRequestSHA256"]})
            Process.put(:abort_lifecycle_events, Process.get(:abort_lifecycle_events, []) ++ [:root_prepare_intent])
            replayed = Process.get(:root_prepare_intent_count, 0) > 0
            Process.put(:root_prepare_intent_count, Process.get(:root_prepare_intent_count, 0) + 1)
            replayed
          else
            false
          end

        {:ok,
         %{
           "ok" => true,
           "receipt" => %{
             "version" => 1,
             "sequence" => 1,
             "hash" => String.duplicate("a", 64),
             "replayed" => receipt
           }
         }}
      end,
      managed_project_profile_id: @profile,
      issue_id: @issue,
      issue_identifier: "HGS-TEST",
      repository_ref: @repo,
      fence_state: fence_state,
      responsibility_graph: graph,
      journal_path: path
    }
  end

  defp host_config(assignment, result_root, abort_root, workspace_root, credential_root) do
    %{
      repository_ref: assignment.repository_ref,
      slot_id: "slot-abort",
      claim_name: "claim-abort",
      image: "registry.example/worker@sha256:" <> String.duplicate("a", 64),
      repository_id: "123456789",
      assignment_subject_digest: @digest,
      provider_url: "https://provider.test",
      runner_token: "runner-token",
      abort_journal_root: abort_root,
      workspace_root: workspace_root,
      result_journal_root: result_root,
      credential_root: credential_root,
      api_server: "https://kube.test",
      client_context_fun: fn current, _operation, _key, _context -> {:ok, %{expected_job: expected_job(current)}} end,
      slot_guard: FakeSlotGuard,
      adapter: ManagedExecutorAdapter
    }
  end

  defp private_dir!(path) do
    File.mkdir!(path)
    unless match?({:win32, _}, :os.type()), do: File.chmod!(path, 0o700)
  end

  defp expected_job(assignment) do
    {:ok, expected} = JobSpec.compile(assignment, config(assignment))
    expected
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
      "projectionId" => "projection-abort",
      "reservationId" => "reservation-abort",
      "workspaceId" => "workspace-abort",
      "companyId" => "hypergrid",
      "reservationNonce" => "nonce-abort",
      "issueId" => @issue,
      "managedProjectProfileId" => @profile,
      "repositoryRef" => @repo,
      "scopeKeys" => ["repo:#{@repo}", "work:abort"]
    }
  end

  defp claim_result_payload do
    %{
      "projectionId" => "projection-abort",
      "projectionState" => "active",
      "mutationState" => "applied",
      "claimEvidence" => %{
        "responsibleDelegationId" => "delegation-abort",
        "executionFenceToken" => "#{@issue}:1",
        "runtimeLeaseId" => "worker-abort-1"
      }
    }
  end
end
