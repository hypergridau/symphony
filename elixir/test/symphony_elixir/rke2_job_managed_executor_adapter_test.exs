Code.require_file("../support/rke2_job_fake_client.exs", __DIR__)

defmodule SymphonyElixir.RKE2JobMalformedClientContext do
  def client_context(_assignment, _operation, _key, _context), do: :not_a_context_response
end

defmodule SymphonyElixir.RKE2JobRaisingClientContext do
  def client_context(_assignment, _operation, _key, _context), do: raise("context provider failed")
end

defmodule SymphonyElixir.RKE2JobFakeClientContext do
  @behaviour SymphonyElixir.RKE2Job.ClientContext

  @impl true
  def client_context(assignment, operation, idempotency_key, agent) do
    Agent.get_and_update(agent, fn state ->
      event = {operation, assignment.sha256, idempotency_key}

      if state.denied do
        {{:error, :synthetic_credential_denial}, %{state | events: [event | state.events]}}
      else
        {{:ok, state.client_context}, %{state | events: [event | state.events]}}
      end
    end)
  end
end

defmodule SymphonyElixir.RKE2JobFakeActivationGuard do
  @behaviour SymphonyElixir.RKE2Job.ActivationGuard

  @impl true
  def authorize(assignment, allocation, idempotency_key, agent) do
    Agent.get_and_update(agent, fn state ->
      event = {:authorize, assignment.sha256, allocation.id, idempotency_key}
      result = if Map.get(state, :activation_denied), do: {:held, :synthetic_activation_denial}, else: :ok
      {result, %{state | events: [event | state.events]}}
    end)
  end
end

defmodule SymphonyElixir.RKE2JobFakeAuthSlotLeaseGuard do
  @behaviour SymphonyElixir.RKE2Job.AuthSlotLeaseGuard

  @impl true
  def reserve(slot, assignment, agent), do: record(:reserve, slot, assignment, nil, agent)

  @impl true
  def bind_uid(slot, assignment, allocation, agent), do: record(:bind_uid, slot, assignment, allocation, agent)

  @impl true
  def authorize(slot, assignment, allocation, agent), do: record(:authorize, slot, assignment, allocation, agent)

  @impl true
  def verify_bound(slot, assignment, allocation, agent), do: record(:verify_bound, slot, assignment, allocation, agent)

  @impl true
  def release(slot, assignment, allocation, agent), do: record(:release, slot, assignment, allocation, agent)

  defp record(action, slot, assignment, allocation, agent) do
    Agent.get_and_update(agent, fn state ->
      event = {action, slot.slot_id, slot.lease_id, assignment.sha256, allocation && allocation.id}

      result =
        cond do
          state.denied == action -> {:held, :synthetic_slot_lease_denial}
          Map.get(state, :forced_action) == action -> Map.get(state, :forced_response)
          true -> :ok
        end

      {result, %{state | events: [event | state.events]}}
    end)
  end
end

defmodule SymphonyElixir.RKE2JobFakePrepareAckGuard do
  @behaviour SymphonyElixir.RKE2Job.PrepareAckGuard

  @impl true
  def verify(_assignment_digest, _allocation_id, _observation, _ack, %{raise?: true}),
    do: raise("prepare acknowledgment verifier failed")

  @impl true
  def verify(assignment_digest, allocation_id, observation, ack, context) when is_map(context) do
    if context.assignment_digest == assignment_digest and context.allocation_id == allocation_id and
         context.observation == observation and context.ack == ack do
      :ok
    else
      {:held, :prepare_ack_observation_mismatch}
    end
  end

  @impl true
  def verify(_assignment_digest, _allocation_id, _observation, _ack, _context),
    do: {:held, :prepare_ack_context_unavailable}
end

defmodule SymphonyElixir.RKE2JobFakeAllocationRegistry do
  @spec ready?(term()) :: boolean()
  def ready?(agent), do: is_pid(agent) and Process.alive?(agent)

  @spec register(String.t(), String.t(), String.t(), pid()) :: :ok | {:held, atom()}
  def register(allocation_id, uid, reservation_id, agent) do
    Agent.get_and_update(agent, fn state ->
      event = {allocation_id, uid, reservation_id}
      {state.result, %{state | events: [event | state.events]}}
    end)
  end
end

defmodule SymphonyElixir.RKE2JobManagedExecutorAdapterTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{JobSpec, ManagedExecutorAdapter, ResultJournal, SuspendedAbort}
  alias SymphonyElixir.RKE2JobFakeClient
  alias SymphonyElixir.Worker.CLI

  setup do
    {:ok, client} =
      Agent.start_link(fn ->
        %{
          jobs: %{},
          creates: 0,
          deletes: [],
          create_error: nil,
          create_commit?: true,
          get_error: nil,
          delete_error: nil,
          delete_commit?: false
        }
      end)

    {:ok, credentials} =
      Agent.start_link(fn -> %{client_context: client, events: [], denied: false} end)

    {:ok, registration} = Agent.start_link(fn -> %{events: [], result: :ok} end)

    {:ok, slot_lease} = Agent.start_link(fn -> %{events: [], denied: nil} end)

    if match?({:win32, _}, :os.type()), do: Process.put(:result_journal_windows_test_only, true)
    root = Path.join(System.tmp_dir!(), "symphony-terminal-finalizer-#{System.unique_integer([:positive])}")
    :ok = File.mkdir_p(root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(root, 0o700)
    on_exit(fn -> File.rm_rf(root) end)

    %{client: client, credentials: credentials, registration: registration, slot_lease: slot_lease, root: root}
  end

  test "terminal finalization journals before UID-fenced deletion and replays after deletion", context do
    assignment = assignment()
    opts = adapter_context(context) |> Map.put(:result_journal_root, context.root)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    terminal_job_and_pod(context, assignment, allocation)

    unrelated = %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "namespace" => config().namespace,
        "name" => "unrelated-pod",
        "uid" => "unrelated-uid",
        "resourceVersion" => "unrelated-rv",
        "labels" => %{},
        "ownerReferences" => []
      }
    }

    Agent.update(context.client, &put_in(&1, [:pods, "unrelated-uid"], unrelated))

    assert {:ok, observation} =
             ManagedExecutorAdapter.finalize_terminal_owned(allocation, assignment, key(assignment, :finalize), opts)

    assert observation["job_uid"] == elem(allocation_uid(allocation), 1)
    assert {:ok, ^observation} = ResultJournal.load(assignment, observation["job_uid"], context.root)
    assert observation["pod_uid"] == "pod-uid-1"
    assert Agent.get(context.client, & &1.deletes) == [observation["job_uid"]]
    assert Agent.get(context.client, &Map.has_key?(&1.pods, "unrelated-uid"))

    assert {:ok, replay} =
             ManagedExecutorAdapter.finalize_terminal_owned(allocation, assignment, key(assignment, :finalize), opts)

    assert replay == observation
    assert Agent.get(context.client, & &1.deletes) == [observation["job_uid"]]
  end

  test "terminal finalization holds before delete when readback or journal is unavailable", context do
    assignment = assignment()
    opts = adapter_context(context) |> Map.put(:result_journal_root, context.root)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    terminal_job_and_pod(context, assignment, allocation)

    Agent.update(context.client, &Map.put(&1, :list_pods_snapshot_error, :timeout))

    assert {:held, :job_pod_result_read_unavailable} =
             ManagedExecutorAdapter.finalize_terminal_owned(allocation, assignment, key(assignment, :finalize), opts)

    assert Agent.get(context.client, & &1.deletes) == []
    Agent.update(context.client, &Map.delete(&1, :list_pods_snapshot_error))

    assert {:error, :invalid_job_result_journal_root} =
             ManagedExecutorAdapter.finalize_terminal_owned(
               allocation,
               assignment,
               key(assignment, :finalize),
               Map.delete(opts, :result_journal_root)
             )

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "slotted terminal finalization keeps the OAuth lease held after Job and Pod cleanup", context do
    assignment = assignment()
    opts = slot_context(context, assignment) |> Map.put(:result_journal_root, context.root)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    terminal_job_and_pod(context, assignment, allocation, :codex)

    assert {:ok, %{"result" => %{"status" => "completed"}}} =
             ManagedExecutorAdapter.finalize_terminal_owned(allocation, assignment, key(assignment, :finalize), opts)

    assert Agent.get(context.client, & &1.deletes) == [elem(allocation_uid(allocation), 1)]
    refute :release in slot_actions(context)
  end

  test "OAuth slot requires a lease guard before creating a Job", context do
    assignment = assignment()
    opts = slot_context(context, assignment) |> Map.delete(:auth_slot_lease_guard)

    assert {:held, :auth_slot_lease_guard_missing} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert Agent.get(context.client, & &1.creates) == 0
  end

  test "OAuth slot reservation denial and malformed guard response hold before Job creation", context do
    assignment = assignment()
    opts = slot_context(context, assignment)
    Agent.update(context.slot_lease, &%{&1 | denied: :reserve})

    assert {:held, :synthetic_slot_lease_denial} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    Agent.update(context.slot_lease, fn state ->
      state |> Map.put(:denied, nil) |> Map.put(:forced_action, :reserve) |> Map.put(:forced_response, :unknown)
    end)

    assert {:held, :invalid_auth_slot_lease_guard_response} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    Agent.update(context.slot_lease, &Map.put(&1, :forced_response, {:error, :unavailable}))

    assert {:held, {:auth_slot_lease_guard_failed, :unavailable}} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert Agent.get(context.client, & &1.creates) == 0
  end

  test "OAuth slot lease binds the Job UID and gates activation and direct deletion", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:ok, _uid} = allocation_uid(allocation)
    assert [:reserve, :bind_uid] == slot_actions(context)

    Agent.update(context.slot_lease, &%{&1 | denied: :authorize})

    assert {:held, :synthetic_slot_lease_denial} =
             ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :activate), opts)

    assert Agent.get(context.client, &Map.get(&1, :activations, 0)) == 0
    Agent.update(context.slot_lease, &%{&1 | denied: nil})

    assert {:ok, _job} = ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :activate), opts)

    assert {:held, :terminal_finalization_required} =
             ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)

    assert Agent.get(context.client, & &1.deletes) == []
    assert [:reserve, :bind_uid, :authorize, :authorize] == slot_actions(context)
  end

  test "OAuth slot remains held after UID binding failure and rejects a direct delete", context do
    assignment = assignment()
    opts = slot_context(context, assignment)
    Agent.update(context.slot_lease, &%{&1 | denied: :bind_uid})

    assert {:held, :synthetic_slot_lease_denial} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert Agent.get(context.client, & &1.creates) == 1
    Agent.update(context.slot_lease, &%{&1 | denied: nil})

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:held, :terminal_finalization_required} =
             ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)

    refute :release in slot_actions(context)
    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "suspended slotted abort removes only the exact unstarted Job and retains the slot lease", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:held, :durable_abort_prepare_ack_required} =
             ManagedExecutorAdapter.abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    assert Agent.get(context.client, & &1.deletes) == []
    assert {:held, :durable_abort_prepare_ack_required} = SuspendedAbort.abort_owned(assignment, "job-uid", [])

    assert {:ok, observation} =
             ManagedExecutorAdapter.prepare_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    {ack, guarded_opts} = ack_context(assignment, allocation, observation, opts)

    assert {:held, :durable_abort_prepare_ack_required} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               opts
             )

    assert {:held, :durable_abort_prepare_ack_required} =
             SuspendedAbort.confirm_owned(assignment, "job-uid", observation, opts)

    assert :ok =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               guarded_opts
             )

    assert Agent.get(context.client, & &1.deletes) == [elem(allocation_uid(allocation), 1)]
    refute :release in slot_actions(context)

    assert {:held, :suspended_abort_job_already_absent} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               guarded_opts
             )
  end

  test "suspended abort requires the current bound slot verification", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    Agent.update(context.slot_lease, &%{&1 | denied: :verify_bound})

    assert {:held, :synthetic_slot_lease_denial} =
             ManagedExecutorAdapter.prepare_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    assert Agent.get(context.client, & &1.deletes) == []
    assert :verify_bound in slot_actions(context)
    refute :release in slot_actions(context)
  end

  test "suspended abort prepare is read-only and confirmation consumes the exact observation", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:ok, observation} =
             ManagedExecutorAdapter.prepare_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    assert observation["schemaVersion"] == 1
    assert is_binary(observation["observedAt"])
    assert observation["compiledIdentity"]["assignmentSHA256"] == assignment.sha256
    assert observation["compiledIdentity"]["compiledJobSHA256"] =~ ~r/\A[0-9a-f]{64}\z/
    assert observation["allocationId"] == allocation.id
    assert observation["slotBinding"]["leaseId"] == opts.config.auth_slot.lease_id

    assert observation["job"] == %{
             "uid" => elem(allocation_uid(allocation), 1),
             "resourceVersion" => "17",
             "generation" => 1,
             "suspended" => true,
             "noExecution" => true
           }

    assert observation["podSnapshot"]["complete"]
    assert observation["podSnapshot"]["ownedPodsAbsent"]
    assert observation["podSnapshot"]["resourceVersion"] == "list-rv-9"
    assert observation["podSnapshot"]["sha256"] =~ ~r/\A[0-9a-f]{64}\z/
    assert Agent.get(context.client, & &1.deletes) == []

    {ack, guarded_opts} = ack_context(assignment, allocation, observation, opts)

    assert :ok =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               guarded_opts
             )

    assert length(Agent.get(context.client, & &1.deletes)) == 1
    refute Map.has_key?(observation, "token")
  end

  test "suspended abort confirmation holds altered and stale prepare observations", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:ok, observation} =
             ManagedExecutorAdapter.prepare_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    altered = put_in(observation, ["job", "resourceVersion"], "tampered-rv")
    {ack, guarded_opts} = ack_context(assignment, allocation, observation, opts)

    assert {:held, :prepare_ack_observation_mismatch} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               altered,
               ack,
               guarded_opts
             )

    assert Agent.get(context.client, & &1.deletes) == []

    changed_binding = put_in(opts, [:config, :auth_slot, :lease_id], "lease:slot-1:43")

    changed_guarded_opts =
      Map.merge(changed_binding, Map.take(guarded_opts, [:prepare_ack_guard, :prepare_ack_guard_context]))

    assert {:held, :suspended_abort_observation_invalid} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               changed_guarded_opts
             )

    assert Agent.get(context.client, & &1.deletes) == []

    Agent.update(context.client, &Map.put(&1, :pod_list_resource_version, "list-rv-10"))

    assert {:held, :suspended_abort_pods_changed_since_prepare} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               guarded_opts
             )

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "suspended abort confirmation rejects replay after successful deletion", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:ok, observation} =
             ManagedExecutorAdapter.prepare_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    {ack, guarded_opts} = ack_context(assignment, allocation, observation, opts)

    assert :ok =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               guarded_opts
             )

    assert {:held, :suspended_abort_job_already_absent} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               guarded_opts
             )

    assert length(Agent.get(context.client, & &1.deletes)) == 1
  end

  test "suspended abort confirmation holds a conditional-delete race after prepare", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:ok, observation} =
             ManagedExecutorAdapter.prepare_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    {ack, guarded_opts} = ack_context(assignment, allocation, observation, opts)

    Agent.update(context.client, &Map.put(&1, :delete_error, :precondition_conflict))

    assert {:held, :suspended_abort_delete_uncertain} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               guarded_opts
             )

    assert Agent.get(context.client, & &1.deletes) == []
    refute :release in slot_actions(context)
  end

  test "suspended abort confirmation requires a matching durable prepare acknowledgment guard", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:ok, observation} =
             ManagedExecutorAdapter.prepare_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    {ack, guarded_opts} = ack_context(assignment, allocation, observation, opts)

    assert {:held, :abort_prepare_ack_guard_missing} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               opts
             )

    changed_ack = Map.put(ack, "prepareId", "different-prepare-id")

    assert {:held, :prepare_ack_observation_mismatch} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               changed_ack,
               guarded_opts
             )

    raising_guard_opts =
      Map.put(guarded_opts, :prepare_ack_guard_context, %{raise?: true})

    assert {:held, :abort_prepare_ack_verification_failed} =
             ManagedExecutorAdapter.confirm_abort_unstarted_owned(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               observation,
               ack,
               raising_guard_opts
             )

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "suspended abort holds if a claim Pod appears after the first snapshot", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    [1, namespace, _name, _uid, _digest] = allocation_payload(allocation)
    pod = unrelated_claim_pod(namespace, opts.config.auth_slot.claim_name)
    Agent.update(context.client, &Map.put(&1, :pod_injected_on_suspended_delete, {"late-pod", pod}))

    assert {:held, :suspended_abort_pod_absence_unverified} =
             abort_with_ack(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    assert length(Agent.get(context.client, & &1.deletes)) == 1
    refute :release in slot_actions(context)
  end

  test "suspended abort holds when the delete committed but its client raised", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    Agent.update(context.client, &Map.put(&1, :raise_after_suspended_delete, true))

    assert {:held, :suspended_abort_delete_uncertain} =
             abort_with_ack(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    assert length(Agent.get(context.client, & &1.deletes)) == 1
    refute :release in slot_actions(context)
  end

  test "suspended abort holds on activation history, conditional-delete conflict and claim mount", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    [1, namespace, name, _uid, _digest] = allocation_payload(allocation)
    job_key = {namespace, name}
    Agent.update(context.client, &put_in(&1, [:jobs, job_key, "metadata", "generation"], 2))

    assert {:held, :suspended_abort_identity_or_start_unverified} =
             abort_with_ack(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    Agent.update(context.client, &put_in(&1, [:jobs, job_key, "metadata", "generation"], 1))
    Agent.update(context.client, &Map.put(&1, :delete_error, :precondition_conflict))

    assert {:held, :suspended_abort_delete_uncertain} =
             abort_with_ack(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    Agent.update(context.client, &Map.put(&1, :delete_error, nil))
    claim = opts.config.auth_slot.claim_name
    pod = unrelated_claim_pod(namespace, claim)

    Agent.update(context.client, fn state ->
      Map.update(state, :pods, %{"claim-pod" => pod}, &Map.put(&1, "claim-pod", pod))
    end)

    assert {:held, :suspended_abort_pod_absence_unverified} =
             abort_with_ack(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "suspended abort rejects execution status, replacement UID and incomplete Pod readback", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    [1, namespace, name, uid, _digest] = allocation_payload(allocation)
    job_key = {namespace, name}
    original = Agent.get(context.client, &Map.fetch!(&1.jobs, job_key))

    call = fn ->
      abort_with_ack(
        allocation,
        assignment,
        key(assignment, :abort_unstarted),
        opts
      )
    end

    Agent.update(context.client, &put_in(&1, [:jobs, job_key, "status"], %{"startTime" => "2026-09-27T00:00:00Z"}))
    assert {:held, :suspended_abort_identity_or_start_unverified} = call.()

    Agent.update(context.client, &put_in(&1, [:jobs, job_key], put_in(original, ["metadata", "uid"], "replacement")))
    assert {:held, :suspended_abort_identity_or_start_unverified} = call.()

    Agent.update(context.client, &put_in(&1, [:jobs, job_key], original))
    Agent.update(context.client, &Map.put(&1, :list_pods_snapshot_error, :timeout))
    assert {:held, :suspended_abort_pod_read_unavailable} = call.()

    Agent.update(context.client, fn state ->
      state |> Map.delete(:list_pods_snapshot_error) |> Map.put(:pod_list_resource_version, nil)
    end)

    assert {:held, :suspended_abort_pod_absence_unverified} = call.()
    assert Agent.get(context.client, & &1.deletes) == []
    assert Agent.get(context.client, fn state -> get_in(state.jobs[job_key], ["metadata", "uid"]) end) == uid
  end

  test "suspended abort holds when a Pod signals the Job or has malformed volume data", context do
    assignment = assignment()
    opts = slot_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    [1, namespace, _name, uid, _digest] = allocation_payload(allocation)
    pod = unrelated_claim_pod(namespace, "other-claim")
    labelled = put_in(pod, ["metadata", "labels"], %{"batch.kubernetes.io/controller-uid" => uid})
    Agent.update(context.client, &Map.put(&1, :pods, %{"claim-pod" => labelled}))

    assert {:held, :suspended_abort_pod_absence_unverified} =
             abort_with_ack(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    malformed = put_in(pod, ["spec", "volumes"], [%{"persistentVolumeClaim" => "invalid"}])
    Agent.update(context.client, &Map.put(&1, :pods, %{"claim-pod" => malformed}))

    assert {:held, :suspended_abort_pod_absence_unverified} =
             abort_with_ack(
               allocation,
               assignment,
               key(assignment, :abort_unstarted),
               opts
             )

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "allocates through the provider and records exact namespace, name, digest, and UID", context do
    assignment = assignment()

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(
               assignment,
               key(assignment, :allocation),
               adapter_context(context)
             )

    assert allocation.status == :ready
    assert {:ok, uid} = allocation_uid(allocation)
    assert is_binary(uid)

    assert [{:allocate, digest, idempotency_key}] = Agent.get(context.credentials, &Enum.reverse(&1.events))
    assert digest == assignment.sha256
    assert idempotency_key == key(assignment, :allocation)
    assert [{allocation_id, ^uid, "reservation-1"}] = Agent.get(context.registration, &Enum.reverse(&1.events))
    assert allocation_id == allocation.id
  end

  test "holds a suspended Job until Dahlia confirms its exact UID", context do
    Agent.update(context.registration, &%{&1 | result: {:held, :job_allocation_registration_unverified}})
    assignment = assignment()

    assert {:held, :job_allocation_registration_unverified} =
             ManagedExecutorAdapter.allocate_or_reconcile(
               assignment,
               key(assignment, :allocation),
               adapter_context(context)
             )

    assert Agent.get(context.client, & &1.creates) == 1
    Agent.update(context.registration, &%{&1 | result: :ok})

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(
               assignment,
               key(assignment, :allocation),
               adapter_context(context)
             )

    assert allocation.status == :ready
    assert Agent.get(context.client, & &1.creates) == 1
    assert length(Agent.get(context.registration, & &1.events)) == 2
  end

  test "holds when the validated provider claim is absent", context do
    assignment = assignment()
    opts = Map.delete(adapter_context(context), :claim_binding)

    assert {:held, :job_allocation_registration_unavailable} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert Agent.get(context.registration, & &1.events) == []
    assert Agent.get(context.client, & &1.creates) == 0
  end

  test "does not create a Job without registration transport", context do
    assignment = assignment()
    opts = Map.delete(adapter_context(context), :allocation_registry_context)

    assert {:held, :job_allocation_registration_unavailable} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert Agent.get(context.client, & &1.creates) == 0
  end

  test "reconciles an ambiguous create by exact provider readback", context do
    Agent.update(context.client, &%{&1 | create_error: :timeout})
    assignment = assignment()

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(
               assignment,
               key(assignment, :allocation),
               adapter_context(context)
             )

    assert {:ok, _uid} = allocation_uid(allocation)
    assert Agent.get(context.client, & &1.creates) == 1
  end

  test "release deletes only the read-back Job with the recorded UID", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert :ok = ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)
    assert Agent.get(context.client, & &1.deletes) == [elem(allocation_uid(allocation), 1)]
    assert :ok = ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)
  end

  test "allocation stays suspended and only the cleanup path accepts an activated owned Job", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(
               assignment,
               key(assignment, :allocation),
               opts
             )

    [1, namespace, name, _uid, _digest] = allocation_payload(allocation)
    job_key = {namespace, name}
    Agent.update(context.client, &put_in(&1, [:jobs, job_key, "spec", "suspend"], false))

    assert {:held, :job_identity_or_spec_mismatch} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert :ok = ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)
  end

  test "activation changes only the allocated suspended Job and replays without another patch", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:ok, active} =
             ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :activate), opts)

    assert get_in(active, ["spec", "suspend"]) == false
    assert Agent.get(context.client, &Map.get(&1, :activations)) == 1

    assert {:ok, ^active} =
             ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :activate), opts)

    assert Agent.get(context.client, &Map.get(&1, :activations)) == 1
    assert :ok = ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)
  end

  test "activation holds a replacement UID, spec drift, or failed patch without starting it", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    [1, namespace, name, _uid, _digest] = allocation_payload(allocation)
    job_key = {namespace, name}
    original = Agent.get(context.client, &Map.fetch!(&1.jobs, job_key))

    Agent.update(context.client, &put_in(&1, [:jobs, job_key, "metadata", "uid"], "uid-replacement"))

    assert {:held, :job_identity_or_spec_mismatch} =
             ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :activate), opts)

    Agent.update(context.client, &put_in(&1, [:jobs, job_key], put_in(original, ["spec", "backoffLimit"], 7)))

    assert {:held, :job_identity_or_spec_mismatch} =
             ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :activate), opts)

    Agent.update(context.client, fn state ->
      state |> put_in([:jobs, job_key], original) |> Map.put(:activate_error, :timeout)
    end)

    assert {:held, {:job_activation_outcome_uncertain, :timeout}} =
             ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :activate), opts)

    assert get_in(Agent.get(context.client, &Map.fetch!(&1.jobs, job_key)), ["spec", "suspend"]) == true
    assert Agent.get(context.client, &Map.get(&1, :activations, 0)) == 0
  end

  test "activation rejects forged stage and denied client context", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:error, :invalid_rke2_job_idempotency_key} =
             ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :allocation), opts)

    Agent.update(context.credentials, &%{&1 | denied: true})

    assert {:error, :rke2_job_client_context_unavailable} =
             ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :activate), opts)

    assert Agent.get(context.client, &Map.get(&1, :activations, 0)) == 0
  end

  test "activation requires an explicit authorization port and holds its denial before client context", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:held, :activation_guard_missing} =
             ManagedExecutorAdapter.activate_owned(
               allocation,
               assignment,
               key(assignment, :activate),
               Map.delete(opts, :activation_guard)
             )

    Agent.update(context.credentials, &Map.put(&1, :activation_denied, true))

    assert {:held, :synthetic_activation_denial} =
             ManagedExecutorAdapter.activate_owned(allocation, assignment, key(assignment, :activate), opts)

    assert Agent.get(context.client, &Map.get(&1, :activations, 0)) == 0

    assert Agent.get(
             context.credentials,
             &Enum.count(&1.events, fn
               {stage, _, _} -> stage == :activate
               _ -> false
             end)
           ) == 0
  end

  test "reconciles a DELETE timeout after readback proves the allocated Job is absent", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    Agent.update(context.client, &%{&1 | delete_error: :timeout, delete_commit?: true})

    assert :ok = ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)
    assert Agent.get(context.client, &map_size(&1.jobs)) == 0
  end

  test "holds a replacement Job UID even when the replacement still matches the assignment", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:ok, expected} = JobSpec.compile(assignment, config())
    name = expected["metadata"]["name"]
    replacement = store_defaulted_job(expected, "uid-replacement-123")
    Agent.update(context.client, &put_in(&1, [:jobs, {config().namespace, name}], replacement))

    assert {:held, :job_allocation_identity_mismatch} =
             ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "rejects a forged allocation and fails closed when client context cannot be issued", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    [version, namespace, name, uid, _digest] = allocation_payload(allocation)
    forged = encode_allocation([version, namespace, name, uid, String.duplicate("0", 64)])

    assert {:error, :job_allocation_identity_mismatch} =
             ManagedExecutorAdapter.delete_owned(forged, assignment, key(assignment, :delete), opts)

    Agent.update(context.credentials, &%{&1 | denied: true})

    assert {:error, :rke2_job_client_context_unavailable} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)
  end

  test "rejects invalid assignments, keys, and adapter ports", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:error, :invalid_rke2_job_assignment} =
             ManagedExecutorAdapter.allocate_or_reconcile(nil, key(assignment, :allocation), opts)

    assert {:error, :invalid_rke2_job_idempotency_key} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, "wrong-key", opts)

    assert {:error, :rke2_job_adapter_ports_invalid} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), nil)
  end

  test "fails closed for malformed and raising client-context providers", context do
    assignment = assignment()
    key = key(assignment, :allocation)

    malformed = Map.put(adapter_context(context), :client_context_provider, SymphonyElixir.RKE2JobMalformedClientContext)
    raising = Map.put(adapter_context(context), :client_context_provider, SymphonyElixir.RKE2JobRaisingClientContext)

    assert {:error, :invalid_rke2_job_client_context_response} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key, malformed)

    assert {:error, :rke2_job_client_context_unavailable} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key, raising)
  end

  test "rejects malformed and invalid-UID allocation identifiers", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:error, :job_allocation_identity_mismatch} =
             ManagedExecutorAdapter.delete_owned(%{id: "rke2job:v1:%%%", status: :ready}, assignment, key(assignment, :delete), opts)

    [version, namespace, name, _uid, digest] = allocation_payload(allocation)
    invalid_uid = encode_allocation([version, namespace, name, "invalid uid", digest])

    assert {:error, :job_allocation_identity_mismatch} =
             ManagedExecutorAdapter.delete_owned(invalid_uid, assignment, key(assignment, :delete), opts)
  end

  defp adapter_context(context) do
    %{
      client: RKE2JobFakeClient,
      client_context_provider: SymphonyElixir.RKE2JobFakeClientContext,
      client_context_provider_context: context.credentials,
      activation_guard: SymphonyElixir.RKE2JobFakeActivationGuard,
      activation_guard_context: context.credentials,
      allocation_registry: SymphonyElixir.RKE2JobFakeAllocationRegistry,
      allocation_registry_context: context.registration,
      claim_binding: %{
        reservation_id: "reservation-1",
        issue_id: "issue-1",
        generation: 4,
        repository_ref: "hypergridau/symphony",
        runner_id: "runner-17"
      },
      config: config()
    }
  end

  defp slot_context(context, assignment) do
    slot = %{
      slot_id: "luna-slot-1",
      claim_name: "frigga-codex-luna-slot-1",
      lease_id: "lease:slot-1:42",
      assignment_sha256: assignment.sha256,
      seat: assignment.seat
    }

    adapter_context(context)
    |> Map.put(:auth_slot_lease_guard, SymphonyElixir.RKE2JobFakeAuthSlotLeaseGuard)
    |> Map.put(:auth_slot_lease_guard_context, context.slot_lease)
    |> update_in([:config], fn config ->
      config
      |> Map.put(:auth_slot, slot)
      |> Map.put(:auth_slot_catalog, %{slot.slot_id => slot.claim_name})
    end)
  end

  defp ack_context(assignment, allocation, observation, opts) do
    ack = %{"prepareId" => "prepare-fixture"}

    guard_context = %{
      assignment_digest: assignment.sha256,
      allocation_id: allocation.id,
      observation: observation,
      ack: ack
    }

    guarded_opts =
      opts
      |> Map.put(:prepare_ack_guard, SymphonyElixir.RKE2JobFakePrepareAckGuard)
      |> Map.put(:prepare_ack_guard_context, guard_context)

    {ack, guarded_opts}
  end

  defp abort_with_ack(allocation, assignment, idempotency_key, opts) do
    with {:ok, observation} <-
           ManagedExecutorAdapter.prepare_abort_unstarted_owned(allocation, assignment, idempotency_key, opts) do
      {ack, guarded_opts} = ack_context(assignment, allocation, observation, opts)

      ManagedExecutorAdapter.confirm_abort_unstarted_owned(
        allocation,
        assignment,
        idempotency_key,
        observation,
        ack,
        guarded_opts
      )
    end
  end

  defp slot_actions(context) do
    Agent.get(context.slot_lease, fn state -> state.events |> Enum.reverse() |> Enum.map(&elem(&1, 0)) end)
  end

  defp unrelated_claim_pod(namespace, claim) do
    %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "namespace" => namespace,
        "name" => "other-worker",
        "uid" => "claim-pod",
        "resourceVersion" => "claim-rv-1",
        "labels" => %{},
        "ownerReferences" => []
      },
      "spec" => %{"volumes" => [%{"name" => "shared", "persistentVolumeClaim" => %{"claimName" => claim}}]}
    }
  end

  defp terminal_job_and_pod(context, assignment, allocation, mode \\ :preflight) do
    [1, namespace, name, uid, _digest] = allocation_payload(allocation)
    job_key = {namespace, name}

    base_receipt =
      CLI.base_result(
        %{
          subject: %{
            assignmentDigest: assignment.sha256,
            issueUuid: assignment.lease.issue_id,
            generation: assignment.lease.generation,
            repositoryRef: assignment.repository_ref,
            branchRef: "refs/heads/" <> assignment.branch
          }
        },
        "preflight_passed",
        "auth_slot_required"
      )

    receipt =
      if mode == :codex do
        Map.merge(base_receipt, %{
          status: "completed",
          reason: "pull_request_created",
          checkout_lease_id: "checkout-1",
          checkout_revocation: "confirmed",
          broker_lease_id: "publish-1",
          revocation: "confirmed",
          codex_exit_code: 0,
          base_oid: String.duplicate("a", 40),
          branch_head_oid: String.duplicate("b", 40),
          head_oid: String.duplicate("c", 40),
          changed_files: 1,
          pull_request_number: 123,
          pull_request_url: "https://github.com/hypergridau/symphony/pull/123"
        })
      else
        base_receipt
      end

    pod = %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "namespace" => namespace,
        "name" => name <> "-a",
        "uid" => "pod-uid-1",
        "resourceVersion" => "pod-rv-8",
        "labels" => %{"batch.kubernetes.io/job-name" => name},
        "ownerReferences" => [
          %{"apiVersion" => "batch/v1", "kind" => "Job", "name" => name, "uid" => uid, "controller" => true}
        ]
      },
      "status" => %{
        "phase" => "Succeeded",
        "containerStatuses" => [
          %{
            "name" => "symphony-worker",
            "ready" => false,
            "state" => %{"terminated" => %{"exitCode" => 0, "message" => Jason.encode!(receipt)}}
          }
        ]
      }
    }

    Agent.update(context.client, fn state ->
      job =
        state.jobs
        |> Map.fetch!(job_key)
        |> put_in(["metadata", "resourceVersion"], "job-rv-7")
        |> put_in(["spec", "suspend"], false)
        |> Map.put("status", %{"conditions" => [%{"type" => "Complete", "status" => "True"}]})

      state
      |> put_in([:jobs, job_key], job)
      |> Map.put(:pods, %{pod["metadata"]["uid"] => pod})
      |> Map.put(:delete_pods_on_delete?, true)
    end)
  end

  defp allocation_uid(allocation) do
    [1, _namespace, _name, uid, _digest] = allocation_payload(allocation)
    {:ok, uid}
  end

  defp allocation_payload(%{id: "rke2job:v1:" <> encoded, status: :ready}) do
    {:ok, payload} = Base.url_decode64(encoded, padding: false)
    {:ok, values} = Jason.decode(payload)
    values
  end

  defp encode_allocation(payload), do: %{id: "rke2job:v1:" <> (Jason.encode!(payload) |> Base.url_encode64(padding: false)), status: :ready}

  defp assignment do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-rke2-provider-adapter",
        seat: "runner-17",
        lease: %{
          issue_id: "issue-1",
          repository: "hypergridau/symphony",
          generation: 4,
          session_id: "worker:issue-1:4",
          process_id: "worker:issue-1:4"
        },
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "RKE2 Job allocation", evidence: "Fake transport tests"},
        context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end

  defp config,
    do: %{
      namespace: "symphony-beta",
      image: "registry.example/symphony-worker@sha256:" <> String.duplicate("a", 64),
      repository_id: "123456789"
    }

  defp key(assignment, stage), do: assignment.sha256 <> ":" <> Atom.to_string(stage)

  defp store_defaulted_job(job, uid) do
    name = job["metadata"]["name"]
    generated = %{"batch.kubernetes.io/controller-uid" => uid, "batch.kubernetes.io/job-name" => name}

    job
    |> put_in(["metadata", "uid"], uid)
    |> put_in(["metadata", "resourceVersion"], "18")
    |> put_in(["metadata", "generation"], 1)
    |> put_in(["metadata", "creationTimestamp"], "2026-09-26T00:00:00Z")
    |> put_in(["metadata", "managedFields"], [%{"manager" => "kube-controller-manager", "operation" => "Update"}])
    |> put_in(["metadata", "labels"], Map.merge(job["metadata"]["labels"], generated))
    |> put_in(["metadata", "annotations"], Map.put(job["metadata"]["annotations"], "batch.kubernetes.io/job-tracking", ""))
    |> put_in(["spec", "completions"], 1)
    |> put_in(["spec", "parallelism"], 1)
    |> put_in(["spec", "completionMode"], "NonIndexed")
    |> put_in(["spec", "manualSelector"], false)
    |> put_in(["spec", "suspend"], true)
    |> put_in(["spec", "podReplacementPolicy"], "TerminatingOrFailed")
    |> put_in(["spec", "selector"], %{"matchLabels" => %{"batch.kubernetes.io/controller-uid" => uid}})
    |> put_in(["spec", "template", "metadata", "creationTimestamp"], nil)
    |> put_in(["spec", "template", "metadata", "labels"], Map.merge(job["spec"]["template"]["metadata"]["labels"], generated))
    |> put_in(["spec", "template", "spec", "dnsPolicy"], "ClusterFirst")
    |> put_in(["spec", "template", "spec", "schedulerName"], "default-scheduler")
    |> put_in(["spec", "template", "spec", "terminationGracePeriodSeconds"], 30)
    |> put_in(["spec", "template", "spec", "enableServiceLinks"], true)
    |> put_in(["spec", "template", "spec", "preemptionPolicy"], "PreemptLowerPriority")
    |> update_in(["spec", "template", "spec"], &Map.put_new(&1, "serviceAccountName", "default"))
    |> update_in(["spec", "template", "spec", "containers", Access.at(0)], fn container ->
      container
      |> Map.put_new("terminationMessagePath", "/dev/termination-log")
      |> Map.put_new("terminationMessagePolicy", "File")
    end)
  end
end
