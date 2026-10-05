defmodule SymphonyElixir.RKE2Job.PreSpawnAbortController do
  @moduledoc """
  Captures only an explicit bound OAuth slot denial before spawn intent.

  The existing claim journal fences the exact allocation. The existing result
  publisher and prepare/confirm journals own subsequent durable checkpoints.
  An abort never becomes a normal activation on restart or a completed task.
  """

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ManagedExecutor.{AbortResultPublisher, Record}

  alias SymphonyElixir.RKE2Job.{
    AbortPrepareCaller,
    AbortPrepareJournal,
    ManagedExecutorAdapter,
    PreSpawnAbortSnapshot,
    RootAbortInputPublisher,
    SuspendedController
  }

  alias SymphonyElixir.WorkPackageClaim.{Dispatch, HostWitness, Journal}

  @test_environment Mix.env() == :test

  @doc "Checks readiness or durably retains the actual typed denial, sharing the spawn transition lock."
  @spec before_resume(map(), map(), map()) :: {:ok, :ready} | {:abort, map()} | {:held, term()} | {:error, term()}
  def before_resume(assignment, input, context) do
    Journal.with_lock(input.journal_path, fn ->
      before_resume_locked(assignment, input, context)
    end)
  rescue
    _ -> {:held, :pre_spawn_abort_boundary_unverified}
  end

  defp before_resume_locked(assignment, input, context) do
    with {:ok, reservation} <- SuspendedController.retained(assignment, input, context) do
      resume_decision(reservation, assignment, input, context)
    end
  end

  defp resume_decision(%{dispatch: %{phase: "spawn_started"}}, _assignment, _input, _context),
    do: {:ok, :ready}

  defp resume_decision(reservation, assignment, input, context),
    do: decide(reservation, assignment, input, context)

  defp decide(%{dispatch: %{phase: "abort_pending"}} = reservation, assignment, input, context) do
    with :ok <- unused_worker(input, assignment, reservation, false),
         :ok <- exact_config(reservation.dispatch.abort_config, assignment),
         true <- reservation.dispatch.abort_config == context.config do
      {:abort, reservation}
    else
      _ -> {:held, :pre_spawn_abort_context_changed}
    end
  end

  defp decide(%{dispatch: %{phase: "allocation_suspended"}} = reservation, assignment, input, context) do
    adapter = Map.get(context, :adapter, ManagedExecutorAdapter)
    allocation = allocation(reservation)

    if is_atom(adapter) and Code.ensure_loaded?(adapter) and function_exported?(adapter, :preflight_owned, 4) do
      case adapter.preflight_owned(allocation, assignment, assignment.sha256 <> ":preflight", context) do
        :ok -> {:ok, :ready}
        {:denied, :codex_auth_slot_denied} -> capture_denial(reservation, assignment, input, context)
        {:held, _} = held -> held
        _ -> {:held, :pre_spawn_authorization_unverified}
      end
    else
      {:held, :pre_spawn_authorization_unavailable}
    end
  end

  defp decide(_reservation, _assignment, _input, _context), do: {:held, :pre_spawn_abort_not_admissible}

  defp capture_denial(reservation, assignment, input, context) do
    with :ok <- unused_worker(input, assignment, reservation, false),
         :ok <- exact_config(context.config, assignment),
         {:ok, request} <- root_request(reservation, assignment, input, "verify_pre_execution_abort_eligibility"),
         :ok <- root_port(context).verify_eligibility(request),
         {:ok, journal} <- Journal.load(input.journal_path),
         key = reservation_key(reservation),
         {:ok, fenced} <- Dispatch.begin_suspended_abort(journal, key, input, reservation.dispatch.allocation_id, context.config),
         :ok <- Journal.save(input.journal_path, fenced),
         {:ok, reread} <- SuspendedController.retained(assignment, input, context),
         true <- reread.dispatch == fenced.reservations[key].dispatch do
      {:abort, reread}
    else
      {:held, _} = held -> held
      {:error, reason} -> {:held, reason}
      _ -> {:held, :pre_spawn_abort_checkpoint_unverified}
    end
  end

  @doc "Publishes and rereads the canonical blocked result before any disposal or worker lease release."
  @spec publish(map(), map(), map()) :: :ok | {:held, term()}
  def publish(reservation, assignment, context) do
    with true <- reservation.dispatch.phase == "abort_pending",
         :ok <- exact_config(reservation.dispatch.abort_config, assignment),
         {:ok, _reference} <-
           AbortResultPublisher.publish_or_reconcile_abort_result(
             allocation(reservation),
             assignment,
             result(assignment),
             assignment.sha256 <> ":abort-result",
             context
           ),
         :ok <-
           AbortResultPublisher.verify_retained_abort_result(
             allocation(reservation),
             assignment,
             result(assignment),
             assignment.sha256 <> ":abort-result",
             context
           ) do
      :ok
    else
      {:error, reason} -> {:held, reason}
      _ -> {:held, :pre_spawn_abort_result_unverified}
    end
  rescue
    _ -> {:held, :pre_spawn_abort_result_unverified}
  end

  @doc "Reverifies retained root no-spawn authority before repairing ownership lost at process restart."
  @spec reconcile_ownership(map(), map(), map(), map()) :: {:ok, map()} | {:held, term()}
  def reconcile_ownership(reservation, assignment, input, context) do
    with {:ok, ^reservation} <- SuspendedController.retained(assignment, input, context),
         true <- reservation.dispatch.phase == "abort_pending",
         true <- reservation.dispatch.abort_config == context.config,
         :ok <- exact_config(context.config, assignment) do
      reconcile_ownership_state(reservation, assignment, input, context)
    else
      _ -> {:held, :pre_spawn_abort_ownership_unverified}
    end
  rescue
    _ -> {:held, :pre_spawn_abort_ownership_unverified}
  end

  defp reconcile_ownership_state(reservation, assignment, input, context) do
    case get_in(input, [:fence_state, :executions, assignment.lease.issue_id, :ownership]) do
      :reconciled ->
        {:ok, input.fence_state}

      :unknown ->
        reconcile_unknown_ownership(reservation, assignment, input, context)

      _ ->
        {:held, :pre_spawn_worker_state_unverified}
    end
  end

  defp reconcile_unknown_ownership(reservation, assignment, input, context) do
    with {:ok, candidate} <- ExecutionFence.reconcile_aborted_claim(input.fence_state, reservation),
         :ok <- unused_worker(%{input | fence_state: candidate}, assignment, reservation, false),
         {:ok, request} <- root_request(reservation, assignment, input, "verify_pre_execution_abort_eligibility"),
         :ok <- root_port(context).verify_eligibility(request) do
      {:ok, candidate}
    else
      {_, reason} -> {:held, reason}
    end
  end

  @doc "Continues the existing abort protocol only after the exact unused worker lease is durably released."
  @spec cleanup(map(), map(), map(), map(), map()) :: :ok | {:held, term()} | {:error, term()}
  def cleanup(reservation, assignment, input, context, host) do
    with :ok <- unused_worker(input, assignment, reservation, true),
         :ok <- publish(reservation, assignment, context) do
      caller = %{
        journal_root: host.abort_journal_root,
        workspace_root: host.workspace_root,
        adapter_context: context,
        witness_input: input,
        reservation: reservation,
        provider_context: %{base_url: host.provider_url, runner_token: host.runner_token},
        pre_execution_result: result(assignment),
        root_abort_input_publisher: root_port(context)
      }

      caller = test_caller_ports(caller, context)
      key = assignment.sha256 <> ":abort_unstarted"

      with {:ok, prepared} <- AbortPrepareCaller.prepare(allocation(reservation), assignment, key, caller) do
        AbortPrepareCaller.confirm(allocation(reservation), assignment, key, prepared, caller)
      end
    end
  rescue
    _ -> {:held, :pre_spawn_abort_cleanup_unverified}
  end

  @doc "Checks the exact never-started execution lease before releasing it, including replay after release."
  @spec unused_worker(map(), map(), map(), boolean()) :: :ok | {:held, atom()}
  def unused_worker(input, assignment, reservation, released?) do
    lease = assignment.lease
    execution = get_in(input, [:fence_state, :executions, lease.issue_id])
    worker = get_in(execution || %{}, [:leases, lease.session_id])
    registry = get_in(input, [:fence_state, :sessions, lease.session_id])

    if ExecutionFence.validate(input.fence_state) == :ok and
         unused_execution?(execution, assignment, lease) and
         unused_worker_identity?(worker, registry, assignment, reservation, lease) and
         unused_worker_state?(worker, released?) do
      :ok
    else
      {:held, :pre_spawn_worker_state_unverified}
    end
  rescue
    _ -> {:held, :pre_spawn_worker_state_unverified}
  end

  defp unused_execution?(execution, assignment, lease) when is_map(execution) do
    execution.issue_id == lease.issue_id and execution.branch == assignment.branch and
      execution.generation == lease.generation and execution.repository == assignment.repository_ref and
      execution.status == :active and execution.ownership == :reconciled and execution.cleanup == :pending and
      map_size(execution.leases) == 1 and is_nil(execution.terminal)
  end

  defp unused_execution?(_execution, _assignment, _lease), do: false

  defp unused_worker_identity?(worker, registry, assignment, reservation, lease) when is_map(worker) do
    worker == registry and worker.repository == assignment.repository_ref and worker.role == :worker and
      worker.generation == lease.generation and worker.issue_id == lease.issue_id and
      worker.session_id == lease.session_id and worker.process_id == lease.process_id and
      worker.branch == assignment.branch and reservation_session?(reservation, lease)
  end

  defp unused_worker_identity?(_worker, _registry, _assignment, _reservation, _lease), do: false

  defp reservation_session?(reservation, lease),
    do: reservation.session_id == lease.session_id and reservation.process_id == lease.process_id

  defp unused_worker_state?(worker, released?) when is_map(worker) do
    worker.head == "unobserved" and worker.last_heartbeat_at == 0 and
      is_nil(Map.get(worker, :supervisor_identity)) and
      Map.get(worker, :termination_required, false) == false and
      is_nil(Map.get(worker, :termination_confirmed_at_ms)) and
      is_nil(Map.get(worker, :termination_evidence)) and
      is_nil(Map.get(worker, :termination_evidence_ref)) and unused_status?(worker, released?)
  end

  defp unused_worker_state?(_worker, _released?), do: false

  defp unused_status?(%{status: :released, release_reason: reason}, _released?)
       when reason in [:spawn_failed, "spawn_failed"],
       do: true

  defp unused_status?(%{status: :active} = worker, false), do: is_nil(Map.get(worker, :release_reason))
  defp unused_status?(_worker, _released?), do: false

  @doc false
  @spec result(map()) :: map()
  def result(assignment) do
    %{
      assignment_digest: assignment.sha256,
      abort_reason: :codex_auth_slot_denied,
      outcome: :blocked,
      summary: Record.pre_execution_summary(:codex_auth_slot_denied),
      evidence_ref: "managed-executor:#{assignment.sha256}:codex_auth_slot_denied"
    }
  end

  defp root_request(reservation, assignment, input, operation) do
    with {:ok, %{"claim" => claim}} <- HostWitness.request(input, "claim_bound", reservation),
         claim = Map.put(claim, "pool", input.pool_key),
         {:ok, claim_hash} <- AbortPrepareJournal.identity_key(claim),
         {:ok, reference} <- AbortResultPublisher.reference_for_assignment(assignment) do
      {:ok,
       %{
         "schemaVersion" => 1,
         "operation" => operation,
         "claimSHA256" => claim_hash,
         "assignmentDigest" => assignment.sha256,
         "allocationId" => reservation.dispatch.allocation_id,
         "resultReference" => reference
       }}
    end
  end

  defp exact_config(config, assignment) do
    PreSpawnAbortSnapshot.validate(config, assignment)
  end

  defp root_port(context) do
    if @test_environment,
      do: Map.get(context, :root_abort_input_publisher, RootAbortInputPublisher),
      else: RootAbortInputPublisher
  end

  defp test_caller_ports(caller, context) do
    if @test_environment, do: Map.merge(caller, Map.get(context, :abort_caller_test_ports, %{})), else: caller
  end

  defp allocation(reservation), do: %{id: reservation.dispatch.allocation_id, status: :ready}

  defp reservation_key(r) do
    Journal.reservation_key(r.issue_id, r.managed_project_profile_id, r.repository_ref, r.generation)
  end
end
