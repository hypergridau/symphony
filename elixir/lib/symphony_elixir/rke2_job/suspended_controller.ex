defmodule SymphonyElixir.RKE2Job.SuspendedController do
  @moduledoc """
  Composes the accepted claim journal with the suspended RKE2 Job adapter.

  Allocation may be retried with the same assignment digest after an uncertain
  response. A Job stays suspended until the orchestrator calls `resume/3` from
  its serialized admission callback. That callback owns the final pause
  decision; the root claim witness and adapter activation guard independently
  revalidate authority before the exact Job UID is activated.

  This module does not own worker results or terminal cleanup. Callers must
  retain the claim and allocation for reconciliation until those paths finish.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.ManagedExecutorAdapter
  alias SymphonyElixir.WorkPackageClaim
  alias SymphonyElixir.WorkPackageClaim.Handoff
  alias SymphonyElixir.WorkPackageClaim.Journal

  @binding_fields [
    :projection_id,
    :reservation_id,
    :workspace_id,
    :company_id,
    :issue_id,
    :runner_id,
    :managed_project_profile_id,
    :repository_ref,
    :generation,
    :session_id,
    :process_id,
    :responsible_delegation_id,
    :execution_fence_token,
    :runtime_lease_id,
    :scope_keys
  ]

  @type result :: {:ok, map()} | {:held, term()} | {:error, term()}

  @doc "Allocates or reconciles one suspended Job, then journals its exact ID."
  @spec allocate(map(), map(), map()) :: result()
  def allocate(assignment, claim_input, context)
      when is_map(assignment) and is_map(claim_input) and is_map(context) do
    with :ok <- preflight(assignment, claim_input, context),
         :ok <- WorkPackageClaim.record_assignment_snapshot(claim_input, assignment),
         {:ok, adapter} <- adapter(context),
         {:ok, allocation} <-
           adapter.allocate_or_reconcile(
             assignment,
             operation_key(assignment, :allocation),
             context
           ) do
      case WorkPackageClaim.record_suspended_allocation(claim_input, allocation) do
        :ok -> {:ok, allocation}
        {:error, reason} -> {:held, {:suspended_allocation_journal_failed, allocation.id, reason}}
      end
    end
  end

  def allocate(_assignment, _claim_input, _context), do: {:error, :invalid_suspended_controller_input}

  @doc "Performs the first create call after the caller durably records and locks allocation_pending."
  @spec allocate_pending(map(), map(), map()) :: result()
  def allocate_pending(assignment, claim_input, context)
      when is_map(assignment) and is_map(claim_input) and is_map(context) do
    with :ok <- validate_inputs(assignment, claim_input, context),
         :ok <- WorkPackageClaim.record_pending_assignment_snapshot(claim_input, assignment),
         {:ok, adapter} <- adapter(context),
         {:ok, allocation} <-
           adapter.allocate_or_reconcile(
             assignment,
             operation_key(assignment, :allocation),
             context
           ) do
      case WorkPackageClaim.record_suspended_allocation(claim_input, allocation) do
        :ok -> {:ok, allocation}
        {:error, reason} -> {:held, {:suspended_allocation_journal_failed, allocation.id, reason}}
      end
    end
  end

  def allocate_pending(_assignment, _claim_input, _context), do: {:error, :invalid_suspended_controller_input}

  @doc "Validates the exact signed claim and journal before host side effects."
  @spec preflight(map(), map(), map()) :: :ok | {:error, term()}
  def preflight(assignment, claim_input, context)
      when is_map(assignment) and is_map(claim_input) and is_map(context) do
    with :ok <- validate_inputs(assignment, claim_input, context) do
      WorkPackageClaim.prepare_suspended_allocation(claim_input)
    end
  end

  def preflight(_assignment, _claim_input, _context),
    do: {:error, :invalid_suspended_controller_input}

  @doc "Replays the exact claim intent before guarded activation of its Job UID."
  @spec resume(map(), map(), map()) :: result()
  def resume(assignment, claim_input, context)
      when is_map(assignment) and is_map(claim_input) and is_map(context) do
    with :ok <- validate_inputs(assignment, claim_input, context),
         :ok <- journal_assignment_snapshot(claim_input, assignment),
         {:ok, adapter} <- adapter(context),
         {:ok, dispatch} <- WorkPackageClaim.handoff_allocation(claim_input) do
      Handoff.resume(dispatch, %{
        begin_intent: fn allocation_id ->
          WorkPackageClaim.begin_suspended_spawn(claim_input, allocation_id)
        end,
        reconcile_intent: fn allocation_id ->
          if allocation_id == dispatch.allocation_id,
            do: WorkPackageClaim.replay_spawn_intent(claim_input),
            else: {:held, :suspended_allocation_identity_changed}
        end,
        activate: fn allocation_id ->
          adapter.activate_owned(
            %{id: allocation_id, status: :ready},
            assignment,
            operation_key(assignment, :activate),
            context
          )
        end
      })
    end
  end

  def resume(_assignment, _claim_input, _context), do: {:error, :invalid_suspended_controller_input}

  defp validate_inputs(assignment, claim_input, context) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         :ok <- rke2_assignment(assignment),
         binding = Map.get(context, :claim_binding),
         :ok <- exact_claim_binding(assignment, claim_input, binding) do
      journal_claim_binding(claim_input, binding)
    end
  end

  defp rke2_assignment(%{environment: %{target_environment: :rke2}}), do: :ok
  defp rke2_assignment(_assignment), do: {:error, :suspended_controller_target_invalid}

  defp exact_claim_binding(
         %{lease: lease, repository_ref: repository_ref},
         %{
           issue_id: issue_id,
           repository_ref: repository_ref,
           runner_id: runner_id,
           managed_project_profile_id: profile_id
         },
         %{
           issue_id: issue_id,
           repository_ref: repository_ref,
           runner_id: runner_id,
           managed_project_profile_id: profile_id
         } = binding
       ) do
    if Map.get(binding, :generation) == lease.generation and
         Map.get(binding, :session_id) == lease.session_id and
         Map.get(binding, :process_id) == lease.process_id and
         lease.issue_id == issue_id do
      :ok
    else
      {:error, :suspended_controller_claim_binding_invalid}
    end
  end

  defp exact_claim_binding(_assignment, _claim_input, _binding),
    do: {:error, :suspended_controller_claim_binding_invalid}

  defp journal_claim_binding(
         %{
           journal_path: path,
           issue_id: issue_id,
           managed_project_profile_id: profile_id,
           repository_ref: repository_ref
         },
         %{generation: generation, nonce_sha256: nonce_sha256} = binding
       )
       when is_binary(path) and is_integer(generation) and generation > 0 and
              is_binary(nonce_sha256) do
    key = Journal.reservation_key(issue_id, profile_id, repository_ref, generation)

    with {:ok, journal} <- Journal.load(path),
         %{reservation_nonce: nonce} = reservation <- Map.get(journal.reservations, key),
         true <- Map.take(reservation, @binding_fields) == Map.take(binding, @binding_fields),
         true <- sha256(nonce) == nonce_sha256 do
      :ok
    else
      _ -> {:error, :suspended_controller_claim_binding_invalid}
    end
  end

  defp journal_claim_binding(_claim_input, _binding),
    do: {:error, :suspended_controller_claim_binding_invalid}

  defp journal_assignment_snapshot(
         %{
           journal_path: path,
           issue_id: issue_id,
           managed_project_profile_id: profile_id,
           repository_ref: repository_ref
         },
         %{lease: %{generation: generation}} = assignment
       )
       when is_binary(path) and is_integer(generation) and generation > 0 do
    key = Journal.reservation_key(issue_id, profile_id, repository_ref, generation)

    with {:ok, journal} <- Journal.load(path),
         %{assignment_snapshot: snapshot} <- Map.get(journal.reservations, key),
         {:ok, ^assignment} <- ManagedAssignmentBundle.from_snapshot(snapshot) do
      :ok
    else
      _ -> {:error, :suspended_controller_assignment_snapshot_changed}
    end
  end

  defp journal_assignment_snapshot(_claim_input, _assignment),
    do: {:error, :suspended_controller_assignment_snapshot_changed}

  defp adapter(context) do
    candidate = Map.get(context, :adapter, ManagedExecutorAdapter)

    if is_atom(candidate) and Code.ensure_loaded?(candidate) and
         function_exported?(candidate, :allocate_or_reconcile, 3) and
         function_exported?(candidate, :activate_owned, 4),
       do: {:ok, candidate},
       else: {:error, :suspended_controller_adapter_unavailable}
  end

  defp operation_key(%{sha256: digest}, operation), do: digest <> ":" <> Atom.to_string(operation)

  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
