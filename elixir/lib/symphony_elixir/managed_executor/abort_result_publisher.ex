defmodule SymphonyElixir.ManagedExecutor.AbortResultPublisher do
  @moduledoc """
  Implements the managed-executor pre-execution result callback with the private
  `AbortResultJournal`.

  The callback accepts only a complete validated assignment, its ready
  allocation, the assignment-derived idempotency key, an exact blocked-result
  map, and an explicitly supplied private journal root. It stores the
  deterministic JSON bytes and returns their stable local reference. It does
  not contact a provider or attest cleanup release.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.ManagedExecutor.{AbortResultJournal, Record}

  @abort_result_stage "abort-result"

  @spec publish_or_reconcile_abort_result(map(), map(), map(), String.t(), term()) ::
          {:ok, String.t()} | {:error, atom()}
  def publish_or_reconcile_abort_result(allocation, assignment, result, idempotency_key, context) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         :ok <- Record.validate_allocation(allocation),
         :ok <- validate_idempotency_key(idempotency_key, assignment),
         :ok <- validate_result(result, assignment),
         {:ok, root} <- journal_root(context),
         {:ok, result_bytes} <- Jason.encode(result),
         binding = binding(assignment, allocation),
         reference = result_reference(idempotency_key),
         {:ok, %{reference: ^reference}} <- AbortResultJournal.record(root, reference, binding, result_bytes) do
      {:ok, reference}
    else
      {:held, reason} -> {:error, reason}
      {:error, reason} when is_atom(reason) -> {:error, reason}
      _ -> {:error, :abort_result_publication_failed}
    end
  rescue
    _ -> {:error, :abort_result_publication_failed}
  end

  defp validate_idempotency_key(key, assignment) do
    if key == assignment.sha256 <> ":" <> @abort_result_stage,
      do: :ok,
      else: {:error, :abort_result_idempotency_key_mismatch}
  end

  defp validate_result(%{abort_reason: reason} = result, assignment) when is_atom(reason),
    do: Record.validate_pre_execution_result(result, assignment, reason)

  defp validate_result(_result, _assignment), do: {:error, :pre_execution_result_invalid}

  defp journal_root(%{abort_result_journal_root: root}) when is_binary(root) do
    if Path.type(root) == :absolute,
      do: {:ok, root},
      else: {:error, :invalid_abort_result_journal_root}
  end

  defp journal_root(_context), do: {:error, :abort_result_journal_root_unavailable}

  defp binding(assignment, allocation) do
    %{
      assignment_digest: assignment.sha256,
      issue_uuid: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      allocation_id: allocation.id
    }
  end

  defp result_reference(idempotency_key) do
    digest = :crypto.hash(:sha256, idempotency_key) |> Base.encode16(case: :lower)
    "managed-abort-result:v1:" <> digest
  end
end
