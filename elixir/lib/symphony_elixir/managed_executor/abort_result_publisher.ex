defmodule SymphonyElixir.ManagedExecutor.AbortResultPublisher do
  @moduledoc """
  Implements the managed-executor pre-execution result callback with the private
  `AbortResultJournal`.

  The callback accepts only a complete validated assignment, its ready
  allocation, the assignment-derived idempotency key, a validated claim
  binding, an exact blocked-result map, and an explicitly supplied private
  journal root. It stores the Dahlia verifier's exact blocked-result JSON
  shape and returns its stable local reference. It does not contact a provider
  or attest cleanup release.
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
         {:ok, claim} <- claim_binding(context, assignment),
         {:ok, root} <- journal_root(context),
         binding = binding(assignment, allocation),
         reference = result_reference(idempotency_key),
         {:ok, result_bytes} <- Jason.encode(blocked_result(claim, assignment, allocation, result, reference)),
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

  defp claim_binding(%{claim_binding: claim}, assignment) when is_map(claim) do
    required = [:projection_id, :reservation_id, :runner_id, :issue_id, :generation, :repository_ref]

    if Enum.all?(required -- [:generation], fn key -> is_binary(Map.get(claim, key)) and Map.get(claim, key) != "" end) and
         is_integer(Map.get(claim, :generation)) and Map.get(claim, :generation) > 0 and
         Map.get(claim, :issue_id) == assignment.lease.issue_id and
         Map.get(claim, :generation) == assignment.lease.generation and
         Map.get(claim, :runner_id) == assignment.seat and
         Map.get(claim, :repository_ref) == assignment.repository_ref do
      {:ok, claim}
    else
      {:error, :abort_result_claim_binding_invalid}
    end
  end

  defp claim_binding(_context, _assignment), do: {:error, :abort_result_claim_binding_unavailable}

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

  defp blocked_result(claim, assignment, allocation, result, reference) do
    %{
      "status" => "blocked",
      "reference" => reference,
      "projectionId" => claim.projection_id,
      "reservationId" => claim.reservation_id,
      "issueId" => claim.issue_id,
      "runnerId" => claim.runner_id,
      "generation" => claim.generation,
      "assignmentDigest" => assignment.sha256,
      "allocationId" => allocation.id,
      "abortReason" => Atom.to_string(result.abort_reason)
    }
  end

  defp result_reference(idempotency_key) do
    digest = :crypto.hash(:sha256, idempotency_key) |> Base.encode16(case: :lower)
    "managed-abort-result:v1:" <> digest
  end
end
