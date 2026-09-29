defmodule SymphonyElixir.ResponsibilityGraph.DisposableReviewCompletion do
  @moduledoc "Completes a disposable generation after its exact remote merge and terminal lease are verified."

  alias SymphonyElixir.{ExecutionFence, ResponsibilityGraph}

  @identity [:issue_id, :repository, :generation, :session_id, :process_id]

  @doc "Completes only the responsible leaf bound to the retained disposable claim."
  @spec complete(map(), map(), map(), map(), non_neg_integer()) :: {:ok, map(), map()} | {:error, term()}
  def complete(graph, fence, entry, evidence, now_ms) do
    with %{execution_token: token, review_reservation: reservation, responsibility_delegation_id: id} <- entry,
         %{accepted_head: head, merge_identity: merge} <- evidence,
         :ok <- ExecutionFence.validate_cleanup(fence, token, head),
         execution when is_map(execution) <- get_in(fence, [:executions, token.issue_id]),
         lease when is_map(lease) <- execution.leases[reservation.session_id],
         :ok <- ResponsibilityGraph.validate(graph),
         delegation when is_map(delegation) <- graph.delegations[id],
         true <- exact_identity?(execution, lease, delegation, reservation, entry),
         true <- execution.terminal.merge_identity == merge do
      complete_delegation(graph, delegation, evidence, now_ms)
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :disposable_review_identity_mismatch}
    end
  end

  defp exact_identity?(execution, lease, delegation, reservation, entry) do
    expected = %{
      issue_id: execution.issue_id,
      repository: execution.repository,
      generation: execution.generation,
      session_id: reservation.session_id,
      process_id: reservation.process_id
    }

    reservation_identity?(reservation, execution, entry) and
      lease_identity?(lease, expected) and
      delegation_identity?(delegation, reservation, execution, entry, expected)
  end

  defp reservation_identity?(reservation, execution, entry) do
    match?(%{phase: "spawn_started", allocation_id: id} when is_binary(id), Map.get(reservation, :dispatch)) and
      is_binary(Map.get(reservation, :assignment_snapshot)) and
      reservation.issue_id == execution.issue_id and reservation.repository_ref == execution.repository and
      reservation.generation == execution.generation and
      reservation.execution_fence_token == "#{execution.issue_id}:#{execution.generation}" and
      reservation.runtime_lease_id == reservation.session_id and
      entry.execution_session_id == reservation.session_id and entry.process_id == reservation.process_id
  end

  defp lease_identity?(lease, expected) do
    Map.take(lease, @identity) == expected and lease.role == :worker and
      lease.status in [:released, :expired] and
      String.starts_with?(lease[:termination_evidence_ref] || "", "sha256:") and
      is_map(lease[:termination_evidence]) and is_binary(lease.termination_evidence[:job_uid]) and
      is_binary(lease.termination_evidence[:pod_uid]) and
      is_binary(lease.termination_evidence[:assignment_digest])
  end

  defp delegation_identity?(delegation, reservation, execution, entry, expected) do
    delegation.role == :responsible and delegation.id == reservation.responsible_delegation_id and
      delegation.id == entry.responsibility_delegation_id and
      delegation.scope.repository == execution.repository and
      delegation.scope.issue_id in [execution.issue_id, entry.issue.identifier] and
      (is_nil(delegation.runtime_lease) or Map.take(delegation.runtime_lease, @identity) == expected)
  end

  defp complete_delegation(graph, %{status: :completed} = delegation, evidence, _now_ms) do
    if evidence_equal?(delegation.terminal_evidence, evidence),
      do: {:ok, graph, %{}},
      else: {:error, :disposable_review_evidence_changed}
  end

  defp complete_delegation(graph, %{status: :active} = delegation, evidence, now_ms),
    do: complete_leaf(graph, delegation.id, evidence, now_ms)

  defp complete_delegation(graph, %{status: :blocked, blocked_on: :restart_reconciliation} = delegation, evidence, now_ms) do
    temporary = put_in(graph, [:delegations, delegation.id], %{delegation | status: :active, blocked_on: nil})
    complete_leaf(temporary, delegation.id, evidence, now_ms)
  end

  defp complete_delegation(_, _, _, _), do: {:error, :disposable_review_not_eligible}

  defp complete_leaf(graph, id, evidence, now_ms) do
    with {:ok, completed, impact} <- ResponsibilityGraph.complete(graph, id, evidence, now_ms),
         true <- Map.delete(completed.delegations, id) == Map.delete(graph.delegations, id) do
      {:ok, completed, impact}
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :disposable_review_has_active_descendants}
    end
  end

  defp evidence_equal?(retained, evidence) when is_map(retained) do
    Enum.all?([:terminal_state, :accepted_head, :merge_identity], fn key ->
      Map.get(retained, key, Map.get(retained, Atom.to_string(key))) == evidence[key]
    end)
  end

  defp evidence_equal?(_, _), do: false
end
