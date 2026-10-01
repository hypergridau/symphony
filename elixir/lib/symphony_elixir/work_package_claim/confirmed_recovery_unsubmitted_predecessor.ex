defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryUnsubmittedPredecessor do
  @moduledoc "Verifies the v3 retirement-only predecessor without fabricating a provider claim."

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence

  @execution_fields ~w(issue_id repository worker_host generation branch worktree status ownership leases terminal retirement cleanup cleanup_receipt termination_unconfirmed admitted_at_ms cleaned_at_ms)
  @lease_fields ~w(issue_id repository generation role session_id process_id branch worktree status registered_at_ms last_heartbeat_at linear_state pr_state head termination_required termination_confirmed_at_ms termination_evidence_ref termination_evidence supervisor_identity release_reason)
  @receipt_fields ~w(active_process evidence_ref generation issue_id linear_state local_claim provider_claim provider_projection_id retired_at_ms workspace type repository_ref managed_project_profile_id prior_accountable_id prior_responsible_id prior_accountable_digest prior_responsible_digest successor_accountable_id successor_responsible_id successor_accountable_digest successor_responsible_digest manifest_sha256 signer_key_sha256 observation_sha256)
  @grant_fields ~w(id parent_delegation_id role actor_id scope authority budget expires_at_ms expected_deliverable expected_evidence return_to_parent)a
  @derived_fields ~w(active_process linear_state local_claim provider_claim provider_projection_id retired_at_ms workspace)
  @retirement_reasons [:unsubmitted_successor, "unsubmitted_successor"]
  @unstarted_optional ~w(release_reason supervisor_identity termination_confirmed_at_ms termination_evidence_ref termination_evidence)a

  @spec validate(map(), map()) :: :ok | {:error, :invalid_confirmed_recovery_evidence}
  def validate(%{"execution" => execution, "claim" => nil, "receipt" => receipt} = predecessor, current)
      when map_size(predecessor) == 3 do
    with true <- keys?(execution, @execution_fields) and keys?(receipt, @receipt_fields),
         true <- execution_matches?(execution, receipt, current),
         true <- receipt_matches?(receipt, current),
         [{session, lease}] <- Map.to_list(execution["leases"]),
         true <- retired_lease?(lease, session, execution, current) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  rescue
    _ -> {:error, :invalid_confirmed_recovery_evidence}
  end

  def validate(_predecessor, _current), do: {:error, :invalid_confirmed_recovery_evidence}

  defp execution_matches?(execution, receipt, current) do
    execution["issue_id"] == current["issueId"] and execution["generation"] == 1 and
      execution["repository"] == current["repositoryRef"] and is_nil(execution["worker_host"]) and
      execution["retirement"] == receipt and execution["cleaned_at_ms"] == receipt["retired_at_ms"] and
      retired_execution?(execution)
  end

  defp retired_execution?(execution) do
    execution["status"] == "retired" and execution["cleanup"] == "cleaned" and
      execution["ownership"] in ["unknown", "reconciled"] and is_nil(execution["terminal"]) and
      is_nil(execution["cleanup_receipt"]) and execution["termination_unconfirmed"] == false
  end

  defp receipt_matches?(receipt, current) do
    receipt_identity?(receipt, current) and receipt_absence?(receipt) and receipt_hashes?(receipt) and
      successor_identity?(receipt, current)
  end

  defp receipt_identity?(receipt, current) do
    receipt["type"] == "unsubmitted_successor" and receipt["generation"] == 1 and
      receipt["issue_id"] == current["issueId"] and receipt["repository_ref"] == current["repositoryRef"] and
      receipt["managed_project_profile_id"] == current["managedProjectProfileId"] and
      receipt["provider_projection_id"] == current["projectionId"] and
      is_integer(receipt["retired_at_ms"]) and receipt["retired_at_ms"] > 0
  end

  defp receipt_absence?(receipt),
    do: Enum.all?(~w(active_process local_claim provider_claim workspace), &(receipt[&1] == "absent"))

  defp receipt_hashes?(receipt) do
    fields = ~w(manifest_sha256 signer_key_sha256 observation_sha256 prior_accountable_digest prior_responsible_digest successor_accountable_digest successor_responsible_digest)

    Evidence.retirement_evidence_ref(receipt) == receipt["evidence_ref"] and
      Enum.all?(fields, &digest?(receipt[&1])) and text?(receipt["linear_state"])
  end

  defp successor_identity?(receipt, current) do
    identifiers = Enum.map(~w(prior_accountable_id prior_responsible_id successor_accountable_id successor_responsible_id), &receipt[&1])

    receipt["successor_responsible_id"] == current["responsibleDelegationId"] and
      Enum.all?(identifiers, &text?/1) and length(Enum.uniq(identifiers)) == 4
  end

  defp retired_lease?(lease, session, execution, current) do
    keys?(lease, @lease_fields) and retired_lease_identity?(lease, session, execution, current) and
      lease["status"] == "released" and lease["release_reason"] == "claim_not_submitted" and unobserved?(lease)
  end

  defp retired_lease_identity?(lease, session, execution, current) do
    text?(session) and lease["session_id"] == session and text?(lease["process_id"]) and
      lease["issue_id"] == current["issueId"] and lease["repository"] == current["repositoryRef"] and
      lease["generation"] == 1 and lease["role"] == "worker" and lease_workspace?(lease, execution)
  end

  defp lease_workspace?(lease, execution),
    do: lease["branch"] == execution["branch"] and lease["worktree"] == execution["worktree"] and text?(execution["worktree"])

  @spec persisted(map(), map(), map()) :: :ok | {:error, :predecessor_retirement_not_persisted}
  def persisted(paths, %{"execution" => execution, "claim" => nil, "receipt" => receipt} = predecessor, current) do
    with :ok <- validate(predecessor, current),
         true <- Enum.all?(Map.values(paths.journal.state.reservations), &(&1.issue_id != current["issueId"] or &1.generation == 2)),
         {:ok, encoded} <- FencePersistence.encode_bytes(paths.fence.state),
         {:ok, fence} <- Jason.decode(encoded),
         [^execution] <- Enum.filter(fence["history"], &(&1["issue_id"] == current["issueId"])),
         :ok <- graph_receipt(paths.graph.state, receipt, current) do
      :ok
    else
      _ -> {:error, :predecessor_retirement_not_persisted}
    end
  rescue
    _ -> {:error, :predecessor_retirement_not_persisted}
  end

  def persisted(_paths, _predecessor, _current), do: {:error, :predecessor_retirement_not_persisted}

  defp graph_receipt(graph, receipt, current) do
    rows = Enum.map(~w(prior_accountable_id prior_responsible_id successor_accountable_id successor_responsible_id), &Map.fetch!(graph.delegations, receipt[&1]))
    [prior_a, prior_r, next_a, next_r] = rows
    evidence = prior_a.terminal_evidence

    with true <- revoked_pair?(prior_a, prior_r),
         true <- blocked_pair?(next_a, next_r),
         true <- pair_identity?(rows),
         true <- evidence == prior_r.terminal_evidence,
         true <- source_receipt?(evidence, receipt),
         true <- Enum.all?(rows, &(&1.scope.issue_id == current["issueId"] and &1.scope.repository == current["repositoryRef"])),
         true <- grant_hashes?(rows, receipt),
         true <- native_digest(evidence["observation"]) == receipt["observation_sha256"] do
      :ok
    else
      _ -> {:error, :predecessor_retirement_not_persisted}
    end
  end

  defp revoked_pair?(accountable, responsible) do
    accountable.status == :revoked and responsible.status == :revoked and
      accountable.terminal_reason in @retirement_reasons and responsible.terminal_reason in @retirement_reasons and
      is_nil(accountable.runtime_lease) and is_nil(responsible.runtime_lease)
  end

  defp blocked_pair?(accountable, responsible) do
    accountable.status == :blocked and responsible.status == :blocked and
      accountable.blocked_on == :restart_reconciliation and responsible.blocked_on == :restart_reconciliation and
      is_nil(accountable.runtime_lease) and is_nil(accountable.terminal_reason) and is_nil(responsible.terminal_reason)
  end

  defp pair_identity?([prior_a, prior_r, next_a, next_r]) do
    prior_a.role == :accountable and prior_r.role == :responsible and
      next_a.role == :accountable and next_r.role == :responsible and
      prior_r.parent_delegation_id == prior_a.id and next_r.parent_delegation_id == next_a.id
  end

  defp source_receipt?(evidence, receipt) do
    shared = @receipt_fields -- @derived_fields

    keys?(evidence, shared ++ ~w(observation prepared_at_ms)) and
      Map.take(evidence, shared) == Map.take(receipt, shared) and
      evidence["prepared_at_ms"] == receipt["retired_at_ms"] and
      evidence["observation"]["provider_projection_id"] == receipt["provider_projection_id"] and
      evidence["observation"]["workspace_absent"] == true and evidence["observation"]["process_count"] == 0
  end

  defp grant_hashes?(rows, receipt) do
    Enum.zip(rows, ~w(prior_accountable_digest prior_responsible_digest successor_accountable_digest successor_responsible_digest))
    |> Enum.all?(fn {row, field} -> native_digest(Map.take(row, @grant_fields)) == receipt[field] end)
  end

  @spec blocked_lease(map(), map()) :: :ok | {:error, :runtime_lease_mismatch}
  def blocked_lease(graph, expected) do
    lease = %{
      issue_id: expected["issueId"],
      repository: expected["repositoryRef"],
      generation: 2,
      session_id: expected["sessionId"],
      process_id: expected["processId"]
    }

    case graph.delegations[expected["responsibleDelegationId"]] do
      %{status: :blocked, blocked_on: :restart_reconciliation, runtime_lease: ^lease} -> :ok
      _ -> {:error, :runtime_lease_mismatch}
    end
  end

  @spec reconciled_fence_candidate(map(), map()) :: {:ok, map()} | {:error, :execution_lease_mismatch}
  def reconciled_fence_candidate(fence, reservation) do
    case fence.executions[reservation.issue_id] do
      %{ownership: :unknown, leases: leases} = execution when map_size(leases) == 1 ->
        with lease when is_map(lease) <- leases[reservation.session_id],
             true <- execution.branch == lease.branch and execution.worktree == lease.worktree,
             true <- Enum.all?(@unstarted_optional, &is_nil(Map.get(lease, &1))),
             {:ok, candidate} <- ExecutionFence.reconcile_unstarted_claim(fence, reservation) do
          {:ok, candidate}
        else
          _ -> {:error, :execution_lease_mismatch}
        end

      _ ->
        {:error, :execution_lease_mismatch}
    end
  end

  defp unobserved?(lease) do
    lease["head"] == "unobserved" and lease["last_heartbeat_at"] == 0 and lease["termination_required"] == false and
      Enum.all?(~w(termination_confirmed_at_ms termination_evidence_ref termination_evidence supervisor_identity), &is_nil(lease[&1]))
  end

  defp native_digest(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic])) |> Base.encode16(case: :lower)
  defp keys?(value, fields), do: is_map(value) and Enum.sort(Map.keys(value)) == Enum.sort(fields)
  defp text?(value), do: is_binary(value) and byte_size(value) > 0
  defp digest?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
end
