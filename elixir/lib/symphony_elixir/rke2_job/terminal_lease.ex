defmodule SymphonyElixir.RKE2Job.TerminalLease do
  @moduledoc """
  Converts an exact finalized disposable Job into execution-fence termination.

  The caller must first receive `TerminalOwner.reconcile/4` success, which means
  a terminated worker container was observed and journaled, the exact Job and
  Pods are absent, and its OAuth slot has been released. This module makes no
  Kubernetes or provider calls.
  """

  alias SymphonyElixir.{ExecutionFence, ManagedAssignmentBundle}
  alias SymphonyElixir.RKE2Job.ResultReader

  @uid ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/

  @doc "Releases and confirms the one worker lease for a finalized Job result."
  @spec confirm(map(), map(), map(), map(), non_neg_integer()) ::
          {:ok, map(), map()} | {:error, term()}
  def confirm(fence, reservation, assignment, observation, now_ms)
      when is_map(reservation) and is_map(assignment) and is_map(observation) and
             is_integer(now_ms) and now_ms >= 0 do
    token = %{issue_id: reservation.issue_id, generation: reservation.generation}

    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         %{target_environment: :rke2} <- assignment.environment,
         true <- exact_binding?(reservation, assignment),
         {:ok, result, exit_code, job_uid, pod_uid} <- validated_observation(observation, assignment),
         {:ok, lease} <- bound_lease(fence, reservation, assignment),
         evidence = evidence(reservation, assignment, job_uid, pod_uid, result["status"], exit_code, now_ms),
         true <- replay_identity_matches?(lease, evidence),
         {:ok, released, _} <- ExecutionFence.release(fence, token, reservation.session_id, :orchestrator_stop),
         {:ok, confirmed, _} <-
           ExecutionFence.confirm_termination(released, token, reservation.session_id, evidence, now_ms) do
      {:ok, confirmed, evidence}
    else
      {:error, _} = error -> error
      _ -> {:error, :disposable_terminal_lease_unverified}
    end
  rescue
    _ -> {:error, :disposable_terminal_lease_unverified}
  end

  def confirm(_, _, _, _, _), do: {:error, :disposable_terminal_lease_unverified}

  defp validated_observation(observation, assignment) do
    result = Map.get(observation, "result") || Map.get(observation, :result)
    exit_code = Map.get(observation, "exit_code") || Map.get(observation, :exit_code)
    job_uid = Map.get(observation, "job_uid") || Map.get(observation, :job_uid)
    pod_uid = Map.get(observation, "pod_uid") || Map.get(observation, :pod_uid)

    if valid_receipt?(result, exit_code, assignment) and valid_uid_pair?(job_uid, pod_uid),
      do: {:ok, result, exit_code, job_uid, pod_uid},
      else: {:error, :disposable_terminal_lease_unverified}
  end

  defp valid_receipt?(result, exit_code, assignment) when is_map(result),
    do: ResultReader.valid_receipt_outcome?(result, exit_code) and exact_result?(result, assignment)

  defp valid_receipt?(_, _, _), do: false

  defp valid_uid_pair?(job_uid, pod_uid), do: valid_uid?(job_uid) and valid_uid?(pod_uid)

  defp bound_lease(fence, reservation, assignment) do
    execution = get_in(fence, [:executions, reservation.issue_id])

    with %{generation: generation, repository: repository, branch: branch, leases: leases} <- execution,
         true <- generation == reservation.generation and repository == assignment.repository_ref,
         true <- branch == assignment.branch,
         %{process_id: process_id, branch: lease_branch, role: :worker} = lease <- leases[reservation.session_id],
         true <- process_id == reservation.process_id and lease_branch == assignment.branch do
      {:ok, lease}
    else
      _ -> {:error, :disposable_terminal_lease_unverified}
    end
  end

  defp exact_binding?(reservation, assignment) do
    reservation.issue_id == assignment.lease.issue_id and
      reservation.generation == assignment.lease.generation and
      reservation.repository_ref == assignment.repository_ref and
      reservation.session_id == assignment.lease.session_id and
      reservation.process_id == assignment.lease.process_id and
      reservation.runtime_lease_id == reservation.session_id and
      reservation.execution_fence_token == "#{reservation.issue_id}:#{reservation.generation}"
  end

  defp exact_result?(result, assignment) do
    result["assignment_digest"] == assignment.sha256 and
      result["issue_uuid"] == assignment.lease.issue_id and
      result["generation"] == assignment.lease.generation and
      result["repository_ref"] == assignment.repository_ref and
      result["branch_ref"] == "refs/heads/" <> assignment.branch
  end

  defp valid_uid?(value) when is_binary(value), do: Regex.match?(@uid, value)
  defp valid_uid?(_), do: false

  defp replay_identity_matches?(%{termination_evidence_ref: existing_ref, termination_evidence: existing}, evidence)
       when is_binary(existing_ref) and is_map(existing),
       do:
         existing_ref == evidence.evidence_ref and
           Map.take(existing, [:job_uid, :pod_uid, :assignment_digest, :terminal_status, :exit_code]) ==
             Map.take(evidence, [:job_uid, :pod_uid, :assignment_digest, :terminal_status, :exit_code])

  defp replay_identity_matches?(%{termination_evidence_ref: existing}, _evidence) when is_binary(existing), do: false

  defp replay_identity_matches?(_lease, _evidence), do: true

  defp evidence(reservation, assignment, job_uid, pod_uid, status, exit_code, now_ms) do
    result_id = :crypto.hash(:sha256, assignment.sha256 <> "\0" <> job_uid) |> Base.encode16(case: :lower)

    %{
      session_id: reservation.session_id,
      process_id: reservation.process_id,
      process_tree: :terminated,
      evidence_ref: "sha256:#{result_id}",
      observed_at_ms: now_ms,
      job_uid: job_uid,
      pod_uid: pod_uid,
      assignment_digest: assignment.sha256,
      terminal_status: status,
      exit_code: exit_code
    }
  end
end
