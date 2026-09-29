defmodule SymphonyElixir.RKE2Job.DisposableCleanupEvidence do
  @moduledoc """
  Verifies the retained disposable teardown against its claim and terminal fence.

  The Job owns its ephemeral checkout. Its exact deletion, Pod absence and OAuth
  slot release are recorded by the trusted host finalizer, so no local workspace
  archive is fabricated for this execution mode.
  """

  alias SymphonyElixir.{ExecutionFence, ManagedAssignmentBundle}
  alias SymphonyElixir.RKE2Job.ResultJournal
  alias SymphonyElixir.WorkPackageClaim.Journal

  @doc "Finds the exact retained disposable reservation, or a local claim on a mixed host."
  @spec reservation(map(), map()) :: {:ok, map()} | :local | {:error, term()}
  def reservation(runtime, token) when is_map(runtime) and is_map(token) do
    with path when is_binary(path) <- Map.get(runtime, :journal_path),
         profile when is_binary(profile) <- Map.get(runtime, :managed_project_profile_id),
         repository when is_binary(repository) <- Map.get(token, :repository_ref),
         issue when is_binary(issue) <- Map.get(token, :issue_id),
         generation when is_integer(generation) and generation > 0 <- Map.get(token, :generation),
         {:ok, journal} <- Journal.load(path),
         key = Journal.reservation_key(issue, profile, repository, generation),
         saved when is_map(saved) <- Map.get(journal.reservations, key) do
      case Map.get(saved, :dispatch) do
        %{phase: "spawn_started"} -> classify_snapshot(saved)
        %{phase: "allocation_suspended"} -> classify_suspended_snapshot(saved)
        _ -> :local
      end
    else
      {:error, reason} -> {:error, reason}
      _ -> {:error, :disposable_cleanup_reservation_missing}
    end
  rescue
    _ -> {:error, :disposable_cleanup_reservation_unavailable}
  end

  def reservation(_runtime, _token), do: {:error, :disposable_cleanup_reservation_unavailable}

  defp classify_snapshot(%{assignment_snapshot: snapshot} = saved) when is_binary(snapshot) do
    case ManagedAssignmentBundle.from_snapshot(snapshot) do
      {:ok, %{environment: %{target_environment: :rke2}}} -> {:ok, saved}
      _ -> {:error, :disposable_cleanup_assignment_invalid}
    end
  end

  defp classify_snapshot(%{dispatch: %{allocation_id: "rke2job:v1:" <> _}}),
    do: {:error, :disposable_cleanup_assignment_missing}

  defp classify_snapshot(_saved), do: :local

  defp classify_suspended_snapshot(%{assignment_snapshot: snapshot}) when is_binary(snapshot),
    do: {:error, :disposable_cleanup_not_started}

  defp classify_suspended_snapshot(%{dispatch: %{allocation_id: "rke2job:v1:" <> _}}),
    do: {:error, :disposable_cleanup_assignment_missing}

  defp classify_suspended_snapshot(_saved), do: :local

  @doc "Returns a bounded receipt reference only for an exact finalized disposable run."
  @spec verify(map(), map(), map(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def verify(runtime, fence, token, head) when is_map(runtime) and is_map(token) and is_binary(head) do
    with :ok <- ExecutionFence.validate_cleanup(fence, token, head),
         {:ok, saved} <- reservation(runtime, token),
         {:ok, assignment} <- ManagedAssignmentBundle.from_snapshot(saved.assignment_snapshot),
         %{environment: %{target_environment: :rke2}} <- assignment,
         %{result_journal_root: root} when is_binary(root) <- Map.get(runtime, :disposable_rke2_host_config),
         execution when is_map(execution) <- get_in(fence, [:executions, token.issue_id]),
         lease when is_map(lease) <- get_in(execution, [:leases, saved.session_id]),
         :ok <- exact_authority(saved, assignment, execution, lease),
         evidence when is_map(evidence) <- Map.get(lease, :termination_evidence),
         job_uid when is_binary(job_uid) <- Map.get(evidence, :job_uid),
         {:ok, observation} <- ResultJournal.load(assignment, job_uid, root),
         {:ok, marker} <- ResultJournal.load_finalization(assignment, job_uid, root),
         :ok <- exact_result(assignment, observation, marker, evidence, lease, head),
         true <- is_binary(get_in(execution, [:terminal, :merge_identity])) do
      seed = assignment.sha256 <> "\0" <> job_uid <> "\0" <> head <> "\0" <> marker["result_sha256"]
      evidence_ref = "sha256:" <> (:crypto.hash(:sha256, seed) |> Base.encode16(case: :lower))

      if fence_evidence_matches?(execution, head, evidence_ref),
        do: {:ok, evidence_ref},
        else: {:error, :disposable_cleanup_evidence_unverified}
    else
      _ -> {:error, :disposable_cleanup_evidence_unverified}
    end
  rescue
    _ -> {:error, :disposable_cleanup_evidence_unverified}
  end

  def verify(_runtime, _fence, _token, _head), do: {:error, :disposable_cleanup_evidence_unverified}

  defp exact_authority(saved, assignment, execution, lease) do
    job_uid = get_in(lease, [:termination_evidence, :job_uid]) || ""
    expected_ref = :crypto.hash(:sha256, assignment.sha256 <> "\0" <> job_uid)
    expected_ref = "sha256:" <> Base.encode16(expected_ref, case: :lower)

    saved_identity = Map.take(saved, [:issue_id, :repository_ref, :generation, :session_id, :process_id])

    expected_identity = %{
      issue_id: assignment.lease.issue_id,
      repository_ref: assignment.repository_ref,
      generation: assignment.lease.generation,
      session_id: assignment.lease.session_id,
      process_id: assignment.lease.process_id
    }

    execution_identity = %{
      issue_id: execution.issue_id,
      repository_ref: execution.repository,
      generation: execution.generation,
      session_id: lease.session_id,
      process_id: lease.process_id
    }

    lease_ready =
      lease.role == :worker and lease.status in [:released, :expired] and
        is_integer(lease.termination_confirmed_at_ms) and lease.termination_evidence_ref == expected_ref

    control_identity = {saved.runtime_lease_id, saved.execution_fence_token, execution.branch}
    expected_control = {saved.session_id, "#{saved.issue_id}:#{saved.generation}", assignment.branch}

    if saved_identity == expected_identity and saved_identity == execution_identity and
         control_identity == expected_control and lease_ready,
       do: :ok,
       else: {:error, :disposable_cleanup_authority_mismatch}
  end

  defp exact_result(assignment, observation, marker, evidence, lease, head) do
    result = observation["result"]

    expected_marker = %{
      "phase" => "job_and_pods_absent_auth_slot_released",
      "assignment_digest" => assignment.sha256,
      "job_uid" => evidence.job_uid,
      "pod_uid" => evidence.pod_uid
    }

    expected_evidence = %{
      assignment_digest: assignment.sha256,
      process_tree: :terminated,
      session_id: lease.session_id,
      process_id: lease.process_id,
      terminal_status: "completed",
      exit_code: observation["exit_code"]
    }

    result_revoked =
      Map.take(result, ["status", "checkout_revocation", "revocation"]) ==
        %{"status" => "completed", "checkout_revocation" => "confirmed", "revocation" => "confirmed"}

    if Map.take(marker, Map.keys(expected_marker)) == expected_marker and
         Map.take(evidence, Map.keys(expected_evidence)) == expected_evidence and
         observation["pod_uid"] == evidence.pod_uid and result["head_oid"] == head and result_revoked,
       do: :ok,
       else: {:error, :disposable_cleanup_result_mismatch}
  end

  defp fence_evidence_matches?(%{cleanup: :pending, cleanup_receipt: nil}, _head, _ref), do: true

  defp fence_evidence_matches?(%{cleanup: :pending, cleanup_receipt: receipt}, head, ref) when is_map(receipt) do
    receipt.phase == :removal_started and receipt.expected_head == head and
      Map.get(receipt, :evidence_ref) in [nil, ref]
  end

  defp fence_evidence_matches?(%{cleanup: :cleaned, cleanup_receipt: receipt}, head, ref) when is_map(receipt),
    do: receipt.phase == :verified and receipt.expected_head == head and receipt.evidence_ref == ref

  defp fence_evidence_matches?(_execution, _head, _ref), do: false
end
