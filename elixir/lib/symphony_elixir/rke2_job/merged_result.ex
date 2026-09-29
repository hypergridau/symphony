defmodule SymphonyElixir.RKE2Job.MergedResult do
  @moduledoc """
  Binds a completed disposable Job result to its exact merged GitHub PR.

  The result is journaled before Job deletion. This check supplies a remote
  accepted head without assuming that a local worktree still exists.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.ResultReader

  @oid ~r/\A[a-f0-9]{40}\z/

  @doc "Validates one merged PR against the signed assignment and terminal result."
  @spec accept(map(), map(), map()) :: {:ok, map()} | {:error, :disposable_merge_unavailable}
  def accept(assignment, observation, pr) when is_map(assignment) and is_map(observation) and is_map(pr) do
    result = Map.get(observation, "result") || Map.get(observation, :result)
    exit_code = Map.get(observation, "exit_code") || Map.get(observation, :exit_code)

    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         %{target_environment: :rke2} <- assignment.environment,
         true <- is_map(result) and ResultReader.valid_receipt_outcome?(result, exit_code),
         true <- result["status"] == "completed" and exit_code == 0,
         true <- result["assignment_digest"] == assignment.sha256,
         true <- result["issue_uuid"] == assignment.lease.issue_id,
         true <- result["generation"] == assignment.lease.generation,
         true <- result["repository_ref"] == assignment.repository_ref,
         true <- result["branch_ref"] == "refs/heads/" <> assignment.branch,
         true <- pr["state"] == "MERGED",
         true <- pr["number"] == result["pull_request_number"],
         true <- pr["url"] == result["pull_request_url"],
         true <- pr["headRefName"] == assignment.branch,
         true <- pr["headRefOid"] == result["head_oid"],
         true <- pr["baseRefName"] == base_branch(assignment.base_ref),
         true <- oid?(get_in(pr, ["mergeCommit", "oid"])),
         true <- timestamp?(pr["mergedAt"]) do
      {:ok, %{accepted_head: result["head_oid"], merge_identity: pr["mergeCommit"]["oid"]}}
    else
      _ -> {:error, :disposable_merge_unavailable}
    end
  rescue
    _ -> {:error, :disposable_merge_unavailable}
  end

  def accept(_, _, _), do: {:error, :disposable_merge_unavailable}

  defp base_branch("refs/remotes/origin/" <> branch), do: branch
  defp base_branch(_), do: nil

  defp oid?(value) when is_binary(value), do: Regex.match?(@oid, value)
  defp oid?(_), do: false

  defp timestamp?(value) when is_binary(value), do: match?({:ok, _, _}, DateTime.from_iso8601(value))
  defp timestamp?(_), do: false
end
