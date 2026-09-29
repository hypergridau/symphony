defmodule SymphonyElixir.RKE2Job.MergedResultEvidence do
  @moduledoc "Reads an exact disposable PR from GitHub on the trusted host."

  alias SymphonyElixir.RKE2Job.MergedResult

  @fields "number,url,state,headRefName,headRefOid,baseRefName,mergeCommit,mergedAt"
  @output_limit 32_768

  @doc "Reads one PR by its receipt number and verifies its merged identity."
  @spec observe(map(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def observe(assignment, observation, opts \\ []) do
    result = Map.get(observation, "result") || Map.get(observation, :result)
    number = if is_map(result), do: result["pull_request_number"]
    repository = if is_map(assignment), do: assignment[:repository_ref]

    with true <- is_integer(number) and number > 0 and safe_repository?(repository),
         runner when is_function(runner, 2) <- Keyword.get(opts, :command_runner, &bounded_command/2),
         {output, 0} when is_binary(output) and byte_size(output) <= @output_limit <-
           runner.("gh", ["pr", "view", Integer.to_string(number), "--repo", repository, "--json", @fields]),
         {:ok, pr} when is_map(pr) <- Jason.decode(output) do
      MergedResult.accept(assignment, observation, pr)
    else
      _ -> {:error, :disposable_merge_evidence_unavailable}
    end
  rescue
    _ -> {:error, :disposable_merge_evidence_unavailable}
  end

  defp bounded_command(executable, args) do
    case :os.type() do
      {:unix, :linux} -> System.cmd("timeout", ["--kill-after=1s", "15s", executable | args], stderr_to_stdout: true)
      _ -> {"", 126}
    end
  end

  defp safe_repository?(value) when is_binary(value),
    do: Regex.match?(~r/\A[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+\z/, value)

  defp safe_repository?(_), do: false
end
