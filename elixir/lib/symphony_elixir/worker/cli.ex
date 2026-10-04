defmodule SymphonyElixir.Worker.CLI do
  @moduledoc "Fixed command-line entrypoint for a disposable assignment or tokenless OAuth-cache check."

  alias SymphonyElixir.RKE2Job.JobSpec
  alias SymphonyElixir.Worker.Assignment, as: WorkerAssignment
  alias SymphonyElixir.Worker.AuthCacheVerifier
  alias SymphonyElixir.Worker.OneShot

  @assignment_arg "--assignment-json"
  @result_path "/tmp/symphony-worker-result"
  @max_result_bytes 3_500

  @type outcome :: %{exit_code: 0 | 1 | 2, result: map()}

  @spec main([String.t()]) :: no_return()
  def main(args) do
    result = run(args, System.get_env())
    encoded = Jason.encode!(result.result)
    _ = write_termination_message(encoded)
    IO.puts("SYMPHONY_RESULT " <> encoded)
    System.halt(result.exit_code)
  end

  @doc false
  def run(args, env, deps \\ %{})

  @spec run([String.t()], map(), map()) :: outcome()
  def run(["--verify-auth-cache"], env, deps) when is_map(env) and is_map(deps),
    do: AuthCacheVerifier.run(env, deps)

  def run(["--diagnose-auth-cache"], env, deps) when is_map(env) and is_map(deps),
    do: AuthCacheVerifier.diagnose(env, deps)

  def run(args, env, deps) when is_list(args) and is_map(env) and is_map(deps) do
    with [@assignment_arg, assignment_json] <- args,
         {:ok, decoded} <- decode_assignment(assignment_json, env),
         outcome <- run_mode(decoded, env, deps) do
      case outcome do
        {:ok, result} -> %{exit_code: if(result.status in ["completed", "preflight_passed"], do: 0, else: 1), result: result}
        {:error, reason, result} -> %{exit_code: 1, result: Map.merge(result, %{status: "failed", reason: reason})}
        {:held, reason, result} -> %{exit_code: 2, result: Map.merge(result, %{status: "held", reason: reason})}
      end
    else
      _ ->
        %{exit_code: 1, result: base_result(nil, "failed", "invalid_job_arguments")}
    end
  end

  @doc false
  @spec decode_assignment(String.t(), map()) :: {:ok, map()} | {:error, atom(), nil}
  def decode_assignment(json, env) when is_binary(json) and is_map(env) do
    digest = Map.get(env, "SYMPHONY_ASSIGNMENT_SHA256")
    repository_id = Map.get(env, "SYMPHONY_REPOSITORY_ID")
    assignment_id = Map.get(env, "SYMPHONY_ASSIGNMENT_ID")

    mode = Map.get(env, "SYMPHONY_WORKER_MODE")

    with {:ok, decoded} <- WorkerAssignment.decode(json, digest, repository_id),
         true <- assignment_id == JobSpec.identity(decoded.bundle),
         {:ok, mode} <- valid_mode(mode, env) do
      {:ok, Map.put(decoded, :mode, mode)}
    else
      _ -> {:error, :invalid_job_assignment, nil}
    end
  end

  def decode_assignment(_json, _env), do: {:error, :invalid_job_assignment, nil}

  defp valid_mode("codex", env) do
    if Map.get(env, "CODEX_HOME") == "/var/lib/frigga-codex-home",
      do: {:ok, :codex},
      else: {:error, :invalid_job_assignment}
  end

  defp valid_mode("preflight", env) do
    case Map.fetch(env, "CODEX_HOME") do
      :error -> {:ok, :preflight}
      _ -> {:error, :invalid_job_assignment}
    end
  end

  defp valid_mode(_mode, _env), do: {:error, :invalid_job_assignment}

  defp run_mode(%{mode: :codex} = decoded, env, deps), do: OneShot.run(decoded, env, deps)

  defp run_mode(%{mode: :preflight} = decoded, _env, _deps) do
    {:ok, decoded |> base_result("preflight_passed", "auth_slot_required") |> Map.put(:revocation, "not_started")}
  end

  @doc false
  @spec base_result(map() | nil, String.t(), String.t()) :: map()
  def base_result(decoded, status, reason) do
    identity = if is_map(decoded), do: decoded.subject, else: %{}

    %{
      schema_version: 1,
      status: status,
      reason: reason,
      assignment_digest: Map.get(identity, :assignmentDigest),
      issue_uuid: Map.get(identity, :issueUuid),
      generation: Map.get(identity, :generation),
      repository_ref: Map.get(identity, :repositoryRef),
      branch_ref: Map.get(identity, :branchRef),
      checkout_lease_id: nil,
      checkout_revocation: "not_started",
      broker_lease_id: nil,
      revocation: "not_started",
      codex_exit_code: nil,
      head_oid: nil,
      branch_head_oid: nil,
      base_oid: nil,
      changed_files: nil,
      pull_request_number: nil,
      pull_request_url: nil
    }
  end

  defp write_termination_message(encoded) when byte_size(encoded) <= @max_result_bytes do
    File.write(@result_path, encoded <> "\n")
  rescue
    _ -> :error
  end

  defp write_termination_message(_encoded), do: :error
end
