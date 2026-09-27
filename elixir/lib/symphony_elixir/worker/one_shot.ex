defmodule SymphonyElixir.Worker.OneShot do
  @moduledoc "Runs one broker-authenticated disposable checkout and Codex turn."

  alias SymphonyElixir.Worker.BoundedOutput
  alias SymphonyElixir.Worker.BrokerClient
  alias SymphonyElixir.Worker.CLI, as: WorkerCLI

  @lease_ttl_seconds 600
  @workspace "/workspace"
  @askpass "/tmp/symphony-git-askpass"
  @codex_home "/var/lib/frigga-codex-home"
  @max_addition_bytes 512 * 1024

  @type result :: {:ok, map()} | {:held, String.t(), map()} | {:error, String.t(), map()}

  @spec run(map(), map(), map()) :: result()
  def run(%{bundle: bundle, subject: subject} = decoded, env, deps)
      when is_map(bundle) and is_map(subject) and is_map(env) and is_map(deps) do
    identity = WorkerCLI.base_result(decoded, "failed", "worker_failed")

    with :ok <- auth_slot_ready(env, deps),
         :ok <- workspace_ready(deps),
         {:ok, checkout_lease} <- issue_lease(subject, :git_checkout, "worker-checkout", deps) do
      case checkout_with_lease(checkout_lease, bundle, subject, deps) do
        {:ok, revocation} ->
          identity = checkout_receipt(identity, checkout_lease, revocation)
          run_assignment(bundle, subject, identity, deps)

        {:held, reason, revocation} ->
          result = checkout_receipt(identity, checkout_lease, revocation)
          {:held, safe_reason(reason), result}

        {:error, reason, revocation} ->
          result = checkout_receipt(identity, checkout_lease, revocation)
          {:error, safe_reason(reason), result}
      end
    else
      {:held, reason} -> {:held, safe_reason(reason), identity}
      {:error, reason} -> {:error, safe_reason(reason), identity}
    end
  end

  def run(_decoded, _env, _deps), do: {:error, "invalid_worker_assignment", %{}}

  defp run_assignment(bundle, subject, identity, deps) do
    with {:ok, codex_exit} <- run_codex(bundle, deps),
         true <- codex_exit == 0,
         {:ok, head, additions} <- inspect_additions(deps),
         true <- additions != [],
         {:ok, publish_lease} <- issue_lease(subject, :git_checkout_push_pr, "worker-publish", deps) do
      publish_with_lease(publish_lease, subject, identity, codex_exit, head, additions, deps)
    else
      false -> {:error, "codex_failed_or_no_additions", Map.put(identity, :revocation, "confirmed")}
      {:held, reason} -> {:held, safe_reason(reason), identity}
      {:error, reason} -> {:error, safe_reason(reason), identity}
    end
  end

  defp publish_with_lease(lease, subject, identity, codex_exit, base_oid, additions, deps) do
    publish_result = publish_once(lease, subject, base_oid, additions, deps)

    lease_id = lease["leaseId"]
    revoke_result = broker_revoke(lease_id, deps)

    case {publish_result, revoke_result} do
      {{:ok, branch_oid, commit_oid, pr}, :ok} ->
        {:ok,
         Map.merge(identity, %{
           status: "completed",
           reason: "pull_request_created",
           broker_lease_id: lease_id,
           revocation: "confirmed",
           codex_exit_code: codex_exit,
           head_oid: commit_oid,
           branch_head_oid: branch_oid,
           base_oid: base_oid,
           changed_files: length(additions),
           pull_request_number: pr.number,
           pull_request_url: pr.url
         })}

      {{:ok, branch_oid, commit_oid, pr}, _} ->
        {:held, "credential_revocation_unconfirmed",
         Map.merge(identity, %{
           status: "held",
           reason: "credential_revocation_unconfirmed",
           broker_lease_id: lease_id,
           revocation: "held",
           codex_exit_code: codex_exit,
           head_oid: commit_oid,
           branch_head_oid: branch_oid,
           base_oid: base_oid,
           changed_files: length(additions),
           pull_request_number: pr.number,
           pull_request_url: pr.url
         })}

      {{:held, reason, progress}, _} ->
        {:held, safe_reason(reason),
         Map.merge(identity, Map.merge(progress, %{status: "held", reason: safe_reason(reason), broker_lease_id: lease_id, revocation: if(revoke_result == :ok, do: "confirmed", else: "held")}))}

      {{:error, reason, progress}, :ok} ->
        {:error, safe_reason(reason), Map.merge(identity, Map.merge(progress, %{broker_lease_id: lease_id, revocation: "confirmed"}))}

      {{:error, reason, progress}, _} ->
        {:held, safe_reason(reason), Map.merge(identity, Map.merge(progress, %{status: "held", reason: safe_reason(reason), broker_lease_id: lease_id, revocation: "held"}))}
    end
  end

  defp publish_once(lease, subject, base_oid, additions, deps) do
    case broker_branch(lease, base_oid, subject.branchRef, deps) do
      {:ok, branch_oid} ->
        publish_branch(lease, subject, base_oid, branch_oid, additions, deps)

      {:held, reason} ->
        {:held, reason, %{base_oid: base_oid}}

      {:error, reason} ->
        {:error, reason, %{base_oid: base_oid}}
    end
  end

  defp publish_branch(lease, subject, base_oid, branch_oid, additions, deps) do
    case broker_commit(lease, branch_oid, additions, deps) do
      {:ok, commit_oid} -> publish_commit(lease, subject, base_oid, branch_oid, commit_oid, additions, deps)
      {:held, reason} -> {:held, reason, %{branch_head_oid: branch_oid, base_oid: base_oid}}
      {:error, reason} -> {:error, reason, %{branch_head_oid: branch_oid, base_oid: base_oid}}
    end
  end

  defp publish_commit(lease, subject, base_oid, branch_oid, commit_oid, additions, deps) do
    progress = %{
      branch_head_oid: branch_oid,
      head_oid: commit_oid,
      base_oid: base_oid,
      changed_files: length(additions)
    }

    case broker_pull_request(lease, subject.repositoryRef, deps) do
      {:ok, pr} -> {:ok, branch_oid, commit_oid, pr}
      {:held, reason} -> {:held, reason, progress}
      {:error, reason} -> {:error, reason, progress}
    end
  end

  defp checkout_with_lease(lease, bundle, subject, deps) do
    checkout_result =
      with {:ok, %{installation_token: token}} <- broker_checkout(lease, subject, deps),
           :ok <- clone_repository(bundle, token, deps) do
        :ok
      else
        {:held, reason} -> {:held, reason}
        {:error, reason} -> {:error, reason}
      end

    revoke_result = broker_revoke(lease["leaseId"], deps)
    File.rm(@askpass)

    case {checkout_result, revoke_result} do
      {:ok, :ok} -> {:ok, "confirmed"}
      {{:held, reason}, :ok} -> {:held, reason, "confirmed"}
      {{:held, reason}, _} -> {:held, reason, "held"}
      {{:error, reason}, :ok} -> {:error, reason, "confirmed"}
      {{:error, reason}, _} -> {:held, reason, "held"}
      {:ok, _} -> {:held, :credential_revocation_unconfirmed, "held"}
    end
  end

  defp checkout_receipt(identity, lease, revocation) do
    Map.merge(identity, %{
      checkout_lease_id: lease["leaseId"],
      checkout_revocation: revocation,
      revocation: revocation
    })
  end

  defp auth_slot_ready(env, deps) do
    case call(
           deps,
           :auth_slot_ready,
           fn -> Map.get(env, "CODEX_HOME") == @codex_home and File.regular?(Path.join(@codex_home, "auth.json")) end,
           []
         ) do
      true -> :ok
      _ -> {:error, :codex_auth_slot_unavailable}
    end
  end

  defp workspace_ready(deps) do
    case call(deps, :workspace_ready, fn -> workspace_empty?(workspace_path(deps)) end, []) do
      true -> :ok
      false -> {:error, :workspace_not_empty}
      _ -> {:error, :workspace_unavailable}
    end
  end

  defp workspace_empty?(workspace) do
    case File.ls(workspace) do
      {:ok, []} -> true
      {:ok, _entries} -> false
      {:error, _reason} -> :unavailable
    end
  end

  defp issue_lease(subject, use, suffix, deps) do
    now = call(deps, :now, &DateTime.utc_now/0, [])
    key = subject.assignmentDigest <> ":" <> suffix

    case call(deps, :broker_issue, &BrokerClient.issue/6, [subject, use, key, now, @lease_ttl_seconds, %{}]) do
      {:ok, %{"leaseId" => id, "notAfter" => cutoff} = lease} when is_binary(id) and is_binary(cutoff) -> {:ok, lease}
      {:ok, _} -> {:held, :broker_lease_metadata_invalid}
      {:held, _} -> {:held, :credential_issuance_uncertain}
      {:error, _} -> {:error, :credential_issuance_denied}
      _ -> {:held, :credential_issuance_uncertain}
    end
  end

  defp broker_checkout(lease, subject, deps) do
    case call(deps, :broker_checkout, &BrokerClient.checkout/4, [lease["leaseId"], subject.repositoryRef, lease["notAfter"], %{}]) do
      {:ok, %{installation_token: token}} when is_binary(token) -> {:ok, %{installation_token: token}}
      {:held, _} -> {:held, :credential_checkout_uncertain}
      {:error, _} -> {:error, :credential_checkout_denied}
      _ -> {:held, :credential_checkout_uncertain}
    end
  end

  defp broker_commit(lease, head, additions, deps) do
    case call(deps, :broker_commit, &BrokerClient.commit/5, [lease["leaseId"], head, "Complete assignment", additions, %{}]) do
      {:ok, oid} when is_binary(oid) -> {:ok, oid}
      {:held, _} -> {:held, :broker_commit_uncertain}
      {:error, _} -> {:error, :broker_commit_denied}
      _ -> {:held, :broker_commit_uncertain}
    end
  end

  defp broker_branch(lease, base_oid, branch_ref, deps) do
    case call(deps, :broker_branch, &BrokerClient.branch/4, [lease["leaseId"], base_oid, branch_ref, %{}]) do
      {:ok, oid} when is_binary(oid) -> {:ok, oid}
      {:held, _} -> {:held, :broker_branch_uncertain}
      {:error, _} -> {:error, :broker_branch_denied}
      _ -> {:held, :broker_branch_uncertain}
    end
  end

  defp broker_pull_request(lease, repository_ref, deps) do
    case call(deps, :broker_pull_request, &BrokerClient.pull_request/3, [lease["leaseId"], repository_ref, %{}]) do
      {:ok, %{number: number, url: url}} -> {:ok, %{number: number, url: url}}
      {:held, _} -> {:held, :pull_request_uncertain}
      {:error, _} -> {:error, :pull_request_denied}
      _ -> {:held, :pull_request_uncertain}
    end
  end

  defp broker_revoke(lease_id, deps), do: call(deps, :broker_revoke, &BrokerClient.revoke/2, [lease_id, %{}])

  defp clone_repository(bundle, token, deps) do
    workspace = workspace_path(deps)

    # Always remove askpass after the Git process exits, including failed checkout.
    # credo:disable-for-next-line Credo.Check.Readability.PreferImplicitTry
    try do
      with {:ok, base_branch} <- base_branch(bundle.base_ref),
           :ok <- call(deps, :create_askpass, &create_askpass/0, []),
           {:ok, _output, 0} <-
             command(
               deps,
               "git",
               ["-c", "credential.helper=", "clone", "--branch", base_branch, "--single-branch", "--no-tags", "--", "https://github.com/" <> bundle.repository_ref <> ".git", workspace],
               [
                 {"GIT_ASKPASS", @askpass},
                 {"GIT_TERMINAL_PROMPT", "0"},
                 {"SYMPHONY_GITHUB_TOKEN", token}
               ],
               65_536
             ),
           {:ok, _output, 0} <- command(deps, "git", ["-C", workspace, "switch", "--create", "--", bundle.branch], [], 65_536) do
        :ok
      else
        _ -> {:error, :repository_checkout_failed}
      end
    after
      File.rm(@askpass)
    end
  end

  defp base_branch("refs/remotes/origin/" <> branch) when byte_size(branch) in 1..240 do
    if Regex.match?(~r{\A[A-Za-z0-9][A-Za-z0-9._/-]*\z}, branch) and
         not String.contains?(branch, "..") and not String.ends_with?(branch, ".lock"),
       do: {:ok, branch},
       else: {:error, :invalid_base_ref}
  end

  defp base_branch(_value), do: {:error, :invalid_base_ref}

  defp create_askpass do
    script = "#!/bin/sh\ncase \"$1\" in\n  *Username*) printf '%s\\n' 'x-access-token' ;;\n  *Password*) printf '%s' \"$SYMPHONY_GITHUB_TOKEN\" ;;\n  *) exit 1 ;;\nesac\n"

    with :ok <- File.write(@askpass, script) do
      File.chmod(@askpass, 0o700)
    end
  rescue
    _ -> {:error, :credential_helper_unavailable}
  end

  defp run_codex(bundle, deps) do
    prompt = prompt(bundle)

    case command(
           deps,
           "codex",
           [
             "--ask-for-approval",
             "never",
             "exec",
             "--ignore-user-config",
             "--ephemeral",
             "--json",
             "--sandbox",
             "workspace-write",
             "--model",
             "gpt-6-luna",
             "--config",
             "model_reasoning_effort=high",
             "--cd",
             workspace_path(deps),
             prompt
           ],
           [{"CODEX_HOME", @codex_home}],
           1_048_576
         ) do
      {:ok, _events, status} when is_integer(status) -> {:ok, status}
      _ -> {:error, :codex_execution_failed}
    end
  end

  defp prompt(bundle) do
    task = %{
      objective: bundle.objective.content,
      repository: bundle.repository_ref,
      base_ref: bundle.base_ref,
      branch: bundle.branch,
      acceptance: bundle.acceptance.deliverable,
      evidence: bundle.acceptance.evidence,
      constraints: bundle.environment.constraints
    }

    """
    Complete this one bounded repository assignment. Work only in the checked out repository and make changes on branch #{bundle.branch}. Do not commit, push, open a pull request, use credentials, delegate to subagents, or persist work outside this workspace. Do not read, copy, or modify the Codex auth cache or projected broker token. Changes must consist only of new regular files. Do not change or delete existing files, create symlinks, or create more than 20 files or 512 KiB total; the host publishes additions through the credential broker. Do not claim that this assignment's signature was verified; the trusted host performed that check before it created the Job.

    Assignment details:
    #{Jason.encode!(task)}
    """
  end

  defp inspect_additions(deps) do
    workspace = workspace_path(deps)

    with {:ok, head, 0} <- command(deps, "git", ["-C", workspace, "rev-parse", "HEAD"], [], 256),
         true <- Regex.match?(~r/\A[a-f0-9]{40}\n?\z/, head),
         {:ok, status, 0} <- command(deps, "git", ["-C", workspace, "status", "--porcelain=v1", "-z", "--untracked-files=all"], [], 65_536),
         {:ok, paths} <- added_paths(status),
         {:ok, additions} <-
           call(deps, :read_additions, fn additions -> read_additions(additions, workspace) end, [paths]) do
      {:ok, String.trim(head), additions}
    else
      {:error, reason} -> {:held, reason}
      false -> {:held, :unsupported_workspace_changes}
      _ -> {:held, :workspace_result_unavailable}
    end
  end

  defp added_paths(status) do
    entries = String.split(status, <<0>>, trim: true)

    Enum.reduce_while(entries, {:ok, []}, fn
      <<"?? ", path::binary>>, {:ok, paths} ->
        if safe_relative_path?(path) and length(paths) < 20,
          do: {:cont, {:ok, [path | paths]}},
          else: {:halt, {:error, :unsupported_workspace_changes}}

      _entry, _acc ->
        {:halt, {:error, :unsupported_workspace_changes}}
    end)
    |> case do
      {:ok, paths} -> {:ok, Enum.reverse(paths)}
      error -> error
    end
  end

  defp read_additions(paths, workspace) do
    Enum.reduce_while(paths, {:ok, [], 0}, fn path, {:ok, additions, total} ->
      full_path = Path.join(workspace, path)

      with :ok <- regular_workspace_file(path, full_path, workspace),
           {:ok, contents} <- File.read(full_path),
           true <- String.valid?(contents) and total + byte_size(contents) <= @max_addition_bytes do
        {:cont, {:ok, [%{path: path, contents: contents} | additions], total + byte_size(contents)}}
      else
        _ -> {:halt, {:error, :unsupported_workspace_changes}}
      end
    end)
    |> case do
      {:ok, additions, _total} -> {:ok, Enum.reverse(additions)}
      error -> error
    end
  end

  defp regular_workspace_file(path, full_path, workspace) do
    components = Path.split(path)

    parents_regular? =
      components
      |> Enum.drop(-1)
      |> Enum.reduce_while(workspace, fn component, parent ->
        candidate = Path.join(parent, component)

        case File.lstat(candidate) do
          {:ok, %{type: :directory}} -> {:cont, candidate}
          _ -> {:halt, :unsafe}
        end
      end)

    case {parents_regular?, File.lstat(full_path)} do
      {parent, {:ok, %{type: :regular}}} when is_binary(parent) -> :ok
      _ -> {:error, :unsafe_file}
    end
  end

  defp safe_relative_path?(path) when is_binary(path) and byte_size(path) in 1..240 do
    components = String.split(path, "/")

    not String.contains?(path, ["\\", <<0>>]) and Path.type(path) == :relative and
      Enum.all?(components, fn part -> part not in ["", ".", ".."] and not String.ends_with?(part, ".lock") end)
  end

  defp safe_relative_path?(_path), do: false

  defp workspace_path(deps), do: Map.get(deps, :workspace_path, @workspace)

  defp command(deps, executable, args, env, limit) do
    sink = struct(BoundedOutput, limit: limit)
    options = [stderr_to_stdout: true, env: env, into: sink]

    case call(deps, :command, &System.cmd/3, [executable, args, options]) do
      {%{__struct__: BoundedOutput, output: output, truncated?: false}, status}
      when is_integer(status) ->
        {:ok, output, status}

      {%{__struct__: BoundedOutput, truncated?: true}, _status} ->
        {:error, :command_output_limit}

      {output, status} when is_binary(output) and is_integer(status) ->
        {:ok, output, status}

      other ->
        other
    end
  rescue
    _ -> {:error, :command_failed}
  end

  defp safe_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp safe_reason(reason) when is_binary(reason) and byte_size(reason) <= 128, do: reason
  defp safe_reason(_reason), do: "worker_operation_failed"

  defp call(deps, key, default, args) do
    case Map.get(deps, key) do
      fun when is_function(fun) -> apply(fun, args)
      _ -> apply(default, args)
    end
  rescue
    _ -> {:error, :dependency_failed}
  end
end
