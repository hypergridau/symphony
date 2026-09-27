defmodule SymphonyElixir.RKE2Job.ResultJournal do
  @moduledoc """
  Stores one immutable, validated Job result observation before Job deletion.

  The trusted host supplies an existing private directory outside the workspace.
  An incomplete or conflicting record is held in place for recovery. A successful
  file sync protects against process restart; power-loss durability of directory
  entries must be qualified on the deployment filesystem before live admission.
  """

  import Bitwise, only: [band: 2]

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.ResultReader

  @schema_version 1
  @max_bytes 8_192
  @uid ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/
  @version ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/
  @hex64 ~r/\A[a-f0-9]{64}\z/
  @observation_keys ~w(job_uid job_resource_version pod_uid pod_resource_version pod_list_resource_version exit_code result)

  @spec record(map(), map(), Path.t()) :: {:ok, Path.t()} | {:held, atom()} | {:error, atom()}
  def record(assignment, observation, root) do
    with {:ok, payload} <- payload(assignment, observation),
         {:ok, path} <- path(root, assignment.sha256, payload["observation"]["job_uid"]),
         {:ok, bytes} <- Jason.encode(payload),
         true <- byte_size(bytes) <= @max_bytes do
      case :file.open(String.to_charlist(path), [:write, :binary, :exclusive, :raw]) do
        {:ok, file} -> write_new(file, path, bytes)
        {:error, :eexist} -> compare_existing(path, bytes)
        _ -> {:held, :job_result_journal_write_unavailable}
      end
    else
      false -> {:error, :invalid_job_result_journal_record}
      {:error, _} = error -> error
    end
  rescue
    _ -> {:held, :job_result_journal_write_unavailable}
  end

  @spec load(map(), String.t(), Path.t()) :: {:ok, map()} | :missing | {:held, atom()} | {:error, atom()}
  def load(assignment, uid, root) do
    with :ok <- valid_assignment?(assignment),
         true <- valid_uid?(uid),
         {:ok, path} <- path(root, assignment.sha256, uid) do
      case read_regular(path) do
        {:ok, bytes} -> decode_existing(bytes, assignment, uid)
        {:error, :enoent} -> :missing
        _ -> {:held, :job_result_journal_read_unavailable}
      end
    else
      false -> {:error, :invalid_job_result_journal_record}
      {:error, _} = error -> error
    end
  rescue
    _ -> {:held, :job_result_journal_read_unavailable}
  end

  defp write_new(file, path, bytes) do
    result =
      with :ok <- private_file(path),
           :ok <- :file.write(file, bytes),
           do: :file.sync(file)

    close_result = :file.close(file)

    if result == :ok and close_result == :ok do
      compare_existing(path, bytes)
    else
      {:held, :job_result_journal_write_unavailable}
    end
  end

  defp compare_existing(path, bytes) do
    case read_regular(path) do
      {:ok, ^bytes} ->
        case sync_directory(Path.dirname(path)) do
          :ok -> {:ok, path}
          _ -> {:held, :job_result_journal_sync_unavailable}
        end

      {:ok, _} ->
        {:held, :job_result_journal_conflict}

      _ ->
        {:held, :job_result_journal_read_unavailable}
    end
  end

  defp decode_existing(bytes, assignment, uid) when byte_size(bytes) <= @max_bytes do
    with {:ok, %{"schema_version" => @schema_version, "observation" => observation} = payload} <- Jason.decode(bytes),
         true <- Map.keys(payload) |> Enum.sort() |> Kernel.==(Enum.sort(~w(schema_version observation))),
         {:ok, expected} <- payload(assignment, observation),
         true <- observation["job_uid"] == uid and expected == payload do
      {:ok, observation}
    else
      _ -> {:held, :job_result_journal_invalid}
    end
  end

  defp decode_existing(_bytes, _assignment, _uid), do: {:held, :job_result_journal_invalid}

  defp payload(assignment, observation) do
    with :ok <- valid_assignment?(assignment),
         {:ok, normalized} <- normalize_observation(observation),
         true <- valid_observation?(normalized, assignment) do
      {:ok, %{"schema_version" => @schema_version, "observation" => normalized}}
    else
      _ -> {:error, :invalid_job_result_journal_record}
    end
  end

  defp normalize_observation(observation) when is_map(observation) do
    if Enum.all?(Map.keys(observation), &is_atom/1) do
      {:ok, Map.new(observation, fn {key, value} -> {Atom.to_string(key), value} end)}
    else
      {:ok, observation}
    end
  end

  defp normalize_observation(_observation), do: {:error, :invalid_job_result_journal_record}

  defp valid_observation?(observation, assignment) do
    result = observation["result"]

    Enum.sort(Map.keys(observation)) == Enum.sort(@observation_keys) and
      valid_uid?(observation["job_uid"]) and
      valid_uid?(observation["pod_uid"]) and
      valid_version?(observation["job_resource_version"]) and
      valid_version?(observation["pod_resource_version"]) and
      valid_version?(observation["pod_list_resource_version"]) and
      ResultReader.valid_receipt_outcome?(result, observation["exit_code"]) and
      valid_result_identity?(result, assignment)
  end

  defp valid_result_identity?(result, assignment) do
    Enum.all?([
      result["assignment_digest"] == assignment.sha256,
      result["issue_uuid"] == assignment.lease.issue_id,
      result["generation"] == assignment.lease.generation,
      result["repository_ref"] == assignment.repository_ref,
      result["branch_ref"] == "refs/heads/" <> assignment.branch
    ])
  end

  defp valid_assignment?(assignment) when is_map(assignment) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         true <- is_binary(assignment.sha256) and Regex.match?(@hex64, assignment.sha256) do
      :ok
    else
      _ -> {:error, :invalid_job_result_journal_record}
    end
  end

  defp valid_assignment?(_assignment), do: {:error, :invalid_job_result_journal_record}

  defp path(root, digest, uid) when is_binary(root) and is_binary(digest) and is_binary(uid) do
    if Path.type(root) == :absolute and valid_uid?(uid) do
      if private_root?(root),
        do: {:ok, Path.join(root, digest <> "-" <> uid <> ".json")},
        else: {:error, :invalid_job_result_journal_root}
    else
      {:error, :invalid_job_result_journal_root}
    end
  end

  defp path(_root, _digest, _uid), do: {:error, :invalid_job_result_journal_root}

  defp private_root?(root) do
    case File.lstat(root) do
      {:ok, %{type: :directory, mode: mode}} ->
        windows_test_only?() or
          (match?({:unix, _}, :os.type()) and band(mode, 0o077) == 0 and no_symlink_ancestors?(root))

      _ ->
        false
    end
  end

  defp no_symlink_ancestors?(root) do
    case Path.split(root) do
      [first | rest] -> Enum.reduce_while(rest, first, &walk_directory/2) != false
      _ -> false
    end
  end

  defp walk_directory(part, prefix) do
    next = Path.join(prefix, part)

    case File.lstat(next) do
      {:ok, %{type: :directory}} -> {:cont, next}
      _ -> {:halt, false}
    end
  end

  defp private_file(path) do
    if windows_test_only?(),
      do: :ok,
      else: File.chmod(path, 0o600)
  end

  defp read_regular(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular, mode: mode}} ->
        if windows_test_only?() or
             (match?({:unix, _}, :os.type()) and band(mode, 0o077) == 0),
           do: File.read(path),
           else: {:error, :insecure_journal_file}

      {:error, :enoent} ->
        {:error, :enoent}

      _ ->
        {:error, :invalid_journal_file}
    end
  end

  defp sync_directory(root) do
    case :os.type() do
      {:win32, _} -> if(windows_test_only?(), do: :ok, else: {:error, :unsupported_journal_host})
      {:unix, _} -> sync_unix_directory(root)
    end
  end

  defp sync_unix_directory(root) do
    case System.cmd("sync", [root], stderr_to_stdout: true) do
      {_output, 0} -> :ok
      _ -> {:error, :directory_sync_failed}
    end
  rescue
    _ -> {:error, :directory_sync_failed}
  end

  defp windows_test_only?,
    do: match?({:win32, _}, :os.type()) and Code.ensure_loaded?(ExUnit) and Process.get(:result_journal_windows_test_only) == true

  defp valid_uid?(value) when is_binary(value), do: Regex.match?(@uid, value)
  defp valid_uid?(_value), do: false
  defp valid_version?(value) when is_binary(value), do: Regex.match?(@version, value)
  defp valid_version?(_value), do: false
end
