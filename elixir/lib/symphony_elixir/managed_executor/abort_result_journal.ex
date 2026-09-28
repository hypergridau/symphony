defmodule SymphonyElixir.ManagedExecutor.AbortResultJournal do
  @moduledoc """
  Private durable storage for exact pre-execution blocked-result bytes.

  Records are immutable and bound to a result reference, assignment, issue,
  execution generation, and allocation. Partial or conflicting records remain
  in place and hold recovery. This module does not publish results or release
  provider capacity.
  """

  import Bitwise, only: [band: 2]

  @schema_version 1
  @max_result_bytes 65_536
  @max_record_bytes 100_000
  @hex64 ~r/\A[a-f0-9]{64}\z/
  @uuid ~r/\A[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\z/
  @reference ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,511}\z/
  @fields ~w(schema_version reference sha256 result_base64 assignment_digest issue_uuid generation allocation_id)

  @type binding :: %{
          assignment_digest: String.t(),
          issue_uuid: String.t(),
          generation: pos_integer(),
          allocation_id: String.t()
        }

  @spec record(Path.t(), String.t(), binding(), binary()) ::
          {:ok, %{reference: String.t(), sha256: String.t()}} | {:held, atom()} | {:error, atom()}
  def record(root, reference, binding, result_bytes) do
    with :ok <- validate_identity(reference, binding),
         true <- is_binary(result_bytes) and byte_size(result_bytes) in 1..@max_result_bytes,
         {:ok, path} <- record_path(root, reference),
         {:ok, bytes, digest} <- encode_record(reference, binding, result_bytes),
         true <- byte_size(bytes) <= @max_record_bytes do
      case exclusive_write(path, bytes) do
        :ok -> verify_written(path, reference, binding, result_bytes, digest)
        {:error, :eexist} -> compare_existing(path, reference, binding, result_bytes, digest)
        _ -> {:held, :abort_result_journal_write_unavailable}
      end
    else
      false -> {:error, :invalid_abort_result_journal_record}
      {:error, _} = error -> error
    end
  rescue
    _ -> {:held, :abort_result_journal_write_unavailable}
  end

  @spec load(Path.t(), String.t(), binding(), String.t()) ::
          {:ok, binary()} | :missing | {:held, atom()} | {:error, atom()}
  def load(root, reference, binding, expected_sha256) do
    with :ok <- validate_identity(reference, binding),
         true <- is_binary(expected_sha256) and Regex.match?(@hex64, expected_sha256),
         {:ok, path} <- record_path(root, reference) do
      case read_regular(path) do
        {:ok, bytes} -> decode_record(bytes, reference, binding, expected_sha256)
        {:error, :enoent} -> :missing
        _ -> {:held, :abort_result_journal_read_unavailable}
      end
    else
      false -> {:error, :invalid_abort_result_journal_record}
      {:error, _} = error -> error
    end
  rescue
    _ -> {:held, :abort_result_journal_read_unavailable}
  end

  defp encode_record(reference, binding, result_bytes) do
    digest = sha256(result_bytes)

    payload = %{
      "schema_version" => @schema_version,
      "reference" => reference,
      "sha256" => digest,
      "result_base64" => Base.encode64(result_bytes),
      "assignment_digest" => binding.assignment_digest,
      "issue_uuid" => binding.issue_uuid,
      "generation" => binding.generation,
      "allocation_id" => binding.allocation_id
    }

    case Jason.encode(payload) do
      {:ok, bytes} -> {:ok, bytes, digest}
      _ -> {:error, :invalid_abort_result_journal_record}
    end
  end

  defp decode_record(bytes, reference, binding, expected_sha256) when byte_size(bytes) <= @max_record_bytes do
    with {:ok, payload} <- Jason.decode(bytes),
         {:ok, ^bytes} <- Jason.encode(payload),
         true <- is_map(payload) and Enum.sort(Map.keys(payload)) == Enum.sort(@fields),
         true <- payload["schema_version"] == @schema_version,
         true <- payload["reference"] == reference,
         true <- binding_matches?(payload, binding),
         true <- payload["sha256"] == expected_sha256,
         {:ok, result_bytes} <- Base.decode64(payload["result_base64"]),
         true <- byte_size(result_bytes) in 1..@max_result_bytes,
         true <- sha256(result_bytes) == expected_sha256 do
      {:ok, result_bytes}
    else
      _ -> {:held, :abort_result_journal_invalid}
    end
  end

  defp decode_record(_bytes, _reference, _binding, _expected_sha256),
    do: {:held, :abort_result_journal_invalid}

  defp binding_matches?(payload, binding) do
    payload["assignment_digest"] == binding.assignment_digest and
      payload["issue_uuid"] == binding.issue_uuid and
      payload["generation"] == binding.generation and
      payload["allocation_id"] == binding.allocation_id
  end

  defp verify_written(path, reference, binding, result_bytes, digest) do
    case load(Path.dirname(path), reference, binding, digest) do
      {:ok, ^result_bytes} -> {:ok, %{reference: reference, sha256: digest}}
      _ -> {:held, :abort_result_journal_readback_failed}
    end
  end

  defp compare_existing(path, reference, binding, result_bytes, digest) do
    case load(Path.dirname(path), reference, binding, digest) do
      {:ok, ^result_bytes} -> {:ok, %{reference: reference, sha256: digest}}
      {:ok, _other_bytes} -> {:held, :abort_result_journal_conflict}
      {:held, :abort_result_journal_read_unavailable} -> {:held, :abort_result_journal_read_unavailable}
      _ -> {:held, :abort_result_journal_conflict}
    end
  end

  defp validate_identity(reference, binding) when is_binary(reference) and is_map(binding) do
    if valid_reference?(reference) and valid_binding?(binding) do
      :ok
    else
      {:error, :invalid_abort_result_journal_record}
    end
  end

  defp validate_identity(_reference, _binding), do: {:error, :invalid_abort_result_journal_record}

  defp valid_reference?(reference), do: Regex.match?(@reference, reference)

  defp valid_binding?(binding) do
    keys = [:assignment_digest, :issue_uuid, :generation, :allocation_id]

    Enum.sort(Map.keys(binding)) == Enum.sort(keys) and valid_assignment_digest?(binding.assignment_digest) and
      valid_issue_uuid?(binding.issue_uuid) and valid_generation?(binding.generation) and
      valid_allocation_id?(binding.allocation_id)
  end

  defp valid_assignment_digest?(value), do: is_binary(value) and Regex.match?(@hex64, value)
  defp valid_issue_uuid?(value), do: is_binary(value) and Regex.match?(@uuid, value)
  defp valid_generation?(value), do: is_integer(value) and value > 0
  defp valid_allocation_id?(value), do: is_binary(value) and byte_size(value) in 1..1024

  defp record_path(root, reference) when is_binary(root) do
    if Path.type(root) == :absolute and private_root?(root) do
      key = sha256(reference)
      {:ok, Path.join(root, key <> ".abort-result.json")}
    else
      {:error, :invalid_abort_result_journal_root}
    end
  end

  defp record_path(_root, _reference), do: {:error, :invalid_abort_result_journal_root}

  defp private_root?(root) do
    case File.lstat(root) do
      {:ok, %{type: :directory, mode: mode}} ->
        windows_test_only?() or
          (match?({:unix, _}, :os.type()) and band(mode, 0o077) == 0 and trusted_ancestors?(root))

      _ ->
        false
    end
  end

  defp trusted_ancestors?(root) do
    with {:ok, %{type: :directory, uid: root_uid}} <- File.lstat(root),
         [first | rest] <- Path.split(root),
         true <- trusted_directory_path?(first, root_uid) do
      trusted_directory_parts?(rest, first, root_uid)
    else
      _ ->
        false
    end
  end

  defp trusted_directory_path?(path, root_uid) do
    case File.lstat(path) do
      {:ok, %{type: :directory, uid: uid, mode: mode}} -> trusted_directory?(uid, mode, root_uid)
      _ -> false
    end
  end

  defp trusted_directory_parts?([], _prefix, _root_uid), do: true

  defp trusted_directory_parts?([part | rest], prefix, root_uid) do
    next = Path.join(prefix, part)

    case File.lstat(next) do
      {:ok, %{type: :directory, uid: uid, mode: mode}} ->
        if trusted_directory?(uid, mode, root_uid), do: trusted_directory_parts?(rest, next, root_uid), else: false

      _ ->
        false
    end
  end

  defp trusted_directory?(uid, mode, root_uid),
    do: uid in [0, root_uid] and band(mode, 0o022) == 0

  defp exclusive_write(path, bytes) do
    case :file.open(String.to_charlist(path), [:write, :binary, :exclusive, :raw]) do
      {:ok, file} ->
        result = with :ok <- private_file(path), :ok <- :file.write(file, bytes), do: :file.sync(file)
        close = :file.close(file)
        if result == :ok and close == :ok, do: sync_directory(Path.dirname(path)), else: {:error, :write_failed}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp private_file(path), do: if(windows_test_only?(), do: :ok, else: File.chmod(path, 0o600))

  defp read_regular(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular, mode: mode} = before} ->
        if windows_test_only?() or
             (match?({:unix, _}, :os.type()) and band(mode, 0o077) == 0),
           do: read_and_revalidate(path, before),
           else: {:error, :insecure_journal_file}

      {:error, :enoent} ->
        {:error, :enoent}

      _ ->
        {:error, :invalid_journal_file}
    end
  end

  defp read_and_revalidate(path, before) do
    with {:ok, bytes} <- File.read(path),
         {:ok, %{type: :regular} = after_read} <- File.lstat(path),
         true <- same_file?(before, after_read) do
      {:ok, bytes}
    else
      _ -> {:error, :invalid_journal_file}
    end
  end

  defp same_file?(left, right) do
    Enum.all?([:inode, :major_device, :minor_device, :uid, :mode], fn field ->
      Map.fetch!(left, field) == Map.fetch!(right, field)
    end)
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
    do: match?({:win32, _}, :os.type()) and Code.ensure_loaded?(ExUnit) and Process.get(:abort_result_journal_windows_test_only) == true

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
