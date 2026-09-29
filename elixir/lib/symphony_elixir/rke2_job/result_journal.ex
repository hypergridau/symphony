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
  alias SymphonyElixir.RKE2Job.{AuthSlotSpec, ResultReader}

  @result_schema_version 2
  @cleanup_schema_version 1
  @finalization_schema_version 1
  @max_cleanup_receipts 8
  @max_bytes 8_192
  @uid ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/
  @version ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/
  @hex64 ~r/\A[a-f0-9]{64}\z/
  @observation_keys ~w(job_uid job_resource_version pod_uid pod_resource_version pod_list_resource_version exit_code result)

  @spec record(map(), map(), Path.t()) :: {:ok, Path.t()} | {:held, atom()} | {:error, atom()}
  def record(assignment, observation, root), do: record(assignment, observation, root, nil)

  @doc "Persists the terminal result and non-secret OAuth slot binding in one immutable record before Job deletion."
  @spec record(map(), map(), Path.t(), map() | nil) :: {:ok, Path.t()} | {:held, atom()} | {:error, atom()}
  def record(assignment, observation, root, slot) do
    with {:ok, payload} <- payload(assignment, observation, slot),
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
    case load_with_slot(assignment, uid, root) do
      {:ok, observation, _slot} -> {:ok, observation}
      other -> other
    end
  end

  @doc "Returns a journaled result with its exact slot binding for terminal replay after Job deletion."
  @spec load_with_slot(map(), String.t(), Path.t()) ::
          {:ok, map(), map() | nil} | :missing | {:held, atom()} | {:error, atom()}
  def load_with_slot(assignment, uid, root) do
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

  @doc "Records the trusted adapter's completed exact Job deletion and OAuth slot release."
  @spec record_finalization(map(), String.t(), Path.t()) ::
          {:ok, Path.t()} | {:held, atom()} | {:error, atom()}
  def record_finalization(assignment, uid, root) do
    with {:ok, observation, slot} <- load_with_slot(assignment, uid, root),
         {:ok, payload} <- finalization_payload(assignment, uid, observation, slot),
         {:ok, result_path} <- path(root, assignment.sha256, uid),
         finalization_path = result_path <> ".finalized",
         {:ok, bytes} <- Jason.encode(payload) do
      case :file.open(String.to_charlist(finalization_path), [:write, :binary, :exclusive, :raw]) do
        {:ok, file} -> write_new(file, finalization_path, bytes)
        {:error, :eexist} -> compare_existing(finalization_path, bytes)
        _ -> {:held, :job_result_journal_write_unavailable}
      end
    else
      :missing -> {:held, :job_result_journal_missing}
      {:held, _} = held -> held
      {:error, _} = error -> error
    end
  rescue
    _ -> {:held, :job_result_journal_write_unavailable}
  end

  @doc "Reads the immutable post-finalization fact against the exact journaled result."
  @spec load_finalization(map(), String.t(), Path.t()) ::
          {:ok, map()} | :missing | {:held, atom()} | {:error, atom()}
  def load_finalization(assignment, uid, root) do
    with {:ok, observation, slot} <- load_with_slot(assignment, uid, root),
         {:ok, expected} <- finalization_payload(assignment, uid, observation, slot),
         {:ok, result_path} <- path(root, assignment.sha256, uid) do
      case read_regular(result_path <> ".finalized") do
        {:ok, bytes} ->
          decode_finalization(bytes, expected)

        {:error, :enoent} ->
          :missing

        _ ->
          {:held, :job_finalization_journal_invalid}
      end
    else
      :missing -> :missing
      other -> other
    end
  rescue
    _ -> {:held, :job_finalization_journal_invalid}
  end

  defp decode_finalization(bytes, expected) when byte_size(bytes) <= @max_bytes do
    case Jason.decode(bytes) do
      {:ok, ^expected} -> {:ok, expected}
      _ -> {:held, :job_finalization_journal_invalid}
    end
  end

  defp decode_finalization(_bytes, _expected), do: {:held, :job_finalization_journal_invalid}

  defp finalization_payload(assignment, uid, observation, slot) do
    with true <- observation["job_uid"] == uid,
         {:ok, bytes} <- Jason.encode(%{"observation" => observation, "auth_slot" => slot_payload(slot)}) do
      {:ok,
       %{
         "schema_version" => @finalization_schema_version,
         "assignment_digest" => assignment.sha256,
         "job_uid" => uid,
         "pod_uid" => observation["pod_uid"],
         "result_sha256" => :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower),
         "phase" => "job_and_pods_absent_auth_slot_released"
       }}
    else
      _ -> {:error, :invalid_job_finalization_record}
    end
  end

  @doc "Persists the one immutable cleanup receipt before a potentially uncertain provider POST."
  @spec record_cleanup_receipt(map(), String.t(), map(), Path.t()) ::
          {:ok, Path.t()} | {:held, atom()} | {:error, atom()}
  def record_cleanup_receipt(assignment, uid, receipt, root),
    do: record_cleanup_receipt(assignment, uid, receipt, root, 0)

  @doc "Appends a fresh receipt after Dahlia confirms the exact Job binding is retained."
  @spec record_cleanup_receipt(map(), String.t(), map(), Path.t(), non_neg_integer()) ::
          {:ok, Path.t()} | {:held, atom()} | {:error, atom()}
  def record_cleanup_receipt(assignment, uid, receipt, root, version)
      when is_integer(version) and version >= 0 and version < @max_cleanup_receipts do
    with {:ok, payload} <- cleanup_payload(assignment, uid, receipt),
         {:ok, path} <- cleanup_path(root, assignment.sha256, uid, version),
         {:ok, bytes} <- Jason.encode(payload),
         true <- byte_size(bytes) <= @max_bytes do
      case :file.open(String.to_charlist(path), [:write, :binary, :exclusive, :raw]) do
        {:ok, file} -> write_new(file, path, bytes)
        {:error, :eexist} -> compare_existing(path, bytes)
        _ -> {:held, :job_result_journal_write_unavailable}
      end
    else
      false -> {:error, :invalid_job_cleanup_receipt}
      {:error, _} = error -> error
    end
  rescue
    _ -> {:held, :job_result_journal_write_unavailable}
  end

  def record_cleanup_receipt(_assignment, _uid, _receipt, _root, _version),
    do: {:held, :job_cleanup_receipt_capacity_exhausted}

  @doc "Loads the previously submitted cleanup receipt for byte-identical provider replay."
  @spec load_cleanup_receipt(map(), String.t(), Path.t()) ::
          {:ok, map()} | :missing | {:held, atom()} | {:error, atom()}
  def load_cleanup_receipt(assignment, uid, root), do: load_cleanup_receipt(assignment, uid, root, 0)

  @doc "Loads one immutable cleanup receipt version."
  @spec load_cleanup_receipt(map(), String.t(), Path.t(), non_neg_integer()) ::
          {:ok, map()} | :missing | {:held, atom()} | {:error, atom()}
  def load_cleanup_receipt(assignment, uid, root, version)
      when is_integer(version) and version >= 0 and version < @max_cleanup_receipts do
    with :ok <- valid_digest_assignment?(assignment),
         true <- valid_uid?(uid),
         {:ok, path} <- cleanup_path(root, assignment.sha256, uid, version) do
      case read_regular(path) do
        {:ok, bytes} -> decode_cleanup_receipt(bytes, assignment, uid)
        {:error, :enoent} -> :missing
        _ -> {:held, :job_result_journal_read_unavailable}
      end
    else
      false -> {:error, :invalid_job_cleanup_receipt}
      {:error, _} = error -> error
    end
  rescue
    _ -> {:held, :job_result_journal_read_unavailable}
  end

  def load_cleanup_receipt(_assignment, _uid, _root, _version),
    do: {:held, :job_cleanup_receipt_capacity_exhausted}

  @doc "Returns contiguous saved receipts newest first, retaining earlier POST replay candidates."
  @spec load_cleanup_receipts(map(), String.t(), Path.t()) ::
          {:ok, [{non_neg_integer(), map()}]} | :missing | {:held, atom()} | {:error, atom()}
  def load_cleanup_receipts(assignment, uid, root) do
    Enum.reduce_while(0..(@max_cleanup_receipts - 1), :missing, fn version, acc ->
      accumulate_cleanup_receipt(assignment, uid, root, version, acc)
    end)
  end

  defp accumulate_cleanup_receipt(assignment, uid, root, version, acc) do
    case load_cleanup_receipt(assignment, uid, root, version) do
      {:ok, receipt} ->
        previous = if acc == :missing, do: [], else: elem(acc, 1)
        {:cont, {:ok, [{version, receipt} | previous]}}

      :missing ->
        {:halt, acc}

      other ->
        {:halt, other}
    end
  end

  defp cleanup_payload(assignment, uid, receipt) do
    with :ok <- valid_digest_assignment?(assignment),
         true <- valid_uid?(uid) and is_map(receipt) and receipt["jobUid"] == uid do
      {:ok,
       %{
         "schema_version" => @cleanup_schema_version,
         "assignment_digest" => assignment.sha256,
         "job_uid" => uid,
         "receipt" => receipt
       }}
    else
      _ -> {:error, :invalid_job_cleanup_receipt}
    end
  end

  defp decode_cleanup_receipt(bytes, assignment, uid) when byte_size(bytes) <= @max_bytes do
    with {:ok, %{"receipt" => receipt} = payload} <- Jason.decode(bytes),
         {:ok, expected} <- cleanup_payload(assignment, uid, receipt),
         true <- expected == payload do
      {:ok, receipt}
    else
      _ -> {:held, :job_cleanup_receipt_journal_invalid}
    end
  end

  defp decode_cleanup_receipt(_bytes, _assignment, _uid), do: {:held, :job_cleanup_receipt_journal_invalid}

  defp valid_digest_assignment?(%{sha256: digest}) when is_binary(digest) do
    if Regex.match?(@hex64, digest), do: :ok, else: {:error, :invalid_job_cleanup_receipt}
  end

  defp valid_digest_assignment?(_assignment), do: {:error, :invalid_job_cleanup_receipt}

  defp cleanup_path(root, digest, uid, version) do
    with {:ok, path} <- path(root, digest, uid),
         do: {:ok, path <> ".cleanup" <> if(version == 0, do: "", else: ".#{version}")}
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
    with {:ok, payload} <- Jason.decode(bytes),
         %{"schema_version" => @result_schema_version, "observation" => observation, "auth_slot" => slot} <- payload,
         true <- Map.keys(payload) |> Enum.sort() |> Kernel.==(Enum.sort(~w(schema_version observation auth_slot))),
         {:ok, normalized_slot} <- normalize_slot(assignment, slot),
         {:ok, expected} <- payload(assignment, observation, normalized_slot),
         true <- observation["job_uid"] == uid and expected == payload do
      {:ok, observation, normalized_slot}
    else
      _ -> {:held, :job_result_journal_invalid}
    end
  end

  defp decode_existing(_bytes, _assignment, _uid), do: {:held, :job_result_journal_invalid}

  defp payload(assignment, observation, slot) do
    with :ok <- valid_assignment?(assignment),
         {:ok, normalized} <- normalize_observation(observation),
         {:ok, normalized_slot} <- normalize_slot(assignment, slot),
         true <- valid_observation?(normalized, assignment) do
      {:ok,
       %{
         "schema_version" => @result_schema_version,
         "observation" => normalized,
         "auth_slot" => slot_payload(normalized_slot)
       }}
    else
      _ -> {:error, :invalid_job_result_journal_record}
    end
  end

  defp normalize_slot(_assignment, nil), do: {:ok, nil}

  defp normalize_slot(assignment, slot) when is_map(slot) do
    keys = ~w(slot_id claim_name claim_uid lease_id assignment_sha256 seat)a

    normalized =
      if Enum.all?(Map.keys(slot), &is_atom/1),
        do: slot,
        else: Map.new(keys, fn key -> {key, Map.get(slot, Atom.to_string(key))} end)

    if map_size(slot) == length(keys) and
         Enum.sort(Map.keys(normalized)) == Enum.sort(keys) and
         match?(
           {:ok, _},
           AuthSlotSpec.compile(assignment, normalized, %{normalized.slot_id => normalized.claim_name})
         ) do
      {:ok, normalized}
    else
      {:error, :invalid_job_result_journal_record}
    end
  rescue
    _ -> {:error, :invalid_job_result_journal_record}
  end

  defp normalize_slot(_assignment, _slot), do: {:error, :invalid_job_result_journal_record}

  defp slot_payload(nil), do: nil
  defp slot_payload(slot), do: Map.new(slot, fn {key, value} -> {Atom.to_string(key), value} end)

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
