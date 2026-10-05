defmodule SymphonyElixir.RKE2Job.AbortPrepareJournal do
  @moduledoc """
  Immutable host journal for the exact Dahlia abort-prepare request and its
  acknowledged root-intent and provider response checkpoints.

  Each checkpoint is created exclusively and synced. Conflicting, partial, or
  unsafe files stay in place and hold the caller for reconciliation.
  """

  import Bitwise, only: [band: 2]
  alias SymphonyElixir.RKE2Job.RootAbortInputPublisher

  @schema_version 1
  @max_bytes 16_384
  @record_fields ~w(
    schema_version claim assignment_digest allocation_id prepare_id request_sha256 request_bytes observation
  )
  @disposal_checkpoint_fields ~w(
    schema_version claim assignment_digest allocation_id prepare_id prepare_request_sha256 result_reference
    result_sha256 receipt_sha256 response
  )

  @spec identity_key(map()) :: {:ok, String.t()} | {:error, atom()}
  def identity_key(claim) when is_map(claim) do
    canonical = Jason.encode!(canonical_term(claim))
    {:ok, :crypto.hash(:sha256, canonical) |> Base.encode16(case: :lower)}
  rescue
    _ -> {:error, :invalid_abort_prepare_journal_claim}
  end

  def identity_key(_claim), do: {:error, :invalid_abort_prepare_journal_claim}

  @spec load(Path.t(), map()) :: {:ok, map()} | :missing | {:held, atom()}
  def load(root, claim) do
    with {:ok, path} <- checkpoint_path(root, claim, "request") do
      case read_regular(path) do
        {:ok, bytes} -> decode_record(bytes, claim)
        {:error, :enoent} -> :missing
        _ -> {:held, :abort_prepare_journal_read_unavailable}
      end
    end
  rescue
    _ -> {:held, :abort_prepare_journal_read_unavailable}
  end

  @spec record(Path.t(), map(), map()) :: {:ok, map()} | {:held, atom()}
  def record(root, claim, record) when is_map(record) do
    with {:ok, path} <- checkpoint_path(root, claim, "request"),
         {:ok, bytes} <- encode_record(claim, record),
         true <- byte_size(bytes) <= @max_bytes do
      case exclusive_write(path, bytes) do
        :ok ->
          load(root, claim)

        {:error, :eexist} ->
          record_existing(root, claim, record)

        _ ->
          {:held, :abort_prepare_journal_write_unavailable}
      end
    else
      false -> {:held, :abort_prepare_journal_record_too_large}
      _ -> {:held, :abort_prepare_journal_write_unavailable}
    end
  rescue
    _ -> {:held, :abort_prepare_journal_write_unavailable}
  end

  @spec record_intent(Path.t(), map(), map(), map()) :: :ok | {:held, atom()}
  def record_intent(root, claim, record, receipt) do
    with {:ok, path} <- checkpoint_path(root, claim, "intent"),
         true <- valid_root_receipt?(receipt),
         {:ok, bytes} <- Jason.encode(intent_record(record, receipt)) do
      case exclusive_write(path, bytes) do
        :ok -> compare_checkpoint(path, bytes, :abort_prepare_intent_conflict)
        {:error, :eexist} -> compare_checkpoint(path, bytes, :abort_prepare_intent_conflict)
        _ -> {:held, :abort_prepare_journal_write_unavailable}
      end
    else
      _ -> {:held, :abort_prepare_journal_write_unavailable}
    end
  rescue
    _ -> {:held, :abort_prepare_journal_write_unavailable}
  end

  @spec intent_recorded?(Path.t(), map(), map()) :: boolean()
  def intent_recorded?(root, claim, record) do
    with {:ok, path} <- checkpoint_path(root, claim, "intent"),
         {:ok, bytes} <- read_regular(path),
         {:ok, decoded} <- Jason.decode(bytes),
         true <- valid_intent_record?(decoded, record) do
      true
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  @spec intent_receipt_matches?(Path.t(), map(), map(), map()) :: boolean()
  def intent_receipt_matches?(root, claim, record, receipt) do
    with true <- valid_root_receipt?(receipt),
         true <- receipt["replayed"] == true,
         {:ok, path} <- checkpoint_path(root, claim, "intent"),
         {:ok, bytes} <- read_regular(path),
         {:ok, decoded} <- Jason.decode(bytes),
         true <- valid_intent_record?(decoded, record),
         %{"root_receipt" => %{"version" => version, "sequence" => sequence, "hash" => hash}} <- decoded do
      version == receipt["version"] and sequence == receipt["sequence"] and hash == receipt["hash"]
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  @spec record_ack(Path.t(), map(), map(), map()) :: :ok | {:held, atom()}
  def record_ack(root, claim, record, acknowledgement) do
    cond do
      not intent_recorded?(root, claim, record) ->
        {:held, :abort_prepare_root_intent_missing}

      not valid_ack?(record, acknowledgement) ->
        {:held, :invalid_abort_prepare_provider_acknowledgement}

      true ->
        persist_ack(root, claim, record, acknowledgement)
    end
  rescue
    _ -> {:held, :abort_prepare_journal_write_unavailable}
  end

  @spec ack_recorded?(Path.t(), map(), map(), map()) :: boolean()
  def ack_recorded?(root, claim, record, acknowledgement) do
    with {:ok, path} <- checkpoint_path(root, claim, "ack"),
         {:ok, bytes} <- read_regular(path),
         {:ok, decoded} <- Jason.decode(bytes) do
      decoded == ack_record(record, acknowledgement)
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  @spec load_confirmed_delete(Path.t(), map(), map(), String.t()) ::
          {:ok, map()} | :missing | {:held, atom()}
  def load_confirmed_delete(root, claim, record, uid) do
    with {:ok, path} <- checkpoint_path(root, claim, "confirmed-delete"),
         {:ok, bytes} <- read_regular(path),
         {:ok, decoded} <- Jason.decode(bytes),
         true <- valid_confirmed_delete?(decoded, claim, record, uid),
         :ok <- sync_checkpoint_directory(Path.dirname(path)) do
      {:ok, decoded}
    else
      {:error, :enoent} -> :missing
      {:held, reason} -> {:held, reason}
      _ -> {:held, :abort_prepare_confirmed_delete_checkpoint_invalid}
    end
  rescue
    _ -> {:held, :abort_prepare_confirmed_delete_checkpoint_invalid}
  end

  @spec record_confirmed_delete(Path.t(), map(), map(), String.t(), map()) :: :ok | {:held, atom()}
  def record_confirmed_delete(root, claim, record, uid, pod_evidence) do
    checkpoint = %{
      "schema_version" => @schema_version,
      "claim" => claim,
      "assignment_digest" => record.assignment_digest,
      "allocation_id" => record.allocation_id,
      "prepare_id" => record.prepare_id,
      "request_sha256" => record.request_sha256,
      "job_uid" => uid,
      "job_resource_version" => record.observation["job"]["resourceVersion"],
      "post_delete_pod_snapshot" => pod_evidence
    }

    with true <- valid_confirmed_delete?(checkpoint, claim, record, uid),
         {:ok, path} <- checkpoint_path(root, claim, "confirmed-delete"),
         {:ok, bytes} <- Jason.encode(checkpoint) do
      case exclusive_write(path, bytes) do
        :ok -> compare_checkpoint(path, bytes, :abort_prepare_confirmed_delete_checkpoint_conflict)
        {:error, :eexist} -> compare_checkpoint(path, bytes, :abort_prepare_confirmed_delete_checkpoint_conflict)
        _ -> {:held, :abort_prepare_journal_write_unavailable}
      end
    else
      _ -> {:held, :abort_prepare_confirmed_delete_checkpoint_invalid}
    end
  rescue
    _ -> {:held, :abort_prepare_journal_write_unavailable}
  end

  @doc "Persists an immutable root disposal authorization, keyed by its exact receipt hash."
  @spec record_disposal_proof(Path.t(), map(), map(), map(), map(), map()) :: {:ok, map()} | {:held, atom()}
  def record_disposal_proof(root, claim, record, request, expected, response) do
    with true <- valid_disposal_proof?(claim, record, request, expected, response),
         {:ok, encoded_response} <- Jason.encode(response),
         receipt_hash <- sha256(encoded_response),
         checkpoint = disposal_proof_record(claim, record, request, expected, response, receipt_hash),
         {:ok, path} <- checkpoint_path(root, claim, "disposal-" <> receipt_hash),
         {:ok, bytes} <- Jason.encode(checkpoint) do
      case exclusive_write(path, bytes) do
        :ok -> load_disposal_proof(root, claim, record, request, expected, receipt_hash)
        {:error, :eexist} -> load_disposal_proof(root, claim, record, request, expected, receipt_hash)
        _ -> {:held, :abort_prepare_journal_write_unavailable}
      end
    else
      _ -> {:held, :abort_prepare_disposal_proof_invalid}
    end
  rescue
    _ -> {:held, :abort_prepare_disposal_proof_invalid}
  end

  @doc "Re-reads the exact immutable disposal proof before the destructive adapter call."
  @spec load_disposal_proof(Path.t(), map(), map(), map(), map(), String.t()) :: {:ok, map()} | {:held, atom()}
  def load_disposal_proof(root, claim, record, request, expected, receipt_hash) do
    with true <- is_binary(receipt_hash) and Regex.match?(~r/\A[a-f0-9]{64}\z/, receipt_hash),
         {:ok, path} <- checkpoint_path(root, claim, "disposal-" <> receipt_hash),
         {:ok, bytes} <- read_regular(path),
         {:ok, decoded} <- Jason.decode(bytes),
         true <- valid_disposal_checkpoint?(decoded, claim, record, request, expected, receipt_hash),
         :ok <- sync_checkpoint_directory(Path.dirname(path)) do
      {:ok, decoded["response"]}
    else
      _ -> {:held, :abort_prepare_disposal_proof_unavailable}
    end
  rescue
    _ -> {:held, :abort_prepare_disposal_proof_unavailable}
  end

  @doc "Loads a previously persisted disposal receipt for the exact claim and result binding."
  @spec find_disposal_proof(Path.t(), map(), map(), map(), map()) :: {:ok, map()} | :missing | {:held, atom()}
  def find_disposal_proof(root, claim, record, request, expected) do
    with true <- Path.type(root) == :absolute and private_root?(root),
         {:ok, key} <- identity_key(claim),
         {:ok, entries} <- File.ls(root),
         {:ok, hashes} <- disposal_receipt_hashes(entries, key <> ".disposal-") do
      case hashes do
        [] ->
          if sync_checkpoint_directory(root) == :ok,
            do: :missing,
            else: {:held, :abort_prepare_journal_sync_unavailable}

        [receipt_hash | remaining] ->
          with {:ok, response} <- load_disposal_proof(root, claim, record, request, expected, receipt_hash),
               :ok <- verify_disposal_candidates(root, claim, record, request, expected, remaining) do
            {:ok, response}
          end
      end
    else
      {:held, _reason} = held -> held
      _ -> {:held, :abort_prepare_disposal_proof_unavailable}
    end
  rescue
    _ -> {:held, :abort_prepare_disposal_proof_unavailable}
  end

  defp encode_record(claim, record) do
    if valid_record?(record, claim) do
      Jason.encode(%{
        "schema_version" => @schema_version,
        "claim" => claim,
        "assignment_digest" => record.assignment_digest,
        "allocation_id" => record.allocation_id,
        "prepare_id" => record.prepare_id,
        "request_sha256" => record.request_sha256,
        "request_bytes" => record.request_bytes,
        "observation" => record.observation
      })
    else
      {:error, :invalid_abort_prepare_journal_record}
    end
  end

  defp decode_record(bytes, claim) when byte_size(bytes) <= @max_bytes do
    with {:ok, decoded} <- Jason.decode(bytes),
         true <-
           Map.keys(decoded) |> Enum.sort() ==
             Enum.sort(@record_fields),
         true <- decoded["schema_version"] == @schema_version and decoded["claim"] == claim,
         record = %{
           claim: claim,
           assignment_digest: decoded["assignment_digest"],
           allocation_id: decoded["allocation_id"],
           prepare_id: decoded["prepare_id"],
           request_sha256: decoded["request_sha256"],
           request_bytes: decoded["request_bytes"],
           observation: decoded["observation"]
         },
         true <- valid_record?(record, claim) do
      {:ok, record}
    else
      _ -> {:held, :abort_prepare_journal_invalid}
    end
  end

  defp decode_record(_bytes, _claim), do: {:held, :abort_prepare_journal_invalid}

  defp valid_record?(record, claim) do
    with true <- is_binary(record.assignment_digest) and Regex.match?(~r/\A[a-f0-9]{64}\z/, record.assignment_digest),
         true <- is_binary(claim["pool"]) and byte_size(claim["pool"]) in 1..128,
         true <- is_binary(record.allocation_id) and byte_size(record.allocation_id) in 1..1024,
         true <-
           is_binary(record.prepare_id) and
             Regex.match?(~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/, record.prepare_id),
         true <- is_binary(record.request_bytes) and is_map(record.observation),
         true <- record.request_sha256 == sha256(record.request_bytes),
         {:ok, request} <- Jason.decode(record.request_bytes),
         true <-
           request["prepareId"] == record.prepare_id and request["assignmentDigest"] == record.assignment_digest and
             request["allocationId"] == record.allocation_id and request["reservationId"] == claim["reservationId"] do
      true
    else
      _ -> false
    end
  end

  defp same_request_identity?(existing, candidate, claim) do
    existing.assignment_digest == candidate.assignment_digest and existing.allocation_id == candidate.allocation_id and
      existing.observation == candidate.observation and existing.request_bytes == candidate.request_bytes and
      existing.prepare_id == candidate.prepare_id and existing.request_sha256 == candidate.request_sha256 and
      claim == candidate.claim
  end

  defp record_existing(root, claim, record) do
    case load(root, claim) do
      {:ok, existing} ->
        if same_request_identity?(existing, record, claim),
          do: {:ok, existing},
          else: {:held, :abort_prepare_journal_conflict}

      _ ->
        {:held, :abort_prepare_journal_conflict}
    end
  end

  defp intent_record(record, receipt),
    do: %{
      "schema_version" => @schema_version,
      "prepare_id" => record.prepare_id,
      "request_sha256" => record.request_sha256,
      "root_receipt" => %{
        "version" => receipt["version"],
        "sequence" => receipt["sequence"],
        "hash" => receipt["hash"]
      }
    }

  defp valid_intent_record?(
         %{
           "schema_version" => @schema_version,
           "prepare_id" => prepare_id,
           "request_sha256" => request_sha256,
           "root_receipt" => %{
             "version" => 1,
             "sequence" => sequence,
             "hash" => hash
           }
         },
         record
       ) do
    prepare_id == record.prepare_id and request_sha256 == record.request_sha256 and is_integer(sequence) and
      sequence > 0 and is_binary(hash) and Regex.match?(~r/\A[a-f0-9]{64}\z/, hash)
  end

  defp valid_intent_record?(_decoded, _record), do: false

  defp valid_root_receipt?(receipt) when is_map(receipt) do
    receipt["version"] == 1 and is_integer(receipt["sequence"]) and receipt["sequence"] > 0 and
      is_binary(receipt["hash"]) and Regex.match?(~r/\A[a-f0-9]{64}\z/, receipt["hash"]) and
      is_boolean(receipt["replayed"])
  end

  defp valid_root_receipt?(_receipt), do: false

  defp ack_record(record, acknowledgement),
    do: %{
      "schema_version" => @schema_version,
      "prepare_id" => record.prepare_id,
      "request_sha256" => record.request_sha256,
      "acknowledgement" => acknowledgement
    }

  defp valid_confirmed_delete?(checkpoint, claim, record, uid) when is_map(checkpoint) do
    valid_confirmed_delete_shape?(checkpoint) and
      confirmed_delete_identity_matches?(checkpoint, claim, record) and
      confirmed_job_matches?(checkpoint, record, uid)
  end

  defp valid_confirmed_delete?(_checkpoint, _claim, _record, _uid), do: false

  defp disposal_proof_record(claim, record, request, expected, response, receipt_hash),
    do: %{
      "schema_version" => @schema_version,
      "claim" => claim,
      "assignment_digest" => record.assignment_digest,
      "allocation_id" => record.allocation_id,
      "prepare_id" => record.prepare_id,
      "prepare_request_sha256" => record.request_sha256,
      "result_reference" => request["resultReference"],
      "result_sha256" => expected["resultSHA256"],
      "receipt_sha256" => receipt_hash,
      "response" => response
    }

  defp valid_disposal_proof?(claim, record, request, expected, response)
       when is_map(claim) and is_map(record) and is_map(request) and is_map(expected) and is_map(response) do
    valid_disposal_contract?(request, response) and
      disposal_selectors_match?(claim, record, request, response) and
      disposal_record_bindings_match?(record, expected, response)
  end

  defp valid_disposal_proof?(_claim, _record, _request, _expected, _response), do: false

  defp valid_disposal_contract?(request, response) do
    RootAbortInputPublisher.validate_disposal_request(request) == :ok and
      RootAbortInputPublisher.validate_disposal_response(response, request) == :ok
  end

  defp disposal_selectors_match?(claim, record, request, response) do
    response["claimSHA256"] == identity_key!(claim) and
      response["assignmentDigest"] == record.assignment_digest and
      response["allocationId"] == record.allocation_id and
      response["resultReference"] == request["resultReference"]
  end

  defp disposal_record_bindings_match?(record, expected, response) do
    Enum.sort(Map.keys(expected)) == Enum.sort(~w(resultSHA256 prepareId prepareRequestSHA256)) and
      response["resultSHA256"] == expected["resultSHA256"] and
      response["prepareId"] == record.prepare_id and
      response["prepareRequestSHA256"] == record.request_sha256 and
      expected["prepareId"] == record.prepare_id and
      expected["prepareRequestSHA256"] == record.request_sha256
  end

  defp valid_disposal_checkpoint?(checkpoint, claim, record, request, expected, receipt_hash) when is_map(checkpoint) do
    valid_disposal_checkpoint_shape?(checkpoint) and
      valid_disposal_checkpoint_binding?(checkpoint, claim, record, request, expected) and
      valid_disposal_receipt?(checkpoint, receipt_hash) and
      valid_disposal_proof?(claim, record, request, expected, checkpoint["response"])
  rescue
    _ -> false
  end

  defp valid_disposal_checkpoint?(_checkpoint, _claim, _record, _request, _expected, _receipt_hash), do: false

  defp disposal_receipt_hashes(entries, prefix) do
    matches = Enum.filter(entries, &String.starts_with?(&1, prefix))

    Enum.reduce_while(matches, {:ok, []}, fn name, {:ok, hashes} ->
      hash = String.replace_prefix(name, prefix, "")

      if Regex.match?(~r/\A[a-f0-9]{64}\.json\z/, hash),
        do: {:cont, {:ok, [String.replace_suffix(hash, ".json", "") | hashes]}},
        else: {:halt, {:held, :abort_prepare_disposal_proof_unavailable}}
    end)
    |> case do
      {:ok, hashes} -> {:ok, Enum.sort(hashes)}
      {:held, _reason} = held -> held
    end
  end

  defp verify_disposal_candidates(_root, _claim, _record, _request, _expected, []), do: :ok

  defp verify_disposal_candidates(root, claim, record, request, expected, [receipt_hash | rest]) do
    with {:ok, _response} <- load_disposal_proof(root, claim, record, request, expected, receipt_hash),
         :ok <- verify_disposal_candidates(root, claim, record, request, expected, rest) do
      :ok
    end
  end

  defp valid_disposal_checkpoint_shape?(checkpoint) do
    Enum.sort(Map.keys(checkpoint)) == Enum.sort(@disposal_checkpoint_fields) and
      checkpoint["schema_version"] == @schema_version
  end

  defp valid_disposal_checkpoint_binding?(checkpoint, claim, record, request, expected) do
    checkpoint["claim"] == claim and
      checkpoint["assignment_digest"] == record.assignment_digest and
      checkpoint["allocation_id"] == record.allocation_id and
      checkpoint["prepare_id"] == record.prepare_id and
      checkpoint["prepare_request_sha256"] == record.request_sha256 and
      checkpoint["result_reference"] == request["resultReference"] and
      checkpoint["result_sha256"] == expected["resultSHA256"]
  end

  defp valid_disposal_receipt?(checkpoint, receipt_hash) do
    checkpoint["receipt_sha256"] == receipt_hash and
      checkpoint["receipt_sha256"] == sha256(Jason.encode!(checkpoint["response"]))
  end

  defp identity_key!(claim) do
    {:ok, key} = identity_key(claim)
    key
  end

  defp valid_confirmed_delete_shape?(checkpoint) do
    fields =
      ~w(schema_version claim assignment_digest allocation_id prepare_id request_sha256 job_uid job_resource_version post_delete_pod_snapshot)

    Enum.sort(Map.keys(checkpoint)) == Enum.sort(fields) and
      checkpoint["schema_version"] == @schema_version and
      valid_pod_evidence?(checkpoint["post_delete_pod_snapshot"])
  end

  defp confirmed_delete_identity_matches?(checkpoint, claim, record) do
    checkpoint["claim"] == claim and checkpoint["assignment_digest"] == record.assignment_digest and
      checkpoint["allocation_id"] == record.allocation_id and checkpoint["prepare_id"] == record.prepare_id and
      checkpoint["request_sha256"] == record.request_sha256
  end

  defp confirmed_job_matches?(checkpoint, record, uid) do
    checkpoint["job_uid"] == uid and
      checkpoint["job_resource_version"] == record.observation["job"]["resourceVersion"]
  end

  defp valid_pod_evidence?(pods) when is_map(pods) do
    fields = ~w(resourceVersion sha256 itemCount complete ownedPodsAbsent)

    Enum.sort(Map.keys(pods)) == Enum.sort(fields) and is_binary(pods["resourceVersion"]) and
      pods["resourceVersion"] != "" and is_binary(pods["sha256"]) and
      Regex.match?(~r/\A[a-f0-9]{64}\z/, pods["sha256"]) and is_integer(pods["itemCount"]) and
      pods["itemCount"] >= 0 and pods["complete"] == true and pods["ownedPodsAbsent"] == true
  end

  defp valid_pod_evidence?(_pods), do: false

  defp valid_ack?(record, acknowledgement) when is_map(acknowledgement) do
    keys = ~w(prepareId projectionId reservationId preparedAt replayed prepareRequestSHA256)

    Enum.sort(Map.keys(acknowledgement)) == Enum.sort(keys) and
      acknowledgement["prepareId"] == record.prepare_id and
      acknowledgement["projectionId"] == record.claim["projectionId"] and
      acknowledgement["reservationId"] == record.claim["reservationId"] and
      acknowledgement["prepareRequestSHA256"] == record.request_sha256 and
      is_boolean(acknowledgement["replayed"]) and valid_timestamp?(acknowledgement["preparedAt"])
  end

  defp valid_ack?(_record, _acknowledgement), do: false

  defp valid_timestamp?(value) when is_binary(value), do: match?({:ok, _, _}, DateTime.from_iso8601(value))
  defp valid_timestamp?(_value), do: false

  defp persist_ack(root, claim, record, acknowledgement) do
    with {:ok, path} <- checkpoint_path(root, claim, "ack"),
         {:ok, bytes} <- Jason.encode(ack_record(record, acknowledgement)) do
      case exclusive_write(path, bytes) do
        :ok -> compare_checkpoint(path, bytes, :abort_prepare_ack_conflict)
        {:error, :eexist} -> compare_checkpoint(path, bytes, :abort_prepare_ack_conflict)
        _ -> {:held, :abort_prepare_journal_write_unavailable}
      end
    else
      _ -> {:held, :abort_prepare_journal_write_unavailable}
    end
  end

  defp compare_checkpoint(path, bytes, conflict) do
    case read_regular(path) do
      {:ok, ^bytes} ->
        if sync_directory(Path.dirname(path)) == :ok,
          do: :ok,
          else: {:held, :abort_prepare_journal_sync_unavailable}

      {:ok, _} ->
        {:held, conflict}

      _ ->
        {:held, :abort_prepare_journal_read_unavailable}
    end
  end

  defp sync_checkpoint_directory(path) do
    if sync_directory(path) == :ok,
      do: :ok,
      else: {:held, :abort_prepare_journal_sync_unavailable}
  end

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

  defp checkpoint_path(root, claim, suffix) when is_binary(root) and is_map(claim) do
    with true <- Path.type(root) == :absolute and private_root?(root),
         {:ok, key} <- identity_key(claim) do
      {:ok, Path.join(root, key <> "." <> suffix <> ".json")}
    else
      _ -> {:error, :invalid_abort_prepare_journal_root}
    end
  end

  defp checkpoint_path(_root, _claim, _suffix), do: {:error, :invalid_abort_prepare_journal_root}

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

  defp private_file(path), do: if(windows_test_only?(), do: :ok, else: File.chmod(path, 0o600))

  defp read_regular(path) do
    case File.lstat(path) do
      {:ok, %{type: :regular, mode: mode}} ->
        if windows_test_only?() or (match?({:unix, _}, :os.type()) and band(mode, 0o077) == 0),
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
      {:win32, _} ->
        if(windows_test_only?(), do: :ok, else: {:error, :unsupported_journal_host})

      {:unix, _} ->
        case System.cmd("sync", [root], stderr_to_stdout: true) do
          {_output, 0} -> :ok
          _ -> {:error, :directory_sync_failed}
        end
    end
  rescue
    _ -> {:error, :directory_sync_failed}
  end

  defp windows_test_only? do
    match?({:win32, _}, :os.type()) and Code.ensure_loaded?(ExUnit) and
      Process.get(:abort_prepare_journal_windows_test_only) == true
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp canonical_term(value) when is_map(value) do
    pairs = Enum.map(Enum.sort_by(value, fn {key, _} -> key end), fn {key, nested} -> [key, canonical_term(nested)] end)
    %{"$map" => pairs}
  end

  defp canonical_term(value) when is_list(value), do: %{"$list" => Enum.map(value, &canonical_term/1)}
  defp canonical_term(value), do: %{"$value" => value}
end
