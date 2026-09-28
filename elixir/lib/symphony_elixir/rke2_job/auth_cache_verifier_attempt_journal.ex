defmodule SymphonyElixir.RKE2Job.AuthCacheVerifierAttemptJournal do
  @moduledoc """
  Persists one random verifier Job name before its first Kubernetes create call.

  A retry for the same assignment Job and OAuth lease reads the existing
  attempt ID. Changed slot, claim or image bindings hold instead of creating a
  second verifier that could race for the writable cache. The host supplies an
  existing private Linux directory outside the workspace. An incomplete or
  conflicting record remains for operator recovery.
  """

  import Bitwise, only: [band: 2]

  @safe_uid ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/
  @hex64 ~r/\A[a-f0-9]{64}\z/
  @uuid4 ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
  @image_prefix "ghcr.io/hypergridau/symphony-worker@sha256:"
  @max_bytes 4_096
  @result_keys ~w(job_uid job_resource_version pod_uid pod_resource_version pod_list_resource_version auth_cache_status auth_cache_bytes)

  @type intent :: map()

  @doc "Creates or replays the exact private verifier intent before Job creation."
  @spec ensure(map(), String.t(), map(), String.t(), Path.t()) :: {:ok, intent()} | {:held, atom()} | {:error, atom()}
  def ensure(%{sha256: digest, seat: seat}, job_uid, slot, image, root)
      when is_binary(digest) and is_binary(seat) and is_binary(job_uid) and is_map(slot) and is_binary(image) do
    with :ok <- valid_binding(digest, seat, job_uid, slot, image),
         {:ok, path} <- journal_path(root, digest, job_uid) do
      write_new(path, digest, seat, job_uid, slot, image)
    end
  rescue
    _ -> {:held, :auth_cache_verifier_attempt_unavailable}
  end

  def ensure(_assignment, _job_uid, _slot, _image, _root),
    do: {:error, :invalid_auth_cache_verifier_attempt}

  @doc "Durably marks the one permitted create call; replay never creates a second Job."
  @spec begin_create(intent(), Path.t()) :: {:ok, :new | :replayed} | {:held, atom()} | {:error, atom()}
  def begin_create(intent, root) do
    with {:ok, path} <- checkpoint_path(intent, root, "create"),
         {:ok, bytes} <- Jason.encode(%{"schemaVersion" => 1, "attempt" => intent}) do
      case :file.open(String.to_charlist(path), [:write, :binary, :exclusive, :raw]) do
        {:ok, file} -> write_create_marker(file, path, bytes)
        {:error, :eexist} -> compare_create_marker(path, bytes)
        _ -> {:held, :auth_cache_verifier_create_marker_unavailable}
      end
    end
  rescue
    _ -> {:held, :auth_cache_verifier_create_marker_unavailable}
  end

  @doc "Stores the server-assigned verifier Job UID before result observation."
  @spec record_job_uid(intent(), String.t(), Path.t()) :: {:ok, String.t()} | {:held, atom()} | {:error, atom()}
  def record_job_uid(intent, uid, root) do
    with true <- safe_uid?(uid),
         {:ok, path} <- checkpoint_path(intent, root, "job-uid"),
         {:ok, _} <- marker_exists(intent, root),
         {:ok, bytes} <- Jason.encode(%{"schemaVersion" => 1, "attempt" => intent, "jobUid" => uid}) do
      case :file.open(String.to_charlist(path), [:write, :binary, :exclusive, :raw]) do
        {:ok, file} -> write_job_uid(file, path, bytes, intent, uid)
        {:error, :eexist} -> compare_job_uid(path, intent, uid)
        _ -> {:held, :auth_cache_verifier_job_uid_unavailable}
      end
    else
      false -> {:error, :invalid_auth_cache_verifier_job_uid}
      other -> other
    end
  rescue
    _ -> {:held, :auth_cache_verifier_job_uid_unavailable}
  end

  @doc "Reads the exact persisted verifier Job UID, or reports a missing checkpoint."
  @spec load_job_uid(intent(), Path.t()) :: {:ok, String.t()} | :missing | {:held, atom()} | {:error, atom()}
  def load_job_uid(intent, root) do
    with {:ok, path} <- checkpoint_path(intent, root, "job-uid") do
      case read_regular(path) do
        {:ok, bytes} -> decode_job_uid(bytes, intent)
        {:error, :enoent} -> :missing
        _ -> {:held, :auth_cache_verifier_job_uid_unavailable}
      end
    end
  rescue
    _ -> {:held, :auth_cache_verifier_job_uid_unavailable}
  end

  defp write_create_marker(file, path, bytes) do
    result = with :ok <- File.chmod(path, 0o600), :ok <- :file.write(file, bytes), do: :file.sync(file)
    closed = :file.close(file)

    if result == :ok and closed == :ok,
      do: compare_create_marker(path, bytes, :new),
      else: {:held, :auth_cache_verifier_create_marker_unavailable}
  end

  defp compare_create_marker(path, bytes, status \\ :replayed) do
    with {:ok, ^bytes} <- read_regular(path),
         :ok <- sync_directory(Path.dirname(path)) do
      {:ok, status}
    else
      _ -> {:held, :auth_cache_verifier_create_marker_conflict}
    end
  end

  defp marker_exists(intent, root) do
    with {:ok, path} <- checkpoint_path(intent, root, "create"),
         {:ok, bytes} <- Jason.encode(%{"schemaVersion" => 1, "attempt" => intent}) do
      case compare_create_marker(path, bytes) do
        {:ok, _} -> {:ok, true}
        _ -> {:held, :auth_cache_verifier_create_marker_unverified}
      end
    end
  end

  defp write_job_uid(file, path, bytes, intent, uid) do
    result = with :ok <- File.chmod(path, 0o600), :ok <- :file.write(file, bytes), do: :file.sync(file)
    closed = :file.close(file)

    if result == :ok and closed == :ok,
      do: compare_job_uid(path, intent, uid),
      else: {:held, :auth_cache_verifier_job_uid_unavailable}
  end

  defp compare_job_uid(path, intent, uid) do
    with {:ok, bytes} <- read_regular(path),
         {:ok, ^uid} <- decode_job_uid(bytes, intent),
         :ok <- sync_directory(Path.dirname(path)) do
      {:ok, uid}
    else
      _ -> {:held, :auth_cache_verifier_job_uid_conflict}
    end
  end

  defp decode_job_uid(bytes, intent) do
    case Jason.decode(bytes) do
      {:ok, %{"schemaVersion" => 1, "attempt" => ^intent, "jobUid" => uid} = saved}
      when map_size(saved) == 3 and is_binary(uid) ->
        if safe_uid?(uid), do: {:ok, uid}, else: {:held, :auth_cache_verifier_job_uid_invalid}

      _ ->
        {:held, :auth_cache_verifier_job_uid_invalid}
    end
  end

  defp checkpoint_path(intent, root, kind) do
    with {:ok, path} <- result_path(intent, root) do
      {:ok, String.replace_suffix(path, ".auth-verifier-result.json", ".auth-verifier-#{kind}.json")}
    end
  end

  @doc "Checkpoints the verified result before the verifier Job can be deleted."
  @spec record_result(intent(), map(), Path.t()) :: {:ok, map()} | {:held, atom()} | {:error, atom()}
  def record_result(intent, evidence, root) do
    with {:ok, path} <- result_path(intent, root),
         {:ok, payload} <- result_payload(intent, evidence),
         {:ok, bytes} <- Jason.encode(payload),
         true <- byte_size(bytes) <= @max_bytes do
      write_result(path, bytes, payload)
    else
      false -> {:error, :invalid_auth_cache_verifier_result_checkpoint}
      other -> other
    end
  rescue
    _ -> {:held, :auth_cache_verifier_result_write_unavailable}
  end

  @doc "Loads only a complete checkpoint bound to the persisted verifier intent."
  @spec load_result(intent(), Path.t()) :: {:ok, map()} | :missing | {:held, atom()} | {:error, atom()}
  def load_result(intent, root) do
    with {:ok, path} <- result_path(intent, root) do
      read_result(path, intent)
    end
  rescue
    _ -> {:held, :auth_cache_verifier_result_read_unavailable}
  end

  defp write_result(path, bytes, payload) do
    case :file.open(String.to_charlist(path), [:write, :binary, :exclusive, :raw]) do
      {:ok, file} -> write_opened_result(file, path, bytes, payload)
      {:error, :eexist} -> compare_result(path, payload)
      _ -> {:held, :auth_cache_verifier_result_write_unavailable}
    end
  end

  defp write_opened_result(file, path, bytes, payload) do
    result = with :ok <- File.chmod(path, 0o600), :ok <- :file.write(file, bytes), do: :file.sync(file)
    closed = :file.close(file)

    if result == :ok and closed == :ok,
      do: compare_result(path, payload),
      else: {:held, :auth_cache_verifier_result_write_unavailable}
  end

  defp read_result(path, intent) do
    case read_regular(path) do
      {:ok, bytes} -> decode_result(bytes, intent)
      {:error, :enoent} -> :missing
      _ -> {:held, :auth_cache_verifier_result_read_unavailable}
    end
  end

  defp decode_result(bytes, intent) do
    with {:ok, %{"evidence" => evidence} = saved} <- Jason.decode(bytes),
         {:ok, ^saved} <- result_payload(intent, evidence) do
      {:ok, evidence}
    else
      _ -> {:held, :auth_cache_verifier_result_checkpoint_invalid}
    end
  end

  defp compare_result(path, expected) do
    case read_regular(path) do
      {:ok, bytes} ->
        compare_saved_result(path, bytes, expected)

      _ ->
        {:held, :auth_cache_verifier_result_read_unavailable}
    end
  end

  defp compare_saved_result(path, bytes, expected) do
    case Jason.decode(bytes) do
      {:ok, ^expected} -> sync_result(path, expected)
      _ -> {:held, :auth_cache_verifier_result_conflict}
    end
  end

  defp sync_result(path, expected) do
    case sync_directory(Path.dirname(path)) do
      :ok -> {:ok, expected["evidence"]}
      _ -> {:held, :auth_cache_verifier_result_sync_unavailable}
    end
  end

  defp result_path(%{"assignmentDigest" => digest, "jobUid" => job_uid} = intent, root) do
    with true <- valid_intent?(intent),
         {:ok, path} <- journal_path(root, digest, job_uid),
         {:ok, saved} <- read_regular(path),
         {:ok, ^intent} <- Jason.decode(saved) do
      {:ok, String.replace_suffix(path, ".auth-verifier-attempt.json", ".auth-verifier-result.json")}
    else
      _ -> {:held, :auth_cache_verifier_attempt_unverified}
    end
  end

  defp result_path(_intent, _root), do: {:error, :invalid_auth_cache_verifier_result_checkpoint}

  defp valid_intent?(intent) do
    slot = %{
      slot_id: intent["slotId"],
      lease_id: intent["leaseId"],
      claim_name: intent["claimName"],
      claim_uid: intent["claimUid"],
      assignment_sha256: intent["assignmentDigest"],
      seat: intent["seat"]
    }

    Regex.match?(@uuid4, intent["attemptId"]) and
      valid_binding(intent["assignmentDigest"], intent["seat"], intent["jobUid"], slot, intent["image"]) == :ok and
      intent == intended_record(intent["assignmentDigest"], intent["seat"], intent["jobUid"], slot, intent["image"], intent["attemptId"])
  end

  defp result_payload(intent, evidence) when is_map(evidence) do
    normalized = Map.new(evidence, fn {key, value} -> {to_string(key), value} end)

    if Enum.sort(Map.keys(normalized)) == Enum.sort(@result_keys) and
         safe_uid?(normalized["job_uid"]) and safe_uid?(normalized["pod_uid"]) and
         Enum.all?(~w(job_resource_version pod_resource_version pod_list_resource_version), &safe_uid?(normalized[&1])) and
         normalized["auth_cache_status"] == "codex_login_status_authenticated" and
         is_integer(normalized["auth_cache_bytes"]) and normalized["auth_cache_bytes"] in 1..10_000_000 do
      {:ok, %{"schemaVersion" => 1, "attempt" => intent, "evidence" => normalized}}
    else
      {:error, :invalid_auth_cache_verifier_result_checkpoint}
    end
  end

  defp result_payload(_intent, _evidence), do: {:error, :invalid_auth_cache_verifier_result_checkpoint}

  defp write_new(path, digest, seat, job_uid, slot, image) do
    intent = intended_record(digest, seat, job_uid, slot, image, Ecto.UUID.generate())
    bytes = Jason.encode!(intent)

    case :file.open(String.to_charlist(path), [:write, :binary, :exclusive, :raw]) do
      {:ok, file} ->
        write_opened(file, path, bytes, digest, seat, job_uid, slot, image)

      {:error, :eexist} ->
        case read_regular(path) do
          {:ok, bytes} -> validate_saved(bytes, digest, seat, job_uid, slot, image)
          _ -> {:held, :auth_cache_verifier_attempt_read_unavailable}
        end

      _ ->
        {:held, :auth_cache_verifier_attempt_write_unavailable}
    end
  end

  defp write_opened(file, path, bytes, digest, seat, job_uid, slot, image) do
    result = with :ok <- File.chmod(path, 0o600), :ok <- :file.write(file, bytes), do: :file.sync(file)
    closed = :file.close(file)

    with :ok <- result,
         :ok <- closed,
         :ok <- sync_directory(Path.dirname(path)),
         {:ok, saved} <- read_regular(path) do
      validate_saved(saved, digest, seat, job_uid, slot, image)
    else
      _ -> {:held, :auth_cache_verifier_attempt_write_unavailable}
    end
  end

  defp validate_saved(bytes, digest, seat, job_uid, slot, image) when byte_size(bytes) <= @max_bytes do
    case Jason.decode(bytes) do
      {:ok, %{"attemptId" => attempt_id} = saved} when is_binary(attempt_id) ->
        if Regex.match?(@uuid4, attempt_id) and saved == intended_record(digest, seat, job_uid, slot, image, attempt_id),
          do: {:ok, saved},
          else: {:held, :auth_cache_verifier_attempt_conflict}

      _ ->
        {:held, :auth_cache_verifier_attempt_invalid}
    end
  end

  defp validate_saved(_bytes, _digest, _seat, _job_uid, _slot, _image),
    do: {:held, :auth_cache_verifier_attempt_invalid}

  defp intended_record(digest, seat, job_uid, slot, image, attempt_id) do
    %{
      "schemaVersion" => 1,
      "assignmentDigest" => digest,
      "seat" => seat,
      "jobUid" => job_uid,
      "slotId" => slot.slot_id,
      "leaseId" => slot.lease_id,
      "claimName" => slot.claim_name,
      "claimUid" => slot.claim_uid,
      "image" => image,
      "attemptId" => attempt_id
    }
  end

  defp valid_binding(digest, seat, job_uid, slot, image) do
    claim = Map.get(slot, :claim_name)
    slot_id = Map.get(slot, :slot_id)
    claim_uid = Map.get(slot, :claim_uid)
    lease_id = Map.get(slot, :lease_id)

    if Regex.match?(@hex64, digest) and safe_uid?(job_uid) and valid_image?(image) and
         valid_slot_binding?(slot, digest, seat, slot_id, claim, claim_uid, lease_id) do
      :ok
    else
      {:error, :invalid_auth_cache_verifier_attempt}
    end
  end

  defp valid_slot_binding?(slot, digest, seat, slot_id, claim, claim_uid, lease_id) do
    safe_uid?(claim_uid) and safe_name?(claim) and safe_name?(slot_id) and
      is_binary(lease_id) and Regex.match?(@uuid4, lease_id) and
      Map.get(slot, :assignment_sha256) == digest and Map.get(slot, :seat) == seat and byte_size(seat) in 1..64
  end

  defp valid_image?(@image_prefix <> digest), do: Regex.match?(@hex64, digest)
  defp valid_image?(_image), do: false

  defp journal_path(root, digest, job_uid) when is_binary(root) do
    if Path.type(root) == :absolute and private_root?(root),
      do: {:ok, Path.join(root, digest <> "-" <> job_uid <> ".auth-verifier-attempt.json")},
      else: {:error, :invalid_auth_cache_verifier_attempt_root}
  end

  defp journal_path(_root, _digest, _job_uid), do: {:error, :invalid_auth_cache_verifier_attempt_root}

  defp private_root?(root) do
    case {effective_uid(), File.lstat(root)} do
      {{:ok, uid}, {:ok, %{type: :directory, mode: mode, uid: owner}}} ->
        owner == uid and band(mode, 0o077) == 0 and trusted_ancestors?(root, uid)

      _ ->
        false
    end
  end

  defp trusted_ancestors?(root, uid) do
    [first | rest] = Path.split(root)
    Enum.reduce_while(rest, first, fn part, prefix -> walk_directory(part, prefix, uid) end) != false
  end

  defp walk_directory(part, prefix, uid) do
    next = Path.join(prefix, part)

    case File.lstat(next) do
      {:ok, %{type: :directory, mode: mode, uid: owner}} ->
        if trusted_directory?(mode, owner, uid), do: {:cont, next}, else: {:halt, false}

      _ ->
        {:halt, false}
    end
  end

  defp trusted_directory?(mode, owner, uid) do
    owner in [0, uid] and
      (band(mode, 0o022) == 0 or (owner == 0 and band(mode, 0o1000) != 0))
  end

  defp read_regular(path) do
    case {effective_uid(), File.lstat(path)} do
      {{:ok, uid}, {:ok, %{type: :regular, mode: mode, size: size, uid: owner, links: 1}}}
      when size in 1..@max_bytes ->
        if owner == uid and band(mode, 0o077) == 0,
          do: File.read(path),
          else: {:error, :insecure_attempt_file}

      {_, {:error, :enoent}} ->
        {:error, :enoent}

      _ ->
        {:error, :invalid_attempt_file}
    end
  end

  defp effective_uid do
    case System.cmd("/usr/bin/id", ["-u"], stderr_to_stdout: true) do
      {output, 0} ->
        case Integer.parse(String.trim(output)) do
          {uid, ""} when uid >= 0 -> {:ok, uid}
          _ -> {:error, :uid_unavailable}
        end

      _ ->
        {:error, :uid_unavailable}
    end
  rescue
    _ -> {:error, :uid_unavailable}
  end

  defp sync_directory(root) do
    case System.cmd("sync", [root], stderr_to_stdout: true) do
      {_output, 0} -> :ok
      _ -> {:error, :directory_sync_failed}
    end
  rescue
    _ -> {:error, :directory_sync_failed}
  end

  defp safe_uid?(value) when is_binary(value), do: Regex.match?(@safe_uid, value)
  defp safe_uid?(_value), do: false

  defp safe_name?(value) when is_binary(value),
    do: byte_size(value) in 1..63 and Regex.match?(~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/, value)

  defp safe_name?(_value), do: false
end
