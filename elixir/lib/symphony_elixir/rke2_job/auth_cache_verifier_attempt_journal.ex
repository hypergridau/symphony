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
