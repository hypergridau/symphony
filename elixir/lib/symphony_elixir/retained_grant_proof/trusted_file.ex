defmodule SymphonyElixir.RetainedGrantProof.TrustedFile do
  @moduledoc """
  Internal integrity prerequisite for root-installed retained-grant broker files.
  The trusted launcher selects the path, digest and limit; requests must not.
  This read-only primitive grants no authority and is not a remote file service.
  """

  import Bitwise, only: [band: 2]

  @hash ~r/\A[0-9a-f]{64}\z/
  @metadata [:major_device, :minor_device, :inode, :uid, :gid, :mode, :links, :size, :mtime, :ctime]

  @spec load(Path.t(), String.t(), pos_integer()) :: {:ok, binary()} | {:error, term()}
  def load(path, expected_sha256, max_bytes) do
    load_file(path, expected_sha256, max_bytes, :public)
  end

  @doc "Internal root-launcher seed read; the installed path and digest never come from a request."
  @spec load_seed(Path.t(), String.t()) :: {:ok, binary()} | {:error, term()}
  def load_seed(path, expected_sha256) do
    with {:ok, bytes} <- load_file(path, expected_sha256, 32, :secret),
         true <- byte_size(bytes) == 32 do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_retained_grant_file}
    end
  end

  defp load_file(path, expected_sha256, max_bytes, visibility) do
    with :ok <- arguments(path, expected_sha256, max_bytes),
         {:ok, before} <- snapshot(path, max_bytes, visibility),
         {:ok, bytes} <- bounded_read(path, max_bytes, elem(before, 1)),
         true <- byte_size(bytes) > 0 and byte_size(bytes) <= max_bytes and digest(bytes) == expected_sha256,
         {:ok, after_read} <- snapshot(path, max_bytes, visibility),
         true <- before == after_read do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_retained_grant_file}
    end
  end

  defp arguments(path, hash, limit) do
    if canonical_path?(path) and valid_hash?(hash) and is_integer(limit) and limit > 0 and limit <= 262_144,
      do: :ok,
      else: {:error, :invalid_retained_grant_file_arguments}
  end

  defp canonical_path?(path) when is_binary(path) do
    String.valid?(path) and String.starts_with?(path, "/") and
      not String.contains?(path, ["\\", "//", <<0>>]) and Path.expand(path) == path
  end

  defp canonical_path?(_), do: false
  defp valid_hash?(hash) when is_binary(hash), do: String.valid?(hash) and Regex.match?(@hash, hash)
  defp valid_hash?(_), do: false

  defp snapshot(path, limit, visibility) do
    with {:ok, ancestors} <- ancestors(Path.dirname(path)),
         {:ok, stat} <- File.lstat(path, time: :posix),
         true <- stat.type == :regular and trusted?(stat) and stat.links == 1 and stat.size > 0 and stat.size <= limit,
         true <- visibility == :public or band(stat.mode, 0o7777) in [0o400, 0o600] do
      {:ok, {ancestors, Map.take(stat, @metadata)}}
    else
      _ -> {:error, :untrusted_retained_grant_file_metadata}
    end
  end

  defp ancestors(path) do
    with {:ok, stat} <- File.lstat(path, time: :posix),
         true <- stat.type == :directory and trusted?(stat),
         {:ok, rest} <- parent_ancestors(path, Path.dirname(path)) do
      {:ok, [{path, Map.take(stat, @metadata)} | rest]}
    else
      _ -> {:error, :untrusted_retained_grant_file_ancestor}
    end
  end

  defp parent_ancestors(path, path), do: {:ok, []}
  defp parent_ancestors(_, parent), do: ancestors(parent)

  defp trusted?(stat), do: stat.uid == 0 and band(stat.mode, 0o022) == 0

  defp bounded_read(path, limit, expected_metadata) do
    case File.open(path, [:read, :binary, :raw]) do
      {:ok, io} ->
        result =
          with :ok <- descriptor_matches(io, expected_metadata),
               {:ok, bytes} <- :file.read(io, limit + 1),
               :ok <- descriptor_matches(io, expected_metadata) do
            {:ok, bytes}
          else
            _ -> {:error, :retained_grant_file_read_failed}
          end

        case {result, File.close(io)} do
          {{:ok, bytes}, :ok} -> {:ok, bytes}
          _ -> {:error, :retained_grant_file_read_failed}
        end

      _ ->
        {:error, :retained_grant_file_read_failed}
    end
  end

  defp descriptor_matches(io, expected_metadata) do
    with {:ok, record} <- :file.read_file_info(io, time: :posix),
         true <- Map.take(File.Stat.from_record(record), @metadata) == expected_metadata do
      :ok
    else
      _ -> {:error, :retained_grant_file_descriptor_changed}
    end
  end

  defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end
