defmodule SymphonyElixir.ManagedResponsibility.Manifest do
  @moduledoc """
  Loads a bounded operator-signed authorization document from a host-pinned file.
  This input is configuration; the orchestrator remains the sole graph writer.
  """

  import Bitwise, only: [band: 2]
  alias SymphonyElixir.{Config, ManagedResponsibility}

  @max_bytes 262_144
  @signature_domains %{1 => "hypergrid.symphony.managed-delegation.v1\0", 2 => "hypergrid.symphony.managed-delegation.v2\0"}

  @spec load(map(), non_neg_integer()) :: {:ok, map() | nil} | {:error, term()}
  def load(env, now_ms) do
    case Config.managed_delegation_config(env) do
      %{
        path: nil,
        sha256: nil,
        signature_ed25519: nil,
        public_key_ed25519: nil,
        pool_key: nil,
        repository_ref: nil
      } ->
        {:ok, nil}

      %{path: nil, sha256: nil} ->
        {:error, :managed_delegation_manifest_required}

      config ->
        load_config(config, now_ms)
    end
  end

  defp load_config(
         %{path: path, sha256: digest, signature_ed25519: signature, public_key_ed25519: public_key} = config,
         now_ms
       )
       when is_binary(path) and is_binary(digest) and is_binary(signature) and is_binary(public_key) do
    with true <- Path.type(path) == :absolute and Regex.match?(~r/\A[0-9a-f]{64}\z/, digest),
         :ok <- plain_ancestors(path),
         {:ok, stat} <- File.lstat(path),
         true <- stat.type == :regular and stat.uid == 0 and band(stat.mode, 0o022) == 0 and stat.size <= @max_bytes,
         {:ok, bytes} <- File.read(path),
         true <- byte_size(bytes) <= @max_bytes,
         true <- Base.encode16(:crypto.hash(:sha256, bytes), case: :lower) == digest,
         {:ok, payload} <- Jason.decode(bytes),
         version when version in [1, 2] <- payload["schema_version"],
         {:ok, signer_key_sha256} <- verify_signature(bytes, signature, public_key, version),
         {:ok, manifest} <- ManagedResponsibility.decode(payload, config, now_ms) do
      {:ok, Map.merge(manifest, %{source_sha256: digest, signer_key_sha256: signer_key_sha256})}
    else
      false -> {:error, :untrusted_managed_delegation_file}
      {:error, reason} -> {:error, {:managed_delegation_file, reason}}
      _ -> {:error, :invalid_managed_delegation_manifest}
    end
  end

  defp load_config(_config, _now_ms), do: {:error, :incomplete_managed_delegation_config}

  @doc false
  @spec verify_signature(binary(), binary(), binary()) ::
          {:ok, binary()} | {:error, :invalid_managed_delegation_signature}
  def verify_signature(bytes, signature_hex, public_key_hex)
      when is_binary(bytes) and is_binary(signature_hex) and is_binary(public_key_hex),
      do: verify_signature(bytes, signature_hex, public_key_hex, 1)

  def verify_signature(_bytes, _signature_hex, _public_key_hex),
    do: {:error, :invalid_managed_delegation_signature}

  @doc false
  @spec verify_signature(binary(), binary(), binary(), 1 | 2) ::
          {:ok, binary()} | {:error, :invalid_managed_delegation_signature}
  def verify_signature(bytes, signature_hex, public_key_hex, version)
      when is_binary(bytes) and is_binary(signature_hex) and is_binary(public_key_hex) do
    with true <- Regex.match?(~r/\A[0-9a-f]{128}\z/, signature_hex),
         true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, public_key_hex),
         {:ok, signature} <- Base.decode16(signature_hex, case: :lower),
         {:ok, public_key} <- Base.decode16(public_key_hex, case: :lower),
         domain when is_binary(domain) <- Map.get(@signature_domains, version),
         true <- valid_signature?(domain <> bytes, signature, public_key) do
      {:ok, Base.encode16(:crypto.hash(:sha256, public_key), case: :lower)}
    else
      _ -> {:error, :invalid_managed_delegation_signature}
    end
  end

  def verify_signature(_bytes, _signature_hex, _public_key_hex, _version),
    do: {:error, :invalid_managed_delegation_signature}

  defp valid_signature?(message, signature, public_key) do
    :crypto.verify(:eddsa, :none, message, signature, [public_key, :ed25519])
  rescue
    _ -> false
  end

  defp plain_ancestors(path) do
    with {:ok, stat} <- File.lstat(path),
         true <- stat.type in [:regular, :directory] do
      parent = Path.dirname(path)
      if parent == path, do: :ok, else: plain_ancestors(parent)
    else
      false -> {:error, :managed_delegation_link}
      {:error, reason} -> {:error, reason}
    end
  end
end
