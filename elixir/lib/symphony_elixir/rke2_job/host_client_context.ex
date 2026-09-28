defmodule SymphonyElixir.RKE2Job.HostClientContext do
  @moduledoc """
  Reads the persistent host's Kubernetes token for one RKE2 Job operation.

  The trusted caller supplies the API endpoint and a root-controlled credential
  directory. A token rotator may atomically replace `kubernetes-api.token` in
  that directory; the bearer is read again for the next operation and is never
  put in an assignment, journal, or application configuration.
  """

  @behaviour SymphonyElixir.RKE2Job.ClientContext

  import Bitwise, only: [&&&: 2]

  @operations ~w(allocate activate delete abort_prepare abort_confirm finalize)a
  @max_token_bytes 16_384
  @digest ~r/\A[a-f0-9]{64}\z/
  @test_build Mix.env() == :test

  @impl true
  @spec client_context(map(), atom(), String.t(), term()) :: {:ok, map()} | {:error, atom()}
  def client_context(assignment, operation, idempotency_key, config)
      when operation in @operations and is_map(assignment) and is_binary(idempotency_key) and is_map(config) do
    with :ok <- exact_assignment(assignment, operation, idempotency_key),
         owner_uid = if(@test_build, do: Map.get(config, :test_owner_uid, 0), else: 0),
         {:ok, root} <- credential_root(config, owner_uid),
         {:ok, token} <- read_token(Path.join(root, "kubernetes-api.token"), owner_uid),
         :ok <- regular_owner_file(Path.join(root, "kubernetes-ca.crt"), 1_048_576, owner_uid),
         api_server when is_binary(api_server) <- Map.get(config, :api_server),
         timeout_ms when is_integer(timeout_ms) and timeout_ms in 1..30_000 <-
           Map.get(config, :timeout_ms, 10_000) do
      {:ok,
       %{
         api_server: api_server,
         namespace: "frigga",
         bearer_token: token,
         ca_certfile: Path.join(root, "kubernetes-ca.crt"),
         timeout_ms: timeout_ms
       }}
    else
      _ -> {:error, :rke2_host_client_context_unavailable}
    end
  end

  def client_context(_assignment, _operation, _idempotency_key, _config),
    do: {:error, :rke2_host_client_context_unavailable}

  defp exact_assignment(
         %{sha256: digest, environment: %{target_environment: :rke2}},
         operation,
         idempotency_key
       )
       when is_binary(digest) do
    if Regex.match?(@digest, digest) and idempotency_key == digest <> ":" <> key_operation(operation),
      do: :ok,
      else: :error
  end

  defp exact_assignment(_assignment, _operation, _idempotency_key), do: :error

  defp key_operation(:allocate), do: "allocation"
  defp key_operation(operation) when operation in [:abort_prepare, :abort_confirm], do: "abort_unstarted"
  defp key_operation(operation), do: Atom.to_string(operation)

  defp credential_root(%{credential_root: root}, owner_uid) when is_binary(root) do
    if Path.type(root) == :absolute and Path.expand(root) == root do
      case File.lstat(root) do
        {:ok, %{type: :directory, uid: ^owner_uid, mode: mode}}
        when (mode &&& 0o022) == 0 ->
          {:ok, root}

        _ ->
          :error
      end
    else
      :error
    end
  end

  defp credential_root(_config, _owner_uid), do: :error

  defp read_token(path, owner_uid) do
    with :ok <- regular_owner_file(path, @max_token_bytes, owner_uid),
         {:ok, raw} <- File.read(path),
         true <- byte_size(raw) <= @max_token_bytes,
         token = String.trim_trailing(raw, "\n"),
         true <- byte_size(token) > 0 and byte_size(token) <= @max_token_bytes,
         true <- String.match?(token, ~r/\A[A-Za-z0-9_\-.]+\z/) do
      {:ok, token}
    else
      _ -> :error
    end
  end

  defp regular_owner_file(path, maximum, owner_uid) do
    case File.lstat(path) do
      {:ok, %{type: :regular, uid: ^owner_uid, mode: mode, size: size}}
      when (mode &&& 0o022) == 0 and size > 0 and size <= maximum ->
        :ok

      _ ->
        :error
    end
  end
end
