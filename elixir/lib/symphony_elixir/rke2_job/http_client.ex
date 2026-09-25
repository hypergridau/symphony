defmodule SymphonyElixir.RKE2Job.HTTPClient do
  @moduledoc """
  HTTPS Kubernetes API client for the RKE2 Job provider port.

  The caller supplies host-owned settings in the client context: `:api_server`,
  `:namespace`, `:bearer_token`, and `:ca_certfile`. These are required;
  `:timeout_ms` defaults to 10 seconds and is capped at 30 seconds. The namespace
  must match the exact namespace argument on every call. `:test_plug` exists only
  to route deterministic tests through Req.Test and must not be configured at runtime.
  """

  @behaviour SymphonyElixir.RKE2Job.Client

  @api_path "/apis/batch/v1/namespaces/"
  @default_timeout_ms 10_000
  @max_timeout_ms 30_000
  @max_ca_file_bytes 1_048_576
  @test_environment Mix.env() == :test

  @impl true
  @spec create_job(String.t(), map(), term()) :: {:ok, map()} | {:error, term()}
  def create_job(namespace, job, context) do
    with {:ok, settings} <- settings(context, namespace),
         true <- is_map(job) do
      request(:post, jobs_path(namespace), job, settings, :job)
    else
      false -> {:error, :invalid_kubernetes_job}
      {:error, _reason} = error -> error
    end
  end

  @impl true
  @spec get_job(String.t(), String.t(), term()) :: {:ok, map()} | {:error, term()}
  def get_job(namespace, name, context) do
    with {:ok, settings} <- settings(context, namespace),
         true <- valid_name?(name) do
      case request(:get, jobs_path(namespace) <> "/" <> name, nil, settings, :job) do
        {:error, {:kubernetes_http_status, 404}} -> {:error, :not_found}
        result -> result
      end
    else
      false -> {:error, :invalid_kubernetes_job_identity}
      {:error, _reason} = error -> error
    end
  end

  @impl true
  @spec delete_job(String.t(), String.t(), String.t(), term()) :: :ok | {:error, term()}
  def delete_job(namespace, name, uid, context) do
    with {:ok, settings} <- settings(context, namespace),
         true <- valid_name?(name),
         true <- valid_uid?(uid) do
      body = %{
        "apiVersion" => "v1",
        "kind" => "DeleteOptions",
        "preconditions" => %{"uid" => uid}
      }

      request(:delete, jobs_path(namespace) <> "/" <> name, body, settings, :delete)
    else
      false -> {:error, :invalid_kubernetes_job_identity}
      {:error, _reason} = error -> error
    end
  end

  defp request(method, path, body, settings, response_kind) do
    case Req.request(request_options(method, path, body, settings)) do
      {:ok, response} -> http_response(method, response_kind, response)
      {:error, reason} -> transport_error(reason)
    end
  end

  defp http_response(_method, response_kind, %{status: status, body: body}) when status in 200..299,
    do: decode_success(response_kind, status, body)

  defp http_response(:post, _response_kind, %{status: 409}), do: {:error, :already_exists}
  defp http_response(_method, _response_kind, %{status: status}) when is_integer(status), do: {:error, {:kubernetes_http_status, status}}

  defp transport_error(%Req.TransportError{reason: :timeout}), do: {:error, :timeout}
  defp transport_error(_reason), do: {:error, :kubernetes_transport_failed}

  defp decode_success(:job, _status, %{"apiVersion" => "batch/v1", "kind" => "Job", "metadata" => metadata} = body)
       when is_map(metadata),
       do: {:ok, body}

  defp decode_success(:job, _status, _body), do: {:error, :invalid_kubernetes_job_response}
  defp decode_success(:delete, 204, _body), do: :ok
  defp decode_success(:delete, _status, %{"kind" => "Status", "status" => "Success"}), do: :ok
  defp decode_success(:delete, _status, _body), do: {:error, :invalid_kubernetes_delete_response}

  defp request_options(method, path, body, settings) do
    opts = [
      method: method,
      url: settings.api_server <> path,
      headers: [{"authorization", "Bearer " <> settings.bearer_token}, {"accept", "application/json"}],
      connect_options: [
        timeout: settings.timeout_ms,
        transport_opts: [
          verify: :verify_peer,
          cacertfile: settings.ca_certfile,
          server_name_indication: String.to_charlist(settings.host)
        ]
      ],
      receive_timeout: settings.timeout_ms,
      retry: false,
      redirect: false
    ]

    opts = if is_nil(body), do: opts, else: Keyword.put(opts, :json, body)
    if settings.test_plug, do: Keyword.put(opts, :plug, {Req.Test, settings.test_plug}), else: opts
  end

  defp settings(context, namespace) when is_map(context) do
    api_server = Map.get(context, :api_server)
    configured_namespace = Map.get(context, :namespace)
    bearer_token = Map.get(context, :bearer_token)
    ca_certfile = Map.get(context, :ca_certfile)
    timeout_ms = Map.get(context, :timeout_ms, @default_timeout_ms)
    test_plug = Map.get(context, :test_plug)

    with true <- configured_namespace == namespace and valid_name?(namespace),
         true <- valid_token?(bearer_token),
         true <- valid_ca_file?(ca_certfile),
         true <- valid_timeout?(timeout_ms),
         {:ok, uri} <- api_server_uri(api_server),
         true <- valid_test_plug?(test_plug) do
      {:ok,
       %{
         api_server: String.trim_trailing(api_server, "/"),
         host: uri.host,
         namespace: namespace,
         bearer_token: bearer_token,
         ca_certfile: ca_certfile,
         timeout_ms: timeout_ms,
         test_plug: test_plug
       }}
    else
      _ -> {:error, :rke2_job_client_config_invalid}
    end
  end

  defp settings(_context, _namespace), do: {:error, :rke2_job_client_config_invalid}

  defp api_server_uri(value) when is_binary(value) do
    uri = URI.parse(value)

    if valid_api_server_uri?(uri) do
      {:ok, uri}
    else
      {:error, :invalid_api_server}
    end
  rescue
    _error -> {:error, :invalid_api_server}
  end

  defp api_server_uri(_value), do: {:error, :invalid_api_server}

  defp valid_api_server_uri?(uri) do
    [
      uri.scheme == "https",
      is_binary(uri.host) and uri.host != "",
      is_nil(uri.userinfo),
      uri.path in [nil, ""],
      is_nil(uri.query),
      is_nil(uri.fragment),
      is_nil(uri.port) or uri.port in 1..65_535
    ]
    |> Enum.all?(& &1)
  end

  defp valid_name?(value) when is_binary(value),
    do: byte_size(value) in 1..63 and Regex.match?(~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/, value)

  defp valid_name?(_value), do: false

  defp valid_uid?(value) when is_binary(value),
    do: byte_size(value) in 1..256 and Regex.match?(~r/\A[a-zA-Z0-9][a-zA-Z0-9._:-]*\z/, value)

  defp valid_uid?(_value), do: false

  defp valid_token?(value) when is_binary(value),
    do: byte_size(value) in 1..8192 and Regex.match?(~r/\A[A-Za-z0-9._~+\/-]+=*\z/, value)

  defp valid_token?(_value), do: false

  defp valid_ca_file?(value) when is_binary(value) and value != "" do
    case File.lstat(value) do
      {:ok, %File.Stat{type: :regular, size: size}} when size in 1..@max_ca_file_bytes ->
        match?({:ok, _contents}, File.read(value))

      _ ->
        false
    end
  end

  defp valid_ca_file?(_value), do: false

  defp valid_timeout?(value), do: is_integer(value) and value in 1..@max_timeout_ms

  defp valid_test_plug?(nil), do: true
  defp valid_test_plug?(plug), do: @test_environment and is_atom(plug)

  defp jobs_path(namespace), do: @api_path <> namespace <> "/jobs"
end
