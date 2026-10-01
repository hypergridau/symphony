defmodule SymphonyElixir.RKE2Job.RootAbortInputPublisher do
  @moduledoc """
  Narrow caller contract for asking the trusted root service to publish HGS-733
  pre-execution abort inputs.

  Symphony sends selectors and bindings only. The root service must read the
  claim, issue mapping, checkpoint bytes, blocked result, and proof context from
  its own trusted state before invoking Dahlia's root-only publisher. The
  paired root socket service is a separate deployment slice; until installed,
  requests fail closed.
  """

  @socket_path "/run/dahlia-pre-execution-abort-input-publisher.sock"
  @timeout_ms 5_000
  @request_fields ~w(schemaVersion operation claimSHA256 assignmentDigest allocationId resultReference)
  @response_fields ~w(status claimSHA256)
  @hex64 ~r/\A[a-f0-9]{64}\z/
  @reference ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,511}\z/

  @type request :: map()

  @spec publish(map()) :: :ok | {:held, atom()}
  def publish(request) when is_map(request) do
    with :ok <- validate_request(request),
         {:ok, response} <- exchange(request),
         :ok <- validate_response(response, request) do
      :ok
    else
      _ -> {:held, :root_abort_input_publication_unavailable}
    end
  rescue
    _ -> {:held, :root_abort_input_publication_unavailable}
  catch
    _kind, _reason -> {:held, :root_abort_input_publication_unavailable}
  end

  def publish(_request), do: {:held, :root_abort_input_publication_unavailable}

  @doc false
  @spec validate_request(term()) :: :ok | {:error, atom()}
  def validate_request(request) when is_map(request) do
    with true <- Enum.sort(Map.keys(request)) == Enum.sort(@request_fields),
         true <- request["schemaVersion"] === 1,
         true <- request["operation"] == "publish_pre_execution_abort_inputs",
         true <- digest?(request["claimSHA256"]),
         true <- digest?(request["assignmentDigest"]),
         true <- text?(request["allocationId"], 1024),
         true <- is_binary(request["resultReference"]) and Regex.match?(@reference, request["resultReference"]) do
      :ok
    else
      _ -> {:error, :invalid_root_abort_input_request}
    end
  end

  def validate_request(_request), do: {:error, :invalid_root_abort_input_request}

  defp exchange(request) do
    options = [:binary, active: false, packet: :line, packet_size: 1_024]

    case :gen_tcp.connect({:local, String.to_charlist(@socket_path)}, 0, options, @timeout_ms) do
      {:ok, socket} ->
        try do
          with :ok <- :gen_tcp.send(socket, Jason.encode!(request) <> "\n"),
               {:ok, line} <- :gen_tcp.recv(socket, 0, @timeout_ms),
               {:ok, %{"ok" => true, "result" => result}} when is_map(result) <- Jason.decode(line) do
            {:ok, result}
          else
            _ -> {:error, :root_abort_input_transport_failed}
          end
        after
          :gen_tcp.close(socket)
        end

      {:error, _reason} ->
        {:error, :root_abort_input_socket_unavailable}
    end
  end

  @doc false
  @spec validate_response(term(), term()) :: :ok | {:error, atom()}
  def validate_response(response, request) when is_map(response) do
    if Enum.sort(Map.keys(response)) == Enum.sort(@response_fields) and
         response["status"] == "root-abort-inputs-published" and
         response["claimSHA256"] == request["claimSHA256"],
       do: :ok,
       else: {:error, :invalid_root_abort_input_acknowledgement}
  end

  def validate_response(_response, _request), do: {:error, :invalid_root_abort_input_acknowledgement}

  defp digest?(value), do: is_binary(value) and Regex.match?(@hex64, value)
  defp text?(value, limit), do: is_binary(value) and byte_size(value) in 1..limit and String.valid?(value)
end
