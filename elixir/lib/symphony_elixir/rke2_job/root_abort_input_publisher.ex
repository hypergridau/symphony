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
  @selector_fields ~w(schemaVersion operation claimSHA256 assignmentDigest allocationId resultReference)
  @eligibility_fields ~w(status claimSHA256 assignmentDigest allocationId resultReference)
  @disposal_fields ~w(
    status claimSHA256 assignmentDigest allocationId resultReference resultSHA256 prepareId prepareRequestSHA256 observedAt
  )
  @response_fields ~w(status claimSHA256)
  @hex64 ~r/\A[a-f0-9]{64}\z/
  @uuid ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\z/
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

  @doc "Checks root-side eligibility before any destructive confirmation."
  @spec verify_eligibility(map()) :: :ok | {:held, atom()}
  def verify_eligibility(request) when is_map(request) do
    with :ok <- validate_eligibility_request(request),
         {:ok, response} <- exchange(request),
         :ok <- validate_eligibility_response(response, request) do
      :ok
    else
      _ -> {:held, :root_abort_input_eligibility_unavailable}
    end
  rescue
    _ -> {:held, :root_abort_input_eligibility_unavailable}
  catch
    _kind, _reason -> {:held, :root_abort_input_eligibility_unavailable}
  end

  def verify_eligibility(_request), do: {:held, :root_abort_input_eligibility_unavailable}

  @doc "Obtains a fresh root-side authorization to dispose the exact prepared Job."
  @spec verify_disposal(map()) :: {:ok, map()} | {:held, atom()}
  def verify_disposal(request) when is_map(request) do
    with :ok <- validate_disposal_request(request),
         {:ok, response} <- exchange(request),
         :ok <- validate_disposal_response(response, request) do
      {:ok, response}
    else
      _ -> {:held, :root_abort_input_disposal_unavailable}
    end
  rescue
    _ -> {:held, :root_abort_input_disposal_unavailable}
  catch
    _kind, _reason -> {:held, :root_abort_input_disposal_unavailable}
  end

  def verify_disposal(_request), do: {:held, :root_abort_input_disposal_unavailable}

  @doc false
  @spec validate_request(term()) :: :ok | {:error, atom()}
  def validate_request(request) when is_map(request) do
    validate_selector_request(request, "publish_pre_execution_abort_inputs")
  end

  def validate_request(_request), do: {:error, :invalid_root_abort_input_request}

  @doc false
  @spec validate_eligibility_request(term()) :: :ok | {:error, atom()}
  def validate_eligibility_request(request) when is_map(request) do
    case validate_selector_request(request, "verify_pre_execution_abort_eligibility") do
      :ok -> :ok
      _ -> {:error, :invalid_root_abort_eligibility_request}
    end
  end

  def validate_eligibility_request(_request), do: {:error, :invalid_root_abort_eligibility_request}

  @doc false
  @spec validate_eligibility_response(term(), term()) :: :ok | {:error, atom()}
  def validate_eligibility_response(response, request) when is_map(response) and is_map(request) do
    if exact_fields?(response, @eligibility_fields) and
         response["status"] == "pre-execution-abort-eligible" and
         Enum.all?(~w(claimSHA256 assignmentDigest allocationId resultReference), &(response[&1] == request[&1])),
       do: :ok,
       else: {:error, :invalid_root_abort_input_eligibility}
  end

  def validate_eligibility_response(_response, _request), do: {:error, :invalid_root_abort_input_eligibility}

  @doc false
  @spec validate_disposal_request(term()) :: :ok | {:error, atom()}
  def validate_disposal_request(request) when is_map(request) do
    case validate_selector_request(request, "verify_pre_execution_abort_disposal") do
      :ok -> :ok
      _ -> {:error, :invalid_root_abort_disposal_request}
    end
  end

  def validate_disposal_request(_request), do: {:error, :invalid_root_abort_disposal_request}

  @doc false
  @spec validate_disposal_response(term(), term()) :: :ok | {:error, atom()}
  def validate_disposal_response(response, request) when is_map(response) and is_map(request) do
    if disposal_response_shape?(response) and disposal_selectors_match?(response, request) and
         disposal_receipt_fields_valid?(response) do
      :ok
    else
      {:error, :invalid_root_abort_disposal_acknowledgement}
    end
  end

  def validate_disposal_response(_response, _request), do: {:error, :invalid_root_abort_disposal_acknowledgement}

  @doc false
  @spec validate_persisted_disposal_response(term(), term()) :: :ok | {:error, atom()}
  def validate_persisted_disposal_response(response, request) when is_map(response) and is_map(request) do
    if validate_disposal_request(request) == :ok and disposal_response_shape?(response) and
         disposal_selectors_match?(response, request) and
         persisted_disposal_receipt_fields_valid?(response) do
      :ok
    else
      {:error, :invalid_root_abort_disposal_acknowledgement}
    end
  end

  def validate_persisted_disposal_response(_response, _request),
    do: {:error, :invalid_root_abort_disposal_acknowledgement}

  defp disposal_response_shape?(response) do
    exact_fields?(response, @disposal_fields) and
      response["status"] == "pre-execution-abort-disposal-verified"
  end

  defp disposal_selectors_match?(response, request) do
    Enum.all?(~w(claimSHA256 assignmentDigest allocationId resultReference), &(response[&1] == request[&1]))
  end

  defp disposal_receipt_fields_valid?(response) do
    digest?(response["resultSHA256"]) and valid_prepare_id?(response["prepareId"]) and
      digest?(response["prepareRequestSHA256"]) and fresh_timestamp?(response["observedAt"])
  end

  defp persisted_disposal_receipt_fields_valid?(response) do
    digest?(response["resultSHA256"]) and valid_prepare_id?(response["prepareId"]) and
      digest?(response["prepareRequestSHA256"]) and valid_timestamp?(response["observedAt"])
  end

  defp valid_prepare_id?(value), do: is_binary(value) and Regex.match?(@uuid, value)

  defp exchange(request) do
    options = [:binary, active: false, packet: :line, packet_size: 4_096]

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
  defp exact_fields?(map, fields), do: Enum.sort(Map.keys(map)) == Enum.sort(fields)

  defp validate_selector_request(request, operation) when is_map(request) do
    with true <- exact_fields?(request, @selector_fields),
         true <- request["schemaVersion"] === 1,
         true <- request["operation"] == operation,
         true <- digest?(request["claimSHA256"]),
         true <- digest?(request["assignmentDigest"]),
         true <- text?(request["allocationId"], 1024),
         true <- is_binary(request["resultReference"]) and Regex.match?(@reference, request["resultReference"]) do
      :ok
    else
      _ -> {:error, :invalid_root_abort_input_request}
    end
  end

  defp fresh_timestamp?(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, _offset} ->
        delta = DateTime.diff(DateTime.utc_now(), datetime, :second)
        delta in 0..300

      _ ->
        false
    end
  end

  defp fresh_timestamp?(_value), do: false

  defp valid_timestamp?(value) when is_binary(value), do: match?({:ok, _, _}, DateTime.from_iso8601(value))
  defp valid_timestamp?(_value), do: false
end
