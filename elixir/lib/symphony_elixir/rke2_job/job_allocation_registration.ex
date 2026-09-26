defmodule SymphonyElixir.RKE2Job.JobAllocationRegistration do
  @moduledoc """
  Registers a suspended Job's Kubernetes UID with Dahlia before allocation succeeds.

  The runner token stays on the trusted Frigga host. An uncertain response is
  held and retried with the same allocation ID; Dahlia accepts only the first UID.
  """

  @connect_timeout_ms 5_000
  @request_timeout_ms 10_000

  @spec ready?(term()) :: boolean()
  def ready?(context), do: match?({:ok, _base_url, _token}, configuration(context))

  @spec register(String.t(), String.t(), String.t(), term()) :: :ok | {:held, atom()}
  def register(allocation_id, expected_uid, reservation_id, context) do
    with {:ok, base_url, token} <- configuration(context),
         true <- valid_reservation_id?(reservation_id) and valid_uid?(expected_uid),
         true <- allocation_matches_uid?(allocation_id, expected_uid),
         url =
           base_url <>
             "/runner/v1/verified-assignments/" <>
             URI.encode(reservation_id, &URI.char_unreserved?/1) <> "/job-allocation",
         {:ok, %Req.Response{status: status, body: body}} <-
           Map.get(context, :post_fun, &Req.post/2).(url,
             headers: [{"authorization", "Bearer " <> token}],
             json: %{allocationId: allocation_id},
             connect_options: [timeout: @connect_timeout_ms],
             receive_timeout: @request_timeout_ms,
             retry: false,
             redirect: false
           ),
         true <- status in 200..299,
         %{"data" => %{"jobUid" => ^expected_uid, "replayed" => replayed}} <- body,
         true <- is_boolean(replayed) do
      :ok
    else
      _ -> {:held, :job_allocation_registration_unverified}
    end
  rescue
    _error -> {:held, :job_allocation_registration_unverified}
  end

  defp configuration(%{base_url: base_url, runner_token: token})
       when is_binary(base_url) and is_binary(token) and byte_size(token) > 0 do
    if valid_base_url?(base_url) do
      {:ok, String.trim_trailing(base_url, "/"), token}
    else
      {:error, :invalid_provider_url}
    end
  end

  defp configuration(_context), do: {:error, :registration_configuration_missing}

  defp valid_base_url?(base_url) do
    uri = URI.parse(base_url)

    uri.scheme == "https" and is_binary(uri.host) and uri.host != "" and
      uri.userinfo == nil and uri.query == nil and uri.fragment == nil and uri.path in [nil, "", "/"]
  end

  defp valid_reservation_id?(value) when is_binary(value),
    do: byte_size(value) in 1..256 and String.valid?(value)

  defp valid_reservation_id?(_value), do: false

  defp valid_uid?(uid) when is_binary(uid),
    do: byte_size(uid) in 1..256 and Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._:-]*\z/, uid)

  defp valid_uid?(_uid), do: false

  defp allocation_matches_uid?("rke2job:v1:" <> encoded, expected_uid) when byte_size(encoded) <= 1_024 do
    with {:ok, raw} <- Base.url_decode64(encoded, padding: false),
         {:ok, [1, _namespace, _name, ^expected_uid, _digest]} <- Jason.decode(raw) do
      true
    else
      _ -> false
    end
  end

  defp allocation_matches_uid?(_allocation_id, _expected_uid), do: false
end
