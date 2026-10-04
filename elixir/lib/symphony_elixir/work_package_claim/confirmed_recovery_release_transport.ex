defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseTransport do
  @moduledoc "Bounded provider transport using the existing host runner and admin identities. No automatic retries."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence

  @origin "https://dahlia.hypergrid.au"
  @prefix "/provider/v1/work-packages/claim-recovery"
  @max_bytes 262_144
  @test_runtime Mix.env() == :test

  @spec approval(map(), String.t(), map()) :: {:ok, map()} | {:error, atom()}
  def approval(binding, id, context),
    do: request(:post, @prefix <> "/release-only/decision-readback", %{"approvalId" => id, "binding" => binding}, context)

  @spec held(map(), map()) :: {:ok, map()} | {:error, atom()}
  def held(binding, context) do
    expected = binding["expected"]
    path = @prefix <> "/" <> URI.encode_www_form(expected["projectionId"]) <> "/held?assignmentDigest=" <> Evidence.tuple_digest(expected)
    request(:get, path, nil, context)
  end

  @spec consume(:prepare | :confirm, map(), String.t(), map()) :: {:ok, map()} | {:error, atom()}
  def consume(phase, bundle, id, context) when phase in [:prepare, :confirm] do
    if context[:protocol_accepted] == true and context[:trust_enrolled] == true,
      do: request(:post, @prefix <> "/release-only/" <> Atom.to_string(phase), %{"bundle" => bundle, "providerApprovalId" => id}, context),
      else: {:error, :hgs740_release_protocol_not_admitted}
  end

  @spec confirmed_readback(map(), String.t(), map()) :: {:ok, map()} | {:error, atom()}
  def confirmed_readback(bundle, id, context) do
    if context[:historical_readback] == true,
      do: request(:post, @prefix <> "/release-only/confirmation-readback", %{"bundle" => bundle, "providerApprovalId" => id}, context, :historical),
      else: {:error, :hgs740_confirmation_readback_not_admitted}
  end

  defp request(method, path, body, context, mode \\ :write) do
    with true <- admitted?(context, mode),
         true <- valid_secret?(context[:runner_token]) and valid_secret?(context[:admin_token]),
         raw = if(is_nil(body), do: nil, else: Evidence.canonical_json(body)),
         true <- is_nil(raw) or byte_size(raw) <= 1_048_576,
         options = options(method, path, raw, context),
         {:ok, _started} <- Application.ensure_all_started(:req),
         {:ok, %Req.Response{status: 200, body: bytes, headers: headers}} <- Req.request(options),
         true <- is_binary(bytes) and byte_size(bytes) <= @max_bytes,
         true <- headers["cache-control"] == ["no-store"],
         {:ok, %{"data" => data}} <- Jason.decode(bytes),
         true <- is_map(data) do
      {:ok, data}
    else
      _ -> {:error, :hgs740_provider_transport_held_closed}
    end
  rescue
    _ -> {:error, :hgs740_provider_transport_held_closed}
  end

  defp admitted?(context, :historical), do: context[:historical_readback] == true
  defp admitted?(context, :write), do: context[:protocol_accepted] == true and context[:trust_enrolled] == true

  defp options(method, path, body, context) do
    options = [
      method: method,
      url: @origin <> path,
      body: body,
      decode_body: false,
      headers: [{"authorization", "Bearer " <> context.runner_token}, {"x-provider-admin-token", context.admin_token}, {"content-type", "application/json"}, {"accept", "application/json"}],
      retry: false,
      redirect: false,
      receive_timeout: 10_000,
      into: &bounded_body/2,
      connect_options: [timeout: 5_000]
    ]

    if @test_runtime and context[:test_plug], do: Keyword.put(options, :plug, {Req.Test, context.test_plug}), else: options
  end

  defp bounded_body({:data, bytes}, {request, response}) do
    body = (response.body || "") <> bytes
    if byte_size(body) > @max_bytes, do: raise("hgs740_provider_response_bound")
    {:cont, {request, %{response | body: body}}}
  end

  defp valid_secret?(value), do: is_binary(value) and byte_size(value) in 1..4096 and not Regex.match?(~r/\s/, value)
end
