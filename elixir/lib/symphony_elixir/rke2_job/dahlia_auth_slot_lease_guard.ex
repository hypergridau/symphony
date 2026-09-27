defmodule SymphonyElixir.RKE2Job.DahliaAuthSlotLeaseGuard do
  @moduledoc """
  Trusted Frigga-host client for Dahlia's exclusive Codex OAuth slot lease.

  The host must first reserve the slot and put Dahlia's returned lease ID and
  claim into the Job configuration. This guard verifies that reservation again
  before creation, then binds and checks the exact registered Job allocation.
  Release stays held until a separate cleanup verifier can prove Pod absence,
  volume detachment, and durable auth-cache health.
  """

  @behaviour SymphonyElixir.RKE2Job.AuthSlotLeaseGuard

  @connect_timeout_ms 5_000
  @request_timeout_ms 10_000

  @impl true
  def reserve(slot, assignment, context) do
    with :ok <- matching_assignment?(slot, assignment),
         {:ok, data} <-
           post(context, "/reserve", %{
             assignmentDigest: assignment.sha256,
             slotId: slot.slot_id
           }),
         true <-
           data["leaseId"] == slot.lease_id and data["slotId"] == slot.slot_id and
             data["claimName"] == slot.claim_name and is_boolean(data["replayed"]) do
      :ok
    else
      _ -> {:held, :codex_auth_slot_reservation_unverified}
    end
  end

  @impl true
  def bind_uid(slot, assignment, allocation, context) do
    with :ok <- matching_assignment?(slot, assignment),
         {:ok, data} <-
           post(context, "/" <> slot.lease_id <> "/bind-job", %{
             allocationId: allocation.id
           }),
         true <- data["bound"] == true do
      :ok
    else
      _ -> {:held, :codex_auth_slot_binding_unverified}
    end
  end

  @impl true
  def authorize(slot, assignment, allocation, context) do
    with :ok <- matching_assignment?(slot, assignment),
         {:ok, data} <-
           post(context, "/" <> slot.lease_id <> "/authorize", %{
             allocationId: allocation.id
           }),
         true <- data["authorized"] == true do
      :ok
    else
      _ -> {:held, :codex_auth_slot_authorization_unverified}
    end
  end

  @impl true
  def release(_slot, _assignment, _allocation, _context),
    do: {:held, :codex_auth_slot_release_verification_unavailable}

  defp matching_assignment?(
         %{assignment_sha256: digest, lease_id: lease_id, slot_id: slot_id, claim_name: claim_name},
         %{sha256: digest}
       )
       when is_binary(lease_id) and is_binary(slot_id) and is_binary(claim_name) do
    if Regex.match?(~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, lease_id) and
         Regex.match?(~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/, slot_id) and
         Regex.match?(~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/, claim_name) and
         byte_size(slot_id) <= 63 and byte_size(claim_name) <= 63 do
      :ok
    else
      :invalid_binding
    end
  end

  defp matching_assignment?(_slot, _assignment), do: :invalid_binding

  defp post(context, suffix, body) do
    with {:ok, base_url, token, reservation_id} <- configuration(context),
         url =
           base_url <>
             "/runner/v1/verified-assignments/" <>
             URI.encode(reservation_id, &URI.char_unreserved?/1) <> "/codex-auth-slots" <> suffix,
         {:ok, %Req.Response{status: status, body: %{"data" => data}}} <-
           Map.get(context, :post_fun, &Req.post/2).(url,
             headers: [{"authorization", "Bearer " <> token}],
             json: body,
             connect_options: [timeout: @connect_timeout_ms],
             receive_timeout: @request_timeout_ms,
             retry: false,
             redirect: false
           ),
         true <- status in 200..299 and is_map(data) do
      {:ok, data}
    else
      _ -> {:error, :unverified}
    end
  rescue
    _error -> {:error, :unverified}
  end

  defp configuration(%{base_url: base_url, runner_token: token, reservation_id: reservation_id})
       when is_binary(base_url) and is_binary(token) and byte_size(token) > 0 and
              is_binary(reservation_id) and byte_size(reservation_id) in 1..256 do
    if String.valid?(reservation_id) and valid_base_url?(base_url) do
      {:ok, String.trim_trailing(base_url, "/"), token, reservation_id}
    else
      {:error, :invalid_configuration}
    end
  end

  defp configuration(_context), do: {:error, :invalid_configuration}

  defp valid_base_url?(base_url) do
    uri = URI.parse(base_url)

    uri.scheme == "https" and is_binary(uri.host) and uri.host != "" and
      uri.userinfo == nil and uri.query == nil and uri.fragment == nil and
      uri.path in [nil, "", "/"]
  end
end
