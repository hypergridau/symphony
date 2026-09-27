defmodule SymphonyElixir.RKE2Job.AuthSlotSpec do
  @moduledoc """
  Compiles a host-selected Codex OAuth session slot into one disposable Job.

  The caller must obtain and retain an exclusive durable lease before supplying
  this configuration. This pure module binds the selected slot to the signed
  assignment; it neither grants a lease nor reads credential material.
  """

  @codex_home "/var/lib/frigga-codex-home"
  @volume_name "codex-auth-slot"

  @type fragments :: %{
          annotations: map(),
          env: [map()],
          volume_mounts: [map()],
          volumes: [map()]
        }

  @spec compile(map(), nil | map(), nil | map()) :: {:ok, fragments()} | {:error, :rke2_job_auth_slot_invalid}
  def compile(_assignment, nil, nil), do: {:ok, %{annotations: %{}, env: [], volume_mounts: [], volumes: []}}

  def compile(%{sha256: digest, seat: seat}, slot, catalog) when is_map(slot) and is_map(catalog) do
    if valid_binding?(slot, catalog, digest, seat) do
      {:ok,
       %{
         annotations: %{
           "symphony.hypergrid.au/codex-auth-slot" => slot.slot_id,
           "symphony.hypergrid.au/codex-auth-lease" => slot.lease_id
         },
         env: [%{"name" => "CODEX_HOME", "value" => @codex_home}],
         volume_mounts: [%{"name" => @volume_name, "mountPath" => @codex_home, "readOnly" => false}],
         volumes: [%{"name" => @volume_name, "persistentVolumeClaim" => %{"claimName" => slot.claim_name, "readOnly" => false}}]
       }}
    else
      {:error, :rke2_job_auth_slot_invalid}
    end
  end

  def compile(_assignment, _slot, _catalog), do: {:error, :rke2_job_auth_slot_invalid}

  defp valid_binding?(slot, catalog, digest, seat) do
    Enum.sort(Map.keys(slot)) == Enum.sort([:slot_id, :claim_name, :lease_id, :assignment_sha256, :seat]) and
      valid_slot_values?(slot, digest, seat) and valid_catalog?(catalog) and
      Map.get(catalog, slot.slot_id) == slot.claim_name
  end

  defp valid_slot_values?(slot, digest, seat) do
    dns_label?(slot.slot_id) and dns_label?(slot.claim_name) and valid_lease_id?(slot.lease_id) and
      slot.assignment_sha256 == digest and slot.seat == seat
  end

  defp valid_catalog?(catalog) when map_size(catalog) in 1..16 do
    claims = Map.values(catalog)

    Enum.all?(catalog, fn {slot_id, claim_name} -> dns_label?(slot_id) and dns_label?(claim_name) end) and
      length(Enum.uniq(claims)) == length(claims)
  end

  defp valid_catalog?(_catalog), do: false

  defp dns_label?(value) when is_binary(value),
    do: byte_size(value) <= 63 and Regex.match?(~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/, value)

  defp dns_label?(_value), do: false

  defp valid_lease_id?(value) when is_binary(value),
    do: byte_size(value) in 1..128 and Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._:-]*\z/, value)

  defp valid_lease_id?(_value), do: false
end
