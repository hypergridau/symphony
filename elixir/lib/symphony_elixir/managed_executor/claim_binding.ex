defmodule SymphonyElixir.ManagedExecutor.ClaimBinding do
  @moduledoc """
  Retains the exact non-secret provider claim identity before managed allocation.

  The caller must supply the result returned by `WorkPackageClaim.claim/2` after
  provider validation and the root `claim_bound` witness acknowledgement. This
  validation binds that result to the assignment; it is not a replacement for
  the provider or root witness.
  """

  @contract "work-package-runtime-attestation.v1"
  @reservation_fields [
    :projection_id,
    :reservation_id,
    :workspace_id,
    :company_id,
    :issue_id,
    :runner_id,
    :managed_project_profile_id,
    :repository_ref,
    :generation,
    :session_id,
    :process_id,
    :responsible_delegation_id,
    :execution_fence_token,
    :runtime_lease_id
  ]
  @attestation_fields [
    :runner_id,
    :managed_project_profile_id,
    :reservation_id,
    :reservation_nonce,
    :issue_id,
    :generation,
    :session_id,
    :process_id,
    :responsible_delegation_id,
    :execution_fence_token,
    :runtime_lease_id,
    :repository_ref,
    :scope_keys
  ]

  @spec from_claim(term(), map()) :: {:ok, map()} | {:error, :provider_claim_invalid}
  def from_claim(claim, assignment) when is_map(assignment),
    do: from_claim(claim, assignment, Map.get(assignment, :seat))

  def from_claim(_claim, _assignment), do: {:error, :provider_claim_invalid}

  @spec from_claim(term(), map(), String.t()) :: {:ok, map()} | {:error, :provider_claim_invalid}
  def from_claim(%{reservation: reservation, attestation: attestation, response: response} = claim, assignment, runner_id)
      when is_map(reservation) and is_map(attestation) and is_map(response) and is_map(assignment) do
    with true <- Enum.sort(Map.keys(claim)) == Enum.sort([:reservation, :attestation, :response]),
         true <- valid_reservation?(reservation),
         true <- valid_attestation?(attestation, reservation),
         true <- matches_assignment?(reservation, assignment, runner_id),
         true <- matches_response?(response, reservation) do
      {:ok,
       reservation
       |> Map.take(@reservation_fields)
       |> Map.put(:scope_keys, Enum.sort(reservation.scope_keys))
       |> Map.put(:nonce_sha256, sha256(reservation.reservation_nonce))}
    else
      _ -> {:error, :provider_claim_invalid}
    end
  end

  def from_claim(_claim, _assignment, _runner_id), do: {:error, :provider_claim_invalid}

  defp valid_reservation?(reservation) do
    generation = Map.get(reservation, :generation)

    Enum.all?(@reservation_fields -- [:generation], &present?(Map.get(reservation, &1))) and
      is_integer(generation) and generation > 0 and
      valid_scopes?(Map.get(reservation, :scope_keys)) and
      present?(Map.get(reservation, :reservation_nonce))
  end

  defp valid_attestation?(attestation, reservation) do
    Enum.sort(Map.keys(attestation)) ==
      Enum.sort(@attestation_fields ++ [:contract_version, :attested_at, :signature]) and
      Map.get(attestation, :contract_version) == @contract and
      present?(Map.get(attestation, :attested_at)) and
      present?(Map.get(attestation, :signature)) and
      Enum.all?(@attestation_fields, fn field -> Map.get(attestation, field) == Map.get(reservation, field) end)
  end

  defp matches_assignment?(reservation, assignment, runner_id) do
    lease = Map.get(assignment, :lease, %{})

    pairs = [
      {:issue_id, Map.get(lease, :issue_id)},
      {:repository_ref, Map.get(assignment, :repository_ref)},
      {:runner_id, runner_id},
      {:responsible_delegation_id, List.last(Map.get(assignment, :intent_ancestry, []))},
      {:generation, Map.get(lease, :generation)},
      {:session_id, Map.get(lease, :session_id)},
      {:process_id, Map.get(lease, :process_id)}
    ]

    Enum.all?(pairs, fn {field, expected} -> Map.get(reservation, field) == expected end) and
      Map.get(reservation, :execution_fence_token) ==
        "#{Map.get(reservation, :issue_id)}:#{Map.get(reservation, :generation)}" and
      Map.get(reservation, :runtime_lease_id) == Map.get(reservation, :session_id)
  end

  defp matches_response?(response, reservation) do
    response == %{
      projection_id: reservation.projection_id,
      projection_state: "active",
      mutation_state: "applied",
      claim_evidence: %{
        "responsibleDelegationId" => reservation.responsible_delegation_id,
        "executionFenceToken" => reservation.execution_fence_token,
        "runtimeLeaseId" => reservation.runtime_lease_id
      }
    }
  end

  defp valid_scopes?(scopes) when is_list(scopes) and scopes != [] do
    length(scopes) == length(Enum.uniq(scopes)) and Enum.all?(scopes, &present?/1)
  end

  defp valid_scopes?(_scopes), do: false

  defp present?(value), do: is_binary(value) and byte_size(value) > 0
  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
