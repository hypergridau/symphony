defmodule SymphonyElixir.RetainedGrantProof.ReceiptEvidence do
  @moduledoc """
  Validates retained cleanup receipt integrity against an independently validated claim.

  This pure check does not authenticate a signer, establish current authority,
  release capacity or scope, or authorize retained-work admission.
  """

  alias SymphonyElixir.WorkPackageCleanupReceipt, as: Cleanup

  @invalid {:error, :retained_cleanup_receipt_invalid}
  @claim_keys ~w(issue_id managed_project_profile_id repository_ref projection_id
    reservation_id reservation_nonce scope_keys runner_id generation session_id
    process_id responsible_delegation_id execution_fence_token runtime_lease_id)a
  @authority_keys @claim_keys -- [:projection_id]
  @claim_text_keys @claim_keys -- [:generation, :scope_keys]
  @receipt_keys @authority_keys ++
                  ~w(contract_version receipt_id receipt_kind terminal_outcome
                    observed_at evidence_ref accepted_head)a
  @receipt_optional_keys [:attested_at, :signature, :acknowledgement]
  @receipt_text_keys @receipt_keys -- [:generation, :scope_keys]
  @ack_keys ~w(projection_id reservation_id receipt_id receipt_kind
    execution_capacity_state scope_state reservation_state generation evidence_ref
    accepted_head replayed)a
  @ack_text_keys @ack_keys -- [:generation, :replayed]

  @spec validate_stored_readonly(term(), term()) ::
          {:ok, %{receipt: map(), acknowledgement: map()}}
          | {:error, :retained_cleanup_receipt_invalid}
  def validate_stored_readonly(receipt, claim) when is_map(receipt) and is_map(claim) do
    with true <- valid_claim?(claim),
         true <- valid_receipt?(receipt),
         true <- valid_acknowledgement?(Map.get(receipt, :acknowledgement)),
         true <- Map.take(receipt, @authority_keys) == Map.take(claim, @authority_keys),
         {:ok, kind} <- receipt_kind(receipt),
         authority <- build_authority(claim),
         :ok <- Cleanup.valid_stored_semantic(receipt, authority, kind),
         {:ok, acknowledgement} <- Cleanup.stored_acknowledgement(receipt, authority, claim, kind) do
      {:ok, %{receipt: receipt, acknowledgement: acknowledgement}}
    else
      _ -> @invalid
    end
  end

  def validate_stored_readonly(_receipt, _claim), do: @invalid

  defp valid_claim?(claim) do
    exact_atom_keys?(claim, @claim_keys, []) and
      valid_text_fields?(claim, @claim_text_keys) and
      valid_scope_keys?(Map.get(claim, :scope_keys)) and
      positive_integer?(Map.get(claim, :generation))
  end

  defp valid_receipt?(receipt) do
    exact_atom_keys?(receipt, @receipt_keys, @receipt_optional_keys) and
      valid_text_fields?(receipt, @receipt_text_keys) and
      valid_scope_keys?(Map.get(receipt, :scope_keys)) and
      positive_integer?(Map.get(receipt, :generation)) and
      Enum.all?([:attested_at, :signature], fn key ->
        not Map.has_key?(receipt, key) or valid_text?(Map.get(receipt, key))
      end)
  end

  defp valid_acknowledgement?(ack) when is_map(ack) and map_size(ack) == 11 do
    pairs = Enum.map(ack, fn {key, value} -> {Cleanup.acknowledgement_key(key), value} end)
    keys = Enum.map(pairs, &elem(&1, 0))

    not Enum.any?(keys, &is_nil/1) and
      length(Enum.uniq(keys)) == 11 and
      Enum.sort(keys) == Enum.sort(@ack_keys) and
      valid_acknowledgement_values?(Map.new(pairs))
  end

  defp valid_acknowledgement?(_ack), do: false

  defp valid_acknowledgement_values?(ack) do
    valid_text_fields?(ack, @ack_text_keys) and
      positive_integer?(Map.get(ack, :generation)) and is_boolean(Map.get(ack, :replayed))
  end

  defp exact_atom_keys?(map, required, optional) do
    keys = Map.keys(map)

    Enum.all?(keys, &is_atom/1) and
      Enum.sort(keys) == Enum.sort(required ++ Enum.filter(optional, &Map.has_key?(map, &1)))
  end

  defp valid_text_fields?(map, keys), do: Enum.all?(keys, &valid_text?(Map.get(map, &1)))

  defp valid_text?(value) when is_binary(value),
    do: String.valid?(value) and String.trim(value) != ""

  defp valid_text?(_value), do: false

  defp valid_scope_keys?(keys) when is_list(keys) do
    keys != [] and Enum.all?(keys, &valid_text?/1) and
      Enum.uniq(keys) == keys and Enum.sort(keys) == keys
  end

  defp valid_scope_keys?(_keys), do: false
  defp positive_integer?(value), do: is_integer(value) and value > 0

  defp build_authority(claim) do
    Map.merge(claim, %{
      execution: %{generation: claim.generation},
      lease: %{session_id: claim.session_id, process_id: claim.process_id}
    })
  end

  defp receipt_kind(%{receipt_kind: "termination_confirmed"}), do: {:ok, :termination_confirmed}

  defp receipt_kind(%{receipt_kind: "repository_cleanup_verified"}),
    do: {:ok, :repository_cleanup_verified}

  defp receipt_kind(_receipt), do: @invalid
end
