defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryProviderRelease do
  @moduledoc false

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence

  @claim_fields ~w(projectionId reservationId workspaceId companyId issueId runnerId managedProjectProfileId repositoryRef scopeKeys generation sessionId processId responsibleDelegationId executionFenceToken runtimeLeaseId nonceHash)
  @receipt_fields ~w(recoveryId projectionId fenceRevision oldTupleDigest oldNonceHash nextGenerationFloor confirmedAt proofDigest projectionState reservationState executionCapacityState scopeState)
  @payload_fields ~w(contractVersion expected receipt localGenerationMax journalSHA256 neverSpawned)
  @proof_fields ~w(contractVersion recoveryId projectionId fenceRevision oldTupleDigest runnerId hostIdentity bootId observedAt evidenceRef globalPause runnerStopped neverSpawned supervisedWorkerAbsent processCount workspaceAbsent localGenerationMax fenceSHA256 claimJournalSHA256)

  @spec canonical_payload(map()) :: binary() | nil
  def canonical_payload(payload) when is_map(payload) do
    with {:ok, expected} <- encode_ordered_object(payload["expected"], @claim_fields),
         {:ok, receipt} <- encode_ordered_object(payload["receipt"], @receipt_fields),
         fields = [
           {"contractVersion", payload["contractVersion"]},
           {"expected", {:raw, expected}},
           {"receipt", {:raw, receipt}},
           {"localGenerationMax", payload["localGenerationMax"]},
           {"journalSHA256", payload["journalSHA256"]},
           {"neverSpawned", payload["neverSpawned"]}
         ],
         {:ok, bytes} <- encode_ordered_fields(fields) do
      bytes
    else
      _ -> nil
    end
  end

  def canonical_payload(_payload), do: nil

  @spec validate_final_payload(map(), map(), String.t()) :: :ok | {:error, :provider_final_proof_invalid}
  def validate_final_payload(payload, marker, journal_sha256)
      when is_map(payload) and is_map(marker) and is_binary(journal_sha256) do
    with true <- exact_keys?(payload, @payload_fields),
         true <- payload["contractVersion"] == "work-package-pre-spawn-recovery.v1",
         true <- payload["expected"] == marker["expected"],
         true <- payload["localGenerationMax"] == 2 and payload["neverSpawned"] == true,
         true <- payload["journalSHA256"] == journal_sha256 and digest?(journal_sha256),
         :ok <- validate_release_receipt(payload["receipt"], marker) do
      :ok
    else
      _ -> {:error, :provider_final_proof_invalid}
    end
  rescue
    _ -> {:error, :provider_final_proof_invalid}
  end

  def validate_final_payload(_payload, _marker, _journal_sha256),
    do: {:error, :provider_final_proof_invalid}

  @spec validate_receipt_binding(map(), map(), String.t(), map(), String.t(), String.t(), binary()) ::
          :ok | {:error, :provider_confirmation_receipt_mismatch}
  def validate_receipt_binding(receipt, confirmed, recovery_id, expected, old_tuple_digest, fence_revision, proof_bytes)
      when is_map(receipt) and is_map(confirmed) and is_binary(recovery_id) and is_map(expected) and
             is_binary(old_tuple_digest) and is_binary(fence_revision) and is_binary(proof_bytes) do
    with true <- exact_keys?(confirmed, @receipt_fields),
         true <- confirmed == receipt,
         true <- receipt["recoveryId"] == recovery_id,
         true <- receipt["projectionId"] == expected["projectionId"],
         true <- receipt["oldTupleDigest"] == old_tuple_digest,
         true <- receipt["fenceRevision"] == fence_revision,
         true <- receipt["proofDigest"] == digest(proof_bytes) do
      :ok
    else
      _ -> {:error, :provider_confirmation_receipt_mismatch}
    end
  end

  def validate_receipt_binding(_receipt, _confirmed, _recovery_id, _expected, _old_tuple_digest, _fence_revision, _proof_bytes),
    do: {:error, :provider_confirmation_receipt_mismatch}

  @spec validate_operation(map(), map(), map(), binary(), binary()) ::
          :ok | {:error, :provider_final_operation_mismatch}
  def validate_operation(files, marker, payload, final_envelope_bytes, public_key)
      when is_map(files) and is_map(marker) and is_map(payload) and is_binary(final_envelope_bytes) and
             is_binary(public_key) do
    expected = marker["expected"]
    old_tuple_digest = ConfirmedRecoveryEvidence.tuple_digest(expected)
    recovery_id = "hgs719-#{marker["pool"]}-#{old_tuple_digest}"

    with :ok <- validate_observations(files, expected, payload),
         :ok <- validate_prepare(files, recovery_id, expected, old_tuple_digest),
         {:ok, proof_bytes} <- canonical_proof(files.confirmation["proof"]),
         :ok <- validate_confirmation(files, recovery_id, expected, old_tuple_digest, proof_bytes, public_key, marker),
         :ok <-
           validate_receipt_binding(
             payload["receipt"],
             files.confirm_response["data"],
             recovery_id,
             expected,
             old_tuple_digest,
             files.prepare_response["data"]["fenceRevision"],
             proof_bytes
           ),
         true <- files.signed_envelope_bytes == final_envelope_bytes do
      :ok
    else
      _ -> {:error, :provider_final_operation_mismatch}
    end
  rescue
    _ -> {:error, :provider_final_operation_mismatch}
  end

  def validate_operation(_files, _marker, _payload, _final_envelope_bytes, _public_key),
    do: {:error, :provider_final_operation_mismatch}

  @spec canonical_proof(map()) :: {:ok, binary()} | {:error, :provider_proof_invalid}
  def canonical_proof(proof) when is_map(proof) do
    with true <- exact_keys?(proof, @proof_fields),
         {:ok, bytes} <- encode_ordered_object(proof, @proof_fields) do
      {:ok, bytes}
    else
      _ -> {:error, :provider_proof_invalid}
    end
  end

  def canonical_proof(_proof), do: {:error, :provider_proof_invalid}

  defp validate_observations(files, expected, payload) do
    with true <- files.prepare_observation["expected"] == expected,
         true <- files.confirm_observation["expected"] == expected,
         true <- files.prepare_observation["localGenerationMax"] == 2,
         true <- files.confirm_observation["localGenerationMax"] == 2,
         true <- files.confirm_observation["claimJournalSHA256"] == payload["journalSHA256"],
         true <- timestamp?(files.prepare_observation["observedAt"]),
         true <- timestamp?(files.confirm_observation["observedAt"]) do
      :ok
    else
      _ -> {:error, :provider_operation_observation_mismatch}
    end
  end

  defp validate_prepare(files, recovery_id, expected, old_tuple_digest) do
    request = files.prepare_request
    response = files.prepare_response["data"]

    with true <- exact_keys?(request, ~w(recoveryId expected reason evidenceRef)),
         true <- request["recoveryId"] == recovery_id and request["expected"] == expected,
         true <- request["evidenceRef"] == "sha256:" <> digest(files.prepare_observation_bytes),
         true <-
           exact_keys?(
             response,
             ~w(recoveryId projectionId fenceRevision oldTupleDigest preparedAt state reservationState executionCapacityState scopeState)
           ),
         true <- response["recoveryId"] == recovery_id and response["projectionId"] == expected["projectionId"],
         true <- response["oldTupleDigest"] == old_tuple_digest,
         true <- response["state"] == "prepared" and response["reservationState"] == "claimed",
         true <- response["executionCapacityState"] == "held" and response["scopeState"] == "held",
         true <- text?(response["fenceRevision"]) and timestamp?(response["preparedAt"]) do
      :ok
    else
      _ -> {:error, :provider_prepare_mismatch}
    end
  end

  defp validate_confirmation(files, recovery_id, expected, old_tuple_digest, proof_bytes, public_key, marker) do
    confirmation = files.confirmation
    proof = confirmation["proof"]
    prepared = files.prepare_response["data"]

    with true <- exact_keys?(confirmation, ~w(recoveryId proof signature)),
         true <- confirmation["recoveryId"] == recovery_id,
         true <- proof["recoveryId"] == recovery_id and proof["projectionId"] == expected["projectionId"],
         true <- proof["fenceRevision"] == prepared["fenceRevision"],
         true <- proof["oldTupleDigest"] == old_tuple_digest and proof["runnerId"] == expected["runnerId"],
         true <- proof["localGenerationMax"] == 2,
         true <- proof["fenceSHA256"] == marker["postimages"]["fence"]["sha256"],
         true <- proof["claimJournalSHA256"] == marker["postimages"]["claimJournal"]["sha256"],
         true <- proof["globalPause"] == true and proof["runnerStopped"] == true and proof["neverSpawned"] == true,
         true <- proof["supervisedWorkerAbsent"] == true and proof["workspaceAbsent"] == true and proof["processCount"] == 0,
         true <- proof["evidenceRef"] == "sha256:" <> digest(files.confirm_observation_bytes),
         {:ok, signature} <- Base.url_decode64(confirmation["signature"], padding: false),
         true <- byte_size(signature) == 64,
         true <- :crypto.verify(:eddsa, :none, proof_bytes, signature, [public_key, :ed25519]) do
      :ok
    else
      _ -> {:error, :provider_confirmation_mismatch}
    end
  end

  defp validate_release_receipt(receipt, marker) when is_map(receipt) do
    expected = marker["expected"]

    with true <- exact_keys?(receipt, @receipt_fields),
         true <- receipt["oldTupleDigest"] == ConfirmedRecoveryEvidence.tuple_digest(expected),
         true <- receipt["oldNonceHash"] == expected["nonceHash"],
         true <- receipt["projectionId"] == expected["projectionId"],
         true <- receipt["projectionState"] == "queued",
         true <- Enum.all?(~w(reservationState executionCapacityState scopeState), &(receipt[&1] == "released")),
         true <- receipt["nextGenerationFloor"] == 3,
         true <- Enum.all?(~w(recoveryId fenceRevision), &text?(receipt[&1])),
         true <- digest?(receipt["proofDigest"]),
         true <- timestamp?(receipt["confirmedAt"]) do
      :ok
    else
      _ -> {:error, :provider_final_proof_invalid}
    end
  end

  defp validate_release_receipt(_receipt, _marker), do: {:error, :provider_final_proof_invalid}

  defp exact_keys?(value, keys) when is_map(value), do: Enum.sort(Map.keys(value)) == Enum.sort(keys)
  defp exact_keys?(_value, _keys), do: false

  defp encode_ordered_object(value, fields) when is_map(value) do
    if exact_keys?(value, fields) do
      fields
      |> Enum.map(fn key -> {key, value[key]} end)
      |> encode_ordered_fields()
    else
      {:error, :invalid_ordered_object}
    end
  end

  defp encode_ordered_object(_value, _fields), do: {:error, :invalid_ordered_object}

  defp encode_ordered_fields(fields) do
    fields
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, parts} ->
      encoded =
        case value do
          {:raw, raw_json} when is_binary(raw_json) -> {:ok, raw_json}
          other -> Jason.encode(other)
        end

      case encoded do
        {:ok, json} -> {:cont, {:ok, [parts, Jason.encode!(key), ":", json, ","]}}
        _ -> {:halt, {:error, :invalid_ordered_value}}
      end
    end)
    |> case do
      {:ok, parts} -> {:ok, ["{", String.trim_trailing(IO.iodata_to_binary(parts), ","), "}"] |> IO.iodata_to_binary()}
      error -> error
    end
  end

  defp digest?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp text?(value), do: is_binary(value) and value != ""
  defp timestamp?(value) when is_binary(value), do: match?({:ok, _, _}, DateTime.from_iso8601(value))
  defp timestamp?(_value), do: false
end
