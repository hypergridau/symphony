defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionProof do
  @moduledoc "Checks the committed provider projection; it never treats it as a legacy signed proof."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryProviderRelease, as: Provider

  @readback_fields ~w(contractVersion sourceIdentity observedAt binding bundleSHA256 installedReviewSHA256 acceptedEnrollmentSHA256 localDecision providerDecision confirmation currentState)
  @confirmation_fields ~w(proof proofDigest publicKeyFingerprint nextGenerationFloor receipt confirmedAt)
  @receipt_fields ~w(recoveryId projectionId fenceRevision oldTupleDigest oldNonceHash nextGenerationFloor confirmedAt proofDigest projectionState reservationState executionCapacityState scopeState)
  @current_fields ~w(projectionState desiredState claimEvidence reservationState executionCapacityState scopeState generation nonceHash)

  @spec verify(map(), map(), map(), String.t()) :: {:ok, map()} | {:error, atom()}
  def verify(readback, bundle, signed, fingerprint) do
    binding = bundle["binding"]
    expected = binding["expected"]
    confirmation = readback["confirmation"]
    receipt = confirmation["receipt"]
    recovery_id = "hgs740-release:" <> signed.authorization["attemptId"]
    evidence_ref = evidence_ref(bundle, readback["providerDecision"])

    with true <- exact?(readback, @readback_fields),
         true <- readback["contractVersion"] == "hgs740-confirmed-release-readback.v1",
         true <- readback["sourceIdentity"] == "provider-core:postgres",
         true <- readback["binding"] == binding and readback["bundleSHA256"] == hash(Evidence.canonical_json(bundle)),
         true <- Enum.all?(~w(installedReviewSHA256 acceptedEnrollmentSHA256), &digest?(readback[&1])),
         true <- exact?(confirmation, @confirmation_fields) and exact?(receipt, @receipt_fields),
         true <- confirmation["confirmedAt"] == receipt["confirmedAt"],
         true <- confirmation["publicKeyFingerprint"] == fingerprint and confirmation["nextGenerationFloor"] === 3,
         true <- released_receipt?(receipt, recovery_id, expected),
         proof <- consumption_proof(binding, signed.attestation, receipt, evidence_ref, fingerprint),
         {:ok, proof_bytes} <- Provider.canonical_proof(proof),
         true <- receipt["proofDigest"] == hash(proof_bytes) and confirmation["proofDigest"] == hash(proof_bytes),
         {:ok, envelope} <- Jason.decode(bundle["attestation"]),
         true <- confirmation["proof"] == %{"recoveryId" => recovery_id, "proof" => proof, "signature" => envelope["signature"]},
         true <- released_current?(readback["currentState"], expected),
         {:ok, observed} <- time(readback["observedAt"]),
         {:ok, confirmed} <- time(receipt["confirmedAt"]),
         true <- confirmed <= observed do
      {:ok,
       %{
         "contractVersion" => "hgs740-release-completion.v1",
         "expected" => expected,
         "receipt" => receipt,
         "localGenerationMax" => 2,
         "journalSHA256" => binding["postimages"]["claimJournalSHA256"],
         "neverSpawned" => true
       }}
    else
      _ -> {:error, :hgs740_confirmed_release_invalid}
    end
  rescue
    _ -> {:error, :hgs740_confirmed_release_invalid}
  end

  @spec fresh?(map(), integer()) :: boolean()
  def fresh?(readback, now) do
    with {:ok, observed} <- time(readback["observedAt"]), do: observed <= now + 5_000 and now - observed <= 60_000
  end

  defp consumption_proof(b, a, r, evidence_ref, fingerprint) do
    %{
      "contractVersion" => "work-package-host-quiescence.v1",
      "recoveryId" => r["recoveryId"],
      "projectionId" => b["expected"]["projectionId"],
      "fenceRevision" => r["fenceRevision"],
      "oldTupleDigest" => Evidence.tuple_digest(b["expected"]),
      "runnerId" => b["expected"]["runnerId"],
      "hostIdentity" => "native-attestor:" <> fingerprint,
      "bootId" => "attested-host-readback:" <> a["readbacks"]["hostReadbackSHA256"],
      "observedAt" => a["observedAt"],
      "evidenceRef" => evidence_ref,
      "globalPause" => true,
      "runnerStopped" => true,
      "neverSpawned" => true,
      "supervisedWorkerAbsent" => true,
      "processCount" => 0,
      "workspaceAbsent" => true,
      "localGenerationMax" => 2,
      "fenceSHA256" => b["postimages"]["fenceSHA256"],
      "claimJournalSHA256" => b["postimages"]["claimJournalSHA256"]
    }
  end

  defp evidence_ref(bundle, provider) do
    "sha256:" <> hash(Evidence.canonical_json(%{"bundle" => bundle, "providerDecisionSHA256" => hash(Evidence.canonical_json(provider))}))
  end

  defp released_receipt?(r, id, e) do
    r["recoveryId"] == id and r["projectionId"] == e["projectionId"] and
      r["oldTupleDigest"] == Evidence.tuple_digest(e) and r["oldNonceHash"] == e["nonceHash"] and
      r["nextGenerationFloor"] === 3 and r["projectionState"] == "queued" and text?(r["fenceRevision"]) and
      Enum.all?(~w(reservationState executionCapacityState scopeState), &(r[&1] == "released"))
  end

  defp released_current?(s, e) do
    exact?(s, @current_fields) and s["projectionState"] == "queued" and s["desiredState"] == "queued" and
      is_nil(s["claimEvidence"]) and s["generation"] in [2, "2"] and s["nonceHash"] == e["nonceHash"] and
      Enum.all?(~w(reservationState executionCapacityState scopeState), &(s[&1] == "released"))
  end

  defp exact?(v, fields), do: is_map(v) and Enum.sort(Map.keys(v)) == Enum.sort(fields)
  defp text?(v), do: is_binary(v) and byte_size(v) in 1..256
  defp digest?(v), do: is_binary(v) and Regex.match?(~r/\A[0-9a-f]{64}\z/, v)
  defp hash(v), do: :crypto.hash(:sha256, v) |> Base.encode16(case: :lower)

  defp time(v) when is_binary(v) do
    with {:ok, dt, 0} <- DateTime.from_iso8601(v), do: {:ok, DateTime.to_unix(dt, :millisecond)}
  end

  defp time(_), do: :error
end
