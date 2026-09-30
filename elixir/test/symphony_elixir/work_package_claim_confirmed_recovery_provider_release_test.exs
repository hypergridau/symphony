defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryProviderReleaseTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryProviderRelease, as: ProviderRelease

  test "canonical final payload and release fields bind the retained claim and next generation" do
    {marker, payload} = final_payload_fixture()
    bytes = ProviderRelease.canonical_payload(payload)

    assert is_binary(bytes)
    assert {:ok, ^payload} = Jason.decode(bytes)
    assert :ok = ProviderRelease.validate_final_payload(payload, marker, payload["journalSHA256"])

    refute :ok ==
             ProviderRelease.validate_final_payload(
               put_in(payload, ["receipt", "nextGenerationFloor"], 4),
               marker,
               payload["journalSHA256"]
             )

    refute :ok ==
             ProviderRelease.validate_final_payload(
               Map.put(payload, "neverSpawned", false),
               marker,
               payload["journalSHA256"]
             )

    refute :ok ==
             ProviderRelease.validate_final_payload(
               Map.put(payload, "extra", true),
               marker,
               payload["journalSHA256"]
             )
  end

  test "HGS719 operation validates exact prepare, signed confirmation, final receipt and provider envelope bytes" do
    {files, marker, payload, envelope_bytes, public_key} = operation_fixture()

    assert :ok = ProviderRelease.validate_operation(files, marker, payload, envelope_bytes, public_key)

    wrong_receipt = put_in(payload, ["receipt", "proofDigest"], sha256("other-proof"))
    refute :ok == ProviderRelease.validate_operation(files, marker, wrong_receipt, envelope_bytes, public_key)
    refute :ok == ProviderRelease.validate_operation(files, marker, payload, envelope_bytes <> " ", public_key)

    refute :ok ==
             ProviderRelease.validate_operation(
               put_in(files, [:prepare_request, "recoveryId"], "other"),
               marker,
               payload,
               envelope_bytes,
               public_key
             )

    refute :ok == ProviderRelease.validate_operation(files, marker, payload, envelope_bytes, <<0::256>>)
  end

  test "malformed final receipts and operation envelopes remain held closed" do
    {marker, payload} = final_payload_fixture()
    assert nil == ProviderRelease.canonical_payload(nil)
    assert {:error, :provider_final_proof_invalid} = ProviderRelease.validate_final_payload(nil, marker, "hash")

    assert {:error, :provider_final_proof_invalid} =
             ProviderRelease.validate_final_payload(payload, marker, payload["journalSHA256"] <> "x")

    assert {:error, :provider_final_proof_invalid} =
             ProviderRelease.validate_final_payload(Map.put(payload, "receipt", nil), marker, payload["journalSHA256"])

    invalid_timestamp = put_in(payload, ["receipt", "confirmedAt"], nil)

    assert {:error, :provider_final_proof_invalid} =
             ProviderRelease.validate_final_payload(invalid_timestamp, marker, payload["journalSHA256"])

    assert {:error, :provider_confirmation_receipt_mismatch} =
             ProviderRelease.validate_receipt_binding(nil, %{}, "id", %{}, "digest", "revision", "proof")

    assert {:error, :provider_final_operation_mismatch} =
             ProviderRelease.validate_operation(nil, marker, payload, "", <<0::256>>)

    assert {:error, :provider_proof_invalid} = ProviderRelease.canonical_proof(nil)
    assert {:error, :provider_proof_invalid} = ProviderRelease.canonical_proof(%{"extra" => true})
  end

  defp final_payload_fixture do
    expected = expected_claim()
    receipt = release_receipt(expected, "hgs719-midgard-#{ConfirmedRecoveryEvidence.tuple_digest(expected)}")

    marker = %{
      "pool" => "midgard",
      "expected" => expected,
      "postimages" => %{
        "fence" => %{"sha256" => sha256("fence-post")},
        "claimJournal" => %{"sha256" => sha256("journal-post")}
      }
    }

    payload = %{
      "contractVersion" => "work-package-pre-spawn-recovery.v1",
      "expected" => expected,
      "receipt" => receipt,
      "localGenerationMax" => 2,
      "journalSHA256" => sha256("journal-post"),
      "neverSpawned" => true
    }

    {marker, payload}
  end

  defp operation_fixture do
    {marker, payload} = final_payload_fixture()
    expected = marker["expected"]
    old_tuple_digest = ConfirmedRecoveryEvidence.tuple_digest(expected)
    recovery_id = "hgs719-#{marker["pool"]}-#{old_tuple_digest}"
    fence_revision = "revision-12"
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519, :binary.copy(<<7>>, 32))
    prepare_observation = %{"expected" => expected, "localGenerationMax" => 2, "observedAt" => "2026-09-30T12:00:00Z"}

    confirm_observation = %{
      "expected" => expected,
      "localGenerationMax" => 2,
      "claimJournalSHA256" => payload["journalSHA256"],
      "observedAt" => "2026-09-30T12:00:05Z"
    }

    prepare_observation_bytes = Jason.encode!(prepare_observation)
    confirm_observation_bytes = Jason.encode!(confirm_observation)

    proof = %{
      "contractVersion" => "work-package-pre-spawn-recovery.v1",
      "recoveryId" => recovery_id,
      "projectionId" => expected["projectionId"],
      "fenceRevision" => fence_revision,
      "oldTupleDigest" => old_tuple_digest,
      "runnerId" => expected["runnerId"],
      "hostIdentity" => "runner-host",
      "bootId" => "boot-id",
      "observedAt" => "2026-09-30T12:00:05Z",
      "evidenceRef" => "sha256:" <> sha256(confirm_observation_bytes),
      "globalPause" => true,
      "runnerStopped" => true,
      "neverSpawned" => true,
      "supervisedWorkerAbsent" => true,
      "processCount" => 0,
      "workspaceAbsent" => true,
      "localGenerationMax" => 2,
      "fenceSHA256" => marker["postimages"]["fence"]["sha256"],
      "claimJournalSHA256" => marker["postimages"]["claimJournal"]["sha256"]
    }

    {:ok, proof_bytes} = ProviderRelease.canonical_proof(proof)
    signature = :crypto.sign(:eddsa, :none, proof_bytes, [private_key, :ed25519]) |> Base.url_encode64(padding: false)
    confirmation = %{"recoveryId" => recovery_id, "proof" => proof, "signature" => signature}
    receipt = Map.put(payload["receipt"], "proofDigest", sha256(proof_bytes))

    prepare_response = %{
      "recoveryId" => recovery_id,
      "projectionId" => expected["projectionId"],
      "fenceRevision" => fence_revision,
      "oldTupleDigest" => old_tuple_digest,
      "preparedAt" => "2026-09-30T12:00:01Z",
      "state" => "prepared",
      "reservationState" => "claimed",
      "executionCapacityState" => "held",
      "scopeState" => "held"
    }

    files = %{
      prepare_observation: prepare_observation,
      prepare_observation_bytes: prepare_observation_bytes,
      prepare_request: %{
        "recoveryId" => recovery_id,
        "expected" => expected,
        "reason" => "confirmed allocation failed before Job creation",
        "evidenceRef" => "sha256:" <> sha256(prepare_observation_bytes)
      },
      prepare_response: %{"data" => prepare_response},
      confirm_observation: confirm_observation,
      confirm_observation_bytes: confirm_observation_bytes,
      confirmation: confirmation,
      confirm_response: %{"data" => receipt},
      signed_envelope_bytes: envelope_bytes = "{\"payload\":\"signed\"}"
    }

    {files, marker, %{payload | "receipt" => receipt}, envelope_bytes, public_key}
  end

  defp release_receipt(expected, recovery_id) do
    %{
      "recoveryId" => recovery_id,
      "projectionId" => expected["projectionId"],
      "fenceRevision" => "revision-12",
      "oldTupleDigest" => ConfirmedRecoveryEvidence.tuple_digest(expected),
      "oldNonceHash" => expected["nonceHash"],
      "nextGenerationFloor" => 3,
      "confirmedAt" => "2026-09-30T12:00:10Z",
      "proofDigest" => sha256("proof"),
      "projectionState" => "queued",
      "reservationState" => "released",
      "executionCapacityState" => "released",
      "scopeState" => "released"
    }
  end

  defp expected_claim do
    %{
      "projectionId" => "projection",
      "reservationId" => "reservation",
      "workspaceId" => "workspace",
      "companyId" => "company",
      "issueId" => "f77e349e-21d9-4bdf-bad3-ce08b302e7e8",
      "runnerId" => "runner",
      "managedProjectProfileId" => "profile",
      "repositoryRef" => "hypergridau/symphony",
      "scopeKeys" => ["repo:hypergridau/symphony"],
      "generation" => 2,
      "sessionId" => "session",
      "processId" => "process",
      "responsibleDelegationId" => "delegation",
      "executionFenceToken" => "f77e349e-21d9-4bdf-bad3-ce08b302e7e8:2",
      "runtimeLeaseId" => "session",
      "nonceHash" => String.duplicate("a", 64)
    }
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
