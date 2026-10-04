defmodule SymphonyElixir.Test.ConfirmedReleaseFixture do
  @moduledoc false
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryProviderRelease, as: Provider
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseProtocol, as: Protocol

  @artifacts ~w(index.js bootstrap/provider-core-app.js work-package-projection/hgs740-release.contract.js work-package-projection/hgs740-release.service.js work-package-projection/hgs740-release.persistence.js work-package-projection/hgs742-approval.contract.js work-package-projection/hgs742-approval.service.js work-package-projection/hgs742-admission-files.js work-package-projection/hgs742-accepted-runtime.js routes/provider/register-provider-routes.js routes/provider/hgs740-release.routes.js routes/experience/claim-approvals.routes.js routes/experience/approvals.routes.js routes/experience/claim-review-page.routes.js routes/experience/claim-review-page.origin.js experience-adapters/security/experience-security-headers.js routes/experience/register-experience-routes.js)
  @spki <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>>
  @issue "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"

  def fixture do
    {public, private} = :crypto.generate_key(:eddsa, :ed25519)
    now = DateTime.to_unix(~U[2026-10-04 08:00:00Z], :millisecond)

    expected = %{
      "issueId" => @issue,
      "projectionId" => "projection",
      "reservationId" => "reservation",
      "workspaceId" => "workspace",
      "companyId" => "company",
      "runnerId" => "runner",
      "managedProjectProfileId" => "profile",
      "repositoryRef" => "hypergridau/hypergrid-gitops",
      "scopeKeys" => ["hypergridau/hypergrid-gitops"],
      "generation" => 2,
      "sessionId" => "session",
      "processId" => "process",
      "responsibleDelegationId" => "delegation",
      "executionFenceToken" => @issue <> ":2",
      "runtimeLeaseId" => "session",
      "nonceHash" => hash("nonce")
    }

    images = Map.new(~w(claimJournal fence responsibilityGraph), &{&1, "synthetic-" <> &1})
    nonce = "11111111-2222-4333-8444-555555555555"
    manifest = %{"epoch" => "epoch-5", "observedAt" => iso(now - 86_400_000)}
    observation = %{"observedAt" => manifest["observedAt"], "reconciliation" => manifest}
    proof_bytes = "synthetic frozen historical proof"
    hashes = Map.new(images, fn {name, bytes} -> {name <> "SHA256", hash(bytes)} end)
    candidate = %{"expected" => expected, "nonce" => nonce, "completedAt" => iso(now - 86_000_000), "postimages" => hashes}

    marker = %{
      "contractVersion" => "work-package-hgs740-local-transition.v3",
      "status" => "local_applied",
      "pool" => "hypergrid-gitops",
      "issueId" => @issue,
      "generation" => 2,
      "nonce" => nonce,
      "assignmentSnapshotState" => "absent",
      "expected" => expected,
      "postimages" => Map.new(images, fn {name, bytes} -> {name, %{"sha256" => hash(bytes), "bytes" => Base.url_encode64(bytes, padding: false)}} end),
      "proofSHA256" => hash(proof_bytes),
      "observationSHA256" => hash(canonical(observation))
    }

    snapshot = %{
      marker_bytes: canonical(marker),
      candidate_bytes: canonical(candidate),
      observation_bytes: canonical(observation),
      proof_bytes: proof_bytes,
      manifest_bytes: canonical(manifest),
      committed_images: images
    }

    {:ok, binding} = Protocol.binding(snapshot, %{"dahlia" => String.duplicate("a", 40), "symphony" => String.duplicate("b", 40)})
    local = local(binding, now)
    {:ok, auth} = Protocol.authorize(local, binding, local["approverRef"], now)
    auth_raw = sign(auth, "hgs740-release-authorization.v1", private)

    attestation = %{
      "contractVersion" => "hgs740-release-attestation.v1",
      "authorizationSHA256" => hash(auth_raw),
      "binding" => binding,
      "observedAt" => iso(now),
      "expiresAt" => iso(now + 60_000),
      "checks" => %{
        "globalPause" => true,
        "unitsMaskedAndQuiescent" => true,
        "nativeCustodyVerified" => true,
        "localPostimagesVerified" => true,
        "workerCount" => 0,
        "jobCount" => 0,
        "podCount" => 0,
        "credentialLeaseCount" => 0,
        "oauthSlotLeaseCount" => 0,
        "completeInventories" => true,
        "providerState" => "claimed_held"
      },
      "readbacks" => Map.new(~w(hostReadbackSHA256 providerReadbackSHA256 jobsReadbackSHA256 podsReadbackSHA256 nativeCustodySHA256), &{&1, hash(&1)})
    }

    bundle = %{
      "binding" => binding,
      "authorization" => auth_raw,
      "attestation" => sign(attestation, "hgs740-release-attestation.v1", private),
      "receipt" => sign(candidate, "hgs740-local-transition-receipt.v3", private)
    }

    data = provider_evidence(bundle, public, local, now)
    Map.merge(data, %{bundle: bundle, snapshot: snapshot, marker: marker, public: public, private: private, now: now, local: local})
  end

  def provider_evidence(bundle, public, local, now) do
    binding = bundle["binding"]
    expected = binding["expected"]
    attestation = decode(bundle["attestation"])
    fingerprint = hash(Base.encode64(@spki <> public))
    provider = local |> Map.put("approvalId", "provider-approval") |> Map.put("targetAction", "hgs740_release_provider")
    provider = put_in(provider["decisionPayload"]["allowedActions"], ~w(prepare_provider_release confirm_provider_release))
    provider = put_in(provider["decisionPayload"]["nativeDecisionId"], local["approvalId"])
    recovery_id = "hgs740-release:" <> local["decisionPayload"]["attemptId"]
    evidence_ref = "sha256:" <> hash(canonical(%{"bundle" => bundle, "providerDecisionSHA256" => hash(canonical(provider))}))

    proof = %{
      "contractVersion" => "work-package-host-quiescence.v1",
      "recoveryId" => recovery_id,
      "projectionId" => expected["projectionId"],
      "fenceRevision" => "confirmed-fence",
      "oldTupleDigest" => Evidence.tuple_digest(expected),
      "runnerId" => expected["runnerId"],
      "hostIdentity" => "native-attestor:" <> fingerprint,
      "bootId" => "attested-host-readback:" <> attestation["readbacks"]["hostReadbackSHA256"],
      "observedAt" => attestation["observedAt"],
      "evidenceRef" => evidence_ref,
      "globalPause" => true,
      "runnerStopped" => true,
      "neverSpawned" => true,
      "supervisedWorkerAbsent" => true,
      "processCount" => 0,
      "workspaceAbsent" => true,
      "localGenerationMax" => 2,
      "fenceSHA256" => binding["postimages"]["fenceSHA256"],
      "claimJournalSHA256" => binding["postimages"]["claimJournalSHA256"]
    }

    {:ok, proof_bytes} = Provider.canonical_proof(proof)

    receipt = %{
      "recoveryId" => recovery_id,
      "projectionId" => expected["projectionId"],
      "fenceRevision" => "confirmed-fence",
      "oldTupleDigest" => Evidence.tuple_digest(expected),
      "oldNonceHash" => expected["nonceHash"],
      "nextGenerationFloor" => 3,
      "confirmedAt" => iso(now + 500),
      "proofDigest" => hash(proof_bytes),
      "projectionState" => "queued",
      "reservationState" => "released",
      "executionCapacityState" => "released",
      "scopeState" => "released"
    }

    review = %{
      "contractVersion" => "hgs740-installed-admission-review.v1",
      "reviewedAt" => iso(now - 60_000),
      "sourceHeads" => binding["sourceHeads"],
      "bindingSHA256" => hash(canonical(binding)),
      "nativeAcceptedBuildReceiptSHA256" => hash("synthetic accepted build"),
      "providerArtifacts" => Map.new(@artifacts, &{&1, hash(&1)})
    }

    review_bytes = canonical(review)

    enrollment = %{
      "contractVersion" => "hgs740-accepted-owner-enrollment.v1",
      "acceptedProtocol" => "hgs740-release-only.v1",
      "acceptedAt" => iso(now - 60_000),
      "expiresAt" => iso(now + 3_600_000),
      "installedReviewSHA256" => hash(review_bytes),
      "ownerUserId" => "synthetic-user",
      "ownerPrincipal" => local["approverRef"],
      "nativeFingerprint" => fingerprint,
      "publicKey" => :public_key.pem_encode([{:SubjectPublicKeyInfo, @spki <> public, :not_encrypted}]),
      "binding" => binding
    }

    enrollment_bytes = canonical(enrollment)

    readback = %{
      "contractVersion" => "hgs740-confirmed-release-readback.v1",
      "sourceIdentity" => "provider-core:postgres",
      "observedAt" => iso(now + 1_000),
      "binding" => binding,
      "bundleSHA256" => hash(canonical(bundle)),
      "installedReviewSHA256" => hash(review_bytes),
      "acceptedEnrollmentSHA256" => hash(enrollment_bytes),
      "localDecision" => local,
      "providerDecision" => provider,
      "confirmation" => %{
        "proof" => %{"recoveryId" => recovery_id, "proof" => proof, "signature" => Jason.decode!(bundle["attestation"])["signature"]},
        "proofDigest" => hash(proof_bytes),
        "publicKeyFingerprint" => fingerprint,
        "nextGenerationFloor" => 3,
        "receipt" => receipt,
        "confirmedAt" => receipt["confirmedAt"]
      },
      "currentState" => %{
        "projectionState" => "queued",
        "desiredState" => "queued",
        "claimEvidence" => nil,
        "reservationState" => "released",
        "executionCapacityState" => "released",
        "scopeState" => "released",
        "generation" => "2",
        "nonceHash" => expected["nonceHash"]
      }
    }

    %{readback: readback, history: {review_bytes, enrollment_bytes}, provider: provider, fingerprint: fingerprint}
  end

  def sign(payload, domain, private) do
    bytes = canonical(payload)
    signature = :crypto.sign(:eddsa, :none, "hypergrid-work-package-recovery:" <> domain <> <<0>> <> bytes, [private, :ed25519])
    canonical(%{"payload" => Base.url_encode64(bytes, padding: false), "signature" => Base.url_encode64(signature, padding: false)})
  end

  def decode(raw), do: raw |> Jason.decode!() |> Map.fetch!("payload") |> Base.url_decode64!(padding: false) |> Jason.decode!()
  def canonical(v), do: Evidence.canonical_json(v)
  def hash(v), do: :crypto.hash(:sha256, v) |> Base.encode16(case: :lower)
  def iso(ms), do: ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()

  defp local(binding, now) do
    %{
      "approvalId" => "local-approval",
      "workspaceId" => binding["expected"]["workspaceId"],
      "companyId" => binding["expected"]["companyId"],
      "approvalState" => "approved",
      "targetAction" => "hgs740_release_local",
      "decisionMode" => "human",
      "approverType" => "user",
      "approverRef" => "synthetic@owner.test",
      "decisionActorType" => "user",
      "decisionActorRef" => "synthetic@owner.test",
      "supersededByApprovalId" => nil,
      "decidedAt" => iso(now - 60_000),
      "expiresAt" => iso(now + 600_000),
      "decisionPayload" => %{
        "protocolVersion" => "hgs740-release-only.v1",
        "bindingSHA256" => hash(canonical(binding)),
        "sourceHeads" => binding["sourceHeads"],
        "attemptId" => "11111111-2222-4333-8444-555555555555",
        "allowedActions" => ~w(collect_release_attestation issue_local_transition_receipt),
        "nativeDecisionId" => nil
      }
    }
  end
end
