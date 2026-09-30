defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidenceTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence

  @now_ms 1_790_762_400_000
  @issue "24e34a86-b214-41bc-8a35-9e1d31bfb8e4"
  @profile "profile-1"
  @repository "hypergrid.au/symphony"
  @reservation "reservation-gen2"
  @assignment_sha String.duplicate("a", 64)
  @nonce "11111111-2222-4333-8444-555555555501"
  @fence_sha String.duplicate("c", 64)
  @journal_sha String.duplicate("d", 64)
  @graph_sha String.duplicate("9", 64)
  @pool "midgard"
  @pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)

  test "accepts a structurally complete confirmed-phase proof with an exact held provider readback" do
    payload = payload()
    assert :ok = Evidence.validate_payload(payload, bindings())
  end

  test "rejects an incomplete or positive claim-bound Kubernetes snapshot" do
    payload = payload()
    refute_valid(put_in(payload, ["observation", "kubernetes", "jobs", "complete"], false))
    refute_valid(put_in(payload, ["observation", "kubernetes", "pods", "claimAbsent"], false))
    refute_valid(put_in(payload, ["observation", "kubernetes", "jobs", "itemCount"], -1))
  end

  test "rejects a changed provider tuple, non-held state, or stale provider readback" do
    payload = payload()
    refute_valid(put_in(payload, ["providerHeld", "expected", "processId"], "foreign-process"))
    refute_valid(put_in(payload, ["providerHeld", "executionCapacityState"], "released"))
    refute_valid(put_in(payload, ["providerHeld", "observedAt"], "2026-09-30T09:00:00Z"))
  end

  test "requires complete empty credential and OAuth lease inventories from the same provider snapshot" do
    payload = payload()

    refute_valid(put_in(payload, ["providerHeld", "credentialLeaseInventory", "leaseIds"], ["credential-1"]))

    refute_valid(put_in(payload, ["providerHeld", "credentialLeaseInventory", "readbacks"], [%{"state" => "released"}]))

    refute_valid(put_in(payload, ["providerHeld", "oauthSlotLeaseInventory", "leaseCount"], 1))

    refute_valid(put_in(payload, ["providerHeld", "oauthSlotLeaseInventory", "leases"], [%{"state" => "released"}]))

    refute_valid(put_in(payload, ["providerHeld", "credentialLeaseInventory", "observedAt"], "2026-09-30T09:59:41Z"))
  end

  test "rejects wrong dispatch phase, incomplete pool history, and a non-retired predecessor" do
    payload = payload()
    refute_valid(put_in(payload, ["observation", "dispatchPhase"], "recovery_pending"))
    refute_valid(put_in(payload, ["observation", "witnessLogSHA256"], %{"midgard" => String.duplicate("b", 64)}))
    refute_valid(put_in(payload, ["observation", "predecessorRetirement", "receipt", "successor_responsible_id"], "other-delegation"))
    refute_valid(put_in(payload, ["observation", "serviceUnits", "asgard", "masked"], false))
    refute_valid(put_in(payload, ["observation", "turnsAbsent"], false))
  end

  test "rejects evidence outside its signed freshness window and a changed assignment digest" do
    payload = payload()
    refute_valid(%{payload | "expiresAt" => "2026-09-30T09:59:59Z"})
    refute_valid(%{payload | "assignmentSHA256" => String.duplicate("c", 64)})
  end

  test "requires the expected nonce and exact journal, fence, and graph preimages" do
    payload = payload()
    refute_valid(put_in(payload, ["nonce"], "11111111-2222-4333-8444-555555555502"))
    refute_valid(put_in(payload, ["observation", "fenceSHA256"], String.duplicate("f", 64)))
    refute_valid(put_in(payload, ["observation", "claimJournalSHA256"], String.duplicate("f", 64)))
    refute_valid(put_in(payload, ["observation", "responsibilityGraphSHA256"], String.duplicate("f", 64)))
  end

  test "rejects unknown signing keys and invalid detached envelopes" do
    {public, _private} = :crypto.generate_key(:eddsa, :ed25519)
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.verify("{}", public, bindings())
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.verify("not-json", <<0::256>>, bindings())
  end

  test "provider tuple digest uses the HGS485 field order and sorted scope keys" do
    claim = claim()
    assert Evidence.tuple_digest(claim) == Evidence.tuple_digest(%{claim | "scopeKeys" => Enum.reverse(claim["scopeKeys"])})
    assert Evidence.tuple_digest(claim) == "98786bf424799b5c00d56df82356d2d0dc56d7d9e35e0e754b7ee67e75959f7d"
  end

  test "retirement receipt evidence uses the producer's deterministic ETF tuple digest" do
    receipt = predecessor(claim())["receipt"]
    assert Evidence.retirement_evidence_ref(receipt) == "28c40b36c7a008a8eb0cbe39d91f576f4968815d0085be9d3009cd2de8681289"
  end

  test "canonical JSON and a detached Ed25519 envelope round-trip with an ephemeral key" do
    assert Evidence.canonical_json(%{"z" => 1, "a" => "é"}) == "{\"a\":\"é\",\"z\":1}"

    seed = :binary.list_to_bin(Enum.to_list(0..31))
    {public, private} = :crypto.generate_key(:eddsa, :ed25519, seed)
    payload_bytes = Evidence.canonical_json(payload())
    signature = :crypto.sign(:eddsa, :none, Evidence.signature_message(payload_bytes), [private, :ed25519])

    envelope =
      Evidence.canonical_json(%{
        "payload" => Base.url_encode64(payload_bytes, padding: false),
        "signature" => Base.url_encode64(signature, padding: false)
      })

    assert {:ok, _payload} = Evidence.verify_test_envelope(envelope, public, bindings())
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.verify(envelope, public, bindings())
  end

  test "malformed signed-input types and claim-bound nested fields fail closed" do
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.verify(nil, <<0::256>>, bindings())
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.verify("{}", "short", bindings())
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.verify_test_envelope(nil, <<0::256>>, bindings())
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.verify_test_envelope("{}", <<0::256>>, bindings())
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.validate_payload(nil, bindings())
    assert nil == Evidence.tuple_digest(nil)
    assert nil == Evidence.retirement_evidence_ref(nil)

    payload = payload()
    refute_valid(Map.put(payload, "issuedAt", nil))
    refute_valid(Map.put(payload, "issuedAt", "2026-09-30T11:59:50+02:00"))
    refute_valid(put_in(payload, ["observation", "expected", "scopeKeys"], nil))
    refute_valid(put_in(payload, ["observation", "kubernetes", "cluster", "apiServer"], nil))
    refute_valid(put_in(payload, ["observation", "predecessorRetirement", "receipt"], nil))
    refute_valid(put_in(payload, ["observation", "witnesses", Access.at(0), "source", "sourceHead"], "bad"))
  end

  test "predecessor retirement and claim digests reject missing custody and unrepresentable fields" do
    payload = payload()
    refute_valid(put_in(payload, ["observation", "predecessorRetirement"], nil))
    refute_valid(put_in(payload, ["observation", "predecessorRetirement", "execution", "leases"], %{}))
    refute_valid(put_in(payload, ["observation", "kubernetes", "cluster", "apiServer"], "http://insecure.invalid"))

    assert nil == Evidence.tuple_digest(Map.put(claim(), "projectionId", self()))
  end

  defp refute_valid(candidate), do: assert({:error, :invalid_confirmed_recovery_evidence} == Evidence.validate_payload(candidate, bindings()))

  defp bindings do
    %{
      pool: @pool,
      issue_id: @issue,
      generation: 2,
      reservation_id: @reservation,
      assignment_sha256: @assignment_sha,
      nonce: @nonce,
      fence_sha256: @fence_sha,
      claim_journal_sha256: @journal_sha,
      responsibility_graph_sha256: @graph_sha,
      now_ms: @now_ms
    }
  end

  defp payload do
    observed = "2026-09-30T09:59:30Z"
    claim = claim()

    %{
      "contractVersion" => "work-package-paused-confirmed-recovery.v1",
      "pool" => @pool,
      "issueId" => @issue,
      "generation" => 2,
      "reservationId" => @reservation,
      "assignmentSHA256" => @assignment_sha,
      "issuedAt" => "2026-09-30T09:59:50Z",
      "expiresAt" => "2026-09-30T10:00:30Z",
      "nonce" => @nonce,
      "observation" => observation(claim, observed),
      "providerHeld" => provider_readback(claim, "2026-09-30T09:59:40Z")
    }
  end

  defp claim do
    %{
      "projectionId" => "projection-gen2",
      "reservationId" => @reservation,
      "workspaceId" => "workspace-1",
      "companyId" => "company-1",
      "issueId" => @issue,
      "runnerId" => "runner-1",
      "managedProjectProfileId" => @profile,
      "repositoryRef" => @repository,
      "scopeKeys" => ["issue:" <> @issue, "repo:symphony"],
      "generation" => 2,
      "sessionId" => "worker:#{@issue}:2",
      "processId" => "worker:#{@issue}:2",
      "responsibleDelegationId" => "responsible-gen2",
      "executionFenceToken" => "#{@issue}:2",
      "runtimeLeaseId" => "worker:#{@issue}:2",
      "nonceHash" => String.duplicate("b", 64)
    }
  end

  defp observation(claim, observed) do
    %{
      "expected" => claim,
      "localGenerationMax" => 2,
      "fenceSHA256" => @fence_sha,
      "claimJournalSHA256" => @journal_sha,
      "responsibilityGraphSHA256" => @graph_sha,
      "globalPause" => true,
      "runnerStopped" => true,
      "neverSpawned" => true,
      "supervisedWorkerAbsent" => true,
      "processCount" => 0,
      "workspaceAbsent" => true,
      "hostIdentity" => "host-1",
      "bootId" => "boot-1",
      "observedAt" => observed,
      "witnesses" => [witness(1), witness(2)],
      "witnessLogSHA256" => Map.new(@pools, &{&1, String.duplicate("e", 64)}),
      "dispatchPhase" => "confirmed",
      "kubernetes" => kubernetes(observed),
      "predecessorRetirement" => predecessor(claim),
      "serviceUnits" => service_units(),
      "witnessUnits" => %{
        "dahlia-claim-witness.service" => %{"activeState" => "inactive", "masked" => true},
        "dahlia-claim-witness.socket" => %{"activeState" => "inactive", "masked" => true}
      },
      "turnsAbsent" => true
    }
  end

  defp kubernetes(observed) do
    %{
      "observedAt" => observed,
      "cluster" => %{"apiServer" => "https://10.0.0.1:6443", "caSha256" => String.duplicate("f", 64)},
      "namespace" => "frigga",
      "claim" => %{"issueId" => @issue, "generation" => 2, "repositoryRef" => @repository, "reservationId" => @reservation, "assignmentSHA256" => @assignment_sha},
      "jobs" => snapshot(),
      "pods" => snapshot()
    }
  end

  defp snapshot do
    %{"resourceVersion" => "12345", "sha256" => String.duplicate("a", 64), "complete" => true, "itemCount" => 4, "claimAbsent" => true}
  end

  defp witness(generation) do
    %{
      "generation" => generation,
      "sequence" => generation,
      "hash" => String.duplicate("a", 64),
      "source" => %{
        "sourceHead" => String.duplicate("a", 40),
        "executableSHA256" => String.duplicate("1", 64),
        "wrapperSHA256" => String.duplicate("2", 64),
        "attestationSHA256" => String.duplicate("3", 64)
      },
      "acceptedBuildReceiptSHA256" => String.duplicate("4", 64)
    }
  end

  defp service_units do
    Map.new(@pools, fn pool ->
      {pool, %{"unit" => "dahlia-symphony@#{pool}.service", "mainPID" => 0, "activeState" => "inactive", "masked" => true, "cgroupProcessCount" => 0}}
    end)
  end

  defp provider_readback(claim, observed) do
    %{
      "observedAt" => observed,
      "sourceIdentity" => "provider-core:postgres",
      "assignmentDigest" => Evidence.tuple_digest(claim),
      "expected" => claim,
      "projectionState" => "active",
      "mutationState" => "applied",
      "reservationState" => "claimed",
      "executionCapacityState" => "held",
      "scopeState" => "held",
      "credentialLeaseInventory" => %{
        "observedAt" => observed,
        "complete" => true,
        "leaseIds" => [],
        "readbacks" => []
      },
      "oauthSlotLeaseInventory" => %{
        "observedAt" => observed,
        "complete" => true,
        "leaseCount" => 0,
        "leaseIds" => [],
        "leases" => []
      }
    }
  end

  defp predecessor(current_claim) do
    old_claim =
      Map.merge(claim(), %{
        "projectionId" => "projection-gen1",
        "reservationId" => "reservation-gen1",
        "generation" => 1,
        "sessionId" => "worker:#{@issue}:1",
        "processId" => "worker:#{@issue}:1",
        "responsibleDelegationId" => "responsible-gen1",
        "executionFenceToken" => "#{@issue}:1",
        "runtimeLeaseId" => "worker:#{@issue}:1"
      })

    receipt = %{
      "active_process" => "absent",
      "evidence_ref" => "28c40b36c7a008a8eb0cbe39d91f576f4968815d0085be9d3009cd2de8681289",
      "generation" => 1,
      "issue_id" => @issue,
      "linear_state" => "In Progress",
      "local_claim" => "absent",
      "provider_claim" => "absent",
      "provider_projection_id" => "projection-gen1",
      "retired_at_ms" => @now_ms - 100_000,
      "workspace" => "absent",
      "type" => "unsubmitted_successor",
      "repository_ref" => @repository,
      "managed_project_profile_id" => @profile,
      "prior_accountable_id" => "accountable-gen1",
      "prior_responsible_id" => "responsible-gen1",
      "prior_accountable_digest" => String.duplicate("5", 64),
      "prior_responsible_digest" => String.duplicate("6", 64),
      "successor_accountable_id" => "accountable-gen2",
      "successor_responsible_id" => current_claim["responsibleDelegationId"],
      "successor_accountable_digest" => String.duplicate("7", 64),
      "successor_responsible_digest" => String.duplicate("8", 64),
      "manifest_sha256" => String.duplicate("9", 64),
      "signer_key_sha256" => String.duplicate("a", 64),
      "observation_sha256" => String.duplicate("b", 64)
    }

    session = "worker:#{@issue}:1"

    lease = %{
      "issue_id" => @issue,
      "repository" => @repository,
      "generation" => 1,
      "role" => "worker",
      "session_id" => session,
      "process_id" => session,
      "branch" => "codex/hgs736",
      "worktree" => "C:/absent",
      "status" => "released",
      "registered_at_ms" => @now_ms - 200_000,
      "last_heartbeat_at" => 0,
      "linear_state" => "In Progress",
      "pr_state" => nil,
      "head" => "unobserved",
      "termination_required" => false,
      "termination_confirmed_at_ms" => nil,
      "termination_evidence_ref" => nil,
      "termination_evidence" => nil,
      "supervisor_identity" => nil,
      "release_reason" => "claim_not_submitted"
    }

    execution = %{
      "issue_id" => @issue,
      "repository" => @repository,
      "worker_host" => "host-1",
      "generation" => 1,
      "branch" => "codex/hgs736",
      "worktree" => "C:/absent",
      "status" => "retired",
      "ownership" => "reconciled",
      "leases" => %{session => lease},
      "terminal" => nil,
      "retirement" => receipt,
      "cleanup" => "cleaned",
      "cleanup_receipt" => nil,
      "termination_unconfirmed" => false,
      "admitted_at_ms" => @now_ms - 200_000,
      "cleaned_at_ms" => @now_ms - 100_000
    }

    %{"execution" => execution, "claim" => old_claim, "receipt" => receipt}
  end
end
