defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidenceTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryContext
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuance
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuer, as: Issuer
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliation
  alias SymphonyElixir.WorkPackageClaim.Journal

  @now_ms 1_790_762_400_000
  @issue "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"
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

  test "v2 accepts only explicit absent state with a null digest and its own binding shape" do
    payload = absent_snapshot_payload()
    bindings = absent_snapshot_bindings()

    assert :ok = Evidence.validate_payload(payload, bindings)
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.validate_payload(payload, bindings())

    refute_payload_valid(Map.delete(payload, "assignmentSnapshotState"), bindings)
    refute_payload_valid(Map.put(payload, "assignmentSnapshotState", "present"), bindings)
    refute_payload_valid(Map.put(payload, "assignmentSHA256", @assignment_sha), bindings)
    refute_payload_valid(Map.put(payload, "extra", true), bindings)
    refute_payload_valid(payload, Map.delete(bindings, :assignment_snapshot_state))
  end

  test "rejects an incomplete or positive claim-bound Kubernetes snapshot" do
    payload = payload()
    refute_valid(put_in(payload, ["observation", "kubernetes", "jobs", "complete"], false))
    refute_valid(put_in(payload, ["observation", "kubernetes", "pods", "claimAbsent"], false))
    refute_valid(put_in(payload, ["observation", "kubernetes", "jobs", "itemCount"], -1))
  end

  test "v3 binds a retirement-only predecessor and cannot be downgraded to v2" do
    payload = absent_snapshot_payload()
    predecessor = payload["observation"]["predecessorRetirement"]
    receipt = Map.put(predecessor["receipt"], "provider_projection_id", payload["observation"]["expected"]["projectionId"])
    receipt = Map.put(receipt, "evidence_ref", Evidence.retirement_evidence_ref(receipt))
    execution = predecessor["execution"] |> Map.put("retirement", receipt) |> Map.put("cleaned_at_ms", receipt["retired_at_ms"]) |> Map.put("worker_host", nil)

    payload =
      payload
      |> Map.put("contractVersion", "work-package-paused-confirmed-recovery.v3")
      |> put_in(["observation", "predecessorRetirement"], %{"execution" => execution, "claim" => nil, "receipt" => receipt})
      |> put_in(["observation", "witnesses"], Enum.filter(payload["observation"]["witnesses"], &(&1["generation"] == 2)))

    assert :ok = Evidence.validate_payload(payload, absent_snapshot_bindings())

    epoch_payload = reconciliation_payload(payload)
    epoch_bindings = Map.put(absent_snapshot_bindings(), :reservation_id, epoch_payload["reservationId"])
    assert :ok = Evidence.validate_payload(epoch_payload, epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["observation", "reconciliation", "historicalSHA256"], %{}), epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["observation", "reconciliation", "epoch"], "epoch-2"), epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["observation", "processCount"], 1), epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["observation", "globalPause"], false), epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["observation", "workspaceAbsent"], false), epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["observation", "kubernetes", "jobs", "claimAbsent"], false), epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["observation", "kubernetes", "pods", "claimAbsent"], false), epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["providerHeld", "scopeState"], "released"), epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["providerHeld", "credentialLeaseInventory", "leaseIds"], ["lease"]), epoch_bindings)
    refute_payload_valid(put_in(epoch_payload, ["providerHeld", "oauthSlotLeaseInventory", "leaseCount"], 1), epoch_bindings)
    refute_payload_valid(epoch_payload, Map.put(epoch_bindings, :now_ms, @now_ms + 61_000))

    bundle =
      Map.take(payload, ~w(assignmentSHA256 assignmentSnapshotState reservationId observation providerHeld))
      |> Map.put("predecessorClaimState", "unsubmitted")

    {public, private} = :crypto.generate_key(:eddsa, :ed25519)
    sign = fn message -> :crypto.sign(:eddsa, :none, message, [private, :ed25519]) end
    verify = fn envelope, bindings -> Evidence.verify_test_envelope(envelope, public, bindings) end
    epoch_bundle = Map.put(bundle, "observation", epoch_payload["observation"]) |> Map.put("reservationId", epoch_payload["reservationId"]) |> Map.put("providerHeld", epoch_payload["providerHeld"])

    assert {:ok, epoch_issued, _bytes, epoch_envelope} =
             Issuer.issue(Evidence.canonical_json(epoch_bundle), @pool, @issue, @nonce, epoch_bindings, sign, verify)

    assert {:ok, ^epoch_issued} = Evidence.verify_test_envelope(epoch_envelope, public, epoch_bindings)
    bindings = Issuer.bindings(bundle, @pool, @issue, @nonce, @now_ms)
    assert {:ok, issued, _bytes, envelope} = Issuer.issue(Evidence.canonical_json(bundle), @pool, @issue, @nonce, bindings, sign, verify)
    assert issued["contractVersion"] == "work-package-paused-confirmed-recovery.v3"
    assert {:ok, ^issued} = Evidence.verify_test_envelope(envelope, public, bindings)
    assert {:error, :invalid_confirmed_recovery_evidence} = Issuer.issue(Evidence.canonical_json(Map.delete(bundle, "predecessorClaimState")), @pool, @issue, @nonce, bindings, sign, verify)
    refute_payload_valid(Map.put(payload, "contractVersion", "work-package-paused-confirmed-recovery.v2"), absent_snapshot_bindings())
    refute_payload_valid(put_in(payload, ["observation", "predecessorRetirement", "claim"], predecessor["claim"]), absent_snapshot_bindings())
    refute_payload_valid(put_in(payload, ["observation", "predecessorRetirement", "execution", "leases", predecessor["claim"]["sessionId"], "head"], "observed"), absent_snapshot_bindings())
    refute Evidence.signature_message("payload", "work-package-paused-confirmed-recovery.v3") == Evidence.signature_message("payload", "work-package-paused-confirmed-recovery.v2")
  end

  defp reconciliation_payload(payload) do
    expected =
      payload["observation"]["expected"]
      |> Map.put("projectionId", "workpkg_4446a7d851764ecf9bf62bfbae26d1cc")
      |> Map.put("reservationId", "workpkgreservation_e19008ccb2764fe79ca68bf500d20a1f")

    payload =
      payload
      |> Map.put("reservationId", expected["reservationId"])
      |> put_in(["observation", "expected"], expected)
      |> put_in(["providerHeld", "expected"], expected)
      |> put_in(["observation", "kubernetes", "claim", "reservationId"], expected["reservationId"])

    payload = put_in(payload, ["providerHeld", "assignmentDigest"], Evidence.tuple_digest(expected))
    retirement = payload["observation"]["predecessorRetirement"]["receipt"] |> Map.put("provider_projection_id", expected["projectionId"])
    retirement = Map.put(retirement, "evidence_ref", Evidence.retirement_evidence_ref(retirement))

    payload =
      payload
      |> put_in(["observation", "predecessorRetirement", "receipt"], retirement)
      |> put_in(["observation", "predecessorRetirement", "execution", "retirement"], retirement)

    metadata = %{
      "contractVersion" => "hgs740-reconciliation-observation.v1",
      "epoch" => "epoch-1",
      "historicalSHA256" => ConfirmedRecoveryReconciliation.historical_hashes(),
      "observedAt" => payload["observation"]["observedAt"],
      "reviewedPreflightSHA256" => @fence_sha,
      "providerReadbackSHA256" => @fence_sha,
      "issuerInputSHA256" => @fence_sha,
      "providerHeldSHA256" => :crypto.hash(:sha256, Evidence.canonical_json(payload["providerHeld"])) |> Base.encode16(case: :lower)
    }

    put_in(payload, ["observation", "reconciliation"], metadata)
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
    assert Evidence.tuple_digest(claim) == "4f2bda1e3d35af5d8b8d413818202dd3ec7ae539a1dde52a57349e9ec7b9b7b0"
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

  test "the v1 signing bytes remain unchanged when the versioned API is used" do
    payload_bytes = Evidence.canonical_json(payload())
    expected = "hypergrid-work-package-recovery:hgs740-confirmed-root.v1\0" <> payload_bytes

    assert Evidence.signature_message(payload_bytes) == expected
    assert Evidence.signature_message(payload_bytes, "work-package-paused-confirmed-recovery.v1") == expected
  end

  test "issuer signs the disjoint v2 null-digest contract and rejects mixed v1/v2 bundle fields" do
    source = absent_snapshot_payload()

    bundle = %{
      "assignmentSHA256" => nil,
      "assignmentSnapshotState" => "absent",
      "reservationId" => source["reservationId"],
      "observation" => source["observation"],
      "providerHeld" => source["providerHeld"]
    }

    {public, private} = :crypto.generate_key(:eddsa, :ed25519, :binary.copy(<<7>>, 32))

    assert {:ok, issued, payload_bytes, envelope_bytes} =
             Issuer.issue(
               Evidence.canonical_json(bundle),
               @pool,
               @issue,
               @nonce,
               absent_snapshot_bindings(),
               fn message -> :crypto.sign(:eddsa, :none, message, [private, :ed25519]) end,
               fn envelope, exact_bindings -> Evidence.verify_test_envelope(envelope, public, exact_bindings) end
             )

    assert issued["contractVersion"] == "work-package-paused-confirmed-recovery.v2"
    assert issued["assignmentSnapshotState"] == "absent"
    assert is_nil(issued["assignmentSHA256"])
    assert Evidence.canonical_json(issued) == payload_bytes
    assert {:ok, ^issued} = Evidence.verify_test_envelope(envelope_bytes, public, absent_snapshot_bindings())

    refute_issuer_bundle_valid(Map.put(bundle, "assignmentSHA256", @assignment_sha), absent_snapshot_bindings())
    refute_issuer_bundle_valid(Map.put(bundle, "unexpected", true), absent_snapshot_bindings())
  end

  test "issuer emits a canonical nonce-bound proof only after independent verifier roundtrip" do
    source = payload()

    bundle = %{
      "assignmentSHA256" => source["assignmentSHA256"],
      "reservationId" => source["reservationId"],
      "observation" => source["observation"],
      "providerHeld" => source["providerHeld"]
    }

    {public, private} = :crypto.generate_key(:eddsa, :ed25519, :binary.copy(<<3>>, 32))

    assert {:ok, issued, payload_bytes, envelope_bytes} =
             Issuer.issue(
               Evidence.canonical_json(bundle),
               @pool,
               @issue,
               @nonce,
               bindings(),
               fn message -> :crypto.sign(:eddsa, :none, message, [private, :ed25519]) end,
               fn envelope, exact_bindings -> Evidence.verify_test_envelope(envelope, public, exact_bindings) end
             )

    assert issued["nonce"] == @nonce
    assert Evidence.canonical_json(issued) == payload_bytes
    assert {:ok, ^issued} = Evidence.verify_test_envelope(envelope_bytes, public, bindings())

    assert {:error, :invalid_confirmed_recovery_evidence} =
             Issuer.issue(Evidence.canonical_json(Map.put(bundle, "extra", true)), @pool, @issue, @nonce, bindings(), fn _ -> <<0::512>> end, fn _, _ -> {:error, :denied} end)
  end

  test "issuer fails closed on signing and independent verification denials" do
    bundle_bytes = Evidence.canonical_json(issuer_test_bundle())
    bindings = bindings()
    denied = fn _, _ -> {:error, :denied} end

    assert {:error, :invalid_confirmed_recovery_evidence} =
             Issuer.issue(bundle_bytes, @pool, @issue, @nonce, bindings, fn _ -> {:error, :signer_unavailable} end, denied)

    assert {:error, :invalid_confirmed_recovery_evidence} =
             Issuer.issue(bundle_bytes, @pool, @issue, @nonce, bindings, fn _ -> {:ok, <<0::256>>} end, denied)

    assert {:error, :invalid_confirmed_recovery_evidence} =
             Issuer.issue(bundle_bytes, @pool, @issue, @nonce, bindings, fn _ -> {:ok, <<0::512>>} end, fn _, _ -> {:ok, %{}} end)
  end

  test "issuance binds exact local preimages, refreshes complete Kubernetes absence, and writes verified bytes" do
    source = issuer_test_bundle()
    reservation_nonce = "issuer-v1-reservation-nonce"
    expected = Map.put(source["observation"]["expected"], "nonceHash", digest(reservation_nonce))
    {assignment, snapshot} = test_assignment_snapshot(expected)
    {:ok, journal_bytes} = Journal.encode_bytes(confirmed_journal(expected, reservation_nonce, snapshot))

    state_bytes = %{
      "journal" => journal_bytes,
      "fence" => "execution-fence",
      "graph" => "responsibility-graph"
    }

    runtime = %{
      pool_key: @pool,
      journal_path: "/state/journal",
      execution_fence_path: "/state/fence",
      responsibility_graph_path: "/state/graph"
    }

    observation =
      source["observation"]
      |> Map.put("expected", expected)
      |> put_in(["kubernetes", "claim", "assignmentSHA256"], assignment.sha256)

    observation =
      observation
      |> Map.put("claimJournalSHA256", digest(state_bytes["journal"]))
      |> Map.put("fenceSHA256", digest(state_bytes["fence"]))
      |> Map.put("responsibilityGraphSHA256", digest(state_bytes["graph"]))

    provider =
      source["providerHeld"]
      |> Map.put("expected", expected)
      |> Map.put("assignmentDigest", Evidence.tuple_digest(expected))

    bundle =
      source
      |> Map.put("assignmentSHA256", assignment.sha256)
      |> Map.put("observation", observation)
      |> Map.put("providerHeld", provider)

    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519, :binary.copy(<<11>>, 32))
    parent = self()

    host_ops = %{
      lstat: fn
        "/state/journal" -> {:ok, state_stat()}
        "/state/fence" -> {:ok, state_stat()}
        "/state/graph" -> {:ok, state_stat()}
      end,
      read: fn
        "/state/journal" -> {:ok, state_bytes["journal"]}
        "/state/fence" -> {:ok, state_bytes["fence"]}
        "/state/graph" -> {:ok, state_bytes["graph"]}
      end,
      now_ms: fn -> @now_ms end,
      sign_recovery_payload: fn bytes ->
        result = {:ok, :crypto.sign(:eddsa, :none, bytes, [private_key, :ed25519])}
        send(parent, {:sign_result, result})
        result
      end,
      verify_signed_evidence: fn envelope, bindings ->
        result = Evidence.verify_test_envelope(envelope, public_key, bindings)
        send(parent, {:verify_result, result})
        result
      end,
      persist_issuer_outputs: fn issue_id, candidate, envelope ->
        send(parent, {:outputs, issue_id, candidate, envelope})
        :ok
      end
    }

    context = %ConfirmedRecoveryContext{
      issue_id: @issue,
      pool: @pool,
      nonce: @nonce,
      workflow_path: "/trusted/workflow.md",
      runtime: runtime,
      host_ops: host_ops
    }

    observe_kubernetes = fn received_observation, assignment_sha ->
      assert assignment_sha == bundle["assignmentSHA256"]
      assert received_observation["expected"]["issueId"] == @issue
      cluster = received_observation["kubernetes"]["cluster"]
      assert cluster["apiServer"] == "https://10.0.0.1:6443"

      ConfirmedRecoveryIssuance.fresh_kubernetes_with_test_observer(
        received_observation,
        assignment_sha,
        fn _claim, _cluster ->
          {:ok,
           %{
             "observedAt" => "2026-09-30T09:59:45Z",
             "namespace" => "frigga",
             "jobs" => %{
               "confirmingResourceVersion" => "12346",
               "sha256" => digest("fresh-jobs"),
               "itemCount" => 0,
               "claimAbsent" => true
             },
             "pods" => %{
               "resourceVersion" => "12347",
               "sha256" => digest("fresh-pods"),
               "itemCount" => 0,
               "claimAbsent" => true
             }
           }}
        end
      )
    end

    issue_result =
      ConfirmedRecoveryIssuance.issue_bundle_with_test_context(
        context,
        Evidence.canonical_json(bundle),
        observe_kubernetes
      )

    assert_received {:sign_result, sign_result}
    assert_received {:verify_result, verify_result}
    assert sign_result != nil
    assert verify_result != nil
    assert :ok = issue_result

    assert_received {:outputs, @issue, candidate_bytes, envelope_bytes}
    assert {:ok, candidate} = Jason.decode(candidate_bytes)
    assert Evidence.canonical_json(candidate) == candidate_bytes
    assert candidate["claimJournalSHA256"] == digest(state_bytes["journal"])
    bindings = Issuer.bindings(bundle, @pool, @issue, @nonce, @now_ms)
    assert {:ok, issued} = Evidence.verify_test_envelope(envelope_bytes, public_key, bindings)
    assert issued["observation"] == candidate
    assert issued["providerHeld"]["observedAt"] > candidate["observedAt"]

    {:ok, missing_snapshot_journal} = Journal.encode_bytes(confirmed_journal(expected, reservation_nonce))
    explicit_null_journal = put_journal_snapshot_null(journal_bytes, expected)

    for changed_journal <- [missing_snapshot_journal, explicit_null_journal] do
      denied_bundle = put_in(bundle, ["observation", "claimJournalSHA256"], digest(changed_journal))

      denied_host =
        Map.put(host_ops, :read, fn
          "/state/journal" -> {:ok, changed_journal}
          "/state/fence" -> {:ok, state_bytes["fence"]}
          "/state/graph" -> {:ok, state_bytes["graph"]}
        end)

      assert {:error, :local_preimage_changed} =
               ConfirmedRecoveryIssuance.issue_bundle_with_test_context(
                 %{context | host_ops: denied_host},
                 Evidence.canonical_json(denied_bundle),
                 fn _observation, _assignment_sha -> flunk("observer must not run") end
               )
    end
  end

  test "issuance stops before signing or output when a retained preimage changed" do
    bundle = issuer_test_bundle()
    state_bytes = %{"journal" => "changed-journal", "fence" => "execution-fence", "graph" => "responsibility-graph"}
    observation = bundle["observation"]

    observation =
      observation
      |> Map.put("claimJournalSHA256", digest("original-journal"))
      |> Map.put("fenceSHA256", digest(state_bytes["fence"]))
      |> Map.put("responsibilityGraphSHA256", digest(state_bytes["graph"]))

    bundle = Map.put(bundle, "observation", observation)
    parent = self()

    host_ops = %{
      lstat: fn _path -> {:ok, state_stat()} end,
      read: fn
        "/state/journal" -> {:ok, state_bytes["journal"]}
        "/state/fence" -> {:ok, state_bytes["fence"]}
        "/state/graph" -> {:ok, state_bytes["graph"]}
      end,
      now_ms: fn -> @now_ms end,
      sign_recovery_payload: fn _bytes ->
        send(parent, :sign_must_not_run)
        <<0::512>>
      end,
      verify_signed_evidence: fn _envelope, _bindings -> flunk("verifier must not run") end,
      persist_issuer_outputs: fn _issue_id, _candidate, _envelope -> flunk("outputs must not be written") end
    }

    context = %ConfirmedRecoveryContext{
      issue_id: @issue,
      pool: @pool,
      nonce: @nonce,
      workflow_path: "/trusted/workflow.md",
      runtime: %{
        pool_key: @pool,
        journal_path: "/state/journal",
        execution_fence_path: "/state/fence",
        responsibility_graph_path: "/state/graph"
      },
      host_ops: host_ops
    }

    assert {:error, :local_preimage_changed} =
             ConfirmedRecoveryIssuance.issue_bundle_with_test_context(
               context,
               Evidence.canonical_json(bundle),
               fn observation, _assignment_sha -> {:ok, observation} end
             )

    refute_received :sign_must_not_run
  end

  test "v2 issuance binds raw journal key absence, reservation state, and a fresh null-hash Kubernetes read" do
    source = absent_snapshot_issuer_bundle()
    reservation_nonce = "issuer-reservation-nonce"
    expected = source["observation"]["expected"]
    expected = Map.put(expected, "nonceHash", digest(reservation_nonce))
    observation = Map.put(source["observation"], "expected", expected)

    provider =
      source["providerHeld"]
      |> Map.put("expected", expected)
      |> Map.put("assignmentDigest", Evidence.tuple_digest(expected))

    {:ok, journal_bytes} = Journal.encode_bytes(confirmed_journal(expected, reservation_nonce))
    fence_bytes = "execution-fence"
    graph_bytes = "responsibility-graph"

    observation =
      observation
      |> Map.put("claimJournalSHA256", digest(journal_bytes))
      |> Map.put("fenceSHA256", digest(fence_bytes))
      |> Map.put("responsibilityGraphSHA256", digest(graph_bytes))

    bundle =
      source
      |> Map.put("observation", observation)
      |> Map.put("providerHeld", provider)

    bundle = put_in(bundle, ["observation", "kubernetes", "claim", "assignmentSHA256"], nil)
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519, :binary.copy(<<11>>, 32))
    parent = self()

    runtime = %{
      pool_key: @pool,
      journal_path: "/state/journal",
      execution_fence_path: "/state/fence",
      responsibility_graph_path: "/state/graph"
    }

    host_ops = %{
      lstat: fn _path -> {:ok, state_stat()} end,
      read: fn
        "/state/journal" -> {:ok, journal_bytes}
        "/state/fence" -> {:ok, fence_bytes}
        "/state/graph" -> {:ok, graph_bytes}
      end,
      now_ms: fn -> @now_ms end,
      sign_recovery_payload: fn message -> :crypto.sign(:eddsa, :none, message, [private_key, :ed25519]) end,
      verify_signed_evidence: fn envelope, exact_bindings ->
        Evidence.verify_test_envelope(envelope, public_key, exact_bindings)
      end,
      persist_issuer_outputs: fn issue_id, candidate, envelope ->
        send(parent, {:absent_outputs, issue_id, candidate, envelope})
        :ok
      end
    }

    context = %ConfirmedRecoveryContext{
      issue_id: @issue,
      pool: @pool,
      nonce: @nonce,
      workflow_path: "/trusted/workflow.md",
      runtime: runtime,
      host_ops: host_ops
    }

    observer = fn received_observation, assignment_sha ->
      assert is_nil(assignment_sha)

      ConfirmedRecoveryIssuance.fresh_kubernetes_with_test_observer(
        received_observation,
        nil,
        fn claim, _cluster ->
          assert claim["assignmentSnapshotState"] == "absent"
          assert is_nil(claim["assignmentSHA256"])

          {:ok,
           %{
             "observedAt" => "2026-09-30T09:59:45Z",
             "namespace" => "frigga",
             "jobs" => %{
               "confirmingResourceVersion" => "12346",
               "sha256" => digest("jobs"),
               "itemCount" => 0,
               "claimAbsent" => true
             },
             "pods" => %{
               "resourceVersion" => "12347",
               "sha256" => digest("pods"),
               "itemCount" => 0,
               "claimAbsent" => true
             }
           }}
        end
      )
    end

    assert :ok =
             ConfirmedRecoveryIssuance.issue_bundle_with_test_context(
               context,
               Evidence.canonical_json(bundle),
               observer
             )

    assert_received {:absent_outputs, @issue, candidate_bytes, envelope_bytes}
    assert {:ok, candidate} = Jason.decode(candidate_bytes)
    assert is_nil(candidate["kubernetes"]["claim"]["assignmentSHA256"])
    exact_bindings = Issuer.bindings(bundle, @pool, @issue, @nonce, @now_ms)
    assert {:ok, payload} = Evidence.verify_test_envelope(envelope_bytes, public_key, exact_bindings)
    assert payload["assignmentSnapshotState"] == "absent"
    assert payload["observation"] == candidate

    explicit_null = put_journal_snapshot_null(journal_bytes, expected)
    bundle = put_in(bundle, ["observation", "claimJournalSHA256"], digest(explicit_null))

    denied_host =
      Map.put(host_ops, :read, fn
        "/state/journal" -> {:ok, explicit_null}
        "/state/fence" -> {:ok, fence_bytes}
        "/state/graph" -> {:ok, graph_bytes}
      end)

    denied_context = %{context | host_ops: denied_host}

    pass_observation = fn observation, _assignment_sha -> {:ok, observation} end

    assert {:error, :local_preimage_changed} =
             ConfirmedRecoveryIssuance.issue_bundle_with_test_context(
               denied_context,
               Evidence.canonical_json(bundle),
               pass_observation
             )
  end

  test "fresh Kubernetes confirmation replaces the bundle snapshot while preserving the host timestamp" do
    bundle = issuer_test_bundle()
    observation = bundle["observation"]
    fresh_at = "2026-09-30T09:59:45Z"

    snapshot = %{
      "observedAt" => fresh_at,
      "namespace" => "frigga",
      "jobs" => %{"confirmingResourceVersion" => "12", "sha256" => String.duplicate("1", 64), "itemCount" => 0, "claimAbsent" => true},
      "pods" => %{"resourceVersion" => "13", "sha256" => String.duplicate("2", 64), "itemCount" => 0, "claimAbsent" => true}
    }

    assert {:ok, updated} =
             ConfirmedRecoveryIssuance.fresh_kubernetes_with_test_observer(
               observation,
               bundle["assignmentSHA256"],
               fn claim, _cluster ->
                 assert claim["issueId"] == @issue
                 assert claim["assignmentSHA256"] == bundle["assignmentSHA256"]
                 {:ok, snapshot}
               end
             )

    assert updated["kubernetes"]["observedAt"] == fresh_at
    assert updated["kubernetes"]["jobs"]["resourceVersion"] == "12"
    assert updated["kubernetes"]["pods"]["resourceVersion"] == "13"
    assert updated["observedAt"] == observation["observedAt"]
  end

  test "fresh Kubernetes refresh fails closed on malformed inputs, denial, and observer failure" do
    bundle = issuer_test_bundle()
    observation = bundle["observation"]
    unavailable = fn _claim, _cluster -> {:error, :api_unavailable} end

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryIssuance.fresh_kubernetes_with_test_observer(
               nil,
               bundle["assignmentSHA256"],
               fn _, _ -> flunk("malformed observation must not reach Kubernetes") end
             )

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryIssuance.fresh_kubernetes_with_test_observer(
               Map.put(observation, "kubernetes", nil),
               bundle["assignmentSHA256"],
               fn _, _ -> flunk("missing Kubernetes context must not make a request") end
             )

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryIssuance.fresh_kubernetes_with_test_observer(
               observation,
               bundle["assignmentSHA256"],
               unavailable
             )

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryIssuance.fresh_kubernetes_with_test_observer(
               observation,
               bundle["assignmentSHA256"],
               fn _, _ -> raise "observer failure" end
             )
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

  defp refute_payload_valid(candidate, exact_bindings) do
    assert {:error, :invalid_confirmed_recovery_evidence} = Evidence.validate_payload(candidate, exact_bindings)
  end

  defp refute_issuer_bundle_valid(bundle, exact_bindings) do
    assert {:error, :invalid_confirmed_recovery_evidence} =
             Issuer.issue(
               Evidence.canonical_json(bundle),
               exact_bindings.pool,
               exact_bindings.issue_id,
               exact_bindings.nonce,
               exact_bindings,
               fn _message -> <<0::512>> end,
               fn _envelope, _bindings -> {:error, :denied} end
             )
  end

  defp absent_snapshot_bindings do
    bindings()
    |> Map.put(:assignment_sha256, nil)
    |> Map.put(:assignment_snapshot_state, "absent")
  end

  defp absent_snapshot_payload do
    payload()
    |> Map.put("contractVersion", "work-package-paused-confirmed-recovery.v2")
    |> Map.put("assignmentSnapshotState", "absent")
    |> Map.put("assignmentSHA256", nil)
    |> put_in(["observation", "kubernetes", "claim", "assignmentSHA256"], nil)
  end

  defp absent_snapshot_issuer_bundle do
    absent_snapshot_payload()
    |> Map.take(["assignmentSHA256", "reservationId", "observation", "providerHeld"])
    |> Map.put("assignmentSnapshotState", "absent")
  end

  defp confirmed_journal(expected, reservation_nonce, assignment_snapshot \\ nil) do
    key =
      Journal.reservation_key(
        expected["issueId"],
        expected["managedProjectProfileId"],
        expected["repositoryRef"],
        2
      )

    reservation = %{
      issue_id: expected["issueId"],
      managed_project_profile_id: expected["managedProjectProfileId"],
      repository_ref: expected["repositoryRef"],
      projection_id: expected["projectionId"],
      reservation_id: expected["reservationId"],
      reservation_nonce: reservation_nonce,
      scope_keys: expected["scopeKeys"],
      runner_id: expected["runnerId"],
      workspace_id: expected["workspaceId"],
      company_id: expected["companyId"],
      generation: expected["generation"],
      session_id: expected["sessionId"],
      process_id: expected["processId"],
      responsible_delegation_id: expected["responsibleDelegationId"],
      execution_fence_token: expected["executionFenceToken"],
      runtime_lease_id: expected["runtimeLeaseId"],
      dispatch: %{
        phase: "confirmed",
        attempts: 1,
        retry_at_ms: 0,
        authority_digest: String.duplicate("a", 64),
        allocation_id: nil
      }
    }

    reservation = maybe_put_assignment_snapshot(reservation, assignment_snapshot)

    {:ok, state} = Journal.put(Journal.new(), key, reservation)
    state
  end

  defp maybe_put_assignment_snapshot(reservation, nil), do: reservation

  defp maybe_put_assignment_snapshot(reservation, snapshot) when is_binary(snapshot),
    do: Map.put(reservation, :assignment_snapshot, snapshot)

  defp test_assignment_snapshot(expected) do
    attrs = %{
      objective: %{id: "objective-gen2", identity: "objective-gen2", content: "Synthetic recovery assignment"},
      repository_ref: expected["repositoryRef"],
      base_ref: "main",
      branch: "codex/hgs740-test",
      seat: expected["runnerId"],
      lease: %{
        issue_id: expected["issueId"],
        repository: expected["repositoryRef"],
        session_id: expected["sessionId"],
        process_id: expected["processId"],
        generation: expected["generation"]
      },
      intent_ancestry: ["HGS-740", expected["responsibleDelegationId"]],
      acceptance: %{deliverable: "Synthetic assignment", evidence: "Fixture only"},
      context_secret_refs: [],
      platform: "linux-x86_64",
      environment_classification: "repository",
      environment_constraints: ["synthetic"],
      placement: :internal_beta,
      target_environment: :rke2
    }

    {:ok, assignment} = ManagedAssignmentBundle.build(attrs)
    {:ok, snapshot} = ManagedAssignmentBundle.snapshot(assignment)
    {assignment, snapshot}
  end

  defp put_journal_snapshot_null(bytes, expected) do
    key =
      Journal.reservation_key(
        expected["issueId"],
        expected["managedProjectProfileId"],
        expected["repositoryRef"],
        2
      )

    bytes
    |> Jason.decode!()
    |> put_in(["reservations", key, "assignment_snapshot"], nil)
    |> Jason.encode!()
  end

  @doc false
  def issuer_test_bundle do
    value = payload()

    %{
      "assignmentSHA256" => value["assignmentSHA256"],
      "reservationId" => value["reservationId"],
      "observation" => value["observation"],
      "providerHeld" => value["providerHeld"]
    }
  end

  @doc false
  def issuer_test_bindings, do: bindings()

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

  defp state_stat, do: %File.Stat{type: :regular, uid: 1000, gid: 1000, mode: 0o600, links: 1}

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
