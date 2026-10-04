Code.require_file("../support/confirmed_release_fixture.exs", __DIR__)

defmodule SymphonyElixir.WorkPackageClaim.ConfirmedReleaseCompletionTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.Test.ConfirmedReleaseFixture, as: Fixture
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletion, as: Completion
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionBundle, as: Bundle
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionHistory, as: History
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionProof, as: Proof

  test "original domains, real decision shape and historical enrollment validate only at confirmation" do
    f = Fixture.fixture()
    assert {:ok, signed} = signed(f)
    assert {:ok, payload} = Proof.verify(f.readback, f.bundle, signed, f.fingerprint)
    assert payload["contractVersion"] == "hgs740-release-completion.v1"
    {review, enrollment} = f.history
    assert {:ok, _} = History.verify(review, enrollment, f.readback, f.public)
    refute Proof.fresh?(f.readback, f.now + 86_400_000)
    assert {:error, _} = Bundle.verify(f.bundle, f.bundle["binding"], f.snapshot.candidate_bytes, f.local, f.provider, f.local["approverRef"], f.public, f.now + 86_400_000)
  end

  test "all three domains and every provider authority field fail closed independently" do
    f = Fixture.fixture()

    for name <- ~w(authorization attestation receipt) do
      envelope = Jason.decode!(f.bundle[name]) |> Map.put("signature", Base.url_encode64(<<0::512>>, padding: false))
      assert {:error, _} = signed(%{f | bundle: Map.put(f.bundle, name, Fixture.canonical(envelope))})
    end

    mutations = [
      {["approvalState"], "pending"},
      {["targetAction"], "hgs740_release_local"},
      {["decisionMode"], "machine"},
      {["approverType"], "human"},
      {["decisionActorType"], "machine"},
      {["approverRef"], "another"},
      {["decisionActorRef"], "another"},
      {["approvalId"], f.local["approvalId"]},
      {["workspaceId"], "other"},
      {["companyId"], "other"},
      {["supersededByApprovalId"], "new"},
      {["expiresAt"], Fixture.iso(f.now)},
      {["decidedAt"], Fixture.iso(f.now + 501)},
      {["decisionPayload", "nativeDecisionId"], "other"},
      {["decisionPayload", "attemptId"], "other"},
      {["decisionPayload", "sourceHeads", "symphony"], String.duplicate("f", 40)},
      {["decisionPayload", "bindingSHA256"], String.duplicate("0", 64)},
      {["decisionPayload", "allowedActions"], []}
    ]

    for {path, value} <- mutations do
      assert {:error, _} = signed(%{f | provider: put_in(f.provider, path, value)})
    end
  end

  test "correct signatures cannot admit stale authority, future collection, false counts or extra fields" do
    f = Fixture.fixture()
    original = Fixture.decode(f.bundle["attestation"])

    changes = [
      {["authorizationSHA256"], String.duplicate("0", 64)},
      {["expiresAt"], Fixture.iso(f.now + 500)},
      {["expiresAt"], Fixture.iso(f.now + 60_001)},
      {["observedAt"], Fixture.iso(f.now + 5_501)},
      {["checks", "workerCount"], false},
      {["checks", "jobCount"], 1},
      {["readbacks", "hostReadbackSHA256"], "bad"}
    ]

    for {path, value} <- changes do
      raw = original |> put_in(path, value) |> Fixture.sign("hgs740-release-attestation.v1", f.private)
      assert {:error, _} = signed(%{f | bundle: Map.put(f.bundle, "attestation", raw)})
    end

    raw = original |> Map.put("additionalAuthority", true) |> Fixture.sign("hgs740-release-attestation.v1", f.private)
    assert {:error, _} = signed(%{f | bundle: Map.put(f.bundle, "attestation", raw)})
  end

  test "immutable confirmation, projected proof and released postimages deny mutations" do
    f = Fixture.fixture()
    {:ok, signed} = signed(f)

    changes = [
      {["confirmation", "receipt", "nextGenerationFloor"], 4},
      {["confirmation", "nextGenerationFloor"], "3"},
      {["confirmation", "receipt", "oldNonceHash"], String.duplicate("0", 64)},
      {["confirmation", "proofDigest"], String.duplicate("0", 64)},
      {["confirmation", "proof", "signature"], "other"},
      {["confirmation", "proof", "proof", "evidenceRef"], "other"},
      {["confirmation", "publicKeyFingerprint"], String.duplicate("0", 64)},
      {["currentState", "generation"], "3"},
      {["currentState", "claimEvidence"], %{}},
      {["currentState", "nonceHash"], String.duplicate("0", 64)},
      {["currentState", "executionCapacityState"], "held"},
      {["bundleSHA256"], String.duplicate("0", 64)},
      {["sourceIdentity"], "untrusted"}
    ]

    for {path, value} <- changes do
      assert {:error, _} = Proof.verify(put_in(f.readback, path, value), f.bundle, signed, f.fingerprint)
    end

    {review, enrollment} = f.history

    for bytes <- [enrollment <> " ", Fixture.canonical(put_in(Jason.decode!(enrollment), ["expiresAt"], Fixture.iso(f.now + 500)))] do
      assert {:error, _} = History.verify(review, bytes, f.readback, f.public)
    end
  end

  test "crash witness is retained, compared to fresh readback and verified offline after marker completion" do
    f = Fixture.fixture()
    {runtime, ports, state} = coordinator(f)
    assert {:ok, bytes, payload} = Completion.verify(f.marker, runtime, true, ports)
    assert Agent.get(state, & &1.readbacks) == 1
    Agent.update(state, &%{&1 | response: put_in(f.readback, ["observedAt"], Fixture.iso(f.now + 2_000))})
    assert {:ok, ^bytes, ^payload} = Completion.verify(f.marker, runtime, true, ports)
    assert Agent.get(state, & &1.creates) == 1
    completed = f.marker |> Map.put("status", "complete") |> Map.put("providerFinalProofSHA256", Fixture.hash(bytes))
    completed = completed |> Map.put("providerReceipt", payload["receipt"]) |> Map.put("providerJournalSHA256", payload["journalSHA256"])
    offline = put_in(runtime.host_ops.read_confirmed_release, fn _, _ -> flunk("restart must not dispatch") end)
    assert {:ok, ^bytes, ^payload} = Completion.verify(completed, offline, false, ports)
    assert {:error, _} = Completion.verify(Map.put(completed, "nonce", "changed"), offline, false, ports)
    Agent.update(state, &%{&1 | response: put_in(f.readback, ["currentState", "generation"], "3")})
    assert {:error, _} = Completion.verify(f.marker, runtime, true, ports)
    assert Agent.get(state, & &1.files["/synthetic/release-completion-witness.json"]) == bytes
    stale_runtime = put_in(runtime.host_ops.now_ms, fn -> f.now + 62_001 end)
    Agent.update(state, &%{&1 | response: put_in(f.readback, ["observedAt"], Fixture.iso(f.now + 62_001))})
    assert {:error, _} = Completion.verify(f.marker, stale_runtime, true, ports)
    assert Agent.get(state, & &1.files["/synthetic/release-completion-witness.json"]) == bytes
    assert :ok = Completion.require_fresh_commit(bytes, f.now + 2_000)
    assert {:error, :hgs740_completion_readback_stale} = Completion.require_fresh_commit(bytes, f.now + 62_001)
  end

  test "partial release files, unreadable evidence and missing terminal witness never fall back to legacy" do
    f = Fixture.fixture()
    {runtime, ports, state} = coordinator(f)
    Agent.update(state, &%{&1 | files: %{}})
    assert :legacy = Completion.verify(f.marker, runtime, true, ports)
    Agent.update(state, &%{&1 | files: %{"/synthetic/release-only-attempt.json" => "partial"}})
    assert {:error, _} = Completion.verify(f.marker, runtime, true, ports)
    assert Agent.get(state, & &1.readbacks) == 0
    denied = %{ports | read: fn _ -> {:error, :untrusted_root_file} end}
    assert {:error, _} = Completion.verify(f.marker, runtime, true, denied)
    Agent.update(state, &%{&1 | files: %{}})
    terminal = put_in(f.marker, ["providerReceipt"], %{"recoveryId" => "hgs740-release:attempt"})
    assert {:error, _} = Completion.verify(terminal, runtime, false, ports)
  end

  defp signed(f), do: Bundle.verify(f.bundle, f.bundle["binding"], f.snapshot.candidate_bytes, f.local, f.provider, f.local["approverRef"], f.public, f.now + 500)

  defp coordinator(f) do
    files = %{
      "/synthetic/transaction.json" => f.snapshot.marker_bytes,
      "/synthetic/local-transition-candidate.json" => f.snapshot.candidate_bytes,
      "/synthetic/reconciliation/epoch-5/candidate.json" => f.snapshot.observation_bytes,
      "/synthetic/reconciliation/epoch-5/confirmed-root-envelope.json" => f.snapshot.proof_bytes,
      "/synthetic/reconciliation/epoch-5/manifest.json" => f.snapshot.manifest_bytes,
      "/synthetic/release-only-bundle.json" => Fixture.canonical(f.bundle),
      "/synthetic/local-transition-receipt.json" => f.bundle["receipt"],
      "/synthetic/release-only-attestation.json" => f.bundle["attestation"],
      "/synthetic/release-only-attempt.json" => Fixture.decode(f.bundle["authorization"]) |> Fixture.canonical()
    }

    {:ok, state} = Agent.start_link(fn -> %{files: files, creates: 0, readbacks: 0, response: f.readback} end)

    runtime = %{
      pool_key: "hypergrid-gitops",
      host_ops: %{
        marker_directory: fn _ -> "/synthetic" end,
        now_ms: fn -> f.now + 2_000 end,
        read_public_key: fn -> {:ok, f.public} end,
        read_release_history: fn _ -> {:ok, f.history} end,
        read_confirmed_release: fn _, _ ->
          Agent.get_and_update(state, fn s ->
            result = {:ok, s.response}
            {result, %{s | readbacks: s.readbacks + 1}}
          end)
        end
      }
    }

    ports = %{
      read: fn path ->
        case Agent.get(state, &Map.fetch(&1.files, path)) do
          {:ok, bytes} -> {:ok, bytes}
          :error -> {:error, :enoent}
        end
      end,
      inputs: fn _ -> {:ok, "/synthetic/reconciliation/epoch-5"} end,
      journal: fn _, _, _ -> {:ok, f.bundle["binding"]["postimages"]["claimJournalSHA256"]} end,
      create: fn path, bytes ->
        Agent.get_and_update(state, fn s ->
          files = Map.put_new(s.files, path, bytes)
          {:ok, %{s | files: files, creates: s.creates + 1}}
        end)
      end
    }

    {runtime, ports, state}
  end
end
