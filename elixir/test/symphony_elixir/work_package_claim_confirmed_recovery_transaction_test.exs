defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransactionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransaction, as: Transaction

  test "WAL replay completes a crash after any partial prefix of state writes" do
    preimages = ["journal-before", "fence-before", "graph-before"]
    postimages = ["journal-after", "fence-after", "graph-after"]
    hashes = Enum.map(preimages, &sha256/1)

    for crash_after <- 0..2 do
      partially_applied =
        Enum.with_index(Enum.zip(preimages, postimages))
        |> Enum.map(fn {{before, after_image}, index} ->
          if index < crash_after, do: after_image, else: before
        end)

      replayed =
        Enum.zip([partially_applied, hashes, postimages])
        |> Enum.map(fn {current, preimage_hash, postimage} ->
          case Transaction.classify_image(current, preimage_hash, postimage) do
            :write -> postimage
            :already_applied -> current
          end
        end)

      assert replayed == postimages

      assert Enum.all?(Enum.zip(replayed, postimages), fn {actual, expected} ->
               Transaction.classify_image(actual, sha256("stale"), expected) == :already_applied
             end)
    end
  end

  test "WAL replay rejects a contradictory or missing preimage" do
    postimage = "exact-postimage"

    assert {:error, :transaction_target_conflict} =
             Transaction.classify_image("changed-by-another-writer", sha256("expected-preimage"), postimage)

    assert {:error, :transaction_target_conflict} = Transaction.classify_image(nil, sha256("before"), postimage)
  end

  test "WAL replay resumes each partial state-write prefix and skips exact postimages" do
    preimages = %{"journal" => "journal-before", "fence" => "fence-before", "graph" => "graph-before"}
    postimages = %{"journal" => "journal-after", "fence" => "fence-after", "graph" => "graph-after"}
    preimage_hashes = Map.new(preimages, fn {name, bytes} -> {name, sha256(bytes)} end)

    for crash_after <- 0..3 do
      {:ok, state} = Agent.start_link(fn -> {preimages, 0} end)

      first_attempt =
        run_replay(state, preimage_hashes, postimages, fn writes -> writes < crash_after end)

      if crash_after < 3 do
        assert {:error, :synthetic_crash} = first_attempt
      else
        assert :ok = first_attempt
      end

      assert :ok = run_replay(state, preimage_hashes, postimages, fn _writes -> true end)
      {replayed, writes_after_replay} = Agent.get(state, & &1)
      assert replayed == postimages

      assert :ok = run_replay(state, preimage_hashes, postimages, fn _writes -> true end)
      assert {^replayed, ^writes_after_replay} = Agent.get(state, & &1)
      Agent.stop(state)
    end
  end

  test "WAL replay repairs ownership after a crash between postimage save and metadata restore" do
    {:ok, state} = Agent.start_link(fn -> %{bytes: "before", uid: 1001, gid: 1001, mode: 0o600} end)
    preimage_sha = sha256("before")

    assert {:error, :synthetic_crash_after_save} =
             Transaction.apply_image("before", preimage_sha, "after", fn bytes ->
               Agent.update(state, fn image -> %{image | bytes: bytes, uid: 0, gid: 0, mode: 0o600} end)
               {:error, :synthetic_crash_after_save}
             end)

    transient = Agent.get(state, & &1)
    assert transient.bytes == "after" and transient.uid == 0

    assert :ok =
             Transaction.apply_image(transient.bytes, preimage_sha, "after", fn bytes ->
               Agent.update(state, fn image -> %{image | bytes: bytes, uid: 1001, gid: 1001, mode: 0o600} end)
               :ok
             end)

    assert %{bytes: "after", uid: 1001, gid: 1001, mode: 0o600} = Agent.get(state, & &1)
    Agent.stop(state)
  end

  test "startup may pass an untouched tree but denies evidence without its write-ahead marker" do
    assert :ok = Transaction.pre_marker_startup_policy(false)
    assert {:error, :hgs740_transaction_marker_missing} = Transaction.pre_marker_startup_policy(true)

    assert :ok = Transaction.no_marker_startup_policy("unrelated-issue", false)

    assert {:error, :hgs740_transaction_marker_missing} =
             Transaction.no_marker_startup_policy("unrelated-issue", true)

    assert {:error, :hgs740_transaction_marker_missing} =
             Transaction.no_marker_startup_policy("f77e349e-21d9-4bdf-bad3-ce08b302e7e8", false)
  end

  test "root entry points reject malformed arguments before any filesystem action" do
    assert {:error, :invalid_confirmed_recovery_request} = Transaction.apply(nil, "midgard", "/workflow.md", "nonce")
    assert {:error, :invalid_hgs740_completion_request} = Transaction.complete("issue", :midgard, "/workflow.md")
    assert {:error, :invalid_hgs740_startup_request} = Transaction.verify_startup("/workflow.md", nil)
    assert Transaction.marker_directory("issue") == "/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery/issue/generation-2"
  end

  test "candidate decoder accepts only exact insertion-order Python JSON bytes" do
    canonical = ~S({"z":1,"a":{"b":2}})
    assert {:ok, %{"z" => 1, "a" => %{"b" => 2}}} = Transaction.decode_candidate_bytes(canonical)
    assert {:error, :invalid_candidate_json} = Transaction.decode_candidate_bytes(~S({ "z":1,"a":{"b":2}}))
    assert {:error, :invalid_candidate_json} = Transaction.decode_candidate_bytes(~S({"z":1,"z":2}))
  end

  test "provider HTTP response parsing allows transport whitespace but rejects duplicate fields" do
    assert {:ok, %{"data" => %{"state" => "prepared"}}} =
             Transaction.decode_provider_response(~S({ "data": { "state": "prepared" } }) <> "\n")

    assert {:error, :invalid_provider_response_json} =
             Transaction.decode_provider_response(~S({"data":{"state":"prepared","state":"released"}}))
  end

  test "recorded state ownership metadata is exact and private" do
    valid = %{
      "claimJournal" => %{"uid" => 1001, "gid" => 1001, "mode" => 0o600},
      "fence" => %{"uid" => 1001, "gid" => 1001, "mode" => 0o600},
      "responsibilityGraph" => %{"uid" => 1001, "gid" => 1001, "mode" => 0o600},
      "directories" => %{
        "/srv/dahlia-runner-state/workspaces/pools/midgard/.symphony" => %{
          "majorDevice" => 8,
          "minorDevice" => 1,
          "inode" => 1234,
          "uid" => 1001,
          "gid" => 1001,
          "mode" => 0o700
        }
      }
    }

    assert Transaction.valid_state_ownership?(valid)
    refute Transaction.valid_state_ownership?(put_in(valid, ["fence", "mode"], 0o644))
    refute Transaction.valid_state_ownership?(Map.delete(valid, "fence"))
    refute Transaction.valid_state_ownership?(put_in(valid, ["responsibilityGraph", "uid"], 0))
    refute Transaction.valid_state_ownership?(put_in(valid, ["directories", "/srv/dahlia-runner-state/workspaces/pools/midgard/.symphony", "inode"], -1))
  end

  test "local transition receipt requires exact candidate bytes and a timezone timestamp" do
    timestamp = "2026-09-30T12:00:00Z"

    payload =
      Map.new([
        {"assignmentDigest", "a"},
        {"assignmentSHA256", "b"},
        {"completedAt", timestamp},
        {"contractVersion", "work-package-hgs740-local-transition-receipt.v1"},
        {"evidenceRef", "c"},
        {"expected", %{}},
        {"generation", 2},
        {"issueId", "issue"},
        {"nonce", "nonce"},
        {"observationSHA256", "d"},
        {"pool", "midgard"},
        {"postconditions", %{}},
        {"postimages", %{}},
        {"preimages", %{}},
        {"proofSHA256", "e"},
        {"reservationId", "reservation"},
        {"transactionId", "nonce"}
      ])

    bytes = Evidence.canonical_json(payload)
    marker = %{"completedAt" => timestamp}

    assert Transaction.local_receipt_bytes_valid?(bytes, bytes, marker)
    refute Transaction.local_receipt_bytes_valid?(bytes, bytes <> " ", marker)

    invalid_timestamp =
      payload
      |> Map.put("completedAt", "2026-09-30T12:00:00")
      |> Evidence.canonical_json()

    refute Transaction.local_receipt_bytes_valid?(invalid_timestamp, invalid_timestamp, %{"completedAt" => "2026-09-30T12:00:00"})

    extra_field =
      payload
      |> Map.put("unexpected", "value")
      |> Evidence.canonical_json()

    refute Transaction.local_receipt_bytes_valid?(extra_field, extra_field, marker)
  end

  test "local transition receipt canonical bytes match the cross-language fixture" do
    fixture =
      ~S({"assignmentDigest":"015dc6bf0c98bab65f34341b2decb7600e7bec094adcd7a392576334eb5572de","assignmentSHA256":"7777777777777777777777777777777777777777777777777777777777777777","completedAt":"2026-09-30T12:00:00Z","contractVersion":"work-package-hgs740-local-transition-receipt.v1","evidenceRef":"sha256:dbe4d79e5afbf0dc82f3b75439c9becd8fa3668de36fa42eb50242977455b576","expected":{"companyId":"company","executionFenceToken":"11111111-2222-3333-4444-555555555555:2","generation":2,"issueId":"11111111-2222-3333-4444-555555555555","managedProjectProfileId":"profile","nonceHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","processId":"process","projectionId":"projection","repositoryRef":"hypergridau/symphony","reservationId":"reservation","responsibleDelegationId":"delegation","runnerId":"runner","runtimeLeaseId":"session","scopeKeys":["repo:hypergridau/symphony"],"sessionId":"session","workspaceId":"workspace"},"generation":2,"issueId":"11111111-2222-3333-4444-555555555555","nonce":"11111111-2222-3333-4444-555555555555","observationSHA256":"dbe4d79e5afbf0dc82f3b75439c9becd8fa3668de36fa42eb50242977455b576","pool":"midgard","postconditions":{"dispatchPhase":"recovery_pending","executionLeaseStatus":"released","executionReleaseReason":"spawn_failed","responsibilityRuntimeLeaseStatus":"released"},"postimages":{"claimJournalSHA256":"6666666666666666666666666666666666666666666666666666666666666666","fenceSHA256":"5555555555555555555555555555555555555555555555555555555555555555","responsibilityGraphSHA256":"4444444444444444444444444444444444444444444444444444444444444444"},"preimages":{"claimJournalSHA256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","fenceSHA256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","responsibilityGraphSHA256":"4444444444444444444444444444444444444444444444444444444444444444"},"proofSHA256":"a4ec297781c69857e4def5fe9fd17cecd0707d74f335ab988874ca0092123710","reservationId":"reservation","transactionId":"11111111-2222-3333-4444-555555555555"})

    assert {:ok, payload} = Jason.decode(fixture)

    payload =
      payload
      |> put_in(["postimages", "claimJournalSHA256"], String.duplicate("6", 64))
      |> put_in(["postimages", "fenceSHA256"], String.duplicate("5", 64))
      |> put_in(["postimages", "responsibilityGraphSHA256"], String.duplicate("4", 64))
      |> put_in(["preimages", "claimJournalSHA256"], String.duplicate("f", 64))
      |> put_in(["preimages", "fenceSHA256"], String.duplicate("e", 64))
      |> put_in(["preimages", "responsibilityGraphSHA256"], String.duplicate("4", 64))

    fixture = Evidence.canonical_json(payload)

    assert byte_size(fixture) == 2_050
    assert sha256(fixture) == "39d8ac6225eb4ff8e8df3f262f6c11f64b120fb4489fdd634cae1b3c97ee53a6"
  end

  test "final provider receipt is bound to the exact retained HGS719 operation proof" do
    proof = ~S({"recoveryId":"hgs719-midgard-digest","projectionId":"projection","fenceRevision":"17","oldTupleDigest":"tuple","runnerId":"runner"})

    receipt = %{
      "recoveryId" => "hgs719-midgard-digest",
      "projectionId" => "projection",
      "fenceRevision" => "17",
      "oldTupleDigest" => "tuple",
      "oldNonceHash" => String.duplicate("a", 64),
      "nextGenerationFloor" => 3,
      "confirmedAt" => "2026-09-30T12:00:00Z",
      "proofDigest" => sha256(proof),
      "projectionState" => "queued",
      "reservationState" => "released",
      "executionCapacityState" => "released",
      "scopeState" => "released"
    }

    expected = %{"projectionId" => "projection"}

    assert :ok =
             Transaction.validate_hgs719_receipt_binding(
               receipt,
               receipt,
               "hgs719-midgard-digest",
               expected,
               "tuple",
               "17",
               proof
             )

    refute :ok ==
             Transaction.validate_hgs719_receipt_binding(
               Map.put(receipt, "recoveryId", "hgs719-midgard-other"),
               receipt,
               "hgs719-midgard-digest",
               expected,
               "tuple",
               "17",
               proof
             )

    refute :ok ==
             Transaction.validate_hgs719_receipt_binding(
               Map.put(receipt, "fenceRevision", "18"),
               receipt,
               "hgs719-midgard-digest",
               expected,
               "tuple",
               "17",
               proof
             )

    refute :ok ==
             Transaction.validate_hgs719_receipt_binding(
               Map.put(receipt, "proofDigest", sha256("different proof")),
               receipt,
               "hgs719-midgard-digest",
               expected,
               "tuple",
               "17",
               proof
             )
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp run_replay(state, preimage_hashes, postimages, should_write?) do
    Enum.reduce_while(postimages, :ok, fn {name, postimage}, :ok ->
      {current_images, writes} = Agent.get(state, & &1)

      result =
        Transaction.apply_image(current_images[name], preimage_hashes[name], postimage, fn bytes ->
          persist_replay_image(state, name, current_images[name], writes, bytes, should_write?)
        end)

      case result do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp persist_replay_image(_state, _name, current, _writes, bytes, _should_write?) when current == bytes,
    do: :ok

  defp persist_replay_image(state, name, _current, writes, bytes, should_write?) do
    if should_write?.(writes) do
      Agent.update(state, fn {images, count} -> {Map.put(images, name, bytes), count + 1} end)
      :ok
    else
      {:error, :synthetic_crash}
    end
  end
end
