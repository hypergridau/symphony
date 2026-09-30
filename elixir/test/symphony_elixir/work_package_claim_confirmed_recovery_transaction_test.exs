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

  test "startup may pass an untouched tree but denies evidence without its write-ahead marker" do
    assert :ok = Transaction.pre_marker_startup_policy(false)
    assert {:error, :hgs740_transaction_marker_missing} = Transaction.pre_marker_startup_policy(true)
  end

  test "candidate decoder accepts only exact insertion-order Python JSON bytes" do
    canonical = ~S({"z":1,"a":{"b":2}})
    assert {:ok, %{"z" => 1, "a" => %{"b" => 2}}} = Transaction.decode_candidate_bytes(canonical)
    assert {:error, :invalid_candidate_json} = Transaction.decode_candidate_bytes(~S({ "z":1,"a":{"b":2}}))
    assert {:error, :invalid_candidate_json} = Transaction.decode_candidate_bytes(~S({"z":1,"z":2}))
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
      ~S({"assignmentDigest":"015dc6bf0c98bab65f34341b2decb7600e7bec094adcd7a392576334eb5572de","assignmentSHA256":"7777777777777777777777777777777777777777777777777777777777777777","completedAt":"2026-09-30T12:00:00Z","contractVersion":"work-package-hgs740-local-transition-receipt.v1","evidenceRef":"sha256:87f6832736b623e9f3b34f9951b362cedb361a567629baf6a4f1531dc6271568","expected":{"companyId":"company","executionFenceToken":"11111111-2222-3333-4444-555555555555:2","generation":2,"issueId":"11111111-2222-3333-4444-555555555555","managedProjectProfileId":"profile","nonceHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","processId":"process","projectionId":"projection","repositoryRef":"hypergridau/symphony","reservationId":"reservation","responsibleDelegationId":"delegation","runnerId":"runner","runtimeLeaseId":"session","scopeKeys":["repo:hypergridau/symphony"],"sessionId":"session","workspaceId":"workspace"},"generation":2,"issueId":"11111111-2222-3333-4444-555555555555","nonce":"11111111-2222-3333-4444-555555555555","observationSHA256":"87f6832736b623e9f3b34f9951b362cedb361a567629baf6a4f1531dc6271568","pool":"midgard","postconditions":{"dispatchPhase":"recovery_pending","executionLeaseStatus":"released","executionReleaseReason":"spawn_failed","responsibilityRuntimeLeaseStatus":"released"},"postimages":{"claimJournalSHA256":"6666666666666666666666666666666666666666666666666666666666666666","fenceSHA256":"5555555555555555555555555555555555555555555555555555555555555555","responsibilityGraphSHA256":"4444444444444444444444444444444444444444444444444444444444444444"},"preimages":{"claimJournalSHA256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","fenceSHA256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","responsibilityGraphSHA256":"4444444444444444444444444444444444444444444444444444444444444444"},"proofSHA256":"ca67cc26ff38f56c99c9d04e386b585c7cb4d92f4423023cbe7df9b9e15c2b15","reservationId":"reservation","transactionId":"11111111-2222-3333-4444-555555555555"})

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
    assert sha256(fixture) == "8cdce746bf74bc83e78001a42abe9425c6c8111c84e42679c1bd13e9020d0421"
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
