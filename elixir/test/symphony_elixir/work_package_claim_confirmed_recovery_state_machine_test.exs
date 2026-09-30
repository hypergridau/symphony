defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryStateMachineTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryStateMachine, as: StateMachine

  test "apply orders custody, exact image replay, postimage verification, and marker phase" do
    parent = self()
    postimage = "journal-after"
    marker = %{"status" => "applying"}
    image = %{name: :journal, preimage_sha256: sha256("journal-before"), postimage_bytes: postimage}

    operations = %{
      freeze: fn ->
        send(parent, :frozen)
        :ok
      end,
      verify_pre_or_post: fn ->
        send(parent, :preimage_checked)
        :ok
      end,
      read_current: fn :journal -> "journal-before" end,
      persist: fn :journal, ^postimage, false ->
        send(parent, :postimage_saved)
        :ok
      end,
      verify_postimages: fn ->
        send(parent, :postimage_verified)
        :ok
      end,
      mark_local: fn ^marker ->
        send(parent, :phase_advanced)
        {:ok, %{"status" => "local_applied"}}
      end
    }

    assert {:ok, %{"status" => "local_applied"}} = StateMachine.apply(marker, [image], operations)
    assert_received :frozen
    assert_received :preimage_checked
    assert_received :postimage_saved
    assert_received :postimage_verified
    assert_received :phase_advanced
  end

  test "apply stops on a contradictory image and never advances the marker" do
    parent = self()
    marker = %{"status" => "applying"}
    image = %{name: :journal, preimage_sha256: sha256("expected"), postimage_bytes: "post"}

    operations = %{
      freeze: fn -> :ok end,
      verify_pre_or_post: fn -> :ok end,
      read_current: fn :journal -> "changed" end,
      persist: fn _name, _bytes, _already? ->
        send(parent, :must_not_persist)
        :ok
      end,
      verify_postimages: fn ->
        send(parent, :must_not_verify)
        :ok
      end,
      mark_local: fn _marker ->
        send(parent, :must_not_advance)
        {:ok, %{}}
      end
    }

    assert {:error, :hgs740_transaction_incomplete} = StateMachine.apply(marker, [image], operations)
    refute_received :must_not_persist
    refute_received :must_not_verify
    refute_received :must_not_advance
  end

  test "apply refuses to start unless the durable marker is in an applying phase" do
    assert {:error, :hgs740_transaction_incomplete} = StateMachine.apply(%{"status" => "complete"}, [], %{})
  end

  test "production apply sequence resumes every saved-image crash prefix" do
    images =
      Enum.map(1..3, fn number ->
        before = "state-before-#{number}"

        %{
          name: number,
          preimage_sha256: sha256(before),
          postimage_bytes: "state-after-#{number}"
        }
      end)

    for crash_after <- 0..3 do
      {:ok, state} = Agent.start_link(fn -> Enum.map(images, &{&1.name, "state-before-#{&1.name}"}) |> Map.new() end)
      marker = %{"status" => "applying"}
      operations = apply_operations(state, images, crash_after)

      assert {:error, :hgs740_transaction_incomplete} = StateMachine.apply(marker, images, operations)
      assert {:ok, %{"status" => "local_applied"}} = StateMachine.apply(marker, images, apply_operations(state, images, nil))

      assert Agent.get(state, & &1) == Map.new(images, &{&1.name, &1.postimage_bytes})
      Agent.stop(state)
    end
  end

  test "local-applied completion requires fresh checks before terminal marker write" do
    parent = self()
    marker = %{"status" => "local_applied"}
    operations = local_completion_operations(parent)

    assert {:ok, :complete} = StateMachine.complete(marker, "issue", "midgard", operations)

    assert_received :receipt_verified
    assert_received :postimages_verified
    assert_received :provider_final_checked
    assert_received :candidate_read
    assert_received :kubernetes_observed
    assert_received :local_release_verified
    assert_received :quiescence_verified
    assert_received :service_stopped
    assert_received :terminal_marker_written
  end

  test "local-applied completion denial prevents terminal marker write" do
    parent = self()

    operations =
      local_completion_operations(parent)
      |> Map.put(:observe, fn _marker, _candidate -> {:error, :job_present} end)

    assert {:error, :hgs740_completion_held_closed} =
             StateMachine.complete(%{"status" => "local_applied"}, "issue", "midgard", operations)

    assert_received :candidate_read
    refute_received :local_release_verified
    refute_received :terminal_marker_written
  end

  test "complete-marker replay restores custody only after proof and live checks" do
    parent = self()
    marker = %{"status" => "complete", "providerFinalProofSHA256" => "proof-hash", "providerJournalSHA256" => "journal-hash"}

    operations = %{
      verify_receipt: fn ^marker, "issue" ->
        send(parent, :receipt_verified)
        :ok
      end,
      provider_final: fn ^marker, false ->
        send(parent, :provider_final_checked)
        {:ok, "proof", %{"journalSHA256" => "journal-hash"}}
      end,
      digest: fn "proof" -> "proof-hash" end,
      valid_postconditions: fn ^marker, _payload ->
        send(parent, :postconditions_checked)
        :ok
      end,
      release_invariants: fn ^marker ->
        send(parent, :release_lineage_checked)
        :ok
      end,
      candidate: fn "issue", ^marker ->
        send(parent, :candidate_read)
        {:ok, :candidate}
      end,
      observe: fn ^marker, :candidate ->
        send(parent, :kubernetes_observed)
        {:ok, :fresh_absence}
      end,
      mutation_quiescent: fn ^marker ->
        send(parent, :quiescence_verified)
        :ok
      end,
      service_stopped: fn "midgard" ->
        send(parent, :service_stopped)
        :ok
      end,
      directories_frozen: fn ^marker ->
        send(parent, :custody_verified)
        :ok
      end,
      restore_directories: fn ^marker ->
        send(parent, :custody_restored)
        :ok
      end
    }

    assert {:ok, :complete} = StateMachine.complete(marker, "issue", "midgard", operations)
    assert_received :receipt_verified
    assert_received :provider_final_checked
    assert_received :postconditions_checked
    assert_received :release_lineage_checked
    assert_received :candidate_read
    assert_received :kubernetes_observed
    assert_received :quiescence_verified
    assert_received :service_stopped
    assert_received :custody_verified
    assert_received :custody_restored
  end

  test "complete-marker replay remains held when final proof binding changes" do
    parent = self()
    marker = %{"status" => "complete", "providerFinalProofSHA256" => "expected", "providerJournalSHA256" => "journal"}

    operations = %{
      verify_receipt: fn _marker, _issue -> :ok end,
      provider_final: fn _marker, _current? -> {:ok, "changed", %{"journalSHA256" => "journal"}} end,
      digest: fn "changed" -> "different" end,
      valid_postconditions: fn _marker, _payload -> :ok end,
      release_invariants: fn _marker -> :ok end,
      candidate: fn _issue, _marker ->
        send(parent, :must_not_read_candidate)
        {:ok, :candidate}
      end,
      observe: fn _marker, _candidate -> {:ok, :observed} end,
      mutation_quiescent: fn _marker -> :ok end,
      service_stopped: fn _pool -> :ok end,
      directories_frozen: fn _marker -> :ok end,
      restore_directories: fn _marker ->
        send(parent, :must_not_restore)
        :ok
      end
    }

    assert {:error, :hgs740_completion_held_closed} = StateMachine.complete(marker, "issue", "midgard", operations)
    refute_received :must_not_read_candidate
    refute_received :must_not_restore
  end

  test "startup accepts a fully revalidated completion and denies an altered final proof" do
    parent = self()
    marker = %{"pool" => "midgard", "providerFinalProofSHA256" => "proof-hash", "providerJournalSHA256" => "journal-hash"}
    operations = startup_operations(parent)

    assert :ok = StateMachine.verify_completed(marker, operations)
    assert_received :runtime_read
    assert_received :provider_final_checked
    assert_received :postconditions_checked
    assert_received :original_directories_checked
    assert_received :release_lineage_checked

    altered = Map.put(operations, :digest, fn _proof -> "altered" end)
    assert {:error, :hgs740_startup_held_closed} = StateMachine.verify_completed(marker, altered)
    refute_received :original_directories_checked
  end

  test "markerless startup policy always holds the protected issue and holds orphan evidence for others" do
    assert {:error, :hgs740_transaction_marker_missing} =
             StateMachine.no_marker_startup_policy("protected", "protected", false)

    assert :ok = StateMachine.no_marker_startup_policy("other", "protected", false)

    assert {:error, :hgs740_transaction_marker_missing} =
             StateMachine.no_marker_startup_policy("other", "protected", true)
  end

  test "marker image validation binds every encoded preimage and postimage" do
    marker = marker_image_fixture()

    assert StateMachine.valid_marker_images?(
             marker,
             "contract.v1",
             ["journal", "fence"],
             fn ownership -> ownership == %{"verified" => true} end,
             &marker["preimages"][&1],
             &sha256/1
           )

    refute StateMachine.valid_marker_images?(
             put_in(marker, ["postimages", "fence", "sha256"], sha256("wrong")),
             "contract.v1",
             ["journal", "fence"],
             fn _ownership -> true end,
             &marker["preimages"][&1],
             &sha256/1
           )

    refute StateMachine.valid_marker_images?(
             put_in(marker, ["preimageImages", "journal"], "%%%"),
             "contract.v1",
             ["journal", "fence"],
             fn _ownership -> true end,
             &marker["preimages"][&1],
             &sha256/1
           )

    refute StateMachine.valid_marker_images?(
             marker,
             "contract.v1",
             ["journal", "fence"],
             fn _ownership -> false end,
             &marker["preimages"][&1],
             &sha256/1
           )
  end

  test "completed marker postconditions bind receipt, generation, phase, and all three state hashes" do
    marker = %{
      "status" => "complete",
      "generation" => 2,
      "pool" => "midgard",
      "providerReceipt" => %{"nextGenerationFloor" => 3},
      "providerJournalSHA256" => "journal",
      "postimages" => %{
        "claimJournal" => %{"sha256" => "journal"},
        "fence" => %{"sha256" => "fence"},
        "responsibilityGraph" => %{"sha256" => "graph"}
      },
      "completionPostimages" => %{
        "claimJournalSHA256" => "journal",
        "fenceSHA256" => "fence",
        "responsibilityGraphSHA256" => "graph"
      }
    }

    payload = %{
      "localGenerationMax" => 2,
      "neverSpawned" => true,
      "receipt" => marker["providerReceipt"],
      "journalSHA256" => "journal"
    }

    assert StateMachine.valid_completed_postconditions?(marker, payload, ["midgard"])

    refute StateMachine.valid_completed_postconditions?(
             put_in(marker, ["completionPostimages", "fenceSHA256"], "altered"),
             payload,
             ["midgard"]
           )

    refute StateMachine.valid_completed_postconditions?(marker, Map.put(payload, "neverSpawned", false), ["midgard"])
    refute StateMachine.valid_completed_postconditions?(marker, put_in(payload, ["receipt", "nextGenerationFloor"], 4), ["midgard"])
  end

  defp local_completion_operations(parent) do
    %{
      verify_receipt: fn _marker, _issue ->
        send(parent, :receipt_verified)
        :ok
      end,
      verify_postimages: fn _marker ->
        send(parent, :postimages_verified)
        :ok
      end,
      provider_final: fn _marker, true ->
        send(parent, :provider_final_checked)
        {:ok, :proof, :payload}
      end,
      candidate: fn _issue, _marker ->
        send(parent, :candidate_read)
        {:ok, :candidate}
      end,
      observe: fn _marker, :candidate ->
        send(parent, :kubernetes_observed)
        {:ok, :observation}
      end,
      final_release_invariants: fn _marker, :payload ->
        send(parent, :local_release_verified)
        :ok
      end,
      mutation_quiescent: fn _marker ->
        send(parent, :quiescence_verified)
        :ok
      end,
      service_stopped: fn "midgard" ->
        send(parent, :service_stopped)
        :ok
      end,
      write_complete: fn _marker, :proof, :payload, :observation ->
        send(parent, :terminal_marker_written)
        :ok
      end
    }
  end

  defp startup_operations(parent) do
    %{
      runtime: fn "midgard" ->
        send(parent, :runtime_read)
        {:ok, :runtime}
      end,
      provider_final: fn _marker, :runtime, false ->
        send(parent, :provider_final_checked)
        {:ok, :proof, %{"journalSHA256" => "journal-hash"}}
      end,
      digest: fn :proof -> "proof-hash" end,
      valid_postconditions: fn _marker, _payload ->
        send(parent, :postconditions_checked)
        :ok
      end,
      directories_original: fn _marker, :runtime ->
        send(parent, :original_directories_checked)
        :ok
      end,
      release_invariants: fn _marker, :runtime ->
        send(parent, :release_lineage_checked)
        :ok
      end
    }
  end

  defp apply_operations(state, images, crash_after) do
    persist_count = Agent.start_link(fn -> 0 end) |> elem(1)

    %{
      freeze: fn -> :ok end,
      verify_pre_or_post: fn ->
        if crash_after == 0, do: {:error, :synthetic_crash}, else: :ok
      end,
      read_current: fn name -> Agent.get(state, &Map.fetch!(&1, name)) end,
      persist: fn name, bytes, _already_applied? ->
        Agent.update(state, &Map.put(&1, name, bytes))
        Agent.update(persist_count, &(&1 + 1))

        if crash_after && Agent.get(persist_count, & &1) == crash_after do
          {:error, :synthetic_crash}
        else
          :ok
        end
      end,
      verify_postimages: fn ->
        expected = Map.new(images, &{&1.name, &1.postimage_bytes})
        if Agent.get(state, & &1) == expected, do: :ok, else: {:error, :postimage_mismatch}
      end,
      mark_local: fn _marker -> {:ok, %{"status" => "local_applied"}} end
    }
  end

  defp marker_image_fixture do
    journal = "journal-postimage"
    fence = "fence-postimage"
    journal_preimage = "journal-preimage"
    fence_preimage = "fence-preimage"

    %{
      "contractVersion" => "contract.v1",
      "status" => "applying",
      "stateOwnership" => %{"verified" => true},
      "postimages" => %{
        "journal" => %{"bytes" => Base.url_encode64(journal, padding: false), "sha256" => sha256(journal)},
        "fence" => %{"bytes" => Base.url_encode64(fence, padding: false), "sha256" => sha256(fence)}
      },
      "preimageImages" => %{
        "journal" => Base.url_encode64(journal_preimage, padding: false),
        "fence" => Base.url_encode64(fence_preimage, padding: false)
      },
      "preimages" => %{"journal" => sha256(journal_preimage), "fence" => sha256(fence_preimage)}
    }
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
