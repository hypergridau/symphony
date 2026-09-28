defmodule SymphonyElixir.RKE2JobDahliaAuthSlotLeaseGuardTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RKE2Job.DahliaAuthSlotLeaseGuard

  @digest String.duplicate("a", 64)
  @lease_id "12345678-1234-4123-8123-123456789abc"
  @pvc_uid "pvc-uid-one"
  @slot %{
    slot_id: "slot-one",
    claim_name: "codex-home-one",
    claim_uid: @pvc_uid,
    lease_id: @lease_id,
    assignment_sha256: @digest,
    seat: "luna-high"
  }
  @assignment %{sha256: @digest, seat: "luna-high"}
  @allocation %{id: "rke2job:v1:exact-allocation"}

  test "releases only an exact host-observed cleanup receipt accepted by Dahlia" do
    caller = self()
    if match?({:win32, _}, :os.type()), do: Process.put(:result_journal_windows_test_only, true)
    root = Path.join(System.tmp_dir!(), "symphony-slot-release-#{System.unique_integer([:positive])}")
    :ok = File.mkdir_p(root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(root, 0o700)
    on_exit(fn -> File.rm_rf(root) end)
    allocation = %{id: "rke2job:v1:" <> Base.url_encode64(Jason.encode!([1, "frigga", "job-one", "job-uid-one", @digest]), padding: false)}

    receipt = %{
      "receiptId" => "12345678-1234-4123-8123-123456789abd",
      "observedAt" => "2026-09-28T15:00:00Z",
      "namespace" => "frigga",
      "jobUid" => "job-uid-one",
      "claimName" => @slot.claim_name,
      "claimUid" => @slot.claim_uid,
      "jobAbsent" => true,
      "ownedPodsAbsent" => true,
      "claimPodsAbsent" => true,
      "podListResourceVersion" => "101",
      "claimPodListResourceVersion" => "102",
      "authCacheStatus" => "codex_login_status_authenticated",
      "authCacheBytes" => 100
    }

    context = %{
      base_url: "https://dahlia.example",
      runner_token: "host-only-token",
      reservation_id: "reservation-one",
      result_journal_root: root,
      cleanup_receipt_fun: fn slot, assignment, observed_allocation ->
        assert slot == @slot
        assert assignment == @assignment
        assert observed_allocation == allocation
        {:ok, receipt}
      end,
      post_fun: fn url, opts ->
        send(caller, {:release_post, url, opts})
        {:ok, %Req.Response{status: 200, body: %{"data" => %{"released" => true}}}}
      end
    }

    mismatch = %{context | cleanup_receipt_fun: fn _, _, _ -> {:ok, %{receipt | "jobUid" => "other"}} end}

    assert {:held, :codex_auth_slot_release_verification_unavailable} =
             DahliaAuthSlotLeaseGuard.release(@slot, @assignment, allocation, mismatch)

    refute_receive {:release_post, _, _}

    malformed = %{context | cleanup_receipt_fun: fn _, _, _ -> {:ok, %{receipt | "receiptId" => "invalid"}} end}

    assert {:held, :codex_auth_slot_release_verification_unavailable} =
             DahliaAuthSlotLeaseGuard.release(@slot, @assignment, allocation, malformed)

    refute_receive {:release_post, _, _}

    timed_out = %{
      context
      | post_fun: fn url, opts ->
          send(caller, {:release_post, url, opts})
          {:error, :timeout}
        end
    }

    assert {:held, :codex_auth_slot_release_verification_unavailable} =
             DahliaAuthSlotLeaseGuard.release(@slot, @assignment, allocation, timed_out)

    assert_receive {:release_post, url, opts}
    assert String.ends_with?(url, "/#{@lease_id}/release")
    assert opts[:json] == %{allocationId: allocation.id, receipt: receipt}

    replay = %{context | cleanup_receipt_fun: fn _, _, _ -> flunk("observer must not mint a second receipt") end}
    assert :ok = DahliaAuthSlotLeaseGuard.release(@slot, @assignment, allocation, replay)
    assert_receive {:release_post, ^url, replay_opts}
    assert replay_opts[:json] == opts[:json]

    denied = %{context | post_fun: fn _, _ -> {:ok, %Req.Response{status: 409, body: %{}}} end}

    assert {:held, :codex_auth_slot_release_verification_unavailable} =
             DahliaAuthSlotLeaseGuard.release(@slot, @assignment, allocation, denied)
  end

  test "refreshes a rejected stale receipt only after Dahlia confirms the lease remains bound" do
    caller = self()
    if match?({:win32, _}, :os.type()), do: Process.put(:result_journal_windows_test_only, true)
    root = Path.join(System.tmp_dir!(), "symphony-stale-slot-#{System.unique_integer([:positive])}")
    :ok = File.mkdir_p(root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(root, 0o700)
    on_exit(fn -> File.rm_rf(root) end)
    allocation = %{id: "rke2job:v1:" <> Base.url_encode64(Jason.encode!([1, "frigga", "job-one", "job-uid-one", @digest]), padding: false)}

    old = cleanup_receipt("12345678-1234-4123-8123-123456789abd", "2026-09-28T14:00:00Z")
    fresh = cleanup_receipt("12345678-1234-4123-8123-123456789abe", "2026-09-28T15:00:00Z")

    post = fn url, opts ->
      send(caller, {:request, url, opts[:json]})

      cond do
        String.ends_with?(url, "/verify-bound") ->
          {:ok, %Req.Response{status: 200, body: %{"data" => %{"bound" => true}}}}

        opts[:json].receipt == old ->
          {:ok, %Req.Response{status: 409, body: %{}}}

        opts[:json].receipt == fresh ->
          {:ok, %Req.Response{status: 200, body: %{"data" => %{"released" => true}}}}
      end
    end

    context = %{
      base_url: "https://dahlia.example",
      runner_token: "host-only-token",
      reservation_id: "reservation-one",
      result_journal_root: root,
      cleanup_receipt_fun: fn _, _, _ -> {:ok, old} end,
      post_fun: fn _, _ -> {:error, :timeout} end
    }

    assert {:held, :codex_auth_slot_release_verification_unavailable} =
             DahliaAuthSlotLeaseGuard.release(@slot, @assignment, allocation, context)

    refreshed = %{context | cleanup_receipt_fun: fn _, _, _ -> {:ok, fresh} end, post_fun: post}
    assert :ok = DahliaAuthSlotLeaseGuard.release(@slot, @assignment, allocation, refreshed)
    assert_receive {:request, old_url, %{receipt: ^old}}
    assert String.ends_with?(old_url, "/release")
    assert_receive {:request, verify_url, %{allocationId: allocation_id}}
    assert allocation_id == allocation.id
    assert String.ends_with?(verify_url, "/verify-bound")
    assert_receive {:request, fresh_url, %{receipt: ^fresh}}
    assert String.ends_with?(fresh_url, "/release")

    replay = %{refreshed | cleanup_receipt_fun: fn _, _, _ -> flunk("saved receipts must replay") end}
    assert :ok = DahliaAuthSlotLeaseGuard.release(@slot, @assignment, allocation, replay)
    assert_receive {:request, _, %{receipt: ^fresh}}
  end

  test "checks the selected lease and exact host API responses" do
    caller = self()

    post = fn url, opts ->
      send(caller, {:post, url, opts})

      data =
        cond do
          String.ends_with?(url, "/reserve") ->
            %{
              "leaseId" => @lease_id,
              "slotId" => "slot-one",
              "claimName" => "codex-home-one",
              "claimUid" => @pvc_uid,
              "replayed" => true
            }

          String.ends_with?(url, "/bind-job") ->
            %{"bound" => true}

          String.ends_with?(url, "/authorize") ->
            %{"authorized" => true}

          String.ends_with?(url, "/verify-bound") ->
            %{"bound" => true}
        end

      {:ok, %Req.Response{status: 200, body: %{"data" => data}}}
    end

    context = %{
      base_url: "https://dahlia.example/",
      runner_token: "host-only-token",
      reservation_id: "reservation-one",
      post_fun: post,
      pvc_namespace: "frigga",
      pvc_read_fun: &read_pvc/3
    }

    assert :ok = DahliaAuthSlotLeaseGuard.reserve(@slot, @assignment, context)
    assert_receive {:post, reserve_url, reserve_opts}
    assert String.ends_with?(reserve_url, "/reservation-one/codex-auth-slots/reserve")
    assert reserve_opts[:json] == %{assignmentDigest: @digest, slotId: "slot-one", claimUid: @pvc_uid}
    assert reserve_opts[:headers] == [{"authorization", "Bearer host-only-token"}]
    assert reserve_opts[:retry] == false
    assert reserve_opts[:redirect] == false

    assert :ok = DahliaAuthSlotLeaseGuard.bind_uid(@slot, @assignment, @allocation, context)
    assert_receive {:post, bind_url, bind_opts}
    assert String.ends_with?(bind_url, "/#{@lease_id}/bind-job")
    assert bind_opts[:json] == %{allocationId: @allocation.id}

    assert :ok = DahliaAuthSlotLeaseGuard.authorize(@slot, @assignment, @allocation, context)
    assert_receive {:post, authorize_url, authorize_opts}
    assert String.ends_with?(authorize_url, "/#{@lease_id}/authorize")
    assert authorize_opts[:json] == %{allocationId: @allocation.id}

    assert :ok = DahliaAuthSlotLeaseGuard.verify_bound(@slot, @assignment, @allocation, context)
    assert_receive {:post, verify_url, verify_opts}
    assert String.ends_with?(verify_url, "/#{@lease_id}/verify-bound")
    assert verify_opts[:json] == %{allocationId: @allocation.id}
  end

  test "holds wrong lease, response, missing configuration, and release" do
    context = %{
      base_url: "https://dahlia.example",
      runner_token: "token",
      reservation_id: "reservation-one",
      pvc_namespace: "frigga",
      pvc_read_fun: &read_pvc/3,
      post_fun: fn _url, _opts ->
        data = %{
          "leaseId" => "another",
          "slotId" => "slot-one",
          "claimName" => "codex-home-one",
          "claimUid" => @pvc_uid,
          "replayed" => false
        }

        {:ok, %Req.Response{status: 200, body: %{"data" => data}}}
      end
    }

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.reserve(@slot, @assignment, context)

    assert {:held, :codex_auth_slot_binding_unverified} =
             DahliaAuthSlotLeaseGuard.bind_uid(@slot, @assignment, @allocation, context)

    assert {:held, :codex_auth_slot_authorization_unverified} =
             DahliaAuthSlotLeaseGuard.authorize(@slot, @assignment, @allocation, context)

    assert {:held, :codex_auth_slot_bound_verification_unverified} =
             DahliaAuthSlotLeaseGuard.verify_bound(@slot, @assignment, @allocation, context)

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.reserve(@slot, @assignment, %{})

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.reserve(%{@slot | lease_id: "../other"}, @assignment, context)

    replaced = %{
      context
      | pvc_read_fun: fn _, _, _ ->
          {:ok,
           %{
             "apiVersion" => "v1",
             "kind" => "PersistentVolumeClaim",
             "metadata" => %{"namespace" => "frigga", "name" => "codex-home-one", "uid" => "replacement-pvc"},
             "status" => %{"phase" => "Bound"}
           }}
        end
    }

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.reserve(@slot, @assignment, replaced)

    assert {:held, :codex_auth_slot_authorization_unverified} =
             DahliaAuthSlotLeaseGuard.authorize(@slot, @assignment, @allocation, replaced)

    assert {:held, :codex_auth_slot_release_verification_unavailable} =
             DahliaAuthSlotLeaseGuard.release(@slot, @assignment, @allocation, context)
  end

  test "verify-bound requires the exact binding response and assignment" do
    context = %{
      base_url: "https://dahlia.example",
      runner_token: "token",
      reservation_id: "reservation-one",
      post_fun: fn _url, _opts ->
        {:ok, %Req.Response{status: 200, body: %{"data" => %{"bound" => true}}}}
      end
    }

    assert :ok = DahliaAuthSlotLeaseGuard.verify_bound(@slot, @assignment, @allocation, context)

    mismatch = %{
      context
      | post_fun: fn _url, _opts ->
          {:ok, %Req.Response{status: 200, body: %{"data" => %{"bound" => false}}}}
        end
    }

    assert {:held, :codex_auth_slot_bound_verification_unverified} =
             DahliaAuthSlotLeaseGuard.verify_bound(@slot, @assignment, @allocation, mismatch)

    extra_field = %{
      context
      | post_fun: fn _url, _opts ->
          {:ok, %Req.Response{status: 200, body: %{"data" => %{"bound" => true, "replayed" => true}}}}
        end
    }

    assert {:held, :codex_auth_slot_bound_verification_unverified} =
             DahliaAuthSlotLeaseGuard.verify_bound(@slot, @assignment, @allocation, extra_field)

    assert {:held, :codex_auth_slot_bound_verification_unverified} =
             DahliaAuthSlotLeaseGuard.verify_bound(
               %{@slot | assignment_sha256: String.duplicate("b", 64)},
               @assignment,
               @allocation,
               context
             )
  end

  test "prepares only Dahlia's exact lease and the trusted one-to-one slot catalog" do
    caller = self()

    post = fn url, opts ->
      send(caller, {:post, url, opts})

      data = %{
        "leaseId" => @lease_id,
        "slotId" => "slot-one",
        "claimName" => "codex-home-one",
        "claimUid" => @pvc_uid,
        "replayed" => false
      }

      {:ok, %Req.Response{status: 200, body: %{"data" => data}}}
    end

    context = %{
      base_url: "https://dahlia.example",
      runner_token: "host-only-token",
      reservation_id: "reservation-one",
      pvc_namespace: "frigga",
      pvc_read_fun: &read_pvc/3,
      post_fun: post
    }

    catalog = %{"slot-one" => "codex-home-one"}

    assert {:ok, slot} = DahliaAuthSlotLeaseGuard.prepare_slot(@assignment, "slot-one", catalog, context)
    assert slot == @slot
    assert_receive {:post, url, opts}
    assert String.ends_with?(url, "/reservation-one/codex-auth-slots/reserve")
    assert opts[:json] == %{assignmentDigest: @digest, slotId: "slot-one", claimUid: @pvc_uid}

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.prepare_slot(@assignment, "slot-one", %{"slot-one" => "other-claim"}, context)

    refute_receive {:post, _, _}

    aliased_catalog = %{"slot-one" => "codex-home-one", "slot-two" => "codex-home-one"}

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.prepare_slot(@assignment, "slot-one", aliased_catalog, context)

    refute_receive {:post, _, _}

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.prepare_slot(@assignment, "slot-one", catalog, %{})

    unbound = %{
      context
      | pvc_read_fun: fn _, _, _ ->
          metadata = %{"namespace" => "frigga", "name" => "codex-home-one", "uid" => @pvc_uid}

          {:ok,
           %{
             "apiVersion" => "v1",
             "kind" => "PersistentVolumeClaim",
             "metadata" => metadata,
             "status" => %{"phase" => "Pending"}
           }}
        end
    }

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.prepare_slot(@assignment, "slot-one", catalog, unbound)

    deleting = %{
      context
      | pvc_read_fun: fn _, _, _ ->
          metadata = %{
            "namespace" => "frigga",
            "name" => "codex-home-one",
            "uid" => @pvc_uid,
            "deletionTimestamp" => "2026-09-27T00:00:00Z"
          }

          {:ok,
           %{
             "apiVersion" => "v1",
             "kind" => "PersistentVolumeClaim",
             "metadata" => metadata,
             "status" => %{"phase" => "Bound"}
           }}
        end
    }

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.prepare_slot(@assignment, "slot-one", catalog, deleting)
  end

  defp read_pvc("frigga", "codex-home-one", _context) do
    metadata = %{"namespace" => "frigga", "name" => "codex-home-one", "uid" => @pvc_uid}

    pvc = %{
      "apiVersion" => "v1",
      "kind" => "PersistentVolumeClaim",
      "metadata" => metadata,
      "status" => %{"phase" => "Bound"}
    }

    {:ok, pvc}
  end

  defp read_pvc(_namespace, _claim_name, _context), do: {:error, :not_found}

  defp cleanup_receipt(receipt_id, observed_at) do
    %{
      "receiptId" => receipt_id,
      "observedAt" => observed_at,
      "namespace" => "frigga",
      "jobUid" => "job-uid-one",
      "claimName" => @slot.claim_name,
      "claimUid" => @slot.claim_uid,
      "jobAbsent" => true,
      "ownedPodsAbsent" => true,
      "claimPodsAbsent" => true,
      "podListResourceVersion" => "101",
      "claimPodListResourceVersion" => "102",
      "authCacheStatus" => "codex_login_status_authenticated",
      "authCacheBytes" => 100
    }
  end
end
