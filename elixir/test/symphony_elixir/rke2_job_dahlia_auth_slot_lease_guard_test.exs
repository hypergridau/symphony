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
end
