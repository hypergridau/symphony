defmodule SymphonyElixir.RKE2JobDahliaAuthSlotLeaseGuardTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RKE2Job.DahliaAuthSlotLeaseGuard

  @digest String.duplicate("a", 64)
  @lease_id "12345678-1234-4123-8123-123456789abc"
  @slot %{slot_id: "slot-one", claim_name: "codex-home-one", lease_id: @lease_id, assignment_sha256: @digest}
  @assignment %{sha256: @digest}
  @allocation %{id: "rke2job:v1:exact-allocation"}

  test "checks the selected lease and exact host API responses" do
    caller = self()

    post = fn url, opts ->
      send(caller, {:post, url, opts})

      data =
        cond do
          String.ends_with?(url, "/reserve") ->
            %{"leaseId" => @lease_id, "slotId" => "slot-one", "claimName" => "codex-home-one", "replayed" => true}

          String.ends_with?(url, "/bind-job") ->
            %{"bound" => true}

          String.ends_with?(url, "/authorize") ->
            %{"authorized" => true}
        end

      {:ok, %Req.Response{status: 200, body: %{"data" => data}}}
    end

    context = %{base_url: "https://dahlia.example/", runner_token: "host-only-token", reservation_id: "reservation-one", post_fun: post}

    assert :ok = DahliaAuthSlotLeaseGuard.reserve(@slot, @assignment, context)
    assert_receive {:post, "https://dahlia.example/runner/v1/verified-assignments/reservation-one/codex-auth-slots/reserve", reserve_opts}
    assert reserve_opts[:json] == %{assignmentDigest: @digest, slotId: "slot-one"}
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
  end

  test "holds wrong lease, response, missing configuration, and release" do
    context = %{
      base_url: "https://dahlia.example",
      runner_token: "token",
      reservation_id: "reservation-one",
      post_fun: fn _url, _opts ->
        {:ok, %Req.Response{status: 200, body: %{"data" => %{"leaseId" => "another", "slotId" => "slot-one", "claimName" => "codex-home-one", "replayed" => false}}}}
      end
    }

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.reserve(@slot, @assignment, context)

    assert {:held, :codex_auth_slot_binding_unverified} =
             DahliaAuthSlotLeaseGuard.bind_uid(@slot, @assignment, @allocation, context)

    assert {:held, :codex_auth_slot_authorization_unverified} =
             DahliaAuthSlotLeaseGuard.authorize(@slot, @assignment, @allocation, context)

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.reserve(@slot, @assignment, %{})

    assert {:held, :codex_auth_slot_reservation_unverified} =
             DahliaAuthSlotLeaseGuard.reserve(%{@slot | lease_id: "../other"}, @assignment, context)

    assert {:held, :codex_auth_slot_release_verification_unavailable} =
             DahliaAuthSlotLeaseGuard.release(@slot, @assignment, @allocation, context)
  end
end
