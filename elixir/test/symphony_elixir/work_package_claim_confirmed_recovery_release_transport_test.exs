defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseTransportTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseRuntime, as: Runtime
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseTransport, as: Transport
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost, as: Host

  test "transport binds fixed origin and exact phase bytes and accepts only bounded noncacheable results" do
    for mode <- [:success, :redirect, :cacheable, :malformed, :oversized, :failure] do
      plug = {__MODULE__, make_ref()}
      context = %{protocol_accepted: true, trust_enrolled: true, runner_token: "synthetic-runner", admin_token: "synthetic-admin", test_plug: plug}
      bundle = %{"binding" => %{"nonce" => "exact"}, "receipt" => "exact retained bytes"}

      Req.Test.stub(plug, fn conn ->
        assert conn.host == "dahlia.hypergrid.au"
        assert conn.scheme == :https
        assert String.ends_with?(conn.request_path, "/confirm")
        assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer synthetic-runner"]
        {:ok, bytes, conn} = Plug.Conn.read_body(conn)
        assert Jason.decode!(bytes) == %{"bundle" => bundle, "providerApprovalId" => "approval-provider"}
        conn = Plug.Conn.put_resp_header(conn, "cache-control", if(mode == :cacheable, do: "public", else: "no-store"))

        case mode do
          :redirect -> conn |> Plug.Conn.put_resp_header("location", "https://untrusted.test") |> Plug.Conn.send_resp(302, "")
          :malformed -> Plug.Conn.send_resp(conn, 200, "not json")
          :oversized -> Plug.Conn.send_resp(conn, 200, String.duplicate("x", 262_145))
          :failure -> Plug.Conn.send_resp(conn, 503, "held")
          _ -> Req.Test.json(conn, %{"data" => %{"receipt" => "durable"}})
        end
      end)

      result = Transport.consume(:confirm, bundle, "approval-provider", context)
      expected = if mode == :success, do: {:ok, %{"receipt" => "durable"}}, else: {:error, :hgs740_provider_transport_held_closed}
      assert result == expected
    end
  end

  test "normal composition and missing credentials fail closed without dispatch" do
    assert {:error, :hgs740_release_protocol_not_admitted} = Transport.consume(:prepare, %{}, "approval", %{})
    admitted = %{protocol_accepted: true, trust_enrolled: true}
    assert {:error, :hgs740_provider_transport_held_closed} = Transport.approval(%{}, "approval", admitted)
    assert {:error, :hgs740_release_protocol_not_admitted} = Host.release_only("issue", "pool", "workflow", "decision", %{})
    assert {:error, :hgs740_provider_identity_unavailable} = Runtime.provider(%{}, %{})
  end

  test "runtime reuses fixed bounded private identity inputs and rejects ambiguous or changed bindings" do
    enrollment = %{protocol_accepted: true, trust_enrolled: true}
    snapshot = fn _ -> {:ok, %{marker_bytes: Jason.encode!(%{"expected" => %{"runnerId" => "runner-synthetic"}})}} end

    for env <- [
          "DAHLIA_RUNNER_ID=runner-synthetic\nDAHLIA_WORK_PACKAGE_RUNNER_TOKEN='synthetic-token'",
          "DAHLIA_RUNNER_ID=changed\nDAHLIA_WORK_PACKAGE_RUNNER_TOKEN=synthetic-token",
          "DAHLIA_RUNNER_ID=runner-synthetic",
          "DAHLIA_RUNNER_ID=runner-synthetic\nDAHLIA_RUNNER_ID=runner-synthetic",
          "DAHLIA_RUNNER_ID=runner-synthetic\nDAHLIA_WORK_PACKAGE_RUNNER_TOKEN=\"$(untrusted)\""
        ] do
      read = fn path, bound ->
        case path do
          "/srv/dahlia-runner-state/identity/managed-pools-hgs382/hypergrid-gitops.env" ->
            assert bound == 16_384
            {:ok, env}

          "/srv/dahlia-runner-state/identity/claim-recovery-hgs485/provider-admin.token" ->
            assert bound == 4096
            {:ok, "synthetic-admin\n"}
        end
      end

      result = Runtime.with_test_reads(%{pool: "hypergrid-gitops"}, enrollment, snapshot, read)

      if String.contains?(env, "'synthetic-token'") do
        assert {:ok, %{runner_token: "synthetic-token", admin_token: "synthetic-admin"}} = result
      else
        assert result == {:error, :hgs740_provider_identity_unavailable}
      end
    end

    denied = fn _path, _bound -> {:error, :untrusted_root_private_file} end

    assert {:error, :hgs740_provider_identity_unavailable} =
             Runtime.with_test_reads(%{pool: "hypergrid-gitops"}, enrollment, snapshot, denied)
  end
end
