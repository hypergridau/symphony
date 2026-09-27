defmodule SymphonyElixir.WorkerBrokerClientTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Worker.BrokerClient

  @subject %{
    assignmentDigest: String.duplicate("a", 64),
    issueUuid: "937400ab-b95e-4ddb-8adf-e28bf13c3852",
    generation: 4,
    runnerId: "runner-17",
    repositoryId: "123456789",
    repositoryRef: "hypergridau/symphony",
    branchRef: "refs/heads/codex/hgs729-canary"
  }
  @now ~U[2026-09-27 06:00:00Z]

  setup do
    token_file = Path.join(System.tmp_dir!(), "frigga-broker-token-#{System.unique_integer([:positive])}")
    File.write!(token_file, "synthetic.job.jwt\n")
    on_exit(fn -> File.rm(token_file) end)
    %{context: %{token_file: token_file, test_plug: __MODULE__}}
  end

  test "issues one short lease using the projected Job token and exact subject", %{context: context} do
    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.method == "POST"
      assert conn.host == "runner-credential-broker.dahlia.svc.cluster.local"
      assert conn.request_path == "/v1/issue"
      assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer synthetic.job.jwt"]
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      request = Jason.decode!(body)
      assert request["subject"] == Jason.decode!(Jason.encode!(@subject))
      assert request["provider"] == "github_app_installation"
      assert request["use"] == "git_checkout"
      assert request["requestedAt"] == "2026-09-27T06:00:00Z"
      assert request["notAfter"] == "2026-09-27T06:05:00Z"

      Req.Test.json(conn, %{
        "issued" => true,
        "metadata" =>
          Map.merge(request, %{
            "leaseId" => "lease-1",
            "state" => "active",
            "providerRepositoryId" => "123456789",
            "providerRepositoryRef" => "hypergridau/symphony",
            "scopeLabels" => ["contents:read"]
          })
      })
    end)

    assert {:ok, %{"leaseId" => "lease-1"}} =
             BrokerClient.issue(@subject, :git_checkout, "checkout-4", @now, 300, context)
  end

  test "redeems checkout token only for the exact repository and cutoff", %{context: context} do
    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.request_path == "/v1/checkout"

      Req.Test.json(conn, %{
        "status" => "issued",
        "installationToken" => "synthetic-installation-token",
        "repositoryRef" => "hypergridau/symphony",
        "useNotAfter" => "2026-09-27T06:05:00Z"
      })
    end)

    assert {:ok, %{installation_token: "synthetic-installation-token"}} =
             BrokerClient.checkout("lease-1", "hypergridau/symphony", "2026-09-27T06:05:00Z", context)

    Req.Test.expect(__MODULE__, fn conn ->
      Req.Test.json(conn, %{
        "status" => "issued",
        "installationToken" => "synthetic-installation-token",
        "repositoryRef" => "hypergridau/other",
        "useNotAfter" => "2026-09-27T06:05:00Z"
      })
    end)

    assert {:held, :broker_uncertain} =
             BrokerClient.checkout("lease-1", "hypergridau/symphony", "2026-09-27T06:05:00Z", context)
  end

  test "holds an issued lease response bound to another repository", %{context: context} do
    Req.Test.expect(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      request = Jason.decode!(body)

      Req.Test.json(conn, %{
        "issued" => true,
        "metadata" =>
          Map.merge(request, %{
            "leaseId" => "lease-foreign",
            "state" => "active",
            "providerRepositoryId" => "999",
            "providerRepositoryRef" => "hypergridau/symphony",
            "scopeLabels" => ["contents:read"]
          })
      })
    end)

    assert {:held, :broker_uncertain} =
             BrokerClient.issue(@subject, :git_checkout, "checkout-4", @now, 300, context)
  end

  test "issues the separate mediated-write lease with exact scopes", %{context: context} do
    Req.Test.expect(__MODULE__, fn conn ->
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      request = Jason.decode!(body)
      assert request["use"] == "git_checkout_push_pr"

      Req.Test.json(conn, %{
        "issued" => true,
        "metadata" =>
          Map.merge(request, %{
            "leaseId" => "lease-push",
            "state" => "active",
            "providerRepositoryId" => "123456789",
            "providerRepositoryRef" => "hypergridau/symphony",
            "scopeLabels" => ["contents:write", "pull_requests:write"]
          })
      })
    end)

    assert {:ok, %{"leaseId" => "lease-push"}} =
             BrokerClient.issue(@subject, :git_checkout_push_pr, "push-4", @now, 300, context)
  end

  test "mediates commit and exact PR, then confirms revocation", %{context: context} do
    old_oid = String.duplicate("a", 40)
    new_oid = String.duplicate("b", 40)
    additions = [%{path: "test/hgs729_canary_test.exs", contents: "test content"}]

    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.request_path == "/v1/commit"
      {:ok, body, conn} = Plug.Conn.read_body(conn)
      assert Jason.decode!(body)["additions"] == Jason.decode!(Jason.encode!(additions))
      Req.Test.json(conn, %{"status" => "committed", "oid" => new_oid})
    end)

    assert {:ok, ^new_oid} = BrokerClient.commit("lease-2", old_oid, "Add canary test", additions, context)

    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.request_path == "/v1/pull-request"
      Req.Test.json(conn, %{"status" => "created", "number" => 74, "url" => "https://github.com/hypergridau/symphony/pull/74"})
    end)

    assert {:ok, %{number: 74, url: "https://github.com/hypergridau/symphony/pull/74"}} =
             BrokerClient.pull_request("lease-2", "hypergridau/symphony", context)

    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.request_path == "/v1/revoke"
      Req.Test.json(conn, %{"issued" => true, "metadata" => %{"leaseId" => "lease-2", "state" => "revoked"}})
    end)

    assert :ok = BrokerClient.revoke("lease-2", context)
  end

  test "denials and uncertain responses fail closed without retries", %{context: context} do
    Req.Test.expect(__MODULE__, fn conn -> Req.Test.json(conn, %{"issued" => false, "reason" => "authority_denied"}) end)
    assert {:error, :broker_denied} = BrokerClient.issue(@subject, :git_checkout, "checkout-4", @now, 300, context)

    for reason <- ["issuance_pending", "revocation_pending", "replay"] do
      Req.Test.expect(__MODULE__, fn conn -> Req.Test.json(conn, %{"issued" => false, "reason" => reason}) end)
      assert {:held, :broker_uncertain} = BrokerClient.issue(@subject, :git_checkout, "checkout-4", @now, 300, context)
    end

    Req.Test.expect(__MODULE__, fn conn -> Plug.Conn.send_resp(conn, 503, "unavailable") end)
    assert {:held, :broker_uncertain} = BrokerClient.issue(@subject, :git_checkout, "checkout-4", @now, 300, context)

    Req.Test.expect(__MODULE__, fn conn -> Req.Test.json(conn, %{"status" => "uncertain"}) end)

    assert {:held, :broker_uncertain} =
             BrokerClient.commit("lease-2", String.duplicate("a", 40), "Add canary", [%{path: "a", contents: "b"}], context)

    assert {:error, :invalid_broker_request} = BrokerClient.issue(@subject, :git_checkout, "key", @now, 601, context)
    assert {:error, :invalid_broker_request} = BrokerClient.commit("lease-2", "bad", "Add canary", [], context)

    assert {:error, :invalid_broker_request} =
             BrokerClient.commit("lease-2", String.duplicate("a", 40), "Add canary", [nil], context)

    assert {:error, :invalid_broker_request} =
             BrokerClient.commit("lease-2", String.duplicate("a", 40), "Add canary", [%{path: "../escape", contents: "x"}], context)
  end

  test "missing projected token prevents transport", %{context: context} do
    File.rm!(context.token_file)
    assert {:held, :broker_uncertain} = BrokerClient.revoke("lease-1", context)
  end

  test "denied or pending revocation keeps cleanup held", %{context: context} do
    for reason <- ["revocation_denied", "revocation_pending"] do
      Req.Test.expect(__MODULE__, fn conn -> Req.Test.json(conn, %{"issued" => false, "reason" => reason}) end)
      assert {:held, :broker_uncertain} = BrokerClient.revoke("lease-1", context)
    end

    Req.Test.expect(__MODULE__, fn conn -> Plug.Conn.send_resp(conn, 401, "denied") end)
    assert {:held, :broker_uncertain} = BrokerClient.revoke("lease-1", context)
  end

  test "reads the projected token again after rotation", %{context: context} do
    File.write!(context.token_file, "rotated.job.jwt\n")

    Req.Test.expect(__MODULE__, fn conn ->
      assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer rotated.job.jwt"]
      Req.Test.json(conn, %{"issued" => true, "metadata" => %{"leaseId" => "lease-1", "state" => "revoked"}})
    end)

    assert :ok = BrokerClient.revoke("lease-1", context)
  end
end
