defmodule SymphonyElixir.RKE2Job.HTTPClientTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.HTTPClient
  alias SymphonyElixir.RKE2Job.{JobSpec, Provider}

  @namespace "symphony-beta"
  @name "symphony-0123456789abcdef01234567"
  @uid "job-uid-0123456789"
  @job %{
    "apiVersion" => "batch/v1",
    "kind" => "Job",
    "metadata" => %{"name" => @name, "namespace" => @namespace}
  }

  test "create posts JSON to the exact HTTPS namespace with bearer auth" do
    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.method == "POST"
      assert conn.scheme == :https
      assert conn.host == "kubernetes.test"
      assert conn.request_path == "/apis/batch/v1/namespaces/#{@namespace}/jobs"
      assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer synthetic.test-token"]
      assert Plug.Conn.get_req_header(conn, "accept") == ["application/json"]
      assert {:ok, @job} = Plug.Conn.read_body(conn) |> decode_body()
      json_response(conn, 201, @job)
    end)

    assert {:ok, @job} = HTTPClient.create_job(@namespace, @job, context())
  end

  test "get maps only Kubernetes 404 to not found and rejects unauthorized responses" do
    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.method == "GET"
      assert conn.request_path == "/apis/batch/v1/namespaces/#{@namespace}/jobs/#{@name}"
      Req.Test.json(conn, @job)
    end)

    assert {:ok, @job} = HTTPClient.get_job(@namespace, @name, context())

    Req.Test.expect(__MODULE__, fn conn -> Plug.Conn.send_resp(conn, 404, "not found") end)
    assert {:error, :not_found} = HTTPClient.get_job(@namespace, @name, context())

    Req.Test.expect(__MODULE__, fn conn -> Plug.Conn.send_resp(conn, 401, "denied") end)
    assert {:error, {:kubernetes_http_status, 401}} = HTTPClient.get_job(@namespace, @name, context())
  end

  test "delete uses the server UID and foreground cascading cleanup" do
    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.method == "DELETE"
      assert conn.request_path == "/apis/batch/v1/namespaces/#{@namespace}/jobs/#{@name}"
      assert Plug.Conn.get_req_header(conn, "authorization") == ["Bearer synthetic.test-token"]

      assert {:ok, %{"apiVersion" => "v1", "kind" => "DeleteOptions", "preconditions" => %{"uid" => @uid}, "propagationPolicy" => "Foreground"}} =
               Plug.Conn.read_body(conn) |> decode_body()

      json_response(conn, 200, %{"kind" => "Status", "status" => "Success"})
    end)

    assert :ok = HTTPClient.delete_job(@namespace, @name, @uid, context())
  end

  test "activation uses atomic UID, resource-version and suspended-state JSON Patch tests" do
    active = put_in(@job, ["spec"], %{"suspend" => false})

    Req.Test.expect(__MODULE__, fn conn ->
      assert conn.method == "PATCH"
      assert conn.request_path == "/apis/batch/v1/namespaces/#{@namespace}/jobs/#{@name}"
      assert Plug.Conn.get_req_header(conn, "content-type") == ["application/json-patch+json"]

      assert {:ok,
              [
                %{"op" => "test", "path" => "/metadata/uid", "value" => @uid},
                %{"op" => "test", "path" => "/metadata/resourceVersion", "value" => "17"},
                %{"op" => "test", "path" => "/spec/suspend", "value" => true},
                %{"op" => "replace", "path" => "/spec/suspend", "value" => false}
              ]} = Plug.Conn.read_body(conn) |> decode_body()

      json_response(conn, 200, active)
    end)

    assert {:ok, ^active} = HTTPClient.activate_job(@namespace, @name, @uid, "17", context())
    assert {:error, :invalid_kubernetes_job_identity} = HTTPClient.activate_job(@namespace, @name, @uid, "", context())
  end

  test "transport timeout remains distinguishable for provider reconciliation" do
    Req.Test.expect(__MODULE__, fn conn -> Req.Test.transport_error(conn, :timeout) end)

    assert {:error, :timeout} = HTTPClient.create_job(@namespace, @job, context())
  end

  test "provider reconciles an ambiguous HTTP create by reading back the exact Job" do
    assignment = assignment()
    {:ok, compiled} = JobSpec.compile(assignment, %{namespace: @namespace, image: image()})
    existing = server_defaulted_job(compiled)

    Req.Test.expect(__MODULE__, 2, fn conn ->
      case {conn.method, conn.request_path} do
        {"POST", "/apis/batch/v1/namespaces/#{@namespace}/jobs"} ->
          Req.Test.transport_error(conn, :timeout)

        {"GET", "/apis/batch/v1/namespaces/#{@namespace}/jobs/" <> _name} ->
          json_response(conn, 200, existing)
      end
    end)

    assert {:ok, ^existing} =
             Provider.ensure(assignment,
               client: HTTPClient,
               client_context: context(),
               config: %{namespace: @namespace, image: image()}
             )
  end

  test "rejects incomplete, mismatched, or insecure settings before issuing a request" do
    assert {:error, :rke2_job_client_config_invalid} = HTTPClient.get_job(@namespace, @name, %{})

    assert {:error, :rke2_job_client_config_invalid} =
             HTTPClient.get_job(@namespace, @name, Map.put(context(), :namespace, "other-namespace"))

    assert {:error, :rke2_job_client_config_invalid} =
             HTTPClient.get_job(@namespace, @name, Map.put(context(), :api_server, "http://kubernetes.test"))

    assert {:error, :rke2_job_client_config_invalid} =
             HTTPClient.get_job(@namespace, @name, Map.put(context(), :bearer_token, " "))

    assert {:error, :rke2_job_client_config_invalid} =
             HTTPClient.get_job(@namespace, @name, Map.put(context(), :ca_certfile, "missing-ca.pem"))

    assert {:error, :rke2_job_client_config_invalid} =
             HTTPClient.get_job(@namespace, @name, Map.put(context(), :timeout_ms, 30_001))
  end

  test "rejects malformed identities and malformed successful API bodies" do
    assert {:error, :invalid_kubernetes_job_identity} = HTTPClient.get_job(@namespace, "../jobs", context())
    assert {:error, :invalid_kubernetes_job_identity} = HTTPClient.delete_job(@namespace, @name, "", context())

    Req.Test.expect(__MODULE__, fn conn -> json_response(conn, 200, %{"kind" => "Status"}) end)
    assert {:error, :invalid_kubernetes_job_response} = HTTPClient.get_job(@namespace, @name, context())

    Req.Test.expect(__MODULE__, fn conn -> json_response(conn, 200, %{"kind" => "Status"}) end)

    assert {:error, :invalid_kubernetes_delete_response} =
             HTTPClient.delete_job(@namespace, @name, @uid, context())
  end

  defp context(overrides \\ %{}) do
    Map.merge(
      %{
        api_server: "https://kubernetes.test",
        namespace: @namespace,
        bearer_token: "synthetic.test-token",
        ca_certfile: __ENV__.file,
        timeout_ms: 1_000,
        test_plug: __MODULE__
      },
      overrides
    )
  end

  defp assignment do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-rke2-http-client",
        seat: "runner-17",
        lease: %{issue_id: "issue-1", repository: "hypergridau/symphony", generation: 4, session_id: "worker:issue-1:4", process_id: "worker:issue-1:4"},
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Job client", evidence: "Fake HTTP coverage"},
        context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end

  defp image, do: "registry.example/symphony-worker@sha256:" <> String.duplicate("a", 64)

  defp server_defaulted_job(job) do
    uid = "uid-01234567"
    name = job["metadata"]["name"]
    generated = %{"batch.kubernetes.io/controller-uid" => uid, "batch.kubernetes.io/job-name" => name}

    job
    |> put_in(["metadata", "uid"], uid)
    |> put_in(["metadata", "resourceVersion"], "17")
    |> put_in(["metadata", "generation"], 1)
    |> put_in(["metadata", "creationTimestamp"], "2026-09-26T00:00:00Z")
    |> put_in(["metadata", "managedFields"], [%{"manager" => "kube-controller-manager", "operation" => "Update"}])
    |> put_in(["metadata", "labels"], Map.merge(job["metadata"]["labels"], generated))
    |> put_in(["metadata", "annotations"], Map.put(job["metadata"]["annotations"], "batch.kubernetes.io/job-tracking", ""))
    |> put_in(["spec", "completions"], 1)
    |> put_in(["spec", "parallelism"], 1)
    |> put_in(["spec", "completionMode"], "NonIndexed")
    |> put_in(["spec", "manualSelector"], false)
    |> put_in(["spec", "suspend"], true)
    |> put_in(["spec", "podReplacementPolicy"], "TerminatingOrFailed")
    |> put_in(["spec", "selector"], %{"matchLabels" => %{"batch.kubernetes.io/controller-uid" => uid}})
    |> put_in(["spec", "template", "metadata", "creationTimestamp"], nil)
    |> put_in(["spec", "template", "metadata", "labels"], Map.merge(job["spec"]["template"]["metadata"]["labels"], generated))
    |> put_in(["spec", "template", "spec", "dnsPolicy"], "ClusterFirst")
    |> put_in(["spec", "template", "spec", "schedulerName"], "default-scheduler")
    |> put_in(["spec", "template", "spec", "terminationGracePeriodSeconds"], 30)
    |> put_in(["spec", "template", "spec", "enableServiceLinks"], true)
    |> put_in(["spec", "template", "spec", "preemptionPolicy"], "PreemptLowerPriority")
    |> put_in(["spec", "template", "spec", "serviceAccountName"], "default")
    |> put_in(["spec", "template", "spec", "containers", Access.at(0), "terminationMessagePath"], "/dev/termination-log")
    |> put_in(["spec", "template", "spec", "containers", Access.at(0), "terminationMessagePolicy"], "File")
  end

  defp decode_body({:ok, body, _conn}), do: Jason.decode(body)
  defp decode_body({:more, body, _conn}), do: Jason.decode(body)

  defp json_response(conn, status, body) do
    conn
    |> Plug.Conn.put_resp_header("content-type", "application/json")
    |> Plug.Conn.send_resp(status, Jason.encode!(body))
  end
end
