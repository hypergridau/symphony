defmodule SymphonyElixir.RKE2JobHostAllocationContextTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.HostAllocationContext

  @image "ghcr.io/hypergridau/symphony-worker@sha256:" <> String.duplicate("a", 64)
  @lease_id "12345678-1234-4123-8123-123456789abc"
  @env %{
    "SYMPHONY_RKE2_API_SERVER" => "https://10.0.14.10:6443",
    "SYMPHONY_RKE2_CREDENTIAL_ROOT" => "/etc/symphony/frigga-kubernetes",
    "SYMPHONY_RKE2_WORKER_IMAGE" => @image,
    "SYMPHONY_RKE2_REPOSITORY_ID" => "123456789",
    "SYMPHONY_RKE2_AUTH_SLOT_ID" => "slot-one",
    "SYMPHONY_RKE2_AUTH_CLAIM_NAME" => "codex-oauth-slot-1"
  }

  test "host settings are all-or-nothing and pinned to a manifest repository" do
    manifest = %{repository_ref: "hypergridau/symphony"}
    assert :disabled = HostAllocationContext.configuration(%{}, manifest, "https://provider.example", "host-token")

    assert {:error, {:incomplete_rke2_host_context, ["SYMPHONY_RKE2_AUTH_CLAIM_NAME"]}} =
             HostAllocationContext.configuration(Map.delete(@env, "SYMPHONY_RKE2_AUTH_CLAIM_NAME"), manifest, "https://provider.example", "host-token")

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(@env, nil, "https://provider.example", "host-token")

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(Map.put(@env, "SYMPHONY_RKE2_WORKER_IMAGE", "worker:latest"), manifest, "https://provider.example", "host-token")

    assert {:ok, config} = HostAllocationContext.configuration(@env, manifest, "https://provider.example", "host-token")
    assert config.image == @image
    assert config.repository_ref == "hypergridau/symphony"
    assert config.slot_id == "slot-one"
  end

  test "prepares one exact slot-bound Job context without storing credentials in the assignment" do
    assignment = assignment()

    binding = %{
      issue_id: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      repository_ref: assignment.repository_ref,
      runner_id: assignment.seat,
      reservation_id: "reservation-one"
    }

    {:ok, base} =
      HostAllocationContext.configuration(@env, %{repository_ref: assignment.repository_ref}, "https://provider.example", "host-token")

    caller = self()

    config =
      base
      |> Map.put(:client_context_fun, fn seen, operation, key, _config ->
        send(caller, {:kube_context_requested, seen.sha256, operation, key})
        {:ok, %{synthetic: true}}
      end)
      |> Map.put(:pvc_read_fun, fn "frigga", "codex-oauth-slot-1", %{synthetic: true} ->
        {:ok,
         %{
           "apiVersion" => "v1",
           "kind" => "PersistentVolumeClaim",
           "metadata" => %{"namespace" => "frigga", "name" => "codex-oauth-slot-1", "uid" => "pvc-uid-one"},
           "status" => %{"phase" => "Bound"}
         }}
      end)
      |> Map.put(:post_fun, fn url, opts ->
        send(caller, {:slot_post, url, opts[:json]})

        {:ok,
         %Req.Response{
           status: 200,
           body: %{"data" => %{"slotId" => "slot-one", "claimName" => "codex-oauth-slot-1", "claimUid" => "pvc-uid-one", "leaseId" => @lease_id, "replayed" => false}}
         }}
      end)

    assert {:ok, context} = HostAllocationContext.prepare(assignment, binding, config)
    assert_receive {:kube_context_requested, digest, :allocate, key}
    assert digest == assignment.sha256
    assert key == digest <> ":allocate"
    assert_receive {:slot_post, url, %{assignmentDigest: ^digest, slotId: "slot-one", claimUid: "pvc-uid-one"}}
    assert String.ends_with?(url, "/reservation-one/codex-auth-slots/reserve")
    assert context.config.auth_slot.lease_id == @lease_id
    assert context.config.auth_slot.claim_uid == "pvc-uid-one"
    assert context.config.repository_id == "123456789"
    assert context.claim_binding == binding
    refute Map.has_key?(assignment, :runner_token)

    assert {:held, :rke2_host_allocation_context_unavailable} =
             HostAllocationContext.prepare(assignment, %{binding | issue_id: "another-issue"}, config)

    refute_receive {:slot_post, _, _}
  end

  defp assignment do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-host-context",
        seat: "runner-17",
        lease: %{
          issue_id: "issue-1",
          repository: "hypergridau/symphony",
          generation: 4,
          session_id: "worker:issue-1:4",
          process_id: "worker:issue-1:4"
        },
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "RKE2 Job allocation", evidence: "Synthetic host context test"},
        context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end
end
