defmodule SymphonyElixir.RKE2JobAuthCacheVerifierResultTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RKE2Job.{AuthCacheVerifierJobSpec, AuthCacheVerifierResult}

  @digest String.duplicate("a", 64)
  @image "ghcr.io/hypergridau/symphony-worker@sha256:" <> String.duplicate("b", 64)
  @assignment %{sha256: @digest, seat: "builder"}
  @slot %{
    slot_id: "oauth-slot-1",
    claim_name: "codex-oauth-slot-1",
    claim_uid: "pvc-uid-1",
    lease_id: "11111111-1111-4111-8111-111111111111",
    assignment_sha256: @digest,
    seat: "builder"
  }
  @config %{
    namespace: "frigga",
    image: @image,
    catalog: %{"oauth-slot-1" => "codex-oauth-slot-1"},
    attempt_id: "22222222-2222-4222-8222-222222222222"
  }

  test "accepts only the exact successful canary on its owned Job and Pod" do
    {expected, job, snapshot} = fixture()

    assert {:ok,
            %{
              job_uid: "verifier-job-uid",
              pod_uid: "verifier-pod-uid",
              pod_list_resource_version: "list-rv-1",
              auth_cache_status: "codex_login_status_authenticated",
              auth_cache_bytes: 12_345
            }} = AuthCacheVerifierResult.verify(expected, "verifier-job-uid", job, snapshot)
  end

  test "rejects injected work, stale ownership, competing candidates and forged success" do
    {expected, job, snapshot} = fixture()
    [pod] = snapshot.items
    verifier = &AuthCacheVerifierResult.verify(expected, "verifier-job-uid", &1, &2)

    cases = [
      {put_in(job, ["spec", "template", "spec", "containers"], get_in(job, ["spec", "template", "spec", "containers"]) ++ [%{"name" => "sidecar"}]), snapshot},
      {job, %{snapshot | items: [put_in(pod, ["spec", "initContainers"], [%{"name" => "injected"}])]}},
      {job, %{snapshot | items: [put_in(pod, ["spec", "containers", Access.at(0), "image"], "other:image")]}},
      {job, %{snapshot | items: [put_in(pod, ["spec", "containers", Access.at(0), "envFrom"], [%{"secretRef" => %{"name" => "other"}}])]}},
      {job, %{snapshot | items: [put_in(pod, ["spec", "containers", Access.at(0), "securityContext", "privileged"], true)]}},
      {job, %{snapshot | items: [put_in(pod, ["metadata", "ownerReferences", Access.at(0), "uid"], "other-uid")]}},
      {job, %{snapshot | items: [pod, pod]}},
      {job,
       %{
         snapshot
         | items: [
             put_in(
               pod,
               ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"],
               Jason.encode!(%{"schemaVersion" => 1, "authCacheStatus" => "unverified", "authCacheBytes" => 0})
             )
           ]
       }},
      {job, %{snapshot | items: [put_in(pod, ["status", "containerStatuses", Access.at(0), "state", "terminated", "exitCode"], 1)]}}
    ]

    for {altered_job, altered_snapshot} <- cases do
      assert {:held, :auth_cache_verifier_result_unverified} = verifier.(altered_job, altered_snapshot)
    end

    allowed_owner = put_in(pod, ["metadata", "ownerReferences", Access.at(0), "blockOwnerDeletion"], true)
    assert {:ok, _} = verifier.(job, %{snapshot | items: [allowed_owner]})
  end

  test "checks admitted Job identity while the verifier is still running" do
    {expected, completed, _snapshot} = fixture()
    running = Map.delete(completed, "status")

    assert AuthCacheVerifierResult.owned_job?(expected, "verifier-job-uid", running)
    refute AuthCacheVerifierResult.owned_job?(expected, "replacement-job-uid", running)

    refute AuthCacheVerifierResult.owned_job?(
             expected,
             "verifier-job-uid",
             put_in(running, ["spec", "template", "spec", "hostNetwork"], true)
           )
  end

  defp fixture do
    {:ok, expected} = AuthCacheVerifierJobSpec.compile(@assignment, @slot, @config)
    name = expected["metadata"]["name"]
    uid = "verifier-job-uid"

    job =
      expected
      |> put_in(["metadata", "uid"], uid)
      |> put_in(["metadata", "resourceVersion"], "job-rv-1")
      |> Map.put("status", %{"conditions" => [%{"type" => "Complete", "status" => "True"}]})

    pod = %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "name" => name <> "-abcde",
        "namespace" => "frigga",
        "uid" => "verifier-pod-uid",
        "resourceVersion" => "pod-rv-1",
        "labels" => expected["spec"]["template"]["metadata"]["labels"],
        "ownerReferences" => [%{"apiVersion" => "batch/v1", "kind" => "Job", "name" => name, "uid" => uid, "controller" => true}]
      },
      "spec" => expected["spec"]["template"]["spec"],
      "status" => %{
        "phase" => "Succeeded",
        "containerStatuses" => [
          %{
            "name" => "oauth-cache-verifier",
            "ready" => false,
            "state" => %{
              "terminated" => %{
                "exitCode" => 0,
                "message" => Jason.encode!(%{"schemaVersion" => 1, "authCacheStatus" => "codex_login_status_authenticated", "authCacheBytes" => 12_345})
              }
            }
          }
        ]
      }
    }

    {expected, job, %{items: [pod], resource_version: "list-rv-1"}}
  end
end
