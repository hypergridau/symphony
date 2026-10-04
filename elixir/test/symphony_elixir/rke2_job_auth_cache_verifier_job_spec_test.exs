defmodule SymphonyElixir.RKE2JobAuthCacheVerifierJobSpecTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RKE2Job.AuthCacheVerifierJobSpec

  @digest String.duplicate("a", 64)
  @image "ghcr.io/hypergridau/symphony-worker@sha256:" <> String.duplicate("b", 64)
  @lease_id "11111111-1111-4111-8111-111111111111"
  @attempt_id "22222222-2222-4222-8222-222222222222"
  @assignment %{sha256: @digest, seat: "builder"}
  @slot %{
    slot_id: "oauth-slot-1",
    claim_name: "codex-oauth-slot-1",
    claim_uid: "pvc-uid-1",
    lease_id: @lease_id,
    assignment_sha256: @digest,
    seat: "builder"
  }
  @config %{
    namespace: "frigga",
    image: @image,
    catalog: %{"oauth-slot-1" => "codex-oauth-slot-1"},
    attempt_id: @attempt_id
  }

  test "compiles a dedicated verifier Job without broker identity or worker egress label" do
    assert {:ok, job} = AuthCacheVerifierJobSpec.compile(@assignment, @slot, @config)
    assert get_in(job, ["metadata", "name"]) == "auth-verify-" <> @attempt_id
    assert get_in(job, ["metadata", "annotations", "symphony.hypergrid.au/verifier-attempt-id"]) == @attempt_id
    assert get_in(job, ["metadata", "annotations", "symphony.hypergrid.au/codex-auth-claim-uid"]) == "pvc-uid-1"
    refute Map.has_key?(get_in(job, ["metadata", "labels"]), "app.kubernetes.io/managed-by")
    assert get_in(job, ["spec", "backoffLimit"]) == 0
    assert get_in(job, ["spec", "activeDeadlineSeconds"]) == 180

    pod_spec = get_in(job, ["spec", "template", "spec"])
    assert pod_spec["automountServiceAccountToken"] == false
    assert pod_spec["serviceAccountName"] == "default"
    assert pod_spec["securityContext"]["fsGroupChangePolicy"] == "OnRootMismatch"
    assert pod_spec["imagePullSecrets"] == [%{"name" => "ghcr-pull-secret"}]
    refute Map.has_key?(get_in(job, ["spec", "template", "metadata", "labels"]), "app.kubernetes.io/managed-by")

    [container] = pod_spec["containers"]
    assert container["image"] == @image
    assert container["args"] == ["--verify-auth-cache"]
    assert container["env"] == [%{"name" => "CODEX_HOME", "value" => "/var/lib/frigga-codex-home"}]
    assert container["securityContext"]["readOnlyRootFilesystem"] == true

    assert container["volumeMounts"] == [
             %{"name" => "codex-auth-slot", "mountPath" => "/var/lib/frigga-codex-home", "readOnly" => false},
             %{"name" => "tmp", "mountPath" => "/tmp", "readOnly" => false}
           ]

    refute Enum.any?(pod_spec["volumes"], &Map.has_key?(&1, "projected"))

    second_attempt = %{@config | attempt_id: "33333333-3333-4333-8333-333333333333"}
    assert {:ok, replay_job} = AuthCacheVerifierJobSpec.compile(@assignment, @slot, second_attempt)
    assert get_in(replay_job, ["metadata", "name"]) != get_in(job, ["metadata", "name"])
  end

  test "rejects mutable images, wrong namespace and stale slot bindings" do
    for config <- [
          %{@config | image: "ghcr.io/hypergridau/symphony-worker:latest"},
          %{@config | namespace: "default"},
          %{@config | attempt_id: "not-a-uuid"},
          %{@config | catalog: %{"oauth-slot-1" => "different-claim"}}
        ] do
      assert {:error, :invalid_auth_cache_verifier_job} = AuthCacheVerifierJobSpec.compile(@assignment, @slot, config)
    end

    stale_slot = %{@slot | assignment_sha256: String.duplicate("c", 64)}

    assert {:error, :invalid_auth_cache_verifier_job} =
             AuthCacheVerifierJobSpec.compile(@assignment, stale_slot, @config)

    assert {:error, :invalid_auth_cache_verifier_job} =
             AuthCacheVerifierJobSpec.compile(@assignment, %{@slot | lease_id: "not-a-uuid"}, @config)

    assert {:error, :invalid_auth_cache_verifier_job} =
             AuthCacheVerifierJobSpec.compile(%{@assignment | sha256: "bad"}, @slot, @config)
  end
end
