defmodule SymphonyElixir.RKE2Job.AuthCacheVerifierJobSpec do
  @moduledoc """
  Compiles a separate OAuth-cache canary Job after the assignment Job is gone.

  The trusted host must first verify that the exact PVC UID is Bound and no Pod
  consumes it. The verifier uses the OAuth cache but receives no broker or
  Kubernetes service-account token. Its result cannot release the slot until
  the host deletes this exact Job and proves a final no-consumer Pod snapshot.
  """

  alias SymphonyElixir.RKE2Job.AuthSlotSpec

  @namespace "frigga"
  @image_prefix "ghcr.io/hypergridau/symphony-worker@sha256:"
  @codex_home "/var/lib/frigga-codex-home"
  @safe_uuid ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/
  @resources %{
    "requests" => %{"cpu" => "100m", "memory" => "256Mi"},
    "limits" => %{"cpu" => "1", "memory" => "1Gi"}
  }

  @type config :: %{
          required(:namespace) => String.t(),
          required(:image) => String.t(),
          required(:catalog) => map(),
          required(:attempt_id) => String.t()
        }

  @spec compile(map(), map(), config()) :: {:ok, map()} | {:error, :invalid_auth_cache_verifier_job}
  def compile(assignment, slot, config) when is_map(assignment) and is_map(slot) and is_map(config) do
    with true <- valid_config?(config),
         true <- valid_assignment?(assignment),
         true <- valid_slot?(slot),
         {:ok, fragments} <- AuthSlotSpec.compile(assignment, slot, config.catalog, Map.get(slot, :binding_sha256)),
         true <- fragments.env != [] do
      name = "auth-verify-" <> config.attempt_id

      labels = %{
        "app.kubernetes.io/name" => "symphony-oauth-cache-verifier",
        "app.kubernetes.io/component" => "oauth-cache-verification",
        "symphony.hypergrid.au/auth-lease" => slot.lease_id
      }

      {:ok,
       %{
         "apiVersion" => "batch/v1",
         "kind" => "Job",
         "metadata" => %{
           "name" => name,
           "namespace" => @namespace,
           "labels" => labels,
           "annotations" =>
             fragments.annotations
             |> Map.put("symphony.hypergrid.au/assignment-sha256", assignment.sha256)
             |> Map.put("symphony.hypergrid.au/verifier-attempt-id", config.attempt_id)
         },
         "spec" => %{
           "parallelism" => 1,
           "completions" => 1,
           "backoffLimit" => 0,
           "activeDeadlineSeconds" => 180,
           "template" => %{
             "metadata" => %{"labels" => labels},
             "spec" => %{
               "restartPolicy" => "Never",
               "automountServiceAccountToken" => false,
               "serviceAccountName" => "default",
               "imagePullSecrets" => [%{"name" => "ghcr-pull-secret"}],
               "dnsConfig" => %{"options" => [%{"name" => "ndots", "value" => "1"}]},
               "securityContext" => %{
                 "runAsNonRoot" => true,
                 "runAsUser" => 10_001,
                 "runAsGroup" => 10_001,
                 "fsGroup" => 10_001,
                 "seccompProfile" => %{"type" => "RuntimeDefault"}
               },
               "containers" => [
                 %{
                   "name" => "oauth-cache-verifier",
                   "image" => config.image,
                   "imagePullPolicy" => "IfNotPresent",
                   "command" => ["/usr/local/bin/symphony-worker"],
                   "args" => ["--verify-auth-cache"],
                   "terminationMessagePath" => "/tmp/symphony-worker-result",
                   "terminationMessagePolicy" => "File",
                   "env" => [%{"name" => "CODEX_HOME", "value" => @codex_home}],
                   "resources" => @resources,
                   "securityContext" => %{
                     "allowPrivilegeEscalation" => false,
                     "readOnlyRootFilesystem" => true,
                     "runAsNonRoot" => true,
                     "runAsUser" => 10_001,
                     "runAsGroup" => 10_001,
                     "capabilities" => %{"drop" => ["ALL"]},
                     "seccompProfile" => %{"type" => "RuntimeDefault"}
                   },
                   "volumeMounts" => fragments.volume_mounts ++ [%{"name" => "tmp", "mountPath" => "/tmp", "readOnly" => false}]
                 }
               ],
               "volumes" => fragments.volumes ++ [%{"name" => "tmp", "emptyDir" => %{"sizeLimit" => "64Mi"}}]
             }
           }
         }
       }}
    else
      _ -> {:error, :invalid_auth_cache_verifier_job}
    end
  end

  def compile(_assignment, _slot, _config), do: {:error, :invalid_auth_cache_verifier_job}

  defp valid_config?(%{namespace: @namespace, image: @image_prefix <> digest, catalog: catalog, attempt_id: attempt_id})
       when is_map(catalog) and is_binary(attempt_id),
       do: byte_size(digest) == 64 and Regex.match?(~r/\A[a-f0-9]{64}\z/, digest) and Regex.match?(@safe_uuid, attempt_id)

  defp valid_config?(_config), do: false

  defp valid_assignment?(%{sha256: digest, seat: seat}) when is_binary(digest) and is_binary(seat),
    do: Regex.match?(~r/\A[a-f0-9]{64}\z/, digest) and byte_size(seat) in 1..64

  defp valid_assignment?(_assignment), do: false

  defp valid_slot?(%{lease_id: lease_id}) when is_binary(lease_id), do: Regex.match?(@safe_uuid, lease_id)
  defp valid_slot?(_slot), do: false
end
