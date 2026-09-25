defmodule SymphonyElixir.RKE2Job.JobSpec do
  @moduledoc """
  Pure compiler from an assignment bundle built from a signature-verified manifest entry.

  Callers must preserve that provenance. This module verifies canonical bundle integrity,
  placement, and trusted image/namespace configuration; it does not verify the manifest
  signature itself.
  """

  alias SymphonyElixir.ManagedAssignmentBundle

  @api_version "batch/v1"
  @kind "Job"
  @worker_command ["/usr/local/bin/symphony-worker"]
  @active_deadline_seconds 3_600
  @backoff_limit 0
  @ttl_seconds_after_finished 86_400
  @resources %{
    "requests" => %{"cpu" => "500m", "memory" => "1Gi"},
    "limits" => %{"cpu" => "2", "memory" => "4Gi"}
  }

  @type config :: %{required(:namespace) => String.t(), required(:image) => String.t()}

  @spec compile(map(), config()) :: {:ok, map()} | {:error, term()}
  def compile(assignment, config) when is_map(assignment) and is_map(config) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         :ok <- valid_target(assignment),
         :ok <- valid_config(config),
         name = job_name(assignment),
         {:ok, assignment_json} <- Jason.encode(assignment),
         true <- byte_size(assignment_json) <= 32_768 do
      {:ok,
       %{
         "apiVersion" => @api_version,
         "kind" => @kind,
         "metadata" => %{
           "name" => name,
           "namespace" => config.namespace,
           "labels" => labels(assignment),
           "annotations" => %{
             "symphony.hypergrid.au/assignment-sha256" => assignment.sha256,
             "symphony.hypergrid.au/assignment-issue-id" => assignment.lease.issue_id,
             "symphony.hypergrid.au/assignment-generation" => Integer.to_string(assignment.lease.generation)
           }
         },
         "spec" => %{
           "activeDeadlineSeconds" => @active_deadline_seconds,
           "backoffLimit" => @backoff_limit,
           "ttlSecondsAfterFinished" => @ttl_seconds_after_finished,
           "template" => %{
             "metadata" => %{"labels" => labels(assignment)},
             "spec" => %{
               "restartPolicy" => "Never",
               "automountServiceAccountToken" => false,
               "securityContext" => %{
                 "runAsNonRoot" => true,
                 "runAsUser" => 10_001,
                 "runAsGroup" => 10_001,
                 "fsGroup" => 10_001,
                 "seccompProfile" => %{"type" => "RuntimeDefault"}
               },
               "containers" => [
                 %{
                   "name" => "symphony-worker",
                   "image" => config.image,
                   "imagePullPolicy" => "IfNotPresent",
                   "command" => @worker_command,
                   "args" => ["--assignment-json", assignment_json],
                   "env" => [
                     %{"name" => "SYMPHONY_ASSIGNMENT_SHA256", "value" => assignment.sha256},
                     %{"name" => "SYMPHONY_ASSIGNMENT_ID", "value" => identity(assignment)}
                   ],
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
                   "volumeMounts" => [
                     %{"name" => "workspace", "mountPath" => "/workspace", "readOnly" => false},
                     %{"name" => "tmp", "mountPath" => "/tmp", "readOnly" => false}
                   ]
                 }
               ],
               "volumes" => [
                 %{"name" => "workspace", "emptyDir" => %{"sizeLimit" => "10Gi"}},
                 %{"name" => "tmp", "emptyDir" => %{"sizeLimit" => "256Mi"}}
               ]
             }
           }
         }
       }}
    else
      {:error, _reason} = error -> error
      false -> {:error, :rke2_job_assignment_too_large}
      _ -> {:error, :invalid_rke2_job_assignment}
    end
  end

  def compile(_assignment, _config), do: {:error, :invalid_rke2_job_assignment}

  @spec identity(map()) :: String.t()
  def identity(%{lease: %{issue_id: issue_id, generation: generation}, sha256: digest}) do
    Enum.join([issue_id, Integer.to_string(generation), digest], ":")
  end

  @spec owned_job?(map(), map()) :: boolean()
  def owned_job?(job, expected) when is_map(job) and is_map(expected) do
    job["apiVersion"] == expected["apiVersion"] and
      job["kind"] == expected["kind"] and
      get_in(job, ["metadata", "name"]) == get_in(expected, ["metadata", "name"]) and
      get_in(job, ["metadata", "namespace"]) == get_in(expected, ["metadata", "namespace"]) and
      get_in(job, ["metadata", "labels"]) == get_in(expected, ["metadata", "labels"]) and
      get_in(job, ["metadata", "annotations"]) == get_in(expected, ["metadata", "annotations"]) and
      get_in(job, ["spec"]) == get_in(expected, ["spec"])
  end

  def owned_job?(_job, _expected), do: false

  @spec labels(map()) :: map()
  def labels(assignment) do
    %{
      "app.kubernetes.io/managed-by" => "symphony",
      "symphony.hypergrid.au/issue-id" => compact(assignment.lease.issue_id),
      "symphony.hypergrid.au/generation" => Integer.to_string(assignment.lease.generation),
      "symphony.hypergrid.au/assignment-sha256" => assignment.sha256
    }
  end

  defp valid_target(%{environment: %{placement: :internal_beta, target_environment: :rke2}}), do: :ok
  defp valid_target(_assignment), do: {:error, :rke2_job_target_not_authorized}

  defp valid_config(%{namespace: namespace, image: image}) do
    if dns_label?(namespace) and digest_image?(image), do: :ok, else: {:error, :rke2_job_trusted_config_invalid}
  end

  defp valid_config(_config), do: {:error, :rke2_job_trusted_config_invalid}

  defp digest_image?(image) when is_binary(image) do
    byte_size(image) <= 512 and Regex.match?(~r|\A[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}\z|, image)
  end

  defp digest_image?(_image), do: false

  defp dns_label?(value) when is_binary(value),
    do: byte_size(value) <= 63 and Regex.match?(~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/, value)

  defp dns_label?(_value), do: false

  defp job_name(assignment) do
    suffix = :crypto.hash(:sha256, identity(assignment)) |> Base.encode16(case: :lower) |> binary_part(0, 24)
    "symphony-#{suffix}"
  end

  defp compact(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower) |> binary_part(0, 32)
end
