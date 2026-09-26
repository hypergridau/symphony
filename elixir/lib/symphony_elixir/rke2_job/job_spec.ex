defmodule SymphonyElixir.RKE2Job.JobSpec do
  @moduledoc """
  Pure compiler from an assignment bundle built from a signature-verified manifest entry.

  Callers must preserve that provenance. This module verifies canonical bundle integrity,
  placement, and trusted image/namespace configuration; it does not verify the manifest
  signature itself. Compiled Jobs start suspended; this module does not activate them.
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
           "suspend" => true,
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
      metadata_matches?(job, expected) and spec_matches?(job, expected)
  end

  def owned_job?(_job, _expected), do: false

  @doc "Accepts exact owned Jobs in suspended or active state for cleanup only."
  @spec owned_job_for_cleanup?(map(), map()) :: boolean()
  def owned_job_for_cleanup?(job, expected) when is_map(job) and is_map(expected) do
    case cleanup_job_metadata(job) do
      {:ok, cleanup_job} ->
        owned_job?(cleanup_job, expected) or
          owned_job?(cleanup_job, put_in(expected, ["spec", "suspend"], false))

      _ ->
        false
    end
  end

  def owned_job_for_cleanup?(_job, _expected), do: false

  defp cleanup_job_metadata(%{"metadata" => metadata} = job) when is_map(metadata) do
    case Map.fetch(metadata, "deletionTimestamp") do
      :error ->
        {:ok, job}

      {:ok, timestamp} ->
        grace = Map.get(metadata, "deletionGracePeriodSeconds")
        finalizers = Map.get(metadata, "finalizers")

        if valid_deletion_timestamp?(timestamp) and
             (is_nil(grace) or (is_integer(grace) and grace >= 0)) and
             finalizers in [nil, [], ["foregroundDeletion"]] do
          {:ok, put_in(job, ["metadata"], Map.drop(metadata, ["deletionTimestamp", "deletionGracePeriodSeconds", "finalizers"]))}
        else
          :error
        end
    end
  end

  defp cleanup_job_metadata(_job), do: :error

  defp valid_deletion_timestamp?(value) when is_binary(value) do
    match?({:ok, _, _}, DateTime.from_iso8601(value))
  end

  defp valid_deletion_timestamp?(_value), do: false

  @spec labels(map()) :: map()
  def labels(assignment) do
    %{
      "app.kubernetes.io/managed-by" => "symphony",
      "symphony.hypergrid.au/issue-id" => compact(assignment.lease.issue_id),
      "symphony.hypergrid.au/generation" => Integer.to_string(assignment.lease.generation),
      "symphony.hypergrid.au/assignment-sha256" => assignment.sha256
    }
  end

  defp valid_server_metadata?(metadata, expected) when is_map(metadata) and is_map(expected) do
    metadata
    |> Map.drop(Map.keys(expected))
    |> Enum.all?(fn
      {"uid", uid} -> is_binary(uid) and uid != ""
      {"resourceVersion", value} -> is_binary(value) and value != ""
      {"generation", value} -> is_integer(value) and value > 0
      {"creationTimestamp", value} -> is_binary(value) and value != ""
      {"managedFields", value} -> is_list(value)
      {"selfLink", value} -> is_binary(value) and String.starts_with?(value, "/")
      _ -> false
    end)
  end

  defp valid_server_metadata?(_metadata, _expected), do: false

  defp metadata_matches?(%{"metadata" => metadata}, %{"metadata" => expected_metadata})
       when is_map(metadata) and is_map(expected_metadata) do
    uid = Map.get(metadata, "uid")

    valid_server_metadata?(metadata, expected_metadata) and is_binary(uid) and uid != "" and
      Enum.all?(["name", "namespace"], fn key ->
        Map.get(metadata, key) == Map.get(expected_metadata, key)
      end) and server_annotations_match?(metadata["annotations"], expected_metadata["annotations"]) and
      server_labels_match?(metadata["labels"], expected_metadata["labels"], metadata["name"], uid)
  end

  defp metadata_matches?(_job, _expected), do: false

  defp spec_matches?(job, expected) do
    uid = get_in(job, ["metadata", "uid"])
    actual_spec = job["spec"]
    expected_spec = expected["spec"]

    generated_selector_matches?(actual_spec, expected, uid) and
      matches_with_defaults?(actual_spec, expected_spec, ["spec"], uid, expected)
  end

  defp generated_selector_matches?(actual_spec, expected, uid) when is_map(actual_spec) do
    expected_labels = get_in(expected, ["spec", "template", "metadata", "labels"])
    selector_labels = get_in(actual_spec, ["selector", "matchLabels"])
    template_labels = get_in(actual_spec, ["template", "metadata", "labels"])
    name = expected["metadata"]["name"]

    selector_matches? = Enum.any?(selector_label_sets(uid), &(&1 == selector_labels))

    template_matches? =
      Enum.any?(generated_label_sets(name, uid), &(Map.merge(expected_labels, &1) == template_labels))

    selector_matches? and template_matches?
  end

  defp generated_selector_matches?(_actual_spec, _expected, _uid), do: false

  defp generated_label_sets(name, uid) do
    modern = %{
      "batch.kubernetes.io/controller-uid" => uid,
      "batch.kubernetes.io/job-name" => name
    }

    legacy = %{"controller-uid" => uid, "job-name" => name}
    [modern, legacy, Map.merge(modern, legacy)]
  end

  defp selector_label_sets(uid), do: [%{"batch.kubernetes.io/controller-uid" => uid}, %{"controller-uid" => uid}]

  defp server_labels_match?(actual, expected, name, uid) when is_map(actual) and is_map(expected) do
    generated_label_sets(name, uid)
    |> Enum.any?(&(Map.merge(expected, &1) == actual))
  end

  defp server_labels_match?(_actual, _expected, _name, _uid), do: false

  defp server_annotations_match?(actual, expected) when is_map(actual) and is_map(expected) do
    actual == expected or actual == Map.put(expected, "batch.kubernetes.io/job-tracking", "")
  end

  defp server_annotations_match?(_actual, _expected), do: false

  defp matches_with_defaults?(actual, expected, path, uid, job) when is_map(actual) and is_map(expected) do
    expected_matches? =
      Enum.all?(expected, fn {key, expected_value} ->
        Map.has_key?(actual, key) and
          matches_with_defaults?(Map.fetch!(actual, key), expected_value, path ++ [key], uid, job)
      end)

    extras_match? =
      actual
      |> Map.drop(Map.keys(expected))
      |> Enum.all?(fn {key, value} -> allowed_default?(path ++ [key], value, uid, job) end)

    expected_matches? and extras_match?
  end

  defp matches_with_defaults?(actual, expected, path, uid, job) when is_list(actual) and is_list(expected) do
    length(actual) == length(expected) and
      Enum.zip(actual, expected)
      |> Enum.with_index()
      |> Enum.all?(fn {{actual_value, expected_value}, index} ->
        matches_with_defaults?(actual_value, expected_value, path ++ [index], uid, job)
      end)
  end

  defp matches_with_defaults?(actual, expected, _path, _uid, _job), do: actual == expected

  defp allowed_default?(["spec", "selector"], %{"matchLabels" => labels} = selector, uid, _job)
       when map_size(selector) == 1 and is_map(labels) do
    selector_label_sets(uid) |> Enum.any?(&(&1 == labels))
  end

  defp allowed_default?(["spec", "template", "metadata", "labels", key], value, uid, job) do
    generated_label_sets(job["metadata"]["name"], uid)
    |> Enum.any?(&(Map.get(&1, key) == value and Map.has_key?(&1, key)))
  end

  defp allowed_default?(["spec", "template", "metadata", "creationTimestamp"], nil, _uid, _job), do: true

  defp allowed_default?(["spec", key], value, _uid, _job) do
    {key, value} in [
      {"completions", 1},
      {"parallelism", 1},
      {"completionMode", "NonIndexed"},
      {"manualSelector", false},
      {"podReplacementPolicy", "TerminatingOrFailed"}
    ]
  end

  defp allowed_default?(["spec", "template", "spec", key], value, _uid, _job) do
    {key, value} in pod_defaults()
  end

  defp allowed_default?(["spec", "template", "spec", "containers", 0, key], value, _uid, _job) do
    {key, value} in [
      {"terminationMessagePath", "/dev/termination-log"},
      {"terminationMessagePolicy", "File"}
    ]
  end

  defp allowed_default?(_path, _value, _uid, _job), do: false

  defp pod_defaults do
    [
      {"dnsPolicy", "ClusterFirst"},
      {"schedulerName", "default-scheduler"},
      {"terminationGracePeriodSeconds", 30},
      {"enableServiceLinks", true},
      {"preemptionPolicy", "PreemptLowerPriority"},
      {"serviceAccountName", "default"},
      {"hostNetwork", false},
      {"hostPID", false},
      {"hostIPC", false},
      {"shareProcessNamespace", false},
      {"setHostnameAsFQDN", false},
      {"hostUsers", true}
    ]
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
