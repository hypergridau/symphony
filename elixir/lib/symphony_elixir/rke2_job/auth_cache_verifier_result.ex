defmodule SymphonyElixir.RKE2Job.AuthCacheVerifierResult do
  @moduledoc """
  Validates the exact verifier Job and its single terminated Pod before cleanup.

  This read-only boundary never treats a Pod's self-reported message as sufficient:
  the Job UID, lease binding, compiled command, image, volume, owner reference,
  terminal status, and bounded result must agree. The host must still delete the
  Job and prove final claim-wide Pod absence before releasing the slot.
  """

  @safe_id ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/
  @max_message_bytes 3_500

  @type evidence :: %{
          job_uid: String.t(),
          pod_uid: String.t(),
          pod_list_resource_version: String.t(),
          auth_cache_status: String.t(),
          auth_cache_bytes: pos_integer()
        }

  @spec verify(map(), String.t(), map(), map()) :: {:ok, evidence()} | {:held, atom()}
  def verify(expected, job_uid, job, %{items: pods, resource_version: version})
      when is_map(expected) and is_binary(job_uid) and is_map(job) and is_list(pods) do
    name = get_in(expected, ["metadata", "name"])
    namespace = get_in(expected, ["metadata", "namespace"])

    with true <- safe_id?(job_uid) and safe_id?(version),
         true <- valid_job?(expected, job_uid, job),
         true <- Enum.all?(pods, &valid_pod_envelope?(&1, namespace)),
         [pod] <- Enum.filter(pods, &candidate?(&1, name, job_uid)),
         true <- valid_pod?(expected, pod, name, job_uid),
         {:ok, result} <- terminated_result(pod) do
      {:ok,
       %{
         job_uid: job_uid,
         pod_uid: get_in(pod, ["metadata", "uid"]),
         pod_list_resource_version: version,
         auth_cache_status: result["authCacheStatus"],
         auth_cache_bytes: result["authCacheBytes"]
       }}
    else
      _ -> {:held, :auth_cache_verifier_result_unverified}
    end
  rescue
    _ -> {:held, :auth_cache_verifier_result_unverified}
  end

  def verify(_expected, _job_uid, _job, _snapshot), do: {:held, :auth_cache_verifier_result_unverified}

  defp valid_job?(expected, uid, job) do
    metadata = Map.get(job, "metadata", %{})
    spec = Map.get(job, "spec", %{})

    job["apiVersion"] == "batch/v1" and job["kind"] == "Job" and
      valid_job_metadata?(metadata, expected["metadata"], uid) and
      subset?(expected["spec"], spec) and
      safe_pod_spec?(get_in(spec, ["template", "spec"])) and
      complete?(job) and not failed?(job)
  end

  defp valid_job_metadata?(metadata, expected, uid) do
    metadata["uid"] == uid and safe_id?(metadata["resourceVersion"]) and
      metadata["name"] == expected["name"] and metadata["namespace"] == expected["namespace"] and
      metadata["labels"] == expected["labels"] and metadata["annotations"] == expected["annotations"]
  end

  defp valid_pod_envelope?(%{"apiVersion" => "v1", "kind" => "Pod", "metadata" => metadata, "spec" => spec}, namespace)
       when is_map(metadata) and is_map(spec) do
    metadata["namespace"] == namespace and safe_id?(metadata["uid"]) and
      safe_id?(metadata["resourceVersion"]) and safe_id?(metadata["name"]) and
      is_map(Map.get(metadata, "labels", %{})) and
      is_list(Map.get(metadata, "ownerReferences", [])) and
      is_list(Map.get(spec, "volumes", []))
  end

  defp valid_pod_envelope?(_pod, _namespace), do: false

  defp candidate?(pod, name, uid) do
    metadata = pod["metadata"]
    labels = Map.get(metadata, "labels", %{})
    owners = Map.get(metadata, "ownerReferences", [])

    Enum.any?(owners, &(&1["uid"] == uid or (&1["kind"] == "Job" and &1["name"] == name))) or
      labels["batch.kubernetes.io/controller-uid"] == uid or
      labels["batch.kubernetes.io/job-name"] == name or
      String.starts_with?(metadata["name"], name <> "-")
  end

  defp valid_pod?(expected, pod, name, uid) do
    metadata = pod["metadata"]
    spec = pod["spec"]
    expected_spec = get_in(expected, ["spec", "template", "spec"])
    expected_labels = get_in(expected, ["spec", "template", "metadata", "labels"])
    owners = Map.get(metadata, "ownerReferences", [])

    owned_by?(owners, name, uid) and
      Map.take(metadata["labels"], Map.keys(expected_labels)) == expected_labels and
      subset?(expected_spec, spec) and safe_pod_spec?(spec) and
      get_in(pod, ["status", "phase"]) == "Succeeded"
  end

  defp owned_by?([owner], name, uid) when is_map(owner) do
    Map.take(owner, ~w(apiVersion kind name uid controller)) ==
      %{"apiVersion" => "batch/v1", "kind" => "Job", "name" => name, "uid" => uid, "controller" => true} and
      Map.drop(owner, ~w(apiVersion kind name uid controller)) in [%{}, %{"blockOwnerDeletion" => true}, %{"blockOwnerDeletion" => false}]
  end

  defp owned_by?(_owners, _name, _uid), do: false

  defp safe_pod_spec?(spec) when is_map(spec) do
    [container] = Map.get(spec, "containers", [nil])
    volumes = Map.get(spec, "volumes", [])

    no_extra_processes?(spec) and no_host_access?(spec) and
      safe_container?(container) and is_list(volumes) and Enum.all?(volumes, &safe_volume?/1)
  rescue
    _ -> false
  end

  defp safe_pod_spec?(_spec), do: false

  defp no_extra_processes?(spec),
    do: spec["initContainers"] in [nil, []] and spec["ephemeralContainers"] in [nil, []]

  defp no_host_access?(spec) do
    spec["hostNetwork"] in [nil, false] and spec["hostPID"] in [nil, false] and
      spec["hostIPC"] in [nil, false] and spec["shareProcessNamespace"] in [nil, false] and
      is_nil(spec["hostAliases"]) and is_nil(spec["runtimeClassName"])
  end

  defp safe_container?(container) when is_map(container) do
    no_extra_container_inputs?(container) and no_extra_container_execution?(container) and
      safe_security_context?(container["securityContext"])
  end

  defp safe_container?(_container), do: false

  defp no_extra_container_inputs?(container) do
    container["envFrom"] in [nil, []] and container["ports"] in [nil, []] and
      container["volumeDevices"] in [nil, []] and is_nil(container["lifecycle"])
  end

  defp no_extra_container_execution?(container) do
    is_nil(container["startupProbe"]) and is_nil(container["livenessProbe"]) and
      is_nil(container["readinessProbe"]) and container["stdin"] in [nil, false] and
      container["stdinOnce"] in [nil, false] and container["tty"] in [nil, false]
  end

  defp safe_security_context?(context) when is_map(context) do
    capabilities = Map.get(context, "capabilities", %{})

    context["privileged"] in [nil, false] and context["procMount"] in [nil, "Default"] and
      context["windowsOptions"] == nil and is_map(capabilities) and
      capabilities["add"] in [nil, []]
  end

  defp safe_security_context?(_context), do: false

  defp safe_volume?(volume) when is_map(volume) do
    case Map.keys(volume) |> Enum.sort() do
      ["name", "persistentVolumeClaim"] -> true
      ["emptyDir", "name"] -> true
      _ -> false
    end
  end

  defp safe_volume?(_volume), do: false

  defp terminated_result(pod) do
    case get_in(pod, ["status", "containerStatuses"]) do
      [%{"name" => "oauth-cache-verifier", "ready" => false, "state" => %{"terminated" => %{"exitCode" => 0, "message" => message}}}]
      when is_binary(message) and byte_size(message) <= @max_message_bytes ->
        case Jason.decode(message) do
          {:ok, %{"schemaVersion" => 1, "authCacheStatus" => "codex_login_status_authenticated", "authCacheBytes" => bytes} = result}
          when map_size(result) == 3 and is_integer(bytes) and bytes in 1..10_000_000 ->
            {:ok, result}

          _ ->
            :error
        end

      _ ->
        :error
    end
  end

  defp subset?(expected, actual) when is_map(expected) and is_map(actual),
    do: Enum.all?(expected, fn {key, value} -> subset?(value, Map.get(actual, key)) end)

  defp subset?(expected, actual) when is_list(expected) and is_list(actual),
    do: length(expected) == length(actual) and Enum.zip(expected, actual) |> Enum.all?(fn {left, right} -> subset?(left, right) end)

  defp subset?(expected, actual), do: expected == actual

  defp complete?(job), do: condition?(job, "Complete")
  defp failed?(job), do: condition?(job, "Failed")

  defp condition?(job, kind) do
    job
    |> get_in(["status", "conditions"])
    |> List.wrap()
    |> Enum.any?(&(&1["type"] == kind and &1["status"] == "True"))
  end

  defp safe_id?(value) when is_binary(value), do: Regex.match?(@safe_id, value)
  defp safe_id?(_value), do: false
end
