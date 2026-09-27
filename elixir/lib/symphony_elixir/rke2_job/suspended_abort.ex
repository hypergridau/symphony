defmodule SymphonyElixir.RKE2Job.SuspendedAbort do
  @moduledoc """
  Removes one never-activated, suspended slotted Job without a worker result.

  This is Kubernetes cleanup only. It does not release the OAuth slot, provider
  claim, or signed pre-execution abort. An uncertain delete remains held for
  operator reconciliation because Job absence alone loses pre-delete history.
  """

  alias SymphonyElixir.RKE2Job.JobSpec

  @safe_version ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/

  @spec abort_owned(map(), String.t(), keyword()) :: :ok | {:held, term()} | {:error, term()}
  def abort_owned(assignment, uid, opts) when is_map(assignment) and is_binary(uid) and is_list(opts) do
    client = Keyword.get(opts, :client)
    context = Keyword.get(opts, :client_context)
    config = Keyword.get(opts, :config)

    with :ok <- validate_client(client),
         {:ok, expected} <- JobSpec.compile(assignment, config),
         {:ok, claim} <- slot_claim(config),
         :ok <- abort_read(client, context, expected, uid, claim) do
      :ok
    else
      {:held, _} = held -> held
      {:error, _} = error -> error
    end
  end

  def abort_owned(_assignment, _uid, _opts), do: {:error, :invalid_suspended_abort_request}

  defp validate_client(client) do
    if is_atom(client) and Code.ensure_loaded?(client) and
         function_exported?(client, :get_job, 3) and
         function_exported?(client, :list_pods_snapshot, 2) and
         function_exported?(client, :delete_suspended_job, 5),
       do: :ok,
       else: {:error, :suspended_abort_client_missing}
  end

  defp slot_claim(%{auth_slot: %{claim_name: claim}}) when is_binary(claim), do: {:ok, claim}
  defp slot_claim(_config), do: {:error, :suspended_abort_slot_missing}

  defp abort_read(client, context, expected, uid, claim) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])

    case safe_client_call(fn -> client.get_job(namespace, name, context) end) do
      {:ok, job} -> abort_verified(client, context, expected, job, uid, claim)
      {:error, :not_found} -> {:held, :suspended_abort_job_already_absent}
      _ -> {:held, :suspended_abort_job_read_unavailable}
    end
  end

  defp abort_verified(client, context, expected, job, uid, claim) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])
    metadata = if is_map(Map.get(job, "metadata")), do: job["metadata"], else: %{}
    version = Map.get(metadata, "resourceVersion")

    if JobSpec.owned_job?(job, expected) and metadata["uid"] == uid and
         metadata["generation"] == 1 and is_nil(metadata["deletionTimestamp"]) and
         safe_version?(version) and no_execution_status?(Map.get(job, "status")) do
      delete_verified(client, context, namespace, name, uid, version, expected, claim)
    else
      {:held, :suspended_abort_identity_or_start_unverified}
    end
  end

  defp delete_verified(client, context, namespace, name, uid, version, expected, claim) do
    with :ok <- pods_absent(client, context, namespace, name, uid, expected, claim),
         :ok <- safe_client_call(fn -> client.delete_suspended_job(namespace, name, uid, version, context) end),
         :ok <- confirm_absent(client, context, namespace, name, uid, expected, claim) do
      :ok
    else
      {:held, _} = held -> held
      _ -> {:held, :suspended_abort_delete_uncertain}
    end
  end

  defp no_execution_status?(nil), do: true

  defp no_execution_status?(status) when is_map(status) do
    allowed = ~w(active ready succeeded failed conditions)
    counters = ~w(active ready succeeded failed)
    conditions = Map.get(status, "conditions", [])

    Enum.all?(Map.keys(status), &(&1 in allowed)) and
      Enum.all?(counters, &(Map.get(status, &1) in [nil, 0])) and
      is_list(conditions) and
      Enum.all?(conditions, fn
        %{"type" => "Suspended", "status" => "True"} -> true
        _ -> false
      end)
  end

  defp no_execution_status?(_status), do: false

  defp pods_absent(client, context, namespace, name, uid, expected, claim) do
    digest = get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-sha256"])

    case safe_client_call(fn -> client.list_pods_snapshot(namespace, context) end) do
      {:ok, %{items: pods, resource_version: version}} when is_list(pods) ->
        if safe_version?(version) and Enum.all?(pods, &valid_pod?(&1, namespace)) and
             not Enum.any?(pods, &relevant_pod?(&1, name, uid, digest, claim)),
           do: :ok,
           else: {:held, :suspended_abort_pod_absence_unverified}

      _ ->
        {:held, :suspended_abort_pod_read_unavailable}
    end
  end

  defp valid_pod?(%{"apiVersion" => "v1", "kind" => "Pod", "metadata" => metadata, "spec" => spec}, namespace)
       when is_map(metadata) and is_map(spec) do
    valid_pod_metadata?(metadata, namespace) and valid_pod_spec?(spec)
  end

  defp valid_pod?(_pod, _namespace), do: false

  defp valid_pod_metadata?(metadata, namespace) do
    owners = Map.get(metadata, "ownerReferences", [])

    metadata["namespace"] == namespace and safe_version?(metadata["uid"]) and
      safe_version?(metadata["resourceVersion"]) and
      is_binary(metadata["name"]) and metadata["name"] != "" and
      is_map(Map.get(metadata, "labels", %{})) and is_list(owners) and Enum.all?(owners, &valid_owner?/1)
  end

  defp valid_owner?(%{"apiVersion" => api, "kind" => kind, "name" => name, "uid" => uid}) do
    Enum.all?([api, kind, name, uid], &(is_binary(&1) and &1 != ""))
  end

  defp valid_owner?(_owner), do: false

  defp valid_pod_spec?(spec) do
    volumes = Map.get(spec, "volumes", [])
    is_list(volumes) and Enum.all?(volumes, &valid_volume?/1)
  end

  defp valid_volume?(volume) when is_map(volume) do
    case Map.get(volume, "persistentVolumeClaim") do
      nil -> true
      %{"claimName" => claim_name} when is_binary(claim_name) and claim_name != "" -> true
      _ -> false
    end
  end

  defp valid_volume?(_volume), do: false

  defp relevant_pod?(pod, name, uid, digest, claim) do
    metadata = pod["metadata"]
    labels = Map.get(metadata, "labels", %{})
    owners = Map.get(metadata, "ownerReferences", [])
    volumes = get_in(pod, ["spec", "volumes"]) || []

    job_identity_pod?(metadata, labels, owners, name, uid, digest) or
      Enum.any?(volumes, &(get_in(&1, ["persistentVolumeClaim", "claimName"]) == claim))
  end

  defp job_identity_pod?(metadata, labels, owners, name, uid, digest) do
    Enum.any?(owners, &(&1["uid"] == uid or (&1["kind"] == "Job" and &1["name"] == name))) or
      labels["batch.kubernetes.io/controller-uid"] == uid or labels["controller-uid"] == uid or
      labels["batch.kubernetes.io/job-name"] == name or labels["job-name"] == name or
      labels["symphony.hypergrid.au/assignment-sha256"] == digest or
      String.starts_with?(metadata["name"], name <> "-")
  end

  defp confirm_absent(client, context, namespace, name, uid, expected, claim) do
    case safe_client_call(fn -> client.get_job(namespace, name, context) end) do
      {:error, :not_found} -> pods_absent(client, context, namespace, name, uid, expected, claim)
      _ -> {:held, :suspended_abort_delete_unconfirmed}
    end
  end

  defp safe_version?(value) when is_binary(value), do: Regex.match?(@safe_version, value)
  defp safe_version?(_value), do: false

  defp safe_client_call(call) do
    call.()
  rescue
    _error -> {:error, :client_callback_raised}
  catch
    _kind, _reason -> {:error, :client_callback_exited}
  end
end
