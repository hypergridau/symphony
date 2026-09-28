defmodule SymphonyElixir.RKE2Job.AuthCacheVerifierObserver do
  @moduledoc """
  Trusted host callback for an OAuth slot cleanup receipt.

  The caller supplies a digest-pinned image, host Kubernetes context, and the
  private journal root. One persisted attempt may create one verifier Job. A
  missing Job after an uncertain create is held for recovery, never replaced.
  The original assignment Job must already be deleted. No receipt is returned
  until the verifier result is checkpointed, its Job is gone, the PVC UID is
  unchanged, and a fresh complete PodList has no claim consumer.
  """

  alias SymphonyElixir.RKE2Job.{AuthCacheVerifierAttemptJournal, AuthCacheVerifierJobSpec}
  alias SymphonyElixir.RKE2Job.{AuthCacheVerifierResult, HTTPClient}

  @namespace "frigga"
  @hex64 ~r/\A[a-f0-9]{64}\z/
  @safe_uid ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/

  @doc "Returns a fresh host-observed slot cleanup receipt, or retains the slot."
  @spec observe(map(), map(), map(), map()) :: {:ok, map()} | {:held, atom()} | {:error, atom()}
  def observe(slot, assignment, allocation, context) do
    with {:ok, ports} <- ports(context),
         {:ok, original_name, original_uid} <- allocation_identity(allocation, assignment),
         {:ok, intent} <-
           AuthCacheVerifierAttemptJournal.ensure(
             assignment,
             original_uid,
             slot,
             ports.image,
             ports.journal_root
           ),
         {:ok, expected} <- expected_job(assignment, slot, intent, ports),
         {:ok, evidence} <- verified_evidence(assignment, slot, original_name, original_uid, intent, expected, ports),
         :ok <- delete_verifier(expected, evidence, ports),
         {:ok, snapshot} <- final_snapshot(slot, original_name, original_uid, expected, evidence, ports) do
      {:ok, receipt(slot, original_uid, evidence, snapshot)}
    else
      {:held, _} = held -> held
      _ -> {:held, :auth_cache_verifier_observation_unavailable}
    end
  rescue
    _ -> {:held, :auth_cache_verifier_observation_unavailable}
  end

  defp ports(%{config: %{image: image, catalog: catalog, journal_root: root}, client_context: client_context} = context)
       when is_binary(image) and is_map(catalog) and is_binary(root) and is_map(client_context) do
    client = Map.get(context, :client, HTTPClient)

    if Code.ensure_loaded?(client) and
         Enum.all?([{:create_job, 3}, {:get_job, 3}, {:delete_job, 4}, {:get_pvc, 3}, {:list_pods_snapshot, 2}], fn {name, arity} ->
           function_exported?(client, name, arity)
         end) do
      {:ok, %{client: client, client_context: client_context, image: image, catalog: catalog, journal_root: root}}
    else
      {:error, :auth_cache_verifier_client_unavailable}
    end
  end

  defp ports(_context), do: {:error, :auth_cache_verifier_context_invalid}

  defp allocation_identity(%{id: "rke2job:v1:" <> encoded, status: :ready}, %{sha256: digest})
       when is_binary(digest) do
    with true <- Regex.match?(@hex64, digest),
         {:ok, bytes} <- Base.url_decode64(encoded, padding: false),
         {:ok, [1, @namespace, name, uid, ^digest]} <- Jason.decode(bytes),
         true <- safe_name?(name) and safe_uid?(uid) do
      {:ok, name, uid}
    else
      _ -> {:error, :auth_cache_verifier_allocation_invalid}
    end
  end

  defp allocation_identity(_allocation, _assignment), do: {:error, :auth_cache_verifier_allocation_invalid}

  defp expected_job(assignment, slot, intent, ports) do
    AuthCacheVerifierJobSpec.compile(assignment, slot, %{
      namespace: @namespace,
      image: ports.image,
      catalog: ports.catalog,
      attempt_id: intent["attemptId"]
    })
  end

  defp verified_evidence(assignment, slot, original_name, original_uid, intent, expected, ports) do
    case AuthCacheVerifierAttemptJournal.load_result(intent, ports.journal_root) do
      {:ok, evidence} -> checkpoint_bound(intent, evidence, ports)
      :missing -> observe_uncheckpointed(assignment, slot, original_name, original_uid, intent, expected, ports)
      other -> other
    end
  end

  defp checkpoint_bound(intent, evidence, ports) do
    with {:ok, :replayed} <- AuthCacheVerifierAttemptJournal.begin_create(intent, ports.journal_root),
         {:ok, uid} <- AuthCacheVerifierAttemptJournal.load_job_uid(intent, ports.journal_root),
         true <- uid == evidence["job_uid"] do
      {:ok, evidence}
    else
      _ -> {:held, :auth_cache_verifier_checkpoint_binding_invalid}
    end
  end

  defp observe_uncheckpointed(assignment, slot, original_name, original_uid, intent, expected, ports) do
    name = get_in(expected, ["metadata", "name"])

    case ports.client.get_job(@namespace, name, ports.client_context) do
      {:ok, job} -> reconcile_existing(job, original_name, intent, expected, ports)
      {:error, :not_found} -> create_first(assignment, slot, original_name, original_uid, intent, expected, ports)
      _ -> {:held, :auth_cache_verifier_job_read_unavailable}
    end
  end

  defp create_first(_assignment, slot, original_name, _original_uid, intent, expected, ports) do
    with :ok <- original_absent(original_name, ports),
         :ok <- bound_pvc(slot, ports),
         {:ok, snapshot} <- pod_snapshot(ports),
         :ok <- no_claim_consumers(snapshot, slot.claim_name),
         {:ok, marker} <- AuthCacheVerifierAttemptJournal.begin_create(intent, ports.journal_root),
         :new <- marker,
         {:ok, _job} <- ports.client.create_job(@namespace, expected, ports.client_context),
         {:ok, created} <- ports.client.get_job(@namespace, expected["metadata"]["name"], ports.client_context) do
      reconcile_existing(created, original_name, intent, expected, ports)
    else
      _ -> {:held, :auth_cache_verifier_create_unavailable}
    end
  end

  defp reconcile_existing(job, original_name, intent, expected, ports) do
    uid = get_in(job, ["metadata", "uid"])

    with {:ok, :replayed} <- AuthCacheVerifierAttemptJournal.begin_create(intent, ports.journal_root),
         :ok <- original_absent(original_name, ports),
         true <- AuthCacheVerifierResult.owned_job?(expected, uid, job),
         {:ok, ^uid} <- AuthCacheVerifierAttemptJournal.record_job_uid(intent, uid, ports.journal_root),
         {:ok, snapshot} <- pod_snapshot(ports),
         {:ok, evidence} <- AuthCacheVerifierResult.verify(expected, uid, job, snapshot),
         :ok <- only_verifier_claim_consumer(snapshot, intent["claimName"], evidence.pod_uid),
         {:ok, saved} <- AuthCacheVerifierAttemptJournal.record_result(intent, evidence, ports.journal_root) do
      {:ok, saved}
    else
      _ -> {:held, :auth_cache_verifier_result_unavailable}
    end
  end

  defp delete_verifier(expected, evidence, ports) do
    name = expected["metadata"]["name"]
    uid = evidence["job_uid"]

    case ports.client.get_job(@namespace, name, ports.client_context) do
      {:error, :not_found} -> :ok
      {:ok, job} -> delete_exact(job, expected, uid, ports)
      _ -> {:held, :auth_cache_verifier_delete_read_unavailable}
    end
  end

  defp delete_exact(job, expected, uid, ports) do
    if AuthCacheVerifierResult.owned_job?(expected, uid, job) do
      _ = ports.client.delete_job(@namespace, expected["metadata"]["name"], uid, ports.client_context)
      original_absent(expected["metadata"]["name"], ports)
    else
      {:held, :auth_cache_verifier_job_identity_changed}
    end
  end

  defp final_snapshot(slot, original_name, original_uid, expected, evidence, ports) do
    with :ok <- original_absent(original_name, ports),
         :ok <- original_absent(expected["metadata"]["name"], ports),
         :ok <- bound_pvc(slot, ports),
         {:ok, snapshot} <- pod_snapshot(ports),
         :ok <- no_claim_consumers(snapshot, slot.claim_name),
         :ok <- no_job_pods(snapshot, original_name, original_uid, expected["metadata"]["name"], evidence["job_uid"]) do
      {:ok, snapshot}
    end
  end

  defp original_absent(name, ports) do
    case ports.client.get_job(@namespace, name, ports.client_context) do
      {:error, :not_found} -> :ok
      _ -> {:held, :auth_cache_verifier_job_still_present}
    end
  end

  defp bound_pvc(slot, ports) do
    case ports.client.get_pvc(@namespace, slot.claim_name, ports.client_context) do
      {:ok, pvc} ->
        if bound_pvc_identity?(pvc, slot),
          do: :ok,
          else: {:held, :auth_cache_verifier_pvc_changed}

      _ ->
        {:held, :auth_cache_verifier_pvc_unavailable}
    end
  end

  defp bound_pvc_identity?(pvc, slot) do
    pvc["apiVersion"] == "v1" and pvc["kind"] == "PersistentVolumeClaim" and
      pvc_metadata_matches?(Map.get(pvc, "metadata", %{}), slot) and
      get_in(pvc, ["status", "phase"]) == "Bound"
  end

  defp pvc_metadata_matches?(metadata, slot) do
    metadata["namespace"] == @namespace and metadata["name"] == slot.claim_name and
      metadata["uid"] == slot.claim_uid and is_nil(metadata["deletionTimestamp"])
  end

  defp pod_snapshot(ports) do
    case ports.client.list_pods_snapshot(@namespace, ports.client_context) do
      {:ok, %{items: pods, resource_version: version} = snapshot}
      when is_list(pods) and is_binary(version) and version != "" ->
        if Enum.all?(pods, &valid_pod?/1), do: {:ok, snapshot}, else: {:held, :auth_cache_verifier_pod_snapshot_invalid}

      _ ->
        {:held, :auth_cache_verifier_pod_snapshot_unavailable}
    end
  end

  defp valid_pod?(%{"apiVersion" => "v1", "kind" => "Pod", "metadata" => metadata, "spec" => spec})
       when is_map(metadata) and is_map(spec) do
    metadata["namespace"] == @namespace and safe_uid?(metadata["uid"]) and
      safe_name?(metadata["name"]) and is_list(Map.get(metadata, "ownerReferences", [])) and
      is_map(Map.get(metadata, "labels", %{})) and is_list(Map.get(spec, "volumes", []))
  end

  defp valid_pod?(_pod), do: false

  defp no_claim_consumers(snapshot, claim) do
    if Enum.any?(snapshot.items, &consumes_claim?(&1, claim)),
      do: {:held, :auth_cache_verifier_claim_consumer_present},
      else: :ok
  end

  defp consumes_claim?(pod, claim) do
    Enum.any?(pod["spec"]["volumes"], fn volume ->
      not is_map(volume) or get_in(volume, ["persistentVolumeClaim", "claimName"]) == claim
    end)
  end

  defp only_verifier_claim_consumer(snapshot, claim, verifier_pod_uid) do
    if Enum.any?(snapshot.items, fn pod ->
         consumes_claim?(pod, claim) and pod["metadata"]["uid"] != verifier_pod_uid
       end),
       do: {:held, :auth_cache_verifier_competing_claim_consumer},
       else: :ok
  end

  defp no_job_pods(snapshot, original_name, original_uid, verifier_name, verifier_uid) do
    if Enum.any?(snapshot.items, &job_pod?(&1, original_name, original_uid, verifier_name, verifier_uid)),
      do: {:held, :auth_cache_verifier_pod_still_present},
      else: :ok
  end

  defp job_pod?(pod, original_name, original_uid, verifier_name, verifier_uid) do
    metadata = pod["metadata"]
    labels = Map.get(metadata, "labels", %{})
    owners = Map.get(metadata, "ownerReferences", [])

    Enum.any?(owners, fn owner ->
      owner["uid"] in [original_uid, verifier_uid] or owner["name"] in [original_name, verifier_name]
    end) or labels["batch.kubernetes.io/controller-uid"] in [original_uid, verifier_uid] or
      labels["controller-uid"] in [original_uid, verifier_uid] or
      labels["batch.kubernetes.io/job-name"] in [original_name, verifier_name] or
      labels["job-name"] in [original_name, verifier_name] or
      Enum.any?([original_name, verifier_name], &String.starts_with?(metadata["name"], &1 <> "-"))
  end

  defp receipt(slot, original_uid, evidence, snapshot) do
    %{
      "receiptId" => Ecto.UUID.generate(),
      "observedAt" => DateTime.utc_now() |> DateTime.to_iso8601(),
      "namespace" => @namespace,
      "jobUid" => original_uid,
      "claimName" => slot.claim_name,
      "claimUid" => slot.claim_uid,
      "jobAbsent" => true,
      "ownedPodsAbsent" => true,
      "claimPodsAbsent" => true,
      "podListResourceVersion" => snapshot.resource_version,
      "claimPodListResourceVersion" => snapshot.resource_version,
      "authCacheStatus" => evidence["auth_cache_status"],
      "authCacheBytes" => evidence["auth_cache_bytes"]
    }
  end

  defp safe_uid?(value) when is_binary(value), do: Regex.match?(@safe_uid, value)
  defp safe_uid?(_value), do: false

  defp safe_name?(value) when is_binary(value),
    do: byte_size(value) in 1..63 and Regex.match?(~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/, value)

  defp safe_name?(_value), do: false
end
