defmodule SymphonyElixir.RKE2Job.SuspendedAbort do
  @moduledoc """
  Observes and conditionally removes one never-activated, suspended slotted Job.

  prepare_owned/3 is read-only and returns a wire-ready, credential-free
  observation for the trusted host to persist with Dahlia before calling
  confirm_owned/4. Confirmation revalidates that exact observation before
  the conditional delete. These operations never release the OAuth slot or
  provider claim; an uncertain outcome remains held for reconciliation.
  """

  alias SymphonyElixir.RKE2Job.AbortPrepareJournal
  alias SymphonyElixir.RKE2Job.JobSpec

  @safe_version ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]{0,255}\z/
  @type observation :: map()

  @spec prepare_owned(map(), String.t(), String.t(), keyword()) ::
          {:ok, observation()} | {:held, term()} | {:error, term()}
  def prepare_owned(assignment, allocation_id, uid, opts)
      when is_map(assignment) and is_binary(allocation_id) and is_binary(uid) and is_list(opts) do
    client = Keyword.get(opts, :client)
    context = Keyword.get(opts, :client_context)
    config = Keyword.get(opts, :config)

    with :ok <- validate_client(client),
         {:ok, expected} <- JobSpec.compile(assignment, config),
         {:ok, claim} <- slot_claim(config),
         {:ok, job} <- read_job(client, context, expected),
         {:ok, job_evidence} <- verify_job(job, expected, uid),
         {:ok, pod_evidence} <- read_pod_evidence(client, context, expected, uid, claim) do
      {:ok,
       %{
         "schemaVersion" => 1,
         "observedAt" => DateTime.utc_now() |> DateTime.truncate(:second) |> DateTime.to_iso8601(),
         "allocationId" => allocation_id,
         "compiledIdentity" => compiled_identity(assignment, expected),
         "slotBinding" => slot_binding(config, assignment),
         "job" => job_evidence,
         "podSnapshot" => pod_evidence
       }}
    else
      {:held, _} = held -> held
      {:error, _} = error -> error
    end
  end

  @deprecated "Use prepare_owned/4 and persist Dahlia's immutable prepare acknowledgment before confirmation."
  @spec prepare_owned(map(), String.t(), keyword()) :: {:held, :durable_abort_prepare_ack_required}
  def prepare_owned(_assignment, _uid, _opts), do: {:held, :durable_abort_prepare_ack_required}

  @spec confirm_owned(map(), String.t(), String.t(), observation(), map(), keyword()) ::
          :ok | {:held, term()} | {:error, term()}
  def confirm_owned(assignment, allocation_id, uid, observation, prepare_ack, opts)
      when is_map(assignment) and is_binary(allocation_id) and is_binary(uid) and is_map(observation) and
             is_map(prepare_ack) and is_list(opts) do
    client = Keyword.get(opts, :client)
    context = Keyword.get(opts, :client_context)
    config = Keyword.get(opts, :config)

    with :ok <- validate_client(client),
         {:ok, expected} <- JobSpec.compile(assignment, config),
         {:ok, claim} <- slot_claim(config),
         :ok <- validate_observation(observation, assignment, allocation_id, expected, uid, config),
         :ok <- verify_prepare_ack(assignment, allocation_id, observation, prepare_ack, opts),
         :ok <- confirm_with_checkpoint(client, context, expected, uid, observation, claim, opts) do
      :ok
    else
      {:held, _} = held -> held
      {:error, _} = error -> error
    end
  end

  def confirm_owned(_assignment, _allocation_id, _uid, _observation, _prepare_ack, _opts),
    do: {:held, :durable_abort_prepare_ack_required}

  @deprecated "Confirmation requires the exact persisted prepare acknowledgment and a trusted guard."
  @spec confirm_owned(map(), String.t(), observation(), keyword()) :: {:held, :durable_abort_prepare_ack_required}
  def confirm_owned(_assignment, _uid, _observation, _opts), do: {:held, :durable_abort_prepare_ack_required}

  @doc "Deprecated compatibility entrypoint; a durable host acknowledgment is mandatory."
  @spec abort_owned(map(), String.t(), keyword()) :: {:held, :durable_abort_prepare_ack_required}
  @deprecated "Use prepare_owned/4, persist Dahlia's acknowledgment, then confirm_owned/6."
  def abort_owned(_assignment, _uid, _opts), do: {:held, :durable_abort_prepare_ack_required}

  defp validate_client(client) do
    if is_atom(client) and Code.ensure_loaded?(client) and function_exported?(client, :get_job, 3) and
         function_exported?(client, :list_pods_snapshot, 2) and function_exported?(client, :delete_suspended_job, 5),
       do: :ok,
       else: {:error, :suspended_abort_client_missing}
  end

  defp slot_claim(%{auth_slot: %{claim_name: claim}}) when is_binary(claim), do: {:ok, claim}
  defp slot_claim(_config), do: {:error, :suspended_abort_slot_missing}

  defp read_job(client, context, expected) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])

    case safe_client_call(fn -> client.get_job(namespace, name, context) end) do
      {:ok, job} -> {:ok, job}
      {:error, :not_found} -> {:held, :suspended_abort_job_already_absent}
      _ -> {:held, :suspended_abort_job_read_unavailable}
    end
  end

  defp verify_job(job, expected, uid) do
    metadata = if is_map(Map.get(job, "metadata")), do: job["metadata"], else: %{}
    version = metadata["resourceVersion"]

    if JobSpec.owned_job?(job, expected) and metadata["uid"] == uid and metadata["generation"] == 1 and
         is_nil(metadata["deletionTimestamp"]) and safe_version?(version) and
         get_in(job, ["spec", "suspend"]) == true and no_execution_status?(Map.get(job, "status")) do
      {:ok,
       %{
         "uid" => uid,
         "resourceVersion" => version,
         "generation" => 1,
         "suspended" => true,
         "noExecution" => true
       }}
    else
      {:held, :suspended_abort_identity_or_start_unverified}
    end
  end

  defp no_execution_status?(nil), do: true

  defp no_execution_status?(status) when is_map(status) do
    allowed = ~w(active ready succeeded failed conditions)
    counters = ~w(active ready succeeded failed)
    conditions = Map.get(status, "conditions", [])

    Enum.all?(Map.keys(status), &(&1 in allowed)) and
      Enum.all?(counters, &(Map.get(status, &1) in [nil, 0])) and is_list(conditions) and
      Enum.all?(conditions, fn
        %{"type" => "Suspended", "status" => "True"} -> true
        _ -> false
      end)
  end

  defp no_execution_status?(_status), do: false

  defp read_pod_evidence(client, context, expected, uid, claim) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])
    digest = get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-sha256"])

    case safe_client_call(fn -> client.list_pods_snapshot(namespace, context) end) do
      {:ok, %{items: pods, resource_version: version}} when is_list(pods) ->
        if safe_version?(version) and Enum.all?(pods, &valid_pod?(&1, namespace)) and
             not Enum.any?(pods, &relevant_pod?(&1, name, uid, digest, claim)) do
          {:ok,
           %{
             "resourceVersion" => version,
             "sha256" => pod_digest(pods),
             "itemCount" => length(pods),
             "complete" => true,
             "ownedPodsAbsent" => true
           }}
        else
          {:held, :suspended_abort_pod_absence_unverified}
        end

      _ ->
        {:held, :suspended_abort_pod_read_unavailable}
    end
  end

  defp pod_digest(pods) do
    encoded_pods = pods |> Enum.map(&canonical_json/1) |> Enum.sort()
    :crypto.hash(:sha256, Jason.encode!(encoded_pods)) |> Base.encode16(case: :lower)
  end

  defp canonical_json(value), do: Jason.encode!(canonical_json_term(value))

  defp canonical_json_term(value) when is_map(value) do
    value
    |> Enum.sort_by(fn {key, _nested} -> key end)
    |> Enum.map(fn {key, nested} -> [key, canonical_json_term(nested)] end)
    |> then(&%{"$map" => &1})
  end

  defp canonical_json_term(value) when is_list(value), do: %{"$list" => Enum.map(value, &canonical_json_term/1)}
  defp canonical_json_term(value), do: %{"$value" => value}

  defp validate_observation(observation, assignment, allocation_id, expected, uid, config) do
    identity = compiled_identity(assignment, expected)
    binding = slot_binding(config, assignment)

    keys = ["schemaVersion", "observedAt", "allocationId", "compiledIdentity", "slotBinding", "job", "podSnapshot"]

    if Enum.sort(Map.keys(observation)) == Enum.sort(keys) and observation["schemaVersion"] == 1 and
         valid_timestamp?(observation["observedAt"]) and observation["compiledIdentity"] == identity and
         observation["allocationId"] == allocation_id and
         observation["slotBinding"] == binding and valid_job_evidence?(observation["job"], uid) and
         valid_pod_evidence?(observation["podSnapshot"]) do
      :ok
    else
      {:held, :suspended_abort_observation_invalid}
    end
  end

  defp verify_prepare_ack(assignment, allocation_id, observation, prepare_ack, opts) do
    guard = Keyword.get(opts, :prepare_ack_guard)
    context = Keyword.get(opts, :prepare_ack_guard_context)

    if is_atom(guard) and Code.ensure_loaded?(guard) and function_exported?(guard, :verify, 5) do
      case guard.verify(assignment.sha256, allocation_id, observation, prepare_ack, context) do
        :ok -> :ok
        {:held, _reason} = held -> held
        {:error, reason} -> {:held, {:abort_prepare_ack_verification_failed, reason}}
        _ -> {:held, :invalid_abort_prepare_ack_verification_response}
      end
    else
      {:held, :abort_prepare_ack_guard_missing}
    end
  rescue
    _error -> {:held, :abort_prepare_ack_verification_failed}
  catch
    _kind, _reason -> {:held, :abort_prepare_ack_verification_failed}
  end

  defp compiled_identity(assignment, expected) do
    encoded = canonical_json(expected)

    %{
      "apiVersion" => expected["apiVersion"],
      "kind" => expected["kind"],
      "namespace" => expected["metadata"]["namespace"],
      "name" => expected["metadata"]["name"],
      "assignmentSHA256" => assignment.sha256,
      "compiledJobSHA256" => :crypto.hash(:sha256, encoded) |> Base.encode16(case: :lower)
    }
  end

  defp slot_binding(%{auth_slot: slot}, assignment) when is_map(slot) do
    %{
      "slotId" => slot.slot_id,
      "leaseId" => slot.lease_id,
      "claimName" => slot.claim_name,
      "assignmentSHA256" => assignment.sha256,
      "seat" => assignment.seat
    }
  end

  defp slot_binding(_config, _assignment), do: nil

  defp valid_job_evidence?(job, uid) when is_map(job) do
    Enum.sort(Map.keys(job)) == Enum.sort(~w(uid resourceVersion generation suspended noExecution)) and
      job["uid"] == uid and safe_version?(job["resourceVersion"]) and job["generation"] == 1 and
      job["suspended"] == true and job["noExecution"] == true
  end

  defp valid_job_evidence?(_job, _uid), do: false

  defp valid_pod_evidence?(pods) when is_map(pods) do
    Enum.sort(Map.keys(pods)) ==
      Enum.sort(~w(resourceVersion sha256 itemCount complete ownedPodsAbsent)) and
      safe_version?(pods["resourceVersion"]) and is_integer(pods["itemCount"]) and pods["itemCount"] >= 0 and
      is_binary(pods["sha256"]) and Regex.match?(~r/\A[0-9a-f]{64}\z/, pods["sha256"]) and
      pods["complete"] == true and pods["ownedPodsAbsent"] == true
  end

  defp valid_pod_evidence?(_pods), do: false

  defp valid_timestamp?(value) when is_binary(value), do: match?({:ok, _, _}, DateTime.from_iso8601(value))
  defp valid_timestamp?(_value), do: false

  defp match_prepared_job(job, expected, uid, evidence) do
    case verify_job(job, expected, uid) do
      {:ok, ^evidence} -> :ok
      {:ok, _} -> {:held, :suspended_abort_job_changed_since_prepare}
      {:held, _} = held -> held
    end
  end

  defp match_prepared_pods(current, prepared) do
    if current == prepared,
      do: :ok,
      else: {:held, :suspended_abort_pods_changed_since_prepare}
  end

  defp confirm_with_checkpoint(client, context, expected, uid, observation, claim, opts) do
    case Keyword.get(opts, :confirmed_delete_journal) do
      %{journal_root: root, claim: journal_claim, record: record} ->
        case AbortPrepareJournal.load_confirmed_delete(root, journal_claim, record, uid) do
          {:ok, _checkpoint} ->
            verify_confirmed_delete_replay(client, context, expected, uid, claim)

          :missing ->
            checkpoint_context = %{journal_root: root, claim: journal_claim, record: record}
            verify_and_delete(client, context, expected, uid, observation, claim, checkpoint_context)

          {:held, _reason} = held ->
            held
        end

      nil ->
        verify_and_delete(client, context, expected, uid, observation, claim, nil)

      _ ->
        {:held, :abort_prepare_confirmed_delete_checkpoint_invalid}
    end
  end

  defp verify_and_delete(client, context, expected, uid, observation, claim, checkpoint_context) do
    with {:ok, job} <- read_job(client, context, expected),
         :ok <- match_prepared_job(job, expected, uid, observation["job"]),
         {:ok, pod_evidence} <- read_pod_evidence(client, context, expected, uid, claim),
         :ok <- match_prepared_pods(pod_evidence, observation["podSnapshot"]),
         {:ok, post_delete_pods} <- delete_and_confirm(client, context, expected, uid, observation, claim),
         :ok <- record_confirmed_delete(checkpoint_context, uid, post_delete_pods) do
      :ok
    else
      {:held, _} = held -> held
    end
  end

  defp verify_confirmed_delete_replay(client, context, expected, uid, claim) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])

    case safe_client_call(fn -> client.get_job(namespace, name, context) end) do
      {:error, :not_found} ->
        case read_pod_evidence(client, context, expected, uid, claim) do
          {:ok, _evidence} -> :ok
          {:held, _} = held -> held
        end

      _ ->
        {:held, :suspended_abort_confirmed_delete_replay_mismatch}
    end
  end

  defp record_confirmed_delete(nil, _uid, _pod_evidence), do: :ok

  defp record_confirmed_delete(%{journal_root: root, claim: claim, record: record}, uid, pod_evidence),
    do: AbortPrepareJournal.record_confirmed_delete(root, claim, record, uid, pod_evidence)

  defp delete_and_confirm(client, context, expected, uid, observation, claim) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])
    version = observation["job"]["resourceVersion"]

    with :ok <- safe_client_call(fn -> client.delete_suspended_job(namespace, name, uid, version, context) end),
         {:ok, evidence} <- confirm_absent(client, context, namespace, name, uid, expected, claim) do
      {:ok, evidence}
    else
      {:held, _} = held -> held
      _ -> {:held, :suspended_abort_delete_uncertain}
    end
  end

  defp valid_pod?(%{"apiVersion" => "v1", "kind" => "Pod", "metadata" => metadata, "spec" => spec}, namespace)
       when is_map(metadata) and is_map(spec) do
    valid_pod_metadata?(metadata, namespace) and valid_pod_spec?(spec)
  end

  defp valid_pod?(_pod, _namespace), do: false

  defp valid_pod_metadata?(metadata, namespace) do
    owners = Map.get(metadata, "ownerReferences", [])

    metadata["namespace"] == namespace and safe_version?(metadata["uid"]) and safe_version?(metadata["resourceVersion"]) and
      is_binary(metadata["name"]) and metadata["name"] != "" and is_map(Map.get(metadata, "labels", %{})) and
      is_list(owners) and Enum.all?(owners, &valid_owner?/1)
  end

  defp valid_owner?(%{"apiVersion" => api, "kind" => kind, "name" => name, "uid" => uid}),
    do: Enum.all?([api, kind, name, uid], &(is_binary(&1) and &1 != ""))

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
      labels["symphony.hypergrid.au/assignment-sha256"] == digest or String.starts_with?(metadata["name"], name <> "-")
  end

  defp confirm_absent(client, context, namespace, name, uid, expected, claim) do
    case safe_client_call(fn -> client.get_job(namespace, name, context) end) do
      {:error, :not_found} ->
        case read_pod_evidence(client, context, expected, uid, claim) do
          {:ok, evidence} -> {:ok, evidence}
          {:held, _} = held -> held
        end

      _ ->
        {:held, :suspended_abort_delete_unconfirmed}
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
