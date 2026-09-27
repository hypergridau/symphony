defmodule SymphonyElixir.RKE2JobFakeClient do
  @behaviour SymphonyElixir.RKE2Job.Client

  @impl true
  def create_job(namespace, job, agent) do
    Agent.get_and_update(agent, &create_job_state(&1, namespace, job))
  end

  @impl true
  def get_job(namespace, name, agent) do
    record_activation_event(agent, :get_job)
    Agent.get_and_update(agent, &get_job_state(&1, {namespace, name}))
  end

  @impl true
  def list_pods(namespace, agent) do
    Agent.get(agent, fn state ->
      case Map.get(state, :list_pods_error) do
        nil -> {:ok, state |> Map.get(:pods, %{}) |> Map.values() |> Enum.filter(&(get_in(&1, ["metadata", "namespace"]) == namespace))}
        reason -> {:error, reason}
      end
    end)
  end

  def list_pods_snapshot(namespace, agent) do
    Agent.get(agent, fn state ->
      case Map.get(state, :list_pods_snapshot_error) do
        nil ->
          {:ok,
           %{
             items: state |> Map.get(:pods, %{}) |> Map.values() |> Enum.filter(&(get_in(&1, ["metadata", "namespace"]) == namespace)),
             resource_version: Map.get(state, :pod_list_resource_version, "list-rv-9")
           }}

        reason ->
          {:error, reason}
      end
    end)
  end

  @impl true
  def activate_job(namespace, name, uid, resource_version, agent) do
    record_activation_event(agent, :activate_job)
    Agent.get_and_update(agent, &activate_job_state(&1, {namespace, name}, uid, resource_version))
  end

  defp record_activation_event(agent, event) do
    case Agent.get(agent, &Map.get(&1, :event_sink)) do
      sink when is_pid(sink) -> send(sink, {:activation_sequence, event})
      _ -> :ok
    end
  end

  defp activate_job_state(state, key, uid, resource_version) do
    case Map.get(state.jobs, key) do
      %{"metadata" => %{"uid" => ^uid, "resourceVersion" => ^resource_version}, "spec" => %{"suspend" => true}} = job ->
        activate_matching_job(state, key, job)

      _ ->
        {{:error, :activation_precondition_failed}, state}
    end
  end

  defp activate_matching_job(%{activate_error: reason} = state, _key, _job) when not is_nil(reason),
    do: {{:error, reason}, state}

  defp activate_matching_job(state, key, job) do
    active = job |> put_in(["spec", "suspend"], false) |> put_in(["metadata", "resourceVersion"], "18")
    {{:ok, active}, state |> put_in([:jobs, key], active) |> Map.update(:activations, 1, &(&1 + 1))}
  end

  @impl true
  def delete_job(namespace, name, uid, agent) do
    Agent.get_and_update(agent, fn state ->
      case {state.delete_error, Map.get(state, :delete_commit?, false), Map.get(state, :delete_pending?, false)} do
        {nil, _commit?, true} ->
          pending_delete(state, namespace, name, uid)

        {nil, _commit?, false} ->
          delete_from_state(state, namespace, name, uid)

        {reason, true, _pending?} ->
          {_response, next} = delete_from_state(state, namespace, name, uid)
          {{:error, reason}, next}

        {reason, false, _pending?} ->
          {{:error, reason}, state}
      end
    end)
  end

  @impl true
  def delete_suspended_job(namespace, name, uid, resource_version, agent) do
    result =
      Agent.get_and_update(agent, fn state ->
        case Map.get(state.jobs, {namespace, name}) do
          %{
            "metadata" => %{"uid" => ^uid, "resourceVersion" => ^resource_version},
            "spec" => %{"suspend" => true}
          }
          when is_nil(state.delete_error) ->
            delete_and_inject_pod(state, namespace, name, uid)

          _ ->
            {{:error, :suspended_delete_precondition_failed}, state}
        end
      end)

    if Agent.get(agent, &Map.get(&1, :raise_after_suspended_delete, false)), do: raise("delete response lost")
    result
  end

  defp delete_and_inject_pod(state, namespace, name, uid) do
    {reply, next} = delete_from_state(state, namespace, name, uid)

    next =
      case Map.get(state, :pod_injected_on_suspended_delete) do
        {id, pod} -> Map.update(next, :pods, %{id => pod}, &Map.put(&1, id, pod))
        _ -> next
      end

    {reply, next}
  end

  defp pending_delete(state, namespace, name, uid) do
    case Map.get(state.jobs, {namespace, name}) do
      %{"metadata" => %{"uid" => ^uid}} = job ->
        terminating =
          job
          |> put_in(["metadata", "deletionTimestamp"], "2026-09-27T00:00:00Z")
          |> put_in(["metadata", "finalizers"], ["foregroundDeletion"])

        {:ok, %{state | jobs: Map.put(state.jobs, {namespace, name}, terminating), deletes: [uid | state.deletes]}}

      _ ->
        {{:error, :uid_precondition_failed}, state}
    end
  end

  defp delete_from_state(state, namespace, name, uid) do
    key = {namespace, name}

    case Map.get(state.jobs, key) do
      %{"metadata" => %{"uid" => ^uid}} ->
        pods = remaining_pods(state, uid)
        {:ok, state |> Map.put(:pods, pods) |> Map.put(:jobs, Map.delete(state.jobs, key)) |> Map.put(:deletes, [uid | state.deletes])}

      _ ->
        {{:error, :uid_precondition_failed}, state}
    end
  end

  defp remaining_pods(state, uid) do
    pods = Map.get(state, :pods, %{})

    if Map.get(state, :delete_pods_on_delete?, false) do
      pods
      |> Enum.reject(fn {_pod_id, pod} ->
        pod
        |> get_in(["metadata", "ownerReferences"])
        |> List.wrap()
        |> Enum.any?(&(&1["uid"] == uid))
      end)
      |> Map.new()
    else
      pods
    end
  end

  defp get_job_state(%{get_error: error} = state, _key) when not is_nil(error), do: {error, state}

  defp get_job_state(state, key) do
    case Map.get(state.jobs, key) do
      nil -> {{:error, :not_found}, state}
      job -> {{:ok, job}, state}
    end
  end

  defp create_job_state(state, namespace, job) do
    key = {namespace, job["metadata"]["name"]}

    case Map.fetch(state.jobs, key) do
      {:ok, _existing} ->
        {{:error, :already_exists}, state}

      :error ->
        create_new(state, key, job)
    end
  end

  defp create_new(%{create_commit?: false, create_error: reason} = state, _key, _job) when not is_nil(reason),
    do: {{:error, reason}, %{state | creates: state.creates + 1}}

  defp create_new(state, key, job) do
    stored = defaulted_job(job)
    next = %{state | jobs: Map.put(state.jobs, key, stored), creates: state.creates + 1}
    {create_result(state.create_error, stored), next}
  end

  defp create_result(nil, job), do: {:ok, job}
  defp create_result(:invalid_response, _job), do: :unexpected_create_response
  defp create_result(reason, _job), do: {:error, reason}

  defp defaulted_job(job) do
    uid = "uid-" <> String.slice(job["metadata"]["name"], -8, 8)
    name = job["metadata"]["name"]

    generated = %{
      "batch.kubernetes.io/controller-uid" => uid,
      "batch.kubernetes.io/job-name" => name
    }

    selector = %{"batch.kubernetes.io/controller-uid" => uid}

    job
    |> put_in(["metadata", "uid"], uid)
    |> put_in(["metadata", "resourceVersion"], "17")
    |> put_in(["metadata", "generation"], 1)
    |> put_in(["metadata", "creationTimestamp"], "2026-09-26T00:00:00Z")
    |> put_in(["metadata", "managedFields"], [%{"manager" => "kube-controller-manager", "operation" => "Update"}])
    |> put_in(["metadata", "labels"], Map.merge(get_in(job, ["metadata", "labels"]), generated))
    |> put_in(["metadata", "annotations"], Map.put(get_in(job, ["metadata", "annotations"]), "batch.kubernetes.io/job-tracking", ""))
    |> put_in(["spec", "completions"], 1)
    |> put_in(["spec", "parallelism"], 1)
    |> put_in(["spec", "completionMode"], "NonIndexed")
    |> put_in(["spec", "manualSelector"], false)
    |> put_in(["spec", "suspend"], true)
    |> put_in(["spec", "podReplacementPolicy"], "TerminatingOrFailed")
    |> put_in(["spec", "selector"], %{"matchLabels" => selector})
    |> put_in(["spec", "template", "metadata", "creationTimestamp"], nil)
    |> put_in(["spec", "template", "metadata", "labels"], Map.merge(get_in(job, ["spec", "template", "metadata", "labels"]), generated))
    |> put_in(["spec", "template", "spec", "dnsPolicy"], "ClusterFirst")
    |> put_in(["spec", "template", "spec", "schedulerName"], "default-scheduler")
    |> put_in(["spec", "template", "spec", "terminationGracePeriodSeconds"], 30)
    |> put_in(["spec", "template", "spec", "enableServiceLinks"], true)
    |> put_in(["spec", "template", "spec", "preemptionPolicy"], "PreemptLowerPriority")
    |> update_in(["spec", "template", "spec"], &Map.put_new(&1, "serviceAccountName", "default"))
    |> update_in(["spec", "template", "spec", "containers", Access.at(0)], fn container ->
      container
      |> Map.put_new("terminationMessagePath", "/dev/termination-log")
      |> Map.put_new("terminationMessagePolicy", "File")
    end)
  end
end
