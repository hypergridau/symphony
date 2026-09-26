defmodule SymphonyElixir.RKE2JobFakeClient do
  @behaviour SymphonyElixir.RKE2Job.Client

  @impl true
  def create_job(namespace, job, agent) do
    Agent.get_and_update(agent, &create_job_state(&1, namespace, job))
  end

  @impl true
  def get_job(namespace, name, agent) do
    Agent.get_and_update(agent, &get_job_state(&1, {namespace, name}))
  end

  @impl true
  def activate_job(namespace, name, uid, resource_version, agent) do
    Agent.get_and_update(agent, &activate_job_state(&1, {namespace, name}, uid, resource_version))
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
      case {state.delete_error, Map.get(state, :delete_commit?, false)} do
        {nil, _commit?} ->
          delete_from_state(state, namespace, name, uid)

        {reason, true} ->
          {_response, next} = delete_from_state(state, namespace, name, uid)
          {{:error, reason}, next}

        {reason, false} ->
          {{:error, reason}, state}
      end
    end)
  end

  defp delete_from_state(state, namespace, name, uid) do
    key = {namespace, name}

    case Map.get(state.jobs, key) do
      %{"metadata" => %{"uid" => ^uid}} ->
        {:ok, %{state | jobs: Map.delete(state.jobs, key), deletes: [uid | state.deletes]}}

      _ ->
        {{:error, :uid_precondition_failed}, state}
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
    |> put_in(["spec", "template", "spec", "serviceAccountName"], "default")
    |> put_in(["spec", "template", "spec", "containers", Access.at(0), "terminationMessagePath"], "/dev/termination-log")
    |> put_in(["spec", "template", "spec", "containers", Access.at(0), "terminationMessagePolicy"], "File")
  end
end
