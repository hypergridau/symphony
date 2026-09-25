defmodule SymphonyElixir.RKE2JobFakeClient do
  @behaviour SymphonyElixir.RKE2Job.Client

  @impl true
  def create_job(namespace, job, agent) do
    Agent.get_and_update(agent, fn state ->
      key = {namespace, job["metadata"]["name"]}

      if Map.has_key?(state.jobs, key) do
        {{:error, :already_exists}, state}
      else
        {{:ok, job}, %{state | jobs: Map.put(state.jobs, key, job), creates: state.creates + 1}}
      end
    end)
  end

  @impl true
  def get_job(namespace, name, agent) do
    case Agent.get(agent, &Map.get(&1.jobs, {namespace, name})) do
      nil -> {:error, :not_found}
      job -> {:ok, job}
    end
  end

  @impl true
  def delete_job(namespace, name, uid, agent) do
    Agent.get_and_update(agent, fn state ->
      key = {namespace, name}

      case Map.get(state.jobs, key) do
        %{"metadata" => %{"uid" => ^uid}} ->
          {:ok, %{state | jobs: Map.delete(state.jobs, key), deletes: [uid | state.deletes]}}

        _ ->
          {{:error, :uid_precondition_failed}, state}
      end
    end)
  end
end
