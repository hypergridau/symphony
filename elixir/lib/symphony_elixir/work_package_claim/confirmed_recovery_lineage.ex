defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryLineage do
  @moduledoc false

  @spec released_fence_lease(map(), String.t(), map()) :: :ok | {:error, :execution_lease_not_released}
  def released_fence_lease(fence, issue_id, expected) when is_map(fence) and is_map(expected) do
    candidates = [Map.get(fence.executions, issue_id) | Enum.filter(fence.history, &(&1.issue_id == issue_id))]

    if Enum.any?(candidates, &exact_released_execution?(&1, expected)) do
      :ok
    else
      {:error, :execution_lease_not_released}
    end
  end

  def released_fence_lease(_fence, _issue_id, _expected), do: {:error, :execution_lease_not_released}

  @spec released_graph_lease(map(), map(), integer()) :: :ok | {:error, :responsibility_lease_not_released}
  def released_graph_lease(graph, expected, release_at_ms)
      when is_map(graph) and is_map(expected) and is_integer(release_at_ms) do
    current = Map.get(graph.delegations, expected["responsibleDelegationId"])
    release_event? = release_event?(graph, expected, release_at_ms)

    case current do
      %{runtime_lease: nil} ->
        result(release_event?)

      %{runtime_lease: lease} when is_map(lease) ->
        rebound? = rebound_after_release?(graph, expected, release_at_ms)
        result(release_event? and lease.issue_id == expected["issueId"] and lease.generation >= 3 and rebound?)

      _ ->
        {:error, :responsibility_lease_not_released}
    end
  end

  def released_graph_lease(_graph, _expected, _release_at_ms), do: {:error, :responsibility_lease_not_released}

  defp exact_released_execution?(
         %{
           generation: 2,
           status: :active,
           ownership: :reconciled,
           cleanup: :pending,
           terminal: nil,
           cleanup_receipt: nil,
           retirement: nil,
           termination_unconfirmed: false,
           leases: leases
         },
         expected
       ) do
    case Map.get(leases, expected["sessionId"]) do
      %{
        process_id: process_id,
        status: :released,
        release_reason: :spawn_failed,
        termination_required: false
      } ->
        process_id == expected["processId"]

      _ ->
        false
    end
  end

  defp exact_released_execution?(_execution, _expected), do: false

  defp release_event?(graph, expected, release_at_ms) do
    Enum.any?(graph.events, fn event ->
      event.type == :runtime_lease_released and
        event.delegation_id == expected["responsibleDelegationId"] and event.at_ms == release_at_ms
    end)
  end

  defp rebound_after_release?(graph, expected, release_at_ms) do
    Enum.any?(graph.events, fn event ->
      event.type == :runtime_lease_bound and
        event.delegation_id == expected["responsibleDelegationId"] and event.at_ms > release_at_ms
    end)
  end

  defp result(true), do: :ok
  defp result(false), do: {:error, :responsibility_lease_not_released}
end
