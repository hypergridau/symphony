defmodule SymphonyElixir.RKE2Job.TerminalOwner do
  @moduledoc """
  Reconciles the exact started disposable Job through its durable terminal result.

  A live Job must still have its bound OAuth lease. After exact Job deletion,
  the retained result and slot binding allow the finalizer to replay cleanup.
  """

  alias SymphonyElixir.RKE2Job.HostAllocationContext

  @doc "Finalizes one retained allocation if its exact terminal result is available."
  @spec reconcile(map(), map(), String.t(), map()) :: {:ok, map()} | {:held, term()} | {:error, term()}
  def reconcile(assignment, binding, allocation_id, host_config) do
    with {:ok, context} <- reattach(assignment, binding, allocation_id, host_config) do
      case context.adapter.finalize_terminal_owned(
             %{id: allocation_id, status: :ready},
             assignment,
             assignment.sha256 <> ":finalize",
             context
           ) do
        {:ok, observation} when is_map(observation) -> {:ok, observation}
        {:held, _reason} = held -> held
        {:error, _reason} = error -> error
        _ -> {:held, :invalid_terminal_finalizer_response}
      end
    end
  rescue
    _ -> {:held, :terminal_owner_unavailable}
  end

  defp reattach(assignment, binding, allocation_id, host_config) do
    case HostAllocationContext.reattach_started(assignment, binding, allocation_id, host_config) do
      {:ok, _context} = result -> result
      {:held, _reason} -> HostAllocationContext.reattach_terminal(assignment, binding, allocation_id, host_config)
    end
  end
end
