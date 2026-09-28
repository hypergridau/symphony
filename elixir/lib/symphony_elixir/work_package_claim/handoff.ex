defmodule SymphonyElixir.WorkPackageClaim.Handoff do
  @moduledoc """
  Fail-closed sequencing contract for a controller resuming a suspended allocation.

  The caller must run this synchronously inside the orchestrator's serialized
  admission callback. `begin_intent` owns the final pause decision and durable
  claim activation intent write. `reconcile_intent` verifies that same intent after restart.
  Activation is called only after either callback proves the intent exists.
  """

  @type dispatch :: %{required(:phase) => String.t(), required(:allocation_id) => String.t()}
  @type ports :: %{
          required(:begin_intent) => (String.t() -> :ok | {:error, term()} | {:held, term()}),
          required(:reconcile_intent) => (String.t() -> :ok | {:error, term()} | {:held, term()}),
          required(:activate) => (String.t() -> {:ok, term()} | {:error, term()} | {:held, term()})
        }

  @doc "Runs the durable-intent then activation order for a recorded exact allocation."
  @spec resume(dispatch(), ports()) :: {:ok, term()} | {:error, term()} | {:held, term()}
  def resume(%{phase: phase, allocation_id: allocation_id}, ports)
      when is_binary(allocation_id) and byte_size(allocation_id) > 0 and is_map(ports) do
    with {:ok, intent_fun} <- required_port(ports, :begin_intent),
         {:ok, reconcile_fun} <- required_port(ports, :reconcile_intent),
         {:ok, activate_fun} <- required_port(ports, :activate),
         :ok <- establish_intent(phase, allocation_id, intent_fun, reconcile_fun) do
      invoke_activation(activate_fun, allocation_id)
    end
  end

  def resume(_dispatch, _ports), do: {:held, :suspended_allocation_handoff_invalid}

  defp establish_intent("allocation_suspended", allocation_id, begin_intent, _reconcile_intent),
    do: invoke_intent(begin_intent, allocation_id)

  defp establish_intent("spawn_started", allocation_id, _begin_intent, reconcile_intent),
    do: invoke_intent(reconcile_intent, allocation_id)

  defp establish_intent(_phase, _allocation_id, _begin_intent, _reconcile_intent),
    do: {:held, :suspended_allocation_handoff_phase_invalid}

  defp required_port(ports, name) do
    case Map.get(ports, name) do
      fun when is_function(fun, 1) -> {:ok, fun}
      _ -> {:held, {:suspended_allocation_handoff_port_missing, name}}
    end
  end

  defp invoke_intent(fun, allocation_id) do
    case fun.(allocation_id) do
      :ok -> :ok
      {:error, reason} -> {:error, reason}
      {:held, reason} -> {:held, reason}
      _ -> {:held, :suspended_allocation_intent_unverified}
    end
  rescue
    _error -> {:held, :suspended_allocation_intent_unavailable}
  end

  defp invoke_activation(fun, allocation_id) do
    case fun.(allocation_id) do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
      {:held, reason} -> {:held, reason}
      _ -> {:held, :suspended_allocation_activation_unverified}
    end
  rescue
    _error -> {:held, :suspended_allocation_activation_unavailable}
  end
end
