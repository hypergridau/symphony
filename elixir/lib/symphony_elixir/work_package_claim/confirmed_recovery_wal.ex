defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWAL do
  @moduledoc false

  @spec replay([term()], (term() -> :ok | {:error, term()})) :: :ok | {:error, term()}
  def replay(names, apply_one) when is_list(names) and is_function(apply_one, 1) do
    Enum.reduce_while(names, :ok, fn name, :ok ->
      case apply_one.(name) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
        _ -> {:halt, {:error, :invalid_replay_result}}
      end
    end)
  end

  def replay(_names, _apply_one), do: {:error, :invalid_replay_request}
end
