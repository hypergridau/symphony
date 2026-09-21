defmodule SymphonyElixir.TerminalOutcome do
  @moduledoc "Maps a terminal tracker state to the provider's outcome vocabulary."

  @spec for_tracker_state(term()) :: :completed | :failed | :blocked
  def for_tracker_state(state) when is_binary(state) do
    case String.downcase(String.trim(state)) do
      "" -> :blocked
      "done" -> :completed
      _terminal_state -> :failed
    end
  end

  def for_tracker_state(_state), do: :blocked
end
