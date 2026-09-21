defmodule SymphonyElixir.TerminalOutcomeTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.TerminalOutcome

  test "only Done means completed; canceled and other terminal states are not success" do
    assert TerminalOutcome.for_tracker_state(" Done ") == :completed

    for state <- ["Canceled", "Cancelled", "Duplicate", "Closed", "custom terminal"] do
      assert TerminalOutcome.for_tracker_state(state) == :failed
    end

    assert TerminalOutcome.for_tracker_state("") == :blocked
    assert TerminalOutcome.for_tracker_state(nil) == :blocked
  end
end
