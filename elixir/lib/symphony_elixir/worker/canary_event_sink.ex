defmodule SymphonyElixir.Worker.CanaryEventSink do
  @moduledoc false
  @max_line_bytes 8192
  @events %{
    "thread.started" => :thread_started,
    "turn.started" => :turn_started,
    "turn.completed" => :turn_completed,
    "turn.failed" => :turn_failed,
    "item.started" => :item_started,
    "item.updated" => :item_updated,
    "item.completed" => :item_completed,
    "error" => :error
  }

  defstruct thread_started: false,
            turn_started: false,
            turn_completed: false,
            turn_failed: false,
            error_seen: false,
            item_error_seen: false,
            response_verified: false,
            response_invalid: false,
            overflow: false,
            malformed: false,
            last_event: nil,
            buffer: <<>>,
            dropping_line: false

  @type t :: %__MODULE__{}

  @spec feed(t(), binary()) :: t()
  def feed(state, <<>>), do: state

  def feed(%__MODULE__{} = state, bytes) when is_binary(bytes) do
    case :binary.match(bytes, "\n") do
      :nomatch ->
        consume_segment(state, bytes, false)

      {index, 1} ->
        segment = binary_part(bytes, 0, index)
        rest = binary_part(bytes, index + 1, byte_size(bytes) - index - 1)
        state |> consume_segment(segment, true) |> feed(rest)
    end
  end

  @spec finish(t()) :: t()
  def finish(state), do: %{state | buffer: <<>>, dropping_line: false}

  @spec summary(t()) :: map()
  def summary(state) do
    %{
      thread_started: state.thread_started,
      turn_started: state.turn_started,
      turn_completed: state.turn_completed,
      turn_failed: state.turn_failed,
      error_seen: state.error_seen,
      item_error_seen: state.item_error_seen,
      response_verified: state.response_verified,
      response_invalid: state.response_invalid,
      overflow: state.overflow,
      malformed: state.malformed,
      last_event: state.last_event
    }
  end

  defp consume_segment(%{dropping_line: true} = state, _segment, terminated?) do
    if terminated?, do: %{state | dropping_line: false}, else: state
  end

  defp consume_segment(state, segment, terminated?) do
    if byte_size(state.buffer) + byte_size(segment) > @max_line_bytes do
      %{state | buffer: <<>>, dropping_line: not terminated?, overflow: true}
    else
      line = if state.buffer == <<>>, do: segment, else: state.buffer <> segment

      if terminated? do
        parse_line(%{state | buffer: <<>>}, line)
      else
        %{state | buffer: :binary.copy(line)}
      end
    end
  end

  defp parse_line(state, <<>>), do: state

  defp parse_line(state, line) do
    case Jason.decode(line) do
      {:ok, %{"type" => type} = event} when is_binary(type) ->
        case Map.fetch(@events, type) do
          {:ok, name} -> record_event(%{state | last_event: name}, name, event)
          :error -> state
        end

      _ ->
        %{state | malformed: true}
    end
  end

  defp record_event(state, name, _event)
       when name in [:thread_started, :turn_started, :turn_completed, :turn_failed],
       do: Map.replace!(state, name, true)

  defp record_event(state, :error, _event), do: %{state | error_seen: true}

  defp record_event(state, :item_completed, event) do
    case event do
      %{"item" => %{"type" => "agent_message", "text" => text}} when is_binary(text) ->
        if String.trim(text) == "verified" do
          %{state | response_verified: true}
        else
          %{state | response_invalid: true}
        end

      %{"item" => %{"type" => "error"}} ->
        %{state | item_error_seen: true}

      _ ->
        state
    end
  end

  defp record_event(state, _name, _event), do: state
end

defimpl Inspect, for: SymphonyElixir.Worker.CanaryEventSink do
  import Inspect.Algebra
  @impl true
  def inspect(_state, _opts), do: string("#CanaryEventSink<redacted>")
end

defimpl Collectable, for: SymphonyElixir.Worker.CanaryEventSink do
  @impl true
  def into(state) do
    {state,
     fn
       acc, {:cont, bytes} -> SymphonyElixir.Worker.CanaryEventSink.feed(acc, bytes)
       acc, :done -> SymphonyElixir.Worker.CanaryEventSink.finish(acc)
       _acc, :halt -> :ok
     end}
  end
end
