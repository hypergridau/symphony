defmodule SymphonyElixir.WorkerCanaryEventSinkTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.Worker.CanaryEventSink, as: Sink

  test "collects fragmented JSON and CRLF without retaining IDs, messages or usage" do
    lines = [
      %{"type" => "thread.started", "thread_id" => "synthetic-secret-id"},
      %{"type" => "item.updated", "item" => %{"type" => "agent_message", "text" => "verified"}},
      %{"type" => "item.completed", "item" => %{"type" => "agent_message", "text" => " verified\n", "id" => "synthetic-secret-id"}},
      %{"type" => "turn.completed", "usage" => %{"synthetic-secret-usage" => 1}}
    ]

    data = Enum.map_join(lines, "", &(Jason.encode!(&1) <> "\r\n"))
    sink = Enum.into(for(<<byte <- data>>, do: <<byte>>), %Sink{})
    assert sink.thread_started and sink.turn_completed and sink.response_verified
    refute sink.response_invalid
    assert sink.buffer == <<>>
    assert sink.last_event == :turn_completed
    refute Jason.encode!(Sink.summary(sink)) =~ "synthetic-secret"
    assert Sink.summary(Map.put(sink, :unexpected_payload, "synthetic-secret")) == Sink.summary(sink)
    assert inspect(sink) == "#CanaryEventSink<redacted>"
    {acc, collector} = Collectable.into(%Sink{})
    assert collector.(acc, :halt) == :ok
  end

  test "updated messages cannot count as a verified response and failures stay sticky" do
    events = [
      %{"type" => "item.updated", "item" => %{"type" => "agent_message", "text" => "verified"}},
      %{"type" => "turn.failed", "error" => %{"message" => "synthetic-secret-error"}},
      %{"type" => "error", "message" => "synthetic-secret-error"},
      %{"type" => "item.completed", "item" => %{"type" => "error", "message" => "synthetic-secret-item"}},
      %{"type" => "turn.completed"}
    ]

    sink = Enum.into(Enum.map(events, &(Jason.encode!(&1) <> "\n")), %Sink{})
    refute sink.response_verified
    assert sink.turn_failed and sink.error_seen and sink.item_error_seen and sink.turn_completed
    refute Jason.encode!(Sink.summary(sink)) =~ "synthetic-secret"
  end

  test "overlong lines across chunks drop through newline and resume with bounded detached storage" do
    sink = Sink.feed(%Sink{}, String.duplicate("x", 8192))
    assert byte_size(sink.buffer) == 8192
    sink = Sink.feed(sink, "x" <> String.duplicate("synthetic-secret", 10_000))
    assert sink.overflow and sink.dropping_line and sink.buffer == <<>>
    sink = Sink.feed(sink, "ignored\n{\"type\":\"turn.started\"}\ntail")
    assert sink.turn_started and sink.overflow
    assert sink.buffer == "tail"
    assert :binary.referenced_byte_size(sink.buffer) == 4
    assert Sink.finish(sink).buffer == <<>>
    refute Jason.encode!(Sink.summary(sink)) =~ "tail"
  end

  test "malformed, invalid UTF8 and unknown types never retain payloads or create event atoms" do
    chunks = ["not-json\n", <<255, 10>>, "[]\n", "{\"type\":\"synthetic-secret-unknown\",\"message\":\"synthetic-secret\"}\n", "{\"type\":\"error\"}"]
    sink = Enum.into(chunks, %Sink{})
    assert sink.malformed
    refute sink.error_seen
    assert sink.last_event == nil and sink.buffer == <<>>
    refute Jason.encode!(Sink.summary(sink)) =~ "synthetic-secret"
  end

  test "fixed stderr discard wrapper keeps stderr outside the JSON stream" do
    if match?({:unix, _}, :os.type()) do
      {sink, 0} =
        System.cmd("/bin/sh", ["-c", "exec \"$@\" 2>/dev/null", "auth-canary", "/bin/sh", "-c", "printf '%s\\n' '{\"type\":\"turn.started\"}'; printf '%s\\n' 'synthetic-secret-stderr' >&2"],
          stderr_to_stdout: true,
          into: %Sink{}
        )

      assert sink.turn_started
      refute sink.malformed
      refute Jason.encode!(Sink.summary(sink)) =~ "synthetic-secret"
    end
  end
end
