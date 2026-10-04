defmodule SymphonyElixir.WorkerCanaryEventSinkTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.Worker.CanaryEventSink, as: Sink

  @disabled_notice "Code Mode is unavailable because code-mode host is disabled. " <>
                     "Code mode will fail closed; enable `features.code_mode_host` and install `codex-code-mode-host`."

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

    assert Enum.sort(Map.keys(Sink.summary(sink))) ==
             Enum.sort([
               :thread_started,
               :turn_started,
               :turn_completed,
               :turn_failed,
               :error_seen,
               :item_error_seen,
               :model_rerouted,
               :other_item_error_seen,
               :code_mode_disabled,
               :response_verified,
               :response_invalid,
               :overflow,
               :malformed,
               :last_event
             ])

    assert inspect(sink) == "#CanaryEventSink<redacted>"
    {acc, collector} = Collectable.into(%Sink{})
    assert collector.(acc, :halt) == :ok
  end

  test "only the exact completed disabled Code Mode notice has the known notice classification" do
    item = %{"type" => "error", "message" => @disabled_notice, "id" => "synthetic-secret-id"}
    data = Jason.encode!(%{"type" => "item.completed", "item" => item}) <> "\r\n"
    sink = Enum.into(for(<<byte <- data>>, do: <<byte>>), %Sink{})
    assert sink.item_error_seen and sink.code_mode_disabled
    refute sink.model_rerouted or sink.other_item_error_seen
    assert sink.buffer == <<>>
    refute Jason.encode!(Sink.summary(sink)) =~ "synthetic-secret"
    refute Jason.encode!(Sink.summary(sink)) =~ "Code Mode"

    for changed <- [
          %{item | "message" => @disabled_notice <> "\n"},
          %{item | "message" => " " <> @disabled_notice},
          %{item | "message" => @disabled_notice <> "synthetic-secret"},
          %{item | "message" => String.downcase(@disabled_notice)},
          Map.delete(item, "id"),
          %{item | "id" => nil},
          %{item | "id" => ""},
          Map.put(item, "recipient", "code_mode_host")
        ] do
      event = Jason.encode!(%{"type" => "item.completed", "item" => changed}) <> "\n"
      rejected = Enum.into([event], %Sink{})
      assert rejected.item_error_seen and rejected.other_item_error_seen
      refute rejected.code_mode_disabled
      both = Sink.feed(sink, event <> data)
      assert both.code_mode_disabled and both.other_item_error_seen
    end
  end

  test "the exact notice in another event or item type cannot classify an error notice" do
    for event <- [
          %{"type" => "item.updated", "item" => %{"type" => "error", "message" => @disabled_notice, "id" => "item_0"}},
          %{"type" => "item.completed", "item" => %{"type" => "agent_message", "text" => @disabled_notice, "id" => "item_0"}}
        ] do
      sink = Enum.into([Jason.encode!(event) <> "\n"], %Sink{})
      refute sink.code_mode_disabled or sink.item_error_seen
    end
  end

  test "fragmented exact model reroute prefix retains only sticky classification flags" do
    event = %{
      "type" => "item.completed",
      "item" => %{"type" => "error", "message" => "model rerouted: synthetic-secret-models", "id" => "synthetic-secret-id"}
    }

    data = Jason.encode!(event) <> "\r\n"
    sink = Enum.into(for(<<byte <- data>>, do: <<byte>>), %Sink{})
    assert sink.item_error_seen and sink.model_rerouted
    refute sink.other_item_error_seen
    assert sink.buffer == <<>>
    refute Jason.encode!(Sink.summary(sink)) =~ "synthetic-secret"

    warning = Jason.encode!(%{"type" => "item.completed", "item" => %{"type" => "error"}}) <> "\n"
    sink = Sink.feed(sink, warning <> data)
    assert sink.item_error_seen and sink.model_rerouted and sink.other_item_error_seen
    refute Jason.encode!(Sink.summary(sink)) =~ "synthetic-secret"
  end

  test "unrecognized error messages remain failed without inferring a model reroute" do
    for message <- [nil, 1, %{}, "", "model rerouted:", "Model rerouted: synthetic-secret", " model rerouted: synthetic-secret", "synthetic-secret-warning"] do
      event = %{"type" => "item.completed", "item" => %{"type" => "error", "message" => message}}
      sink = Enum.into([Jason.encode!(event) <> "\n"], %Sink{})
      assert sink.item_error_seen and sink.other_item_error_seen
      refute sink.model_rerouted
      refute Jason.encode!(Sink.summary(sink)) =~ "synthetic-secret"
    end
  end

  test "updated error items do not classify completed model reroutes" do
    event = %{"type" => "item.updated", "item" => %{"type" => "error", "message" => "model rerouted: synthetic-secret"}}
    sink = Enum.into([Jason.encode!(event) <> "\n"], %Sink{})
    refute sink.item_error_seen or sink.model_rerouted or sink.other_item_error_seen
    refute Jason.encode!(Sink.summary(sink)) =~ "synthetic-secret"
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
