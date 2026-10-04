defmodule SymphonyElixir.WorkerCommandTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.Worker.CanaryEventSink
  alias SymphonyElixir.Worker.Command

  @moduletag skip: not match?({:unix, _}, :os.type())

  test "a piped stdin reader blocks under System.cmd and completes with explicit EOF" do
    script = "cat >/dev/null; printf '%s\\n' READY"
    args = ["--kill-after=1s", "1s", "/bin/sh", "-c", script]
    assert {"", 124} = System.cmd("/usr/bin/timeout", args, stderr_to_stdout: true)
    assert {"READY\n", 0} = Command.run("/usr/bin/timeout", args, stderr_to_stdout: true)
  end

  test "passes a hostile prompt as literal argv without evaluating shell syntax" do
    prompt = "quotes '\"; $(printf expanded); `printf expanded`\nnext line"
    args = ["-c", "cat >/dev/null; printf '%s' \"$1\"", "fixture", prompt]
    assert {^prompt, 0} = Command.run("/bin/sh", args, stderr_to_stdout: true)
  end

  test "preserves caller options and discards stderr only when requested" do
    script = "cat >/dev/null; printf '%s\\n' READY; printf '%s\\n' synthetic-stderr >&2"
    args = ["-c", script]
    assert {output, 0} = Command.run("/bin/sh", args, stderr_to_stdout: true)
    assert output =~ "READY\n" and output =~ "synthetic-stderr\n"
    assert {"READY\n", 0} = Command.run("/bin/sh", args, stderr_to_stdout: true, discard_stderr: true)

    environment = [{"SYNTHETIC_COMMAND_VALUE", "synthetic-value"}]
    environment_args = ["-c", "printf '%s' \"$SYNTHETIC_COMMAND_VALUE\""]

    assert {"synthetic-value", 0} =
             Command.run("/bin/sh", environment_args, env: environment)
  end

  test "collects finite events after EOF without retaining stderr payloads" do
    script = "cat >/dev/null; printf '%s\\n' '{\"type\":\"turn.started\"}'; printf '%s\\n' synthetic-secret >&2"
    options = [stderr_to_stdout: true, discard_stderr: true, into: %CanaryEventSink{}]
    assert {sink, 0} = Command.run("/bin/sh", ["-c", script], options)
    assert sink.turn_started
    refute sink.malformed
    refute Jason.encode!(CanaryEventSink.summary(sink)) =~ "synthetic-secret"
  end

  test "preserves nonzero exit status and fails closed for missing or non-executable targets" do
    assert {"", 37} = Command.run("/bin/sh", ["-c", "exit 37"], stderr_to_stdout: true)
    assert {"", 127} = Command.run("/symphony-synthetic-missing-stdin-test", [], discard_stderr: true)
    assert {"", 126} = Command.run("/dev/null", [], discard_stderr: true)
  end
end
