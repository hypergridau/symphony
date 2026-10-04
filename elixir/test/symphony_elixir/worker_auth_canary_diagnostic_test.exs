defmodule SymphonyElixir.WorkerAuthCanaryDiagnosticTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.Worker.CanaryEventSink, as: Sink
  alias SymphonyElixir.Worker.CLI

  @env %{"CODEX_HOME" => "/var/lib/frigga-codex-home"}
  @stat %{type: :regular, size: 4034, mode: 0o600}

  defp deps do
    %{windows_test_only: true, stat: fn _ -> {:ok, @stat} end, auth_shape: fn -> true end}
  end

  defp verified do
    item = %{"type" => "agent_message", "text" => "verified"}

    Enum.into(
      [
        Jason.encode!(%{"type" => "item.completed", "item" => item}) <> "\n",
        Jason.encode!(%{"type" => "turn.completed"}) <> "\n"
      ],
      %Sink{}
    )
  end

  test "preflight failures never invoke a canary or disclose cache content" do
    caller = self()

    base =
      Map.put(deps(), :canary_events, fn ->
        send(caller, :canary)
        {verified(), 0}
      end)

    for {phase, env, changed} <- [
          {"invalid_context", %{}, base},
          {"cache_unavailable", @env, %{base | stat: fn _ -> {:error, :enoent} end}},
          {"cache_custody_invalid", @env, %{base | stat: fn _ -> {:ok, %{@stat | mode: 0o660}} end}},
          {"cache_shape_invalid", @env, %{base | auth_shape: fn -> false end}}
        ] do
      outcome = CLI.run(["--diagnose-auth-canary"], env, changed)
      assert %{exit_code: 1, result: %{"phase" => ^phase, "canaryExit" => "not_started"}} = outcome
      refute_receive :canary
    end
  end

  test "requires successful exit, completed turn and only the completed verified response" do
    for sink <- [
          %Sink{},
          %{verified() | turn_completed: false},
          %{verified() | response_verified: false},
          %{verified() | turn_failed: true},
          %{verified() | error_seen: true},
          %{verified() | item_error_seen: true},
          %{verified() | model_rerouted: true},
          %{verified() | other_item_error_seen: true},
          %{verified() | response_invalid: true},
          %{verified() | overflow: true},
          %{verified() | malformed: true}
        ] do
      assert %{exit_code: 1, result: %{"phase" => "canary_response_unverified"}} =
               CLI.run(["--diagnose-auth-canary"], @env, Map.put(deps(), :canary_events, fn -> {sink, 0} end))
    end

    for code <- [1, 124, 137] do
      phase = if code in [124, 137], do: "canary_timeout", else: "canary_failed"

      assert %{exit_code: 1, result: %{"phase" => ^phase}} =
               CLI.run(["--diagnose-auth-canary"], @env, Map.put(deps(), :canary_events, fn -> {verified(), code} end))
    end
  end

  test "bounded fixed invocation discards stderr and exposes no verifier fields" do
    caller = self()

    command = fn executable, args, opts ->
      send(caller, {:command, executable, args, opts})
      {verified(), 0}
    end

    assert %{exit_code: 0, result: result} = CLI.run(["--diagnose-auth-canary"], @env, Map.put(deps(), :cmd, command))
    assert_receive {:command, "/usr/bin/timeout", args, opts}
    assert Enum.take(args, 4) == ["--kill-after=5s", "90s", "/usr/bin/env", "-i"]
    assert "--json" in args and "--ephemeral" in args and "--ignore-user-config" in args and "--ignore-rules" in args
    assert Enum.count(args, &(&1 == "--disable")) == 6
    assert "gpt-6-luna" in args and "model_reasoning_effort=\"high\"" in args
    assert List.last(args) == "Reply with the single word verified. Do not use tools."
    assert opts[:into] == %Sink{} and opts[:stderr_to_stdout]
    assert opts[:discard_stderr] == true
    assert Enum.sort(Map.keys(result)) == ["canaryExit", "contractVersion", "events", "phase", "status"]
    assert result["contractVersion"] == "symphony-auth-canary-diagnostic.v2"

    for key <- ["authCacheStatus", "authCacheBytes", "schemaVersion", "acceptedHead", "cleanupReceipt"] do
      refute Map.has_key?(result, key)
    end
  end

  test "a completed verified turn cannot override either completed error item classification" do
    for message <- ["model rerouted: synthetic-secret", "synthetic-secret-warning", nil] do
      event = %{"type" => "item.completed", "item" => %{"type" => "error", "message" => message}}
      sink = Sink.feed(verified(), Jason.encode!(event) <> "\n")
      caller = self()

      stat = fn _ ->
        send(caller, :stat)
        {:ok, @stat}
      end

      base = %{deps() | stat: stat}
      outcome = CLI.run(["--diagnose-auth-canary"], @env, Map.put(base, :canary_events, fn -> {sink, 0} end))
      assert %{exit_code: 1, result: %{"phase" => "canary_response_unverified", "events" => events}} = outcome
      assert events.item_error_seen
      assert events.model_rerouted == (message == "model rerouted: synthetic-secret")
      assert events.other_item_error_seen == (message != "model rerouted: synthetic-secret")
      assert_receive :stat
      refute_receive :stat
      refute Jason.encode!(outcome.result) =~ "synthetic-secret"
    end
  end

  test "post-canary cache refresh remains allowed but private custody and shape are required" do
    base = Map.put(deps(), :canary_events, fn -> {verified(), 0} end)

    for {phase, after_stat} <- [
          {"post_canary_cache_unavailable", {:error, :enoent}},
          {"post_canary_cache_custody_invalid", {:ok, %{@stat | mode: 0o660}}}
        ] do
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stat = fn _ ->
        Agent.get_and_update(counter, fn n ->
          observed = if n == 0, do: {:ok, @stat}, else: after_stat
          {observed, n + 1}
        end)
      end

      outcome = CLI.run(["--diagnose-auth-canary"], @env, %{base | stat: stat})
      assert %{exit_code: 1, result: %{"phase" => ^phase}} = outcome
      Agent.stop(counter)
    end

    {:ok, counter} = Agent.start_link(fn -> 0 end)
    shape = fn -> Agent.get_and_update(counter, fn n -> {n == 0, n + 1} end) end
    outcome = CLI.run(["--diagnose-auth-canary"], @env, %{base | auth_shape: shape})
    assert %{exit_code: 1, result: %{"phase" => "post_canary_cache_shape_invalid"}} = outcome
    Agent.stop(counter)
  end

  test "exceptions and invalid dependency results cannot leak their payload" do
    exception = fn -> raise "synthetic-secret" end
    throwing = fn -> throw("synthetic-secret") end
    invalid = fn -> {"synthetic-secret", 0} end

    for canary <- [exception, throwing, invalid] do
      assert %{exit_code: 1, result: %{"phase" => "unexpected_failure"} = result} =
               CLI.run(["--diagnose-auth-canary"], @env, Map.put(deps(), :canary_events, canary))

      refute Jason.encode!(result) =~ "synthetic-secret"
    end
  end
end
