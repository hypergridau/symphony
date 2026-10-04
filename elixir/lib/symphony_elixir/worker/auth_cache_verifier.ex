defmodule SymphonyElixir.Worker.AuthCacheVerifier.OutputSink do
  @moduledoc false
  defstruct []
end

defimpl Collectable, for: SymphonyElixir.Worker.AuthCacheVerifier.OutputSink do
  @impl true
  def into(sink) do
    {sink,
     fn
       acc, {:cont, _chunk} -> acc
       acc, :done -> acc
       _acc, :halt -> :ok
     end}
  end
end

defmodule SymphonyElixir.Worker.AuthCacheVerifier do
  @moduledoc """
  Broker-tokenless check for a detached disposable worker's OAuth slot.

  A dedicated verifier Job mounts the one PVC, runs this fixed command, then
  exits. The trusted host must delete that Job and prove no Pod still consumes
  the claim before using this result in a slot-release receipt.
  """

  import Bitwise, only: [band: 2]

  alias SymphonyElixir.Worker.AuthCacheVerifier.OutputSink
  alias SymphonyElixir.Worker.CanaryEventSink

  @codex_home "/var/lib/frigga-codex-home"
  @auth_file Path.join(@codex_home, "auth.json")
  @max_auth_bytes 10_000_000
  @path "/opt/codex/node_modules/.bin:/usr/local/bin:/usr/bin:/bin"
  @failed_event_fields [:turn_failed, :error_seen, :item_error_seen, :response_invalid, :overflow, :malformed]

  @type outcome :: %{exit_code: 0 | 1, result: map()}

  @doc "Returns only normalized cache status and a bounded file byte count."
  def run(env, deps \\ %{})

  @spec run(map(), map()) :: outcome()
  def run(env, deps) when is_map(env) and is_map(deps) do
    case verify(env, deps) do
      {:ok, bytes} -> %{exit_code: 0, result: result("codex_login_status_authenticated", bytes)}
      {:error, _phase} -> %{exit_code: 1, result: result("unverified", 0)}
    end
  end

  def run(_env, _deps), do: %{exit_code: 1, result: result("unverified", 0)}

  @doc "Operator diagnosis retains only the failed check; it is never a cleanup receipt."
  @spec diagnose(map(), map()) :: outcome()
  def diagnose(env, deps \\ %{}) do
    {status, phase, exit_code} =
      case verify(env, deps) do
        {:ok, _bytes} -> {"passed", "complete", 0}
        {:error, phase} -> {"failed", phase, 1}
      end

    %{
      exit_code: exit_code,
      result: %{"contractVersion" => "symphony-auth-cache-diagnostic.v1", "status" => status, "phase" => phase}
    }
  end

  @doc "Fixed operator canary with finite event metadata; never a verifier or cleanup result."
  @spec diagnose_canary(map(), map()) :: outcome()
  def diagnose_canary(env, deps \\ %{}) do
    {phase, canary_exit, events} = canary_diagnosis(env, deps)
    canary_diagnostic(phase, canary_exit, events)
  rescue
    _ -> canary_diagnostic("unexpected_failure", "unknown", CanaryEventSink.summary(%CanaryEventSink{}))
  catch
    _, _ -> canary_diagnostic("unexpected_failure", "unknown", CanaryEventSink.summary(%CanaryEventSink{}))
  end

  defp canary_diagnosis(env, deps) when is_map(env) and is_map(deps) do
    case preflight(env, deps) do
      :ok ->
        canary = Map.get(deps, :canary_events, fn -> oauth_canary_events(deps) end)
        {%CanaryEventSink{} = sink, code} = canary.()
        events = CanaryEventSink.summary(CanaryEventSink.finish(sink))
        phase = event_canary_phase(code, events, deps)
        {phase, canary_exit(code), events}

      {:error, phase} ->
        {phase, "not_started", CanaryEventSink.summary(%CanaryEventSink{})}
    end
  end

  defp canary_diagnosis(_env, _deps),
    do: {"invalid_context", "not_started", CanaryEventSink.summary(%CanaryEventSink{})}

  defp event_canary_phase(0, events, deps) do
    if events.turn_completed and events.response_verified and
         not Enum.any?(@failed_event_fields, &Map.fetch!(events, &1)) do
      case postflight(deps) do
        {:ok, _bytes} -> "complete"
        {:error, phase} -> phase
      end
    else
      "canary_response_unverified"
    end
  end

  defp event_canary_phase(code, _events, _deps) do
    {:error, phase} = canary_result(code)
    phase
  end

  defp canary_exit(0), do: "completed"
  defp canary_exit(code) when code in [124, 137], do: "timeout"
  defp canary_exit(_code), do: "failed"

  defp canary_diagnostic(phase, canary_exit, events) do
    passed = phase == "complete"

    %{
      exit_code: if(passed, do: 0, else: 1),
      result: %{
        "contractVersion" => "symphony-auth-canary-diagnostic.v1",
        "status" => if(passed, do: "passed", else: "failed"),
        "phase" => phase,
        "canaryExit" => canary_exit,
        "events" => events
      }
    }
  end

  defp verify(env, deps) when is_map(env) and is_map(deps) do
    canary = Map.get(deps, :canary, &oauth_canary/0)

    with :ok <- preflight(env, deps),
         :ok <- canary_result(canary.()),
         {:ok, bytes} <- postflight(deps) do
      {:ok, bytes}
    else
      {:error, phase} -> {:error, phase}
    end
  rescue
    _ -> {:error, "unexpected_failure"}
  catch
    _, _ -> {:error, "unexpected_failure"}
  end

  defp verify(_env, _deps), do: {:error, "invalid_context"}

  defp preflight(env, deps) do
    stat = Map.get(deps, :stat, &File.lstat/1)
    read_auth = Map.get(deps, :read_auth, &File.read/1)
    auth_shape = Map.get(deps, :auth_shape, fn -> oauth_cache_shape?(read_auth) end)

    with :ok <- check(supported_host?(deps), "unsupported_host"),
         :ok <- check(env["CODEX_HOME"] == @codex_home, "invalid_context"),
         {:ok, _before} <- private_cache(stat, "cache_unavailable", "cache_custody_invalid"),
         do: check(auth_shape.(), "cache_shape_invalid")
  end

  defp postflight(deps) do
    stat = Map.get(deps, :stat, &File.lstat/1)
    read_auth = Map.get(deps, :read_auth, &File.read/1)
    auth_shape = Map.get(deps, :auth_shape, fn -> oauth_cache_shape?(read_auth) end)

    with {:ok, after_stat} <- private_cache(stat, "post_canary_cache_unavailable", "post_canary_cache_custody_invalid"),
         :ok <- check(auth_shape.(), "post_canary_cache_shape_invalid"),
         do: {:ok, after_stat.size}
  end

  defp check(true, _phase), do: :ok
  defp check(_other, phase), do: {:error, phase}

  defp private_cache(stat, unavailable, custody) do
    case stat.(@auth_file) do
      {:ok, value} -> if valid_auth_file?(value), do: {:ok, value}, else: {:error, custody}
      _ -> {:error, unavailable}
    end
  end

  defp canary_result(0), do: :ok
  defp canary_result(code) when code in [124, 137], do: {:error, "canary_timeout"}
  defp canary_result(_code), do: {:error, "canary_failed"}

  defp supported_host?(deps),
    do: match?({:unix, _}, :os.type()) or (Code.ensure_loaded?(ExUnit) and deps[:windows_test_only] == true)

  defp valid_auth_file?(%{type: :regular, size: size, mode: mode})
       when is_integer(size) and is_integer(mode),
       do: size in 1..@max_auth_bytes and band(mode, 0o077) == 0

  defp valid_auth_file?(_stat), do: false

  defp result(status, bytes),
    do: %{"schemaVersion" => 1, "authCacheStatus" => status, "authCacheBytes" => bytes}

  defp oauth_cache_shape?(read_auth) do
    with {:ok, bytes} <- read_auth.(@auth_file),
         {:ok, %{"auth_mode" => "chatgpt", "tokens" => tokens}} <- Jason.decode(bytes),
         %{"access_token" => access, "refresh_token" => refresh} <- tokens do
      is_binary(access) and byte_size(access) > 0 and
        is_binary(refresh) and byte_size(refresh) > 0
    else
      _ -> false
    end
  end

  defp oauth_canary do
    case System.cmd("/usr/bin/timeout", canary_args([]), stderr_to_stdout: true, into: %OutputSink{}) do
      {_discarded, status} -> status
    end
  end

  defp oauth_canary_events(deps) do
    cmd = Map.get(deps, :cmd, &System.cmd/3)

    # The fixed shell wrapper discards stderr before executing the fixed argv.
    # No caller-provided shell text, paths, flags or model selection is accepted.
    cmd.("/bin/sh", ["-c", "exec \"$@\" 2>/dev/null", "auth-canary", "/usr/bin/timeout" | canary_args(["--json"])],
      stderr_to_stdout: true,
      into: %CanaryEventSink{}
    )
  end

  defp canary_args(extra) do
    [
      "--kill-after=5s",
      "90s",
      "/usr/bin/env",
      "-i",
      "PATH=" <> @path,
      "HOME=/tmp",
      "CODEX_HOME=" <> @codex_home,
      "codex",
      "exec"
    ] ++
      extra ++
      [
        "--disable",
        "shell_tool",
        "--disable",
        "unified_exec",
        "--disable",
        "browser_use",
        "--disable",
        "computer_use",
        "--disable",
        "apps",
        "--disable",
        "code_mode_host",
        "--ignore-user-config",
        "--ignore-rules",
        "--skip-git-repo-check",
        "--ephemeral",
        "--sandbox",
        "read-only",
        "--model",
        "gpt-6-luna",
        "--config",
        "model_reasoning_effort=\"high\"",
        "Reply with the single word verified. Do not use tools."
      ]
  end
end
