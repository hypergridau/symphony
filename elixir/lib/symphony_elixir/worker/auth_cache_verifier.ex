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

  @codex_home "/var/lib/frigga-codex-home"
  @auth_file Path.join(@codex_home, "auth.json")
  @max_auth_bytes 10_000_000
  @path "/opt/codex/node_modules/.bin:/usr/local/bin:/usr/bin:/bin"

  @type outcome :: %{exit_code: 0 | 1, result: map()}

  @doc "Returns only normalized cache status and a bounded file byte count."
  def run(env, deps \\ %{})

  @spec run(map(), map()) :: outcome()
  def run(env, deps) when is_map(env) and is_map(deps) do
    stat = Map.get(deps, :stat, &File.lstat/1)
    read_auth = Map.get(deps, :read_auth, &File.read/1)
    auth_shape = Map.get(deps, :auth_shape, fn -> oauth_cache_shape?(read_auth) end)
    canary = Map.get(deps, :canary, &oauth_canary/0)

    with true <- supported_host?(deps),
         true <- env["CODEX_HOME"] == @codex_home,
         {:ok, before} <- stat.(@auth_file),
         true <- valid_auth_file?(before),
         true <- auth_shape.(),
         0 <- canary.(),
         {:ok, after_stat} <- stat.(@auth_file),
         true <- valid_auth_file?(after_stat),
         true <- auth_shape.() do
      %{exit_code: 0, result: result("codex_login_status_authenticated", after_stat.size)}
    else
      _ -> %{exit_code: 1, result: result("unverified", 0)}
    end
  rescue
    _ -> %{exit_code: 1, result: result("unverified", 0)}
  end

  def run(_env, _deps), do: %{exit_code: 1, result: result("unverified", 0)}

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
    args = [
      "--kill-after=5s",
      "90s",
      "/usr/bin/env",
      "-i",
      "PATH=" <> @path,
      "HOME=/tmp",
      "CODEX_HOME=" <> @codex_home,
      "codex",
      "exec",
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

    case System.cmd("/usr/bin/timeout", args,
           stderr_to_stdout: true,
           into: %OutputSink{}
         ) do
      {_discarded, status} -> status
    end
  end
end
