defmodule SymphonyElixir.WorkerAuthCacheVerifierTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Worker.AuthCacheVerifier.OutputSink
  alias SymphonyElixir.Worker.CLI

  @home "/var/lib/frigga-codex-home"
  @auth_file @home <> "/auth.json"
  @stat %{type: :regular, size: 1_024, mode: 0o600, inode: 42, mtime: {{2026, 9, 28}, {15, 0, 0}}}

  test "output sink discards every CLI chunk and halted output" do
    assert Enum.into(["synthetic-access-token", "synthetic-refresh-token"], %OutputSink{}) == %OutputSink{}

    {acc, collector} = Collectable.into(%OutputSink{})
    assert collector.(acc, {:cont, "synthetic-secret"}) == %OutputSink{}
    assert collector.(acc, :done) == %OutputSink{}
    assert collector.(acc, :halt) == :ok
  end

  test "requires ChatGPT OAuth token fields before the model canary" do
    caller = self()
    valid = Jason.encode!(%{"auth_mode" => "chatgpt", "tokens" => %{"access_token" => "synthetic-access", "refresh_token" => "synthetic-refresh"}})

    deps = %{
      windows_test_only: true,
      stat: fn _ -> {:ok, @stat} end,
      read_auth: fn path ->
        send(caller, {:read_auth, path})
        {:ok, valid}
      end,
      canary: fn -> 0 end
    }

    assert %{exit_code: 0} = CLI.run(["--verify-auth-cache"], %{"CODEX_HOME" => @home}, deps)
    assert_receive {:read_auth, @auth_file}
    assert_receive {:read_auth, @auth_file}

    for malformed <- ["{}", "not-json", Jason.encode!(%{"auth_mode" => "api_key", "tokens" => %{}})] do
      assert %{exit_code: 1} =
               CLI.run(["--verify-auth-cache"], %{"CODEX_HOME" => @home}, %{
                 deps
                 | read_auth: fn _ -> {:ok, malformed} end,
                   canary: fn -> send(caller, :unexpected_canary) end
               })

      refute_receive :unexpected_canary
    end
  end

  test "reports only authenticated status after OAuth cache shape and model canary pass" do
    caller = self()

    deps = %{
      windows_test_only: true,
      stat: fn path ->
        send(caller, {:stat, path})
        {:ok, @stat}
      end,
      auth_shape: fn ->
        send(caller, :auth_shape)
        true
      end,
      canary: fn ->
        send(caller, :canary)
        0
      end
    }

    assert %{exit_code: 0, result: result} = CLI.run(["--verify-auth-cache"], %{"CODEX_HOME" => @home}, deps)

    assert result == %{
             "schemaVersion" => 1,
             "authCacheStatus" => "codex_login_status_authenticated",
             "authCacheBytes" => 1_024
           }

    assert_receive {:stat, @auth_file}
    assert_receive :auth_shape
    assert_receive :canary
    assert_receive {:stat, @auth_file}
    assert_receive :auth_shape
    refute_receive _
  end

  test "rejects missing, linked, public, empty, changed and unauthenticated caches" do
    caller = self()
    env = %{"CODEX_HOME" => @home}
    good = fn _ -> {:ok, @stat} end

    verified = fn ->
      send(caller, :canary)
      0
    end

    base = %{windows_test_only: true, stat: good, auth_shape: fn -> true end, canary: verified}

    for bad <- [
          {:error, :enoent},
          {:ok, %{@stat | type: :symlink}},
          {:ok, %{@stat | mode: 0o644}},
          {:ok, %{@stat | mode: 0o660}},
          {:ok, %{@stat | size: 0}},
          {:ok, %{@stat | size: 10_000_001}}
        ] do
      assert %{exit_code: 1, result: %{"authCacheStatus" => "unverified", "authCacheBytes" => 0}} =
               CLI.run(["--verify-auth-cache"], env, %{base | stat: fn _ -> bad end})

      refute_receive :canary
    end

    assert %{exit_code: 1} = CLI.run(["--verify-auth-cache"], env, %{base | auth_shape: fn -> false end})
    refute_receive :canary
    assert %{exit_code: 1} = CLI.run(["--verify-auth-cache"], env, %{base | canary: fn -> 1 end})

    {:ok, counter} = Agent.start_link(fn -> 0 end)

    changed = fn _ ->
      seen = Agent.get_and_update(counter, fn value -> {value, value + 1} end)
      {:ok, if(seen == 0, do: @stat, else: %{@stat | size: 1_025})}
    end

    assert %{exit_code: 0, result: %{"authCacheBytes" => 1_025}} =
             CLI.run(["--verify-auth-cache"], env, %{base | stat: changed})

    assert %{exit_code: 1} = CLI.run(["--verify-auth-cache"], %{"CODEX_HOME" => "/other"}, base)
  end

  test "operator diagnosis identifies the failed check without changing the cleanup result" do
    caller = self()
    env = %{"CODEX_HOME" => @home}

    base = %{
      windows_test_only: true,
      stat: fn _ -> {:ok, @stat} end,
      auth_shape: fn -> true end,
      canary: fn ->
        send(caller, :canary)
        0
      end
    }

    for {phase, changed, context} <- [
          {"invalid_context", base, %{}},
          {"cache_unavailable", %{base | stat: fn _ -> {:error, :enoent} end}, env},
          {"cache_custody_invalid", %{base | stat: fn _ -> {:ok, %{@stat | mode: 0o644}} end}, env},
          {"cache_shape_invalid", %{base | auth_shape: fn -> false end}, env}
        ] do
      assert %{exit_code: 1, result: diagnostic} = CLI.run(["--diagnose-auth-cache"], context, changed)
      assert diagnostic == %{"contractVersion" => "symphony-auth-cache-diagnostic.v1", "status" => "failed", "phase" => phase}
      refute_receive :canary

      assert %{exit_code: 1, result: %{"schemaVersion" => 1, "authCacheStatus" => "unverified", "authCacheBytes" => 0}} =
               CLI.run(["--verify-auth-cache"], context, changed)

      refute_receive :canary
    end

    assert %{exit_code: 0, result: %{"status" => "passed", "phase" => "complete"}} = CLI.run(["--diagnose-auth-cache"], env, base)
    assert_receive :canary
  end

  test "operator diagnosis maps canary failures and exceptions to secret-free finite phases" do
    env = %{"CODEX_HOME" => @home}
    base = %{windows_test_only: true, stat: fn _ -> {:ok, @stat} end, auth_shape: fn -> true end, canary: fn -> 0 end}

    for {phase, canary} <- [
          {"canary_timeout", fn -> 124 end},
          {"canary_timeout", fn -> 137 end},
          {"canary_failed", fn -> 1 end},
          {"canary_failed", fn -> "synthetic-secret-output" end},
          {"unexpected_failure", fn -> raise "synthetic-secret-exception" end},
          {"unexpected_failure", fn -> throw("synthetic-secret-throw") end}
        ] do
      assert %{exit_code: 1, result: diagnostic} = CLI.run(["--diagnose-auth-cache"], env, %{base | canary: canary})
      assert diagnostic == %{"contractVersion" => "symphony-auth-cache-diagnostic.v1", "status" => "failed", "phase" => phase}
      refute Jason.encode!(diagnostic) =~ "synthetic-secret"
      refute Map.has_key?(diagnostic, "authCacheStatus")
    end
  end

  test "operator diagnosis distinguishes post-canary custody and shape after a refresh" do
    env = %{"CODEX_HOME" => @home}
    base = %{windows_test_only: true, stat: fn _ -> {:ok, @stat} end, auth_shape: fn -> true end, canary: fn -> 0 end}

    for {phase, after_stat} <- [
          {"post_canary_cache_unavailable", {:error, :enoent}},
          {"post_canary_cache_custody_invalid", {:ok, %{@stat | mode: 0o644}}}
        ] do
      {:ok, counter} = Agent.start_link(fn -> 0 end)

      stat = fn _ ->
        seen = Agent.get_and_update(counter, fn value -> {value, value + 1} end)
        if seen == 0, do: {:ok, @stat}, else: after_stat
      end

      assert %{exit_code: 1, result: %{"phase" => ^phase}} = CLI.run(["--diagnose-auth-cache"], env, %{base | stat: stat})
      Agent.stop(counter)
    end

    {:ok, counter} = Agent.start_link(fn -> 0 end)
    shape = fn -> Agent.get_and_update(counter, fn value -> {value == 0, value + 1} end) end

    assert %{exit_code: 1, result: %{"phase" => "post_canary_cache_shape_invalid"}} =
             CLI.run(["--diagnose-auth-cache"], env, %{base | auth_shape: shape})

    Agent.stop(counter)
  end
end
