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
end
