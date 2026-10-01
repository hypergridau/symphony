defmodule SymphonyElixir.CLITest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.CLI

  @ack_flag "--i-understand-that-this-will-be-running-without-the-usual-guardrails"
  @activate_flag "--activate-responsibility-graph"

  test "issuer context preflight preserves configuration reasons and redacts arbitrary error text" do
    args = ["--verify-hgs740-issuer-context", "--workflow", "/trusted/WORKFLOW.md", "midgard"]

    assert {:error, "missing_linear_api_token"} =
             CLI.evaluate_hgs740_issuer_context(args, fn workflow, pool ->
               assert {workflow, pool} == {"/trusted/WORKFLOW.md", "midgard"}
               {:error, :missing_linear_api_token}
             end)

    assert {:error, "invalid_authority_or_evidence"} =
             CLI.evaluate_hgs740_issuer_context(args, fn _, _ -> {:error, "secret provider response"} end)

    assert {:error, "configured_state_path_mismatch"} =
             CLI.evaluate_hgs740_issuer_context(args, fn _, _ -> {:error, {:configured_state_path_mismatch, "secret detail"}} end)

    assert {:error, _usage} =
             CLI.evaluate_hgs740_issuer_context(args ++ ["--bundle", "/not/read"], fn _, _ ->
               flunk("malformed context preflight must not run")
             end)
  end

  test "routes HGS-740 startup verification before ordinary service startup" do
    parent = self()

    assert :ok =
             CLI.evaluate_hgs740(
               ["--verify-hgs740-startup", "--workflow", "/trusted/WORKFLOW.md", "midgard"],
               fn workflow, pool ->
                 send(parent, {:verify, workflow, pool})
                 :ok
               end,
               fn _, _, _, _ -> flunk("startup verification must not apply a transition") end,
               fn _, _, _ -> flunk("startup verification must not complete a transaction") end
             )

    assert_received {:verify, "/trusted/WORKFLOW.md", "midgard"}
  end

  test "routes exact HGS-740 apply arguments to the root-only transaction" do
    parent = self()

    assert :ok =
             CLI.evaluate_hgs740(
               [
                 "--apply-hgs740-confirmed-recovery",
                 "--workflow",
                 "/trusted/WORKFLOW.md",
                 "--nonce",
                 "11111111-2222-4333-8444-555555555501",
                 "24e34a86-b214-41bc-8a35-9e1d31bfb8e4",
                 "midgard"
               ],
               fn _, _ -> flunk("apply must not invoke startup verification") end,
               fn issue, pool, workflow, nonce ->
                 send(parent, {:apply, issue, pool, workflow, nonce})
                 {:ok, :applied}
               end,
               fn _, _, _ -> flunk("apply must not complete a transaction") end
             )

    assert_received({:apply, "24e34a86-b214-41bc-8a35-9e1d31bfb8e4", "midgard", "/trusted/WORKFLOW.md", "11111111-2222-4333-8444-555555555501"})
  end

  test "routes exact HGS-740 issuance arguments to the root-only issuer" do
    assert :ok =
             CLI.evaluate_hgs740_issue(
               [
                 "--issue-hgs740-confirmed-recovery",
                 "--workflow",
                 "/trusted/workflow.md",
                 "--nonce",
                 "11111111-2222-4333-8444-555555555501",
                 "--bundle",
                 "/root/input.json",
                 "24e34a86-b214-41bc-8a35-9e1d31bfb8e4",
                 "midgard"
               ],
               fn issue_id, pool, workflow_path, nonce, bundle_path ->
                 assert issue_id == "24e34a86-b214-41bc-8a35-9e1d31bfb8e4"
                 assert pool == "midgard"
                 assert workflow_path == "/trusted/workflow.md"
                 assert nonce == "11111111-2222-4333-8444-555555555501"
                 assert bundle_path == "/root/input.json"
                 :ok
               end
             )
  end

  test "rejects HGS-740 issuance argument drift without invoking issuer" do
    assert {:error, _} = CLI.evaluate_hgs740_issue(["--issue-hgs740-confirmed-recovery", "--bundle", "/tmp/input.json"], fn _, _, _, _, _ -> flunk("issuer must not run") end)
  end

  test "routes HGS-740 completion to the root-only final proof verifier" do
    parent = self()

    assert :ok =
             CLI.evaluate_hgs740(
               ["--complete-hgs740-recovery", "--workflow", "/trusted/WORKFLOW.md", "24e34a86-b214-41bc-8a35-9e1d31bfb8e4", "midgard"],
               fn _, _ -> flunk("completion must not invoke startup verification") end,
               fn _, _, _, _ -> flunk("completion must not apply a transition") end,
               fn issue, pool, workflow ->
                 send(parent, {:complete, issue, pool, workflow})
                 :ok
               end
             )

    assert_received {:complete, "24e34a86-b214-41bc-8a35-9e1d31bfb8e4", "midgard", "/trusted/WORKFLOW.md"}
  end

  test "rejects HGS-740 argument drift without invoking either operation" do
    assert {:error, message} =
             CLI.evaluate_hgs740(
               ["--verify-hgs740-startup", "midgard"],
               fn _, _ -> flunk("malformed startup args must be rejected") end,
               fn _, _, _, _ -> flunk("malformed apply args must be rejected") end,
               fn _, _, _ -> flunk("malformed completion args must be rejected") end
             )

    assert message =~ "Usage: symphony --verify-hgs740-startup"
  end

  test "returns the guardrails acknowledgement banner when the flag is missing" do
    parent = self()

    deps = %{
      file_regular?: fn _path ->
        send(parent, :file_checked)
        true
      end,
      set_workflow_file_path: fn _path ->
        send(parent, :workflow_set)
        :ok
      end,
      set_logs_root: fn _path ->
        send(parent, :logs_root_set)
        :ok
      end,
      set_server_port_override: fn _port ->
        send(parent, :port_set)
        :ok
      end,
      ensure_all_started: fn ->
        send(parent, :started)
        {:ok, [:symphony_elixir]}
      end
    }

    assert {:error, banner} = CLI.evaluate(["WORKFLOW.md"], deps)
    assert banner =~ "This Symphony implementation is a low key engineering preview."
    assert banner =~ "Codex will run without any guardrails."
    assert banner =~ "SymphonyElixir is not a supported product and is presented as-is."
    assert banner =~ @ack_flag
    refute_received :file_checked
    refute_received :workflow_set
    refute_received :logs_root_set
    refute_received :port_set
    refute_received :started
  end

  test "defaults to WORKFLOW.md when workflow path is missing" do
    deps = %{
      file_regular?: fn path -> Path.basename(path) == "WORKFLOW.md" end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert :ok = CLI.evaluate([@ack_flag], deps)
  end

  test "uses an explicit workflow path override when provided" do
    parent = self()
    workflow_path = "tmp/custom/WORKFLOW.md"
    expanded_path = Path.expand(workflow_path)

    deps = %{
      file_regular?: fn path ->
        send(parent, {:workflow_checked, path})
        path == expanded_path
      end,
      set_workflow_file_path: fn path ->
        send(parent, {:workflow_set, path})
        :ok
      end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert :ok = CLI.evaluate([@ack_flag, workflow_path], deps)
    assert_received {:workflow_checked, ^expanded_path}
    assert_received {:workflow_set, ^expanded_path}
  end

  test "accepts --logs-root and passes an expanded root to runtime deps" do
    parent = self()

    deps = %{
      file_regular?: fn _path -> true end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn path ->
        send(parent, {:logs_root, path})
        :ok
      end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert :ok = CLI.evaluate([@ack_flag, "--logs-root", "tmp/custom-logs", "WORKFLOW.md"], deps)
    assert_received {:logs_root, expanded_path}
    assert expanded_path == Path.expand("tmp/custom-logs")
  end

  test "returns not found when workflow file does not exist" do
    deps = %{
      file_regular?: fn _path -> false end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert {:error, message} = CLI.evaluate([@ack_flag, "WORKFLOW.md"], deps)
    assert message =~ "Workflow file not found:"
  end

  test "returns startup error when app cannot start" do
    deps = %{
      file_regular?: fn _path -> true end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:error, :boom} end
    }

    assert {:error, message} = CLI.evaluate([@ack_flag, "WORKFLOW.md"], deps)
    assert message =~ "Failed to start Symphony with workflow"
    assert message =~ ":boom"
  end

  test "returns ok when workflow exists and app starts" do
    deps = %{
      file_regular?: fn _path -> true end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert :ok = CLI.evaluate([@ack_flag, "WORKFLOW.md"], deps)
  end

  test "activates responsibility graph only after the runtime starts" do
    parent = self()

    deps = %{
      file_regular?: fn _path -> true end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn ->
        send(parent, :runtime_started)
        {:ok, [:symphony_elixir]}
      end,
      activate_responsibility_graph: fn now_ms ->
        send(parent, {:activation_requested, now_ms})
        :ok
      end
    }

    assert :ok = CLI.evaluate([@ack_flag, @activate_flag, "WORKFLOW.md"], deps)
    assert_received :runtime_started
    assert_received {:activation_requested, now_ms}
    assert is_integer(now_ms)
  end

  test "activation fails closed when the local callback is unavailable" do
    deps = %{
      file_regular?: fn _path -> true end,
      set_workflow_file_path: fn _path -> :ok end,
      set_logs_root: fn _path -> :ok end,
      set_server_port_override: fn _port -> :ok end,
      ensure_all_started: fn -> {:ok, [:symphony_elixir]} end
    }

    assert {:error, "Responsibility graph activation is unavailable"} =
             CLI.evaluate([@ack_flag, @activate_flag, "WORKFLOW.md"], deps)
  end

  test "one-shot retirement CLI path calls its bounded handler without service startup" do
    parent = self()
    workflow_path = "/etc/dahlia-managed-delegations/hgs736-v7/hypergrid-gitops.workflow.md"

    assert {:ok, :retired} =
             CLI.evaluate_unsubmitted_successor_retirement(
               ["--retire-unsubmitted-successor", "--workflow", workflow_path, "HGS-736"],
               fn identifier, path ->
                 send(parent, {:retirement_called, identifier, path})
                 {:ok, :retired}
               end
             )

    assert_received {:retirement_called, "HGS-736", ^workflow_path}
  end

  test "one-shot retirement rejects malformed arguments without calling its handler" do
    parent = self()

    assert {:error, message} =
             CLI.evaluate_unsubmitted_successor_retirement(
               ["--retire-unsubmitted-successor", "HGS-736", "--workflow", "/trusted/workflow.md"],
               fn _identifier, _path -> send(parent, :unexpected_retirement_call) end
             )

    assert message =~ "Usage: symphony --retire-unsubmitted-successor"
    refute_received :unexpected_retirement_call
  end

  test "malformed retirement CLI invocation cannot fall through to application startup" do
    parent = self()

    assert {:retirement, {:error, message}} =
             CLI.dispatch_unsubmitted_successor_retirement(
               ["--retire-unsubmitted-successor", "HGS-736", "--workflow"],
               fn ->
                 send(parent, :application_started)
                 {:ok, [:symphony_elixir]}
               end,
               fn _identifier, _workflow -> send(parent, :retirement_executed) end
             )

    assert message =~ "Usage: symphony --retire-unsubmitted-successor"
    refute_received :application_started
    refute_received :retirement_executed
  end

  test "ordinary CLI invocations retain the normal startup path" do
    parent = self()

    start = fn ->
      send(parent, :application_started)
      {:ok, [:symphony_elixir]}
    end

    assert {:normal, ^start} =
             CLI.dispatch_unsubmitted_successor_retirement(
               ["WORKFLOW.md"],
               start,
               fn _identifier, _workflow -> send(parent, :retirement_executed) end
             )

    refute_received :application_started
    refute_received :retirement_executed
  end
end
