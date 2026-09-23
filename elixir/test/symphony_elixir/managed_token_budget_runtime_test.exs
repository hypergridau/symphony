Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.ManagedTokenBudgetRuntimeTest do
  use SymphonyElixir.TestSupport
  alias SymphonyElixir.{ManagedResponsibility, ManagedTokenBudget, ResponsibilityGraph}
  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture
  alias SymphonyElixir.ManagedTokenBudget.Runtime

  setup context do
    options = [
      tracker_kind: "memory",
      max_concurrent_agents: 1,
      codex_stall_timeout_ms: 0,
      codex_max_no_progress_tokens: 0,
      codex_max_total_tokens: context[:configured_limit] || 500_000
    ]

    root = Path.dirname(Workflow.workflow_file_path())
    options = Keyword.put(options, :workspace_root, Path.join(root, "workspaces"))
    write_workflow_file!(Workflow.workflow_file_path(), options)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    now = System.system_time(:millisecond)

    payload =
      update_in(Fixture.payload(now), ["entries"], fn [first, second] ->
        first =
          if context[:progress_scoped] do
            Enum.reduce(["accountable", "responsible"], first, fn role, entry ->
              entry
              |> put_in([role, "budget", "mode"], "progress_scoped")
              |> put_in([role, "budget", "max_tokens"], nil)
            end)
          else
            first
          end

        second = put_in(second, ["accountable", "budget", "max_tokens"], 750_000)
        [first, put_in(second, ["responsible", "budget", "max_tokens"], 750_000)]
      end)

    {:ok, manifest} = ManagedResponsibility.decode(payload, Fixture.context(), now)
    {:ok, graph, _} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), now)
    runtime = %{managed_delegations: manifest}
    state = Fixture.initialize_budget(%Orchestrator.State{responsibility_graph: graph, work_package_runtime: runtime})

    if historical = context[:historical_tokens] do
      {:ok, _} = ManagedTokenBudget.observe(state.managed_token_budget, Fixture.issue(2).id, 1, "retained-history", historical)
    end

    {:ok, state} = Runtime.load(state)
    issue = Fixture.issue(1)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [issue])
    {:ok, state, token, session, _, _} = Orchestrator.admit_execution_for_test(state, issue, nil)
    name = Module.concat(__MODULE__, "Runtime#{System.unique_integer([:positive])}")
    start_options = [name: name, work_package_runtime: state.work_package_runtime]
    child = Supervisor.child_spec({Orchestrator, start_options}, id: name)
    pid = start_supervised!(child)
    assert is_map(Orchestrator.responsibility_snapshot(pid))
    worker = spawn(fn -> Process.sleep(:infinity) end)
    on_exit(fn -> Process.exit(worker, :kill) end)
    at = DateTime.utc_now()

    entry = %{
      pid: worker,
      ref: make_ref(),
      identifier: issue.identifier,
      issue: issue,
      execution_token: token,
      execution_session_id: session,
      session_id: "thread-turn",
      codex_session_identity: %{thread_id: "thread", turn_id: "turn"},
      workspace_path: nil,
      started_at: at,
      last_codex_timestamp: at,
      last_codex_event: :session_started,
      last_codex_message: nil,
      turn_count: 1,
      codex_total_tokens: 0,
      codex_input_tokens: 0,
      codex_output_tokens: 0,
      codex_last_reported_total_tokens: 0
    }

    :sys.replace_state(pid, fn current ->
      %{current | running: %{issue.id => entry}, claimed: MapSet.new([issue.id]), execution_fence: state.execution_fence, responsibility_graph: state.responsibility_graph, tick_token: nil}
    end)

    %{pid: pid, worker: worker, entry: entry, issue: issue, child: child, name: name, state: state, options: options}
  end

  test "real OTP usage rejects stale identity, records queued overshoot, and survives restart", c do
    send_usage(c.pid, %{c.entry | execution_session_id: "stale"}, 999_999)
    assert :sys.get_state(c.pid).codex_issue_totals[c.issue.id] == 0
    send_usage(c.pid, c.entry, 400_000)
    assert :sys.get_state(c.pid).codex_issue_totals[c.issue.id] == 400_000
    send_usage(c.pid, c.entry, 500_001)
    send_usage(c.pid, c.entry, 500_050)
    state = :sys.get_state(c.pid)
    assert state.codex_issue_totals[c.issue.id] == 500_050
    refute Map.has_key?(state.running, c.issue.id)
    refute Process.alive?(c.worker)
    assert state.managed_token_budget_error == nil
    assert Runtime.release_totals(state, c.issue.id)[c.issue.id] == 500_050
    assert {:error, _} = Orchestrator.admit_execution_for_test(state, c.issue, nil)
    assert state.execution_fence == c.state.execution_fence
    stop_supervised!(c.name)
    restarted = start_supervised!(c.child)
    restored = :sys.get_state(restarted)
    assert restored.codex_issue_totals[c.issue.id] == 500_050
    assert {:error, _} = Orchestrator.admit_execution_for_test(restored, c.issue, nil)
    assert restored.running == %{}
  end

  @tag progress_scoped: true
  test "a progress-scoped Luna worker stays live past the former token ceiling", c do
    assert Runtime.effective_limit(:sys.get_state(c.pid), c.issue.id) == {:ok, :unbounded}

    for total <- [500_001, 3_000_001] do
      send_usage(c.pid, c.entry, total)
      state = :sys.get_state(c.pid)
      assert state.codex_issue_totals[c.issue.id] == total
      assert Runtime.release_totals(state, c.issue.id)[c.issue.id] == total
      assert state.running[c.issue.id].pid == c.worker
      assert Process.alive?(c.worker)
      assert state.managed_token_budget_error == nil
      refute Map.has_key?(state.blocked, c.issue.id)

      send(c.pid, :run_poll_cycle)
      polled = :sys.get_state(c.pid)
      assert polled.running[c.issue.id].pid == c.worker
      assert Process.alive?(c.worker)
      assert polled.managed_token_budget_error == nil
      refute Map.has_key?(polled.blocked, c.issue.id)
    end
  end

  test "a changed ledger blocks the running worker and startup even after bytes are restored", c do
    path = c.state.managed_token_budget.path
    before = File.read!(path)
    File.write!(path, before <> "broken\n")
    send_usage(c.pid, c.entry, 10)
    state = :sys.get_state(c.pid)
    assert state.managed_token_budget_error
    assert state.codex_issue_totals[c.issue.id] == 0
    assert state.execution_fence == c.state.execution_fence
    refute Process.alive?(c.worker)
    stop_supervised!(c.name)
    File.write!(path, before)
    assert {:error, _} = start_supervised(c.child)
    assert File.regular?(path <> ".blocked")
  end

  test "unknown issues and generations below the explicit floor cannot admit", c do
    state = :sys.get_state(c.pid)
    unknown = %{c.issue | id: "33333333-3333-4333-8333-333333333333"}
    assert {:error, _} = Orchestrator.admit_execution_for_test(state, unknown, nil)
    assert {:error, _} = Runtime.generation(state, %{issue_id: c.issue.id, generation: 0})
    assert state.execution_fence == c.state.execution_fence
  end

  @tag configured_limit: 750_000, historical_tokens: 600_000
  test "smaller grant stops at equality while larger grant retains its own allowance after restart", c do
    state = :sys.get_state(c.pid)
    larger_id = Fixture.issue(2).id
    assert Runtime.effective_limit(state, c.issue.id) == {:ok, 500_000}
    assert Runtime.effective_limit(state, larger_id) == {:ok, 750_000}
    assert Runtime.admission(state, larger_id) == :ok
    send_usage(c.pid, c.entry, 500_000)
    stopped = :sys.get_state(c.pid)
    refute Process.alive?(c.worker)
    assert stopped.blocked[c.issue.id].stall_diagnostic.total_token_threshold == 500_000
    assert stopped.codex_issue_totals[c.issue.id] == 500_000
    assert stopped.codex_issue_totals[larger_id] == 600_000
    assert {:error, _} = Runtime.admission(stopped, c.issue.id)
    stop_supervised!(c.name)
    restarted = start_supervised!(c.child)
    restored = :sys.get_state(restarted)
    assert restored.codex_issue_totals == stopped.codex_issue_totals
    assert Runtime.admission(restored, larger_id) == :ok
    assert {:error, _} = Runtime.admission(restored, c.issue.id)
  end

  @tag configured_limit: 750_000, historical_tokens: 750_001
  test "larger grant exhaustion remains rejected after a cold restart", c do
    larger_id = Fixture.issue(2).id
    assert {:error, _} = Runtime.admission(:sys.get_state(c.pid), larger_id)
    stop_supervised!(c.name)
    restarted = start_supervised!(c.child)
    restored = :sys.get_state(restarted)
    assert restored.codex_issue_totals[larger_id] == 750_001
    assert {:error, _} = Runtime.admission(restored, larger_id)
  end

  test "a lower configured ceiling stops on polling with the effective diagnostic", c do
    send_usage(c.pid, c.entry, 200_000)
    assert :sys.get_state(c.pid).codex_issue_totals[c.issue.id] == 200_000
    write_workflow_file!(Workflow.workflow_file_path(), Keyword.put(c.options, :codex_max_total_tokens, 200_000))
    send(c.pid, :run_poll_cycle)
    state = :sys.get_state(c.pid)
    refute Process.alive?(c.worker)
    assert state.blocked[c.issue.id].stall_diagnostic.total_token_threshold == 200_000
    assert state.codex_issue_totals[c.issue.id] == 200_000
  end

  test "managed zero cannot skip authority enforcement when other stall guards are disabled", c do
    write_workflow_file!(Workflow.workflow_file_path(), Keyword.put(c.options, :codex_max_total_tokens, 0))
    send(c.pid, :run_poll_cycle)
    state = :sys.get_state(c.pid)
    refute Process.alive?(c.worker)
    assert state.managed_token_budget_error
    assert File.regular?(state.managed_token_budget.path <> ".blocked")
  end

  test "a changed live grant latches and stops while retaining the last usage", c do
    :sys.replace_state(c.pid, fn state ->
      put_in(state.responsibility_graph.delegations["responsible-1"].budget.max_tokens, 499_999)
    end)

    send_usage(c.pid, c.entry, 1)
    state = :sys.get_state(c.pid)
    refute Process.alive?(c.worker)
    assert state.managed_token_budget_error
    assert state.codex_issue_totals[c.issue.id] == 1
    assert File.regular?(state.managed_token_budget.path <> ".blocked")
    stop_supervised!(c.name)
    assert {:error, _} = start_supervised(c.child)
  end

  test "authority failure retains exactly bound queued usage before latching", c do
    :sys.suspend(c.pid)

    :sys.replace_state(c.pid, fn state ->
      put_in(state.responsibility_graph.delegations["responsible-1"].budget.max_tokens, 499_999)
    end)

    send_usage(c.pid, c.entry, 1)
    send_usage(c.pid, c.entry, 25)
    :sys.resume(c.pid)
    state = :sys.get_state(c.pid)
    refute Process.alive?(c.worker)
    assert state.codex_issue_totals[c.issue.id] == 25
    assert state.managed_token_budget_error
    assert File.regular?(state.managed_token_budget.path <> ".blocked")
  end

  test "missing graph and malformed bound lease fail closed without raising", c do
    state = :sys.get_state(c.pid)
    assert {:error, _} = Runtime.effective_limit(%{state | responsibility_graph: nil}, c.issue.id)
    changed = put_in(state.responsibility_graph.delegations["responsible-1"].runtime_lease.generation, 2)
    assert {:error, _} = Runtime.effective_limit(changed, c.issue.id)
    malformed = put_in(state.work_package_runtime.managed_delegations.entries, [%{issue_id: c.issue.id}])
    assert {:error, _} = Runtime.effective_limit(malformed, c.issue.id)

    stripped =
      update_in(state.work_package_runtime.managed_delegations.entries, fn [first | rest] ->
        [%{first | responsible: Map.delete(first.responsible, :authority)} | rest]
      end)

    assert {:error, _} = Runtime.effective_limit(stripped, c.issue.id)
  end

  defp send_usage(pid, entry, total) do
    send(
      pid,
      {:codex_worker_update, entry.issue.id,
       %{
         event: :notification,
         timestamp: DateTime.utc_now(),
         execution_token: entry.execution_token,
         execution_session_id: entry.execution_session_id,
         payload: %{"method" => "thread/tokenUsage/updated", "params" => %{"threadId" => "thread", "tokenUsage" => %{"total" => %{"totalTokens" => total}}}}
       }}
    )
  end
end
