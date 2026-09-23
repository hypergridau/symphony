Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.ManagedTokenBudgetProgressScopedTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{ExecutionFence, ManagedResponsibility, ManagedTokenBudget, Orchestrator, ResponsibilityGraph}
  alias SymphonyElixir.Codex.ModelRouter
  alias SymphonyElixir.ManagedResponsibility.Admission
  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture
  alias SymphonyElixir.ManagedTokenBudget.{Limit, Runtime}
  alias SymphonyElixir.ResponsibilityGraph.Persistence

  @local_limit 500_000
  @grant_limit 3_000_000

  setup do
    root = Path.dirname(Workflow.workflow_file_path())

    write_workflow_file!(
      Workflow.workflow_file_path(),
      tracker_kind: "memory",
      workspace_root: Path.join(root, "workspaces"),
      codex_max_total_tokens: @local_limit,
      codex_stall_timeout_ms: 0,
      codex_max_no_progress_tokens: 10
    )

    now = System.system_time(:millisecond)
    {:ok, manifest} = ManagedResponsibility.decode(progress_payload(now), Fixture.context(), now)
    {:ok, graph, _} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), now)

    state = %Orchestrator.State{
      execution_fence: ExecutionFence.new(),
      responsibility_graph: graph,
      execution_fence_path: Config.execution_fence_state_path(),
      responsibility_graph_path: Config.responsibility_graph_state_path(),
      work_package_runtime: %{managed_delegations: manifest}
    }

    %{now: now, manifest: manifest, state: Fixture.initialize_budget(state)}
  end

  test "explicit progress_scoped Luna admission has no per-task token ceiling", c do
    issue = Fixture.issue(1)

    assert {:ok, admitted} =
             Admission.prepare(
               c.state.responsibility_graph,
               c.state.execution_fence,
               c.manifest,
               issue,
               nil,
               c.now
             )

    assert admitted.delegations["responsible-1"].budget.mode == :progress_scoped
    assert :ok = Persistence.save(c.state.responsibility_graph_path, admitted)
    assert {:ok, restored} = Persistence.load(c.state.responsibility_graph_path)
    assert restored.delegations["responsible-1"].budget.max_tokens == nil

    assert {:ok, :unbounded, grant} =
             Limit.resolve(@local_limit, c.state.work_package_runtime, issue.id)

    assert grant.budget.mode == :progress_scoped
    assert grant.budget.max_tokens == nil
  end

  test "default GPT-6 Luna route and progress-scoped grant remain aligned across retry and ledger reload", c do
    issue = Fixture.issue(1)
    {:ok, manifest} = ManagedResponsibility.decode(progress_payload(c.now), Fixture.context(), c.now)
    runtime = %{c.state.work_package_runtime | managed_delegations: manifest}
    state = %{c.state | work_package_runtime: runtime}

    for attempt <- [nil, 1, 2, 3] do
      assert {:ok, %{model: "gpt-6-luna"}} = ModelRouter.resolve_managed(issue, attempt)

      assert {:ok, admitted} =
               Admission.prepare(
                 state.responsibility_graph,
                 state.execution_fence,
                 manifest,
                 issue,
                 attempt,
                 c.now
               )

      assert admitted.delegations["responsible-1"].budget.model == "gpt-6-luna"
      assert admitted.delegations["responsible-1"].budget.max_tokens == nil
    end

    assert {:ok, :unbounded, _} = Limit.resolve(@local_limit, state.work_package_runtime, issue.id)

    ledger = observe!(state.managed_token_budget, issue.id, @grant_limit + 100_000)
    {:ok, reloaded} = ManagedTokenBudget.load(ledger.path, ledger.identity)
    restarted = %{state | managed_token_budget: reloaded, codex_issue_totals: reloaded.issue_totals}
    assert Runtime.admission(restarted, issue.id) == :ok
  end

  test "finite grants retain the configured ceiling while progress grants have no task count" do
    issue_id = "issue-limit"
    finite = grant(issue_id, :finite)
    progress = grant(issue_id, :progress_scoped)
    finite_runtime = %{managed_delegations: %{entries: [finite]}}
    progress_runtime = %{managed_delegations: %{entries: [progress]}}

    assert {:ok, @local_limit, _} = Limit.resolve(@local_limit, finite_runtime, issue_id)
    assert {:ok, :unbounded, _} = Limit.resolve(@local_limit, progress_runtime, issue_id)
  end

  test "malformed authority, mode, model, and child limits fail closed" do
    raw = Fixture.payload(System.system_time(:millisecond))["entries"] |> hd() |> Map.fetch!("responsible")

    assert {:error, _} = Persistence.decode_delegation_input(Map.put(raw, "authority", nil))
    assert {:error, _} = Persistence.decode_delegation_input(put_in(raw, ["budget", "mode"], "unbounded"))
    assert {:error, _} = Persistence.decode_delegation_input(put_in(raw, ["budget", "mode"], nil))
    assert {:error, _} = Persistence.decode_delegation_input(put_in(raw, ["budget", "max_children"], -1))
    assert {:error, _} = Persistence.decode_delegation_input(put_in(raw, ["budget", "max_tokens"], nil))

    assert {:error, _} =
             Persistence.decode_delegation_input(
               raw
               |> put_in(["budget", "mode"], "progress_scoped")
               |> put_in(["budget", "max_tokens"], @grant_limit)
             )

    assert {:error, _} =
             Persistence.decode_delegation_input(
               raw
               |> put_in(["budget", "mode"], "progress_scoped")
               |> put_in(["budget", "model"], "gpt-5.6-sol")
             )

    invalid_model = grant("issue-limit", :progress_scoped, "gpt-5.6-sol")

    assert Limit.resolve(@local_limit, %{managed_delegations: %{entries: [invalid_model]}}, "issue-limit") ==
             {:error, :managed_token_budget_unavailable_or_exhausted}
  end

  test "persistence save rejects malformed in-memory modes without raising", c do
    accountable = hd(c.manifest.entries).accountable
    {:ok, graph, _} = ResponsibilityGraph.delegate(c.state.responsibility_graph, accountable, c.now)
    malformed = put_in(graph.delegations[accountable.id].budget.mode, :unbounded)

    assert {:error, :invalid_state} = Persistence.save(c.state.responsibility_graph_path, malformed)
  end

  test "useful progress beyond the former local threshold remains admissible across reload", c do
    issue = Fixture.issue(1)
    total = @local_limit + 250_000
    ledger = observe!(c.state.managed_token_budget, issue.id, total)
    state = %{c.state | managed_token_budget: ledger, codex_issue_totals: ledger.issue_totals}

    assert state.codex_issue_totals[issue.id] == total
    assert Runtime.admission(state, issue.id) == :ok

    {:ok, reloaded} = ManagedTokenBudget.load(ledger.path, ledger.identity)
    restarted = %{state | managed_token_budget: reloaded, codex_issue_totals: reloaded.issue_totals}
    assert restarted.codex_issue_totals[issue.id] == total
    assert Runtime.admission(restarted, issue.id) == :ok

    beyond_old_grant = observe!(reloaded, issue.id, @grant_limit + 100_000)

    after_grant = %{
      restarted
      | managed_token_budget: beyond_old_grant,
        codex_issue_totals: beyond_old_grant.issue_totals
    }

    assert Runtime.admission(after_grant, issue.id) == :ok
  end

  test "progress mode does not disable genuine no-progress controls" do
    assert Config.settings!().codex.max_no_progress_tokens == 10

    manifest = %{managed_delegations: %{entries: [grant("issue-stall", :progress_scoped)]}}

    assert {:ok, :unbounded, _} =
             Limit.resolve(@local_limit, manifest, "issue-stall")
  end

  test "model, scope, child, and parent-mode boundaries remain stable", c do
    issue = Fixture.issue(1)
    assert c.manifest.entries |> hd() |> get_in([:responsible, :scope, :issue_id]) == issue.id
    assert c.manifest.entries |> hd() |> get_in([:responsible, :budget, :model]) == "gpt-6-luna"
    assert c.manifest.entries |> hd() |> get_in([:responsible, :budget, :max_children]) == 0

    assert {:ok, _} = Admission.prepare(c.state.responsibility_graph, c.state.execution_fence, c.manifest, issue, 3, c.now)

    {:ok, stale_manifest} = ManagedResponsibility.decode(progress_payload(c.now, "gpt-5.6-luna"), Fixture.context(), c.now)

    assert {:error, :managed_responsibility_budget_exceeded} =
             Admission.prepare(c.state.responsibility_graph, c.state.execution_fence, stale_manifest, issue, nil, c.now)

    mixed =
      progress_payload(c.now)
      |> update_in(["entries"], fn [entry | rest] ->
        changed =
          entry
          |> put_in(["responsible", "budget", "mode"], "finite")
          |> put_in(["responsible", "budget", "max_tokens"], @grant_limit)

        [changed | rest]
      end)

    {:ok, mixed_manifest} = ManagedResponsibility.decode(mixed, Fixture.context(), c.now)

    assert {:error, :managed_responsibility_budget_mode_mismatch} =
             Admission.prepare(c.state.responsibility_graph, c.state.execution_fence, mixed_manifest, issue, nil, c.now)

    bad_scope = put_in(Fixture.payload(c.now), ["entries", Access.at(0), "responsible", "scope", "repository"], "other/repository")
    assert {:error, _} = ManagedResponsibility.decode(bad_scope, Fixture.context(), c.now)
  end

  defp progress_payload(now, model \\ "gpt-6-luna") do
    Fixture.payload(now)
    |> update_in(["entries"], fn entries ->
      Enum.map(entries, fn entry ->
        entry
        |> put_in(["accountable", "budget", "mode"], "progress_scoped")
        |> put_in(["accountable", "budget", "model"], model)
        |> put_in(["accountable", "budget", "max_tokens"], nil)
        |> put_in(["responsible", "budget", "mode"], "progress_scoped")
        |> put_in(["responsible", "budget", "model"], model)
        |> put_in(["responsible", "budget", "max_tokens"], nil)
      end)
    end)
  end

  defp grant(issue_id, mode, model \\ "gpt-6-luna") do
    %{
      issue_id: issue_id,
      responsible: %{
        id: "grant-#{issue_id}",
        budget: %{
          model: model,
          effort: :max,
          mode: mode,
          max_tokens: if(mode == :progress_scoped, do: nil, else: @grant_limit),
          max_children: 0
        }
      }
    }
  end

  defp observe!(ledger, issue_id, total) do
    {:ok, next} = ManagedTokenBudget.observe(ledger, issue_id, 1, "progress-thread", total)
    next
  end
end
