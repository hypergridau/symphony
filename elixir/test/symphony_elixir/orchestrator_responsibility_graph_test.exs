Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.OrchestratorResponsibilityGraphTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{AgentRunner, ExecutionFence, ManagedResponsibility, Orchestrator, ResponsibilityGraph}
  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture

  test "orchestrator callback persists, authorizes, and revokes a delegation" do
    {fence, token} = active_fence()

    state = %Orchestrator.State{
      execution_fence: fence,
      responsibility_graph: ResponsibilityGraph.new(),
      responsibility_graph_path: nil
    }

    {:reply, {:ok, _owner}, state1} =
      Orchestrator.handle_call(
        {:responsibility_delegate, delegation("owner", :accountable), 0},
        self(),
        state
      )

    {:reply, {:ok, _worker}, state2} =
      Orchestrator.handle_call(
        {:responsibility_delegate, delegation("worker", :responsible, parent_delegation_id: "owner", runtime_lease: runtime_lease()), 1},
        self(),
        state1
      )

    {:reply, {:ok, authorization}, state3} =
      Orchestrator.handle_call({:responsibility_authorize, "worker", :commit}, self(), state2)

    assert authorization.execution_fence.generation == token.generation

    terminal = %{terminal_state: "done", accepted_head: "abc123", merge_identity: "merge-1"}

    {:reply, {:ok, %{delegation_ids: ["worker"]}}, state4} =
      Orchestrator.handle_call(
        {:responsibility_revoke, "worker", :terminal, terminal, 2},
        self(),
        state3
      )

    assert state4.responsibility_graph.delegations["worker"].status == :revoked
    assert state4.execution_fence.executions["HGS-300"].status == :terminal
  end

  test "enforced graph gates normal worker admission and binds the execution lease" do
    {:ok, graph, :activated} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), 0)

    {:ok, graph, _owner} =
      ResponsibilityGraph.delegate(
        graph,
        delegation("owner", :accountable, scope: Map.put(scope(), :repository, "openai/symphony")),
        1
      )

    {:ok, graph, _worker} =
      ResponsibilityGraph.delegate(
        graph,
        delegation(
          "worker",
          :responsible,
          parent_delegation_id: "owner",
          scope: Map.put(scope(), :repository, "openai/symphony")
        ),
        2
      )

    state = %Orchestrator.State{
      execution_fence: ExecutionFence.new(),
      responsibility_graph: graph,
      execution_fence_path: nil,
      responsibility_graph_path: nil
    }

    issue = %Issue{id: "HGS-300", identifier: "HGS-300", title: "Graph admission", state: "Todo", branch_name: "codex/hgs-300"}

    assert {:ok, admitted, token, session_id, "worker", runtime_lease} =
             Orchestrator.admit_execution_for_test(state, issue, nil)

    assert admitted.responsibility_graph.delegations["worker"].runtime_lease == runtime_lease
    assert runtime_lease.session_id == session_id

    assert {:reply, {:ok, _authorization}, ^admitted} =
             Orchestrator.handle_call(
               {:execution_authorize, token, "worker", :state_mutation},
               {self(), make_ref()},
               admitted
             )
  end

  test "managed spawn is held before the provider spawn journal when assignment context is incomplete" do
    {:ok, graph, :activated} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), 0)

    {:ok, graph, _owner} =
      ResponsibilityGraph.delegate(
        graph,
        delegation("owner", :accountable, scope: Map.put(scope(), :repository, "openai/symphony")),
        1
      )

    {:ok, graph, _worker} =
      ResponsibilityGraph.delegate(
        graph,
        delegation(
          "worker",
          :responsible,
          parent_delegation_id: "owner",
          scope: Map.put(scope(), :repository, "openai/symphony")
        ),
        2
      )

    issue = %Issue{id: "HGS-300", identifier: "HGS-300", title: "Managed assignment", state: "Todo", branch_name: "codex/hgs-300"}

    state = %Orchestrator.State{
      execution_fence: ExecutionFence.new(),
      responsibility_graph: graph,
      running: %{},
      blocked: %{}
    }

    assert {:ok, admitted, token, session_id, "worker", lease} =
             Orchestrator.admit_execution_for_test(state, issue, nil)

    managed = %{admitted | work_package_runtime: %{managed_delegations: %{repository_ref: "openai/symphony"}}}
    held = Orchestrator.spawn_fenced_issue_for_test(managed, issue, token, session_id, "worker", lease)

    assert held.running == %{}
    assert Map.has_key?(held.blocked, issue.id)
  end

  test "orchestrator assignment construction produces the bundle accepted by AgentRunner" do
    issue = Fixture.issue(300)

    managed_scope =
      scope()
      |> Map.merge(%{
        objective_id: "objective-test",
        issue_id: issue.id,
        repository: "openai/symphony",
        paths: ["."],
        environments: ["repository"]
      })

    {:ok, graph, :activated} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), 0)

    {:ok, graph, _owner} =
      ResponsibilityGraph.delegate(
        graph,
        delegation("owner", :accountable, actor_id: issue.assignee_id, scope: managed_scope, authority: Map.put(authority(), :environments, ["repository"])),
        1
      )

    {:ok, graph, _worker} =
      ResponsibilityGraph.delegate(
        graph,
        delegation(
          "worker",
          :responsible,
          actor_id: "runner-test",
          parent_delegation_id: "owner",
          scope: managed_scope,
          authority: Map.put(authority(), :environments, ["repository"])
        ),
        2
      )

    state = %Orchestrator.State{execution_fence: ExecutionFence.new(), responsibility_graph: graph}

    assert {:ok, admitted, token, session_id, "worker", lease} =
             Orchestrator.admit_execution_for_test(state, issue, nil)

    payload = Fixture.payload(System.system_time(:millisecond), "openai/symphony")
    [signed_entry | rest] = payload["entries"]

    signed_entry =
      signed_entry
      |> Map.put("issue_id", issue.id)
      |> Map.put("identifier", issue.identifier)
      |> Map.put("owner_id", issue.assignee_id)
      |> update_in(["accountable", "id"], fn _ -> "owner" end)
      |> update_in(["responsible", "id"], fn _ -> "worker" end)
      |> update_in(["responsible", "parent_delegation_id"], fn _ -> "owner" end)
      |> update_in(["accountable", "scope", "objective_id"], fn _ -> "objective-test" end)
      |> update_in(["responsible", "scope", "objective_id"], fn _ -> "objective-test" end)
      |> update_in(["accountable", "scope", "issue_id"], fn _ -> issue.id end)
      |> update_in(["responsible", "scope", "issue_id"], fn _ -> issue.id end)
      |> update_in(["accountable", "scope", "repository"], fn _ -> "openai/symphony" end)
      |> update_in(["responsible", "scope", "repository"], fn _ -> "openai/symphony" end)
      |> update_in(["responsible", "actor_id"], fn _ -> "runner-test" end)

    payload = %{payload | "entries" => [signed_entry | rest]}
    {:ok, signed_manifest} = ManagedResponsibility.decode(payload, Fixture.context(), System.system_time(:millisecond))

    runtime = %{
      managed_delegations: signed_manifest,
      secret_environment_names: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"]
    }

    managed = %{admitted | work_package_runtime: runtime}

    assert {:ok, bundle} =
             Orchestrator.managed_assignment_bundle_for_test(managed, issue, token, session_id, "worker")

    assert bundle.lease == lease
    assert bundle.intent_ancestry == ["owner", "worker"]

    assert :ok =
             AgentRunner.assignment_bundle_preflight_for_test(
               assignment_bundle: bundle,
               execution_checkout: %{branch: bundle.branch},
               managed_model_route: true
             )
  end

  defp delegation(id, role, overrides \\ []) do
    Map.merge(
      %{
        id: id,
        parent_delegation_id: nil,
        role: role,
        actor_id: "actor-#{id}",
        scope: scope(),
        authority: authority(),
        budget: %{model: "luna", effort: :max, max_tokens: 10_000, max_children: 4},
        runtime_lease: nil,
        expires_at_ms: System.system_time(:millisecond) + 60_000,
        expected_deliverable: "bounded deliverable",
        expected_evidence: "tests and review evidence",
        return_to_parent: %{owner_id: "owner", contract: "return evidence and outcome"}
      },
      Map.new(overrides)
    )
  end

  defp scope do
    %{
      company_id: "hypergrid",
      objective_id: "objective-1",
      initiative_id: "initiative-1",
      project_id: "project-1",
      work_package_id: "package-1",
      issue_id: "HGS-300",
      repository: "orchestrator",
      paths: [],
      modules: [],
      environments: ["local"],
      actions: [:read, :observe, :delegate, :reconcile, :edit, :commit, :push, :state_mutation, :cleanup, :review, :report]
    }
  end

  defp authority do
    %{
      class: :routine_engineering,
      capabilities: [:read, :observe, :delegate, :reconcile, :edit, :commit, :push, :state_mutation, :cleanup, :review, :report],
      environments: ["local"]
    }
  end

  defp runtime_lease do
    %{issue_id: "HGS-300", repository: "orchestrator", generation: 1, session_id: "worker", process_id: "process-worker"}
  end

  defp active_fence do
    {:ok, state, token} =
      ExecutionFence.admit(
        ExecutionFence.new(),
        %{issue_id: "HGS-300", repository: "orchestrator", branch: "hgs-300", worktree: "worktree"},
        0
      )

    {:ok, state, :registered} =
      ExecutionFence.register(
        state,
        token,
        :worker,
        %{
          session_id: "worker",
          process_id: "process-worker",
          branch: "hgs-300",
          worktree: "worktree",
          linear_state: "In Progress",
          pr_state: "none",
          head: "abc123",
          last_heartbeat_at: 0
        },
        0
      )

    {state, token}
  end
end
