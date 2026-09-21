defmodule SymphonyElixir.OrchestratorReviewHandoffTest do
  use SymphonyElixir.TestSupport

  alias SymphonyElixir.{ExecutionFence, ExecutionSupervisor, ResponsibilityGraph}
  alias SymphonyElixir.ExecutionFence.Persistence
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.ResponsibilityGraph.ReviewCompletion
  alias SymphonyElixir.WorkPackageClaim.Journal

  @id "00000000-0000-4000-8000-000000000500"
  @session "worker:#{@id}:1"
  @repo "hypergridau/grid"
  @merge String.duplicate("b", 40)

  setup do
    root = Path.join(System.tmp_dir!(), "review-handoff-#{System.unique_integer([:positive])}")
    workspace = Path.join(root, "HGS-500")
    File.mkdir_p!(workspace)
    on_exit(fn -> File.rm_rf(root) end)
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", workspace_root: root)

    for args <- [["init", "-b", "main"], ["-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "fixture"]] do
      assert {_output, 0} = System.cmd("git", args, cd: workspace, stderr_to_stdout: true)
    end

    {:ok, head} = Workspace.current_head(workspace)
    issue = %Issue{id: @id, identifier: "HGS-500", state: "In Review", title: "Useful work"}
    admission = %{issue_id: @id, repository: @repo, branch: "tracker-branch", worktree: workspace}
    {:ok, fence, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)

    session = %{
      session_id: @session,
      process_id: @session,
      branch: "tracker-branch",
      worktree: workspace,
      head: "old-base",
      linear_state: "In Progress",
      pr_state: "none",
      last_heartbeat_at: 100
    }

    {:ok, fence, :registered} = ExecutionFence.register(fence, token, :worker, session, 100)
    identity = ExecutionSupervisor.identity(@id, 1, @session, @session, 100)
    identity = Map.merge(identity, %{control_group: "/user.slice/review-handoff.scope", launch_processes: [111], main_pid: 111})
    {:ok, fence} = ExecutionFence.record_supervisor(fence, token, @session, identity)
    {:ok, fence, :released} = ExecutionFence.release(fence, token, @session, :orchestrator_stop)
    journal_path = Path.join(root, "journal.json")

    reservation = %{
      issue_id: @id,
      managed_project_profile_id: "profile",
      repository_ref: @repo,
      projection_id: "projection",
      reservation_id: "reservation",
      reservation_nonce: "nonce",
      scope_keys: ["repo:" <> @repo],
      runner_id: "runner",
      generation: 1,
      session_id: @session,
      process_id: @session,
      responsible_delegation_id: "worker",
      execution_fence_token: "#{@id}:1",
      runtime_lease_id: @session
    }

    key = Journal.reservation_key(@id, "profile", @repo, 1)
    {:ok, journal} = Journal.put(Journal.new(), key, reservation)
    :ok = Journal.save(journal_path, journal)
    parent = self()

    runtime = %{
      base_url: "http://provider.invalid",
      runner_token: "test-runner",
      attestation_key: "test-key",
      runner_id: "runner",
      managed_project_profile_id: "profile",
      journal_path: journal_path,
      request_fun: fn _url, options ->
        send(parent, {:receipt, Keyword.fetch!(options, :json)})
        {:error, :offline}
      end
    }

    state = %Orchestrator.State{
      execution_fence: fence,
      responsibility_graph: graph(),
      work_package_runtime: runtime,
      execution_termination_fun: &terminate/1,
      review_handoff_evidence: fn _ -> {:ok, %{accepted_head: head, merge_identity: @merge}} end
    }

    %{state: state, issue: issue, root: root, workspace: workspace, head: head, journal_path: journal_path}
  end

  test "review confirms the execution session without a terminal receipt", c do
    state = Orchestrator.reconcile_review_handoff_issues_for_test(c.state, [c.issue])
    execution = state.execution_fence.executions[@id]
    assert execution.leases[@session].termination_confirmed_at_ms
    assert execution.status == :active
    assert execution.terminal == nil
    assert execution.cleanup == :pending
    assert File.dir?(c.workspace)
    refute_received {:receipt, _}
    {:ok, journal} = Journal.load(c.journal_path)
    assert :missing = Journal.cleanup_receipt(journal, Journal.reservation_key(@id, "profile", @repo), "termination_confirmed")
  end

  test "a crash after lease release retains the termination fence and blocks repository reuse", c do
    path = Path.join(c.root, "released-before-stop.json")
    :ok = Persistence.save(path, c.state.execution_fence)
    {:ok, restored} = Persistence.load(path)
    assert restored.executions[@id].termination_unconfirmed
    assert restored.executions[@id].ownership == :unknown
    assert restored.executions[@id].leases[@session].termination_required
    admission = %{issue_id: "successor", repository: @repo, branch: "next", worktree: Path.join(c.root, "next")}
    assert {:error, {:repository_not_quiescent, @id}} = ExecutionFence.admit(restored, admission, 101)
    still_alive = fn _ -> {:error, :process_still_alive} end
    state = %{c.state | execution_fence: restored, execution_termination_fun: still_alive}
    held = Orchestrator.reconcile_review_handoff_issues_for_test(state, [%{c.issue | state: "Done"}])
    assert held.execution_fence.executions[@id].terminal == nil
    assert held.execution_fence.executions[@id].ownership == :unknown
    assert File.dir?(c.workspace)
    refute_received {:receipt, _}
  end

  test "merge evidence cannot replace execution identity", c do
    observer = fn _ ->
      {:ok,
       %{
         accepted_head: c.head,
         merge_identity: @merge,
         execution_token: %{issue_id: "other", generation: 99},
         workspace_path: "/wrong",
         execution_session_id: "other-session",
         responsibility_delegation_id: "owner"
       }}
    end

    state = %{c.state | review_handoff_evidence: observer}
    done = Orchestrator.reconcile_review_handoff_issues_for_test(state, [%{c.issue | state: "Done"}])
    assert Map.keys(done.execution_fence.executions) == [@id]
    assert done.execution_fence.executions[@id].generation == 1
    assert done.execution_fence.executions[@id].terminal.accepted_head == c.head
    assert done.responsibility_graph.delegations["worker"].status == :completed
    assert done.responsibility_graph.delegations["owner"].status == :active
    assert File.dir?(c.workspace)
  end

  test "malformed completion records fail closed", c do
    issue = %{c.issue | state: "Done"}
    done = Orchestrator.reconcile_review_handoff_issues_for_test(c.state, [issue])
    evidence = %{terminal_state: "Done", accepted_head: c.head, merge_identity: @merge}
    {:ok, entry} = SymphonyElixir.ReviewHandoff.entry(done.execution_fence.executions[@id], issue)
    {:ok, journal} = Journal.load(c.journal_path)
    reservation = journal.reservations[Journal.reservation_key(@id, "profile", @repo, 1)]
    entry = Map.merge(entry, %{review_reservation: reservation, responsibility_delegation_id: "worker"})
    graph = done.responsibility_graph
    fence = done.execution_fence
    assert {:ok, ^graph, %{}} = ReviewCompletion.complete(graph, fence, entry, evidence, 102)

    for {candidate_graph, candidate_fence, candidate, proof} <- [
          {nil, fence, entry, evidence},
          {graph, nil, entry, evidence},
          {graph, fence, entry, %{}},
          {graph, fence, %{entry | workspace_path: "/wrong"}, evidence},
          {graph, fence, %{entry | review_reservation: nil}, evidence}
        ] do
      assert {:error, _} = ReviewCompletion.complete(candidate_graph, candidate_fence, candidate, proof, 102)
    end
  end

  test "ordinary OTP polling retains a stopped review claim without spawning", c do
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [c.issue])
    name = Module.concat(__MODULE__, "Poll#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name})

    :sys.replace_state(pid, fn state ->
      %{
        state
        | execution_fence: c.state.execution_fence,
          responsibility_graph: c.state.responsibility_graph,
          work_package_runtime: c.state.work_package_runtime,
          execution_termination_fun: c.state.execution_termination_fun,
          review_handoff_evidence: c.state.review_handoff_evidence
      }
    end)

    send(pid, :run_poll_cycle)
    state = :sys.get_state(pid)
    assert state.running == %{}
    assert MapSet.member?(state.claimed, @id)
    assert state.execution_fence.executions[@id].leases[@session].termination_confirmed_at_ms
    assert state.execution_fence.executions[@id].terminal == nil
    refute_received {:receipt, _}
  end

  test "restart retains the review generation and uses the merged head after Done", c do
    reviewing = Orchestrator.reconcile_review_handoff_issues_for_test(c.state, [c.issue])
    path = Path.join(c.root, "fence.json")
    :ok = Persistence.save(path, reviewing.execution_fence)
    {:ok, restored} = Persistence.load(path)
    restarted = %{c.state | execution_fence: restored, running: %{}, claimed: MapSet.new(), retry_attempts: %{}}
    done = Orchestrator.reconcile_review_handoff_issues_for_test(restarted, [%{c.issue | state: "Done"}])
    execution = done.execution_fence.executions[@id]
    assert execution.generation == 1
    assert execution.terminal.accepted_head == c.head
    assert execution.terminal.merge_identity == @merge
    assert done.responsibility_graph.delegations["worker"].status == :completed
    # An unavailable archive provider must retain the workspace, even after merge.
    assert execution.cleanup == :pending
    assert File.dir?(c.workspace)
    assert_received {:receipt, %{"terminalOutcome" => "completed", "acceptedHead" => head}}
    assert head == c.head
  end

  test "a canceled budget-stopped generation never submits a completed outcome", c do
    issue = %{c.issue | state: "Canceled"}
    next = Orchestrator.reconcile_review_handoff_issues_for_test(c.state, [issue])
    execution = next.execution_fence.executions[@id]

    assert execution.terminal.state == "Canceled"
    assert execution.terminal.accepted_head == c.head
    assert execution.cleanup == :pending
    assert File.dir?(c.workspace)
    assert_received {:receipt, %{"receiptKind" => "termination_confirmed", "terminalOutcome" => "failed"}}
    refute_received {:receipt, %{"terminalOutcome" => "completed"}}
  end

  test "missing merge evidence preserves an active fence after native Done", c do
    state = %{c.state | review_handoff_evidence: fn _ -> {:error, :not_merged} end}
    next = Orchestrator.reconcile_review_handoff_issues_for_test(state, [%{c.issue | state: "Done"}])
    assert next.execution_fence.executions[@id].terminal == nil
    assert next.responsibility_graph.delegations["worker"].status == :active
    refute_received {:receipt, _}
  end

  test "restart between fence release and graph release completes only the exact responsible leaf", c do
    lease = c.state.execution_fence.executions[@id].leases[@session]
    identity = Map.take(lease, [:issue_id, :repository, :generation, :session_id, :process_id])
    {:ok, graph} = ResponsibilityGraph.bind_runtime_lease(c.state.responsibility_graph, "worker", identity, 102)
    {:ok, restarted_graph} = ResponsibilityGraph.mark_unreconciled_after_restart(graph)
    assert restarted_graph.delegations["worker"].blocked_on == :restart_reconciliation
    restarted = %{c.state | responsibility_graph: restarted_graph}
    done = Orchestrator.reconcile_review_handoff_issues_for_test(restarted, [%{c.issue | state: "Done"}])
    assert done.responsibility_graph.delegations["worker"].status == :completed
    assert done.responsibility_graph.delegations["owner"] == restarted_graph.delegations["owner"]
    path = Path.join(c.root, "completed-graph.json")
    :ok = GraphPersistence.save(path, done.responsibility_graph)
    {:ok, loaded} = GraphPersistence.load(path)
    restored = %{done | responsibility_graph: loaded}
    repeated = Orchestrator.reconcile_review_handoff_issues_for_test(restored, [%{c.issue | state: "Done"}])
    assert repeated.responsibility_graph == loaded
    assert_received {:receipt, first}
    assert_received {:receipt, replay}
    assert first["receiptId"] == replay["receiptId"]
    assert first["generation"] == replay["generation"]
  end

  test "a stale graph lease does not complete another generation", c do
    lease = c.state.execution_fence.executions[@id].leases[@session]
    identity = Map.take(lease, [:issue_id, :repository, :generation, :session_id, :process_id]) |> Map.put(:generation, 2)
    {:ok, graph} = ResponsibilityGraph.bind_runtime_lease(c.state.responsibility_graph, "worker", identity, 102)
    state = %{c.state | responsibility_graph: graph}
    next = Orchestrator.reconcile_review_handoff_issues_for_test(state, [%{c.issue | state: "Done"}])
    assert next.responsibility_graph.delegations["worker"].status == :active
    assert next.execution_fence.executions[@id].cleanup == :pending
    assert File.dir?(c.workspace)
    refute_received {:receipt, _}
  end

  test "native state changed during merge observation retains the workspace", c do
    fetcher = fn [@id] -> {:ok, [%{c.issue | state: "In Progress"}]} end
    state = %{c.state | review_issue_fetcher: fetcher}
    next = Orchestrator.reconcile_review_handoff_issues_for_test(state, [%{c.issue | state: "Done"}])
    assert next.execution_fence.executions[@id].terminal == nil
    assert next.responsibility_graph.delegations["worker"].status == :active
    assert File.dir?(c.workspace)
    refute_received {:receipt, _}
  end

  test "a stale supervisor result cannot authorize merge or cleanup", c do
    observer = fn identity ->
      {:ok, evidence} = terminate(identity)
      {:ok, %{evidence | session_id: "stale-session"}}
    end

    state = %{c.state | execution_termination_fun: observer}
    next = Orchestrator.reconcile_review_handoff_issues_for_test(state, [%{c.issue | state: "Done"}])
    assert next.execution_fence.executions[@id].ownership == :unknown
    assert next.execution_fence.executions[@id].terminal == nil
    refute_received {:receipt, _}
  end

  test "current GitHub evidence qualifies the actual branch and rejects wrong remotes and truncated results", c do
    execution = c.state.execution_fence.executions[@id]

    pr = %{
      "state" => "MERGED",
      "headRefName" => "actual-branch",
      "headRefOid" => c.head,
      "baseRefName" => "main",
      "mergeCommit" => %{"oid" => @merge},
      "mergedAt" => "2026-09-10T18:00:00Z",
      "number" => 222,
      "url" => "https://github.com/#{@repo}/pull/222"
    }

    runner = evidence_reader("https://github.com/#{@repo}.git", [pr])

    assert {:ok, %{accepted_head: head, merge_identity: @merge}} =
             SymphonyElixir.ReviewHandoffEvidence.observe(execution, command_runner: runner)

    assert head == c.head

    for {remote, prs} <- [{"https://github.com/other/repo.git", [pr]}, {"https://github.com/#{@repo}.git", List.duplicate(pr, 100)}] do
      assert {:error, _} =
               SymphonyElixir.ReviewHandoffEvidence.observe(execution,
                 command_runner: evidence_reader(remote, prs)
               )
    end

    assert {:error, _} =
             SymphonyElixir.ReviewHandoffEvidence.observe(execution,
               command_runner: fn _, _, _ -> {"private diagnostic", 124} end
             )
  end

  defp evidence_reader(remote, prs) do
    fn
      "git", ["branch", "--show-current"], _ -> {"actual-branch\n", 0}
      "git", ["status", "--porcelain=v1", "--untracked-files=all"], _ -> {"", 0}
      "git", ["remote", "get-url", "origin"], _ -> {remote <> "\n", 0}
      "gh", ["pr", "list", "--repo", @repo, "--state", "all", "--head", "actual-branch", "--limit", "100", "--json", _], _ -> {Jason.encode!(prs), 0}
    end
  end

  defp terminate(identity) do
    ExecutionSupervisor.terminate(identity,
      command_runner: fn _executable, _args, _opts -> {"LoadState=not-found\nActiveState=inactive\nControlGroup=\nMainPID=0\n", 0} end,
      cgroup_reader: fn _ -> {:error, :enoent} end,
      now_ms: System.system_time(:millisecond)
    )
  end

  defp graph do
    actions = [:read, :observe, :delegate, :reconcile, :edit, :commit, :push]
    actions = actions ++ [:state_mutation, :cleanup, :review, :report]

    scope = %{
      company_id: "company",
      objective_id: "objective",
      initiative_id: "initiative",
      project_id: "project",
      work_package_id: "package",
      issue_id: @id,
      repository: @repo,
      paths: [],
      modules: [],
      environments: ["local"],
      actions: actions
    }

    attrs = %{
      id: "owner",
      parent_delegation_id: nil,
      role: :accountable,
      actor_id: "owner",
      scope: scope,
      authority: %{class: :routine_engineering, capabilities: actions, environments: ["local"]},
      budget: %{model: "luna", effort: :high, max_tokens: 1000, max_children: 2},
      runtime_lease: nil,
      expires_at_ms: System.system_time(:millisecond) + 60_000,
      expected_deliverable: "source",
      expected_evidence: "tests",
      return_to_parent: %{owner_id: "owner", contract: "evidence"}
    }

    {:ok, graph, _} = ResponsibilityGraph.delegate(ResponsibilityGraph.new(), attrs, 100)
    worker = %{attrs | id: "worker", role: :responsible, actor_id: "worker", parent_delegation_id: "owner"}
    {:ok, graph, _} = ResponsibilityGraph.delegate(graph, worker, 101)
    graph
  end
end
