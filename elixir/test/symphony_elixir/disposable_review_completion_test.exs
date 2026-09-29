defmodule SymphonyElixir.DisposableReviewCompletionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{ExecutionFence, Orchestrator, ResponsibilityGraph}
  alias SymphonyElixir.ResponsibilityGraph.DisposableReviewCompletion
  alias SymphonyElixir.Tracker.Issue

  @issue "issue-1"
  @repo "hypergridau/symphony"
  @head String.duplicate("a", 40)
  @merge String.duplicate("b", 40)

  test "completes only the exact disposable responsible leaf" do
    {graph, fence, entry, evidence, _confirmed} = fixture()

    assert {:ok, completed, _} = DisposableReviewCompletion.complete(graph, fence, entry, evidence, 102)
    assert completed.delegations["worker"].status == :completed
    assert completed.delegations["owner"].status == :active
    assert {:ok, ^completed, %{}} = DisposableReviewCompletion.complete(completed, fence, entry, evidence, 103)

    for changed <- [
          put_in(entry, [:review_reservation, :generation], 2),
          put_in(entry, [:review_reservation, :process_id], "replacement"),
          put_in(entry, [:review_reservation, :dispatch, :phase], "allocation_suspended"),
          put_in(entry, [:review_reservation, :assignment_snapshot], nil),
          %{entry | responsibility_delegation_id: "owner"}
        ] do
      assert {:error, _} = DisposableReviewCompletion.complete(graph, fence, changed, evidence, 102)
    end

    assert {:error, _} =
             DisposableReviewCompletion.complete(graph, fence, entry, %{evidence | accepted_head: @merge}, 102)
  end

  test "poll handoff fences only a native terminal issue with the verified remote head" do
    {graph, _fence, entry, evidence, confirmed} = fixture()
    issue = entry.issue

    state = %Orchestrator.State{
      execution_fence: confirmed,
      responsibility_graph: graph,
      work_package_runtime: %{
        disposable_merge_evidence_fun: fn _, _ ->
          {:ok, %{accepted_head: evidence.accepted_head, merge_identity: evidence.merge_identity}}
        end
      },
      review_issue_fetcher: fn [@issue] -> {:ok, [issue]} end
    }

    held = %{state | review_issue_fetcher: fn [@issue] -> {:ok, [%{issue | state: "In Progress"}]} end}

    assert Orchestrator.reconcile_disposable_remote_review_for_test(
             held,
             entry.review_reservation,
             %{},
             %{}
           ).execution_fence.executions[@issue].status == :active

    accepted = Orchestrator.reconcile_disposable_remote_review_for_test(state, entry.review_reservation, %{}, %{})
    assert accepted.execution_fence.executions[@issue].terminal.accepted_head == @head
    assert accepted.responsibility_graph.delegations["worker"].status == :completed
    assert accepted.responsibility_graph.delegations["owner"].status == :active
  end

  defp fixture do
    branch = "codex/hgs729-disposable"
    worktree = "/synthetic/issue-1"
    session = "worker:issue-1:1"
    process = "process-1"

    {:ok, admitted, token} =
      ExecutionFence.admit(
        ExecutionFence.new(),
        %{issue_id: @issue, repository: @repo, branch: branch, worktree: worktree},
        0
      )

    {:ok, registered, :registered} =
      ExecutionFence.register(
        admitted,
        token,
        :worker,
        %{
          session_id: session,
          process_id: process,
          branch: branch,
          worktree: worktree,
          linear_state: "In Progress",
          pr_state: "none",
          head: "unobserved",
          last_heartbeat_at: 0
        },
        0
      )

    {:ok, released, :released} = ExecutionFence.release(registered, token, session, :orchestrator_stop)

    termination = %{
      session_id: session,
      process_id: process,
      process_tree: :terminated,
      evidence_ref: "sha256:#{String.duplicate("c", 64)}",
      observed_at_ms: 100,
      job_uid: "job-uid-1",
      pod_uid: "pod-uid-1",
      assignment_digest: String.duplicate("c", 64)
    }

    {:ok, confirmed, :confirmed} = ExecutionFence.confirm_termination(released, token, session, termination, 100)

    {:ok, fence, :fenced} =
      ExecutionFence.fence(confirmed, token, %{terminal_state: "Done", accepted_head: @head, merge_identity: @merge}, 101)

    graph = graph()

    reservation = %{
      issue_id: @issue,
      repository_ref: @repo,
      generation: 1,
      session_id: session,
      process_id: process,
      responsible_delegation_id: "worker",
      runtime_lease_id: session,
      execution_fence_token: "issue-1:1",
      assignment_snapshot: "signed-assignment",
      dispatch: %{phase: "spawn_started", allocation_id: "rke2job:v1:allocation"}
    }

    entry = %{
      issue: %Issue{id: @issue, identifier: "HGS-729", state: "Done"},
      execution_token: token,
      execution_session_id: session,
      process_id: process,
      responsibility_delegation_id: "worker",
      review_reservation: reservation
    }

    {graph, fence, entry, %{terminal_state: "Done", accepted_head: @head, merge_identity: @merge}, confirmed}
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
      issue_id: @issue,
      repository: @repo,
      paths: [],
      modules: [],
      environments: ["local"],
      actions: actions
    }

    owner = %{
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

    {:ok, graph, _} = ResponsibilityGraph.delegate(ResponsibilityGraph.new(), owner, 100)
    worker = %{owner | id: "worker", role: :responsible, actor_id: "worker", parent_delegation_id: "owner"}
    {:ok, graph, _} = ResponsibilityGraph.delegate(graph, worker, 101)
    graph
  end
end
