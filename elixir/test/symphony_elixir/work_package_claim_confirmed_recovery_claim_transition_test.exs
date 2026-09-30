defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryClaimTransitionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.WorkPackageClaim.{ConfirmedRecoveryClaimTransition, Journal}

  @issue "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"
  @profile "profile-1"
  @repository "hypergridau/symphony"
  @responsible "responsible-2"
  @session "session-2"
  @process "process-2"

  test "computes durable confirmed to recovery-pending postimages for the exact generation-two claim" do
    {journal, fence, graph, payload} = confirmed_state()

    assert {:ok, postimages} =
             ConfirmedRecoveryClaimTransition.prepare_postimages(journal, fence, graph, payload, 500)

    assert postimages.reservationId == "reservation-2"
    assert {:ok, next_journal} = Journal.decode_bytes(postimages.claimJournal)
    assert {:ok, next_fence} = FencePersistence.decode_bytes(postimages.fence)
    assert {:ok, next_graph} = GraphPersistence.decode_bytes(postimages.responsibilityGraph)

    key = Journal.reservation_key(@issue, @profile, @repository, 2)
    assert next_journal.reservations[key].dispatch.phase == "recovery_pending"
    assert next_journal.reservations[key].dispatch.allocation_id == nil

    released_execution = next_fence.executions[@issue]
    assert released_execution.generation == 2
    assert released_execution.leases[@session].status == :released
    assert released_execution.leases[@session].release_reason == "spawn_failed"

    released_delegation = next_graph.delegations[@responsible]
    assert released_delegation.status == :active
    assert released_delegation.runtime_lease == nil

    assert {:ok, journal_bytes} = Journal.encode_bytes(next_journal)
    assert {:ok, fence_bytes} = FencePersistence.encode_bytes(next_fence)
    assert {:ok, graph_bytes} = GraphPersistence.encode_bytes(next_graph)
    assert journal_bytes == postimages.claimJournal
    assert fence_bytes == postimages.fence
    assert graph_bytes == postimages.responsibilityGraph
  end

  test "rejects a changed claim phase, mismatched runtime lease, or malformed payload" do
    {journal, fence, graph, payload} = confirmed_state()
    key = Journal.reservation_key(@issue, @profile, @repository, 2)
    changed = put_in(journal, [:reservations, key, :dispatch, :phase], "recovery_pending")

    assert {:error, :confirmed_claim_transition_rejected} =
             ConfirmedRecoveryClaimTransition.prepare_postimages(changed, fence, graph, payload, 500)

    wrong_graph = put_in(graph, [:delegations, @responsible, :runtime_lease, :process_id], "other-process")

    assert {:error, :confirmed_claim_transition_rejected} =
             ConfirmedRecoveryClaimTransition.prepare_postimages(journal, fence, wrong_graph, payload, 500)

    assert {:error, :confirmed_claim_transition_rejected} =
             ConfirmedRecoveryClaimTransition.prepare_postimages(journal, fence, graph, %{}, 500)
  end

  defp confirmed_state do
    expected = %{
      "issueId" => @issue,
      "managedProjectProfileId" => @profile,
      "repositoryRef" => @repository,
      "sessionId" => @session,
      "processId" => @process,
      "responsibleDelegationId" => @responsible
    }

    key = Journal.reservation_key(@issue, @profile, @repository, 2)

    reservation = %{
      issue_id: @issue,
      managed_project_profile_id: @profile,
      repository_ref: @repository,
      projection_id: "projection-2",
      reservation_id: "reservation-2",
      reservation_nonce: "nonce-2",
      scope_keys: ["repo:hypergridau/symphony"],
      runner_id: "runner-1",
      generation: 2,
      session_id: @session,
      process_id: @process,
      responsible_delegation_id: @responsible,
      execution_fence_token: "#{@issue}:2",
      runtime_lease_id: @session,
      dispatch: %{
        phase: "confirmed",
        attempts: 1,
        retry_at_ms: 0,
        authority_digest: String.duplicate("a", 64),
        allocation_id: nil
      }
    }

    {:ok, journal} = Journal.put(Journal.new(), key, reservation)
    fence = generation_two_fence()
    graph = responsible_graph()

    payload = %{
      "issueId" => @issue,
      "reservationId" => "reservation-2",
      "observation" => %{"expected" => expected}
    }

    {journal, fence, graph, payload}
  end

  defp generation_two_fence do
    admission = %{
      issue_id: @issue,
      repository: @repository,
      branch: "codex/hgs-740",
      worktree: "C:/worktrees/hgs-740"
    }

    {:ok, initial, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 100)

    session = %{
      issue_id: @issue,
      repository: @repository,
      generation: 1,
      role: :worker,
      session_id: @session,
      process_id: @process,
      branch: admission.branch,
      worktree: admission.worktree,
      linear_state: "In Progress",
      pr_state: "OPEN",
      head: "abc123",
      last_heartbeat_at: 100
    }

    {:ok, registered, :registered} = ExecutionFence.register(initial, token, :worker, session, 100)
    execution = registered.executions[@issue] |> Map.put(:generation, 2)
    leases = Map.new(execution.leases, fn {id, lease} -> {id, Map.put(lease, :generation, 2)} end)
    execution = %{execution | leases: leases}

    sessions =
      Map.new(registered.sessions, fn {id, worker_session} ->
        {id, Map.put(worker_session, :generation, 2)}
      end)

    %{registered | executions: %{@issue => execution}, sessions: sessions}
  end

  defp responsible_graph do
    actions =
      [:read, :observe, :delegate, :reconcile, :edit, :commit, :push] ++
        [:state_mutation, :cleanup, :review, :report]

    scope = %{
      company_id: "company",
      objective_id: "objective",
      initiative_id: "initiative",
      project_id: "project",
      work_package_id: "package",
      issue_id: @issue,
      repository: @repository,
      paths: [],
      modules: [],
      environments: ["local"],
      actions: actions
    }

    accountable = %{
      id: "accountable",
      parent_delegation_id: nil,
      role: :accountable,
      actor_id: "owner",
      scope: scope,
      authority: %{class: :routine_engineering, capabilities: actions, environments: ["local"]},
      budget: %{model: "luna", effort: :high, max_tokens: 1000, max_children: 2},
      runtime_lease: nil,
      expires_at_ms: 60_000,
      expected_deliverable: "source",
      expected_evidence: "tests",
      return_to_parent: %{owner_id: "accountable", contract: "evidence"}
    }

    {:ok, graph, _} = ResponsibilityGraph.delegate(ResponsibilityGraph.new(), accountable, 100)
    responsible = %{accountable | id: @responsible, parent_delegation_id: "accountable", role: :responsible}
    {:ok, graph, _} = ResponsibilityGraph.delegate(graph, responsible, 101)

    lease = %{
      issue_id: @issue,
      repository: @repository,
      generation: 2,
      session_id: @session,
      process_id: @process
    }

    {:ok, graph} = ResponsibilityGraph.bind_runtime_lease(graph, @responsible, lease, 102)
    graph
  end
end
