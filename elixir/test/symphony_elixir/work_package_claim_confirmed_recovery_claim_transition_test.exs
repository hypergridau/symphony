defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryClaimTransitionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.WorkPackageClaim.{ConfirmedRecoveryClaimTransition, Journal}
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryCore
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryUnsubmittedPredecessor, as: Predecessor
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWAL

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

  test "v3 reconciles a never-started fence and releases its lease without restoring blocked authority" do
    {journal, fence, graph, payload} = confirmed_state()
    payload = Map.put(payload, "contractVersion", "work-package-paused-confirmed-recovery.v3")
    lease = fence.executions[@issue].leases[@session] |> Map.merge(%{head: "unobserved", last_heartbeat_at: 0})

    fence =
      fence
      |> put_in([:executions, @issue, :ownership], :unknown)
      |> put_in([:executions, @issue, :leases, @session], lease)
      |> put_in([:sessions, @session], lease)

    graph =
      Enum.reduce(["accountable", @responsible], graph, fn id, acc ->
        acc
        |> put_in([:delegations, id, :status], :blocked)
        |> put_in([:delegations, id, :blocked_on], :restart_reconciliation)
      end)

    assert {:ok, images} = ConfirmedRecoveryClaimTransition.prepare_postimages(journal, fence, graph, payload, 500)
    assert {:ok, after_graph} = GraphPersistence.decode_bytes(images.responsibilityGraph)
    assert {:ok, after_fence} = FencePersistence.decode_bytes(images.fence)
    assert after_graph.delegations[@responsible].status == :blocked
    assert after_graph.delegations["accountable"].status == :blocked
    assert after_graph.delegations[@responsible].runtime_lease == nil
    assert after_fence.executions[@issue].ownership == :reconciled
    assert after_fence.executions[@issue].leases[@session].status == :released
    observed = put_in(fence, [:executions, @issue, :leases, @session, :head], "observed")

    assert {:error, :confirmed_claim_transition_rejected} =
             ConfirmedRecoveryClaimTransition.prepare_postimages(journal, observed, graph, payload, 500)
  end

  test "persisted retirement binds both revoked grants, their original observation, and the sole history entry" do
    {paths, predecessor, expected} = retirement_only_state()
    assert :ok = Predecessor.persisted(paths, predecessor, expected)
    assert :ok = Predecessor.blocked_lease(paths.graph.state, expected)
    [reservation] = Map.values(paths.journal.state.reservations)
    assert {:ok, candidate} = Predecessor.reconciled_fence_candidate(paths.fence.state, reservation)
    assert candidate.executions[@issue].ownership == :reconciled
    assert paths.fence.state.executions[@issue].ownership == :unknown

    altered = put_in(paths, [:graph, :state, :delegations, "prior-accountable", :budget, :max_tokens], 9000)
    assert {:error, :predecessor_retirement_not_persisted} = Predecessor.persisted(altered, predecessor, expected)
    altered = put_in(paths, [:graph, :state, :delegations, "prior-accountable", :terminal_evidence, "observation", "process_count"], 1)
    assert {:error, :predecessor_retirement_not_persisted} = Predecessor.persisted(altered, predecessor, expected)
    altered = put_in(paths, [:graph, :state, :delegations, "accountable", :status], :active)
    assert {:error, :predecessor_retirement_not_persisted} = Predecessor.persisted(altered, predecessor, expected)
    altered = put_in(paths, [:fence, :state, :history], [])
    assert {:error, :predecessor_retirement_not_persisted} = Predecessor.persisted(altered, predecessor, expected)
    altered = put_in(paths, [:journal, :state, :reservations, "fabricated-prior"], %{reservation | generation: 1})
    assert {:error, :predecessor_retirement_not_persisted} = Predecessor.persisted(altered, predecessor, expected)
  end

  test "never-started recovery rejects observed workers, supervisor evidence, and mismatched blocked leases" do
    {paths, predecessor, expected} = retirement_only_state()
    [reservation] = Map.values(paths.journal.state.reservations)
    altered = put_in(paths.fence.state, [:executions, @issue, :leases, @session, :supervisor_identity], "observed-supervisor")
    assert {:error, :execution_lease_mismatch} = Predecessor.reconciled_fence_candidate(altered, reservation)
    altered = put_in(paths.fence.state, [:executions, @issue, :leases, @session, :head], "observed-head")
    assert {:error, :execution_lease_mismatch} = Predecessor.reconciled_fence_candidate(altered, reservation)
    altered = put_in(paths.graph.state, [:delegations, @responsible, :runtime_lease, :process_id], "foreign-process")
    assert {:error, :runtime_lease_mismatch} = Predecessor.blocked_lease(altered, expected)
    altered = put_in(predecessor, ["execution", "leases", "prior-session", "last_heartbeat_at"], 1)
    assert {:error, :invalid_confirmed_recovery_evidence} = Predecessor.validate(altered, expected)
    assert {:error, :invalid_confirmed_recovery_evidence} = Predecessor.validate(%{}, expected)
    assert {:error, :predecessor_retirement_not_persisted} = Predecessor.persisted(paths, %{}, expected)
  end

  test "v3 local preconditions require an absent snapshot and the exact blocked generation-two claim" do
    {paths, predecessor, expected} = retirement_only_state()
    [reservation] = Map.values(paths.journal.state.reservations)

    expected =
      Map.merge(expected, %{
        "reservationId" => reservation.reservation_id,
        "workspaceId" => nil,
        "companyId" => nil,
        "runnerId" => reservation.runner_id,
        "scopeKeys" => reservation.scope_keys,
        "generation" => 2,
        "executionFenceToken" => reservation.execution_fence_token,
        "runtimeLeaseId" => reservation.runtime_lease_id,
        "nonceHash" => :crypto.hash(:sha256, reservation.reservation_nonce) |> Base.encode16(case: :lower)
      })

    {:ok, bytes} = Journal.encode_bytes(paths.journal.state)
    paths = put_in(paths, [:journal, :bytes], bytes)

    observation = %{
      "expected" => expected,
      "predecessorRetirement" => predecessor,
      "globalPause" => true,
      "runnerStopped" => true,
      "neverSpawned" => true,
      "supervisedWorkerAbsent" => true,
      "processCount" => 0,
      "workspaceAbsent" => true,
      "turnsAbsent" => true,
      "dispatchPhase" => "confirmed"
    }

    payload = %{
      "contractVersion" => "work-package-paused-confirmed-recovery.v3",
      "assignmentSnapshotState" => "absent",
      "assignmentSHA256" => nil,
      "issueId" => @issue,
      "reservationId" => reservation.reservation_id,
      "observation" => observation
    }

    assert :ok = ConfirmedRecoveryCore.verify_local_claim(paths, payload, %{})

    assert {:error, :confirmed_claim_precondition_changed} =
             ConfirmedRecoveryCore.verify_local_claim(paths, put_in(payload, ["observation", "processCount"], 1), %{})

    assert {:error, :confirmed_claim_precondition_changed} =
             ConfirmedRecoveryCore.verify_local_claim(paths, Map.put(payload, "assignmentSnapshotState", "present"), %{})

    assert {:error, :confirmed_claim_precondition_changed} =
             ConfirmedRecoveryCore.verify_local_claim(paths, put_in(payload, ["observation", "expected", "processId"], "foreign"), %{})
  end

  test "native v3 WAL replay finishes every partial write without duplicating release events" do
    {paths, _predecessor, expected} = retirement_only_state()
    payload = %{"contractVersion" => "work-package-paused-confirmed-recovery.v3", "issueId" => @issue, "reservationId" => "reservation-2", "observation" => %{"expected" => expected}}

    assert {:ok, post} =
             ConfirmedRecoveryClaimTransition.prepare_postimages(
               paths.journal.state,
               paths.fence.state,
               paths.graph.state,
               payload,
               500
             )

    {:ok, journal} = Journal.encode_bytes(paths.journal.state)
    {:ok, fence} = FencePersistence.encode_bytes(paths.fence.state)
    {:ok, graph} = GraphPersistence.encode_bytes(paths.graph.state)
    before = %{journal: journal, fence: fence, graph: graph}
    after_images = %{journal: post.claimJournal, fence: post.fence, graph: post.responsibilityGraph}
    names = [:journal, :fence, :graph]
    images = Enum.map(names, fn name -> %{name: name, preimage_sha256: :crypto.hash(:sha256, before[name]) |> Base.encode16(case: :lower), postimage_bytes: after_images[name]} end)

    for prefix <- 0..3 do
      current = Enum.take(names, prefix) |> Enum.reduce(before, fn name, acc -> Map.put(acc, name, after_images[name]) end)
      {:ok, state} = Agent.start_link(fn -> current end)
      read = fn name -> Agent.get(state, &Map.fetch!(&1, name)) end
      persist = fn name, bytes, _already -> Agent.update(state, &Map.put(&1, name, bytes)) end
      assert :ok = ConfirmedRecoveryWAL.apply_images(images, read, persist)
      assert :ok = ConfirmedRecoveryWAL.apply_images(images, read, persist)
      assert Agent.get(state, & &1) == after_images
      assert {:ok, released} = GraphPersistence.decode_bytes(read.(:graph))
      assert released.delegations[@responsible].status == :blocked
      assert released.delegations[@responsible].runtime_lease == nil
      Agent.stop(state)
    end

    unrelated = put_in(paths.graph.state, [:delegations, @responsible, :runtime_lease], nil)
    {:ok, unrelated_bytes} = GraphPersistence.encode_bytes(unrelated)

    assert {:error, :transaction_target_conflict} =
             ConfirmedRecoveryWAL.apply_images(
               [List.last(images)],
               fn :graph -> unrelated_bytes end,
               fn _, _, _ -> flunk("unbound missing lease must hold") end
             )
  end

  defp retirement_only_state do
    {journal, fence, graph, payload} = confirmed_state()
    expected = Map.put(payload["observation"]["expected"], "projectionId", "projection-2")
    lease = fence.executions[@issue].leases[@session] |> Map.merge(%{head: "unobserved", last_heartbeat_at: 0})
    fence = put_in(fence, [:executions, @issue, :ownership], :unknown)
    fence = put_in(fence, [:executions, @issue, :leases, @session], lease)
    fence = put_in(fence, [:sessions, @session], lease)

    graph =
      Enum.reduce(["accountable", @responsible], graph, fn id, state ->
        state
        |> put_in([:delegations, id, :status], :blocked)
        |> put_in([:delegations, id, :blocked_on], :restart_reconciliation)
      end)

    prior_a = %{graph.delegations["accountable"] | id: "prior-accountable", status: :revoked, blocked_on: nil}
    prior_a = Map.put(prior_a, :terminal_reason, :unsubmitted_successor)

    prior_r = %{
      graph.delegations[@responsible]
      | id: "prior-responsible",
        parent_delegation_id: prior_a.id,
        status: :revoked,
        blocked_on: nil,
        runtime_lease: nil,
        terminal_reason: :unsubmitted_successor
    }

    observation = %{"provider_projection_id" => "projection-2", "workspace_absent" => true, "process_count" => 0}
    grant_fields = ~w(id parent_delegation_id role actor_id scope authority budget expires_at_ms expected_deliverable expected_evidence return_to_parent)a

    receipt = %{
      "type" => "unsubmitted_successor",
      "issue_id" => @issue,
      "generation" => 1,
      "repository_ref" => @repository,
      "managed_project_profile_id" => @profile,
      "provider_projection_id" => "projection-2",
      "retired_at_ms" => 400,
      "linear_state" => "In Progress",
      "prior_accountable_id" => prior_a.id,
      "prior_responsible_id" => prior_r.id,
      "successor_accountable_id" => "accountable",
      "successor_responsible_id" => @responsible,
      "manifest_sha256" => String.duplicate("a", 64),
      "signer_key_sha256" => String.duplicate("b", 64),
      "observation_sha256" => native_digest(observation),
      "active_process" => "absent",
      "local_claim" => "absent",
      "provider_claim" => "absent",
      "workspace" => "absent"
    }

    rows = [prior_a, prior_r, graph.delegations["accountable"], graph.delegations[@responsible]]
    fields = ~w(prior_accountable_digest prior_responsible_digest successor_accountable_digest successor_responsible_digest)

    receipt =
      Enum.zip(rows, fields)
      |> Enum.reduce(receipt, fn {row, field}, acc ->
        Map.put(acc, field, native_digest(Map.take(row, grant_fields)))
      end)

    receipt = Map.put(receipt, "evidence_ref", Evidence.retirement_evidence_ref(receipt))

    source =
      Map.drop(receipt, ~w(active_process linear_state local_claim provider_claim provider_projection_id retired_at_ms workspace))
      |> Map.put("observation", observation)
      |> Map.put("prepared_at_ms", 400)

    graph = %{
      graph
      | delegations:
          graph.delegations
          |> Map.put(prior_a.id, %{prior_a | terminal_evidence: source})
          |> Map.put(prior_r.id, %{prior_r | terminal_evidence: source})
    }

    prior_lease = %{lease | generation: 1, session_id: "prior-session", process_id: "prior-process", status: :released}
    prior_lease = Map.put(prior_lease, :release_reason, "claim_not_submitted")

    native_receipt =
      Map.new(receipt, fn {key, value} ->
        {String.to_existing_atom(key), if(value == "absent", do: :absent, else: value)}
      end)

    retired_fields = %{
      generation: 1,
      leases: %{"prior-session" => prior_lease},
      status: :retired,
      cleanup: :cleaned,
      cleaned_at_ms: 400,
      retirement: native_receipt
    }

    prior = Map.merge(fence.executions[@issue], retired_fields)
    fence = %{fence | history: [prior]}
    {:ok, bytes} = FencePersistence.encode_bytes(fence)
    [execution] = Jason.decode!(bytes)["history"]
    predecessor = %{"execution" => execution, "claim" => nil, "receipt" => execution["retirement"]}
    {%{journal: %{state: journal}, fence: %{state: fence}, graph: %{state: graph}}, predecessor, expected}
  end

  defp native_digest(value), do: :crypto.hash(:sha256, :erlang.term_to_binary(value, [:deterministic])) |> Base.encode16(case: :lower)

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
