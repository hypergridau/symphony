Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.ManagedResponsibilityAdmissionTest do
  use SymphonyElixir.TestSupport
  alias SymphonyElixir.{ExecutionFence, ManagedResponsibility, ResponsibilityGraph}

  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ManagedResponsibility.Admission
  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.WorkPackageClaim.Journal

  setup do
    root = Path.dirname(Workflow.workflow_file_path())
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", workspace_root: Path.join(root, "workspaces"), codex_max_total_tokens: 500_000)
    Application.put_env(:symphony_elixir, :memory_tracker_issues, [])
    now = System.system_time(:millisecond)
    {:ok, graph, _} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), now)
    {:ok, manifest} = ManagedResponsibility.decode(Fixture.payload(now), Fixture.context(), now)

    state = %Orchestrator.State{
      execution_fence: ExecutionFence.new(),
      responsibility_graph: graph,
      execution_fence_path: Config.execution_fence_state_path(),
      responsibility_graph_path: Config.responsibility_graph_state_path(),
      work_package_runtime: %{managed_delegations: manifest}
    }

    %{root: root, now: now, graph: graph, manifest: manifest, state: Fixture.initialize_budget(state)}
  end

  test "normal admission persists matching graph and generation, and restart blocks the bound grant", %{state: state, manifest: manifest, now: now} do
    assert {:ok, admitted, token, session, "responsible-1", lease} =
             Orchestrator.admit_execution_for_test(state, Fixture.issue(1), nil)

    assert token.generation == 1
    assert admitted.responsibility_graph.delegations["responsible-1"].runtime_lease == lease
    assert {:ok, saved_graph} = GraphPersistence.load(state.responsibility_graph_path)
    assert {:ok, saved_fence} = FencePersistence.load(state.execution_fence_path)
    assert saved_graph.delegations["responsible-1"].runtime_lease.session_id == session
    assert saved_fence.executions[Fixture.issue(1).id].leases[session].generation == token.generation
    assert map_size(saved_graph.delegations) == 2
    name = Module.concat(__MODULE__, "Restart#{System.unique_integer([:positive])}")
    pid = start_supervised!({Orchestrator, name: name})
    snapshot = Orchestrator.responsibility_snapshot(pid)
    assert Enum.any?(snapshot.delegations, &(&1.id == "responsible-1" and &1.status == :blocked))
    restarted = :sys.get_state(pid)
    assert {:error, _} = Admission.prepare(restarted.responsibility_graph, restarted.execution_fence, manifest, Fixture.issue(1), nil, now + 1)
    assert {:error, _} = Admission.prepare(restarted.responsibility_graph, restarted.execution_fence, manifest, Fixture.issue(2), nil, now + 1)
    assert restarted.running == %{}
  end

  test "a terminal graph does not bypass pending repository cleanup", %{state: state, manifest: manifest, now: now} do
    {:ok, admitted, token, session, _, lease} = Orchestrator.admit_execution_for_test(state, Fixture.issue(1), nil)
    {:ok, completed, _} = ResponsibilityGraph.complete(admitted.responsibility_graph, "responsible-1", %{head: "reviewed"}, now + 1)
    {:ok, terminal, _} = ExecutionFence.fence(admitted.execution_fence, token, %{terminal_state: "Done", accepted_head: "abc123"}, now + 1)
    assert {:error, :previous_repository_cleanup_required} = Admission.prepare(completed, terminal, manifest, Fixture.issue(2), nil, now + 2)
    forged_cleaned = put_in(terminal, [:executions, Fixture.issue(1).id, :cleanup], :cleaned)
    assert {:error, _} = Admission.prepare(completed, forged_cleaned, manifest, Fixture.issue(2), nil, now + 2)
    {:ok, released, _} = ExecutionFence.release(terminal, token, session, :orchestrator_stop)

    evidence = %{
      session_id: session,
      process_id: lease.process_id,
      process_tree: :terminated,
      evidence_ref: "test:local-contract-termination",
      observed_at_ms: now + 2
    }

    {:ok, confirmed, _} = ExecutionFence.confirm_termination(released, token, session, evidence, now + 2)
    {:ok, cleaned, _} = ExecutionFence.cleanup(confirmed, token, "abc123", now + 2)
    assert {:ok, successor} = Admission.prepare(completed, cleaned, manifest, Fixture.issue(2), nil, now + 2)
    assert successor.delegations["responsible-2"].status == :active
  end

  test "model escalation and unbounded token configuration do not exceed operator budget", %{graph: graph, manifest: manifest, now: now} do
    assert {:ok, _} = Admission.prepare(graph, ExecutionFence.new(), manifest, Fixture.issue(1), 2, now)
    assert {:ok, _} = Admission.prepare(graph, ExecutionFence.new(), manifest, Fixture.issue(1), 3, now)
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", codex_max_total_tokens: 0)
    assert {:error, :managed_responsibility_budget_exceeded} = Admission.prepare(graph, ExecutionFence.new(), manifest, Fixture.issue(1), nil, now)
  end

  test "managed GPT-6 Luna default requires a matching grant and retry effort ceiling", %{graph: graph, now: now} do
    issue = Fixture.issue(1)

    assert {:ok, ^graph} = Admission.prepare(graph, nil, nil, nil, nil, now)

    assert {:error, :invalid_issue} =
             Admission.prepare(graph, nil, nil, %{labels: ["model:gpt-6-luna"]}, nil, now)

    assert {:ok, ^graph} = Admission.prepare(graph, ExecutionFence.new(), nil, issue, nil, now)

    assert {:error, :managed_responsibility_required_for_gpt6_luna} =
             Admission.prepare(graph, ExecutionFence.new(), nil, %{issue | labels: ["model:gpt-6-luna"]}, nil, now)

    old_payload =
      update_in(Fixture.payload(now), ["entries"], fn entries ->
        Enum.map(entries, fn entry ->
          entry
          |> put_in(["accountable", "budget", "model"], "gpt-5.6-luna")
          |> put_in(["responsible", "budget", "model"], "gpt-5.6-luna")
        end)
      end)

    {:ok, old_manifest} = ManagedResponsibility.decode(old_payload, Fixture.context(), now)

    assert {:error, :managed_responsibility_budget_exceeded} =
             Admission.prepare(graph, ExecutionFence.new(), old_manifest, issue, nil, now)

    payload =
      update_in(Fixture.payload(now), ["entries"], fn entries ->
        Enum.map(entries, fn entry ->
          entry
          |> put_in(["accountable", "budget", "effort"], "high")
          |> put_in(["responsible", "budget", "effort"], "high")
        end)
      end)

    assert {:ok, new_manifest} = ManagedResponsibility.decode(payload, Fixture.context(), now)
    assert {:ok, _} = Admission.prepare(graph, ExecutionFence.new(), new_manifest, issue, nil, now)

    # Scheduler attempts alone do not prove a failed worker turn.
    assert {:ok, _} = Admission.prepare(graph, ExecutionFence.new(), new_manifest, issue, 9, now)

    write_failed_turn_journal!(issue, new_manifest, now)

    assert {:error, :managed_responsibility_budget_exceeded} =
             Admission.prepare(graph, ExecutionFence.new(), new_manifest, issue, nil, now)

    for label <- ["model:luna", "model:terra", "model:sol", "model:spark", "model:unknown"] do
      labeled = %{issue | labels: ["symphony-ready", label]}

      assert {:error, :unsupported_managed_model_label} =
               Admission.prepare(graph, ExecutionFence.new(), new_manifest, labeled, nil, now)
    end
  end

  test "a fence write failure cannot produce a dispatchable restart or silently rebind", %{state: state, root: root, manifest: manifest, now: now} do
    invalid_parent = Path.join(root, "regular-file")
    File.write!(invalid_parent, "not a directory")
    failed_state = %{state | execution_fence_path: Path.join(invalid_parent, "fence.json")}
    assert {:error, _} = Orchestrator.admit_execution_for_test(failed_state, Fixture.issue(1), nil)
    assert {:ok, saved} = GraphPersistence.load(state.responsibility_graph_path)
    assert saved.delegations["responsible-1"].runtime_lease != nil
    {:ok, restarted} = ResponsibilityGraph.mark_unreconciled_after_restart(saved)
    assert restarted.delegations["responsible-1"].status == :blocked
    assert {:error, _} = Admission.prepare(restarted, ExecutionFence.new(), manifest, Fixture.issue(1), nil, now + 1)
    assert {:error, _} = Admission.prepare(restarted, ExecutionFence.new(), manifest, Fixture.issue(2), nil, now + 1)
  end

  test "a larger configured ceiling admits a smaller grant without changing its terms", %{graph: graph, manifest: manifest, now: now} do
    write_workflow_file!(Workflow.workflow_file_path(), tracker_kind: "memory", codex_max_total_tokens: 750_000)
    assert {:ok, admitted} = Admission.prepare(graph, ExecutionFence.new(), manifest, Fixture.issue(1), nil, now)
    assert admitted.delegations["responsible-1"].budget == hd(manifest.entries).responsible.budget
    assert admitted.delegations["responsible-1"].budget.max_tokens == 500_000
  end

  defp write_failed_turn_journal!(issue, manifest, now) do
    repository = manifest.repository_ref
    profile = manifest.managed_project_profile_id
    key = Journal.reservation_key(issue.id, profile, repository)

    reservation = %{
      issue_id: issue.id,
      managed_project_profile_id: profile,
      repository_ref: repository,
      projection_id: "projection-1",
      reservation_id: "reservation-1",
      reservation_nonce: "nonce-1",
      scope_keys: ["repo:#{repository}"],
      runner_id: "runner-test",
      generation: 1,
      session_id: "session-1",
      process_id: "process-1",
      responsible_delegation_id: "responsible-1",
      execution_fence_token: "#{issue.id}:1",
      runtime_lease_id: "session-1"
    }

    evidence = %{
      thread_id: "thread-1",
      turn_id: "turn-1",
      observed_at_ms: now,
      payload_sha256: String.duplicate("a", 64)
    }

    {:ok, journal} = Journal.put(Journal.new(), key, reservation)
    {:ok, journal} = Journal.put_failed_worker_turn(journal, key, "thread-1:turn-1", evidence)
    :ok = Journal.save(Config.execution_fence_state_path() <> ".work-package", journal)
  end
end
