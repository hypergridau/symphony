defmodule SymphonyElixir.ResponsibilityGraphPersistenceTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.ResponsibilityGraph.Persistence

  @scope %{
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

  @authority %{
    class: :routine_engineering,
    capabilities: [:read, :observe, :delegate, :reconcile, :edit, :commit, :push, :state_mutation, :cleanup, :review, :report],
    environments: ["local"]
  }

  @budget %{model: "luna", effort: :max, max_tokens: 10_000, max_children: 2}

  setup do
    root = Path.join(System.tmp_dir!(), "symphony-responsibility-#{System.unique_integer([:positive])}")
    path = Path.join(root, "graph.json")
    on_exit(fn -> File.rm_rf(root) end)
    {:ok, path: path}
  end

  test "exact-byte decoding preserves graph validation without filesystem effects", %{path: path} do
    state = ResponsibilityGraph.new()
    assert :ok = Persistence.save(path, state)
    bytes = File.read!(path)
    assert {:ok, ^state} = Persistence.decode_bytes(bytes)
    File.write!(path, "{broken}")
    assert {:ok, ^state} = Persistence.decode_bytes(bytes)
    assert File.read!(path) == "{broken}"

    for invalid <- [nil, 7, "{broken}", ~s({"schema_version":1,"delegations":[],"events":[]})] do
      assert {:error, {:invalid_snapshot, _}} = Persistence.decode_bytes(invalid)
    end
  end

  test "persists and rehydrates the versioned delegation contract", %{path: path} do
    {:ok, graph, _owner} = ResponsibilityGraph.delegate(ResponsibilityGraph.new(), delegation("owner", :accountable), 0)

    {:ok, graph2, _worker} =
      ResponsibilityGraph.delegate(
        graph,
        delegation("worker", :responsible,
          parent_delegation_id: "owner",
          runtime_lease: runtime_lease()
        ),
        1
      )

    assert :ok = Persistence.save(path, graph2)
    assert {:ok, restored} = Persistence.load(path)
    assert :ok = ResponsibilityGraph.validate(restored)
    assert restored.schema_version == 1
    assert restored.enforcement == :manual
    assert restored.delegations["owner"].role == :accountable
    assert restored.delegations["worker"].scope.repository == "orchestrator"
    assert restored.delegations["worker"].runtime_lease.session_id == "worker"
    assert restored.delegations["worker"].budget.max_tokens == 10_000
  end

  test "persists machine-enforced admission mode", %{path: path} do
    {:ok, graph, :activated} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), 0)
    assert :ok = Persistence.save(path, graph)
    assert {:ok, restored} = Persistence.load(path)
    assert restored.enforcement == :enforced
    assert ResponsibilityGraph.enforced?(restored)
  end

  test "rejects malformed snapshots and safely replaces an existing file", %{path: path} do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, ~s({"schema_version":1,"delegations":[],"events":[]}))
    assert {:error, {:invalid_snapshot, _reason}} = Persistence.load(path)

    graph = ResponsibilityGraph.new()
    assert :ok = Persistence.save(path, graph)
    assert {:ok, ^graph} = Persistence.load(path)
  end

  defp delegation(id, role, overrides \\ []) do
    Map.merge(
      %{
        id: id,
        parent_delegation_id: nil,
        role: role,
        actor_id: "actor-#{id}",
        scope: @scope,
        authority: @authority,
        budget: @budget,
        runtime_lease: nil,
        expires_at_ms: 10_000,
        expected_deliverable: "bounded deliverable",
        expected_evidence: "tests and review evidence",
        return_to_parent: %{owner_id: "owner", contract: "return evidence and outcome"}
      },
      Map.new(overrides)
    )
  end

  defp runtime_lease do
    %{issue_id: "HGS-300", repository: "orchestrator", generation: 1, session_id: "worker", process_id: "process-worker"}
  end
end
