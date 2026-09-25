defmodule SymphonyElixir.RuntimeIdentityTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{ExecutionFence, ResponsibilityGraph, RuntimeIdentity}

  @source_head_1 String.duplicate("1", 40)
  @source_head_2 String.duplicate("2", 40)

  @env %{
    "SYMPHONY_POOL_KEY" => "pool-engineering",
    "SYMPHONY_REPOSITORY_REF" => "openai/symphony",
    "SYMPHONY_ACCEPTED_SOURCE_HEAD" => @source_head_1,
    "SYMPHONY_CURRENT_SOURCE_HEAD" => @source_head_1
  }

  @pause %{configured?: true, paused?: false, state: "running", path: "/run/symphony/pause", reason: nil}

  test "loaded authorization is visible as configuration without inventing active workers" do
    {:ok, graph, _} = ResponsibilityGraph.activate(ResponsibilityGraph.new(), 1)
    digest = String.duplicate("a", 64)
    signer = String.duplicate("b", 64)

    snapshot =
      RuntimeIdentity.snapshot(ExecutionFence.new(), graph,
        env: @env,
        workspace_root: "/srv/symphony/workspaces",
        pause_snapshot: @pause,
        managed_pool?: true,
        managed_runtime_configured?: true,
        managed_delegation_manifest: %{source_sha256: digest, signer_key_sha256: signer, entries: [%{secret: "not projected"}]}
      )

    assert snapshot.execution_authority.delegation_posture == "quiescent"

    assert snapshot.managed_work_package.delegation_manifest ==
             %{state: "configured", sha256: digest, signer_key_sha256: signer, authorized_issue_count: 1}

    refute inspect(snapshot) =~ "not projected"
  end

  test "configured identity reports the effective paths and real authority posture" do
    snapshot =
      RuntimeIdentity.snapshot(ExecutionFence.new(), enforced_graph(),
        env: @env,
        workspace_root: "/srv/symphony/workspaces",
        pause_snapshot: @pause,
        managed_pool?: true,
        managed_runtime_configured?: true
      )

    assert snapshot.runtime_identity == %{
             pool_key: "pool-engineering",
             repository_ref: "openai/symphony",
             workspace_root: "/srv/symphony/workspaces",
             global_pause_file: "/run/symphony/pause",
             accepted_source_head: @source_head_1,
             status: "configured",
             source_head_status: "verified"
           }

    assert snapshot.execution_authority == %{
             fence: "hgs294",
             delegation: "hgs300",
             fence_posture: "quiescent",
             delegation_posture: "active",
             status: "ready"
           }

    assert snapshot.managed_work_package == %{
             required?: true,
             configured?: true,
             state: "configured"
           }

    assert snapshot.readiness == %{ready?: true, status: "ready", reasons: []}
  end

  test "a changed observed source head makes readiness stale" do
    snapshot =
      RuntimeIdentity.snapshot(ExecutionFence.new(), enforced_graph(),
        env: Map.put(@env, "SYMPHONY_CURRENT_SOURCE_HEAD", @source_head_2),
        workspace_root: "/srv/symphony/workspaces",
        pause_snapshot: @pause,
        managed_pool?: true,
        managed_runtime_configured?: true
      )

    assert snapshot.runtime_identity.status == "stale"
    assert snapshot.runtime_identity.source_head_status == "stale"
    assert snapshot.readiness.ready? == false
    assert "accepted_source_head_stale" in snapshot.readiness.reasons
  end

  test "a missing observed source head keeps managed readiness unverified" do
    snapshot =
      RuntimeIdentity.snapshot(ExecutionFence.new(), enforced_graph(),
        env:
          @env
          |> Map.delete("SYMPHONY_CURRENT_SOURCE_HEAD")
          |> Map.put("SYMPHONY_SOURCE_HEAD", @source_head_1),
        workspace_root: "/srv/symphony/workspaces",
        pause_snapshot: @pause,
        managed_pool?: true,
        managed_runtime_configured?: true
      )

    assert snapshot.runtime_identity.status == "configured"
    assert snapshot.runtime_identity.source_head_status == "unverified"
    assert snapshot.readiness.ready? == false
    assert "accepted_source_head_unverified" in snapshot.readiness.reasons
  end

  test "source revisions must be full Git object IDs" do
    snapshot =
      RuntimeIdentity.snapshot(ExecutionFence.new(), enforced_graph(),
        env: Map.put(@env, "SYMPHONY_ACCEPTED_SOURCE_HEAD", "source-head-1"),
        workspace_root: "/srv/symphony/workspaces",
        pause_snapshot: @pause,
        managed_pool?: true,
        managed_runtime_configured?: true
      )

    assert snapshot.runtime_identity.status == "invalid"
    assert snapshot.runtime_identity.source_head_status == "invalid"
    assert snapshot.readiness.ready? == false
    assert "accepted_source_head_invalid" in snapshot.readiness.reasons

    observed_invalid =
      RuntimeIdentity.snapshot(ExecutionFence.new(), enforced_graph(),
        env: Map.put(@env, "SYMPHONY_CURRENT_SOURCE_HEAD", "not-a-source-id"),
        workspace_root: "/srv/symphony/workspaces",
        pause_snapshot: @pause,
        managed_pool?: true,
        managed_runtime_configured?: true
      )

    assert observed_invalid.runtime_identity.source_head_status == "invalid"
    assert "accepted_source_head_invalid" in observed_invalid.readiness.reasons
  end

  test "an enforced graph without active responsible execution reports quiescent authority" do
    graph = enforced_graph()

    observer_only_graph = %{
      graph
      | delegations: Map.update!(graph.delegations, "delegation-runtime", &Map.put(&1, :role, :observer))
    }

    snapshot =
      RuntimeIdentity.snapshot(ExecutionFence.new(), observer_only_graph,
        env: @env,
        workspace_root: "/srv/symphony/workspaces",
        pause_snapshot: @pause,
        managed_pool?: true,
        managed_runtime_configured?: true
      )

    assert snapshot.execution_authority.delegation_posture == "quiescent"
    assert snapshot.execution_authority.status == "ready"
    assert snapshot.readiness == %{ready?: true, status: "ready", reasons: []}
  end

  test "managed runtime with missing identity fails readiness" do
    snapshot =
      RuntimeIdentity.snapshot(ExecutionFence.new(), ResponsibilityGraph.new(),
        env: %{},
        workspace_root: nil,
        pause_snapshot: %{path: nil},
        managed_pool?: true,
        managed_runtime_configured?: false
      )

    assert snapshot.runtime_identity.status == "missing"
    assert snapshot.managed_work_package.state == "missing"
    assert snapshot.readiness.ready? == false
    assert "managed_runtime_configuration_missing" in snapshot.readiness.reasons
    assert "runtime_identity_pool_key_missing" in snapshot.readiness.reasons
    assert "runtime_identity_accepted_source_head_missing" in snapshot.readiness.reasons
  end

  defp enforced_graph do
    scope = %{
      company_id: "company",
      objective_id: "objective",
      initiative_id: "initiative",
      project_id: "project",
      work_package_id: "package",
      issue_id: :any,
      repository: "openai/symphony",
      paths: [],
      modules: [],
      environments: ["local"],
      actions: [:read, :observe, :delegate, :reconcile, :edit, :commit, :push, :state_mutation, :cleanup, :review, :report]
    }

    authority = %{
      class: :routine_engineering,
      capabilities: scope.actions,
      environments: ["local"]
    }

    attrs = %{
      id: "delegation-runtime",
      parent_delegation_id: nil,
      role: :responsible,
      actor_id: "actor-runtime",
      scope: scope,
      authority: authority,
      budget: %{model: "luna", effort: :max, max_tokens: 10_000, max_children: 1},
      runtime_lease: nil,
      expires_at_ms: 10_000,
      expected_deliverable: "bounded deliverable",
      expected_evidence: "tests",
      return_to_parent: %{owner_id: "owner", contract: "return evidence"}
    }

    {:ok, graph, _delegation} = ResponsibilityGraph.delegate(ResponsibilityGraph.new(), attrs, 0)
    %{graph | enforcement: :enforced}
  end
end
