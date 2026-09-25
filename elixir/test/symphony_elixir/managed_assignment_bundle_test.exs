defmodule SymphonyElixir.ManagedAssignmentBundleTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{AgentRunner, ManagedAssignmentBundle}

  test "a managed checkout cannot enter AgentRunner without its assignment bundle" do
    assert {:error, :assignment_bundle_missing} =
             AgentRunner.assignment_bundle_preflight_for_test(execution_checkout: %{branch: "codex/task"})

    assert {:error, :assignment_bundle_missing} =
             AgentRunner.assignment_bundle_preflight_for_test(managed_model_route: true)

    assert :ok = AgentRunner.assignment_bundle_preflight_for_test([])
  end

  test "builds a stable digest and validates the complete assignment contract" do
    attrs = valid_attrs()

    assert {:ok, first} = ManagedAssignmentBundle.build(attrs)
    assert {:ok, reordered} = ManagedAssignmentBundle.build(Map.new(Enum.reverse(Map.to_list(attrs))))
    assert first == reordered

    set_order = %{
      attrs
      | context_secret_refs: ["Z_LAST_SECRET", "A_FIRST_SECRET"],
        environment_constraints: ["no-production-workload", "repository"]
    }

    sorted_sets = %{
      set_order
      | context_secret_refs: ["A_FIRST_SECRET", "Z_LAST_SECRET"],
        environment_constraints: ["repository", "no-production-workload"]
    }

    assert {:ok, stable_sets} = ManagedAssignmentBundle.build(set_order)
    assert {:ok, stable_sets_sorted} = ManagedAssignmentBundle.build(sorted_sets)
    assert stable_sets == stable_sets_sorted
    assert :ok = ManagedAssignmentBundle.validate_bundle(first)
    assert first.objective == %{id: "objective-1", identity: "objective-1", content: "Ship the managed runner"}
    assert first.context_secret_refs == ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"]
    refute Map.has_key?(first, :runner_token)
  end

  test "fails closed when required objective or environment context is missing" do
    attrs = valid_attrs()

    assert {:error, :assignment_bundle_objective_invalid} =
             ManagedAssignmentBundle.build(Map.put(attrs, :objective, %{id: "objective-1", identity: "objective-1"}))

    assert {:error, :assignment_bundle_context_missing} =
             ManagedAssignmentBundle.build(Map.delete(attrs, :platform))

    assert {:error, :assignment_bundle_context_missing} =
             ManagedAssignmentBundle.build(Map.delete(attrs, :environment_constraints))

    assert {:error, :assignment_bundle_objective_invalid} =
             ManagedAssignmentBundle.build(put_in(attrs, [:objective, :identity], "other-objective"))
  end

  test "rejects secret values and digest tampering" do
    attrs = valid_attrs()

    assert {:error, :assignment_bundle_secret_refs_invalid} =
             ManagedAssignmentBundle.build(%{attrs | context_secret_refs: ["token-value"]})

    assert {:ok, bundle} = ManagedAssignmentBundle.build(attrs)

    assert {:error, :assignment_bundle_digest_mismatch} =
             ManagedAssignmentBundle.validate_bundle(%{bundle | branch: "codex/other"})

    assert {:error, :assignment_bundle_lease_invalid} =
             ManagedAssignmentBundle.build(%{attrs | repository_ref: "hypergridau/other"})
  end

  defp valid_attrs do
    %{
      objective: %{id: "objective-1", identity: "objective-1", content: "Ship the managed runner"},
      repository_ref: "hypergridau/symphony",
      base_ref: "refs/remotes/origin/main",
      branch: "codex/hgs729-assignment",
      seat: "runner-17",
      lease: %{
        issue_id: "issue-1",
        repository: "hypergridau/symphony",
        generation: 4,
        session_id: "worker:issue-1:4",
        process_id: "worker:issue-1:4"
      },
      intent_ancestry: ["objective-root", "delegation-1"],
      acceptance: %{deliverable: "Assignment bundle", evidence: "Focused test coverage"},
      context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
      platform: "linux-x86_64",
      environment_constraints: ["repository", "no-production-workload"]
    }
  end
end
