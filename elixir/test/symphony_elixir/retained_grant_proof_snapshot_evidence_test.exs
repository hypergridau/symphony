Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.RetainedGrantProofSnapshotEvidenceTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ManagedResponsibility
  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture
  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.RetainedGrantProof.GrantEvidence
  alias SymphonyElixir.RetainedGrantProof.SnapshotEvidence
  alias SymphonyElixir.WorkPackageClaim.Journal

  setup do
    dir = Path.join(System.tmp_dir!(), "retained-snapshot-#{Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)}")
    File.mkdir!(dir)
    {:ok, manifest} = ManagedResponsibility.decode(Fixture.payload(10_000), Fixture.context(), 10_000)
    entry = hd(manifest.entries)
    {:ok, graph, _} = ResponsibilityGraph.delegate(ResponsibilityGraph.new(), entry.accountable, 10_000)
    {:ok, graph, _} = ResponsibilityGraph.delegate(graph, entry.responsible, 10_000)

    {:ok, fence, _} =
      ExecutionFence.admit(ExecutionFence.new(), %{issue_id: entry.issue_id, repository: entry.responsible.scope.repository, branch: "fixture-retained", worktree: Path.join(dir, "workspace")}, 10_000)

    fp = Path.join(dir, "fence.json")
    gp = Path.join(dir, "graph.json")
    :ok = FencePersistence.save(fp, fence)
    :ok = GraphPersistence.save(gp, graph)
    fb = File.read!(fp)
    gb = File.read!(gp)

    %{
      fence_bytes: fb,
      graph_bytes: gb,
      binding: %{fence_sha256: hex(fb), graph_sha256: hex(gb)},
      fence: fence,
      graph: graph,
      issue_id: entry.issue_id
    }
  end

  test "actual nonempty snapshots retain validated state without conferring admission", data do
    assert {:ok, result} = SnapshotEvidence.decode(data.fence_bytes, data.graph_bytes, data.binding)
    assert result.fence == data.fence
    assert result.graph.delegations == data.graph.delegations
    assert result.graph.events == Jason.decode!(Jason.encode!(data.graph.events))
    assert result.fence.executions[data.issue_id].generation == 1
    assert map_size(result.graph.delegations) == 2
    refute Map.has_key?(result, :signature)
    refute Map.has_key?(result, :ready)
  end

  test "a structurally valid replacement cannot satisfy the pinned original bytes", data do
    replacement = data.fence_bytes |> Jason.decode!() |> put_in(["executions", data.issue_id, "branch"], "replacement") |> Jason.encode!()
    assert {:ok, _} = FencePersistence.decode_bytes(replacement)
    assert {:error, _} = SnapshotEvidence.decode(replacement, data.graph_bytes, data.binding)
    assert {:error, _} = SnapshotEvidence.decode(data.fence_bytes, data.graph_bytes <> " ", data.binding)
  end

  test "hash matching does not waive malformed structure or size limits", data do
    malformed = ~s({"schema_version":1,"executions":[],"sessions":{},"history":[]})
    assert {:error, _} = SnapshotEvidence.decode(malformed, data.graph_bytes, %{data.binding | fence_sha256: hex(malformed)})

    for invalid <- [nil, %{}, Map.put(data.binding, :caller_override, true), %{data.binding | fence_sha256: <<255>>}] do
      assert {:error, _} = SnapshotEvidence.decode(data.fence_bytes, data.graph_bytes, invalid)
    end

    oversized = String.duplicate(" ", 262_145)
    assert {:error, _} = SnapshotEvidence.decode(oversized, data.graph_bytes, %{data.binding | fence_sha256: hex(oversized)})
  end

  test "retained consistency joins original pair and exact terminated worker generation" do
    {snapshots, grant, binding} = retained()
    assert {:ok, facts} = SnapshotEvidence.match_retained(snapshots, grant, binding, 10_100)
    assert facts.generation == 1
    assert facts.terminal.state == "Failed attempt"
    assert facts.terminal.accepted_head == "original-head"
    assert snapshots.fence.executions[grant.issue_id].cleanup == :pending
    assert {:error, _} = ExecutionFence.validate_cleanup(snapshots.fence, %{issue_id: grant.issue_id, generation: 1}, "changed-retained-head")

    for {key, value} <- [{:generation, 2}, {:repository, "other/repo"}, {:branch, "other"}, {:worktree, "/other"}, {:issue_id, Fixture.issue(2).id}, {:terminal, %{}}] do
      assert {:error, _} = SnapshotEvidence.match_retained(snapshots, grant, Map.put(binding, key, value), 10_100)
    end

    wrong_lease = %{binding | runtime_lease: %{binding.runtime_lease | generation: 2}}
    assert {:error, _} = SnapshotEvidence.match_retained(snapshots, grant, wrong_lease, 10_100)
    assert {:error, _} = SnapshotEvidence.match_retained(snapshots, grant, binding, 70_000)
    assert {:error, _} = SnapshotEvidence.match_retained(snapshots, grant, binding, 9_999)

    incomplete = %{accountable: %{id: grant.accountable_id}, responsible: %{id: grant.responsible_id}}

    for invalid <- [nil, %{}, %{delegations: %{}}, %{grant | delegations: incomplete}] do
      assert {:error, _} = SnapshotEvidence.match_retained(snapshots, invalid, binding, 10_100)
    end
  end

  test "independently valid graph replacement and revocation deny the original retained proof" do
    {snapshots, grant, binding} = retained()
    changed = put_in(snapshots.graph, [:delegations, grant.responsible_id, :runtime_lease, :generation], 2)
    assert :ok = ResponsibilityGraph.validate(changed)
    assert {:error, _} = SnapshotEvidence.match_retained(%{snapshots | graph: changed}, grant, binding, 10_100)
    {:ok, revoked, _} = ResponsibilityGraph.revoke(snapshots.graph, grant.accountable_id, :test_revocation, 10_050)
    assert :ok = ResponsibilityGraph.validate(revoked)
    assert {:error, _} = SnapshotEvidence.match_retained(%{snapshots | graph: revoked}, grant, binding, 10_100)
    changed_budget = put_in(snapshots.graph, [:delegations, grant.responsible_id, :budget, :max_tokens], 499_999)
    assert :ok = ResponsibilityGraph.validate(changed_budget)
    assert {:error, _} = SnapshotEvidence.match_retained(%{snapshots | graph: changed_budget}, grant, binding, 10_100)
  end

  test "active and unreconciled retained executions never yield consistency facts" do
    {snapshots, grant, binding} = retained()

    for replacement <- [
          %{snapshots.fence.executions[grant.issue_id] | ownership: :unknown},
          %{snapshots.fence.executions[grant.issue_id] | status: :active, terminal: nil}
        ] do
      fence = put_in(snapshots.fence, [:executions, grant.issue_id], replacement)
      assert :ok = ExecutionFence.validate(fence)
      assert {:error, _} = SnapshotEvidence.match_retained(%{snapshots | fence: fence}, grant, binding, 10_100)
    end
  end

  defp retained do
    payload = Fixture.payload(10_000)
    {:ok, manifest} = ManagedResponsibility.decode(payload, Fixture.context(), 10_000)
    entry = hd(manifest.entries)
    {:ok, grant} = GrantEvidence.decode(Jason.encode!(payload), Fixture.context(), entry.issue_id, 10_000)
    admission = %{issue_id: entry.issue_id, repository: entry.responsible.scope.repository, branch: "retained", worktree: "/fixture/workspace"}

    reference = %{
      issue_id: entry.issue_id,
      repository: admission.repository,
      generation: 1,
      session_id: "worker",
      process_id: "process-worker"
    }

    {:ok, graph, _} = ResponsibilityGraph.delegate(ResponsibilityGraph.new(), entry.accountable, 10_000)
    {:ok, graph, _} = ResponsibilityGraph.delegate(graph, Map.put(entry.responsible, :runtime_lease, reference), 10_000)
    {:ok, fence, token} = ExecutionFence.admit(ExecutionFence.new(), admission, 10_000)

    session =
      Map.merge(admission, %{
        session_id: "worker",
        process_id: "process-worker",
        linear_state: "In Progress",
        pr_state: "OPEN",
        head: "original-head",
        last_heartbeat_at: 10_000
      })

    {:ok, fence, :registered} = ExecutionFence.register(fence, token, :worker, session, 10_000)
    {:ok, fence, :fenced} = ExecutionFence.fence(fence, token, %{terminal_state: "Failed attempt", accepted_head: "original-head"}, 10_010)
    {:ok, fence, :released} = ExecutionFence.release(fence, token, "worker", :orchestrator_stop)
    evidence = %{session_id: "worker", process_id: "process-worker", process_tree: :terminated, evidence_ref: "test:termination", observed_at_ms: 10_020}
    {:ok, fence, :confirmed} = ExecutionFence.confirm_termination(fence, token, "worker", evidence, 10_020)
    binding = Map.merge(admission, %{generation: 1, runtime_lease: reference, terminal: Jason.decode!(Jason.encode!(fence.executions[entry.issue_id].terminal))})
    {%{fence: fence, graph: graph}, grant, binding}
  end

  test "pinned original claim matches exact grant and worker tuple" do
    {_snapshots, grant, binding} = retained()
    {bytes, expected} = claim_fixture(grant, binding)
    assert {:ok, facts} = SnapshotEvidence.match_claim(bytes, hex(bytes), grant, binding, expected)
    assert facts.claim == expected
    assert facts.journal_sha256 == hex(bytes)
    assert facts.cleanup_receipts == %{}

    for {key, value} <- [
          {:reservation_id, "other"},
          {:reservation_nonce, "other"},
          {:projection_id, "other"},
          {:execution_fence_token, "other"},
          {:runtime_lease_id, "other"},
          {:runner_id, "other"},
          {:responsible_delegation_id, "other"},
          {:scope_keys, ["other"]}
        ] do
      assert {:error, _} = SnapshotEvidence.match_claim(bytes, hex(bytes), grant, binding, Map.put(expected, key, value))
    end

    assert {:error, _} = SnapshotEvidence.match_claim(bytes, String.duplicate("0", 64), grant, binding, expected)
    assert {:error, _} = SnapshotEvidence.match_claim(bytes, hex(bytes), grant, %{binding | generation: 0}, expected)
    no_profile = %{expected | managed_project_profile_id: nil}
    assert {:error, _} = SnapshotEvidence.match_claim(bytes, hex(bytes), grant, binding, no_profile)
    assert {:error, _} = SnapshotEvidence.match_claim("{broken}", hex("{broken}"), grant, binding, expected)
  end

  test "duplicate tuple aliases and valid changed claims cannot replace original claim" do
    {_snapshots, grant, binding} = retained()
    {bytes, expected} = claim_fixture(grant, binding)
    payload = Jason.decode!(bytes)
    original = payload["reservations"] |> Map.values() |> hd()
    duplicate = payload |> put_in(["reservations", "alias"], original) |> Jason.encode!()
    assert {:ok, _} = Journal.decode_bytes(duplicate)
    assert {:error, _} = SnapshotEvidence.match_claim(duplicate, hex(duplicate), grant, binding, expected)
    changed = payload |> update_in(["reservations"], fn reservations -> Map.new(reservations, fn {key, claim} -> {key, Map.put(claim, "reservation_nonce", "replacement")} end) end) |> Jason.encode!()
    assert {:ok, _} = Journal.decode_bytes(changed)
    assert {:error, _} = SnapshotEvidence.match_claim(changed, hex(bytes), grant, binding, expected)
    assert {:error, _} = SnapshotEvidence.match_claim(changed, hex(changed), grant, binding, expected)
  end

  defp claim_fixture(grant, binding) do
    expected = %{
      issue_id: grant.issue_id,
      managed_project_profile_id: grant.context.managed_project_profile_id,
      repository_ref: grant.context.repository_ref,
      projection_id: "projection-test",
      reservation_id: "reservation-test",
      reservation_nonce: "nonce-test",
      scope_keys: ["repo:test"],
      runner_id: grant.context.runner_id,
      generation: binding.generation,
      session_id: binding.runtime_lease.session_id,
      process_id: binding.runtime_lease.process_id,
      responsible_delegation_id: grant.responsible_id,
      execution_fence_token: "fence-test",
      runtime_lease_id: "lease-test"
    }

    profile = grant.context.managed_project_profile_id
    key = Journal.reservation_key(grant.issue_id, profile, grant.context.repository_ref, binding.generation)
    {:ok, journal} = Journal.put(Journal.new(), key, expected)
    assert :ok = Journal.validate(journal)
    raw = Map.new(expected, fn {key, value} -> {Atom.to_string(key), value} end)
    {Jason.encode!(%{"schema_version" => 1, "reservations" => %{key => raw}}), expected}
  end

  defp hex(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end
