Code.require_file("../support/managed_executor_fake_adapter.exs", __DIR__)

defmodule SymphonyElixir.ManagedExecutorTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.ManagedExecutor
  alias SymphonyElixir.ManagedExecutor.{FakeAdapter, FakeJournal}

  test "runs the exact assignment lifecycle and replays terminal evidence with verification only" do
    {adapter, journal, opts} = ports()
    assignment = assignment()

    assert {:ok, %{phase: :terminal, cleanup_evidence: evidence}} = ManagedExecutor.run(assignment, opts)
    assert evidence.assignment_digest == assignment.sha256
    assert evidence.accepted_head == "89abcdef0123456789abcdef0123456789abcdef"
    assert evidence.generation == assignment.lease.generation
    assert evidence.credentials_revoked
    assert evidence.workspace_removed

    events = FakeAdapter.events(adapter)

    assert Enum.map(events, &elem(&1, 0)) == [
             :allocate_or_reconcile,
             :prepare_checkout,
             :execute,
             :publish_or_reconcile_result,
             :ensure_terminal_cleanup,
             :verify_terminal_cleanup
           ]

    {:prepare_checkout, _allocation_id, intent, checkout_key} = Enum.at(events, 1)
    assert intent == %{repository_ref: "hypergridau/symphony", base_ref: "refs/remotes/origin/main", branch: "codex/hgs729-fixture"}
    assert checkout_key == "#{assignment.sha256}:checkout"
    assert assignment.context_secret_refs == ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"]
    refute Map.has_key?(assignment, :runner_token)
    refute Enum.any?(events, &(inspect(&1) =~ "runner_token"))

    prior_count = length(events)
    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, opts)
    replay_events = FakeAdapter.events(adapter)
    assert length(replay_events) == prior_count + 1
    assert Enum.count(replay_events, &(elem(&1, 0) != :verify_terminal_cleanup)) == prior_count - 1
    assert is_pid(journal)
  end

  test "reconciles allocation with the same key after an interrupted allocation request" do
    {adapter, _journal, opts} = ports(faults: %{allocate: 1})
    assignment = assignment()

    assert {:held, :allocation_reconciliation_failed, %{phase: :allocation_pending}} =
             ManagedExecutor.run(assignment, opts)

    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, opts)
    keys = for {:allocate_or_reconcile, idempotency_key, _digest} <- FakeAdapter.events(adapter), do: idempotency_key
    assert keys == ["#{assignment.sha256}:allocation", "#{assignment.sha256}:allocation"]
  end

  test "holds an unknown execution outcome after crash and never executes it twice" do
    {adapter, _journal, opts} = ports(faults: %{execute: 1})
    assignment = assignment()

    assert {:held, :execution_outcome_unknown, %{phase: :execution_started}} =
             ManagedExecutor.run(assignment, opts)

    assert {:held, :execution_outcome_unknown, %{phase: :execution_started}} = ManagedExecutor.run(assignment, opts)
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :reconcile_execution)) == 1
  end

  test "holds checkout drift before execution" do
    {adapter, _journal, opts} = ports(checkout_mismatch: true)
    assignment = assignment()

    assert {:held, :checkout_intent_mismatch, %{phase: :checkout_pending}} = ManagedExecutor.run(assignment, opts)
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute))
  end

  test "holds cleanup unless signed terminal cleanup evidence confirms workspace and credential removal" do
    {adapter, journal, opts} = ports(cleanup_invalid: true)
    assignment = assignment()

    assert {:held, :cleanup_evidence_invalid, %{phase: :cleanup_pending}} =
             ManagedExecutor.run(assignment, opts)

    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, opts)
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :ensure_terminal_cleanup)) == 2
    assert is_pid(journal)
  end

  test "holds a structurally complete cleanup receipt when its adapter signature check fails" do
    {adapter, _journal, opts} = ports(signature_invalid: true)
    assignment = assignment()

    assert {:held, :cleanup_unverified, %{phase: :cleanup_pending}} = ManagedExecutor.run(assignment, opts)
    assert Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :verify_terminal_cleanup))
  end

  test "rejects a changed assignment on replay even when issue and generation match" do
    {_adapter, _journal, opts} = ports()
    first = assignment()
    changed = assignment(branch: "codex/other")

    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(first, opts)
    assert {:error, :assignment_replay_mismatch} = ManagedExecutor.run(changed, opts)
  end

  test "recovers a recorded result from an execution-started crash without starting another execution" do
    assignment = assignment()

    recovered = %{
      assignment_digest: assignment.sha256,
      outcome: :completed,
      summary: "Recovered fixture",
      evidence_ref: "recovered-evidence",
      accepted_head: "89abcdef0123456789abcdef0123456789abcdef"
    }

    {adapter, journal, opts} = ports(faults: %{execute: 1}, execution_reconciliation: recovered)

    assert {:held, :execution_outcome_unknown, _} = ManagedExecutor.run(assignment, opts)
    assert {:ok, %{phase: :terminal, execution_result: ^recovered}} = ManagedExecutor.run(assignment, opts)
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute)) == 1
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :reconcile_execution)) == 1
    assert is_pid(journal)
  end

  test "reconciles a lost result acknowledgement with the same result key" do
    {adapter, _journal, opts} = ports(faults: %{result: 1})
    assignment = assignment()

    assert {:held, :result_reconciliation_failed, %{phase: :result_pending}} = ManagedExecutor.run(assignment, opts)
    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, opts)
    events = FakeAdapter.events(adapter)
    result_keys = for {:publish_or_reconcile_result, _allocation, _digest, _result, key} <- events, do: key
    assert result_keys == ["#{assignment.sha256}:result", "#{assignment.sha256}:result"]
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
  end

  defp ports(adapter_opts \\ []) do
    {:ok, adapter} = FakeAdapter.start_link(adapter_opts)
    {:ok, journal} = FakeJournal.start_link()
    {adapter, journal, [adapter: FakeAdapter, adapter_context: adapter, journal: FakeJournal, journal_context: journal]}
  end

  defp assignment(overrides \\ []) do
    attrs = %{
      objective: %{id: "objective-hgs729", identity: "objective-hgs729", content: "Execute the signed assignment once."},
      repository_ref: "hypergridau/symphony",
      base_ref: "refs/remotes/origin/main",
      branch: "codex/hgs729-fixture",
      seat: "runner-fixture",
      lease: %{issue_id: "issue-hgs729", repository: "hypergridau/symphony", generation: 3, session_id: "worker:hgs729:3", process_id: "worker:hgs729:3"},
      intent_ancestry: ["objective-root", "delegation-fixture"],
      acceptance: %{deliverable: "One managed result", evidence: "Signed terminal cleanup"},
      context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
      platform: "linux-x86_64",
      environment_classification: "repository",
      environment_constraints: ["no-production-workload", "synthetic-test-only"]
    }

    attrs = Enum.reduce(overrides, attrs, fn {key, value}, acc -> Map.put(acc, key, value) end)
    {:ok, bundle} = ManagedAssignmentBundle.build(attrs)
    bundle
  end
end
