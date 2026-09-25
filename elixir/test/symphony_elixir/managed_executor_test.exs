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
             :acquire_credential_lease,
             :renew_credential_lease,
             :execute,
             :publish_or_reconcile_result,
             :revoke_credential_lease,
             :ensure_terminal_cleanup,
             :verify_terminal_cleanup
           ]

    {:prepare_checkout, _allocation_id, intent, checkout_key} = Enum.at(events, 1)
    assert intent == %{repository_ref: "hypergridau/symphony", base_ref: "refs/remotes/origin/main", branch: "codex/hgs729-fixture"}
    assert checkout_key == "#{assignment.sha256}:checkout"
    assert assignment.context_secret_refs == ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"]
    refute Map.has_key?(assignment, :runner_token)
    assert assignment.environment.placement == :internal_beta
    assert assignment.environment.target_environment == :rke2
    refute Enum.any?(events, &(inspect(&1) =~ "runner_token"))
    refute Enum.any?(events, &(inspect(&1) =~ "synthetic-secret-value"))

    assert [{:acquire_credential_lease, "allocation-fixture-1", digest, acquire_key}] =
             Enum.filter(events, &(elem(&1, 0) == :acquire_credential_lease))

    assert digest == assignment.sha256
    assert acquire_key == "#{assignment.sha256}:credential-acquire"

    assert [{:renew_credential_lease, "allocation-fixture-1", ^digest, "credential-lease-fixture-1", renew_key}] =
             Enum.filter(events, &(elem(&1, 0) == :renew_credential_lease))

    assert renew_key == "#{assignment.sha256}:credential-renew"

    assert [{:revoke_credential_lease, "allocation-fixture-1", ^digest, "credential-lease-fixture-1", revoke_key}] =
             Enum.filter(events, &(elem(&1, 0) == :revoke_credential_lease))

    assert revoke_key == "#{assignment.sha256}:credential-revoke"

    prior_count = length(events)
    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, opts)
    replay_events = FakeAdapter.events(adapter)
    assert length(replay_events) == prior_count + 1
    assert Enum.count(replay_events, &(elem(&1, 0) != :verify_terminal_cleanup)) == prior_count - 1
    assert is_pid(journal)
  end

  test "credential lease denial blocks before execution" do
    {adapter, _journal, opts} = ports(credential_denied: true)
    assignment = assignment()

    assert {:blocked, :credential_lease_denied, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :acquire_credential_lease)) == 1
    refute Enum.any?(events, &(elem(&1, 0) in [:renew_credential_lease, :execute, :revoke_credential_lease]))
    assert {:blocked, :credential_lease_denied, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
  end

  test "a lease with a different assignment binding never reaches execution" do
    {adapter, journal, opts} = ports(credential_wrong_binding: true, faults: %{credential_revoke: 1})
    assignment = assignment()

    assert {:held, :credential_lease_candidate_revocation_failed, pending} = ManagedExecutor.run(assignment, opts)
    assert pending.phase == :credential_candidate_revocation_pending
    assert pending.candidate_lease_ref == "credential-lease-fixture-1"
    assert FakeAdapter.active_credential_refs(adapter) == ["credential-lease-fixture-1"]
    refute inspect(pending) =~ "synthetic-secret-value"

    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    assert {:ok, ^pending} = FakeJournal.load(key, journal)
    assert {:blocked, :credential_lease_invalid, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute))
    assert FakeAdapter.active_credential_refs(adapter) == []

    assert {:blocked, :credential_lease_invalid, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
    keys = for {:acquire_credential_lease, _allocation, _digest, key} <- FakeAdapter.events(adapter), do: key
    assert keys == ["#{assignment.sha256}:credential-acquire"]
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute))
  end

  test "a changed renewal reference revokes the known lease and blocks execution" do
    {adapter, _journal, opts} = ports(credential_renew_wrong_ref: true)
    assignment = assignment()

    assert {:blocked, :credential_lease_invalid, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
    events = FakeAdapter.events(adapter)
    refute Enum.any?(events, &(elem(&1, 0) == :execute))
    assert Enum.count(events, &(elem(&1, 0) == :revoke_credential_lease)) == 2
    assert FakeAdapter.active_credential_refs(adapter) == []
    assert {:blocked, :credential_lease_invalid, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
  end

  test "failed candidate revocation is durable, replayed, and does not expose credential material" do
    {adapter, journal, opts} = ports(credential_renew_wrong_ref: true, faults: %{credential_revoke: 1})
    assignment = assignment()

    assert {:held, :credential_lease_candidate_revocation_failed, pending} = ManagedExecutor.run(assignment, opts)
    assert pending.phase == :credential_candidate_revocation_pending
    assert pending.candidate_lease_ref == "credential-lease-fixture-2"
    refute inspect(pending) =~ "synthetic-secret-value"

    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    assert {:ok, ^pending} = FakeJournal.load(key, journal)
    assert {:blocked, :credential_lease_invalid, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
    assert FakeAdapter.active_credential_refs(adapter) == []

    events = FakeAdapter.events(adapter)

    candidate_revocations =
      for {:revoke_credential_lease, _allocation, _digest, ref, revoke_key} <- events,
          String.contains?(revoke_key, "credential-candidate-revoke"),
          do: {ref, revoke_key}

    assert Enum.map(candidate_revocations, &elem(&1, 0)) == ["credential-lease-fixture-2", "credential-lease-fixture-2"]
    assert Enum.uniq(Enum.map(candidate_revocations, &elem(&1, 1))) |> length() == 1
    refute Enum.any?(events, &(elem(&1, 0) == :execute))
  end

  test "an unsafe returned reference stays quarantined by request key until adapter cleanup" do
    {adapter, journal, opts} =
      ports(credential_wrong_binding: true, credential_unsafe_ref: true, faults: %{credential_revoke: 1})

    assignment = assignment()

    assert {:held, :credential_lease_candidate_revocation_failed, pending} = ManagedExecutor.run(assignment, opts)
    assert pending.phase == :credential_request_revocation_pending
    assert pending.candidate_lease_operation == :acquire
    assert pending.candidate_lease_ref == nil
    assert FakeAdapter.active_credential_refs(adapter) == ["unsafe.secret.material"]
    refute inspect(pending) =~ "unsafe.secret.material"

    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    assert {:ok, ^pending} = FakeJournal.load(key, journal)
    assert {:blocked, :credential_lease_invalid, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
    assert FakeAdapter.active_credential_refs(adapter) == []

    request_revocations =
      for {:revoke_credential_lease_request, _allocation, _digest, request_key, revoke_key} <- FakeAdapter.events(adapter),
          do: {request_key, revoke_key}

    assert Enum.map(request_revocations, &elem(&1, 0)) ==
             ["#{assignment.sha256}:credential-acquire", "#{assignment.sha256}:credential-acquire"]

    assert Enum.uniq(Enum.map(request_revocations, &elem(&1, 1))) |> length() == 1
    refute Enum.any?(FakeAdapter.events(adapter), &(inspect(&1) =~ "unsafe.secret.material"))
  end

  test "lease expiry during the execution-started journal write prevents execution" do
    {adapter, _journal, opts} =
      ports(credential_short_renewal: true, execution_started_delay_ms: 150)

    assignment = assignment()

    assert {:blocked, :credential_lease_expired, %{phase: :abort_cleanup_pending}} =
             ManagedExecutor.run(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.any?(events, &(elem(&1, 0) == :revoke_credential_lease))
    refute Enum.any?(events, &(elem(&1, 0) == :execute))
    assert FakeAdapter.active_credential_refs(adapter) == []
    assert {:blocked, :credential_lease_expired, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
  end

  test "renewal denial revokes the lease and blocks before execution" do
    {adapter, _journal, opts} = ports(credential_renew_denied: true)
    assignment = assignment()

    assert {:blocked, :credential_lease_denied, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :revoke_credential_lease)) == 2
    refute Enum.any?(events, &(elem(&1, 0) == :execute))
  end

  test "renewal and revocation failures retain safe journal debt for retry" do
    assignment = assignment()
    {renew_adapter, _journal, renew_opts} = ports(faults: %{credential_renew: 1})

    assert {:held, :credential_lease_renewal_failed, %{phase: :credential_lease_ready}} =
             ManagedExecutor.run(assignment, renew_opts)

    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, renew_opts)
    renew_events = FakeAdapter.events(renew_adapter)
    renew_keys = for {:renew_credential_lease, _allocation, _digest, _ref, key} <- renew_events, do: key
    assert renew_keys == ["#{assignment.sha256}:credential-renew", "#{assignment.sha256}:credential-renew"]
    assert Enum.count(renew_events, &(elem(&1, 0) == :execute)) == 1

    {revoke_adapter, _journal, revoke_opts} = ports(faults: %{credential_revoke: 1})

    assert {:held, :credential_lease_revocation_failed, %{phase: :cleanup_pending}} =
             ManagedExecutor.run(assignment, revoke_opts)

    assert FakeAdapter.active_credential_refs(revoke_adapter) == ["credential-lease-fixture-1"]

    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, revoke_opts)
    revoke_events = FakeAdapter.events(revoke_adapter)
    revoke_keys = for {:revoke_credential_lease, _allocation, _digest, _ref, key} <- revoke_events, do: key
    assert revoke_keys == ["#{assignment.sha256}:credential-revoke", "#{assignment.sha256}:credential-revoke"]
    assert Enum.count(revoke_events, &(elem(&1, 0) == :execute)) == 1
    assert Enum.count(revoke_events, &(elem(&1, 0) == :ensure_terminal_cleanup)) == 1
    assert FakeAdapter.active_credential_refs(revoke_adapter) == []
  end

  test "expired leases are revoked and cannot reach execution" do
    {adapter, _journal, opts} = ports(credential_expired: true)
    assignment = assignment()

    assert {:blocked, :credential_lease_expired, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :revoke_credential_lease)) == 1
    refute Enum.any?(events, &(elem(&1, 0) in [:renew_credential_lease, :execute]))
    assert {:blocked, :credential_lease_expired, %{phase: :abort_cleanup_pending}} = ManagedExecutor.run(assignment, opts)
  end

  test "uncertain lease acquisition retries with the same key and terminal replay does not reacquire" do
    {adapter, _journal, opts} = ports(faults: %{credential_acquire: 1})
    assignment = assignment()

    assert {:held, :credential_lease_acquisition_failed, %{phase: :credential_lease_pending}} =
             ManagedExecutor.run(assignment, opts)

    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, opts)
    prior = FakeAdapter.events(adapter)
    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, opts)
    events = FakeAdapter.events(adapter)
    assert length(events) == length(prior) + 1
    keys = for {:acquire_credential_lease, _allocation_id, _digest, key} <- events, do: key
    assert keys == ["#{assignment.sha256}:credential-acquire", "#{assignment.sha256}:credential-acquire"]
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
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

  test "fails closed on a persisted lifecycle record with missing phase fields" do
    {adapter, journal, opts} = ports()
    assignment = assignment()
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

    malformed = %{
      schema_version: 1,
      key: key,
      assignment_digest: assignment.sha256,
      phase: :terminal,
      version: 0
    }

    assert :ok = FakeJournal.compare_and_swap(key, 0, malformed, journal)
    assert {:error, :invalid_lifecycle_journal} = ManagedExecutor.run(assignment, opts)
    assert FakeAdapter.events(adapter) == []
  end

  test "a schema v1 lifecycle record is rejected rather than replayed without a credential lease" do
    {adapter, journal, opts} = ports()
    assignment = assignment()
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    legacy = %{schema_version: 1, key: key, assignment_digest: assignment.sha256, phase: :planned, version: 0}

    assert :ok = FakeJournal.compare_and_swap(key, 0, legacy, journal)
    assert {:error, :invalid_lifecycle_journal} = ManagedExecutor.run(assignment, opts)
    assert FakeAdapter.events(adapter) == []
  end

  test "a schema v2 lifecycle record is rejected when candidate lease debt is not represented" do
    {adapter, journal, opts} = ports()
    assignment = assignment()
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

    legacy = %{
      schema_version: 2,
      key: key,
      assignment_digest: assignment.sha256,
      phase: :planned,
      version: 0,
      credential_lease: nil
    }

    assert :ok = FakeJournal.compare_and_swap(key, 0, legacy, journal)
    assert {:error, :invalid_lifecycle_journal} = ManagedExecutor.run(assignment, opts)
    assert FakeAdapter.events(adapter) == []
  end

  test "does not advance when allocation reconciliation returns an invalid response" do
    {adapter, _journal, opts} = ports(invalid_allocation_response: true)
    assignment = assignment()

    assert {:held, :invalid_allocation, %{phase: :allocation_pending}} = ManagedExecutor.run(assignment, opts)
    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :allocate_or_reconcile)) == 2
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
  end

  test "reconciles an empty result acknowledgement without executing again" do
    {adapter, _journal, opts} = ports(invalid_result_ack: true)
    assignment = assignment()

    assert {:held, :invalid_result_acknowledgement, %{phase: :result_pending}} =
             ManagedExecutor.run(assignment, opts)

    assert {:ok, %{phase: :terminal}} = ManagedExecutor.run(assignment, opts)
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :publish_or_reconcile_result)) == 2
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

  test "keeps an invalid execution result in reconciliation without executing again" do
    assignment = assignment()

    invalid_result = %{
      assignment_digest: assignment.sha256,
      outcome: :completed,
      summary: " ",
      evidence_ref: "invalid-evidence",
      accepted_head: "89abcdef0123456789abcdef0123456789abcdef"
    }

    {adapter, _journal, opts} = ports(result: invalid_result)

    assert {:held, :execution_outcome_unknown, %{phase: :execution_started}} =
             ManagedExecutor.run(assignment, opts)

    assert {:held, :execution_outcome_unknown, %{phase: :execution_started}} =
             ManagedExecutor.run(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :reconcile_execution)) == 1
  end

  test "cleans up a checkout intent mismatch without executing" do
    {adapter, _journal, opts} = ports(checkout_mismatch: true)
    assignment = assignment()

    assert {:blocked, :checkout_intent_mismatch, record} = ManagedExecutor.run(assignment, opts)
    assert record.phase == :abort_cleanup_pending
    assert %{pre_execution_result: result, abort_result_ref: result_ref} = record

    assert result.outcome == :blocked
    assert result.abort_reason == :checkout_intent_mismatch
    refute Map.has_key?(result, :accepted_head)
    assert result_ref == "abort-result-fixture-1"
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute))
    assert Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :ensure_abort_cleanup))
    assert Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :publish_or_reconcile_abort_result))
  end

  test "records pre-execution cleanup debt and retries uncertain cleanup without re-preparing checkout" do
    {adapter, _journal, opts} = ports(checkout_failure: true, faults: %{abort_cleanup: 1})
    assignment = assignment()

    assert {:held, :abort_cleanup_unverified, pending} = ManagedExecutor.run(assignment, opts)
    assert pending.phase == :abort_cleanup_pending
    assert %{allocation: %{id: allocation_id}, abort_result_ref: "abort-result-fixture-1"} = pending

    assert {:blocked, :checkout_preparation_failed, blocked} = ManagedExecutor.run(assignment, opts)
    assert blocked.phase == :abort_cleanup_pending
    assert %{allocation: %{id: ^allocation_id}, pre_execution_result: result} = blocked

    assert result.abort_reason == :checkout_preparation_failed
    assert result.outcome == :blocked
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :prepare_checkout)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :ensure_abort_cleanup)) == 2
    assert Enum.count(events, &(elem(&1, 0) == :publish_or_reconcile_abort_result)) == 1
    refute Enum.any?(events, &(elem(&1, 0) == :execute))

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_pending}} =
             ManagedExecutor.run(assignment, opts)

    replay_events = FakeAdapter.events(adapter)
    assert Enum.count(replay_events, &(elem(&1, 0) == :ensure_abort_cleanup)) == 3
    assert Enum.count(replay_events, &(elem(&1, 0) == :publish_or_reconcile_abort_result)) == 1
  end

  test "rejects a noncanonical checkout head and cleans up before execution" do
    {adapter, _journal, opts} = ports(checkout_head: String.duplicate("a", 41))
    assignment = assignment()

    assert {:blocked, :checkout_intent_mismatch, %{phase: :abort_cleanup_pending}} =
             ManagedExecutor.run(assignment, opts)

    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute))
  end

  test "reconciles a lost blocked-result acknowledgement before attempting cleanup" do
    {adapter, _journal, opts} = ports(checkout_failure: true, faults: %{abort_result: 1})
    assignment = assignment()

    assert {:held, :abort_result_reconciliation_failed, %{phase: :abort_result_pending}} =
             ManagedExecutor.run(assignment, opts)

    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :ensure_abort_cleanup))

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_pending}} =
             ManagedExecutor.run(assignment, opts)

    events = FakeAdapter.events(adapter)
    result_keys = for {:publish_or_reconcile_abort_result, _allocation_id, _digest, _result, key} <- events, do: key
    assert result_keys == ["#{assignment.sha256}:abort-result", "#{assignment.sha256}:abort-result"]
    assert Enum.count(events, &(elem(&1, 0) == :ensure_abort_cleanup)) == 1
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute))
  end

  test "retries a malformed blocked-result acknowledgement before cleanup" do
    {adapter, _journal, opts} = ports(checkout_failure: true, invalid_abort_result_ack: true)
    assignment = assignment()

    assert {:held, :invalid_abort_result_acknowledgement, %{phase: :abort_result_pending}} =
             ManagedExecutor.run(assignment, opts)

    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :ensure_abort_cleanup))

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_pending}} =
             ManagedExecutor.run(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :publish_or_reconcile_abort_result)) == 2
    assert Enum.count(events, &(elem(&1, 0) == :ensure_abort_cleanup)) == 1
  end

  test "holds a malformed pre-execution cleanup response and retries cleanup only" do
    {adapter, _journal, opts} = ports(checkout_failure: true, invalid_abort_cleanup_response: true)
    assignment = assignment()

    assert {:held, :abort_cleanup_unverified, %{phase: :abort_cleanup_pending}} =
             ManagedExecutor.run(assignment, opts)

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_pending}} =
             ManagedExecutor.run(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :publish_or_reconcile_abort_result)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :ensure_abort_cleanup)) == 2
    refute Enum.any?(events, &(elem(&1, 0) == :execute))
  end

  test "journal compare-and-swap admits only one concurrent writer for a lifecycle version" do
    {:ok, journal} = FakeJournal.start_link()
    initial = %{version: 0, phase: :planned}
    assert :ok = FakeJournal.compare_and_swap("issue:3", 0, initial, journal)

    results =
      [:writer_a, :writer_b]
      |> Task.async_stream(
        fn writer ->
          FakeJournal.compare_and_swap("issue:3", 0, %{version: 1, writer: writer}, journal)
        end,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == :ok)) == 1
    assert Enum.count(results, &(&1 == {:error, :conflict})) == 1
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

  test "re-verifies a terminal record and holds if cleanup signature verification fails" do
    {adapter, journal, opts} = ports()
    assignment = assignment()

    assert {:ok, %{phase: :terminal} = terminal} = ManagedExecutor.run(assignment, opts)
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

    assert {:ok, ^terminal} = FakeJournal.load(key, journal)

    tampered = %{
      terminal
      | version: terminal.version + 1,
        cleanup_evidence: %{terminal.cleanup_evidence | signature: "invalid-signature"}
    }

    assert :ok = FakeJournal.compare_and_swap(key, terminal.version, tampered, journal)

    assert {:held, :terminal_cleanup_reverification_failed, %{phase: :terminal}} =
             ManagedExecutor.run(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :allocate_or_reconcile)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :verify_terminal_cleanup)) == 2
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
    {:ok, journal} = FakeJournal.start_link(adapter_opts)
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
      environment_constraints: ["no-production-workload", "synthetic-test-only"],
      placement: :internal_beta,
      target_environment: :rke2
    }

    attrs = Enum.reduce(overrides, attrs, fn {key, value}, acc -> Map.put(acc, key, value) end)
    {:ok, bundle} = ManagedAssignmentBundle.build(attrs)
    bundle
  end
end
