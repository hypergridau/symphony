Code.require_file("../support/managed_executor_fake_adapter.exs", __DIR__)

defmodule SymphonyElixir.ManagedExecutorTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.ManagedExecutor
  alias SymphonyElixir.ManagedExecutor.{FakeAdapter, FakeJournal}

  test "runs the exact assignment lifecycle and replays terminal evidence with verification only" do
    {adapter, journal, opts} = ports()
    assignment = assignment()

    assert {:ok, %{phase: :terminal, cleanup_evidence: evidence}} = run_claimed(assignment, opts)
    assert evidence.assignment_digest == assignment.sha256
    assert evidence.accepted_head == "89abcdef0123456789abcdef0123456789abcdef"
    assert evidence.generation == assignment.lease.generation
    assert evidence.credentials_revoked
    assert evidence.workspace_removed

    events = FakeAdapter.events(adapter)

    assert Enum.map(events, &elem(&1, 0)) == [
             :allocate_or_reconcile,
             :acquire_credential_lease,
             :renew_credential_lease,
             :prepare_checkout,
             :execute,
             :publish_or_reconcile_result,
             :revoke_credential_lease,
             :ensure_terminal_cleanup,
             :verify_terminal_cleanup
           ]

    {:prepare_checkout, _allocation_id, intent, lease_ref, checkout_key} = Enum.at(events, 3)
    assert intent == %{repository_ref: "hypergridau/symphony", base_ref: "refs/remotes/origin/main", branch: "codex/hgs729-fixture"}
    assert lease_ref == "#{assignment.sha256}:credential-acquire"
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

    handle = "#{assignment.sha256}:credential-acquire"

    assert [{:renew_credential_lease, "allocation-fixture-1", ^digest, ^handle, renew_key}] =
             Enum.filter(events, &(elem(&1, 0) == :renew_credential_lease))

    assert renew_key == "#{assignment.sha256}:credential-renew"

    assert [{:revoke_credential_lease, "allocation-fixture-1", ^digest, ^handle, revoke_key}] =
             Enum.filter(events, &(elem(&1, 0) == :revoke_credential_lease))

    assert revoke_key == "#{assignment.sha256}:credential-revoke"

    prior_count = length(events)
    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)
    replay_events = FakeAdapter.events(adapter)
    assert length(replay_events) == prior_count + 1
    assert Enum.count(replay_events, &(elem(&1, 0) != :verify_terminal_cleanup)) == prior_count - 1
    assert is_pid(journal)
  end

  test "credential lease denial retains abort cleanup debt before execution" do
    {adapter, journal, opts} = ports(credential_denied: true)
    assignment = assignment()

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending} = blocked} =
             run_claimed(assignment, opts)

    assert blocked.checkout == nil

    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    assert {:ok, ^blocked} = FakeJournal.load(key, journal)
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :acquire_credential_lease)) == 1
    refute Enum.any?(events, &(elem(&1, 0) == :prepare_checkout))
    refute Enum.any?(events, &(elem(&1, 0) in [:renew_credential_lease, :execute, :revoke_credential_lease]))

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)

    changed = %{blocked | version: blocked.version + 1, checkout: %{head: "wrong-head"}}
    assert :ok = FakeJournal.compare_and_swap(key, blocked.version, changed, journal)
    assert {:error, :invalid_lifecycle_journal} = run_claimed(assignment, opts)
  end

  test "a lease with a different assignment binding never reaches execution" do
    {adapter, journal, opts} = ports(credential_wrong_binding: true, faults: %{credential_revoke: 1})
    assignment = assignment()

    assert {:held, :credential_lease_candidate_revocation_failed, pending} = run_claimed(assignment, opts)
    assert pending.phase == :credential_request_revocation_pending
    assert pending.candidate_lease_operation == :acquire
    assert FakeAdapter.active_credential_refs(adapter) == ["#{assignment.sha256}:credential-acquire"]
    refute inspect(pending) =~ "synthetic-secret-value"

    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    assert {:ok, ^pending} = FakeJournal.load(key, journal)

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending} = blocked} =
             run_claimed(assignment, opts)

    assert blocked.checkout == nil
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute))
    assert FakeAdapter.active_credential_refs(adapter) == []

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)

    keys = for {:acquire_credential_lease, _allocation, _digest, key} <- FakeAdapter.events(adapter), do: key
    assert keys == ["#{assignment.sha256}:credential-acquire"]
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute))
  end

  test "an unexpected renewal response is revoked by its stable request key" do
    {adapter, _journal, opts} = ports(credential_renew_wrong_ref: true)
    assignment = assignment()

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)

    events = FakeAdapter.events(adapter)
    refute Enum.any?(events, &(elem(&1, 0) == :execute))
    assert Enum.count(events, &(elem(&1, 0) == :revoke_credential_lease_request)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :revoke_credential_lease)) == 1
    assert FakeAdapter.active_credential_refs(adapter) == []

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)
  end

  test "failed candidate revocation is durable, replayed, and does not expose credential material" do
    {adapter, journal, opts} = ports(credential_renew_wrong_ref: true, faults: %{credential_revoke: 1})
    assignment = assignment()

    assert {:held, :credential_lease_candidate_revocation_failed, pending} = run_claimed(assignment, opts)
    assert pending.phase == :credential_request_revocation_pending
    assert pending.candidate_lease_operation == :renew
    refute inspect(pending) =~ "YWx0ZXJuYXRlLWJlYXJlci10b2tlbg"
    refute inspect(pending) =~ "synthetic-secret-value"

    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    assert {:ok, ^pending} = FakeJournal.load(key, journal)

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)

    assert FakeAdapter.active_credential_refs(adapter) == []

    events = FakeAdapter.events(adapter)

    candidate_revocations =
      for {:revoke_credential_lease_request, _allocation, _digest, request_key, revoke_key} <- events,
          do: {request_key, revoke_key}

    assert Enum.map(candidate_revocations, &elem(&1, 0)) ==
             ["#{assignment.sha256}:credential-renew", "#{assignment.sha256}:credential-renew"]

    assert Enum.uniq(Enum.map(candidate_revocations, &elem(&1, 1))) |> length() == 1
    refute Enum.any?(events, &(elem(&1, 0) == :execute))
  end

  test "an unsafe returned reference stays quarantined by request key until adapter cleanup" do
    {adapter, journal, opts} =
      ports(credential_wrong_binding: true, credential_unsafe_ref: true, faults: %{credential_revoke: 1})

    assignment = assignment()

    assert {:held, :credential_lease_candidate_revocation_failed, pending} = run_claimed(assignment, opts)
    assert pending.phase == :credential_request_revocation_pending
    assert pending.candidate_lease_operation == :acquire
    assert FakeAdapter.active_credential_refs(adapter) == ["#{assignment.sha256}:credential-acquire"]
    refute inspect(pending) =~ "YWJjZGVmZ2hpamtsbW5vcA"

    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    assert {:ok, ^pending} = FakeJournal.load(key, journal)

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)

    assert FakeAdapter.active_credential_refs(adapter) == []

    request_revocations =
      for {:revoke_credential_lease_request, _allocation, _digest, request_key, revoke_key} <- FakeAdapter.events(adapter),
          do: {request_key, revoke_key}

    assert Enum.map(request_revocations, &elem(&1, 0)) ==
             ["#{assignment.sha256}:credential-acquire", "#{assignment.sha256}:credential-acquire"]

    assert Enum.uniq(Enum.map(request_revocations, &elem(&1, 1))) |> length() == 1
    refute Enum.any?(FakeAdapter.events(adapter), &(inspect(&1) =~ "YWJjZGVmZ2hpamtsbW5vcA"))
  end

  test "lease expiry during the execution-started journal write prevents execution" do
    {adapter, _journal, opts} =
      ports(credential_short_renewal: true, execution_started_delay_ms: 150)

    assignment = assignment()

    assert {:held, :post_checkout_cleanup_required, %{phase: :execution_started} = blocked} =
             run_claimed(assignment, opts)

    assert blocked.checkout.head == "0123456789abcdef0123456789abcdef01234567"

    events = FakeAdapter.events(adapter)
    assert Enum.any?(events, &(elem(&1, 0) == :revoke_credential_lease))
    refute Enum.any?(events, &(elem(&1, 0) == :execute))
    refute Enum.any?(events, &(elem(&1, 0) in [:publish_or_reconcile_abort_result, :ensure_abort_cleanup]))
    assert FakeAdapter.active_credential_refs(adapter) == []
    assert {:held, :execution_outcome_unknown, %{phase: :execution_started}} = run_claimed(assignment, opts)
  end

  test "renewal denial revokes the lease and retains unsupported abort cleanup debt" do
    {adapter, _journal, opts} = ports(credential_renew_denied: true)
    assignment = assignment()

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :revoke_credential_lease)) == 2
    refute Enum.any?(events, &(elem(&1, 0) == :execute))
  end

  test "renewal and revocation failures retain safe journal debt for retry" do
    assignment = assignment()
    {renew_adapter, _journal, renew_opts} = ports(faults: %{credential_renew: 1})

    assert {:held, :credential_lease_renewal_failed, %{phase: :credential_lease_ready}} =
             run_claimed(assignment, renew_opts)

    assert FakeAdapter.credential_renewal_materializations(renew_adapter) == 1
    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, renew_opts)
    assert FakeAdapter.credential_renewal_materializations(renew_adapter) == 1
    renew_events = FakeAdapter.events(renew_adapter)
    renew_keys = for {:renew_credential_lease, _allocation, _digest, _ref, key} <- renew_events, do: key
    assert renew_keys == ["#{assignment.sha256}:credential-renew", "#{assignment.sha256}:credential-renew"]
    assert Enum.count(renew_events, &(elem(&1, 0) == :execute)) == 1

    {revoke_adapter, _journal, revoke_opts} = ports(faults: %{credential_revoke: 1})

    assert {:held, :credential_lease_revocation_failed, %{phase: :cleanup_pending}} =
             run_claimed(assignment, revoke_opts)

    assert FakeAdapter.active_credential_refs(revoke_adapter) == ["#{assignment.sha256}:credential-acquire"]

    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, revoke_opts)
    revoke_events = FakeAdapter.events(revoke_adapter)
    revoke_keys = for {:revoke_credential_lease, _allocation, _digest, _ref, key} <- revoke_events, do: key
    assert revoke_keys == ["#{assignment.sha256}:credential-revoke", "#{assignment.sha256}:credential-revoke"]
    assert Enum.count(revoke_events, &(elem(&1, 0) == :execute)) == 1
    assert Enum.count(revoke_events, &(elem(&1, 0) == :ensure_terminal_cleanup)) == 1
    assert FakeAdapter.active_credential_refs(revoke_adapter) == []
  end

  test "a revoke whose acknowledgement is lost is replayed by key without a duplicate effect" do
    assignment = assignment()
    {adapter, _journal, opts} = ports(faults: %{credential_revoke_lost_ack: 1})

    assert {:held, :credential_lease_revocation_failed, %{phase: :cleanup_pending}} =
             run_claimed(assignment, opts)

    assert FakeAdapter.active_credential_refs(adapter) == []
    assert FakeAdapter.credential_revocation_effects(adapter) == 1
    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)

    events = FakeAdapter.events(adapter)
    revoke_keys = for {:revoke_credential_lease, _allocation, _digest, _handle, key} <- events, do: key
    assert revoke_keys == ["#{assignment.sha256}:credential-revoke", "#{assignment.sha256}:credential-revoke"]
    assert FakeAdapter.credential_revocation_effects(adapter) == 1
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
  end

  test "expired leases are revoked and cannot reach execution" do
    {adapter, _journal, opts} = ports(credential_expired: true)
    assignment = assignment()

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :revoke_credential_lease)) == 1
    refute Enum.any?(events, &(elem(&1, 0) in [:renew_credential_lease, :execute]))

    assert {:held, :abort_cleanup_contract_unsupported, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)
  end

  test "uncertain lease acquisition retries with the same key and terminal replay does not reacquire" do
    {adapter, _journal, opts} = ports(faults: %{credential_acquire: 1})
    assignment = assignment()

    assert {:held, :credential_lease_acquisition_failed, %{phase: :credential_lease_pending}} =
             run_claimed(assignment, opts)

    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)
    prior = FakeAdapter.events(adapter)
    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)
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
             run_claimed(assignment, opts)

    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)
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
    assert {:error, :invalid_lifecycle_journal} = run_claimed(assignment, opts)
    assert FakeAdapter.events(adapter) == []
  end

  test "requires a complete validated claim before any allocation or journal write" do
    {adapter, journal, opts} = ports()
    assignment = assignment()
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

    assert {:error, :provider_claim_invalid} = ManagedExecutor.run(assignment, opts)
    assert {:ok, nil} = FakeJournal.load(key, journal)
    assert FakeAdapter.events(adapter) == []

    claim = provider_claim(assignment)

    for changed <- [
          put_in(claim, [:reservation, :workspace_id], nil),
          put_in(claim, [:reservation, :scope_keys], []),
          update_in(claim, [:reservation], &Map.delete(&1, :generation)),
          put_in(claim, [:attestation, :signature], nil),
          put_in(claim, [:attestation, :unexpected], "untrusted"),
          put_in(claim, [:response, :projection_id], "other-projection"),
          put_in(claim, [:reservation, :generation], assignment.lease.generation + 1),
          put_in(claim, [:reservation, :process_id], "other-process"),
          claim
          |> put_in([:reservation, :responsible_delegation_id], "other-delegation")
          |> put_in([:attestation, :responsible_delegation_id], "other-delegation")
        ] do
      assert {:error, :provider_claim_invalid} =
               ManagedExecutor.run(assignment, Keyword.put(opts, :provider_claim, changed))
    end

    assert {:ok, nil} = FakeJournal.load(key, journal)
    assert FakeAdapter.events(adapter) == []
  end

  test "journals the exact claim binding before allocator side effects and rejects changed replay" do
    {adapter, journal, opts} = ports(invalid_allocation_response: true)
    assignment = assignment()
    claim = provider_claim(assignment)
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

    assert {:held, :invalid_allocation, %{phase: :allocation_pending}} = run_claimed(assignment, opts)
    assert {:ok, record} = FakeJournal.load(key, journal)
    assert record.schema_version == 8
    assert record.claim_binding.reservation_id == claim.reservation.reservation_id
    assert record.claim_binding.projection_id == claim.reservation.projection_id
    assert record.claim_binding.generation == assignment.lease.generation
    refute Map.has_key?(record.claim_binding, :reservation_nonce)
    refute Map.has_key?(record.claim_binding, :signature)
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :allocate_or_reconcile)) == 1

    changed =
      claim
      |> put_in([:reservation, :reservation_id], "reservation-other")
      |> put_in([:attestation, :reservation_id], "reservation-other")

    assert {:error, :provider_claim_replay_mismatch} =
             ManagedExecutor.run(assignment, Keyword.put(opts, :provider_claim, changed))

    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :allocate_or_reconcile)) == 1

    renewed_attestation =
      claim
      |> put_in([:attestation, :attested_at], "2026-09-26T00:01:00.000Z")
      |> put_in([:attestation, :signature], "new-valid-synthetic-signature")

    assert {:ok, %{phase: :terminal}} =
             ManagedExecutor.run(assignment, Keyword.put(opts, :provider_claim, renewed_attestation))
  end

  test "holds an unbound v5 planned journal before allocation" do
    {adapter, journal, opts} = ports()
    assignment = assignment()
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

    legacy = %{
      schema_version: 5,
      key: key,
      assignment_digest: assignment.sha256,
      phase: :planned,
      version: 0,
      credential_lease: nil
    }

    assert :ok = FakeJournal.compare_and_swap(key, 0, legacy, journal)
    assert {:error, :legacy_claim_requires_reconciliation} = run_claimed(assignment, opts)
    assert FakeAdapter.events(adapter) == []
  end

  for schema_version <- [6, 7] do
    test "holds a v#{schema_version} in-flight journal before v8 execution" do
      {adapter, journal, opts} = ports(invalid_allocation_response: true)
      assignment = assignment()
      key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

      assert {:held, :invalid_allocation, %{phase: :allocation_pending}} = run_claimed(assignment, opts)
      assert {:ok, record} = FakeJournal.load(key, journal)
      legacy = %{record | schema_version: unquote(schema_version), version: record.version + 1}
      assert :ok = FakeJournal.compare_and_swap(key, record.version, legacy, journal)

      event_count = length(FakeAdapter.events(adapter))
      assert {:error, :legacy_claim_requires_reconciliation} = run_claimed(assignment, opts)
      assert length(FakeAdapter.events(adapter)) == event_count
    end
  end

  test "reverifies valid v5 terminal evidence without rewriting its unbound journal" do
    {adapter, journal, opts} = ports()
    assignment = assignment()
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)
    assert {:ok, record} = FakeJournal.load(key, journal)
    legacy = record |> Map.delete(:claim_binding) |> Map.put(:schema_version, 5)
    assert :ok = FakeJournal.compare_and_swap(key, record.version, legacy, journal)

    prior_count = length(FakeAdapter.events(adapter))
    assert {:ok, %{schema_version: 5, phase: :terminal}} = run_claimed(assignment, opts)
    assert length(FakeAdapter.events(adapter)) == prior_count + 1
    assert {:ok, ^legacy} = FakeJournal.load(key, journal)
  end

  for schema_version <- [6, 7] do
    test "reverifies v#{schema_version} terminal evidence with its original provider claim binding" do
      {adapter, journal, opts} = ports()
      assignment = assignment()
      key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

      assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)
      assert {:ok, record} = FakeJournal.load(key, journal)
      legacy = %{record | schema_version: unquote(schema_version), version: record.version + 1}
      assert :ok = FakeJournal.compare_and_swap(key, record.version, legacy, journal)

      prior_count = length(FakeAdapter.events(adapter))
      assert {:ok, %{schema_version: unquote(schema_version), phase: :terminal}} = run_claimed(assignment, opts)
      assert length(FakeAdapter.events(adapter)) == prior_count + 1

      changed =
        provider_claim(assignment)
        |> put_in([:reservation, :reservation_id], "reservation-other")
        |> put_in([:attestation, :reservation_id], "reservation-other")

      assert {:error, :provider_claim_replay_mismatch} =
               ManagedExecutor.run(assignment, Keyword.put(opts, :provider_claim, changed))
    end
  end

  test "a schema v1 lifecycle record is rejected rather than replayed without a credential lease" do
    {adapter, journal, opts} = ports()
    assignment = assignment()
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    legacy = %{schema_version: 1, key: key, assignment_digest: assignment.sha256, phase: :planned, version: 0}

    assert :ok = FakeJournal.compare_and_swap(key, 0, legacy, journal)
    assert {:error, :invalid_lifecycle_journal} = run_claimed(assignment, opts)
    assert FakeAdapter.events(adapter) == []
  end

  test "a schema v2 lifecycle record is rejected when candidate lease debt is not represented" do
    {adapter, journal, opts} = ports()
    assignment = assignment()
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

    for schema_version <- [2, 3] do
      legacy = %{
        schema_version: schema_version,
        key: key,
        assignment_digest: assignment.sha256,
        phase: :planned,
        version: 0,
        credential_lease: nil
      }

      assert :ok = FakeJournal.compare_and_swap(key, 0, legacy, journal)
      assert {:error, :invalid_lifecycle_journal} = run_claimed(assignment, opts)
      assert FakeAdapter.events(adapter) == []
    end
  end

  test "does not advance when allocation reconciliation returns an invalid response" do
    {adapter, _journal, opts} = ports(invalid_allocation_response: true)
    assignment = assignment()

    assert {:held, :invalid_allocation, %{phase: :allocation_pending}} = run_claimed(assignment, opts)
    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :allocate_or_reconcile)) == 2
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
  end

  test "reconciles an empty result acknowledgement without executing again" do
    {adapter, _journal, opts} = ports(invalid_result_ack: true)
    assignment = assignment()

    assert {:held, :invalid_result_acknowledgement, %{phase: :result_pending}} =
             run_claimed(assignment, opts)

    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :publish_or_reconcile_result)) == 2
  end

  test "holds an unknown execution outcome after crash and never executes it twice" do
    {adapter, _journal, opts} = ports(faults: %{execute: 1})
    assignment = assignment()

    assert {:held, :execution_outcome_unknown, %{phase: :execution_started}} =
             run_claimed(assignment, opts)

    assert {:held, :execution_outcome_unknown, %{phase: :execution_started}} = run_claimed(assignment, opts)
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
             run_claimed(assignment, opts)

    assert {:held, :execution_outcome_unknown, %{phase: :execution_started}} =
             run_claimed(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :execute)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :reconcile_execution)) == 1
  end

  test "holds a mismatched checkout receipt without asserting no checkout" do
    {adapter, _journal, opts} = ports(checkout_mismatch: true)
    assignment = assignment()

    assert {:held, :checkout_outcome_unknown, %{phase: :checkout_outcome_unknown}} = run_claimed(assignment, opts)
    events = FakeAdapter.events(adapter)
    refute Enum.any?(events, &(elem(&1, 0) in [:execute, :ensure_abort_cleanup, :publish_or_reconcile_abort_result]))
    assert FakeAdapter.active_credential_refs(adapter) == []
  end

  test "an uncertain checkout result retains the claim for reconciliation" do
    {adapter, _journal, opts} = ports(checkout_uncertain: true)
    assignment = assignment()

    assert {:held, :checkout_outcome_unknown, %{phase: :checkout_outcome_unknown}} = run_claimed(assignment, opts)
    events = FakeAdapter.events(adapter)
    refute Enum.any?(events, &(elem(&1, 0) in [:execute, :ensure_abort_cleanup, :publish_or_reconcile_abort_result]))
    assert FakeAdapter.active_credential_refs(adapter) == []
    assert {:held, :checkout_outcome_unknown, %{phase: :checkout_outcome_unknown}} = run_claimed(assignment, opts)
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :prepare_checkout)) == 1
  end

  test "retries failed revocation of an uncertain checkout without preparing it again" do
    {adapter, _journal, opts} = ports(checkout_uncertain: true, faults: %{credential_revoke: 1})
    assignment = assignment()

    assert {:held, :credential_lease_revocation_failed, %{phase: :checkout_outcome_unknown}} =
             run_claimed(assignment, opts)

    assert FakeAdapter.active_credential_refs(adapter) == ["#{assignment.sha256}:credential-acquire"]

    assert {:held, :checkout_outcome_unknown, %{phase: :checkout_outcome_unknown}} =
             run_claimed(assignment, opts)

    assert FakeAdapter.active_credential_refs(adapter) == []
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :prepare_checkout)) == 1
    refute Enum.any?(events, &(elem(&1, 0) in [:execute, :publish_or_reconcile_abort_result]))
  end

  test "an expired lease after an uncertain checkout never claims no checkout" do
    {adapter, journal, opts} = ports(checkout_uncertain: true)
    assignment = assignment()

    assert {:held, :checkout_outcome_unknown, %{phase: :checkout_outcome_unknown} = pending} =
             run_claimed(assignment, opts)

    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    expired_lease = %{pending.credential_lease | expires_at_ms: System.system_time(:millisecond) - 1}
    expired = %{pending | version: pending.version + 1, credential_lease: expired_lease}
    assert :ok = FakeJournal.compare_and_swap(key, pending.version, expired, journal)

    assert {:held, :checkout_outcome_unknown, %{phase: :checkout_outcome_unknown}} = run_claimed(assignment, opts)
    events = FakeAdapter.events(adapter)
    assert Enum.any?(events, &(elem(&1, 0) == :revoke_credential_lease))
    refute Enum.any?(events, &(elem(&1, 0) in [:ensure_abort_cleanup, :publish_or_reconcile_abort_result]))
  end

  test "records pre-execution cleanup debt and retries uncertain cleanup without re-preparing checkout" do
    {adapter, _journal, opts} = ports(checkout_failure: true, faults: %{abort_cleanup: 1})
    assignment = assignment()

    assert {:held, :abort_cleanup_unverified, pending} = run_claimed(assignment, opts)
    assert pending.phase == :abort_cleanup_pending
    assert %{allocation: %{id: allocation_id}, abort_result_ref: "abort-result-fixture-1"} = pending

    assert {:blocked, :checkout_preparation_failed, blocked} = run_claimed(assignment, opts)
    assert blocked.phase == :abort_cleanup_verified
    assert %{allocation: %{id: ^allocation_id}, pre_execution_result: result} = blocked

    assert result.abort_reason == :checkout_preparation_failed
    assert result.outcome == :blocked
    assert blocked.abort_release_ack.abort_result_ref == blocked.abort_result_ref
    assert blocked.abort_release_ack.projection_id == blocked.claim_binding.projection_id
    assert blocked.abort_release_ack.reservation_id == blocked.claim_binding.reservation_id
    assert blocked.abort_release_ack.reservation_state == "released"
    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :prepare_checkout)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :ensure_abort_cleanup)) == 2
    assert Enum.count(events, &(elem(&1, 0) == :publish_or_reconcile_abort_result)) == 1
    refute Enum.any?(events, &(elem(&1, 0) == :execute))

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_verified}} =
             run_claimed(assignment, opts)

    replay_events = FakeAdapter.events(adapter)
    assert Enum.count(replay_events, &(elem(&1, 0) == :ensure_abort_cleanup)) == 2
    assert Enum.count(replay_events, &(elem(&1, 0) == :publish_or_reconcile_abort_result)) == 1
    assert FakeAdapter.abort_release_effects(adapter) == 1
  end

  test "rejects a release acknowledgement for another blocked result and reconciles by key" do
    {adapter, _journal, opts} = ports(checkout_failure: true, abort_cleanup_wrong_ref: true)
    assignment = assignment()

    assert {:held, :abort_cleanup_unverified, %{phase: :abort_cleanup_pending}} = run_claimed(assignment, opts)
    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_verified}} = run_claimed(assignment, opts)

    assert FakeAdapter.abort_release_effects(adapter) == 1
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :ensure_abort_cleanup)) == 2
  end

  test "reconciles provider release after the verified journal checkpoint fails" do
    {adapter, _journal, opts} = ports(checkout_failure: true, abort_verified_write_failure: true)
    assignment = assignment()

    assert {:held, :abort_cleanup_unverified, %{phase: :abort_cleanup_pending}} = run_claimed(assignment, opts)
    assert FakeAdapter.abort_release_effects(adapter) == 1

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_verified}} = run_claimed(assignment, opts)
    assert FakeAdapter.abort_release_effects(adapter) == 1
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :ensure_abort_cleanup)) == 2
  end

  test "rejects a changed persisted abort release acknowledgement on replay" do
    {adapter, journal, opts} = ports(checkout_failure: true)
    assignment = assignment()

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_verified} = verified} =
             run_claimed(assignment, opts)

    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
    changed_ack = %{verified.abort_release_ack | reservation_id: "other-reservation"}
    changed = %{verified | version: verified.version + 1, abort_release_ack: changed_ack}
    assert :ok = FakeJournal.compare_and_swap(key, verified.version, changed, journal)

    assert {:error, :invalid_lifecycle_journal} = run_claimed(assignment, opts)
    assert FakeAdapter.abort_release_effects(adapter) == 1
  end

  test "rejects a noncanonical checkout head without asserting no checkout" do
    {adapter, _journal, opts} = ports(checkout_head: String.duplicate("a", 41))
    assignment = assignment()

    assert {:held, :checkout_outcome_unknown, %{phase: :checkout_outcome_unknown}} =
             run_claimed(assignment, opts)

    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) in [:execute, :ensure_abort_cleanup, :publish_or_reconcile_abort_result]))
  end

  test "reconciles a lost blocked-result acknowledgement before attempting cleanup" do
    {adapter, _journal, opts} = ports(checkout_failure: true, faults: %{abort_result: 1})
    assignment = assignment()

    assert {:held, :abort_result_reconciliation_failed, %{phase: :abort_result_pending}} =
             run_claimed(assignment, opts)

    assert FakeAdapter.active_credential_refs(adapter) == []
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :ensure_abort_cleanup))

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_verified}} =
             run_claimed(assignment, opts)

    events = FakeAdapter.events(adapter)
    result_keys = for {:publish_or_reconcile_abort_result, _allocation_id, _digest, _result, key} <- events, do: key
    assert result_keys == ["#{assignment.sha256}:abort-result", "#{assignment.sha256}:abort-result"]
    assert Enum.count(events, &(elem(&1, 0) == :ensure_abort_cleanup)) == 1
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute))
  end

  test "holds abort publication until an issued checkout lease is revoked" do
    {adapter, _journal, opts} = ports(checkout_failure: true, faults: %{credential_revoke: 1})
    assignment = assignment()

    assert {:held, :credential_lease_revocation_failed, %{phase: :abort_pending}} =
             run_claimed(assignment, opts)

    assert FakeAdapter.active_credential_refs(adapter) == ["#{assignment.sha256}:credential-acquire"]
    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :publish_or_reconcile_abort_result))

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_verified}} =
             run_claimed(assignment, opts)

    assert FakeAdapter.active_credential_refs(adapter) == []
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :prepare_checkout)) == 1
  end

  test "retries a malformed blocked-result acknowledgement before cleanup" do
    {adapter, _journal, opts} = ports(checkout_failure: true, invalid_abort_result_ack: true)
    assignment = assignment()

    assert {:held, :invalid_abort_result_acknowledgement, %{phase: :abort_result_pending}} =
             run_claimed(assignment, opts)

    refute Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :ensure_abort_cleanup))

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_verified}} =
             run_claimed(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :publish_or_reconcile_abort_result)) == 2
    assert Enum.count(events, &(elem(&1, 0) == :ensure_abort_cleanup)) == 1
  end

  test "holds a malformed pre-execution cleanup response and retries cleanup only" do
    {adapter, _journal, opts} = ports(checkout_failure: true, invalid_abort_cleanup_response: true)
    assignment = assignment()

    assert {:held, :abort_cleanup_unverified, %{phase: :abort_cleanup_pending}} =
             run_claimed(assignment, opts)

    assert {:blocked, :checkout_preparation_failed, %{phase: :abort_cleanup_verified}} =
             run_claimed(assignment, opts)

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
             run_claimed(assignment, opts)

    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :ensure_terminal_cleanup)) == 2
    assert is_pid(journal)
  end

  test "holds a structurally complete cleanup receipt when its adapter signature check fails" do
    {adapter, _journal, opts} = ports(signature_invalid: true)
    assignment = assignment()

    assert {:held, :cleanup_unverified, %{phase: :cleanup_pending}} = run_claimed(assignment, opts)
    assert Enum.any?(FakeAdapter.events(adapter), &(elem(&1, 0) == :verify_terminal_cleanup))
  end

  test "re-verifies a terminal record and holds if cleanup signature verification fails" do
    {adapter, journal, opts} = ports()
    assignment = assignment()

    assert {:ok, %{phase: :terminal} = terminal} = run_claimed(assignment, opts)
    key = "#{assignment.lease.issue_id}:#{assignment.lease.generation}"

    assert {:ok, ^terminal} = FakeJournal.load(key, journal)

    tampered = %{
      terminal
      | version: terminal.version + 1,
        cleanup_evidence: %{terminal.cleanup_evidence | signature: "invalid-signature"}
    }

    assert :ok = FakeJournal.compare_and_swap(key, terminal.version, tampered, journal)

    assert {:held, :terminal_cleanup_reverification_failed, %{phase: :terminal}} =
             run_claimed(assignment, opts)

    events = FakeAdapter.events(adapter)
    assert Enum.count(events, &(elem(&1, 0) == :allocate_or_reconcile)) == 1
    assert Enum.count(events, &(elem(&1, 0) == :verify_terminal_cleanup)) == 2
  end

  test "rejects a changed assignment on replay even when issue and generation match" do
    {_adapter, _journal, opts} = ports()
    first = assignment()
    changed = assignment(branch: "codex/other")

    assert {:ok, %{phase: :terminal}} = run_claimed(first, opts)
    assert {:error, :assignment_replay_mismatch} = run_claimed(changed, opts)
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

    assert {:held, :execution_outcome_unknown, _} = run_claimed(assignment, opts)
    assert {:ok, %{phase: :terminal, execution_result: ^recovered}} = run_claimed(assignment, opts)
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :execute)) == 1
    assert Enum.count(FakeAdapter.events(adapter), &(elem(&1, 0) == :reconcile_execution)) == 1
    assert is_pid(journal)
  end

  test "reconciles a lost result acknowledgement with the same result key" do
    {adapter, _journal, opts} = ports(faults: %{result: 1})
    assignment = assignment()

    assert {:held, :result_reconciliation_failed, %{phase: :result_pending}} = run_claimed(assignment, opts)
    assert {:ok, %{phase: :terminal}} = run_claimed(assignment, opts)
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

  defp run_claimed(assignment, opts) do
    ManagedExecutor.run(assignment, Keyword.put_new(opts, :provider_claim, provider_claim(assignment)))
  end

  defp provider_claim(assignment) do
    lease = assignment.lease

    reservation = %{
      projection_id: "projection-hgs729",
      reservation_id: "reservation-hgs729",
      reservation_nonce: "nonce-hgs729",
      workspace_id: "workspace-hgs729",
      company_id: "company-hgs729",
      issue_id: lease.issue_id,
      runner_id: assignment.seat,
      managed_project_profile_id: "profile-hgs729",
      repository_ref: assignment.repository_ref,
      scope_keys: ["repository:#{assignment.repository_ref}"],
      generation: lease.generation,
      session_id: lease.session_id,
      process_id: lease.process_id,
      responsible_delegation_id: "delegation-fixture",
      execution_fence_token: "#{lease.issue_id}:#{lease.generation}",
      runtime_lease_id: lease.session_id
    }

    attestation =
      reservation
      |> Map.take([
        :runner_id,
        :managed_project_profile_id,
        :reservation_id,
        :reservation_nonce,
        :issue_id,
        :generation,
        :session_id,
        :process_id,
        :responsible_delegation_id,
        :execution_fence_token,
        :runtime_lease_id,
        :repository_ref,
        :scope_keys
      ])
      |> Map.merge(%{
        contract_version: "work-package-runtime-attestation.v1",
        attested_at: "2026-09-26T00:00:00.000Z",
        signature: "synthetic-attestation-signature"
      })

    response = %{
      projection_id: reservation.projection_id,
      projection_state: "active",
      mutation_state: "applied",
      claim_evidence: %{
        "responsibleDelegationId" => reservation.responsible_delegation_id,
        "executionFenceToken" => reservation.execution_fence_token,
        "runtimeLeaseId" => reservation.runtime_lease_id
      }
    }

    %{reservation: reservation, attestation: attestation, response: response}
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
