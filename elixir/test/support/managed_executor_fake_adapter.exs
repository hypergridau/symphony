defmodule SymphonyElixir.ManagedExecutor.FakeJournal do
  @behaviour SymphonyElixir.ManagedExecutor.Journal

  def start_link(opts \\ []) do
    Agent.start_link(fn ->
      %{
        records: %{},
        execution_started_delay_ms: Keyword.get(opts, :execution_started_delay_ms, 0),
        abort_verified_write_failure: Keyword.get(opts, :abort_verified_write_failure, false)
      }
    end)
  end

  @impl true
  def load(key, pid), do: Agent.get(pid, &{:ok, Map.get(&1.records, key)})

  @impl true
  def compare_and_swap(key, expected_version, record, pid) do
    delay =
      if Map.get(record, :phase) == :execution_started,
        do: Agent.get(pid, & &1.execution_started_delay_ms),
        else: 0

    if delay > 0, do: Process.sleep(delay)

    Agent.get_and_update(pid, fn state ->
      cond do
        state.abort_verified_write_failure and Map.get(record, :phase) == :abort_cleanup_verified ->
          {{:error, :synthetic_abort_verified_write_failure}, %{state | abort_verified_write_failure: false}}

        is_nil(Map.get(state.records, key)) and expected_version == 0 ->
          {:ok, %{state | records: Map.put(state.records, key, record)}}

        match?(%{version: ^expected_version}, Map.get(state.records, key)) ->
          {:ok, %{state | records: Map.put(state.records, key, record)}}

        true ->
          {{:error, :conflict}, state}
      end
    end)
  end
end

defmodule SymphonyElixir.ManagedExecutor.FakeAdapter do
  @behaviour SymphonyElixir.ManagedExecutor.Adapter

  def start_link(opts \\ []) do
    Agent.start_link(fn ->
      %{
        events: [],
        faults: Keyword.get(opts, :faults, %{}),
        invalid_allocation_response: Keyword.get(opts, :invalid_allocation_response, false),
        allocation_held: Keyword.get(opts, :allocation_held, false),
        invalid_result_ack: Keyword.get(opts, :invalid_result_ack, false),
        invalid_abort_result_ack: Keyword.get(opts, :invalid_abort_result_ack, false),
        invalid_abort_cleanup_response: Keyword.get(opts, :invalid_abort_cleanup_response, false),
        abort_cleanup_wrong_ref: Keyword.get(opts, :abort_cleanup_wrong_ref, false),
        result: Keyword.get(opts, :result),
        checkout_mismatch: Keyword.get(opts, :checkout_mismatch, false),
        checkout_failure: Keyword.get(opts, :checkout_failure, false),
        checkout_uncertain: Keyword.get(opts, :checkout_uncertain, false),
        checkout_head: Keyword.get(opts, :checkout_head, "0123456789abcdef0123456789abcdef01234567"),
        execution_reconciliation: Keyword.get(opts, :execution_reconciliation),
        cleanup_invalid: Keyword.get(opts, :cleanup_invalid, false),
        signature_invalid: Keyword.get(opts, :signature_invalid, false),
        credential_denied: Keyword.get(opts, :credential_denied, false),
        credential_expired: Keyword.get(opts, :credential_expired, false),
        credential_renew_denied: Keyword.get(opts, :credential_renew_denied, false),
        credential_wrong_binding: Keyword.get(opts, :credential_wrong_binding, false),
        credential_unsafe_ref: Keyword.get(opts, :credential_unsafe_ref, false),
        credential_renew_wrong_ref: Keyword.get(opts, :credential_renew_wrong_ref, false),
        credential_short_renewal: Keyword.get(opts, :credential_short_renewal, false),
        credential_leases: %{},
        credential_renewals: %{},
        active_credential_refs: [],
        credential_material: "synthetic-secret-value",
        credential_renewal_materializations: 0,
        credential_revoked_keys: MapSet.new(),
        credential_revocation_effects: 0,
        abort_release_results: %{},
        abort_release_effects: 0
      }
    end)
  end

  def events(pid), do: Agent.get(pid, &Enum.reverse(&1.events))
  def active_credential_refs(pid), do: Agent.get(pid, & &1.active_credential_refs)
  def credential_renewal_materializations(pid), do: Agent.get(pid, & &1.credential_renewal_materializations)
  def credential_revocation_effects(pid), do: Agent.get(pid, & &1.credential_revocation_effects)
  def abort_release_effects(pid), do: Agent.get(pid, & &1.abort_release_effects)

  @impl true
  def allocate_or_reconcile(assignment, idempotency_key, pid) do
    with_event(pid, {:allocate_or_reconcile, idempotency_key, assignment.sha256}, fn state ->
      allocation_response(state)
    end)
  end

  @impl true
  def prepare_checkout(allocation, assignment, intent, credential_lease, idempotency_key, pid) do
    with_event(pid, {:prepare_checkout, allocation.id, intent, credential_lease.lease_ref, idempotency_key}, &checkout_response(&1, assignment, intent))
  end

  @impl true
  def acquire_credential_lease(allocation, assignment, idempotency_key, pid) do
    event = {:acquire_credential_lease, allocation.id, assignment.sha256, idempotency_key}
    with_event(pid, event, &acquire_credential_lease_response(&1, allocation, assignment, idempotency_key))
  end

  defp acquire_credential_lease_response(%{credential_denied: true} = state, _allocation, _assignment, _key),
    do: {{:error, :denied}, %{state | credential_denied: false}}

  defp acquire_credential_lease_response(state, allocation, assignment, key) do
    case Map.fetch(state.credential_leases, key) do
      {:ok, lease} -> {{:ok, lease}, state}
      :error -> create_credential_lease(state, allocation, assignment, key)
    end
  end

  defp create_credential_lease(%{credential_wrong_binding: true} = state, allocation, assignment, key) do
    response = make_credential_lease_response(state, allocation, assignment) |> maybe_unsafe_response(state)
    refs = Enum.uniq([key | state.active_credential_refs])
    leases = Map.put(state.credential_leases, key, response)

    next = %{
      state
      | credential_wrong_binding: false,
        active_credential_refs: refs,
        credential_leases: leases
    }

    {{:ok, response}, next}
  end

  defp create_credential_lease(state, allocation, assignment, key) do
    lease = make_credential_lease_response(state, allocation, assignment) |> maybe_unsafe_response(state)
    leases = Map.put(state.credential_leases, key, lease)
    refs = Enum.uniq([key | state.active_credential_refs])
    next = %{state | credential_leases: leases, active_credential_refs: refs, credential_expired: false}
    fail_once(next, :credential_acquire, {:ok, lease})
  end

  defp maybe_unsafe_response(lease, %{credential_unsafe_ref: true}),
    do: Map.put(lease, :lease_ref, "YWJjZGVmZ2hpamtsbW5vcA")

  defp maybe_unsafe_response(lease, _state), do: lease

  defp make_credential_lease_response(state, allocation, assignment) do
    expiry = credential_expiry(state)
    digest = credential_assignment_digest(state, assignment)

    %{assignment_digest: digest, allocation_id: allocation.id, expires_at_ms: expiry}
  end

  defp credential_expiry(%{credential_expired: true}), do: System.system_time(:millisecond) - 1
  defp credential_expiry(_state), do: System.system_time(:millisecond) + 60_000

  defp credential_assignment_digest(%{credential_wrong_binding: true}, _assignment),
    do: String.duplicate("0", 64)

  defp credential_assignment_digest(_state, assignment), do: assignment.sha256

  @impl true
  def renew_credential_lease(allocation, assignment, lease, idempotency_key, pid) do
    event = {:renew_credential_lease, allocation.id, assignment.sha256, lease.lease_ref, idempotency_key}
    with_event(pid, event, &renew_credential_lease_response(&1, lease, idempotency_key))
  end

  defp renew_credential_lease_response(%{credential_renew_denied: true} = state, _lease, _key),
    do: {{:error, :denied}, %{state | credential_renew_denied: false}}

  defp renew_credential_lease_response(state, lease, key) do
    case Map.fetch(state.credential_renewals, key) do
      {:ok, response} ->
        {{:ok, response}, state}

      :error ->
        response = %{
          assignment_digest: lease.assignment_digest,
          allocation_id: lease.allocation_id,
          expires_at_ms: renewed_expiry(state)
        }

        response = maybe_invalid_renewal_response(response, state)
        renewals = Map.put(state.credential_renewals, key, response)
        refs = if state.credential_renew_wrong_ref, do: Enum.uniq([key | state.active_credential_refs]), else: state.active_credential_refs

        next = %{
          state
          | credential_renewals: renewals,
            credential_renewal_materializations: state.credential_renewal_materializations + 1,
            credential_renew_wrong_ref: false,
            credential_short_renewal: false,
            active_credential_refs: refs
        }

        fail_once(next, :credential_renew, {:ok, response})
    end
  end

  defp maybe_invalid_renewal_response(response, %{credential_renew_wrong_ref: true}),
    do: Map.put(response, :lease_ref, "YWx0ZXJuYXRlLWJlYXJlci10b2tlbg")

  defp maybe_invalid_renewal_response(response, _state), do: response

  defp renewed_expiry(%{credential_short_renewal: true}), do: System.system_time(:millisecond) + 100
  defp renewed_expiry(_state), do: System.system_time(:millisecond) + 60_000

  @impl true
  def revoke_credential_lease(allocation, assignment, lease_ref, idempotency_key, pid) do
    with_event(pid, {:revoke_credential_lease, allocation.id, assignment.sha256, lease_ref, idempotency_key}, fn state ->
      revoke_credential_lease_response(state, lease_ref, idempotency_key)
    end)
    |> case do
      {:ok, _} -> :ok
      other -> other
    end
  end

  @impl true
  def revoke_credential_lease_request(allocation, assignment, request_key, revoke_key, pid) do
    with_event(
      pid,
      {:revoke_credential_lease_request, allocation.id, assignment.sha256, request_key, revoke_key},
      fn state ->
        revoke_credential_lease_response(state, request_key, revoke_key)
      end
    )
    |> case do
      {:ok, _} -> :ok
      other -> other
    end
  end

  defp revoke_credential_lease_response(state, lease_ref, idempotency_key) do
    if MapSet.member?(state.credential_revoked_keys, idempotency_key) do
      {:ok, state}
    else
      revoke_response_unrecorded(state, lease_ref, idempotency_key)
    end
  end

  defp revoke_response_unrecorded(state, lease_ref, idempotency_key) do
    case fail_once(state, :credential_revoke, :ok) do
      {:ok, next} ->
        revoked_keys = MapSet.put(next.credential_revoked_keys, idempotency_key)

        effected = %{
          next
          | active_credential_refs: List.delete(next.active_credential_refs, lease_ref),
            credential_revoked_keys: revoked_keys,
            credential_revocation_effects: next.credential_revocation_effects + 1
        }

        return_lost_revoke_ack(effected)

      {error, next} ->
        {error, next}
    end
  end

  defp return_lost_revoke_ack(state) do
    case Map.get(state.faults, :credential_revoke_lost_ack, 0) do
      remaining when remaining > 0 ->
        faults = Map.put(state.faults, :credential_revoke_lost_ack, remaining - 1)
        {{:error, :lost_ack}, %{state | faults: faults}}

      _ ->
        {:ok, state}
    end
  end

  @impl true
  def execute(allocation, assignment, checkout, credential_lease, idempotency_key, pid) do
    with_event(pid, {:execute, allocation.id, checkout.head, credential_lease.lease_ref, idempotency_key}, fn state ->
      fail_once(state, :execute, {:ok, result(state, assignment)})
    end)
  end

  @impl true
  def reconcile_execution(allocation, assignment, checkout, credential_lease, idempotency_key, pid) do
    with_event(pid, {:reconcile_execution, allocation.id, checkout.head, credential_lease.lease_ref, idempotency_key}, fn state ->
      case state.execution_reconciliation do
        nil -> {{:ok, nil}, state}
        result -> {{:ok, result || result(state, assignment)}, state}
      end
    end)
  end

  @impl true
  def publish_or_reconcile_result(allocation, assignment, execution_result, idempotency_key, pid) do
    with_event(pid, {:publish_or_reconcile_result, allocation.id, assignment.sha256, execution_result, idempotency_key}, fn state ->
      result_response(state)
    end)
  end

  @impl true
  def ensure_terminal_cleanup(allocation, assignment, _execution_result, idempotency_key, pid) do
    with_event(pid, {:ensure_terminal_cleanup, allocation.id, assignment.sha256, idempotency_key}, fn state ->
      evidence = cleanup_evidence(allocation, assignment)
      evidence = if state.cleanup_invalid, do: %{evidence | credentials_revoked: false}, else: evidence
      evidence = if state.signature_invalid, do: %{evidence | signature: "invalid-signature"}, else: evidence
      {{:ok, evidence}, %{state | cleanup_invalid: false}}
    end)
  end

  @impl true
  def ensure_abort_cleanup(
        allocation,
        assignment,
        claim_binding,
        abort_reason,
        abort_result_ref,
        idempotency_key,
        pid
      ) do
    event = {:ensure_abort_cleanup, allocation.id, assignment.sha256, abort_reason, abort_result_ref, idempotency_key}

    with_event(pid, event, fn state ->
      case Map.fetch(state.abort_release_results, idempotency_key) do
        {:ok, ack} ->
          {{:ok, Map.put(ack, :replayed, true)}, state}

        :error ->
          create_abort_release_result(state, allocation, assignment, claim_binding, abort_result_ref, idempotency_key)
      end
    end)
  end

  defp create_abort_release_result(state, allocation, assignment, claim_binding, abort_result_ref, idempotency_key) do
    case abort_cleanup_response(state) do
      {:ok, next} ->
        ack = %{
          projection_id: claim_binding.projection_id,
          reservation_id: claim_binding.reservation_id,
          receipt_id: "abort-receipt-fixture-1",
          receipt_kind: "pre_execution_abort_cleanup_verified",
          assignment_digest: assignment.sha256,
          allocation_id: allocation.id,
          abort_result_ref: abort_result_ref,
          generation: assignment.lease.generation,
          execution_capacity_state: "released",
          scope_state: "released",
          reservation_state: "released",
          evidence_ref: "abort-evidence-fixture-1"
        }

        response_ack = if next.abort_cleanup_wrong_ref, do: %{ack | abort_result_ref: "wrong-result"}, else: ack

        updated = %{
          next
          | abort_release_results: Map.put(next.abort_release_results, idempotency_key, ack),
            abort_release_effects: next.abort_release_effects + 1,
            abort_cleanup_wrong_ref: false
        }

        {{:ok, Map.put(response_ack, :replayed, false)}, updated}

      other ->
        other
    end
  end

  @impl true
  def publish_or_reconcile_abort_result(allocation, assignment, abort_result, idempotency_key, pid) do
    with_event(
      pid,
      {:publish_or_reconcile_abort_result, allocation.id, assignment.sha256, abort_result, idempotency_key},
      fn state -> abort_result_response(state) end
    )
  end

  @impl true
  def verify_terminal_cleanup(evidence, _allocation, _assignment, _execution_result, pid) do
    with_event(pid, {:verify_terminal_cleanup, evidence.evidence_ref}, fn state ->
      response = if evidence.signature == "synthetic-signature", do: :ok, else: {:error, :signature_invalid}
      {response, state}
    end)
  end

  defp with_event(pid, event, fun) do
    Agent.get_and_update(pid, fn state ->
      {response, next} = fun.(state)
      {response, %{next | events: [event | next.events]}}
    end)
  end

  defp fail_once(state, key, success) do
    case Map.get(state.faults, key, 0) do
      remaining when remaining > 0 ->
        {{:error, {:synthetic_failure, key}}, %{state | faults: Map.put(state.faults, key, remaining - 1)}}

      _ ->
        {success, state}
    end
  end

  defp checkout_response(%{checkout_failure: true} = state, _assignment, _intent),
    do: {{:error, :no_checkout}, %{state | checkout_failure: false}}

  defp checkout_response(%{checkout_uncertain: true} = state, _assignment, _intent),
    do: {{:error, :uncertain_checkout}, %{state | checkout_uncertain: false}}

  defp checkout_response(state, assignment, intent) do
    receipt =
      Map.merge(intent, %{
        assignment_digest: assignment.sha256,
        head: state.checkout_head
      })

    receipt = if state.checkout_mismatch, do: %{receipt | branch: "codex/other"}, else: receipt
    {{:ok, receipt}, state}
  end

  defp allocation_response(%{invalid_allocation_response: true} = state),
    do: {:unexpected_allocation_response, %{state | invalid_allocation_response: false}}

  defp allocation_response(%{allocation_held: true} = state),
    do: {{:held, :job_allocation_registration_unverified}, state}

  defp allocation_response(state),
    do: fail_once(state, :allocate, {:ok, %{id: "allocation-fixture-1", status: :ready}})

  defp result_response(%{invalid_result_ack: true} = state),
    do: {{:ok, ""}, %{state | invalid_result_ack: false}}

  defp result_response(state), do: fail_once(state, :result, {:ok, "result-fixture-1"})

  defp abort_result_response(%{invalid_abort_result_ack: true} = state),
    do: {:unexpected_abort_result_ack, %{state | invalid_abort_result_ack: false}}

  defp abort_result_response(state), do: fail_once(state, :abort_result, {:ok, "abort-result-fixture-1"})

  defp abort_cleanup_response(%{invalid_abort_cleanup_response: true} = state),
    do: {:unexpected_abort_cleanup_response, %{state | invalid_abort_cleanup_response: false}}

  defp abort_cleanup_response(state), do: fail_once(state, :abort_cleanup, :ok)

  defp result(%{result: nil}, assignment), do: result(%{result: :default}, assignment)

  defp result(%{result: :default}, assignment),
    do: %{
      assignment_digest: assignment.sha256,
      outcome: :completed,
      summary: "Synthetic success",
      evidence_ref: "evidence-fixture-1",
      accepted_head: "89abcdef0123456789abcdef0123456789abcdef"
    }

  defp result(%{result: supplied}, _assignment), do: supplied

  defp cleanup_evidence(allocation, assignment) do
    %{
      contract_version: "work-package-cleanup-receipt.v1",
      receipt_kind: "repository_cleanup_verified",
      assignment_digest: assignment.sha256,
      issue_id: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      session_id: assignment.lease.session_id,
      process_id: assignment.lease.process_id,
      repository_ref: assignment.repository_ref,
      accepted_head: "89abcdef0123456789abcdef0123456789abcdef",
      terminal_outcome: :completed,
      allocation_id: allocation.id,
      workspace_removed: true,
      credentials_revoked: true,
      reviewer_leases_released: true,
      evidence_ref: "cleanup-fixture-1",
      checksum: String.duplicate("a", 64),
      signer_id: "synthetic-cleanup-signer",
      signature: "synthetic-signature"
    }
  end
end
