defmodule SymphonyElixir.ManagedExecutor.FakeJournal do
  @behaviour SymphonyElixir.ManagedExecutor.Journal

  def start_link, do: Agent.start_link(fn -> %{} end)

  @impl true
  def load(key, pid), do: Agent.get(pid, &{:ok, Map.get(&1, key)})

  @impl true
  def compare_and_swap(key, expected_version, record, pid) do
    Agent.get_and_update(pid, fn records ->
      case Map.get(records, key) do
        nil when expected_version == 0 -> {:ok, Map.put(records, key, record)}
        %{version: ^expected_version} -> {:ok, Map.put(records, key, record)}
        _ -> {{:error, :conflict}, records}
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
        invalid_result_ack: Keyword.get(opts, :invalid_result_ack, false),
        invalid_abort_result_ack: Keyword.get(opts, :invalid_abort_result_ack, false),
        invalid_abort_cleanup_response: Keyword.get(opts, :invalid_abort_cleanup_response, false),
        result: Keyword.get(opts, :result),
        checkout_mismatch: Keyword.get(opts, :checkout_mismatch, false),
        checkout_failure: Keyword.get(opts, :checkout_failure, false),
        checkout_head: Keyword.get(opts, :checkout_head, "0123456789abcdef0123456789abcdef01234567"),
        execution_reconciliation: Keyword.get(opts, :execution_reconciliation),
        cleanup_invalid: Keyword.get(opts, :cleanup_invalid, false),
        signature_invalid: Keyword.get(opts, :signature_invalid, false),
        credential_denied: Keyword.get(opts, :credential_denied, false),
        credential_expired: Keyword.get(opts, :credential_expired, false),
        credential_renew_denied: Keyword.get(opts, :credential_renew_denied, false),
        credential_wrong_binding: Keyword.get(opts, :credential_wrong_binding, false),
        credential_material: "synthetic-secret-value"
      }
    end)
  end

  def events(pid), do: Agent.get(pid, &Enum.reverse(&1.events))

  @impl true
  def allocate_or_reconcile(assignment, idempotency_key, pid) do
    with_event(pid, {:allocate_or_reconcile, idempotency_key, assignment.sha256}, fn state ->
      allocation_response(state)
    end)
  end

  @impl true
  def prepare_checkout(allocation, assignment, intent, idempotency_key, pid) do
    with_event(pid, {:prepare_checkout, allocation.id, intent, idempotency_key}, &checkout_response(&1, assignment, intent))
  end

  @impl true
  def acquire_credential_lease(allocation, assignment, idempotency_key, pid) do
    with_event(pid, {:acquire_credential_lease, allocation.id, assignment.sha256, idempotency_key}, fn state ->
      cond do
        state.credential_denied ->
          {{:error, :denied}, %{state | credential_denied: false}}

        true ->
          expires_at_ms = if state.credential_expired, do: System.system_time(:millisecond) - 1, else: System.system_time(:millisecond) + 60_000

          lease = %{
            lease_ref: "credential-lease-fixture-1",
            assignment_digest: if(state.credential_wrong_binding, do: String.duplicate("0", 64), else: assignment.sha256),
            allocation_id: allocation.id,
            expires_at_ms: expires_at_ms
          }

          fail_once(%{state | credential_expired: false, credential_wrong_binding: false}, :credential_acquire, {:ok, lease})
      end
    end)
  end

  @impl true
  def renew_credential_lease(allocation, assignment, lease, idempotency_key, pid) do
    with_event(pid, {:renew_credential_lease, allocation.id, assignment.sha256, lease.lease_ref, idempotency_key}, fn state ->
      if state.credential_renew_denied do
        {{:error, :denied}, %{state | credential_renew_denied: false}}
      else
        fail_once(state, :credential_renew, {:ok, %{lease | expires_at_ms: System.system_time(:millisecond) + 60_000}})
      end
    end)
  end

  @impl true
  def revoke_credential_lease(allocation, assignment, lease, idempotency_key, pid) do
    with_event(pid, {:revoke_credential_lease, allocation.id, assignment.sha256, lease.lease_ref, idempotency_key}, fn state ->
      fail_once(state, :credential_revoke, :ok)
    end)
    |> case do
      {:ok, _} -> :ok
      other -> other
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
  def ensure_abort_cleanup(allocation, assignment, abort_reason, idempotency_key, pid) do
    with_event(pid, {:ensure_abort_cleanup, allocation.id, assignment.sha256, abort_reason, idempotency_key}, fn state ->
      abort_cleanup_response(state)
    end)
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
    do: {{:error, :synthetic_checkout_failure}, %{state | checkout_failure: false}}

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
