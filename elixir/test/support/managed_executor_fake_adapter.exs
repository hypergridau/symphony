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
        result: Keyword.get(opts, :result),
        checkout_mismatch: Keyword.get(opts, :checkout_mismatch, false),
        checkout_failure: Keyword.get(opts, :checkout_failure, false),
        checkout_head: Keyword.get(opts, :checkout_head, "0123456789abcdef0123456789abcdef01234567"),
        execution_reconciliation: Keyword.get(opts, :execution_reconciliation),
        cleanup_invalid: Keyword.get(opts, :cleanup_invalid, false),
        abort_cleanup_mismatch: Keyword.get(opts, :abort_cleanup_mismatch, false),
        signature_invalid: Keyword.get(opts, :signature_invalid, false)
      }
    end)
  end

  def events(pid), do: Agent.get(pid, &Enum.reverse(&1.events))

  @impl true
  def allocate_or_reconcile(assignment, idempotency_key, pid) do
    with_event(pid, {:allocate_or_reconcile, idempotency_key, assignment.sha256}, fn state ->
      fail_once(state, :allocate, {:ok, %{id: "allocation-fixture-1", status: :ready}})
    end)
  end

  @impl true
  def prepare_checkout(allocation, assignment, intent, idempotency_key, pid) do
    with_event(pid, {:prepare_checkout, allocation.id, intent, idempotency_key}, &checkout_response(&1, assignment, intent))
  end

  @impl true
  def execute(allocation, assignment, checkout, idempotency_key, pid) do
    with_event(pid, {:execute, allocation.id, checkout.head, idempotency_key}, fn state ->
      fail_once(state, :execute, {:ok, result(state, assignment)})
    end)
  end

  @impl true
  def reconcile_execution(allocation, assignment, checkout, idempotency_key, pid) do
    with_event(pid, {:reconcile_execution, allocation.id, checkout.head, idempotency_key}, fn state ->
      case state.execution_reconciliation do
        nil -> {{:ok, nil}, state}
        result -> {{:ok, result || result(state, assignment)}, state}
      end
    end)
  end

  @impl true
  def publish_or_reconcile_result(allocation, assignment, execution_result, idempotency_key, pid) do
    with_event(pid, {:publish_or_reconcile_result, allocation.id, assignment.sha256, execution_result, idempotency_key}, fn state ->
      fail_once(state, :result, {:ok, "result-fixture-1"})
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
      evidence = abort_cleanup_evidence(allocation, assignment, abort_reason)

      if state.abort_cleanup_mismatch do
        {{:ok, %{evidence | allocation_id: "another-allocation"}}, %{state | abort_cleanup_mismatch: false}}
      else
        fail_once(state, :abort_cleanup, {:ok, evidence})
      end
    end)
  end

  @impl true
  def verify_terminal_cleanup(evidence, _allocation, _assignment, _execution_result, pid) do
    with_event(pid, {:verify_terminal_cleanup, evidence.evidence_ref}, fn state ->
      response = if evidence.signature == "synthetic-signature", do: :ok, else: {:error, :signature_invalid}
      {response, state}
    end)
  end

  @impl true
  def verify_abort_cleanup(evidence, _allocation, _assignment, _abort_reason, pid) do
    with_event(pid, {:verify_abort_cleanup, evidence.evidence_ref}, fn state ->
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

  defp abort_cleanup_evidence(allocation, assignment, abort_reason) do
    %{
      contract_version: "managed-executor-abort-receipt.v1",
      receipt_kind: "pre_execution_cleanup_verified",
      assignment_digest: assignment.sha256,
      allocation_id: allocation.id,
      issue_id: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      session_id: assignment.lease.session_id,
      process_id: assignment.lease.process_id,
      repository_ref: assignment.repository_ref,
      abort_reason: abort_reason,
      workspace_removed: true,
      credentials_revoked: true,
      reviewer_leases_released: true,
      evidence_ref: "abort-cleanup-fixture-1",
      checksum: String.duplicate("b", 64),
      signer_id: "synthetic-cleanup-signer",
      signature: "synthetic-signature"
    }
  end
end
