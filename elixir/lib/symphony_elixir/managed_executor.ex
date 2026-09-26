defmodule SymphonyElixir.ManagedExecutor do
  @moduledoc """
  Drives one assignment through a managed executor using explicit typed ports.

  This module is a source contract only. It is intentionally not wired to the
  Orchestrator, a live provisioner, a host checkout, or an execution adapter.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.ManagedExecutor.Adapter
  alias SymphonyElixir.ManagedExecutor.Record

  @type result :: {:ok, map()} | {:blocked, term(), map()} | {:held, term(), map()} | {:error, term()}

  @doc "Runs or safely reconciles one exact assignment lifecycle."
  @spec run(map(), keyword()) :: result()
  def run(assignment, opts) when is_map(assignment) and is_list(opts) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         {:ok, ports} <- ports(opts),
         key = assignment_key(assignment),
         {:ok, record} <- Record.load_or_create(ports.journal, key, assignment, ports.journal_context) do
      advance(record, assignment, ports)
    end
  end

  def run(_assignment, _opts), do: {:error, :invalid_assignment}

  @spec checkout_intent(Adapter.assignment()) :: Adapter.checkout_intent()
  def checkout_intent(assignment) when is_map(assignment) do
    Record.checkout_intent(assignment)
  end

  defp advance(%{phase: :terminal} = record, assignment, ports) do
    with :ok <-
           Record.validate_cleanup_evidence(
             record.cleanup_evidence,
             record.allocation,
             assignment,
             record.execution_result
           ),
         :ok <-
           ports.adapter.verify_terminal_cleanup(
             record.cleanup_evidence,
             record.allocation,
             assignment,
             record.execution_result,
             ports.adapter_context
           ) do
      {:ok, record}
    else
      _ -> {:held, :terminal_cleanup_reverification_failed, record}
    end
  end

  defp advance(%{phase: :abort_pending} = record, assignment, ports),
    do: record_abort_result(record, assignment, ports)

  defp advance(%{phase: :abort_result_pending} = record, assignment, ports),
    do: publish_abort_result(record, assignment, ports)

  defp advance(%{phase: :abort_cleanup_pending} = record, assignment, ports),
    do: ensure_abort_cleanup(record, assignment, ports)

  defp advance(%{phase: :credential_request_revocation_pending} = record, assignment, ports),
    do: revoke_candidate_request(record, assignment, ports)

  defp advance(%{phase: :execution_started} = record, assignment, ports) do
    case reconcile_execution(record, assignment, ports) do
      {:ok, nil} -> {:held, :execution_outcome_unknown, record}
      {:ok, result} -> record_result(record, assignment, result, ports)
      {:error, _reason} -> {:held, :execution_reconciliation_unavailable, record}
      _ -> {:held, :invalid_execution_reconciliation, record}
    end
  end

  defp advance(%{phase: phase} = record, assignment, ports) when phase in [:planned, :allocation_pending],
    do: allocate(record, assignment, ports)

  defp advance(%{phase: phase} = record, assignment, ports) when phase in [:allocated, :checkout_pending],
    do: checkout(record, assignment, ports)

  defp advance(%{phase: :checkout_ready} = record, assignment, ports),
    do: acquire_credential_lease(record, assignment, ports)

  defp advance(%{phase: phase} = record, assignment, ports)
       when phase in [:credential_lease_pending, :credential_lease_ready],
       do: credential_lease(record, assignment, ports)

  defp advance(%{phase: :result_recorded} = record, assignment, ports),
    do: report_result(record, assignment, ports)

  defp advance(%{phase: :result_pending} = record, assignment, ports),
    do: report_result(record, assignment, ports)

  defp advance(%{phase: phase} = record, assignment, ports) when phase in [:result_reported, :cleanup_pending],
    do: finish(record, assignment, ports)

  defp advance(%{phase: phase} = record, _assignment, _ports), do: {:held, {:unsupported_phase, phase}, record}

  defp allocate(record, assignment, ports) do
    case checkpoint(record, :allocation_pending, ports) do
      {:ok, pending} ->
        allocate_pending(pending, assignment, ports)

      {:error, reason} ->
        {:held, reason, record}
    end
  end

  defp allocate_pending(record, assignment, ports) do
    case ports.adapter.allocate_or_reconcile(assignment, key(assignment, "allocation"), ports.adapter_context) do
      {:ok, allocation} -> save_allocation(record, allocation, assignment, ports)
      {:error, _reason} -> {:held, :allocation_reconciliation_failed, record}
      _ -> {:held, :invalid_allocation, record}
    end
  end

  defp save_allocation(record, allocation, assignment, ports) do
    with :ok <- Record.validate_allocation(allocation),
         {:ok, allocated} <- checkpoint(record, :allocated, ports, %{allocation: allocation}) do
      advance(allocated, assignment, ports)
    else
      {:error, _reason} -> {:held, :allocation_reconciliation_failed, record}
    end
  end

  defp checkout(record, assignment, ports) do
    case checkpoint(record, :checkout_pending, ports) do
      {:ok, pending} ->
        checkout_pending(pending, assignment, ports)

      {:error, reason} ->
        {:held, reason, record}
    end
  end

  defp checkout_pending(record, assignment, ports) do
    intent = checkout_intent(assignment)

    case ports.adapter.prepare_checkout(
           record.allocation,
           assignment,
           intent,
           key(assignment, "checkout"),
           ports.adapter_context
         ) do
      {:ok, receipt} -> save_checkout(record, receipt, assignment, intent, ports)
      {:error, _reason} -> abort_before_execution(record, assignment, :checkout_preparation_failed, ports)
      _ -> abort_before_execution(record, assignment, :checkout_intent_mismatch, ports)
    end
  end

  defp save_checkout(record, receipt, assignment, intent, ports) do
    with :ok <- Record.validate_checkout(receipt, assignment, intent),
         {:ok, ready} <- checkpoint(record, :checkout_ready, ports, %{checkout: receipt}) do
      advance(ready, assignment, ports)
    else
      {:error, _reason} -> abort_before_execution(record, assignment, :checkout_intent_mismatch, ports)
    end
  end

  defp acquire_credential_lease(record, assignment, ports) do
    case checkpoint(record, :credential_lease_pending, ports) do
      {:ok, pending} -> credential_lease(pending, assignment, ports)
      {:error, reason} -> {:held, reason, record}
    end
  end

  defp credential_lease(%{phase: :credential_lease_pending} = record, assignment, ports) do
    case ports.adapter.acquire_credential_lease(
           record.allocation,
           assignment,
           key(assignment, "credential-acquire"),
           ports.adapter_context
         ) do
      {:ok, lease} -> save_credential_lease(record, lease, assignment, ports)
      {:error, :denied} -> abort_before_execution(record, assignment, :credential_lease_denied, ports)
      {:error, _reason} -> {:held, :credential_lease_acquisition_failed, record}
      _ -> quarantine_candidate_request(record, assignment, ports, :acquire)
    end
  end

  defp credential_lease(%{phase: :credential_lease_ready} = record, assignment, ports) do
    lease = record.credential_lease

    if lease.expires_at_ms <= now_ms() do
      revoke_before_abort(record, assignment, :credential_lease_expired, ports)
    else
      case ports.adapter.renew_credential_lease(
             record.allocation,
             assignment,
             lease,
             key(assignment, "credential-renew"),
             ports.adapter_context
           ) do
        {:ok, renewed} -> save_renewed_credential_lease(record, renewed, assignment, ports)
        {:error, :denied} -> revoke_before_abort(record, assignment, :credential_lease_denied, ports)
        {:error, _reason} -> {:held, :credential_lease_renewal_failed, record}
        _ -> quarantine_candidate_request(record, assignment, ports, :renew)
      end
    end
  end

  defp save_credential_lease(record, lease_response, assignment, ports) do
    case Record.validate_credential_lease_response(lease_response, record.allocation, assignment) do
      :ok ->
        lease = Record.credential_lease_from_response(lease_response, assignment)
        persist_or_revoke_credential_lease(record, lease, assignment, ports)

      {:error, _reason} ->
        quarantine_candidate_request(record, assignment, ports, :acquire)
    end
  end

  defp persist_or_revoke_credential_lease(record, lease, assignment, ports) do
    if lease.expires_at_ms <= now_ms() do
      revoke_unpersisted_lease(record, assignment, lease, :credential_lease_expired, ports)
    else
      case checkpoint(record, :credential_lease_ready, ports, %{credential_lease: lease}) do
        {:ok, ready} -> advance(ready, assignment, ports)
        {:error, reason} -> {:held, reason, record}
      end
    end
  end

  defp save_renewed_credential_lease(record, renewed_response, assignment, ports) do
    case Record.validate_credential_lease_response(renewed_response, record.allocation, assignment) do
      :ok ->
        renewed = Record.credential_lease_from_response(renewed_response, assignment)
        persist_renewed_credential_lease(record, renewed, assignment, ports)

      {:error, _reason} ->
        quarantine_candidate_request(record, assignment, ports, :renew)
    end
  end

  defp persist_renewed_credential_lease(record, renewed, assignment, ports) do
    if renewed.expires_at_ms <= now_ms() do
      revoke_before_abort(record, assignment, :credential_lease_expired, ports)
    else
      case checkpoint(record, :execution_started, ports, %{credential_lease: renewed}) do
        {:ok, started} -> execute_started(started, assignment, ports)
        {:error, reason} -> {:held, reason, record}
      end
    end
  end

  defp revoke_unpersisted_lease(record, assignment, lease, reason, ports) do
    case ports.adapter.revoke_credential_lease(
           record.allocation,
           assignment,
           lease.lease_ref,
           key(assignment, "credential-revoke"),
           ports.adapter_context
         ) do
      :ok -> abort_before_execution(record, assignment, reason, ports)
      _ -> {:held, :credential_lease_revocation_failed, record}
    end
  end

  defp revoke_before_abort(record, assignment, reason, ports) do
    case revoke_credential_lease(record, assignment, ports) do
      :ok -> abort_before_execution(record, assignment, reason, ports)
      _ -> {:held, :credential_lease_revocation_failed, record}
    end
  end

  defp revoke_credential_lease(record, assignment, ports) do
    ports.adapter.revoke_credential_lease(
      record.allocation,
      assignment,
      record.credential_lease.lease_ref,
      key(assignment, "credential-revoke"),
      ports.adapter_context
    )
  end

  defp quarantine_candidate_request(record, assignment, ports, operation) do
    attrs = %{candidate_lease_operation: operation}

    case checkpoint(record, :credential_request_revocation_pending, ports, attrs) do
      {:ok, pending} -> revoke_candidate_request(pending, assignment, ports)
      {:error, checkpoint_reason} -> {:held, checkpoint_reason, record}
    end
  end

  defp revoke_candidate_request(record, assignment, ports) do
    operation_key = key(assignment, "credential-#{record.candidate_lease_operation}")
    revoke_key = key(assignment, "credential-request-revoke:#{record.candidate_lease_operation}")

    case ports.adapter.revoke_credential_lease_request(
           record.allocation,
           assignment,
           operation_key,
           revoke_key,
           ports.adapter_context
         ) do
      :ok -> abort_before_execution(record, assignment, :credential_lease_invalid, ports)
      _ -> {:held, :credential_lease_candidate_revocation_failed, record}
    end
  end

  defp execute_started(record, assignment, ports) do
    if record.credential_lease.expires_at_ms <= now_ms() do
      revoke_before_abort(record, assignment, :credential_lease_expired, ports)
    else
      case ports.adapter.execute(
             record.allocation,
             assignment,
             record.checkout,
             record.credential_lease,
             key(assignment, "execute"),
             ports.adapter_context
           ) do
        {:ok, result} -> save_execution_result(record, result, assignment, ports)
        {:error, _reason} -> {:held, :execution_outcome_unknown, record}
        _ -> {:held, :invalid_execution_result, record}
      end
    end
  end

  defp reconcile_execution(record, assignment, ports) do
    ports.adapter.reconcile_execution(
      record.allocation,
      assignment,
      record.checkout,
      record.credential_lease,
      key(assignment, "execute"),
      ports.adapter_context
    )
  end

  defp save_execution_result(record, result, assignment, ports) do
    with :ok <- Record.validate_result(result, assignment),
         {:ok, recorded} <- checkpoint(record, :result_recorded, ports, %{execution_result: result}) do
      advance(recorded, assignment, ports)
    else
      {:error, _reason} -> {:held, :execution_outcome_unknown, record}
    end
  end

  defp record_result(record, assignment, result, ports) do
    with :ok <- Record.validate_result(result, assignment),
         {:ok, recorded} <- checkpoint(record, :result_recorded, ports, %{execution_result: result}) do
      advance(recorded, assignment, ports)
    else
      _ -> {:held, :invalid_reconciled_execution_result, record}
    end
  end

  defp report_result(record, assignment, ports) do
    case checkpoint(record, :result_pending, ports) do
      {:ok, pending} ->
        publish_result(pending, assignment, ports)

      {:error, reason} ->
        {:held, reason, record}
    end
  end

  defp publish_result(record, assignment, ports) do
    response =
      ports.adapter.publish_or_reconcile_result(
        record.allocation,
        assignment,
        record.execution_result,
        key(assignment, "result"),
        ports.adapter_context
      )

    case response do
      {:ok, result_ref} when is_binary(result_ref) -> save_result_ref(record, result_ref, assignment, ports)
      {:error, _reason} -> {:held, :result_reconciliation_failed, record}
      _ -> {:held, :invalid_result_acknowledgement, record}
    end
  end

  defp save_result_ref(record, result_ref, assignment, ports) do
    if Record.nonempty_text?(result_ref) do
      case checkpoint(record, :result_reported, ports, %{result_ref: result_ref}) do
        {:ok, reported} -> advance(reported, assignment, ports)
        {:error, reason} -> {:held, reason, record}
      end
    else
      {:held, :invalid_result_acknowledgement, record}
    end
  end

  defp finish(record, assignment, ports) do
    case checkpoint(record, :cleanup_pending, ports) do
      {:ok, pending} ->
        ensure_cleanup(pending, assignment, ports)

      {:error, reason} ->
        {:held, reason, record}
    end
  end

  defp ensure_cleanup(record, assignment, ports) do
    case revoke_credential_lease(record, assignment, ports) do
      :ok -> ensure_terminal_cleanup(record, assignment, ports)
      _ -> {:held, :credential_lease_revocation_failed, record}
    end
  end

  defp ensure_terminal_cleanup(record, assignment, ports) do
    response =
      ports.adapter.ensure_terminal_cleanup(
        record.allocation,
        assignment,
        record.execution_result,
        key(assignment, "cleanup"),
        ports.adapter_context
      )

    case response do
      {:ok, evidence} -> verify_and_record_cleanup(record, evidence, assignment, ports)
      {:error, _reason} -> {:held, :cleanup_unverified, record}
      _ -> {:held, :cleanup_evidence_invalid, record}
    end
  end

  defp verify_and_record_cleanup(record, evidence, assignment, ports) do
    with :ok <-
           Record.validate_cleanup_evidence(
             evidence,
             record.allocation,
             assignment,
             record.execution_result
           ),
         :ok <-
           ports.adapter.verify_terminal_cleanup(
             evidence,
             record.allocation,
             assignment,
             record.execution_result,
             ports.adapter_context
           ),
         {:ok, terminal} <-
           checkpoint(record, :terminal, ports, %{cleanup_evidence: evidence, credential_lease: nil}) do
      {:ok, terminal}
    else
      {:error, :cleanup_evidence_invalid} -> {:held, :cleanup_evidence_invalid, record}
      {:error, _reason} -> {:held, :cleanup_unverified, record}
      _ -> {:held, :cleanup_evidence_invalid, record}
    end
  end

  defp abort_before_execution(record, assignment, reason, ports) do
    attrs = %{abort_reason: reason, checkout: Map.get(record, :checkout)}

    case checkpoint(record, :abort_pending, ports, attrs) do
      {:ok, pending} -> advance(pending, assignment, ports)
      {:error, checkpoint_reason} -> {:held, checkpoint_reason, record}
    end
  end

  defp record_abort_result(record, assignment, ports) do
    abort_result = %{
      assignment_digest: assignment.sha256,
      abort_reason: record.abort_reason,
      outcome: :blocked,
      summary: Record.pre_execution_summary(record.abort_reason),
      evidence_ref: "managed-executor:#{assignment.sha256}:#{record.abort_reason}"
    }

    with :ok <- Record.validate_pre_execution_result(abort_result, assignment, record.abort_reason),
         {:ok, pending} <- checkpoint(record, :abort_result_pending, ports, %{pre_execution_result: abort_result}) do
      advance(pending, assignment, ports)
    else
      {:error, reason} -> {:held, reason, record}
    end
  end

  defp publish_abort_result(record, assignment, ports) do
    response =
      ports.adapter.publish_or_reconcile_abort_result(
        record.allocation,
        assignment,
        record.pre_execution_result,
        key(assignment, "abort-result"),
        ports.adapter_context
      )

    case response do
      {:ok, result_ref} when is_binary(result_ref) ->
        save_abort_result_ref(record, result_ref, assignment, ports)

      {:error, _reason} ->
        {:held, :abort_result_reconciliation_failed, record}

      _ ->
        {:held, :invalid_abort_result_acknowledgement, record}
    end
  end

  defp save_abort_result_ref(record, result_ref, assignment, ports) do
    case Record.nonempty_text?(result_ref) do
      true ->
        case checkpoint(record, :abort_cleanup_pending, ports, %{abort_result_ref: result_ref}) do
          {:ok, pending} -> advance(pending, assignment, ports)
          {:error, reason} -> {:held, reason, record}
        end

      false ->
        {:held, :invalid_abort_result_acknowledgement, record}
    end
  end

  defp ensure_abort_cleanup(record, assignment, ports) do
    revoked =
      if is_map(record.credential_lease), do: revoke_credential_lease(record, assignment, ports), else: :ok

    response =
      if revoked == :ok do
        ports.adapter.ensure_abort_cleanup(
          record.allocation,
          assignment,
          record.abort_reason,
          key(assignment, "abort-cleanup"),
          ports.adapter_context
        )
      else
        {:error, :credential_lease_revocation_failed}
      end

    case response do
      :ok -> {:blocked, record.abort_reason, record}
      {:error, _reason} -> {:held, :abort_cleanup_unverified, record}
      _ -> {:held, :abort_cleanup_unverified, record}
    end
  end

  defp checkpoint(record, phase, ports, attrs \\ %{}) do
    Record.checkpoint(record, phase, ports.journal, ports.journal_context, attrs)
  end

  defp ports(opts) do
    required = [:adapter, :journal]

    if Enum.all?(required, &Keyword.has_key?(opts, &1)) do
      adapter = Keyword.fetch!(opts, :adapter)
      journal = Keyword.fetch!(opts, :journal)

      adapter_callbacks = [
        {:allocate_or_reconcile, 3},
        {:prepare_checkout, 5},
        {:acquire_credential_lease, 4},
        {:renew_credential_lease, 5},
        {:revoke_credential_lease, 5},
        {:revoke_credential_lease_request, 5},
        {:execute, 6},
        {:reconcile_execution, 6},
        {:publish_or_reconcile_result, 5},
        {:ensure_terminal_cleanup, 5},
        {:verify_terminal_cleanup, 5},
        {:ensure_abort_cleanup, 5},
        {:publish_or_reconcile_abort_result, 5}
      ]

      adapter_valid? =
        Code.ensure_loaded?(adapter) and
          Enum.all?(adapter_callbacks, &function_exported?(adapter, elem(&1, 0), elem(&1, 1)))

      journal_valid? =
        Code.ensure_loaded?(journal) and function_exported?(journal, :load, 2) and
          function_exported?(journal, :compare_and_swap, 4)

      if adapter_valid? and journal_valid? do
        {:ok,
         %{
           adapter: adapter,
           adapter_context: Keyword.get(opts, :adapter_context),
           journal: journal,
           journal_context: Keyword.get(opts, :journal_context)
         }}
      else
        {:error, :managed_executor_adapter_invalid}
      end
    else
      {:error, :managed_executor_adapters_missing}
    end
  end

  defp assignment_key(assignment), do: "#{assignment.lease.issue_id}:#{assignment.lease.generation}"
  defp key(assignment, stage), do: "#{assignment.sha256}:#{stage}"
  defp now_ms, do: System.system_time(:millisecond)
end
