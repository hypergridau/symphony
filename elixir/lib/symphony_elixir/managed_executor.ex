defmodule SymphonyElixir.ManagedExecutor do
  @moduledoc """
  Drives one assignment through a managed executor using explicit typed ports.

  This module is a source contract only. It is intentionally not wired to the
  Orchestrator, a live provisioner, a host checkout, or an execution adapter.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.ManagedExecutor.Adapter
  alias SymphonyElixir.ManagedExecutor.Record

  @type result :: {:ok, map()} | {:held, term(), map()} | {:error, term()}

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
    do: execute(record, assignment, ports)

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
      {:error, _reason} -> {:held, :checkout_preparation_failed, record}
      _ -> {:held, :checkout_intent_mismatch, record}
    end
  end

  defp save_checkout(record, receipt, assignment, intent, ports) do
    with :ok <- Record.validate_checkout(receipt, assignment, intent),
         {:ok, ready} <- checkpoint(record, :checkout_ready, ports, %{checkout: receipt}) do
      advance(ready, assignment, ports)
    else
      {:error, _reason} -> {:held, :checkout_intent_mismatch, record}
    end
  end

  defp execute(record, assignment, ports) do
    case checkpoint(record, :execution_started, ports) do
      {:ok, started} ->
        execute_started(started, assignment, ports)

      {:error, reason} ->
        {:held, reason, record}
    end
  end

  defp execute_started(record, assignment, ports) do
    case ports.adapter.execute(
           record.allocation,
           assignment,
           record.checkout,
           key(assignment, "execute"),
           ports.adapter_context
         ) do
      {:ok, result} -> save_execution_result(record, result, assignment, ports)
      {:error, _reason} -> {:held, :execution_outcome_unknown, record}
      _ -> {:held, :invalid_execution_result, record}
    end
  end

  defp reconcile_execution(record, assignment, ports) do
    ports.adapter.reconcile_execution(
      record.allocation,
      assignment,
      record.checkout,
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
    with :ok <- Record.validate_cleanup_evidence(evidence, record.allocation, assignment, record.execution_result),
         :ok <-
           ports.adapter.verify_terminal_cleanup(
             evidence,
             record.allocation,
             assignment,
             record.execution_result,
             ports.adapter_context
           ),
         {:ok, terminal} <- checkpoint(record, :terminal, ports, %{cleanup_evidence: evidence}) do
      {:ok, terminal}
    else
      {:error, :cleanup_evidence_invalid} -> {:held, :cleanup_evidence_invalid, record}
      {:error, _reason} -> {:held, :cleanup_unverified, record}
      _ -> {:held, :cleanup_evidence_invalid, record}
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
        {:execute, 5},
        {:reconcile_execution, 5},
        {:publish_or_reconcile_result, 5},
        {:ensure_terminal_cleanup, 5},
        {:verify_terminal_cleanup, 5}
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
end
