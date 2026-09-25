defmodule SymphonyElixir.ManagedExecutor.Record do
  @moduledoc false

  @phases [
    :planned,
    :allocation_pending,
    :allocated,
    :checkout_pending,
    :checkout_ready,
    :credential_lease_pending,
    :credential_lease_ready,
    :execution_started,
    :result_recorded,
    :result_pending,
    :result_reported,
    :cleanup_pending,
    :terminal,
    :abort_pending,
    :abort_result_pending,
    :abort_cleanup_pending
  ]
  @phase_fields %{
    planned: [],
    allocation_pending: [],
    allocated: [:allocation],
    checkout_pending: [:allocation],
    checkout_ready: [:allocation, :checkout],
    credential_lease_pending: [:allocation, :checkout],
    credential_lease_ready: [:allocation, :checkout],
    execution_started: [:allocation, :checkout],
    result_recorded: [:allocation, :checkout, :execution_result],
    result_pending: [:allocation, :checkout, :execution_result],
    result_reported: [:allocation, :checkout, :execution_result, :result_ref],
    cleanup_pending: [:allocation, :checkout, :execution_result, :result_ref],
    terminal: [:allocation, :checkout, :execution_result, :result_ref, :cleanup_evidence],
    abort_pending: [:allocation, :abort_reason],
    abort_result_pending: [:allocation, :abort_reason, :pre_execution_result],
    abort_cleanup_pending: [:allocation, :abort_reason, :pre_execution_result, :abort_result_ref]
  }
  @checkout_phases [
    :checkout_ready,
    :credential_lease_pending,
    :credential_lease_ready,
    :execution_started,
    :result_recorded,
    :result_pending,
    :result_reported,
    :cleanup_pending,
    :terminal
  ]
  @result_phases [:result_recorded, :result_pending, :result_reported, :cleanup_pending, :terminal]
  @reported_phases [:result_reported, :cleanup_pending, :terminal]
  @outcomes [:completed, :failed, :blocked]
  @lease_phases [:credential_lease_ready, :execution_started, :result_recorded, :result_pending, :result_reported, :cleanup_pending]
  @abort_reasons [:checkout_preparation_failed, :checkout_intent_mismatch, :credential_lease_denied, :credential_lease_expired]

  @spec load_or_create(module(), String.t(), map(), term()) :: {:ok, map()} | {:error, term()}
  def load_or_create(journal, key, assignment, context) do
    case journal.load(key, context) do
      {:ok, nil} -> create_or_load(journal, key, assignment, context)
      {:ok, record} -> verify(record, key, assignment)
      {:error, _reason} -> {:error, :lifecycle_journal_read_failed}
      _ -> {:error, :invalid_journal_response}
    end
  end

  @spec checkpoint(map(), atom(), module(), term(), map()) :: {:ok, map()} | {:error, term()}
  def checkpoint(record, phase, journal, context, attrs) do
    next = record |> Map.merge(attrs) |> Map.put(:phase, phase) |> Map.put(:version, record.version + 1)

    case journal.compare_and_swap(record.key, record.version, next, context) do
      :ok -> {:ok, next}
      {:error, _reason} -> {:error, :lifecycle_journal_compare_and_swap_failed}
      _ -> {:error, :invalid_journal_write_response}
    end
  end

  @spec checkout_intent(map()) :: %{repository_ref: term(), base_ref: term(), branch: term()}
  def checkout_intent(assignment) when is_map(assignment),
    do: Map.take(assignment, [:repository_ref, :base_ref, :branch])

  @spec validate_allocation(term()) :: :ok | {:error, :allocation_invalid}
  def validate_allocation(%{id: id, status: :ready} = allocation)
      when map_size(allocation) == 2 and is_binary(id) and byte_size(id) > 0,
      do: :ok

  def validate_allocation(_allocation), do: {:error, :allocation_invalid}

  @spec validate_credential_lease(term(), map(), map()) :: :ok | {:error, :credential_lease_invalid}
  def validate_credential_lease(
        %{lease_ref: lease_ref, assignment_digest: digest, allocation_id: allocation_id, expires_at_ms: expiry} = lease,
        allocation,
        assignment
      ) do
    exact_keys = MapSet.new(Map.keys(lease)) == MapSet.new([:lease_ref, :assignment_digest, :allocation_id, :expires_at_ms])

    if exact_keys and nonempty_text?(lease_ref) and digest == assignment.sha256 and
         allocation_id == allocation.id and is_integer(expiry) and expiry >= 0 do
      :ok
    else
      {:error, :credential_lease_invalid}
    end
  end

  def validate_credential_lease(_lease, _allocation, _assignment), do: {:error, :credential_lease_invalid}

  @spec validate_checkout(term(), map(), map()) :: :ok | {:error, :checkout_intent_mismatch}
  def validate_checkout(receipt, assignment, intent) when is_map(receipt) do
    expected = Map.merge(intent, %{assignment_digest: assignment.sha256})
    exact_keys = MapSet.new(Map.keys(receipt)) == MapSet.new(Map.keys(expected) ++ [:head])

    if exact_keys and Enum.all?(expected, fn {key, value} -> Map.get(receipt, key) == value end) and
         valid_head?(Map.get(receipt, :head)),
       do: :ok,
       else: {:error, :checkout_intent_mismatch}
  end

  def validate_checkout(_receipt, _assignment, _intent), do: {:error, :checkout_intent_mismatch}

  @spec validate_result(term(), map()) :: :ok | {:error, :execution_result_invalid}
  def validate_result(
        %{assignment_digest: digest, outcome: outcome, summary: summary, evidence_ref: evidence_ref} = result,
        assignment
      )
      when map_size(result) == 5 and is_binary(digest) and outcome in @outcomes do
    if digest == assignment.sha256 and nonempty_text?(summary) and nonempty_text?(evidence_ref) and
         valid_head?(Map.get(result, :accepted_head)),
       do: :ok,
       else: {:error, :execution_result_invalid}
  end

  def validate_result(_result, _assignment), do: {:error, :execution_result_invalid}

  @spec validate_cleanup_evidence(term(), map(), map(), map()) :: :ok | {:error, :cleanup_evidence_invalid}
  def validate_cleanup_evidence(evidence, allocation, assignment, result) when is_map(evidence) do
    expected = %{
      contract_version: "work-package-cleanup-receipt.v1",
      receipt_kind: "repository_cleanup_verified",
      assignment_digest: assignment.sha256,
      issue_id: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      session_id: assignment.lease.session_id,
      process_id: assignment.lease.process_id,
      repository_ref: assignment.repository_ref,
      accepted_head: result.accepted_head,
      terminal_outcome: result.outcome,
      workspace_removed: true,
      credentials_revoked: true,
      reviewer_leases_released: true
    }

    exact_keys =
      MapSet.new(Map.keys(evidence)) ==
        MapSet.new(Map.keys(expected) ++ [:evidence_ref, :checksum, :signer_id, :signature, :allocation_id])

    valid = Enum.all?(expected, fn {key, value} -> Map.get(evidence, key) == value end)
    allocation_matches? = Map.get(evidence, :allocation_id) == Map.get(allocation, :id)
    signed = Enum.all?([:evidence_ref, :signer_id, :signature], &nonempty_text?(Map.get(evidence, &1)))
    checksum = Map.get(evidence, :checksum)
    checksum_valid? = is_binary(checksum) and Regex.match?(~r/\A[a-f0-9]{64}\z/, checksum)

    if exact_keys and valid and allocation_matches? and signed and checksum_valid?,
      do: :ok,
      else: {:error, :cleanup_evidence_invalid}
  end

  def validate_cleanup_evidence(_evidence, _allocation, _assignment, _result), do: {:error, :cleanup_evidence_invalid}

  @spec validate_pre_execution_result(term(), map(), atom()) :: :ok | {:error, :pre_execution_result_invalid}
  def validate_pre_execution_result(result, assignment, abort_reason) when is_map(result) do
    expected = %{
      assignment_digest: assignment.sha256,
      abort_reason: abort_reason,
      outcome: :blocked,
      summary: pre_execution_summary(abort_reason),
      evidence_ref: "managed-executor:#{assignment.sha256}:#{abort_reason}"
    }

    if MapSet.new(Map.keys(result)) == MapSet.new(Map.keys(expected)) and result == expected,
      do: :ok,
      else: {:error, :pre_execution_result_invalid}
  end

  def validate_pre_execution_result(_result, _assignment, _abort_reason),
    do: {:error, :pre_execution_result_invalid}

  @spec pre_execution_summary(atom()) :: String.t()
  def pre_execution_summary(:checkout_preparation_failed), do: "Checkout preparation failed before execution."
  def pre_execution_summary(:checkout_intent_mismatch), do: "Checkout intent or commit did not match the assignment."
  def pre_execution_summary(:credential_lease_denied), do: "The assignment credential lease was denied before execution."
  def pre_execution_summary(:credential_lease_expired), do: "The assignment credential lease expired before execution."
  def pre_execution_summary(_reason), do: "Assignment was blocked before execution."

  @spec nonempty_text?(term()) :: boolean()
  def nonempty_text?(value), do: is_binary(value) and String.valid?(value) and String.trim(value) != ""

  defp valid_head?(head),
    do: is_binary(head) and Regex.match?(~r/\A(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\z/, head)

  defp create_or_load(journal, key, assignment, context) do
    initial = %{schema_version: 2, key: key, assignment_digest: assignment.sha256, phase: :planned, version: 0, credential_lease: nil}

    case journal.compare_and_swap(key, 0, initial, context) do
      :ok -> {:ok, initial}
      {:error, :conflict} -> load_existing(journal, key, assignment, context)
      {:error, _reason} -> {:error, :lifecycle_journal_write_failed}
      _ -> {:error, :invalid_journal_write_response}
    end
  end

  defp load_existing(journal, key, assignment, context) do
    case journal.load(key, context) do
      {:ok, record} when is_map(record) -> verify(record, key, assignment)
      _ -> {:error, :journal_create_race_unresolved}
    end
  end

  defp verify(
         %{schema_version: 2, key: key, assignment_digest: digest, phase: phase, version: version} = record,
         key,
         assignment
       )
       when is_binary(digest) and phase in @phases and is_integer(version) and version >= 0 do
    cond do
      digest != assignment.sha256 -> {:error, :assignment_replay_mismatch}
      not valid_payload?(record, assignment) -> {:error, :invalid_lifecycle_journal}
      true -> {:ok, record}
    end
  end

  defp verify(_record, _key, _assignment), do: {:error, :invalid_lifecycle_journal}

  defp valid_payload?(record, assignment) do
    base_keys = [:schema_version, :key, :assignment_digest, :phase, :version, :credential_lease]

    valid_phase_keys?(record, base_keys) and valid_allocation_phase?(record) and
      valid_checkout_phase?(record, assignment) and valid_result_phase?(record, assignment) and
      valid_reported_phase?(record) and valid_cleanup_phase?(record, assignment) and
      valid_abort_phase?(record, assignment) and valid_credential_lease_phase?(record, assignment)
  end

  defp valid_phase_keys?(record, base_keys) do
    MapSet.new(Map.keys(record)) == MapSet.new(base_keys ++ Map.fetch!(@phase_fields, record.phase))
  end

  defp valid_allocation_phase?(%{phase: phase}) when phase in [:planned, :allocation_pending], do: true
  defp valid_allocation_phase?(record), do: validate_allocation(Map.get(record, :allocation)) == :ok

  defp valid_checkout_phase?(%{phase: phase}, _assignment) when phase not in @checkout_phases, do: true

  defp valid_checkout_phase?(record, assignment),
    do: validate_checkout(Map.get(record, :checkout), assignment, checkout_intent(assignment)) == :ok

  defp valid_result_phase?(%{phase: phase}, _assignment) when phase not in @result_phases, do: true

  defp valid_result_phase?(record, assignment),
    do: validate_result(Map.get(record, :execution_result), assignment) == :ok

  defp valid_reported_phase?(%{phase: phase}) when phase not in @reported_phases, do: true
  defp valid_reported_phase?(record), do: nonempty_text?(Map.get(record, :result_ref))

  defp valid_cleanup_phase?(%{phase: phase}, _assignment) when phase != :terminal, do: true

  defp valid_cleanup_phase?(record, assignment),
    do:
      validate_cleanup_evidence(
        Map.get(record, :cleanup_evidence),
        Map.get(record, :allocation),
        assignment,
        Map.get(record, :execution_result)
      ) == :ok

  defp valid_abort_phase?(%{phase: phase}, _assignment)
       when phase not in [:abort_pending, :abort_result_pending, :abort_cleanup_pending],
       do: true

  defp valid_abort_phase?(record, assignment) do
    abort_reason = Map.get(record, :abort_reason)

    abort_reason in @abort_reasons and validate_allocation(Map.get(record, :allocation)) == :ok and
      (record.phase == :abort_pending or
         (validate_pre_execution_result(Map.get(record, :pre_execution_result), assignment, abort_reason) == :ok and
            (record.phase == :abort_result_pending or nonempty_text?(Map.get(record, :abort_result_ref)))))
  end

  defp valid_credential_lease_phase?(%{phase: phase, credential_lease: lease}, _assignment)
       when phase in [:planned, :allocation_pending, :allocated, :checkout_pending, :checkout_ready, :credential_lease_pending, :terminal] do
    is_nil(lease)
  end

  defp valid_credential_lease_phase?(%{phase: phase, credential_lease: lease, allocation: allocation}, assignment)
       when phase in @lease_phases do
    validate_credential_lease(lease, allocation, assignment) == :ok
  end

  defp valid_credential_lease_phase?(%{phase: phase, credential_lease: nil}, _assignment)
       when phase in [:abort_pending, :abort_result_pending, :abort_cleanup_pending],
       do: true

  defp valid_credential_lease_phase?(%{phase: phase, credential_lease: lease, allocation: allocation}, assignment)
       when phase in [:abort_pending, :abort_result_pending, :abort_cleanup_pending],
       do: validate_credential_lease(lease, allocation, assignment) == :ok
end
