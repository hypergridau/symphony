defmodule SymphonyElixir.ManagedExecutor.Record do
  @moduledoc false

  @phases [
    :planned,
    :allocation_pending,
    :allocated,
    :checkout_pending,
    :checkout_outcome_unknown,
    :checkout_ready,
    :credential_lease_pending,
    :credential_lease_ready,
    :credential_request_revocation_pending,
    :execution_started,
    :result_recorded,
    :result_pending,
    :result_reported,
    :cleanup_pending,
    :terminal,
    :abort_pending,
    :abort_result_pending,
    :abort_cleanup_pending,
    :abort_cleanup_verified
  ]
  @phase_fields %{
    planned: [],
    allocation_pending: [],
    allocated: [:allocation],
    checkout_pending: [:allocation],
    checkout_outcome_unknown: [:allocation],
    checkout_ready: [:allocation, :checkout],
    credential_lease_pending: [:allocation],
    credential_lease_ready: [:allocation],
    credential_request_revocation_pending: [:allocation, :candidate_lease_operation],
    execution_started: [:allocation, :checkout],
    result_recorded: [:allocation, :checkout, :execution_result],
    result_pending: [:allocation, :checkout, :execution_result],
    result_reported: [:allocation, :checkout, :execution_result, :result_ref],
    cleanup_pending: [:allocation, :checkout, :execution_result, :result_ref],
    terminal: [:allocation, :checkout, :execution_result, :result_ref, :cleanup_evidence],
    abort_pending: [:allocation, :abort_reason, :checkout],
    abort_result_pending: [:allocation, :abort_reason, :checkout, :pre_execution_result],
    abort_cleanup_pending: [:allocation, :abort_reason, :checkout, :pre_execution_result, :abort_result_ref],
    abort_cleanup_verified: [
      :allocation,
      :abort_reason,
      :checkout,
      :pre_execution_result,
      :abort_result_ref,
      :abort_release_ack
    ]
  }
  @checkout_phases [
    :checkout_ready,
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
  @lease_phases [
    :credential_lease_ready,
    :checkout_pending,
    :checkout_outcome_unknown,
    :checkout_ready,
    :execution_started,
    :result_recorded,
    :result_pending,
    :result_reported,
    :cleanup_pending
  ]
  @abort_reasons [
    :checkout_preparation_failed,
    :checkout_intent_mismatch,
    :credential_lease_denied,
    :credential_lease_expired,
    :credential_lease_invalid
  ]

  @spec load_or_create(module(), String.t(), map(), map(), term()) :: {:ok, map()} | {:error, term()}
  def load_or_create(journal, key, assignment, claim_binding, context) do
    case journal.load(key, context) do
      {:ok, nil} -> create_or_load(journal, key, assignment, claim_binding, context)
      {:ok, record} -> verify(record, key, assignment, claim_binding)
      {:error, _reason} -> {:error, :lifecycle_journal_read_failed}
      _ -> {:error, :invalid_journal_response}
    end
  end

  @spec checkpoint(map(), atom(), module(), term(), map()) :: {:ok, map()} | {:error, term()}
  def checkpoint(record, phase, journal, context, attrs) do
    base_keys = [:schema_version, :key, :assignment_digest, :claim_binding, :phase, :version, :credential_lease]

    keys = base_keys ++ Map.fetch!(@phase_fields, phase)

    next =
      record
      |> Map.merge(attrs)
      |> Map.put(:phase, phase)
      |> Map.put(:version, record.version + 1)
      |> Map.take(keys)

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
    expected_keys = [:lease_ref, :assignment_digest, :allocation_id, :expires_at_ms]
    exact_keys = Enum.sort(Map.keys(lease)) == Enum.sort(expected_keys)

    if exact_keys and lease_ref == credential_lease_handle(assignment) and
         digest == assignment.sha256 and
         allocation_id == allocation.id and is_integer(expiry) and expiry >= 0 do
      :ok
    else
      {:error, :credential_lease_invalid}
    end
  end

  def validate_credential_lease(_lease, _allocation, _assignment), do: {:error, :credential_lease_invalid}

  @spec validate_credential_lease_response(term(), map(), map()) :: :ok | {:error, :credential_lease_invalid}
  def validate_credential_lease_response(
        %{assignment_digest: digest, allocation_id: allocation_id, expires_at_ms: expiry} = response,
        allocation,
        assignment
      ) do
    expected_keys = [:assignment_digest, :allocation_id, :expires_at_ms]
    exact_keys = Enum.sort(Map.keys(response)) == Enum.sort(expected_keys)

    if exact_keys and digest == assignment.sha256 and allocation_id == allocation.id and
         is_integer(expiry) and expiry >= 0 do
      :ok
    else
      {:error, :credential_lease_invalid}
    end
  end

  def validate_credential_lease_response(_response, _allocation, _assignment),
    do: {:error, :credential_lease_invalid}

  @spec credential_lease_handle(map()) :: String.t()
  def credential_lease_handle(assignment), do: "#{assignment.sha256}:credential-acquire"

  @spec credential_lease_from_response(map(), map()) :: map()
  def credential_lease_from_response(response, assignment) do
    %{
      lease_ref: credential_lease_handle(assignment),
      assignment_digest: response.assignment_digest,
      allocation_id: response.allocation_id,
      expires_at_ms: response.expires_at_ms
    }
  end

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

  @spec validate_abort_release_ack(term(), map(), map(), map(), String.t()) ::
          :ok | {:error, :abort_release_ack_invalid}
  def validate_abort_release_ack(ack, claim_binding, allocation, assignment, result_ref) when is_map(ack) do
    expected = %{
      projection_id: claim_binding.projection_id,
      reservation_id: claim_binding.reservation_id,
      receipt_kind: "pre_execution_abort_cleanup_verified",
      assignment_digest: assignment.sha256,
      allocation_id: allocation.id,
      abort_result_ref: result_ref,
      generation: assignment.lease.generation,
      execution_capacity_state: "released",
      scope_state: "released",
      reservation_state: "released"
    }

    exact_keys = MapSet.new(Map.keys(ack)) == MapSet.new(Map.keys(expected) ++ [:receipt_id, :evidence_ref])
    matches = Enum.all?(expected, fn {key, value} -> Map.get(ack, key) == value end)

    if exact_keys and matches and nonempty_text?(Map.get(ack, :receipt_id)) and
         nonempty_text?(Map.get(ack, :evidence_ref)),
       do: :ok,
       else: {:error, :abort_release_ack_invalid}
  end

  def validate_abort_release_ack(_ack, _claim_binding, _allocation, _assignment, _result_ref),
    do: {:error, :abort_release_ack_invalid}

  @spec pre_execution_summary(atom()) :: String.t()
  def pre_execution_summary(:checkout_preparation_failed), do: "Checkout preparation failed before execution."
  def pre_execution_summary(:checkout_intent_mismatch), do: "Checkout intent or commit did not match the assignment."
  def pre_execution_summary(:credential_lease_denied), do: "The assignment credential lease was denied before execution."
  def pre_execution_summary(:credential_lease_expired), do: "The assignment credential lease expired before execution."
  def pre_execution_summary(:credential_lease_invalid), do: "The assignment credential lease response was invalid before execution."
  def pre_execution_summary(_reason), do: "Assignment was blocked before execution."

  @spec nonempty_text?(term()) :: boolean()
  def nonempty_text?(value), do: is_binary(value) and String.valid?(value) and String.trim(value) != ""

  defp valid_head?(head),
    do: is_binary(head) and Regex.match?(~r/\A(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})\z/, head)

  defp create_or_load(journal, key, assignment, claim_binding, context) do
    initial = %{
      schema_version: 8,
      key: key,
      assignment_digest: assignment.sha256,
      claim_binding: claim_binding,
      phase: :planned,
      version: 0,
      credential_lease: nil
    }

    case journal.compare_and_swap(key, 0, initial, context) do
      :ok -> {:ok, initial}
      {:error, :conflict} -> load_existing(journal, key, assignment, claim_binding, context)
      {:error, _reason} -> {:error, :lifecycle_journal_write_failed}
      _ -> {:error, :invalid_journal_write_response}
    end
  end

  defp load_existing(journal, key, assignment, claim_binding, context) do
    case journal.load(key, context) do
      {:ok, record} when is_map(record) -> verify(record, key, assignment, claim_binding)
      _ -> {:error, :journal_create_race_unresolved}
    end
  end

  defp verify(
         %{
           schema_version: 8,
           key: key,
           assignment_digest: digest,
           claim_binding: binding,
           phase: phase,
           version: version
         } = record,
         key,
         assignment,
         claim_binding
       )
       when is_binary(digest) and phase in @phases and is_integer(version) and version >= 0 do
    cond do
      digest != assignment.sha256 -> {:error, :assignment_replay_mismatch}
      binding != claim_binding -> {:error, :provider_claim_replay_mismatch}
      not valid_payload?(record, assignment) -> {:error, :invalid_lifecycle_journal}
      true -> {:ok, record}
    end
  end

  defp verify(%{schema_version: 6, key: key, assignment_digest: digest, claim_binding: binding} = record, key, assignment, claim_binding) do
    cond do
      digest != assignment.sha256 -> {:error, :assignment_replay_mismatch}
      binding != claim_binding -> {:error, :provider_claim_replay_mismatch}
      Map.get(record, :phase) != :terminal -> {:error, :legacy_claim_requires_reconciliation}
      not is_integer(Map.get(record, :version)) or Map.get(record, :version) < 0 -> {:error, :invalid_lifecycle_journal}
      not valid_payload?(record, assignment) -> {:error, :invalid_lifecycle_journal}
      true -> {:ok, record}
    end
  end

  defp verify(%{schema_version: 7, key: key, assignment_digest: digest, claim_binding: binding} = record, key, assignment, claim_binding) do
    cond do
      digest != assignment.sha256 -> {:error, :assignment_replay_mismatch}
      binding != claim_binding -> {:error, :provider_claim_replay_mismatch}
      Map.get(record, :phase) != :terminal -> {:error, :legacy_claim_requires_reconciliation}
      not is_integer(Map.get(record, :version)) or Map.get(record, :version) < 0 -> {:error, :invalid_lifecycle_journal}
      not valid_payload?(record, assignment) -> {:error, :invalid_lifecycle_journal}
      true -> {:ok, record}
    end
  end

  defp verify(%{schema_version: 5, key: key, assignment_digest: digest} = record, key, assignment, _claim_binding) do
    if digest == assignment.sha256 and Map.get(record, :phase) == :terminal and
         is_integer(Map.get(record, :version)) and Map.get(record, :version) >= 0 and
         valid_payload?(record, assignment),
       do: {:ok, record},
       else: {:error, :legacy_claim_requires_reconciliation}
  end

  defp verify(_record, _key, _assignment, _claim_binding), do: {:error, :invalid_lifecycle_journal}

  defp valid_payload?(record, assignment) do
    base_keys = [:schema_version, :key, :assignment_digest, :phase, :version, :credential_lease]
    base_keys = if record.schema_version in [6, 7, 8], do: [:claim_binding | base_keys], else: base_keys

    valid_phase_keys?(record, base_keys) and valid_allocation_phase?(record) and
      valid_checkout_phase?(record, assignment) and valid_result_phase?(record, assignment) and
      valid_remaining_phases?(record, assignment)
  end

  defp valid_remaining_phases?(record, assignment) do
    valid_reported_phase?(record) and valid_cleanup_phase?(record, assignment) and
      valid_abort_phase?(record, assignment) and valid_credential_lease_phase?(record, assignment) and
      valid_lease_quarantine_phase?(record)
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
       when phase not in [:abort_pending, :abort_result_pending, :abort_cleanup_pending, :abort_cleanup_verified],
       do: true

  defp valid_abort_phase?(record, assignment) do
    abort_reason = Map.get(record, :abort_reason)
    checkout = Map.get(record, :checkout)

    checkout_matches_phase? = is_nil(checkout)

    abort_reason in @abort_reasons and validate_allocation(Map.get(record, :allocation)) == :ok and
      checkout_matches_phase? and
      (record.phase == :abort_pending or
         (validate_pre_execution_result(Map.get(record, :pre_execution_result), assignment, abort_reason) == :ok and
            (record.phase == :abort_result_pending or
               (nonempty_text?(Map.get(record, :abort_result_ref)) and
                  (record.phase == :abort_cleanup_pending or
                     validate_abort_release_ack(
                       Map.get(record, :abort_release_ack),
                       record.claim_binding,
                       record.allocation,
                       assignment,
                       record.abort_result_ref
                     ) == :ok)))))
  end

  defp valid_credential_lease_phase?(%{phase: phase, credential_lease: lease}, _assignment)
       when phase in [
              :planned,
              :allocation_pending,
              :allocated,
              :credential_lease_pending,
              :terminal,
              :abort_cleanup_verified
            ] do
    is_nil(lease)
  end

  defp valid_credential_lease_phase?(%{phase: phase, credential_lease: lease, allocation: allocation}, assignment)
       when phase in @lease_phases do
    validate_credential_lease(lease, allocation, assignment) == :ok
  end

  defp valid_credential_lease_phase?(
         %{phase: :credential_request_revocation_pending, credential_lease: nil, candidate_lease_operation: :acquire},
         _assignment
       ),
       do: true

  defp valid_credential_lease_phase?(
         %{
           phase: :credential_request_revocation_pending,
           credential_lease: lease,
           allocation: allocation,
           candidate_lease_operation: :renew
         },
         assignment
       ),
       do: validate_credential_lease(lease, allocation, assignment) == :ok

  defp valid_credential_lease_phase?(%{phase: phase, credential_lease: nil}, _assignment)
       when phase in [:abort_pending, :abort_result_pending, :abort_cleanup_pending],
       do: true

  defp valid_credential_lease_phase?(%{phase: phase, credential_lease: lease, allocation: allocation}, assignment)
       when phase in [:abort_pending, :abort_result_pending, :abort_cleanup_pending],
       do: validate_credential_lease(lease, allocation, assignment) == :ok

  defp valid_lease_quarantine_phase?(%{phase: :credential_request_revocation_pending, candidate_lease_operation: operation}),
    do: operation in [:acquire, :renew]

  defp valid_lease_quarantine_phase?(_record), do: true
end
