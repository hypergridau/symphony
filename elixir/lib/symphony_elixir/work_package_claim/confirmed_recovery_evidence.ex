defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence do
  @moduledoc """
  Verifies the root-signed HGS-740 proof needed before a confirmed claim may enter
  paused recovery. This module is a pure verifier; it performs no I/O or state
  transition.
  """

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliation
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryUnsubmittedPredecessor

  @contract_v1 "work-package-paused-confirmed-recovery.v1"
  @contract_v2 "work-package-paused-confirmed-recovery.v2"
  @contract_v3 "work-package-paused-confirmed-recovery.v3"
  @signature_domain_v1 "hypergrid-work-package-recovery:hgs740-confirmed-root.v1\0"
  @signature_domain_v2 "hypergrid-work-package-recovery:hgs740-confirmed-root.v2\0"
  @hgs485_fingerprint "903b66d70e23219ee947bdbbdd738b29851302a24985edd4a69abc8a2875d8e6"
  @pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)
  @claim_fields ~w(projectionId reservationId workspaceId companyId issueId runnerId managedProjectProfileId repositoryRef scopeKeys generation sessionId processId responsibleDelegationId executionFenceToken runtimeLeaseId nonceHash)
  @payload_fields_v1 ~w(contractVersion pool issueId generation reservationId assignmentSHA256 issuedAt expiresAt nonce observation providerHeld)
  @payload_fields_v2 ~w(assignmentSnapshotState contractVersion pool issueId generation reservationId assignmentSHA256 issuedAt expiresAt nonce observation providerHeld)
  @observation_fields ~w(expected localGenerationMax fenceSHA256 claimJournalSHA256 responsibilityGraphSHA256 globalPause runnerStopped neverSpawned supervisedWorkerAbsent processCount workspaceAbsent hostIdentity bootId observedAt witnesses witnessLogSHA256 dispatchPhase kubernetes predecessorRetirement serviceUnits witnessUnits turnsAbsent)
  @provider_fields ~w(observedAt sourceIdentity assignmentDigest expected projectionState mutationState reservationState executionCapacityState scopeState credentialLeaseInventory oauthSlotLeaseInventory)
  @credential_inventory_fields ~w(observedAt complete leaseIds readbacks)
  @oauth_slot_inventory_fields ~w(observedAt complete leaseCount leaseIds leases)
  @retirement_fields ~w(active_process evidence_ref generation issue_id linear_state local_claim provider_claim provider_projection_id retired_at_ms workspace type repository_ref managed_project_profile_id prior_accountable_id prior_responsible_id prior_accountable_digest prior_responsible_digest successor_accountable_id successor_responsible_id successor_accountable_digest successor_responsible_digest manifest_sha256 signer_key_sha256 observation_sha256)
  @execution_fields ~w(issue_id repository worker_host generation branch worktree status ownership leases terminal retirement cleanup cleanup_receipt termination_unconfirmed admitted_at_ms cleaned_at_ms)
  @lease_fields ~w(issue_id repository generation role session_id process_id branch worktree status registered_at_ms last_heartbeat_at linear_state pr_state head termination_required termination_confirmed_at_ms termination_evidence_ref termination_evidence supervisor_identity release_reason)
  @max_age_ms 60_000
  @clock_skew_ms 5_000

  @type bindings :: %{
          required(:pool) => String.t(),
          required(:issue_id) => String.t(),
          required(:generation) => pos_integer(),
          required(:reservation_id) => String.t(),
          required(:assignment_sha256) => String.t() | nil,
          required(:nonce) => String.t(),
          required(:fence_sha256) => String.t(),
          required(:claim_journal_sha256) => String.t(),
          required(:responsibility_graph_sha256) => String.t(),
          required(:now_ms) => non_neg_integer(),
          optional(:assignment_snapshot_state) => String.t()
        }

  @doc "Verifies the detached, domain-separated root signature and every claim-bound precondition."
  @spec verify(binary(), binary(), bindings()) :: {:ok, map()} | {:error, :invalid_confirmed_recovery_evidence}
  def verify(encoded, public_key, bindings)
      when is_binary(encoded) and is_binary(public_key) and is_map(bindings) do
    with :ok <- trusted_public_key(public_key),
         {:ok, payload} <- verify_envelope(encoded, public_key),
         :ok <- validate_payload(payload, bindings) do
      {:ok, payload}
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  rescue
    _ -> {:error, :invalid_confirmed_recovery_evidence}
  catch
    _kind, _reason -> {:error, :invalid_confirmed_recovery_evidence}
  end

  def verify(_encoded, _public_key, _bindings),
    do: {:error, :invalid_confirmed_recovery_evidence}

  if Mix.env() == :test do
    @doc false
    @spec verify_test_envelope(binary(), binary(), bindings()) ::
            {:ok, map()} | {:error, :invalid_confirmed_recovery_evidence}
    def verify_test_envelope(encoded, public_key, bindings)
        when is_binary(encoded) and is_binary(public_key) and is_map(bindings) do
      with {:ok, payload} <- verify_envelope(encoded, public_key),
           :ok <- validate_payload(payload, bindings) do
        {:ok, payload}
      else
        _ -> {:error, :invalid_confirmed_recovery_evidence}
      end
    rescue
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    catch
      _kind, _reason -> {:error, :invalid_confirmed_recovery_evidence}
    end

    def verify_test_envelope(_encoded, _public_key, _bindings),
      do: {:error, :invalid_confirmed_recovery_evidence}
  end

  defp verify_envelope(encoded, public_key) do
    with {:ok, envelope} <- decode_canonical_object(encoded),
         true <- exact_keys?(envelope, ~w(payload signature)),
         {:ok, payload_bytes} <- Base.url_decode64(envelope["payload"], padding: false),
         {:ok, signature} <- Base.url_decode64(envelope["signature"], padding: false),
         true <- byte_size(signature) == 64,
         {:ok, payload} <- decode_canonical_object(payload_bytes),
         true <- verify_signature(payload_bytes, signature, public_key, payload["contractVersion"]) do
      {:ok, payload}
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  @doc false
  @spec canonical_json(term()) :: binary()
  def canonical_json(value), do: encode_canonical_json(value)

  defp encode_canonical_json(value) when is_map(value) do
    entries =
      value
      |> Enum.sort_by(fn {key, _value} -> key end)
      |> Enum.map(fn {key, child} -> [Jason.encode!(key), ":", encode_canonical_json(child)] end)

    ["{", Enum.intersperse(entries, ","), "}"] |> IO.iodata_to_binary()
  end

  defp encode_canonical_json(value) when is_list(value) do
    ["[", value |> Enum.map(&encode_canonical_json/1) |> Enum.intersperse(","), "]"]
    |> IO.iodata_to_binary()
  end

  defp encode_canonical_json(value), do: Jason.encode!(value)

  @doc false
  @spec signature_message(binary()) :: binary()
  def signature_message(payload) when is_binary(payload), do: @signature_domain_v1 <> payload

  @doc false
  @spec signature_message(binary(), String.t()) :: binary() | nil
  def signature_message(payload, @contract_v1) when is_binary(payload), do: @signature_domain_v1 <> payload
  def signature_message(payload, @contract_v2) when is_binary(payload), do: @signature_domain_v2 <> payload
  def signature_message(payload, @contract_v3) when is_binary(payload), do: "hypergrid-work-package-recovery:hgs740-confirmed-root.v3\0" <> payload
  def signature_message(_payload, _contract), do: nil

  @doc false
  @spec validate_payload(map(), bindings()) :: :ok | {:error, :invalid_confirmed_recovery_evidence}
  def validate_payload(payload, bindings) when is_map(payload) and is_map(bindings) do
    with :ok <- validate_contract(payload),
         :ok <- valid_bindings(payload, bindings),
         {:ok, issued_at_ms} <- timestamp_ms(payload["issuedAt"]),
         {:ok, expires_at_ms} <- timestamp_ms(payload["expiresAt"]),
         true <- expires_at_ms > issued_at_ms and expires_at_ms - issued_at_ms <= @max_age_ms,
         true <- issued_at_ms <= bindings.now_ms + @clock_skew_ms and expires_at_ms >= bindings.now_ms,
         true <- uuid?(payload["nonce"]),
         :ok <- validate_observation(payload["observation"], payload, bindings),
         :ok <- validate_provider_readback(payload["providerHeld"], payload, bindings),
         :ok <- reconciliation_provider_binding(payload),
         {:ok, observation_at_ms} <- timestamp_ms(payload["observation"]["observedAt"]),
         {:ok, provider_at_ms} <- timestamp_ms(payload["providerHeld"]["observedAt"]),
         {:ok, issued_at_ms} <- timestamp_ms(payload["issuedAt"]),
         true <- issued_at_ms >= observation_at_ms and issued_at_ms >= provider_at_ms do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  rescue
    _ -> {:error, :invalid_confirmed_recovery_evidence}
  catch
    _kind, _reason -> {:error, :invalid_confirmed_recovery_evidence}
  end

  def validate_payload(_payload, _bindings),
    do: {:error, :invalid_confirmed_recovery_evidence}

  defp reconciliation_provider_binding(%{"observation" => %{"reconciliation" => metadata}} = payload) do
    hash = :crypto.hash(:sha256, canonical_json(payload["providerHeld"])) |> Base.encode16(case: :lower)
    if hash == metadata["providerHeldSHA256"], do: :ok, else: {:error, :invalid_confirmed_recovery_evidence}
  end

  defp reconciliation_provider_binding(_payload), do: :ok

  defp validate_contract(%{"contractVersion" => @contract_v1} = payload) do
    if exact_keys?(payload, @payload_fields_v1), do: :ok, else: {:error, :invalid_confirmed_recovery_evidence}
  end

  defp validate_contract(%{"contractVersion" => @contract_v2, "assignmentSnapshotState" => "absent"} = payload) do
    if exact_keys?(payload, @payload_fields_v2) and is_nil(payload["assignmentSHA256"]),
      do: :ok,
      else: {:error, :invalid_confirmed_recovery_evidence}
  end

  defp validate_contract(%{"contractVersion" => @contract_v3, "assignmentSnapshotState" => "absent"} = payload) do
    if exact_keys?(payload, @payload_fields_v2) and is_nil(payload["assignmentSHA256"]),
      do: :ok,
      else: {:error, :invalid_confirmed_recovery_evidence}
  end

  defp validate_contract(_payload), do: {:error, :invalid_confirmed_recovery_evidence}

  defp valid_bindings(payload, bindings) do
    expected_binding_keys =
      if payload["contractVersion"] in [@contract_v2, @contract_v3],
        do: ~w(assignment_sha256 assignment_snapshot_state claim_journal_sha256 fence_sha256 generation issue_id now_ms nonce pool reservation_id responsibility_graph_sha256)a,
        else: ~w(assignment_sha256 claim_journal_sha256 fence_sha256 generation issue_id now_ms nonce pool reservation_id responsibility_graph_sha256)a

    with true <- Enum.sort(Map.keys(bindings)) == Enum.sort(expected_binding_keys),
         true <- payload["pool"] in @pools,
         true <- payload["pool"] == bindings.pool,
         true <- payload["issueId"] == bindings.issue_id and uuid?(payload["issueId"]),
         true <- payload["generation"] == 2 and payload["generation"] == bindings.generation,
         true <- payload["reservationId"] == bindings.reservation_id and text?(payload["reservationId"]),
         :ok <- valid_assignment_binding(payload, bindings),
         true <- uuid?(payload["nonce"]) and payload["nonce"] == bindings.nonce,
         true <- is_integer(bindings.now_ms) and bindings.now_ms >= 0 do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp valid_assignment_binding(%{"contractVersion" => @contract_v1} = payload, bindings) do
    if digest?(payload["assignmentSHA256"]) and payload["assignmentSHA256"] == bindings.assignment_sha256,
      do: :ok,
      else: {:error, :invalid_confirmed_recovery_evidence}
  end

  defp valid_assignment_binding(%{"contractVersion" => version, "assignmentSnapshotState" => "absent"} = payload, bindings) when version in [@contract_v2, @contract_v3] do
    if is_nil(payload["assignmentSHA256"]) and is_nil(bindings.assignment_sha256) and
         bindings.assignment_snapshot_state == "absent",
       do: :ok,
       else: {:error, :invalid_confirmed_recovery_evidence}
  end

  defp valid_assignment_binding(_payload, _bindings), do: {:error, :invalid_confirmed_recovery_evidence}

  defp validate_observation(observation, payload, bindings) do
    with true <- is_map(observation),
         :ok <- observation_shape(observation, payload["contractVersion"]),
         true <- observation["dispatchPhase"] == "confirmed" and observation["localGenerationMax"] == 2,
         true <- Enum.all?(~w(globalPause runnerStopped neverSpawned supervisedWorkerAbsent workspaceAbsent turnsAbsent), &(observation[&1] == true)),
         true <- observation["processCount"] == 0,
         true <- observation["fenceSHA256"] == bindings.fence_sha256,
         true <- observation["claimJournalSHA256"] == bindings.claim_journal_sha256,
         true <- observation["responsibilityGraphSHA256"] == bindings.responsibility_graph_sha256,
         true <- Enum.all?([bindings.fence_sha256, bindings.claim_journal_sha256, bindings.responsibility_graph_sha256], &digest?/1),
         true <- text?(observation["hostIdentity"]) and text?(observation["bootId"]),
         {:ok, observed_at_ms} <- timestamp_ms(observation["observedAt"]),
         true <- fresh?(observed_at_ms, bindings.now_ms),
         :ok <- validate_expected_claim(observation["expected"], payload, bindings),
         :ok <- validate_service_units(observation["serviceUnits"], observation["witnessUnits"]),
         :ok <- validate_witness_history(observation["witnesses"], observation["witnessLogSHA256"], payload["contractVersion"]),
         :ok <- validate_kubernetes(observation["kubernetes"], payload, bindings),
         :ok <- validate_predecessor(observation["predecessorRetirement"], payload, observation["expected"]) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp observation_shape(%{"reconciliation" => _metadata} = observation, @contract_v3) do
    if exact_keys?(observation, ["reconciliation" | @observation_fields]),
      do: ConfirmedRecoveryReconciliation.validate(observation),
      else: {:error, :invalid_confirmed_recovery_evidence}
  end

  defp observation_shape(observation, _version) do
    if exact_keys?(observation, @observation_fields), do: :ok, else: {:error, :invalid_confirmed_recovery_evidence}
  end

  defp validate_expected_claim(claim, payload, bindings) do
    with true <- is_map(claim) and exact_keys?(claim, @claim_fields),
         true <- claim["issueId"] == payload["issueId"] and claim["generation"] == 2,
         true <- claim["reservationId"] == payload["reservationId"],
         true <- claim["repositoryRef"] == payload["observation"]["kubernetes"]["claim"]["repositoryRef"],
         true <- claim["managedProjectProfileId"] == payload["providerHeld"]["expected"]["managedProjectProfileId"],
         true <- valid_scope_keys?(claim["scopeKeys"]),
         true <- claim["executionFenceToken"] == "#{claim["issueId"]}:2" and claim["runtimeLeaseId"] == claim["sessionId"],
         true <- digest?(claim["nonceHash"]),
         true <- claim["runnerId"] == payload["providerHeld"]["expected"]["runnerId"],
         true <- bindings.pool in @pools do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp validate_service_units(service_units, witness_units) do
    expected_witness_units = ["dahlia-claim-witness.service", "dahlia-claim-witness.socket"]

    with true <- exact_map_keys?(service_units, @pools),
         true <- exact_map_keys?(witness_units, expected_witness_units),
         true <- Enum.all?(@pools, &valid_service_unit?(service_units[&1], &1)),
         true <- Enum.all?(expected_witness_units, &valid_witness_unit?(witness_units[&1])) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp valid_service_unit?(unit, pool) do
    is_map(unit) and exact_keys?(unit, ~w(unit mainPID activeState masked cgroupProcessCount)) and
      unit["unit"] == "dahlia-symphony@#{pool}.service" and unit["mainPID"] == 0 and
      unit["activeState"] in ["inactive", "failed"] and unit["masked"] == true and
      unit["cgroupProcessCount"] == 0
  end

  defp valid_witness_unit?(unit) do
    is_map(unit) and exact_keys?(unit, ~w(activeState masked)) and
      unit["activeState"] == "inactive" and unit["masked"] == true
  end

  defp exact_map_keys?(value, expected) do
    is_map(value) and Enum.sort(Map.keys(value)) == Enum.sort(expected)
  end

  defp validate_witness_history(witnesses, log_hashes, version) do
    generations = witness_generations(version)

    with true <- is_list(witnesses) and length(witnesses) == length(generations),
         true <- Enum.map(witnesses, &Map.get(&1, "generation")) == generations,
         true <-
           Enum.all?(witnesses, fn row ->
             is_map(row) and exact_keys?(row, ~w(generation sequence hash source acceptedBuildReceiptSHA256)) and
               is_integer(row["sequence"]) and row["sequence"] > 0 and digest?(row["hash"]) and
               digest?(row["acceptedBuildReceiptSHA256"]) and valid_source?(row["source"])
           end),
         true <- is_map(log_hashes) and Enum.sort(Map.keys(log_hashes)) == Enum.sort(@pools),
         true <- Enum.all?(Map.values(log_hashes), &digest?/1) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp witness_generations(@contract_v3), do: [2]
  defp witness_generations(_version), do: [1, 2]

  defp validate_kubernetes(kube, payload, bindings) do
    with true <- is_map(kube) and exact_keys?(kube, ~w(observedAt cluster namespace claim jobs pods)),
         {:ok, kube_at_ms} <- timestamp_ms(kube["observedAt"]),
         true <- fresh?(kube_at_ms, bindings.now_ms),
         true <- kube["namespace"] == "frigga",
         true <- is_map(kube["cluster"]) and exact_keys?(kube["cluster"], ~w(apiServer caSha256)),
         true <- uri?(kube["cluster"]["apiServer"]) and digest?(kube["cluster"]["caSha256"]),
         true <- is_map(kube["claim"]) and exact_keys?(kube["claim"], ~w(issueId generation repositoryRef reservationId assignmentSHA256)),
         true <- kube["claim"]["issueId"] == payload["issueId"] and kube["claim"]["generation"] == 2,
         true <- kube["claim"]["reservationId"] == payload["reservationId"],
         true <- kube["claim"]["assignmentSHA256"] == payload["assignmentSHA256"],
         true <- kube["claim"]["repositoryRef"] == payload["observation"]["expected"]["repositoryRef"],
         :ok <- validate_absent_snapshot(kube["jobs"]),
         :ok <- validate_absent_snapshot(kube["pods"]) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp validate_absent_snapshot(snapshot) do
    if is_map(snapshot) and exact_keys?(snapshot, ~w(resourceVersion sha256 complete itemCount claimAbsent)) and
         text?(snapshot["resourceVersion"]) and digest?(snapshot["sha256"]) and
         snapshot["complete"] == true and is_integer(snapshot["itemCount"]) and snapshot["itemCount"] >= 0 and
         snapshot["claimAbsent"] == true do
      :ok
    else
      {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp validate_predecessor(predecessor, %{"contractVersion" => @contract_v3}, current_claim),
    do: ConfirmedRecoveryUnsubmittedPredecessor.validate(predecessor, current_claim)

  defp validate_predecessor(%{"execution" => execution, "claim" => claim, "receipt" => receipt} = predecessor, payload, current_claim)
       when map_size(predecessor) == 3 do
    with true <- exact_keys?(execution, @execution_fields),
         true <- exact_keys?(claim, @claim_fields),
         true <- exact_keys?(receipt, @retirement_fields),
         true <- predecessor_execution_matches?(execution, receipt, payload, current_claim),
         true <- predecessor_claim_matches?(claim, receipt, payload, current_claim),
         true <- predecessor_receipt_matches?(receipt, claim, payload, current_claim),
         true <- retired_execution_lease?(execution, claim) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp validate_predecessor(_predecessor, _payload, _current_claim),
    do: {:error, :invalid_confirmed_recovery_evidence}

  defp predecessor_execution_matches?(execution, receipt, payload, current_claim) do
    predecessor_execution_identity?(execution, payload, current_claim) and
      predecessor_execution_retired?(execution) and execution["retirement"] == receipt
  end

  defp predecessor_execution_identity?(execution, payload, current_claim) do
    execution["issue_id"] == payload["issueId"] and execution["generation"] == 1 and
      execution["repository"] == current_claim["repositoryRef"]
  end

  defp predecessor_execution_retired?(execution) do
    execution["status"] == "retired" and execution["cleanup"] == "cleaned" and
      execution["ownership"] in ["reconciled", "unknown"] and is_nil(execution["terminal"]) and
      is_nil(execution["cleanup_receipt"]) and execution["termination_unconfirmed"] == false
  end

  defp predecessor_claim_matches?(claim, receipt, payload, current_claim) do
    predecessor_claim_identity?(claim, payload, current_claim) and
      predecessor_claim_delegation?(claim, receipt) and
      claim["executionFenceToken"] == "#{payload["issueId"]}:1" and
      claim["runtimeLeaseId"] == claim["sessionId"] and digest?(claim["nonceHash"])
  end

  defp predecessor_claim_identity?(claim, payload, current_claim) do
    claim["issueId"] == payload["issueId"] and claim["generation"] == 1 and
      claim["repositoryRef"] == current_claim["repositoryRef"] and
      claim["managedProjectProfileId"] == current_claim["managedProjectProfileId"] and
      claim["runnerId"] == current_claim["runnerId"] and claim["workspaceId"] == current_claim["workspaceId"] and
      claim["companyId"] == current_claim["companyId"]
  end

  defp predecessor_claim_delegation?(claim, receipt) do
    claim["responsibleDelegationId"] == receipt["prior_responsible_id"]
  end

  defp predecessor_receipt_matches?(receipt, claim, payload, current_claim) do
    predecessor_receipt_identity?(receipt, claim, payload, current_claim) and
      predecessor_receipt_absence?(receipt) and predecessor_receipt_hashes?(receipt) and
      predecessor_receipt_delegations?(receipt, claim, current_claim)
  end

  defp predecessor_receipt_identity?(receipt, claim, payload, current_claim) do
    receipt["type"] == "unsubmitted_successor" and receipt["issue_id"] == payload["issueId"] and
      receipt["generation"] == 1 and receipt["repository_ref"] == current_claim["repositoryRef"] and
      receipt["managed_project_profile_id"] == current_claim["managedProjectProfileId"] and
      receipt["provider_projection_id"] == claim["projectionId"] and text?(receipt["provider_projection_id"]) and
      is_integer(receipt["retired_at_ms"]) and receipt["retired_at_ms"] > 0
  end

  defp predecessor_receipt_absence?(receipt) do
    receipt["active_process"] == "absent" and receipt["local_claim"] == "absent" and
      receipt["provider_claim"] == "absent" and receipt["workspace"] == "absent"
  end

  defp predecessor_receipt_hashes?(receipt) do
    digest_fields =
      ~w(prior_accountable_digest prior_responsible_digest successor_accountable_digest successor_responsible_digest manifest_sha256 signer_key_sha256 observation_sha256)

    Enum.all?(digest_fields, &digest?(receipt[&1])) and
      retirement_evidence_ref(receipt) == receipt["evidence_ref"] and text?(receipt["linear_state"])
  end

  defp predecessor_receipt_delegations?(receipt, claim, current_claim) do
    receipt["successor_responsible_id"] == current_claim["responsibleDelegationId"] and
      receipt["prior_responsible_id"] == claim["responsibleDelegationId"] and
      receipt["prior_accountable_id"] != receipt["prior_responsible_id"] and
      receipt["successor_accountable_id"] != receipt["successor_responsible_id"] and
      receipt["prior_accountable_id"] != receipt["successor_accountable_id"] and
      receipt["prior_responsible_id"] != receipt["successor_responsible_id"]
  end

  defp retired_execution_lease?(execution, claim) do
    case Map.to_list(execution["leases"]) do
      [{session_id, lease}] ->
        session_id == claim["sessionId"] and exact_keys?(lease, @lease_fields) and
          retired_lease_identity?(lease, claim, execution) and retired_lease_state?(lease) and
          retired_lease_unobserved?(lease)

      _ ->
        false
    end
  end

  defp retired_lease_identity?(lease, claim, execution) do
    lease["issue_id"] == claim["issueId"] and lease["generation"] == 1 and
      lease["repository"] == claim["repositoryRef"] and lease["role"] == "worker" and
      lease["session_id"] == claim["sessionId"] and lease["process_id"] == claim["processId"] and
      lease["branch"] == execution["branch"] and lease["worktree"] == execution["worktree"]
  end

  defp retired_lease_state?(lease) do
    lease["status"] == "released" and lease["release_reason"] == "claim_not_submitted" and
      lease["termination_required"] == false and is_nil(lease["termination_confirmed_at_ms"])
  end

  defp retired_lease_unobserved?(lease) do
    lease["head"] == "unobserved" and lease["last_heartbeat_at"] == 0 and
      is_nil(lease["termination_evidence_ref"]) and is_nil(lease["termination_evidence"])
  end

  defp validate_provider_readback(readback, payload, bindings) do
    with true <- is_map(readback) and exact_keys?(readback, @provider_fields),
         true <- readback["sourceIdentity"] == "provider-core:postgres" and readback["mutationState"] == "applied",
         true <-
           readback["projectionState"] == "active" and readback["reservationState"] == "claimed" and
             readback["executionCapacityState"] == "held" and readback["scopeState"] == "held",
         true <- digest?(readback["assignmentDigest"]) and readback["assignmentDigest"] == tuple_digest(readback["expected"]),
         true <- readback["assignmentDigest"] == tuple_digest(payload["observation"]["expected"]),
         true <- readback["expected"] == payload["observation"]["expected"],
         true <-
           readback["expected"]["issueId"] == bindings.issue_id and
             readback["expected"]["generation"] == 2 and readback["expected"]["reservationId"] == bindings.reservation_id,
         {:ok, observed_at_ms} <- timestamp_ms(readback["observedAt"]),
         true <- fresh?(observed_at_ms, bindings.now_ms),
         {:ok, observation_at_ms} <- timestamp_ms(payload["observation"]["observedAt"]),
         true <- observed_at_ms >= observation_at_ms,
         :ok <- validate_credential_lease_inventory(readback["credentialLeaseInventory"], observed_at_ms, bindings),
         :ok <- validate_oauth_slot_lease_inventory(readback["oauthSlotLeaseInventory"], observed_at_ms, bindings) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp validate_credential_lease_inventory(inventory, held_at_ms, bindings) do
    with true <- is_map(inventory) and exact_keys?(inventory, @credential_inventory_fields),
         true <- inventory["complete"] == true and inventory["leaseIds"] == [] and inventory["readbacks"] == [],
         {:ok, observed_at_ms} <- timestamp_ms(inventory["observedAt"]),
         true <- fresh?(observed_at_ms, bindings.now_ms) and observed_at_ms == held_at_ms do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp validate_oauth_slot_lease_inventory(inventory, held_at_ms, bindings) do
    with true <- is_map(inventory) and exact_keys?(inventory, @oauth_slot_inventory_fields),
         true <- inventory["complete"] == true and inventory["leaseCount"] == 0,
         true <- inventory["leaseIds"] == [] and inventory["leases"] == [],
         {:ok, observed_at_ms} <- timestamp_ms(inventory["observedAt"]),
         true <- fresh?(observed_at_ms, bindings.now_ms) and observed_at_ms == held_at_ms do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  @doc false
  @spec tuple_digest(map()) :: String.t() | nil
  def tuple_digest(claim) when is_map(claim) do
    with true <- exact_keys?(claim, @claim_fields),
         true <- valid_scope_keys?(claim["scopeKeys"]),
         {:ok, encoded_fields} <- encode_ordered_claim(claim) do
      :crypto.hash(:sha256, encoded_fields) |> Base.encode16(case: :lower)
    else
      _ -> nil
    end
  end

  def tuple_digest(_claim), do: nil

  @doc false
  @spec retirement_evidence_ref(map()) :: String.t() | nil
  def retirement_evidence_ref(receipt) when is_map(receipt) do
    fields = [
      "manifest_sha256",
      "signer_key_sha256",
      "observation_sha256",
      "prior_accountable_digest",
      "prior_responsible_digest",
      "successor_accountable_id",
      "successor_responsible_id",
      "successor_accountable_digest",
      "successor_responsible_digest"
    ]

    with true <- Enum.all?(fields, &Map.has_key?(receipt, &1)),
         true <- Enum.all?(Enum.take(fields, 5) ++ Enum.drop(fields, 7), &digest?(receipt[&1])),
         true <- text?(receipt["successor_accountable_id"]) and text?(receipt["successor_responsible_id"]) do
      tuple =
        {receipt["manifest_sha256"], receipt["signer_key_sha256"], receipt["observation_sha256"], receipt["prior_accountable_digest"], receipt["prior_responsible_digest"],
         receipt["successor_accountable_id"], receipt["successor_responsible_id"], receipt["successor_accountable_digest"], receipt["successor_responsible_digest"]}

      :crypto.hash(:sha256, :erlang.term_to_binary(tuple, [:deterministic]))
      |> Base.encode16(case: :lower)
    else
      _ -> nil
    end
  end

  def retirement_evidence_ref(_receipt), do: nil

  defp encode_ordered_claim(claim) do
    fields = [
      "projectionId",
      "reservationId",
      "workspaceId",
      "companyId",
      "issueId",
      "runnerId",
      "managedProjectProfileId",
      "repositoryRef",
      "scopeKeys",
      "generation",
      "sessionId",
      "processId",
      "responsibleDelegationId",
      "executionFenceToken",
      "runtimeLeaseId",
      "nonceHash"
    ]

    values = Map.put(claim, "scopeKeys", Enum.sort(claim["scopeKeys"]))

    fields
    |> Enum.reduce_while({:ok, []}, fn field, {:ok, chunks} ->
      case Jason.encode(Map.fetch!(values, field)) do
        {:ok, encoded} -> {:cont, {:ok, [chunks, Jason.encode!(field), ":", encoded, ","]}}
        _ -> {:halt, {:error, :invalid_claim}}
      end
    end)
    |> case do
      {:ok, chunks} ->
        encoded = IO.iodata_to_binary(chunks)
        {:ok, ["{", String.trim_trailing(encoded, ","), "}"] |> IO.iodata_to_binary()}

      error ->
        error
    end
  end

  defp valid_source?(source) do
    is_map(source) and exact_keys?(source, ~w(sourceHead executableSHA256 wrapperSHA256 attestationSHA256)) and
      is_binary(source["sourceHead"]) and Regex.match?(~r/\A[0-9a-f]{40}\z/, source["sourceHead"]) and
      Enum.all?(~w(executableSHA256 wrapperSHA256 attestationSHA256), &digest?(source[&1]))
  end

  defp trusted_public_key(key) when byte_size(key) == 32 do
    der = <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>> <> key
    fingerprint = Base.encode64(der) |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
    if fingerprint == @hgs485_fingerprint, do: :ok, else: {:error, :untrusted_recovery_key}
  end

  defp trusted_public_key(_key), do: {:error, :untrusted_recovery_key}

  defp verify_signature(payload, signature, key, contract) do
    case signature_message(payload, contract) do
      message when is_binary(message) -> :crypto.verify(:eddsa, :none, message, signature, [key, :ed25519])
      _ -> false
    end
  rescue
    _ -> false
  end

  defp decode_canonical_object(bytes) do
    with {:ok, value} when is_map(value) <- Jason.decode(bytes),
         canonical <- canonical_json(value),
         true <- canonical == bytes do
      {:ok, value}
    else
      _ -> {:error, :invalid_canonical_json}
    end
  end

  defp exact_keys?(value, expected) when is_map(value), do: Map.keys(value) |> Enum.sort() == Enum.sort(expected)
  defp exact_keys?(_value, _expected), do: false

  defp timestamp_ms(value) when is_binary(value) do
    case DateTime.from_iso8601(value) do
      {:ok, datetime, 0} -> {:ok, DateTime.to_unix(datetime, :millisecond)}
      _ -> {:error, :invalid_timestamp}
    end
  end

  defp timestamp_ms(_value), do: {:error, :invalid_timestamp}

  defp fresh?(observed_at_ms, now_ms),
    do: observed_at_ms <= now_ms + @clock_skew_ms and now_ms - observed_at_ms <= @max_age_ms

  defp valid_scope_keys?(keys) when is_list(keys) and length(keys) in 1..64 do
    Enum.all?(keys, &(text?(&1) and Regex.match?(~r/\A[\x20-\x7E]+\z/, &1))) and Enum.uniq(keys) == keys
  end

  defp valid_scope_keys?(_keys), do: false

  defp uri?(value) when is_binary(value) do
    case URI.parse(value) do
      %URI{scheme: "https", host: host} when is_binary(host) and host != "" -> true
      _ -> false
    end
  end

  defp uri?(_value), do: false

  defp uuid?(value),
    do: is_binary(value) and Regex.match?(~r/\A[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\z/, value)

  defp digest?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp text?(value), do: is_binary(value) and value != ""
end
