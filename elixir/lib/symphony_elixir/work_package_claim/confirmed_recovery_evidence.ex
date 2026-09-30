defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence do
  @moduledoc """
  Verifies the root-signed HGS-740 proof needed before a confirmed claim may enter
  paused recovery. This module is a pure verifier; it performs no I/O or state
  transition.
  """

  @contract "work-package-paused-confirmed-recovery.v1"
  @signature_domain "hypergrid-work-package-recovery:hgs740-confirmed-root.v1\0"
  @hgs485_fingerprint "903b66d70e23219ee947bdbbdd738b29851302a24985edd4a69abc8a2875d8e6"
  @pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)
  @claim_fields ~w(projectionId reservationId workspaceId companyId issueId runnerId managedProjectProfileId repositoryRef scopeKeys generation sessionId processId responsibleDelegationId executionFenceToken runtimeLeaseId nonceHash)
  @payload_fields ~w(contractVersion pool issueId generation reservationId assignmentSHA256 issuedAt expiresAt nonce observation providerHeld)
  @observation_fields ~w(expected localGenerationMax fenceSHA256 claimJournalSHA256 globalPause runnerStopped neverSpawned supervisedWorkerAbsent processCount workspaceAbsent hostIdentity bootId observedAt witnesses witnessLogSHA256 dispatchPhase kubernetes predecessorRetirement serviceUnits witnessUnits turnsAbsent)
  @provider_fields ~w(observedAt sourceIdentity assignmentDigest expected projectionState mutationState reservationState executionCapacityState scopeState)
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
          required(:assignment_sha256) => String.t(),
          required(:now_ms) => non_neg_integer()
        }

  @doc "Verifies the detached, domain-separated root signature and every claim-bound precondition."
  @spec verify(binary(), binary(), bindings()) :: {:ok, map()} | {:error, :invalid_confirmed_recovery_evidence}
  def verify(encoded, public_key, bindings)
      when is_binary(encoded) and is_binary(public_key) and is_map(bindings) do
    with :ok <- trusted_public_key(public_key),
         {:ok, envelope} <- decode_canonical_object(encoded),
         true <- exact_keys?(envelope, ~w(payload signature)),
         {:ok, payload_bytes} <- Base.url_decode64(envelope["payload"], padding: false),
         {:ok, signature} <- Base.url_decode64(envelope["signature"], padding: false),
         true <- byte_size(signature) == 64,
         true <- verify_signature(payload_bytes, signature, public_key),
         {:ok, payload} <- decode_canonical_object(payload_bytes),
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

  @doc false
  @spec signature_message(binary()) :: binary()
  def signature_message(payload) when is_binary(payload), do: @signature_domain <> payload

  @doc false
  @spec validate_payload(map(), bindings()) :: :ok | {:error, :invalid_confirmed_recovery_evidence}
  def validate_payload(payload, bindings) when is_map(payload) and is_map(bindings) do
    with true <- exact_keys?(payload, @payload_fields),
         true <- payload["contractVersion"] == @contract,
         :ok <- valid_bindings(payload, bindings),
         {:ok, issued_at_ms} <- timestamp_ms(payload["issuedAt"]),
         {:ok, expires_at_ms} <- timestamp_ms(payload["expiresAt"]),
         true <- expires_at_ms > issued_at_ms and expires_at_ms - issued_at_ms <= @max_age_ms,
         true <- issued_at_ms <= bindings.now_ms + @clock_skew_ms and expires_at_ms >= bindings.now_ms,
         true <- uuid?(payload["nonce"]),
         :ok <- validate_observation(payload["observation"], payload, bindings),
         :ok <- validate_provider_readback(payload["providerHeld"], payload, bindings),
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

  defp valid_bindings(payload, bindings) do
    with true <- Enum.sort(Map.keys(bindings)) == Enum.sort(~w(assignment_sha256 generation issue_id now_ms pool reservation_id)a),
         true <- payload["pool"] in @pools,
         true <- payload["pool"] == bindings.pool,
         true <- payload["issueId"] == bindings.issue_id and uuid?(payload["issueId"]),
         true <- payload["generation"] == 2 and payload["generation"] == bindings.generation,
         true <- payload["reservationId"] == bindings.reservation_id and text?(payload["reservationId"]),
         true <- digest?(payload["assignmentSHA256"]) and payload["assignmentSHA256"] == bindings.assignment_sha256,
         true <- is_integer(bindings.now_ms) and bindings.now_ms >= 0 do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp validate_observation(observation, payload, bindings) do
    with true <- is_map(observation) and exact_keys?(observation, @observation_fields),
         true <- observation["dispatchPhase"] == "confirmed" and observation["localGenerationMax"] == 2,
         true <- Enum.all?(~w(globalPause runnerStopped neverSpawned supervisedWorkerAbsent workspaceAbsent turnsAbsent), &(observation[&1] == true)),
         true <- observation["processCount"] == 0,
         true <- digest?(observation["fenceSHA256"]) and digest?(observation["claimJournalSHA256"]),
         true <- text?(observation["hostIdentity"]) and text?(observation["bootId"]),
         {:ok, observed_at_ms} <- timestamp_ms(observation["observedAt"]),
         true <- fresh?(observed_at_ms, bindings.now_ms),
         :ok <- validate_expected_claim(observation["expected"], payload, bindings),
         :ok <- validate_service_units(observation["serviceUnits"], observation["witnessUnits"]),
         :ok <- validate_witness_history(observation["witnesses"], observation["witnessLogSHA256"]),
         :ok <- validate_kubernetes(observation["kubernetes"], payload, bindings),
         :ok <- validate_predecessor(observation["predecessorRetirement"], payload, observation["expected"]) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
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

    with true <- is_map(service_units) and Enum.sort(Map.keys(service_units)) == Enum.sort(@pools),
         true <- is_map(witness_units) and Enum.sort(Map.keys(witness_units)) == Enum.sort(expected_witness_units),
         true <-
           Enum.all?(@pools, fn pool ->
             unit = Map.get(service_units, pool)

             is_map(unit) and exact_keys?(unit, ~w(unit mainPID activeState masked cgroupProcessCount)) and
               unit["unit"] == "dahlia-symphony@#{pool}.service" and unit["mainPID"] == 0 and
               unit["activeState"] in ["inactive", "failed"] and unit["masked"] == true and
               unit["cgroupProcessCount"] == 0
           end),
         true <-
           Enum.all?(expected_witness_units, fn name ->
             unit = Map.get(witness_units, name)

             is_map(unit) and exact_keys?(unit, ~w(activeState masked)) and
               unit["activeState"] == "inactive" and unit["masked"] == true
           end) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp validate_witness_history(witnesses, log_hashes) do
    with true <- is_list(witnesses) and length(witnesses) == 2,
         true <- Enum.map(witnesses, &Map.get(&1, "generation")) == [1, 2],
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

  defp validate_predecessor(%{"execution" => execution, "claim" => claim, "receipt" => receipt} = predecessor, payload, current_claim)
       when map_size(predecessor) == 3 do
    with true <- exact_keys?(execution, @execution_fields),
         true <- exact_keys?(claim, @claim_fields),
         true <- exact_keys?(receipt, @retirement_fields),
         true <- execution["issue_id"] == payload["issueId"] and execution["generation"] == 1,
         true <-
           execution["repository"] == current_claim["repositoryRef"] and
             execution["status"] == "retired" and execution["cleanup"] == "cleaned" and
             execution["ownership"] in ["reconciled", "unknown"] and is_nil(execution["terminal"]) and
             is_nil(execution["cleanup_receipt"]) and execution["termination_unconfirmed"] == false and
             execution["retirement"] == receipt,
         true <-
           claim["issueId"] == payload["issueId"] and claim["generation"] == 1 and
             claim["repositoryRef"] == current_claim["repositoryRef"] and
             claim["managedProjectProfileId"] == current_claim["managedProjectProfileId"] and
             claim["runnerId"] == current_claim["runnerId"] and claim["workspaceId"] == current_claim["workspaceId"] and
             claim["companyId"] == current_claim["companyId"] and
             claim["responsibleDelegationId"] == receipt["prior_responsible_id"] and
             claim["executionFenceToken"] == "#{payload["issueId"]}:1" and
             claim["runtimeLeaseId"] == claim["sessionId"] and digest?(claim["nonceHash"]),
         true <-
           receipt["type"] == "unsubmitted_successor" and receipt["issue_id"] == payload["issueId"] and
             receipt["generation"] == 1 and receipt["repository_ref"] == current_claim["repositoryRef"] and
             receipt["managed_project_profile_id"] == current_claim["managedProjectProfileId"] and
             receipt["provider_projection_id"] == claim["projectionId"],
         true <-
           receipt["active_process"] == "absent" and receipt["local_claim"] == "absent" and
             receipt["provider_claim"] == "absent" and receipt["workspace"] == "absent",
         true <- text?(receipt["provider_projection_id"]) and is_integer(receipt["retired_at_ms"]) and receipt["retired_at_ms"] > 0,
         true <-
           Enum.all?(
             ~w(prior_accountable_digest prior_responsible_digest successor_accountable_digest successor_responsible_digest manifest_sha256 signer_key_sha256 observation_sha256),
             &digest?(receipt[&1])
           ),
         true <-
           Regex.match?(~r/\Asha256:[0-9a-f]{64}\z/, receipt["evidence_ref"]) and
             text?(receipt["linear_state"]) and receipt["successor_responsible_id"] == current_claim["responsibleDelegationId"],
         true <- receipt["prior_responsible_id"] == claim["responsibleDelegationId"],
         true <-
           receipt["prior_accountable_id"] != receipt["prior_responsible_id"] and
             receipt["successor_accountable_id"] != receipt["successor_responsible_id"] and
             receipt["prior_accountable_id"] != receipt["successor_accountable_id"] and
             receipt["prior_responsible_id"] != receipt["successor_responsible_id"],
         true <- retired_execution_lease?(execution, claim) do
      :ok
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp validate_predecessor(_predecessor, _payload, _current_claim),
    do: {:error, :invalid_confirmed_recovery_evidence}

  defp retired_execution_lease?(execution, claim) do
    case Map.to_list(execution["leases"]) do
      [{session_id, lease}] ->
        session_id == claim["sessionId"] and exact_keys?(lease, @lease_fields) and
          lease["issue_id"] == claim["issueId"] and lease["generation"] == 1 and
          lease["repository"] == claim["repositoryRef"] and lease["role"] == "worker" and
          lease["session_id"] == claim["sessionId"] and lease["process_id"] == claim["processId"] and
          lease["branch"] == execution["branch"] and lease["worktree"] == execution["worktree"] and
          lease["status"] == "released" and lease["release_reason"] == "claim_not_submitted" and
          lease["head"] == "unobserved" and lease["last_heartbeat_at"] == 0 and
          lease["termination_required"] == false and is_nil(lease["termination_confirmed_at_ms"]) and
          is_nil(lease["termination_evidence_ref"]) and is_nil(lease["termination_evidence"])

      _ ->
        false
    end
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
         true <- observed_at_ms >= observation_at_ms do
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
        {:ok, encoded} -> {:cont, {:ok, [chunks, Jason.encode!(field), ":", encoded]}}
        _ -> {:halt, {:error, :invalid_claim}}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, ["{", chunks, "}"] |> IO.iodata_to_binary()}
      error -> error
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

  defp verify_signature(payload, signature, key) do
    :crypto.verify(:eddsa, :none, signature_message(payload), signature, [key, :ed25519])
  rescue
    _ -> false
  end

  defp decode_canonical_object(bytes) do
    with {:ok, value} when is_map(value) <- Jason.decode(bytes),
         {:ok, canonical} <- Jason.encode(value),
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
