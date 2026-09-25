defmodule SymphonyElixir.WorkPackageClaim.Abandonment do
  @moduledoc "Verifies a host-signed provider recovery receipt without changing retained local authority."

  alias SymphonyElixir.{ExecutionFence, WorkPackageClaim.Journal}

  @maximum 9_007_199_254_740_990
  @envelope_keys ~w(contractVersion expected receipt localGenerationMax journalSHA256 neverSpawned)
  @claim_keys ~w(projectionId reservationId workspaceId companyId issueId runnerId managedProjectProfileId repositoryRef scopeKeys generation sessionId processId responsibleDelegationId executionFenceToken runtimeLeaseId nonceHash)
  @receipt_keys ~w(recoveryId projectionId fenceRevision oldTupleDigest oldNonceHash nextGenerationFloor confirmedAt proofDigest projectionState reservationState executionCapacityState scopeState)
  @journal_fields [
    projection_id: "projectionId",
    reservation_id: "reservationId",
    issue_id: "issueId",
    runner_id: "runnerId",
    managed_project_profile_id: "managedProjectProfileId",
    repository_ref: "repositoryRef",
    generation: "generation",
    session_id: "sessionId",
    process_id: "processId",
    responsible_delegation_id: "responsibleDelegationId",
    execution_fence_token: "executionFenceToken",
    runtime_lease_id: "runtimeLeaseId"
  ]

  @spec check(map(), map(), String.t()) :: :missing | :authorized | {:error, atom()}
  def check(runtime, fence, issue_id) do
    case runtime[:claim_recovery] do
      nil -> :missing
      config -> verify(config, runtime, fence, issue_id)
    end
  end

  defp verify(config, runtime, fence, issue_id) do
    with true <- uuid?(issue_id),
         :ok <- ExecutionFence.validate(fence),
         %{generation: generation} = execution <- fence.executions[issue_id],
         {:ok, encoded} <- read_regular(Path.join(config.directory, issue_id <> ".json"), 131_072),
         {:ok, envelope} <- signed_envelope(encoded, config.public_key),
         true <- valid_envelope?(envelope, runtime, issue_id) do
      if generation > envelope["localGenerationMax"] do
        :missing
      else
        verify_retained(envelope, runtime, fence, execution)
      end
    else
      {:error, :enoent} -> :missing
      _ -> {:error, :claim_abandonment_evidence_invalid}
    end
  rescue
    _ -> {:error, :claim_abandonment_evidence_invalid}
  end

  defp signed_envelope(encoded, key) do
    with {:ok, outer} <- Jason.decode(encoded),
         true <- keys?(outer, ~w(payload signature)),
         {:ok, payload} <- Base.url_decode64(outer["payload"], padding: false),
         {:ok, signature} <- Base.url_decode64(outer["signature"], padding: false),
         true <- byte_size(key) == 32 and byte_size(signature) == 64,
         true <- :crypto.verify(:eddsa, :none, payload, signature, [key, :ed25519]) do
      Jason.decode(payload)
    end
  end

  defp valid_envelope?(value, runtime, issue_id) do
    valid_envelope_metadata?(value) and valid_claim?(value["expected"]) and
      value["expected"]["issueId"] == issue_id and
      value["expected"]["runnerId"] == runtime.runner_id and
      value["expected"]["managedProjectProfileId"] == runtime.managed_project_profile_id and
      value["expected"]["generation"] <= value["localGenerationMax"] and valid_receipt?(value)
  end

  defp valid_envelope_metadata?(value) do
    keys?(value, @envelope_keys) and value["contractVersion"] == "work-package-pre-spawn-recovery.v1" and
      value["neverSpawned"] == true and positive?(value["localGenerationMax"]) and digest?(value["journalSHA256"])
  end

  defp valid_claim?(claim) do
    keys?(claim, @claim_keys) and Enum.all?(@claim_keys -- ~w(scopeKeys generation), &text?(claim[&1])) and
      positive?(claim["generation"]) and digest?(claim["nonceHash"]) and
      valid_scopes?(claim["scopeKeys"]) and
      claim["executionFenceToken"] == "#{claim["issueId"]}:#{claim["generation"]}" and
      claim["runtimeLeaseId"] == claim["sessionId"]
  end

  defp valid_scopes?(scopes) do
    is_list(scopes) and length(scopes) in 1..64 and Enum.all?(scopes, &text?/1) and Enum.uniq(scopes) == scopes
  end

  defp valid_receipt?(value) do
    receipt = value["receipt"]
    claim = value["expected"]

    keys?(receipt, @receipt_keys) and Enum.all?(@receipt_keys -- ["nextGenerationFloor"], &text?(receipt[&1])) and
      receipt_matches_claim?(receipt, claim) and digest?(receipt["proofDigest"]) and
      receipt["nextGenerationFloor"] == value["localGenerationMax"] + 1 and
      match?({:ok, _, _}, DateTime.from_iso8601(receipt["confirmedAt"])) and
      receipt_released?(receipt)
  end

  defp receipt_matches_claim?(receipt, claim) do
    receipt["projectionId"] == claim["projectionId"] and receipt["oldNonceHash"] == claim["nonceHash"] and
      receipt["oldTupleDigest"] == old_tuple_digest(claim)
  end

  defp receipt_released?(receipt) do
    receipt["projectionState"] == "queued" and receipt["reservationState"] == "released" and
      receipt["executionCapacityState"] == "released" and receipt["scopeState"] == "released"
  end

  defp verify_retained(value, runtime, fence, execution) do
    with true <- fence_safe?(fence, value["expected"], value["localGenerationMax"]),
         {:error, :enoent} <- File.lstat(execution.worktree),
         {:ok, raw} <- read_regular(runtime.journal_path, 8_388_608),
         true <- sha256(raw) == value["journalSHA256"],
         {:ok, journal} <- Journal.decode_bytes(raw),
         {:ok, ^raw} <- read_regular(runtime.journal_path, 8_388_608),
         entries = Map.values(journal.reservations),
         true <- Enum.count(entries, &matching_claim?(&1, value["expected"])) == 1,
         true <- Enum.all?(entries, &unstarted_reservation?(&1, value["expected"], value["localGenerationMax"])) do
      :authorized
    else
      _ -> {:error, :claim_abandonment_local_state_changed}
    end
  end

  defp matching_claim?(reservation, claim) do
    Enum.all?(@journal_fields, fn {local, remote} -> reservation[local] == claim[remote] end) and
      scope_matches?(reservation, claim) and
      Enum.sort(reservation.scope_keys) == Enum.sort(claim["scopeKeys"]) and sha256(reservation.reservation_nonce) == claim["nonceHash"]
  end

  defp scope_matches?(reservation, claim) do
    Enum.all?([{:workspace_id, "workspaceId"}, {:company_id, "companyId"}], fn {local, remote} ->
      reservation[local] == claim[remote]
    end)
  end

  defp unstarted_reservation?(reservation, claim, maximum) do
    same_reservation?(reservation, claim) and
      reservation.generation <= maximum and get_in(reservation, [:dispatch, :phase]) != "spawn_started" and
      map_size(Map.get(reservation, :cleanup_receipts, %{})) == 0
  end

  defp same_reservation?(reservation, claim) do
    fields = Keyword.take(@journal_fields, [:issue_id, :repository_ref, :runner_id, :managed_project_profile_id, :projection_id, :reservation_id])

    Enum.all?(fields, fn {local, remote} -> reservation[local] == claim[remote] end) and
      Enum.sort(reservation.scope_keys) == Enum.sort(claim["scopeKeys"])
  end

  defp fence_safe?(fence, expected, maximum) do
    current = fence.executions[expected["issueId"]]
    entries = [current | Enum.filter(fence.history, &(&1.issue_id == expected["issueId"]))]
    generations = Enum.map(entries, & &1.generation)
    old = Enum.find(entries, &(&1.generation == expected["generation"]))

    current.generation == maximum and length(entries) == maximum and
      Enum.sort(generations) == Enum.to_list(1..length(entries)) and
      Enum.all?(entries, &unstarted_execution?(&1, expected)) and is_map(old) and
      Enum.any?(Map.values(old.leases), fn lease ->
        lease.session_id == expected["sessionId"] and lease.process_id == expected["processId"]
      end)
  end

  defp unstarted_execution?(execution, expected) do
    execution.repository == expected["repositoryRef"] and execution.status == :active and
      execution.ownership == :reconciled and execution.cleanup == :pending and is_nil(execution.terminal) and
      not Map.get(execution, :termination_unconfirmed, false) and map_size(execution.leases) == 1 and
      absent_workspace_and_unstarted_lease?(execution)
  end

  defp absent_workspace_and_unstarted_lease?(execution) do
    File.lstat(execution.worktree) == {:error, :enoent} and
      Enum.all?(Map.values(execution.leases), &unstarted_lease?(&1, execution.generation))
  end

  defp unstarted_lease?(lease, generation) do
    lease.role == :worker and lease.status == :released and lease.generation == generation and
      lease[:release_reason] in [:spawn_failed, "spawn_failed", :claim_not_submitted, "claim_not_submitted"] and
      no_worker_observation?(lease)
  end

  defp no_worker_observation?(lease) do
    lease.head == "unobserved" and lease.last_heartbeat_at == 0 and
      not Map.get(lease, :termination_required, false) and is_nil(lease[:supervisor_identity]) and
      is_nil(lease[:termination_evidence])
  end

  defp old_tuple_digest(claim) do
    fields =
      Enum.map_join(@claim_keys, ",", fn key ->
        value = if key == "scopeKeys", do: Enum.sort(claim[key]), else: claim[key]
        Jason.encode!(key) <> ":" <> Jason.encode!(value)
      end)

    sha256("{" <> fields <> "}")
  end

  defp read_regular(path, maximum) do
    with {:ok, %{type: :regular, size: size}} <- File.lstat(path),
         true <- size <= maximum,
         {:ok, file} <- File.open(path, [:read, :binary]) do
      try do
        case IO.binread(file, maximum + 1) do
          bytes when is_binary(bytes) and byte_size(bytes) <= maximum -> {:ok, bytes}
          _ -> {:error, :invalid_recovery_file}
        end
      after
        File.close(file)
      end
    end
  end

  defp keys?(value, keys), do: is_map(value) and Enum.sort(Map.keys(value)) == Enum.sort(keys)
  defp text?(value), do: is_binary(value) and byte_size(value) in 1..256
  defp positive?(value), do: is_integer(value) and value in 1..@maximum
  defp digest?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp uuid?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-fA-F]{8}(?:-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}\z/, value)
  defp sha256(value), do: :crypto.hash(:sha256, value) |> Base.encode16(case: :lower)
end
