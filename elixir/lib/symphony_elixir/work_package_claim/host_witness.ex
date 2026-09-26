defmodule SymphonyElixir.WorkPackageClaim.HostWitness do
  @moduledoc """
  Synchronous root-owned witness for managed claim and worker-spawn intent.

  The root socket service persists each exact claim event before returning an
  acknowledgement. An unavailable or uncertain service holds the claim.
  """

  @socket_path "/run/dahlia-claim-witness.sock"
  @timeout_ms 5_000
  @operations ~w(claim_intent claim_bound spawn_intent)
  @abort_fields ~w(proofId proofSHA256 receiptId receiptSHA256 assignmentDigest allocationId abortResultRef)
  @pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)

  @spec record(map(), String.t(), map()) :: :ok | {:error, term()}
  def record(input, operation, reservation) when is_map(input) and operation in @operations do
    with {:ok, request} <- request(input, operation, reservation),
         {:ok, response} <- invoke(input, request) do
      validate_response(response)
    end
  end

  def record(_input, _operation, _reservation), do: {:error, :invalid_host_witness_operation}

  @doc "Records exact pre-execution abort proof references as root-witness provenance only."
  @spec record_abort(map(), map(), map()) :: :ok | {:error, term()}
  def record_abort(input, reservation, abort_proof)
      when is_map(input) and is_map(reservation) and is_map(abort_proof) do
    with :ok <- validate_abort_proof(abort_proof),
         {:ok, base} <- request(input, "abort_cleanup_intent", reservation),
         request = base |> Map.put("version", 2) |> Map.put("abortProof", abort_proof),
         {:ok, response} <- invoke(input, request) do
      validate_response(response)
    end
  end

  def record_abort(_input, _reservation, _abort_proof), do: {:error, :invalid_abort_proof_reference}

  @doc false
  @spec request(map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def request(input, operation, reservation) when is_map(input) and is_map(reservation) do
    fields = [
      {"projectionId", :projection_id},
      {"reservationId", :reservation_id},
      {"workspaceId", :workspace_id},
      {"companyId", :company_id},
      {"issueId", :issue_id},
      {"runnerId", :runner_id},
      {"managedProjectProfileId", :managed_project_profile_id},
      {"repositoryRef", :repository_ref},
      {"scopeKeys", :scope_keys},
      {"generation", :generation},
      {"sessionId", :session_id},
      {"processId", :process_id},
      {"responsibleDelegationId", :responsible_delegation_id},
      {"executionFenceToken", :execution_fence_token},
      {"runtimeLeaseId", :runtime_lease_id}
    ]

    claim = Map.new(fields, fn {wire, local} -> {wire, Map.get(reservation, local)} end)
    nonce = Map.get(reservation, :reservation_nonce)
    pool = Map.get(input, :pool_key)

    strings = fields |> Enum.map(&elem(&1, 0)) |> Enum.reject(&(&1 in ["scopeKeys", "generation"]))

    if pool in @pools and is_binary(nonce) and nonce != "" and
         valid_strings?(claim, strings) and valid_scopes?(claim) and
         valid_identity?(claim, input) and valid_fence?(claim) do
      claim = Map.put(claim, "nonceHash", :crypto.hash(:sha256, nonce) |> Base.encode16(case: :lower))
      {:ok, %{"version" => 1, "pool" => pool, "operation" => operation, "claim" => claim}}
    else
      {:error, :host_witness_claim_incomplete}
    end
  end

  def request(_input, _operation, _reservation), do: {:error, :host_witness_claim_incomplete}

  defp valid_strings?(claim, strings) do
    Enum.all?(strings, fn key -> is_binary(claim[key]) and claim[key] != "" end)
  end

  defp valid_scopes?(%{"scopeKeys" => scopes, "generation" => generation}) do
    is_integer(generation) and generation > 0 and is_list(scopes) and scopes != [] and
      Enum.all?(scopes, &(is_binary(&1) and &1 != "")) and length(Enum.uniq(scopes)) == length(scopes)
  end

  defp valid_identity?(claim, input) do
    claim["issueId"] == input[:issue_id] and claim["runnerId"] == input[:runner_id] and
      claim["managedProjectProfileId"] == input[:managed_project_profile_id] and
      claim["repositoryRef"] == input[:repository_ref]
  end

  defp valid_fence?(claim) do
    claim["executionFenceToken"] == "#{claim["issueId"]}:#{claim["generation"]}" and
      claim["runtimeLeaseId"] == claim["sessionId"]
  end

  defp validate_abort_proof(proof) do
    ids = [{"proofId", 256}, {"receiptId", 256}, {"allocationId", 256}, {"abortResultRef", 512}]
    hashes = ~w(proofSHA256 receiptSHA256 assignmentDigest)

    valid? =
      MapSet.equal?(MapSet.new(Map.keys(proof)), MapSet.new(@abort_fields)) and
        Enum.all?(ids, fn {key, limit} ->
          value = Map.get(proof, key)
          is_binary(value) and byte_size(value) in 1..limit and String.valid?(value)
        end) and
        Enum.all?(hashes, fn key ->
          value = Map.get(proof, key)
          is_binary(value) and String.match?(value, ~r/\A[a-f0-9]{64}\z/)
        end)

    if valid?, do: :ok, else: {:error, :invalid_abort_proof_reference}
  end

  defp invoke(%{host_witness_fun: witness}, request) when is_function(witness, 1), do: witness.(request)

  defp invoke(_input, request) do
    options = [:binary, packet: :line, active: false]

    case :gen_tcp.connect({:local, String.to_charlist(@socket_path)}, 0, options, @timeout_ms) do
      {:ok, socket} ->
        try do
          with :ok <- :gen_tcp.send(socket, Jason.encode!(request) <> "\n"),
               {:ok, line} <- :gen_tcp.recv(socket, 0, @timeout_ms),
               {:ok, response} <- Jason.decode(line) do
            {:ok, response}
          else
            {:error, reason} -> {:error, {:host_witness_io, reason}}
          end
        after
          :gen_tcp.close(socket)
        end

      {:error, reason} ->
        {:error, {:host_witness_unavailable, reason}}
    end
  end

  defp validate_response(%{"ok" => true, "receipt" => %{"version" => 1, "sequence" => sequence, "hash" => digest, "replayed" => replayed}})
       when is_integer(sequence) and sequence > 0 and is_binary(digest) and byte_size(digest) == 64 and
              is_boolean(replayed) do
    if String.match?(digest, ~r/\A[0-9a-f]{64}\z/), do: :ok, else: {:error, :invalid_host_witness_receipt}
  end

  defp validate_response(%{"ok" => false, "error" => reason}) when is_binary(reason),
    do: {:error, {:host_witness_rejected, reason}}

  defp validate_response(_response), do: {:error, :invalid_host_witness_receipt}
end
