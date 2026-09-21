defmodule SymphonyElixir.RetainedGrantProof.SnapshotEvidence do
  @moduledoc """
  Internal validation of exact fence and responsibility snapshot checkpoints.
  The root launcher supplies independently installed hashes and bounded bytes
  read under the actual quiescent locks. This module performs no path lookup,
  recovery, writes, signing or admission. Validated snapshots alone do not
  establish current claim, revocation, archive or native authority.
  """

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.WorkPackageClaim.Journal

  @maximum_bytes 262_144
  @keys [:fence_sha256, :graph_sha256]
  @original_keys ~w(id parent_delegation_id role actor_id scope authority budget expires_at_ms expected_deliverable expected_evidence return_to_parent)a
  @claim_keys ~w(issue_id managed_project_profile_id repository_ref projection_id reservation_id reservation_nonce scope_keys runner_id generation session_id process_id responsible_delegation_id execution_fence_token runtime_lease_id)a
  @claim_identity ~w(issue_id managed_project_profile_id repository_ref generation)a
  @worker_identity ~w(issue_id generation session_id process_id)a

  @spec decode(binary(), binary(), map()) :: {:ok, map()} | {:error, term()}
  def decode(fence_bytes, graph_bytes, binding) when is_map(binding) do
    with true <- Enum.sort(Map.keys(binding)) == @keys,
         true <- pinned?(fence_bytes, binding.fence_sha256),
         true <- pinned?(graph_bytes, binding.graph_sha256),
         {:ok, fence} <- FencePersistence.decode_bytes(fence_bytes),
         {:ok, graph} <- GraphPersistence.decode_bytes(graph_bytes) do
      {:ok, %{fence: fence, graph: graph, fence_sha256: binding.fence_sha256, graph_sha256: binding.graph_sha256}}
    else
      _ -> {:error, :retained_snapshot_evidence_invalid}
    end
  end

  def decode(_, _, _), do: {:error, :retained_snapshot_evidence_invalid}

  @doc "Internal consistency check; installed binding and independently validated grant are required."
  @spec match_retained(map(), map(), map(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def match_retained(%{fence: fence, graph: graph}, %{delegations: originals, issue_id: issue_id, context: %{repository_ref: repository}}, binding, now_ms)
      when is_map(binding) and is_map(originals) and is_integer(now_ms) and now_ms >= 0 do
    with true <- Enum.sort(Map.keys(binding)) == [:branch, :generation, :issue_id, :repository, :runtime_lease, :terminal, :worktree],
         :ok <- ExecutionFence.validate(fence),
         :ok <- ResponsibilityGraph.validate(graph),
         true <- binding.issue_id == issue_id and binding.repository == repository,
         execution when is_map(execution) <- fence.executions[binding.issue_id],
         :ok <- ExecutionFence.retained_process_quiescence(fence, binding.issue_id, binding.generation),
         true <- Map.take(execution, [:issue_id, :repository, :generation, :branch, :worktree]) == Map.take(binding, [:issue_id, :repository, :generation, :branch, :worktree]),
         true <- execution.status == :terminal and execution.cleanup == :pending and execution.ownership == :reconciled,
         true <- not Map.get(execution, :termination_unconfirmed, false),
         true <- Jason.decode!(Jason.encode!(execution.terminal)) == binding.terminal,
         true <- matching_pair?(graph, originals, now_ms),
         responsible = graph.delegations[originals.responsible.id],
         true <- responsible.runtime_lease == binding.runtime_lease,
         true <- matching_lease?(execution, binding.runtime_lease) do
      {:ok, %{issue_id: execution.issue_id, generation: execution.generation, branch: execution.branch, worktree: execution.worktree, terminal: execution.terminal}}
    else
      _ -> {:error, :retained_snapshot_consistency_invalid}
    end
  end

  def match_retained(_, _, _, _), do: {:error, :retained_snapshot_consistency_invalid}

  @doc "Checks a pinned original claim journal; provider history and acknowledgements remain separate authority checks."
  @spec match_claim(binary(), String.t(), map(), map(), map()) :: {:ok, map()} | {:error, term()}
  def match_claim(
        bytes,
        digest,
        %{issue_id: issue, responsible_id: responsible, context: context},
        %{issue_id: issue, repository: repository, generation: generation, runtime_lease: reference},
        expected
      )
      when is_map(context) and is_map(reference) and is_map(expected) do
    with true <- valid_claim_tuple?(issue, repository, generation),
         true <- valid_expected?(expected),
         true <- valid_reference?(reference, repository),
         true <- pinned?(bytes, digest),
         {:ok, journal} <- Journal.decode_bytes(bytes),
         true <- claim_tuple_matches?(expected, issue, repository, generation),
         true <- claim_context_matches?(expected, context, responsible),
         true <- Map.take(expected, @worker_identity) == Map.take(reference, @worker_identity),
         {:ok, reservation} <- selected_claim(journal, expected),
         true <- unique_claim?(journal, expected) do
      {:ok, %{claim: Map.take(reservation, @claim_keys), journal_sha256: digest, cleanup_receipts: Map.get(reservation, :cleanup_receipts, %{})}}
    else
      _ -> {:error, :retained_claim_evidence_invalid}
    end
  end

  def match_claim(_, _, _, _, _), do: {:error, :retained_claim_evidence_invalid}

  defp valid_claim_tuple?(issue, repository, generation) do
    is_binary(issue) and is_binary(repository) and is_integer(generation) and generation > 0
  end

  defp valid_expected?(expected) do
    Enum.sort(Map.keys(expected)) == Enum.sort(@claim_keys) and is_binary(expected[:managed_project_profile_id])
  end

  defp valid_reference?(reference, repository) do
    Enum.sort(Map.keys(reference)) == [:generation, :issue_id, :process_id, :repository, :session_id] and
      reference[:repository] == repository
  end

  defp claim_tuple_matches?(expected, issue, repository, generation) do
    {expected.issue_id, expected.repository_ref, expected.generation} == {issue, repository, generation}
  end

  defp claim_context_matches?(expected, context, responsible) do
    expected.managed_project_profile_id == context[:managed_project_profile_id] and
      expected.repository_ref == context[:repository_ref] and expected.runner_id == context[:runner_id] and
      expected.responsible_delegation_id == responsible
  end

  defp selected_claim(journal, expected) do
    profile = expected.managed_project_profile_id
    key = Journal.reservation_key(expected.issue_id, profile, expected.repository_ref, expected.generation)

    case journal.reservations[key] do
      reservation when is_map(reservation) ->
        if Map.take(reservation, @claim_keys) == expected, do: {:ok, reservation}, else: :error

      _ ->
        :error
    end
  end

  defp unique_claim?(journal, expected) do
    identity = Map.take(expected, @claim_identity)
    Enum.count(Map.values(journal.reservations), &(Map.take(&1, @claim_identity) == identity)) == 1
  end

  defp matching_pair?(graph, %{accountable: accountable, responsible: responsible}, now_ms) when is_map(accountable) and is_map(responsible) do
    Enum.all?([accountable, responsible], fn original ->
      case graph.delegations[Map.get(original, :id)] do
        %{status: :active, expires_at_ms: expiry} = current ->
          Enum.sort(Map.keys(original)) == Enum.sort(@original_keys) and
            expiry > now_ms and current.last_heartbeat_at <= now_ms and Map.take(current, @original_keys) == original

        _ ->
          false
      end
    end)
  end

  defp matching_pair?(_, _, _), do: false

  defp matching_lease?(execution, %{issue_id: issue, repository: repository, generation: generation, session_id: session, process_id: process} = reference) do
    Enum.sort(Map.keys(reference)) == [:generation, :issue_id, :process_id, :repository, :session_id] and
      issue == execution.issue_id and repository == execution.repository and generation == execution.generation and
      match?(%{role: :worker, process_id: ^process}, execution.leases[session])
  end

  defp matching_lease?(_, _), do: false

  defp pinned?(bytes, digest)
       when is_binary(bytes) and byte_size(bytes) > 0 and byte_size(bytes) <= @maximum_bytes and is_binary(digest) do
    String.valid?(digest) and Regex.match?(~r/\A[0-9a-f]{64}\z/, digest) and hex(bytes) == digest
  end

  defp pinned?(_, _), do: false
  defp hex(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end
