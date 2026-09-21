defmodule SymphonyElixir.WorkPackageClaim.Unsubmitted do
  @moduledoc "Distinguishes current claim uncertainty from retained historical attempts."

  alias SymphonyElixir.{ExecutionFence, ResponsibilityGraph}
  alias SymphonyElixir.WorkPackageClaim.Journal

  @grant_fields ~w(id parent_delegation_id role actor_id scope authority budget expires_at_ms expected_deliverable expected_evidence return_to_parent)a

  @doc "Builds retirement candidates from retained authorization and independently observed host/provider absence."
  @spec retire_expired(map(), map(), map(), map(), map(), non_neg_integer()) ::
          {:ok, map(), map()} | {:error, term()}
  def retire_expired(runtime, fence, graph, %{issue_id: _, accountable: %{id: _}, responsible: %{id: _, scope: %{issue_id: _, repository: _}}} = entry, observation, now_ms)
      when is_map(runtime) and is_map(observation) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- ExecutionFence.validate(fence),
         :ok <- ResponsibilityGraph.validate(graph),
         execution when is_map(execution) <- fence.executions[entry.issue_id],
         true <- observation["issue_id"] == execution.issue_id and observation["generation"] == execution.generation,
         true <- observation["provider_claim"] == "absent" and observation["active_process"] == "absent",
         ref when is_binary(ref) and ref != "" <- observation["evidence_ref"],
         true <- absent_local_workspace?(execution),
         :absent <- current_claim(runtime, execution),
         true <- immutable_match?(graph.delegations[entry.accountable.id], entry.accountable),
         true <- immutable_match?(graph.delegations[entry.responsible.id], entry.responsible),
         %{role: :accountable, runtime_lease: nil} <- graph.delegations[entry.accountable.id],
         %{role: :responsible, parent_delegation_id: parent} <- graph.delegations[entry.responsible.id],
         true <- parent == entry.accountable.id,
         true <- entry.responsible.scope.issue_id == execution.issue_id and entry.responsible.scope.repository == execution.repository,
         [worker] <- Map.values(execution.leases),
         lease = Map.take(worker, [:issue_id, :repository, :generation, :session_id, :process_id]),
         token = %{issue_id: execution.issue_id, generation: execution.generation},
         {:ok, next_fence} <- ExecutionFence.release_unsubmitted_claim(fence, token, worker.session_id),
         receipt = %{
           "type" => "expired_never_submitted",
           "issue_id" => execution.issue_id,
           "generation" => execution.generation,
           "repository" => execution.repository,
           "profile" => runtime.managed_project_profile_id,
           "worktree" => execution.worktree,
           "session_id" => worker.session_id,
           "process_id" => worker.process_id,
           "accountable_id" => entry.accountable.id,
           "responsible_id" => entry.responsible.id,
           "accountable_digest" => grant_digest(entry.accountable),
           "responsible_digest" => grant_digest(entry.responsible),
           "evidence_ref" => ref
         },
         {:ok, graph} <- ResponsibilityGraph.retire_expired_unsubmitted(graph, entry.accountable.id, nil, receipt, now_ms),
         {:ok, graph} <- ResponsibilityGraph.retire_expired_unsubmitted(graph, entry.responsible.id, lease, receipt, now_ms) do
      {:ok, next_fence, graph}
    else
      _ -> {:error, :expired_unsubmitted_retirement_not_proven}
    end
  end

  def retire_expired(_runtime, _fence, _graph, _entry, _observation, _now_ms),
    do: {:error, :expired_unsubmitted_retirement_not_proven}

  @doc "Retires a terminal issue that never acquired a provider claim or checkout."
  @spec retire_terminal(map(), map(), map(), map(), map(), non_neg_integer()) ::
          {:ok, map(), map()} | {:error, term()}
  def retire_terminal(
        runtime,
        fence,
        graph,
        %{issue_id: _, responsible: %{expires_at_ms: _}} = entry,
        observation,
        now_ms
      )
      when is_map(runtime) and is_map(observation) and
             is_integer(now_ms) and now_ms >= 0 do
    with :ok <- ExecutionFence.validate(fence),
         :ok <- ResponsibilityGraph.validate(graph),
         %{issue_id: issue_id, generation: generation} = execution <- fence.executions[entry.issue_id],
         true <- observation["issue_id"] == issue_id and observation["generation"] == generation,
         state when is_binary(state) <- observation["linear_state"],
         true <- String.downcase(state) in ~w(closed cancelled canceled duplicate done),
         true <- observation["provider_claim"] == "absent" and observation["active_process"] == "absent",
         projection_id when is_binary(projection_id) and projection_id != "" <- observation["provider_projection_id"],
         ref when is_binary(ref) and ref != "" <- observation["evidence_ref"],
         true <- absent_local_workspace?(execution),
         :absent <- current_claim(runtime, execution),
         {:ok, released_fence, released_graph} <-
           release_terminal_authority(runtime, fence, graph, entry, execution, observation, now_ms),
         evidence = %{
           issue_id: issue_id,
           generation: generation,
           linear_state: state,
           provider_projection_id: projection_id,
           provider_claim: :absent,
           active_process: :absent,
           local_claim: :absent,
           workspace: :absent,
           evidence_ref: ref
         },
         token = %{issue_id: issue_id, generation: generation},
         {:ok, retired_fence, _} <- ExecutionFence.retire_unsubmitted(released_fence, token, evidence, now_ms) do
      {:ok, retired_fence, released_graph}
    else
      _ -> {:error, :terminal_unsubmitted_retirement_not_proven}
    end
  end

  def retire_terminal(_runtime, _fence, _graph, _entry, _observation, _now_ms),
    do: {:error, :terminal_unsubmitted_retirement_not_proven}

  defp release_terminal_authority(runtime, fence, graph, entry, execution, observation, now_ms) do
    cond do
      execution.status == :retired ->
        if settled_terminal_authority?(runtime, fence, graph, entry, execution),
          do: {:ok, fence, graph},
          else: {:error, :terminal_unsubmitted_partial_recovery}

      entry.responsible.expires_at_ms <= now_ms ->
        retire_expired(runtime, fence, graph, entry, observation, now_ms)

      true ->
        case prepare(runtime, fence, graph, execution, now_ms) do
          {:new, next_fence, next_graph} -> {:ok, next_fence, next_graph}
          _ -> {:error, :unsubmitted_claim_not_released}
        end
    end
  end

  defp settled_terminal_authority?(runtime, fence, graph, entry, execution) do
    accountable = graph.delegations[entry.accountable.id]
    responsible = graph.delegations[entry.responsible.id]

    immutable_match?(accountable, entry.accountable) and
      immutable_match?(responsible, entry.responsible) and
      is_nil(accountable.runtime_lease) and is_nil(responsible.runtime_lease) and
      ((accountable.status == :active and responsible.status == :active) or
         retired_authorization?(runtime, fence, graph, execution))
  end

  defp grant_digest(grant) do
    grant |> Map.take(@grant_fields) |> :erlang.term_to_binary([:deterministic]) |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
  end

  defp absent_local_workspace?(%{worker_host: nil, worktree: path}) when is_binary(path) do
    Path.type(path) == :absolute and Path.expand(path) == path and
      not String.starts_with?(path, ["//", "\\\\"]) and
      File.lstat(path) == {:error, :enoent} and plain_directory_ancestors?(Path.dirname(path))
  end

  defp absent_local_workspace?(_execution), do: false

  defp retired_authorization?(runtime, fence, graph, execution) do
    Enum.any?(graph.delegations, fn {_id, delegation} ->
      with %{role: :responsible, status: :expired, runtime_lease: nil, terminal_evidence: receipt} <- delegation,
           %{"type" => "expired_never_submitted"} <- receipt,
           true <- receipt["issue_id"] == execution.issue_id and receipt["generation"] == execution.generation,
           true <- receipt["repository"] == execution.repository and receipt["worktree"] == execution.worktree,
           true <- receipt["profile"] == runtime[:managed_project_profile_id],
           true <- receipt["responsible_id"] == delegation.id and receipt["responsible_digest"] == grant_digest(delegation),
           parent when is_map(parent) <- graph.delegations[receipt["accountable_id"]],
           true <- retired_parent?(parent, delegation, receipt),
           [%{status: :released, release_reason: reason} = worker] <- Map.values(execution.leases),
           true <- reason in [:claim_not_submitted, "claim_not_submitted"],
           true <- worker.session_id == receipt["session_id"] and worker.process_id == receipt["process_id"],
           {:ok, ^execution} <- retired_execution(runtime, fence, execution) do
        true
      else
        _ -> false
      end
    end)
  end

  defp retired_parent?(parent, delegation, receipt) do
    match?(%{role: :accountable, status: :expired, runtime_lease: nil}, parent) and
      parent.terminal_evidence == receipt and delegation.parent_delegation_id == parent.id and
      receipt["accountable_digest"] == grant_digest(parent)
  end

  defp retired_execution(runtime, fence, execution) do
    token = %{issue_id: execution.issue_id, generation: execution.generation}
    [worker] = Map.values(execution.leases)

    with :absent <- current_claim(runtime, execution),
         true <- absent_local_workspace?(execution),
         :ok <- ExecutionFence.validate(fence),
         true <- execution.ownership == :reconciled,
         true <-
           retired_terminal_execution?(execution) or
             match?({:ok, ^fence}, ExecutionFence.release_unsubmitted_claim(fence, token, worker.session_id)) do
      {:ok, execution}
    else
      _ -> {:error, :retired_execution_changed}
    end
  end

  defp retired_terminal_execution?(%{status: :retired, cleanup: :cleaned, retirement: retirement} = execution)
       when is_map(retirement) do
    retirement.issue_id == execution.issue_id and retirement.generation == execution.generation and
      retirement.provider_claim == :absent and retirement.local_claim == :absent and
      retirement.workspace == :absent and retirement.active_process == :absent
  end

  defp retired_terminal_execution?(_execution), do: false

  @doc "Proves an already released local generation has no claim or workspace blocking another issue."
  @spec released_without_workspace?(map() | nil, map(), map(), map(), non_neg_integer()) :: boolean()
  def released_without_workspace?(runtime, fence, graph, %{worker_host: nil, worktree: path} = execution, now_ms)
      when is_map(runtime) and is_binary(path) do
    with :ok <- ExecutionFence.validate(fence),
         :ok <- ResponsibilityGraph.validate(graph),
         ^execution <- fence.executions[execution.issue_id],
         true <- Path.type(path) == :absolute and Path.expand(path) == path,
         false <- String.starts_with?(path, ["//", "\\\\"]),
         {:error, :enoent} <- File.lstat(path),
         true <- plain_directory_ancestors?(Path.dirname(path)),
         true <- matching_authorization?(runtime, graph, execution),
         :absent <- current_claim(runtime, execution),
         true <-
           retired_terminal_execution?(execution) or
             match?({:new, ^fence, ^graph}, prepare(runtime, fence, graph, execution, now_ms)) do
      true
    else
      _ -> retired_authorization?(runtime, fence, graph, execution)
    end
  end

  def released_without_workspace?(_runtime, _fence, _graph, _execution, _now_ms), do: false

  defp matching_authorization?(%{managed_project_profile_id: profile, managed_delegations: manifest}, graph, execution) do
    with %{managed_project_profile_id: ^profile, repository_ref: repository, entries: entries} <- manifest,
         true <- repository == execution.repository,
         entry when is_map(entry) <- Enum.find(entries, &(&1.issue_id == execution.issue_id)) do
      Enum.all?([entry.accountable, entry.responsible], &immutable_match?(graph.delegations[&1.id], &1))
    else
      _ -> false
    end
  end

  defp matching_authorization?(_runtime, _graph, _execution), do: false

  defp immutable_match?(current, expected) when is_map(current), do: Map.take(current, Map.keys(expected)) == expected
  defp immutable_match?(_current, _expected), do: false

  defp plain_directory_ancestors?(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory}} ->
        parent = Path.dirname(path)
        parent == path or plain_directory_ancestors?(parent)

      _ ->
        false
    end
  end

  @spec claim_may_exist?(map(), map(), String.t()) :: boolean()
  def claim_may_exist?(runtime, fence, issue_id) do
    case current_claim(runtime, fence.executions[issue_id]) do
      :absent -> false
      :missing -> false
      _ -> true
    end
  end

  @spec prepare(map(), map(), map(), map(), non_neg_integer()) ::
          :submitted | {:new, map(), map()} | {:error, term()}
  def prepare(runtime, fence, graph, execution, now_ms) do
    case current_claim(runtime, execution) do
      :present -> :submitted
      :absent -> release(runtime, fence, graph, execution, now_ms)
      :missing -> prepare_missing(runtime, fence, graph, execution, now_ms)
      {:error, _reason} = error -> error
    end
  end

  defp prepare_missing(runtime, fence, graph, execution, now_ms) do
    case Map.values(execution.leases) do
      [%{status: :released, release_reason: reason}] when reason in [:claim_not_submitted, "claim_not_submitted"] ->
        release(runtime, fence, graph, execution, now_ms)

      _ ->
        {:error, :claim_recovery_journal_missing}
    end
  end

  defp current_claim(runtime, %{issue_id: issue_id, repository: repository, generation: generation}) do
    with profile when is_binary(profile) and profile != "" <- runtime[:managed_project_profile_id],
         path when is_binary(path) and path != "" <- runtime[:journal_path] do
      case Journal.load(path) do
        {:ok, journal} -> reservation_status(journal, issue_id, profile, repository, generation)
        :missing -> :missing
        {:error, _reason} = error -> error
      end
    else
      _ -> {:error, :claim_recovery_identity_missing}
    end
  end

  defp current_claim(_runtime, _execution), do: {:error, :claim_recovery_identity_missing}

  defp reservation_status(journal, issue_id, profile, repository, generation) do
    current = Enum.filter(journal.reservations, fn {_key, r} -> r.issue_id == issue_id and r.generation >= generation end)
    key = Journal.reservation_key(issue_id, profile, repository, generation)

    case current do
      [] ->
        :absent

      [{^key, %{managed_project_profile_id: ^profile, repository_ref: ^repository, generation: ^generation}}] ->
        :present

      _ ->
        {:error, :claim_recovery_identity_conflict}
    end
  end

  defp release(runtime, fence, graph, execution, now_ms) do
    token = %{issue_id: execution.issue_id, generation: execution.generation}

    with :ok <- ExecutionFence.validate(fence),
         :ok <- ResponsibilityGraph.validate(graph),
         [worker] <- Map.values(execution.leases),
         {:ok, released_fence} <- ExecutionFence.release_unsubmitted_claim(fence, token, worker.session_id),
         %{entries: entries} <- runtime[:managed_delegations],
         entry when is_map(entry) <- Enum.find(entries, &(&1.issue_id == execution.issue_id)),
         %{role: :responsible, parent_delegation_id: parent} <- graph.delegations[entry.responsible.id],
         true <- parent == entry.accountable.id,
         lease = Map.take(worker, [:issue_id, :repository, :generation, :session_id, :process_id]),
         {:ok, graph} <- reconcile_parent(graph, parent, now_ms),
         {:ok, graph} <- reconcile_responsible(graph, entry.responsible.id, lease, now_ms),
         {:ok, graph, _} <- ResponsibilityGraph.release_runtime_lease(graph, entry.responsible.id, lease, now_ms) do
      {:new, released_fence, graph}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :unsubmitted_claim_responsibility_changed}
    end
  end

  defp reconcile_parent(graph, parent, now_ms) do
    case graph.delegations[parent] do
      %{role: :accountable, runtime_lease: nil, status: :active} ->
        {:ok, graph}

      %{role: :accountable, runtime_lease: nil, status: :blocked, blocked_on: :restart_reconciliation} ->
        ResponsibilityGraph.reconcile_delegation(graph, parent, nil, now_ms)

      _ ->
        {:error, :claim_accountability_changed}
    end
  end

  defp reconcile_responsible(graph, id, lease, now_ms) do
    case graph.delegations[id] do
      %{status: :active, runtime_lease: current} when current == lease or is_nil(current) ->
        {:ok, graph}

      %{status: :blocked, blocked_on: :restart_reconciliation, runtime_lease: current}
      when current == lease or is_nil(current) ->
        ResponsibilityGraph.reconcile_delegation(graph, id, current, now_ms)

      _ ->
        {:error, :claim_responsibility_changed}
    end
  end
end
