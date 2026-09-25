defmodule SymphonyElixir.WorkPackageClaim.Recovery do
  @moduledoc "Recovers pre-spawn authority and reconciles released failed attempts before fresh admission."

  alias SymphonyElixir.{ExecutionFence, ResponsibilityGraph}
  alias SymphonyElixir.ManagedResponsibility.Admission
  alias SymphonyElixir.WorkPackageClaim.{Abandonment, Dispatch, Journal, Unsubmitted}

  @doc "Computes a cleanup-bound retirement reference; it does not revoke or authorize a replacement grant."
  @spec authority_revocation_ref(map(), map(), map(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def authority_revocation_ref(fence, graph, %{issue_id: id, generation: generation} = token, prior_id)
      when is_binary(id) and is_integer(generation) and generation > 0 and is_binary(prior_id) do
    with :ok <- ExecutionFence.validate(fence),
         :ok <- ResponsibilityGraph.validate(graph),
         %{generation: ^generation, status: :terminal, cleanup: :cleaned} = execution <- fence.executions[id],
         %{state: "Failed attempt"} <- execution.terminal,
         %{phase: :verified} <- execution.cleanup_receipt,
         :ok <- ExecutionFence.validate_cleanup(fence, token, execution.terminal.accepted_head),
         %{role: :responsible, runtime_lease: nil} = responsible <- graph.delegations[prior_id],
         parent = responsible.parent_delegation_id,
         accountable <- graph.delegations[parent],
         %{role: :accountable, runtime_lease: nil, parent_delegation_id: nil} <- accountable,
         true <-
           responsible.scope == accountable.scope and responsible.scope.issue_id == id and
             responsible.scope.repository == execution.repository do
      mutable = [:status, :blocked_on, :terminal_reason, :terminal_evidence]
      prior_accountable = Map.drop(accountable, mutable)
      prior_responsible = Map.drop(responsible, mutable)
      binding = {token, prior_accountable, prior_responsible, execution.terminal, execution.cleanup_receipt}
      digest = :crypto.hash(:sha256, :erlang.term_to_binary(binding, [:deterministic])) |> Base.encode16(case: :lower)
      {:ok, "sha256:" <> digest}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :authority_revocation_not_available}
    end
  end

  def authority_revocation_ref(_fence, _graph, _token, _prior_id), do: {:error, :authority_revocation_not_available}

  @spec prepare(map(), map(), map(), map(), non_neg_integer() | nil, non_neg_integer()) ::
          :new | {:new, map()} | {:new, map(), map()} | {:ok, map(), map(), map()} | {:error, term()}
  def prepare(runtime, fence, graph, issue, attempt, now_ms) do
    case fence.executions[issue.id] do
      nil ->
        new_without_claim(runtime, issue.id)

      %{status: :terminal, cleanup: :cleaned, terminal: %{state: "Failed attempt"}} = execution ->
        with :new <- completed_claim(runtime, issue.id, execution) do
          prepare_terminal_failed_claim(runtime, fence, graph, issue, attempt, execution, now_ms)
        end

      %{status: :terminal, cleanup: :cleaned} = execution ->
        completed_claim(runtime, issue.id, execution)

      execution ->
        case Abandonment.check(runtime, fence, issue.id) do
          :authorized ->
            prepare_released_claim(runtime, fence, graph, issue, attempt, now_ms)

          :missing ->
            prepare_existing_claim(runtime, fence, graph, issue, attempt, now_ms, execution)

          {:error, _reason} = error ->
            error
        end
    end
  end

  defp prepare_existing_claim(runtime, fence, graph, issue, attempt, now_ms, execution) do
    case Unsubmitted.prepare(runtime, fence, graph, execution, now_ms) do
      :submitted -> recover(runtime, fence, graph, issue, attempt, now_ms, execution)
      result -> result
    end
  end

  defp prepare_terminal_failed_claim(runtime, fence, graph, issue, attempt, execution, now_ms) do
    with {:ok, journal} <- load_journal(runtime),
         key = reservation_key(runtime, issue.id, execution),
         %{responsible_delegation_id: prior_id} <- journal.reservations[key],
         %{entries: entries} <- runtime[:managed_delegations],
         entry when is_map(entry) <- Enum.find(entries, &(&1.issue_id == issue.id)) do
      if prior_id == entry.responsible.id do
        prepare_released_claim(runtime, fence, graph, issue, attempt, now_ms)
      else
        prepare_distinct_failed_claim(
          runtime,
          fence,
          graph,
          issue,
          attempt,
          execution,
          now_ms,
          {prior_id, entry}
        )
      end
    else
      {:error, _reason} = error -> error
      _ -> {:error, :claim_abandonment_responsibility_changed}
    end
  end

  defp prepare_distinct_failed_claim(runtime, fence, graph, issue, attempt, execution, now_ms, {prior_id, entry}) do
    with nil <- graph.delegations[entry.accountable.id],
         nil <- graph.delegations[entry.responsible.id],
         {:ok, graph, _expiry} <- ResponsibilityGraph.reconcile(graph, now_ms),
         true <- retired_previous_pair?(fence, graph, prior_id, entry, issue, execution, now_ms),
         :ok <-
           ExecutionFence.validate_cleanup(
             fence,
             %{issue_id: issue.id, generation: execution.generation},
             execution.terminal.accepted_head
           ),
         {:ok, candidate} <-
           Admission.prepare(graph, fence, runtime.managed_delegations, issue, attempt, now_ms, runtime) do
      {:new, candidate}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :claim_abandonment_responsibility_changed}
    end
  end

  defp retired_previous_pair?(fence, graph, prior_id, entry, issue, execution, now_ms) do
    with %{
           id: ^prior_id,
           role: :responsible,
           status: status,
           runtime_lease: nil,
           parent_delegation_id: parent
         } = responsible <-
           graph.delegations[prior_id],
         %{
           id: ^parent,
           role: :accountable,
           status: ^status,
           runtime_lease: nil,
           parent_delegation_id: nil
         } = accountable <-
           graph.delegations[parent],
         true <- prior_authority_retired?(status, fence, graph, {responsible, accountable}, entry, execution, now_ms) do
      previous_pair_matches_claim?(responsible, accountable, entry, issue, execution)
    else
      _ -> false
    end
  end

  defp prior_authority_retired?(:expired, _fence, _graph, {responsible, accountable}, _entry, _execution, now_ms) do
    is_integer(responsible.expires_at_ms) and responsible.expires_at_ms <= now_ms and
      is_integer(accountable.expires_at_ms) and accountable.expires_at_ms <= now_ms
  end

  defp prior_authority_retired?(:revoked, fence, graph, {responsible, accountable}, entry, execution, _now_ms) do
    token = %{issue_id: execution.issue_id, generation: execution.generation}

    with {:ok, ref} <- authority_revocation_ref(fence, graph, token, responsible.id),
         %{prior_authority_revocation_ref: ^ref} <- entry,
         ^ref <- accountable.terminal_reason do
      responsible.terminal_reason in [{:ancestor_terminal, accountable.id}, inspect({:ancestor_terminal, accountable.id})]
    else
      _ -> false
    end
  end

  defp prior_authority_retired?(_status, _fence, _graph, _pair, _entry, _execution, _now_ms), do: false

  defp previous_pair_matches_claim?(responsible, accountable, entry, issue, execution) do
    accountable.actor_id == entry.owner_id and
      entry.owner_id == issue.assignee_id and
      responsible.actor_id == entry.responsible.actor_id and
      responsible.scope == accountable.scope and
      responsible.scope == entry.responsible.scope and
      entry.responsible.scope == entry.accountable.scope and
      responsible.scope.issue_id == issue.id and
      responsible.scope.repository == execution.repository
  end

  defp prepare_released_claim(runtime, fence, graph, issue, attempt, now_ms) do
    with %{entries: entries} = manifest <- runtime[:managed_delegations],
         entry when is_map(entry) <- Enum.find(entries, &(&1.issue_id == issue.id)),
         %{role: :responsible, status: :active, runtime_lease: nil, parent_delegation_id: parent} <- graph.delegations[entry.responsible.id],
         true <- parent == entry.accountable.id,
         {:ok, graph} <- reconcile_parent(graph, parent, now_ms),
         {:ok, graph} <- Admission.prepare(graph, fence, manifest, issue, attempt, now_ms, runtime) do
      {:new, graph}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :claim_abandonment_responsibility_changed}
    end
  end

  @spec held?(map(), String.t()) :: boolean()
  def held?(fence, issue_id) do
    case fence.executions[issue_id] do
      %{status: :active, leases: leases} -> Enum.any?(leases, fn {_id, lease} -> lease.status == :active end)
      _ -> false
    end
  end

  @spec unstarted_claims(map() | nil, map()) :: {:ok, [map()]} | {:error, term()}
  def unstarted_claims(nil, _fence), do: {:ok, []}

  def unstarted_claims(runtime, fence) do
    case load_journal(runtime) do
      {:ok, journal} ->
        {:ok, Enum.filter(Map.values(journal.reservations), &unstarted?/1)}

      :missing ->
        missing_journal_claims(fence)

      {:error, _reason} = error ->
        error
    end
  end

  defp missing_journal_claims(fence) do
    if Enum.any?(fence.executions, fn {id, _execution} -> held?(fence, id) end),
      do: {:error, :claim_recovery_journal_missing},
      else: {:ok, []}
  end

  defp unstarted?(%{dispatch: %{phase: phase}}), do: phase in ["submitted", "confirmed", "recovery_pending", "blocked"]
  defp unstarted?(_reservation), do: false

  defp new_without_claim(runtime, issue_id) do
    case load_journal(runtime) do
      :missing ->
        :new

      {:ok, journal} ->
        new_if_no_reservation(journal, issue_id)

      error ->
        error
    end
  end

  defp new_if_no_reservation(journal, issue_id) do
    if Enum.any?(journal.reservations, fn {_key, reservation} -> reservation.issue_id == issue_id end), do: {:error, :claim_exists_without_matching_fence}, else: :new
  end

  defp completed_claim(runtime, issue_id, execution) do
    with {:ok, journal} <- load_journal(runtime),
         key = reservation_key(runtime, issue_id, execution),
         %{generation: generation, cleanup_receipts: receipts} <- journal.reservations[key],
         true <- generation == execution.generation,
         %{acknowledgement: ack} <- receipts["repository_cleanup_verified"],
         true <-
           ack[:reservation_state] == "released" and ack[:scope_state] == "released" and
             ack[:accepted_head] == execution.terminal.accepted_head do
      :new
    else
      _ -> {:error, :claim_terminal_acknowledgement_required}
    end
  end

  defp reservation_key(runtime, issue_id, execution) do
    Journal.reservation_key(issue_id, runtime.managed_project_profile_id, execution.repository, execution.generation)
  end

  defp load_journal(%{journal_path: path}) when is_binary(path), do: Journal.load(path)
  defp load_journal(_runtime), do: :missing

  defp recover(runtime, fence, graph, issue, attempt, now_ms, execution) do
    with path when is_binary(path) <- runtime[:journal_path],
         {:ok, journal} <- Journal.load(path),
         {:ok, reservation} <-
           Dispatch.find(
             journal,
             issue.id,
             runtime.managed_project_profile_id,
             execution.repository,
             execution.generation
           ),
         :ok <- Dispatch.retry_status(reservation, now_ms),
         input = Map.merge(runtime, %{repository_ref: execution.repository}),
         true <- same_authority?(reservation, runtime, input),
         {:ok, fence} <- ExecutionFence.reconcile_unstarted_claim(fence, reservation),
         lease = runtime_lease(reservation),
         {:ok, graph} <- reconcile_graph(graph, reservation.responsible_delegation_id, lease, now_ms),
         {:ok, graph} <-
           Admission.prepare(graph, fence, runtime[:managed_delegations], issue, attempt, now_ms, runtime),
         {:ok, delegation} <-
           ResponsibilityGraph.admission_delegation(graph, issue.id, issue.identifier, execution.repository),
         true <- delegation.id == reservation.responsible_delegation_id and delegation.runtime_lease == lease do
      {:ok, fence, graph,
       %{
         token: %{issue_id: issue.id, generation: reservation.generation},
         session_id: reservation.session_id,
         delegation_id: delegation.id,
         runtime_lease: lease
       }}
    else
      {:error, _reason} = error -> error
      :missing -> {:error, :claim_recovery_journal_missing}
      _ -> {:error, :claim_recovery_not_ready}
    end
  end

  defp same_authority?(reservation, runtime, input) do
    reservation.runner_id == runtime.runner_id and
      reservation.dispatch.authority_digest == Dispatch.authority_digest(input)
  end

  defp reconcile_graph(graph, id, lease, now_ms) do
    case graph.delegations[id] do
      %{runtime_lease: ^lease, status: :active} ->
        {:ok, graph}

      %{runtime_lease: ^lease, status: :blocked, blocked_on: :restart_reconciliation, parent_delegation_id: parent} ->
        with {:ok, graph} <- reconcile_parent(graph, parent, now_ms),
             do: ResponsibilityGraph.reconcile_delegation(graph, id, lease, now_ms)

      _ ->
        {:error, :claim_responsibility_changed}
    end
  end

  defp reconcile_parent(graph, nil, _now_ms), do: {:ok, graph}

  defp reconcile_parent(graph, id, now_ms) do
    case graph.delegations[id] do
      %{role: :accountable, runtime_lease: nil, status: :active} ->
        {:ok, graph}

      %{role: :accountable, runtime_lease: nil, status: :blocked, blocked_on: :restart_reconciliation} ->
        ResponsibilityGraph.reconcile_delegation(graph, id, nil, now_ms)

      _ ->
        {:error, :claim_accountability_changed}
    end
  end

  defp runtime_lease(reservation) do
    %{
      issue_id: reservation.issue_id,
      repository: reservation.repository_ref,
      generation: reservation.generation,
      session_id: reservation.session_id,
      process_id: reservation.process_id
    }
  end
end
