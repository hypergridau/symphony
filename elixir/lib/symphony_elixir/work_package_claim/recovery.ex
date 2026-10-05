defmodule SymphonyElixir.WorkPackageClaim.Recovery do
  @moduledoc "Recovers pre-spawn authority and reconciles released failed attempts before fresh admission."

  alias SymphonyElixir.{ExecutionFence, ResponsibilityGraph}
  alias SymphonyElixir.ManagedResponsibility.Admission
  alias SymphonyElixir.WorkPackageClaim.{Abandonment, Dispatch, Journal, Unsubmitted, UnsubmittedSuccessor}

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
  @spec prepare(map(), map(), map(), map(), non_neg_integer() | nil, non_neg_integer(), keyword()) ::
          :new | {:new, map()} | {:new, map(), map()} | {:ok, map(), map(), map()} | {:error, term()}
  def prepare(runtime, fence, graph, issue, attempt, now_ms, opts \\ []) do
    case fence.executions[issue.id] do
      nil ->
        new_without_claim(runtime, issue.id)

      %{status: :terminal, cleanup: :cleaned, terminal: %{state: "Failed attempt"}} = execution ->
        with :new <- completed_claim(runtime, issue.id, execution) do
          prepare_terminal_failed_claim(runtime, fence, graph, issue, attempt, execution, now_ms)
        end

      %{status: :terminal, cleanup: :cleaned} = execution ->
        completed_claim(runtime, issue.id, execution)

      %{status: :retired, cleanup: :cleaned, retirement: %{type: "unsubmitted_successor"}} = execution ->
        prepare_retired_unsubmitted_successor(runtime, fence, graph, issue, attempt, now_ms, execution)

      execution ->
        prepare_nonterminal_claim(runtime, fence, graph, issue, attempt, now_ms, execution, opts)
    end
  end

  defp prepare_retired_unsubmitted_successor(runtime, fence, graph, issue, attempt, now_ms, execution) do
    with %{entries: entries} = manifest <- runtime[:managed_delegations],
         entry when is_map(entry) <- Enum.find(entries, &(&1.issue_id == issue.id)),
         true <- execution.issue_id == issue.id,
         {:ok, verified_fence, verified_graph, retirement_result} <-
           UnsubmittedSuccessor.prepare(runtime, fence, graph, entry, now_ms),
         true <- retirement_result in [:retired, :already_retired],
         {:ok, verified_graph} <- release_orphan_successor_lease(verified_graph, entry, execution, now_ms),
         {:ok, candidate} <-
           Admission.prepare(verified_graph, verified_fence, manifest, issue, attempt, now_ms, runtime) do
      {:new, candidate}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :unsubmitted_successor_not_proven}
    end
  end

  defp retired_successor_graph?(%{managed_delegations: %{entries: entries}}, graph, issue_id, generation)
       when is_list(entries) and is_integer(generation) do
    case Enum.find(entries, &(&1.issue_id == issue_id)) do
      %{prior_unsubmitted_authority: %{accountable_id: accountable_id, responsible_id: responsible_id}} ->
        with %{status: :revoked, terminal_evidence: receipt} <- Map.get(graph.delegations, accountable_id),
             %{status: :revoked, terminal_evidence: ^receipt} <- Map.get(graph.delegations, responsible_id),
             true <-
               is_map(receipt) and receipt["type"] == "unsubmitted_successor" and
                 receipt["generation"] == generation do
          true
        else
          _ -> false
        end

      _ ->
        false
    end
  end

  defp retired_successor_graph?(_runtime, _graph, _issue_id, _generation), do: false

  defp prepare_nonterminal_claim(runtime, fence, graph, issue, attempt, now_ms, execution, opts) do
    if retired_successor_graph?(runtime, graph, issue.id, execution.generation) do
      prepare_retired_unsubmitted_successor(runtime, fence, graph, issue, attempt, now_ms, execution)
    else
      prepare_claim_by_abandonment(runtime, fence, graph, issue, attempt, now_ms, execution, opts)
    end
  end

  defp prepare_claim_by_abandonment(runtime, fence, graph, issue, attempt, now_ms, execution, opts) do
    case Abandonment.check(runtime, fence, issue.id) do
      :authorized ->
        prepare_released_claim(runtime, fence, graph, issue, attempt, now_ms)

      :missing ->
        prepare_existing_claim(runtime, fence, graph, issue, attempt, now_ms, execution, opts)

      {:error, _reason} = error ->
        error
    end
  end

  defp release_orphan_successor_lease(graph, entry, execution, now_ms) do
    case Map.get(graph.delegations, entry.responsible.id) do
      %{runtime_lease: nil} ->
        {:ok, graph}

      %{runtime_lease: lease} ->
        expected_session = "worker:#{entry.issue_id}:#{execution.generation + 1}"

        expected_lease = %{
          issue_id: entry.issue_id,
          repository: execution.repository,
          generation: execution.generation + 1,
          session_id: expected_session,
          process_id: expected_session
        }

        release_expected_orphan_lease(graph, entry, lease, expected_lease, now_ms)

      nil ->
        {:ok, graph}
    end
  end

  defp release_expected_orphan_lease(graph, entry, lease, expected_lease, now_ms) do
    if lease == expected_lease do
      with {:ok, graph} <- reconcile_orphan_successor_pair(graph, entry, lease, now_ms),
           {:ok, released_graph, :released} <-
             ResponsibilityGraph.release_runtime_lease(graph, entry.responsible.id, lease, now_ms) do
        {:ok, released_graph}
      else
        _ -> {:error, :unsubmitted_successor_not_proven}
      end
    else
      {:error, :unsubmitted_successor_not_proven}
    end
  end

  defp reconcile_orphan_successor_pair(graph, entry, lease, now_ms) do
    case {Map.get(graph.delegations, entry.accountable.id), Map.get(graph.delegations, entry.responsible.id)} do
      {%{status: :active, runtime_lease: nil}, %{status: :active, runtime_lease: ^lease}} ->
        {:ok, graph}

      {
        %{status: :blocked, blocked_on: :restart_reconciliation, runtime_lease: nil},
        %{status: :blocked, blocked_on: :restart_reconciliation, runtime_lease: ^lease}
      } ->
        with {:ok, graph} <-
               ResponsibilityGraph.reconcile_delegation(graph, entry.accountable.id, nil, now_ms) do
          ResponsibilityGraph.reconcile_delegation(graph, entry.responsible.id, lease, now_ms)
        end

      _ ->
        {:error, :unsubmitted_successor_not_proven}
    end
  end

  defp prepare_existing_claim(runtime, fence, graph, issue, attempt, now_ms, execution, opts) do
    case Unsubmitted.prepare(runtime, fence, graph, execution, now_ms) do
      :submitted -> recover(runtime, fence, graph, issue, attempt, now_ms, execution, opts)
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

  defp unstarted?(%{dispatch: %{phase: phase}}), do: phase in ["submitted", "confirmed", "recovery_pending", "blocked", "abort_pending"]
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

  defp recover(runtime, fence, graph, issue, attempt, now_ms, execution, opts) do
    with path when is_binary(path) <- runtime[:journal_path],
         {:ok, journal} <- Journal.load(path),
         {:ok, reservation} <-
           Dispatch.find(
             journal,
             issue.id,
             runtime.managed_project_profile_id,
             execution.repository,
             execution.generation
           ) do
      recover_reservation(runtime, fence, graph, issue, attempt, now_ms, reservation, opts)
    else
      {:error, _reason} = error -> error
      :missing -> {:error, :claim_recovery_journal_missing}
      _ -> {:error, :claim_recovery_not_ready}
    end
  end

  defp recover_reservation(runtime, fence, graph, issue, attempt, now_ms, reservation, opts) do
    retained =
      reservation.dispatch.phase in ["allocation_suspended", "spawn_started"] and
        is_binary(reservation.dispatch.allocation_id)

    with :ok <- retry_gate(reservation, now_ms, retained),
         input = Map.merge(runtime, %{repository_ref: reservation.repository_ref}),
         true <- same_authority?(reservation, runtime, input),
         {:ok, fence} <- reconcile_fence(fence, reservation, retained),
         lease = runtime_lease(reservation),
         {:ok, graph} <- reconcile_graph(graph, reservation.responsible_delegation_id, lease, now_ms),
         {:ok, graph} <-
           Admission.prepare(graph, fence, runtime[:managed_delegations], issue, attempt, now_ms, runtime),
         {:ok, delegation} <-
           ResponsibilityGraph.admission_delegation(graph, issue.id, issue.identifier, reservation.repository_ref),
         true <- delegation.id == reservation.responsible_delegation_id and delegation.runtime_lease == lease,
         :ok <- verify_retained(reservation, fence, graph, retained, opts) do
      {:ok, fence, graph,
       %{
         token: %{issue_id: issue.id, generation: reservation.generation},
         session_id: reservation.session_id,
         delegation_id: delegation.id,
         runtime_lease: lease
       }}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :claim_recovery_not_ready}
    end
  end

  defp retry_gate(_reservation, _now_ms, true), do: :ok
  defp retry_gate(reservation, now_ms, false), do: Dispatch.retry_status(reservation, now_ms)

  defp reconcile_fence(fence, %{dispatch: %{phase: "allocation_suspended"}} = reservation, true),
    do: ExecutionFence.reconcile_suspended_claim(fence, reservation)

  defp reconcile_fence(fence, %{dispatch: %{phase: "spawn_started"}} = reservation, true),
    do: ExecutionFence.reconcile_disposable_spawn_claim(fence, reservation)

  defp reconcile_fence(fence, reservation, false), do: ExecutionFence.reconcile_unstarted_claim(fence, reservation)

  defp verify_retained(_reservation, _fence, _graph, false, _opts), do: :ok

  defp verify_retained(reservation, fence, graph, true, opts) do
    case Keyword.get(opts, :verify_retained) do
      verifier when is_function(verifier, 3) ->
        case verifier.(reservation, fence, graph) do
          :ok -> :ok
          {:error, _reason} = error -> error
          _ -> {:error, :suspended_allocation_recovery_unverified}
        end

      _ ->
        {:error, :suspended_allocation_recovery_unavailable}
    end
  rescue
    _ -> {:error, :suspended_allocation_recovery_unverified}
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
