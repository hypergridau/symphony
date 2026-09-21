defmodule SymphonyElixir.ExecutionFence do
  @moduledoc """
  Serializable coordination contract for issue execution ownership.

  The orchestrator currently keeps scheduling state in memory. This module keeps
  the generation, lease, and quiescence rules pure and serializable so a caller
  can persist the returned state atomically without giving this contract any
  Git, process, tracker, or cleanup side effects.

  A generation token is required for every worker, reviewer, mutation guard,
  and cleanup decision. Terminal fencing never releases leases implicitly;
  cleanup becomes admissible only after all known leases are released or
  expired and active-session ownership has been reconciled.
  """

  @schema_version 1
  @default_lease_ttl_ms 300_000
  @roles [:worker, :reviewer]
  @mutable_actions [:commit, :push, :state_mutation]
  @retirement_evidence_keys [
    :active_process,
    :evidence_ref,
    :generation,
    :issue_id,
    :linear_state,
    :local_claim,
    :provider_claim,
    :provider_projection_id,
    :workspace
  ]

  @type token :: %{issue_id: String.t(), generation: pos_integer()}
  @type state :: %{
          schema_version: 1,
          executions: %{optional(String.t()) => map()},
          sessions: %{optional(String.t()) => map()},
          history: [map()],
          triage_records: %{optional(String.t()) => map()}
        }

  @doc "Creates an empty, versioned execution-fence snapshot."
  @spec new() :: state()
  def new do
    %{
      schema_version: @schema_version,
      executions: %{},
      sessions: %{},
      history: [],
      triage_records: %{}
    }
  end

  @doc "Validates a fence state before it is persisted or used for admission."
  @spec validate(state()) :: :ok | {:error, :invalid_state}
  def validate(state), do: validate_state(state)

  @doc "Read-only retained-generation process/lease check; does not establish cleanup or authorize reuse."
  @spec retained_process_quiescence(state(), String.t(), pos_integer()) :: :ok | {:error, term()}
  def retained_process_quiescence(state, issue_id, generation) when is_binary(issue_id) and is_integer(generation) and generation > 0 do
    with :ok <- validate_state(state),
         %{generation: ^generation} = execution <- state.executions[issue_id],
         true <- execution.ownership == :reconciled,
         false <- termination_unconfirmed?(execution),
         [] <- active_lease_ids(execution) do
      :ok
    else
      _ -> {:error, :retained_execution_not_quiescent}
    end
  end

  def retained_process_quiescence(_, _, _), do: {:error, :retained_execution_not_quiescent}

  @doc "Marks non-cleaned executions unknown after an orchestrator restart."
  @spec mark_unreconciled_after_restart(state()) :: {:ok, state()} | {:error, :invalid_state}
  def mark_unreconciled_after_restart(state) do
    with :ok <- validate_state(state) do
      executions =
        Map.new(state.executions, fn {issue_id, execution} ->
          next_execution =
            if termination_unconfirmed?(execution) do
              %{execution | ownership: :unknown}
            else
              if execution.cleanup == :cleaned or active_lease_ids(execution) == [] do
                %{execution | ownership: :reconciled}
              else
                %{execution | ownership: :unknown}
              end
            end

          {issue_id, next_execution}
        end)

      {:ok, %{state | executions: executions}}
    end
  end

  @doc "Reconciles only a journal-proven, never-started claim at the unchanged current generation."
  @spec reconcile_unstarted_claim(state(), map()) :: {:ok, state()} | {:error, term()}
  def reconcile_unstarted_claim(state, reservation) do
    with :ok <- validate_state(state),
         %{dispatch: %{phase: phase}} <- reservation,
         true <- phase in ["submitted", "confirmed", "blocked"],
         %{status: :active, cleanup: :pending} = execution <- state.executions[reservation.issue_id],
         true <- execution.generation == reservation.generation and execution.repository == reservation.repository_ref,
         true <- execution.ownership in [:reconciled, :unknown] and not Map.get(execution, :termination_unconfirmed, false),
         [session_id] <- active_lease_ids(execution),
         true <- session_id == reservation.session_id,
         %{role: :worker, status: :active, head: "unobserved", last_heartbeat_at: 0} = lease <- execution.leases[session_id],
         true <- lease.process_id == reservation.process_id and is_nil(lease[:supervisor_identity]),
         true <- not Map.get(lease, :termination_required, false),
         true <-
           reservation.execution_fence_token == "#{reservation.issue_id}:#{reservation.generation}" and
             reservation.runtime_lease_id == session_id do
      {:ok, put_in(state, [:executions, reservation.issue_id, :ownership], :reconciled)}
    else
      _ -> {:error, :unstarted_claim_not_reconcilable}
    end
  end

  @doc "Releases untouched local authority after the claim journal proves this generation was never submitted."
  @spec release_unsubmitted_claim(state(), token(), String.t()) :: {:ok, state()} | {:error, term()}
  def release_unsubmitted_claim(state, token, session_id) do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         true <- execution.status == :active and execution.cleanup == :pending,
         true <- execution.ownership in [:reconciled, :unknown] and not termination_unconfirmed?(execution),
         [{^session_id, lease}] <- Map.to_list(execution.leases),
         true <- lease.role == :worker and lease.head == "unobserved" and lease.last_heartbeat_at == 0,
         true <- is_nil(lease[:supervisor_identity]) and not Map.get(lease, :termination_required, false),
         true <- lease.status == :active or (lease.status == :released and lease[:release_reason] in [:claim_not_submitted, "claim_not_submitted"]),
         {:ok, released, _} <- release(state, token, session_id, :claim_not_submitted) do
      {:ok, put_in(released, [:executions, token.issue_id, :ownership], :reconciled)}
    else
      _ -> {:error, :unsubmitted_claim_not_reconcilable}
    end
  end

  @doc "Retires a never-submitted generation without inventing a Git terminal head."
  @spec retire_unsubmitted(state(), token(), map(), non_neg_integer()) ::
          {:ok, state(), :retired | :already_retired} | {:error, term()}
  def retire_unsubmitted(state, token, evidence, now_ms)
      when is_map(evidence) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         true <- valid_unsubmitted_retirement_evidence?(evidence),
         true <- evidence.issue_id == execution.issue_id and evidence.generation == execution.generation,
         true <- unsubmitted_retirement_lease?(execution) do
      finish_unsubmitted_retirement(state, execution, evidence, now_ms)
    else
      _ -> {:error, :unsubmitted_retirement_not_proven}
    end
  end

  def retire_unsubmitted(_state, _token, _evidence, _now_ms),
    do: {:error, :unsubmitted_retirement_not_proven}

  defp finish_unsubmitted_retirement(state, %{status: :active, terminal: nil, cleanup: :pending, cleanup_receipt: nil} = execution, evidence, now_ms) do
    retirement = Map.put(evidence, :retired_at_ms, now_ms)
    updated = Map.merge(execution, %{status: :retired, cleanup: :cleaned, cleaned_at_ms: now_ms})
    next_state = put_execution(state, Map.put(updated, :retirement, retirement))

    with :ok <- validate_state(next_state),
         do: {:ok, next_state, :retired}
  end

  defp finish_unsubmitted_retirement(state, %{status: :retired, cleanup: :cleaned, retirement: %{retired_at_ms: at_ms} = retirement}, evidence, now_ms) do
    if Map.delete(retirement, :retired_at_ms) == evidence and at_ms <= now_ms,
      do: {:ok, state, :already_retired},
      else: {:error, :unsubmitted_retirement_conflict}
  end

  defp finish_unsubmitted_retirement(_state, _execution, _evidence, _now_ms),
    do: {:error, :unsubmitted_retirement_conflict}

  defp unsubmitted_retirement_lease?(execution) do
    execution.ownership == :reconciled and not termination_unconfirmed?(execution) and
      case Map.values(execution.leases) do
        [lease] -> untouched_released_lease?(lease)
        _ -> false
      end
  end

  defp untouched_released_lease?(lease) do
    lease.status == :released and lease.release_reason in [:claim_not_submitted, "claim_not_submitted"] and
      lease.head == "unobserved" and lease.last_heartbeat_at == 0 and
      is_nil(Map.get(lease, :supervisor_identity)) and not Map.get(lease, :termination_required, false)
  end

  defp valid_unsubmitted_retirement_evidence?(evidence) do
    is_binary(Map.get(evidence, :issue_id)) and positive_integer?(Map.get(evidence, :generation)) and
      present_string?(Map.get(evidence, :linear_state)) and
      present_string?(Map.get(evidence, :provider_projection_id)) and
      present_string?(Map.get(evidence, :evidence_ref)) and
      Enum.all?([:provider_claim, :active_process, :local_claim, :workspace], &(Map.get(evidence, &1) == :absent)) and
      Enum.sort(Map.keys(evidence)) == @retirement_evidence_keys
  end

  @doc "Returns a sanitized, deterministic projection for operator/API observability."
  @spec snapshot(state()) :: map() | {:error, :invalid_state}
  def snapshot(state) do
    case validate_state(state) do
      :ok ->
        %{
          schema_version: @schema_version,
          executions:
            state.executions
            |> Map.values()
            |> Enum.sort_by(&execution_sort_key/1)
            |> Enum.map(&sanitize_execution/1),
          sessions:
            state.sessions
            |> Map.values()
            |> Enum.sort_by(&session_sort_key/1)
            |> Enum.map(&sanitize_session/1),
          history: Enum.map(state.history, &sanitize_execution/1),
          triage_records:
            state.triage_records
            |> Map.values()
            |> Enum.sort_by(&triage_sort_key/1)
            |> Enum.map(&sanitize_triage_record/1)
        }

      {:error, :invalid_state} = error ->
        error
    end
  end

  @doc "Admits one mutable generation for an issue and repository."
  @spec admit(state(), map(), non_neg_integer()) ::
          {:ok, state(), token()} | {:error, atom() | tuple()}
  def admit(state, attrs, now_ms) do
    with :ok <- validate_state(state),
         :ok <- validate_admission(attrs, now_ms),
         :ok <- admission_allowed(state, attrs) do
      issue_id = attrs.issue_id
      previous = Map.get(state.executions, issue_id)
      generation = if previous, do: previous.generation + 1, else: 1

      execution = %{
        issue_id: issue_id,
        repository: attrs.repository,
        worker_host: Map.get(attrs, :worker_host),
        generation: generation,
        branch: attrs.branch,
        worktree: attrs.worktree,
        status: :active,
        ownership: :reconciled,
        leases: %{},
        terminal: nil,
        cleanup: :pending,
        cleanup_receipt: nil,
        termination_unconfirmed: false,
        admitted_at_ms: now_ms
      }

      next_state =
        state
        |> archive_previous_execution(previous)
        |> remove_previous_sessions(previous)
        |> put_in([:executions, issue_id], execution)

      {:ok, next_state, token(issue_id, generation)}
    end
  end

  @doc "Registers or renews an explicitly identified worker or reviewer lease."
  @spec register(state(), token(), :worker | :reviewer, map(), non_neg_integer()) ::
          {:ok, state(), :registered | :already_registered} | {:error, term()}
  def register(state, token, role, attrs, now_ms) do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         :ok <- active_execution(execution),
         :ok <- validate_registration(role, attrs, now_ms),
         :ok <- session_available(state, token, attrs.session_id),
         :ok <- worker_available(execution, role, attrs.session_id) do
      session =
        Map.merge(
          %{
            issue_id: execution.issue_id,
            repository: execution.repository,
            generation: execution.generation,
            role: role,
            session_id: attrs.session_id,
            process_id: attrs.process_id,
            branch: attrs.branch,
            worktree: attrs.worktree,
            status: :active,
            registered_at_ms: now_ms,
            termination_required: false
          },
          Map.take(attrs, [:linear_state, :pr_state, :head, :last_heartbeat_at])
        )

      registration_result(state, execution, session)
    end
  end

  @doc "Persists the OS supervisor identity after a process is admitted."
  @spec record_supervisor(state(), token(), String.t(), map()) ::
          {:ok, state()} | {:error, term()}
  def record_supervisor(state, token, session_id, identity)
      when is_binary(session_id) and is_map(identity) do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         %{status: :active} = lease <- Map.get(execution.leases, session_id),
         :ok <- validate_supervisor_identity(identity, execution, lease) do
      {:ok, put_lease(state, execution, Map.put(lease, :supervisor_identity, identity))}
    else
      nil -> {:error, :unknown_session}
      {:error, _reason} = error -> error
      _ -> {:error, :supervisor_registration_rejected}
    end
  end

  def record_supervisor(_state, _token, _session_id, _identity),
    do: {:error, :invalid_supervisor_identity}

  @doc "Renews a live lease; terminal or stale generations cannot renew."
  @spec heartbeat(state(), token(), String.t(), non_neg_integer()) ::
          {:ok, state()} | {:error, term()}
  def heartbeat(state, token, session_id, now_ms)
      when is_binary(session_id) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         :ok <- active_execution(execution),
         %{status: :active} = lease <- Map.get(execution.leases, session_id) do
      updated_lease = Map.put(lease, :last_heartbeat_at, now_ms)
      {:ok, put_lease(state, execution, updated_lease)}
    else
      nil -> {:error, :unknown_session}
      {:error, _reason} = error -> error
      _ -> {:error, :unknown_session}
    end
  end

  def heartbeat(_state, _token, _session_id, _now_ms), do: {:error, :invalid_session}

  @doc "Persists an exact head observation for a live generation-bound session."
  @spec observe_session_head(state(), token(), String.t(), String.t(), non_neg_integer()) ::
          {:ok, state()} | {:error, term()}
  def observe_session_head(state, token, session_id, head, now_ms)
      when is_binary(session_id) and is_binary(head) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         :ok <- active_execution(execution),
         %{status: :active} = lease <- Map.get(execution.leases, session_id),
         true <- present_string?(head) do
      updated_lease =
        lease
        |> Map.put(:head, head)
        |> Map.put(:last_heartbeat_at, now_ms)

      {:ok, put_lease(state, execution, updated_lease)}
    else
      nil -> {:error, :unknown_session}
      false -> {:error, :invalid_head}
      {:error, _reason} = error -> error
      _ -> {:error, :unknown_session}
    end
  end

  def observe_session_head(_state, _token, _session_id, _head, _now_ms),
    do: {:error, :invalid_session_head}

  @doc "Records one idempotent triage record for a post-terminal head divergence."
  @spec record_head_divergence(state(), token(), String.t(), String.t(), non_neg_integer()) ::
          {:ok, state(), :recorded | :already_recorded} | {:error, term()}
  def record_head_divergence(state, token, expected_head, observed_head, now_ms)
      when is_binary(expected_head) and is_binary(observed_head) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         :ok <- terminal_execution(execution),
         true <- present_string?(expected_head),
         true <- present_string?(observed_head) do
      triage_id = triage_id(execution)

      case Map.get(state.triage_records, triage_id) do
        nil ->
          record = %{
            id: triage_id,
            type: :post_terminal_head_divergence,
            issue_id: execution.issue_id,
            repository: execution.repository,
            generation: execution.generation,
            branch: execution.branch,
            worktree: execution.worktree,
            expected_head: expected_head,
            observed_head: observed_head,
            detected_at_ms: now_ms
          }

          next_state = %{state | triage_records: Map.put(state.triage_records, triage_id, record)}
          {:ok, next_state, :recorded}

        existing ->
          if same_triage_incident?(existing, execution, expected_head) do
            {:ok, state, :already_recorded}
          else
            {:error, :triage_conflict}
          end
      end
    else
      false -> {:error, :invalid_head}
      {:error, _reason} = error -> error
      _ -> {:error, :not_terminal}
    end
  end

  def record_head_divergence(_state, _token, _expected_head, _observed_head, _now_ms),
    do: {:error, :invalid_triage_record}

  @doc "Releases one generation-bound lease. Releasing twice is harmless."
  @spec release(state(), token(), String.t(), atom()) ::
          {:ok, state(), :released | :already_released} | {:error, term()}
  def release(state, token, session_id, reason \\ :released)

  @spec release(state(), token(), String.t(), atom()) ::
          {:ok, state(), :released | :already_released} | {:error, term()}
  def release(state, token, session_id, reason)
      when is_binary(session_id) and is_atom(reason) do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         %{status: status} = lease when status in [:active, :released, :expired] <-
           Map.get(execution.leases, session_id) do
      if status == :active do
        updated_lease =
          lease
          |> Map.put(:status, :released)
          |> Map.put(:release_reason, reason)
          |> Map.put(:termination_required, reason == :orchestrator_stop)

        next_state = put_lease(state, execution, updated_lease)

        if reason == :orchestrator_stop do
          updated_execution =
            next_state
            |> get_in([:executions, execution.issue_id])
            |> Map.merge(%{ownership: :unknown, termination_unconfirmed: true})

          {:ok, put_execution(next_state, updated_execution), :released}
        else
          {:ok, next_state, :released}
        end
      else
        if reason == :orchestrator_stop and not Map.get(lease, :termination_required, false) do
          upgraded_lease = Map.put(lease, :termination_required, true)
          next_state = put_lease(state, execution, upgraded_lease)

          updated_execution =
            next_state
            |> get_in([:executions, execution.issue_id])
            |> Map.merge(%{ownership: :unknown, termination_unconfirmed: true})

          {:ok, put_execution(next_state, updated_execution), :already_released}
        else
          {:ok, state, :already_released}
        end
      end
    else
      nil -> {:ok, state, :already_released}
      {:error, _reason} = error -> error
      _ -> {:error, :unknown_session}
    end
  end

  def release(_state, _token, _session_id, _reason), do: {:error, :invalid_session}

  @doc "Confirms a generation-bound process tree is terminated from explicit evidence."
  @spec confirm_termination(state(), token(), String.t(), map(), non_neg_integer()) ::
          {:ok, state(), :confirmed | :already_confirmed} | {:error, term()}
  def confirm_termination(state, token, session_id, evidence, now_ms)
      when is_binary(session_id) and is_map(evidence) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         %{status: status} = lease when status in [:expired, :released] <-
           Map.get(execution.leases, session_id),
         :ok <- termination_confirmation_required(lease),
         :ok <- validate_termination_evidence(lease, session_id, evidence, now_ms) do
      if Map.get(lease, :termination_confirmed_at_ms) do
        {:ok, state, :already_confirmed}
      else
        confirmed_lease =
          lease
          |> Map.put(:termination_confirmed_at_ms, now_ms)
          |> Map.put(:termination_evidence_ref, evidence.evidence_ref)
          |> Map.put(:termination_evidence, evidence)

        confirmed_state = put_lease(state, execution, confirmed_lease)
        confirmed_execution = get_in(confirmed_state, [:executions, execution.issue_id])

        if execution_requires_termination?(confirmed_execution) do
          {:ok, confirmed_state, :confirmed}
        else
          updated_execution =
            confirmed_execution
            |> Map.put(:termination_unconfirmed, false)
            |> maybe_reconcile_confirmed_ownership()

          {:ok, put_execution(confirmed_state, updated_execution), :confirmed}
        end
      end
    else
      nil -> {:error, :unknown_session}
      {:error, _reason} = error -> error
      _ -> {:error, :termination_not_confirmable}
    end
  end

  def confirm_termination(_state, _token, _session_id, _evidence, _now_ms),
    do: {:error, :invalid_termination_confirmation}

  @doc false
  @spec confirm_termination(state(), token(), map(), non_neg_integer()) ::
          {:ok, state(), :confirmed | :already_confirmed} | {:error, term()}
  def confirm_termination(state, token, evidence, now_ms) when is_map(evidence) do
    case Map.get(evidence, :session_id) do
      session_id when is_binary(session_id) ->
        confirm_termination(state, token, session_id, evidence, now_ms)

      _ ->
        {:error, :invalid_termination_confirmation}
    end
  end

  @doc "Begins a durable, replayable filesystem cleanup phase."
  @spec prepare_cleanup(state(), token(), String.t(), non_neg_integer()) ::
          {:ok, state(), :prepared | :already_prepared} | {:error, term()}
  def prepare_cleanup(state, token, expected_head, now_ms), do: prepare_cleanup(state, token, expected_head, now_ms, nil)

  @spec prepare_cleanup(state(), token(), String.t(), non_neg_integer(), atom() | nil) ::
          {:ok, state(), :prepared | :already_prepared} | {:error, term()}
  def prepare_cleanup(state, token, expected_head, now_ms, terminal_outcome)
      when is_binary(expected_head) and is_integer(now_ms) and now_ms >= 0 and
             terminal_outcome in [nil, :completed, :failed, :blocked] do
    with :ok <- validate_cleanup(state, token, expected_head),
         {:ok, execution} <- current_execution(state, token) do
      receipt = Map.get(execution, :cleanup_receipt)

      cond do
        execution.cleanup == :cleaned ->
          {:ok, state, :already_prepared}

        match?(%{phase: :removal_started}, receipt) and receipt.expected_head == expected_head ->
          {:ok, state, :already_prepared}

        is_nil(receipt) ->
          receipt = %{
            phase: :removal_started,
            expected_head: expected_head,
            prepared_at_ms: now_ms
          }

          receipt =
            if is_nil(terminal_outcome),
              do: receipt,
              else: Map.put(receipt, :terminal_outcome, terminal_outcome)

          {:ok, put_execution(state, Map.put(execution, :cleanup_receipt, receipt)), :prepared}

        true ->
          {:error, :cleanup_conflict}
      end
    end
  end

  def prepare_cleanup(_state, _token, _expected_head, _now_ms, _terminal_outcome),
    do: {:error, :invalid_cleanup}

  @doc "Persists independent cleanup evidence before filesystem removal begins."
  @spec record_cleanup_evidence(state(), token(), String.t(), String.t(), non_neg_integer()) ::
          {:ok, state()} | {:error, term()}
  def record_cleanup_evidence(state, token, expected_head, evidence_ref, now_ms)
      when is_binary(expected_head) and is_binary(evidence_ref) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_cleanup(state, token, expected_head),
         true <- present_string?(evidence_ref),
         {:ok, execution} <- current_execution(state, token),
         %{phase: :removal_started, expected_head: ^expected_head} = receipt <- Map.get(execution, :cleanup_receipt) do
      next_receipt =
        receipt
        |> Map.put(:evidence_ref, evidence_ref)
        |> Map.put(:evidence_recorded_at_ms, now_ms)

      {:ok, put_execution(state, Map.put(execution, :cleanup_receipt, next_receipt))}
    else
      false -> {:error, :invalid_cleanup_evidence}
      nil -> {:error, :cleanup_not_prepared}
      {:error, _reason} = error -> error
      _ -> {:error, :cleanup_not_prepared}
    end
  end

  def record_cleanup_evidence(_state, _token, _expected_head, _evidence_ref, _now_ms),
    do: {:error, :invalid_cleanup_evidence}

  defp registration_result(state, execution, session) do
    case Map.get(state.sessions, session.session_id) do
      nil ->
        {:ok, put_lease(state, execution, session), :registered}

      existing ->
        refresh_registration(state, execution, existing, session)
    end
  end

  defp refresh_registration(state, execution, existing, session) do
    if same_registration?(existing, session) do
      updated =
        Map.merge(
          existing,
          Map.take(session, [:last_heartbeat_at, :linear_state, :pr_state, :head])
        )

      {:ok, put_lease(state, execution, updated), :already_registered}
    else
      {:error, :registration_conflict}
    end
  end

  @doc "Fences a generation on terminal tracker/merge observation."
  @spec fence(state(), token(), map(), non_neg_integer()) ::
          {:ok, state(), :fenced | :already_fenced} | {:error, term()}
  def fence(state, token, attrs, now_ms) do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         :ok <- validate_terminal(attrs, now_ms) do
      terminal = %{
        state: attrs.terminal_state,
        accepted_head: attrs.accepted_head,
        merge_identity: Map.get(attrs, :merge_identity),
        observed_at_ms: now_ms
      }

      terminal =
        if Map.has_key?(attrs, :failure_evidence_ref),
          do: Map.put(terminal, :failure_evidence_ref, attrs.failure_evidence_ref),
          else: terminal

      fence_execution(state, execution, terminal)
    end
  end

  defp fence_execution(state, %{status: :active} = execution, terminal) do
    updated = %{execution | status: :terminal, terminal: terminal, cleanup: :pending}
    {:ok, put_execution(state, updated), :fenced}
  end

  defp fence_execution(state, %{status: :terminal, terminal: existing}, terminal) do
    if same_terminal?(existing, terminal) do
      {:ok, state, :already_fenced}
    else
      {:error, :terminal_conflict}
    end
  end

  @doc "Guards commit, push, and tracker-state mutations for one live generation."
  @spec authorize(state(), token(), atom()) :: {:ok, map()} | {:error, term()}
  def authorize(state, token, action) when action in @mutable_actions do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token),
         :ok <- active_execution(execution),
         :ok <- reconciled_ownership(execution) do
      {:ok, %{issue_id: execution.issue_id, generation: execution.generation, action: action}}
    end
  end

  def authorize(_state, _token, _action), do: {:error, :unsupported_action}

  @doc """
  Approves generation-bound cleanup only after terminal fencing, quiescence,
  exact-head reconciliation, and released/expired leases.
  """
  @spec validate_cleanup(state(), token(), String.t()) :: :ok | {:error, term()}
  def validate_cleanup(state, token, expected_head) when is_binary(expected_head) do
    with :ok <- validate_state(state),
         {:ok, execution} <- current_execution(state, token) do
      cond do
        execution.status != :terminal ->
          {:error, :not_terminal}

        termination_unconfirmed?(execution) ->
          {:error, :ownership_unreconciled}

        execution.ownership != :reconciled ->
          {:error, :ownership_unreconciled}

        active_lease_ids(execution) != [] ->
          {:error, {:leases_active, active_lease_ids(execution)}}

        execution.terminal.accepted_head != expected_head ->
          {:error, :head_diverged}

        true ->
          :ok
      end
    end
  end

  def validate_cleanup(_state, _token, _expected_head), do: {:error, :invalid_cleanup}

  @doc """
  Persists a terminal cleanup marker after its filesystem postconditions are verified.
  """
  @spec cleanup(state(), token(), String.t(), non_neg_integer()) ::
          {:ok, state(), :cleaned | :already_cleaned} | {:error, term()}
  def cleanup(state, token, expected_head, now_ms)
      when is_binary(expected_head) and is_integer(now_ms) and now_ms >= 0 do
    with :ok <- validate_cleanup(state, token, expected_head),
         {:ok, execution} <- current_execution(state, token) do
      if execution.cleanup == :cleaned do
        {:ok, state, :already_cleaned}
      else
        receipt =
          execution
          |> Map.get(:cleanup_receipt)
          |> verified_cleanup_receipt(expected_head, now_ms)

        updated =
          execution
          |> Map.put(:cleanup, :cleaned)
          |> Map.put(:cleaned_at_ms, now_ms)
          |> Map.put(:cleanup_receipt, receipt)

        {:ok, put_execution(state, updated), :cleaned}
      end
    end
  end

  @doc """
  Reconciles an explicit session snapshot. Unknown, contradictory, stale, or
  missing ownership blocks the affected execution; leases older than the
  supplied TTL are expired deterministically.
  """
  @spec reconcile_sessions(state(), [map()], non_neg_integer(), pos_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def reconcile_sessions(state, observations, now_ms, ttl_ms \\ @default_lease_ttl_ms)

  @spec reconcile_sessions(state(), [map()], non_neg_integer(), pos_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def reconcile_sessions(state, observations, now_ms, ttl_ms)
      when is_list(observations) and is_integer(now_ms) and now_ms >= 0 and
             is_integer(ttl_ms) and ttl_ms > 0 do
    reconcile_session_set(state, observations, now_ms, ttl_ms, MapSet.new())
  end

  def reconcile_sessions(_state, _observations, _now_ms, _ttl_ms),
    do: {:error, :invalid_reconciliation_input}

  @doc "Preserves journal-proven pre-spawn leases from missing-heartbeat expiry; actual observations still dominate."
  @spec reconcile_claim_sessions(state(), [map()], [map()], non_neg_integer(), pos_integer()) ::
          {:ok, state(), map()} | {:error, term()}
  def reconcile_claim_sessions(state, observations, claims, now_ms, ttl_ms)
      when is_list(observations) and is_list(claims) and is_integer(now_ms) and now_ms >= 0 and
             is_integer(ttl_ms) and ttl_ms > 0 do
    with :ok <- validate_state(state),
         {:ok, observations} <- canonical_observations(observations) do
      protected = protected_claim_keys(state, observations, claims)
      reconcile_session_set(state, observations, now_ms, ttl_ms, protected)
    end
  end

  def reconcile_claim_sessions(_state, _observations, _claims, _now_ms, _ttl_ms),
    do: {:error, :invalid_reconciliation_input}

  defp protected_claim_keys(state, observations, claims) do
    Enum.reduce(claims, MapSet.new(), fn
      %{issue_id: issue_id, generation: generation, session_id: session_id} = claim, protected ->
        observed = Enum.any?(observations, &(&1.issue_id == issue_id))

        if not observed and match?({:ok, _}, reconcile_unstarted_claim(state, claim)) do
          MapSet.put(protected, {issue_id, generation, session_id})
        else
          protected
        end

      _claim, protected ->
        protected
    end)
  end

  defp reconcile_session_set(state, observations, now_ms, ttl_ms, protected) do
    with :ok <- validate_state(state),
         {:ok, observations} <- canonical_observations(observations) do
      initial = %{state | executions: reset_ownership(state.executions)}

      {reconciled_state, summary, seen} =
        Enum.reduce(observations, {initial, empty_summary(), protected}, fn observation, {state_acc, summary_acc, seen_acc} ->
          reconcile_observation(state_acc, summary_acc, seen_acc, observation, now_ms, ttl_ms)
        end)

      {final_state, final_summary} =
        expire_or_block_missing_leases(reconciled_state, summary, seen, now_ms, ttl_ms)

      {:ok, final_state, final_summary |> add_unconfirmed_reasons(final_state) |> finalize_summary()}
    end
  end

  defp validate_state(%{
         schema_version: @schema_version,
         executions: executions,
         sessions: sessions,
         history: history,
         triage_records: triage_records
       })
       when is_map(executions) and is_map(sessions) and is_list(history) and is_map(triage_records) do
    if Enum.all?(executions, fn {issue_id, execution} -> valid_execution?(issue_id, execution) end) and
         Enum.all?(sessions, fn {session_id, session} -> valid_lease?(session_id, session) end) and
         valid_session_registry?(executions, sessions) and
         Enum.all?(history, &valid_history_execution?/1) and
         valid_triage_records?(triage_records) do
      :ok
    else
      {:error, :invalid_state}
    end
  end

  defp validate_state(_state), do: {:error, :invalid_state}

  defp valid_execution?(issue_id, execution) when is_binary(issue_id) and is_map(execution) do
    valid_execution_identity?(issue_id, execution) and
      valid_execution_status?(execution) and valid_execution_leases?(execution) and
      valid_terminal_consistency?(execution) and valid_cleanup_consistency?(execution) and
      valid_termination_consistency?(execution) and valid_retirement_consistency?(execution)
  end

  defp valid_execution?(_issue_id, _execution), do: false

  defp valid_session_registry?(executions, sessions) do
    Enum.all?(sessions, fn {session_id, session} ->
      session_in_execution?(executions, session_id, session)
    end) and
      Enum.all?(executions, fn {_issue_id, execution} ->
        Enum.all?(execution.leases, fn {session_id, lease} ->
          Map.get(sessions, session_id) == lease
        end)
      end)
  end

  defp session_in_execution?(executions, session_id, session) do
    case Map.get(executions, session.issue_id) do
      %{generation: generation, leases: leases} when generation == session.generation ->
        Map.get(leases, session_id) == session

      _ ->
        false
    end
  end

  defp valid_execution_identity?(issue_id, execution) do
    Map.get(execution, :issue_id) == issue_id and
      present_string?(Map.get(execution, :repository)) and
      optional_string?(Map.get(execution, :worker_host)) and
      positive_integer?(Map.get(execution, :generation)) and
      present_string?(Map.get(execution, :branch)) and
      present_string?(Map.get(execution, :worktree)) and
      non_negative_integer?(Map.get(execution, :admitted_at_ms)) and
      optional_non_negative_integer?(Map.get(execution, :cleaned_at_ms)) and
      Map.get(execution, :termination_unconfirmed, false) in [true, false] and
      valid_cleanup_receipt?(Map.get(execution, :cleanup_receipt))
  end

  defp valid_history_execution?(execution) when is_map(execution) do
    case Map.get(execution, :issue_id) do
      issue_id when is_binary(issue_id) -> valid_execution?(issue_id, execution)
      _ -> false
    end
  end

  defp valid_history_execution?(_execution), do: false

  defp valid_triage_records?(records) when is_map(records) do
    Enum.all?(records, fn {triage_id, record} -> valid_triage_record?(triage_id, record) end)
  end

  defp valid_triage_records?(_records), do: false

  defp valid_triage_record?(triage_id, record) when is_binary(triage_id) and is_map(record) do
    Map.get(record, :id) == triage_id and
      Map.get(record, :type) == :post_terminal_head_divergence and
      present_string?(Map.get(record, :issue_id)) and
      present_string?(Map.get(record, :repository)) and
      positive_integer?(Map.get(record, :generation)) and
      present_string?(Map.get(record, :branch)) and
      present_string?(Map.get(record, :worktree)) and
      present_string?(Map.get(record, :expected_head)) and
      present_string?(Map.get(record, :observed_head)) and
      non_negative_integer?(Map.get(record, :detected_at_ms)) and
      triage_id == triage_id_for(Map.get(record, :issue_id), Map.get(record, :generation))
  end

  defp valid_triage_record?(_triage_id, _record), do: false

  defp valid_execution_status?(execution) do
    valid_status_cleanup_pair?(Map.get(execution, :status), Map.get(execution, :cleanup)) and
      Map.get(execution, :ownership) in [:reconciled, :unknown, :contradictory]
  end

  defp valid_status_cleanup_pair?(:active, :pending), do: true
  defp valid_status_cleanup_pair?(:terminal, cleanup), do: cleanup in [:pending, :cleaned]
  defp valid_status_cleanup_pair?(:retired, :cleaned), do: true
  defp valid_status_cleanup_pair?(_status, _cleanup), do: false

  defp valid_execution_leases?(execution) do
    leases = Map.get(execution, :leases)

    is_map(leases) and
      Enum.all?(leases, fn {session_id, lease} ->
        valid_lease?(session_id, lease) and
          lease.issue_id == execution.issue_id and
          lease.repository == execution.repository and
          lease.generation == execution.generation and
          lease.branch == execution.branch and
          lease.worktree == execution.worktree
      end)
  end

  defp valid_terminal_consistency?(%{status: :active, terminal: nil}), do: true
  defp valid_terminal_consistency?(%{status: :retired, terminal: nil}), do: true

  defp valid_terminal_consistency?(%{status: :terminal, terminal: terminal}),
    do: is_map(terminal) and valid_terminal?(terminal)

  defp valid_terminal_consistency?(_execution), do: false

  defp valid_retirement_consistency?(%{status: :retired, retirement: retirement} = execution)
       when is_map(retirement) do
    evidence = Map.delete(retirement, :retired_at_ms)

    valid_unsubmitted_retirement_evidence?(evidence) and
      evidence.issue_id == execution.issue_id and evidence.generation == execution.generation and
      non_negative_integer?(Map.get(retirement, :retired_at_ms)) and
      execution.cleanup_receipt == nil and execution.cleaned_at_ms == retirement.retired_at_ms and
      match?(
        [%{status: :released, release_reason: reason}] when reason in [:claim_not_submitted, "claim_not_submitted"],
        Map.values(execution.leases)
      )
  end

  defp valid_retirement_consistency?(%{status: :retired}), do: false
  defp valid_retirement_consistency?(execution), do: is_nil(Map.get(execution, :retirement))

  defp valid_lease?(session_id, lease) when is_binary(session_id) and is_map(lease) do
    valid_lease_identity?(session_id, lease) and valid_lease_scope?(lease) and
      valid_lease_status?(lease) and valid_lease_clock?(lease)
  end

  defp valid_lease?(_session_id, _lease), do: false

  defp valid_lease_identity?(session_id, lease) do
    Map.get(lease, :session_id) == session_id and
      present_string?(Map.get(lease, :issue_id)) and
      present_string?(Map.get(lease, :process_id))
  end

  defp valid_lease_scope?(lease) do
    present_string?(Map.get(lease, :repository)) and
      positive_integer?(Map.get(lease, :generation)) and
      Map.get(lease, :role) in @roles and
      present_string?(Map.get(lease, :branch)) and
      present_string?(Map.get(lease, :worktree))
  end

  defp valid_lease_status?(lease) do
    Map.get(lease, :status) in [:active, :released, :expired] and
      present_string?(Map.get(lease, :linear_state)) and
      present_string?(Map.get(lease, :pr_state)) and
      present_string?(Map.get(lease, :head))
  end

  defp valid_lease_clock?(lease) do
    non_negative_integer?(Map.get(lease, :last_heartbeat_at)) and
      non_negative_integer?(Map.get(lease, :registered_at_ms)) and
      Map.get(lease, :termination_required, false) in [true, false] and
      optional_non_negative_integer?(Map.get(lease, :termination_confirmed_at_ms)) and
      optional_string?(Map.get(lease, :termination_evidence_ref)) and
      valid_termination_evidence?(Map.get(lease, :termination_evidence)) and
      valid_supervisor_identity?(Map.get(lease, :supervisor_identity))
  end

  defp valid_supervisor_identity?(nil), do: true

  defp valid_supervisor_identity?(identity) when is_map(identity) do
    Map.get(identity, :supervisor) == :systemd_user and
      valid_supervisor_unit?(Map.get(identity, :unit)) and
      present_string?(Map.get(identity, :issue_id)) and
      positive_integer?(Map.get(identity, :generation)) and
      present_string?(Map.get(identity, :session_id)) and
      present_string?(Map.get(identity, :process_id)) and
      non_negative_integer?(Map.get(identity, :launched_at_ms))
  end

  defp valid_supervisor_identity?(_identity), do: false

  defp valid_termination_evidence?(nil), do: true

  defp valid_termination_evidence?(evidence) when is_map(evidence) do
    present_string?(Map.get(evidence, :session_id)) and
      present_string?(Map.get(evidence, :process_id)) and
      Map.get(evidence, :process_tree) == :terminated and
      present_string?(Map.get(evidence, :evidence_ref)) and
      non_negative_integer?(Map.get(evidence, :observed_at_ms)) and
      optional_string?(Map.get(evidence, :active_state)) and
      (is_nil(Map.get(evidence, :active_state)) or Map.get(evidence, :active_state) == "inactive") and
      optional_non_negative_integer?(Map.get(evidence, :remaining_processes)) and
      (is_nil(Map.get(evidence, :remaining_processes)) or Map.get(evidence, :remaining_processes) == 0) and
      valid_termination_evidence_supervisor?(Map.get(evidence, :supervisor))
  end

  defp valid_termination_evidence?(_evidence), do: false

  defp valid_termination_evidence_supervisor?(nil), do: true
  defp valid_termination_evidence_supervisor?(:systemd_user), do: true
  defp valid_termination_evidence_supervisor?(_supervisor), do: false

  defp valid_supervisor_unit?(unit) when is_binary(unit) do
    byte_size(unit) <= 180 and Regex.match?(~r/\Asymphony-exec-[a-f0-9]+\.scope\z/, unit)
  end

  defp valid_supervisor_unit?(_unit), do: false

  defp validate_supervisor_identity(identity, execution, lease) do
    if valid_supervisor_identity?(identity) and
         attested_supervisor_identity?(identity) and
         identity.issue_id == execution.issue_id and
         identity.generation == execution.generation and
         identity.session_id == lease.session_id and
         identity.process_id == lease.process_id do
      :ok
    else
      {:error, :supervisor_identity_mismatch}
    end
  end

  defp attested_supervisor_identity?(identity) when is_map(identity) do
    is_binary(Map.get(identity, :control_group)) and
      Map.get(identity, :control_group) != "" and
      is_list(Map.get(identity, :launch_processes)) and
      Map.get(identity, :launch_processes) != [] and
      Enum.all?(Map.get(identity, :launch_processes), &(is_integer(&1) and &1 > 0)) and
      optional_positive_integer?(Map.get(identity, :main_pid))
  end

  defp attested_supervisor_identity?(_identity), do: false

  defp valid_cleanup_receipt?(nil), do: true

  defp valid_cleanup_receipt?(receipt) when is_map(receipt) do
    Map.get(receipt, :phase) in [:removal_started, :verified] and
      present_string?(Map.get(receipt, :expected_head)) and
      non_negative_integer?(Map.get(receipt, :prepared_at_ms)) and
      optional_non_negative_integer?(Map.get(receipt, :verified_at_ms)) and
      optional_string?(Map.get(receipt, :evidence_ref)) and
      optional_non_negative_integer?(Map.get(receipt, :evidence_recorded_at_ms)) and
      (Map.get(receipt, :phase) != :verified or
         non_negative_integer?(Map.get(receipt, :verified_at_ms))) and
      optional_terminal_outcome?(Map.get(receipt, :terminal_outcome))
  end

  defp valid_cleanup_receipt?(_receipt), do: false

  defp optional_terminal_outcome?(nil), do: true
  defp optional_terminal_outcome?(outcome), do: outcome in [:completed, :failed, :blocked]

  defp valid_cleanup_consistency?(execution) when is_map(execution) do
    case {Map.get(execution, :cleanup), Map.get(execution, :cleanup_receipt)} do
      {:pending, nil} -> true
      {:pending, %{phase: :removal_started}} -> true
      {:cleaned, %{phase: :verified}} -> true
      {:cleaned, nil} -> true
      _ -> false
    end
  end

  defp valid_cleanup_consistency?(_execution), do: false

  defp valid_termination_consistency?(execution) when is_map(execution) do
    not execution_requires_termination?(execution) or
      Map.get(execution, :termination_unconfirmed, false)
  end

  defp valid_termination_consistency?(_execution), do: false

  defp validate_termination_evidence(lease, session_id, evidence, now_ms) do
    cond do
      Map.get(evidence, :session_id) != session_id ->
        {:error, :termination_session_mismatch}

      Map.get(evidence, :process_id) != lease.process_id ->
        {:error, :termination_process_mismatch}

      Map.get(evidence, :process_tree) != :terminated ->
        {:error, :termination_not_proven}

      not present_string?(Map.get(evidence, :evidence_ref)) ->
        {:error, :termination_evidence_missing}

      not non_negative_integer?(Map.get(evidence, :observed_at_ms)) or
          Map.get(evidence, :observed_at_ms) > now_ms ->
        {:error, :invalid_termination_timestamp}

      not supervised_evidence_matches?(lease, evidence) ->
        {:error, :termination_supervisor_mismatch}

      true ->
        :ok
    end
  end

  defp supervised_evidence_matches?(lease, evidence) do
    case Map.get(lease, :supervisor_identity) do
      nil ->
        true

      identity ->
        attested_supervisor_identity?(identity) and
          Map.get(evidence, :supervisor) == :systemd_user and
          Map.get(evidence, :unit) == Map.get(identity, :unit) and
          Map.get(evidence, :pre_control_group) == Map.get(identity, :control_group) and
          is_list(Map.get(evidence, :pre_processes)) and
          Map.get(evidence, :pre_processes) != [] and
          Map.get(evidence, :active_state) == "inactive" and
          Map.get(evidence, :remaining_processes) == 0
    end
  end

  defp maybe_reconcile_confirmed_ownership(%{ownership: :contradictory} = execution),
    do: execution

  defp maybe_reconcile_confirmed_ownership(execution),
    do: Map.put(execution, :ownership, :reconciled)

  defp verified_cleanup_receipt(nil, expected_head, now_ms) do
    %{
      phase: :verified,
      expected_head: expected_head,
      prepared_at_ms: now_ms,
      verified_at_ms: now_ms
    }
  end

  defp verified_cleanup_receipt(receipt, expected_head, now_ms) do
    receipt
    |> Map.put(:phase, :verified)
    |> Map.put(:expected_head, expected_head)
    |> Map.put(:verified_at_ms, now_ms)
  end

  defp valid_terminal?(nil), do: true

  defp valid_terminal?(terminal) when is_map(terminal) do
    present_string?(Map.get(terminal, :state)) and
      present_string?(Map.get(terminal, :accepted_head)) and
      optional_string?(Map.get(terminal, :merge_identity)) and
      valid_failure_evidence?(terminal, :state) and
      is_integer(Map.get(terminal, :observed_at_ms)) and Map.get(terminal, :observed_at_ms) >= 0
  end

  defp valid_terminal?(_terminal), do: false

  defp valid_failure_evidence?(value, state_key) do
    case Map.fetch(value, :failure_evidence_ref) do
      :error ->
        true

      {:ok, ref} when is_binary(ref) ->
        Map.get(value, state_key) == "Failed attempt" and Regex.match?(~r/\Asha256:[0-9a-f]{64}\z/, ref)

      _ ->
        false
    end
  end

  defp execution_sort_key(execution), do: {execution.issue_id, execution.generation}

  defp session_sort_key(session), do: {session.issue_id, session.generation, session.session_id}

  defp triage_sort_key(record), do: record.id

  defp sanitize_execution(execution) do
    execution
    |> Map.take([
      :issue_id,
      :repository,
      :worker_host,
      :generation,
      :branch,
      :worktree,
      :status,
      :ownership,
      :cleanup,
      :termination_unconfirmed,
      :admitted_at_ms,
      :cleaned_at_ms
    ])
    |> Map.put(:terminal, sanitize_terminal(execution.terminal))
    |> Map.put(:retirement, Map.get(execution, :retirement))
    |> Map.put(:cleanup_receipt, sanitize_cleanup_receipt(Map.get(execution, :cleanup_receipt)))
    |> Map.put(
      :sessions,
      execution.leases
      |> Map.values()
      |> Enum.sort_by(&session_sort_key/1)
      |> Enum.map(&sanitize_session/1)
    )
  end

  defp sanitize_terminal(nil), do: nil

  defp sanitize_terminal(terminal),
    do: Map.take(terminal, [:state, :accepted_head, :merge_identity, :observed_at_ms])

  defp sanitize_cleanup_receipt(nil), do: nil

  defp sanitize_cleanup_receipt(receipt),
    do:
      Map.take(receipt, [
        :phase,
        :expected_head,
        :prepared_at_ms,
        :verified_at_ms,
        :terminal_outcome,
        :evidence_ref,
        :evidence_recorded_at_ms
      ])

  defp sanitize_session(session) do
    Map.take(session, [
      :issue_id,
      :repository,
      :generation,
      :role,
      :session_id,
      :process_id,
      :branch,
      :worktree,
      :status,
      :registered_at_ms,
      :last_heartbeat_at,
      :linear_state,
      :pr_state,
      :head,
      :release_reason,
      :termination_required,
      :termination_confirmed_at_ms,
      :termination_evidence_ref,
      :termination_evidence,
      :supervisor_identity
    ])
  end

  defp sanitize_triage_record(record) do
    Map.take(record, [
      :id,
      :type,
      :issue_id,
      :repository,
      :generation,
      :branch,
      :worktree,
      :expected_head,
      :observed_head,
      :detected_at_ms
    ])
  end

  defp positive_integer?(value), do: is_integer(value) and value > 0

  defp non_negative_integer?(value), do: is_integer(value) and value >= 0

  defp optional_non_negative_integer?(nil), do: true
  defp optional_non_negative_integer?(value), do: non_negative_integer?(value)

  defp optional_positive_integer?(nil), do: true
  defp optional_positive_integer?(value), do: positive_integer?(value)

  defp validate_admission(attrs, now_ms)
       when is_map(attrs) and is_integer(now_ms) and now_ms >= 0 do
    required = [:issue_id, :repository, :branch, :worktree]

    if Enum.all?(required, &present_string?(Map.get(attrs, &1))) do
      :ok
    else
      {:error, :invalid_admission}
    end
  end

  defp validate_admission(_attrs, _now_ms), do: {:error, :invalid_admission}

  defp admission_allowed(state, attrs) do
    case Map.get(state.executions, attrs.issue_id) do
      %{status: :active} = execution ->
        if quiescent?(execution) do
          repository_execution_blocker(state.executions, attrs.repository)
        else
          {:error, :generation_active}
        end

      %{status: :terminal, cleanup: cleanup} when cleanup != :cleaned ->
        {:error, :execution_not_quiescent}

      _ ->
        repository_execution_blocker(state.executions, attrs.repository)
    end
  end

  defp repository_execution_blocker(executions, repository) do
    Enum.find_value(executions, :ok, fn {issue_id, execution} ->
      if execution.repository == repository and not quiescent?(execution) do
        {:error, {:repository_not_quiescent, issue_id}}
      end
    end)
  end

  defp quiescent?(execution) do
    execution.ownership == :reconciled and not termination_unconfirmed?(execution) and
      active_lease_ids(execution) == [] and
      (execution.status == :active or
         (execution.status in [:terminal, :retired] and execution.cleanup == :cleaned))
  end

  defp archive_previous_execution(state, nil), do: state

  defp archive_previous_execution(state, previous) do
    %{state | history: [previous | state.history]}
  end

  defp remove_previous_sessions(state, nil), do: state

  defp remove_previous_sessions(state, previous) do
    %{state | sessions: Map.drop(state.sessions, Map.keys(previous.leases))}
  end

  defp current_execution(state, %{issue_id: issue_id, generation: generation})
       when is_binary(issue_id) and is_integer(generation) and generation > 0 do
    case Map.get(state.executions, issue_id) do
      nil ->
        {:error, :unknown_execution}

      %{generation: ^generation} = execution ->
        {:ok, execution}

      _execution ->
        {:error, :stale_generation}
    end
  end

  defp current_execution(_state, _token), do: {:error, :invalid_generation_token}

  defp active_execution(%{status: :active}), do: :ok
  defp active_execution(%{status: :terminal}), do: {:error, :terminal_fenced}
  defp active_execution(_execution), do: {:error, :invalid_execution}

  defp terminal_execution(%{status: :terminal}), do: :ok
  defp terminal_execution(_execution), do: {:error, :not_terminal}

  defp reconciled_ownership(%{ownership: :reconciled}), do: :ok
  defp reconciled_ownership(_execution), do: {:error, :ownership_unreconciled}

  defp validate_registration(role, attrs, now_ms)
       when role in @roles and is_map(attrs) and is_integer(now_ms) and now_ms >= 0 do
    required = [
      :session_id,
      :process_id,
      :branch,
      :worktree,
      :linear_state,
      :pr_state,
      :head,
      :last_heartbeat_at
    ]

    if Enum.all?(required, &present_registration_field?(Map.get(attrs, &1))) and
         is_integer(attrs.last_heartbeat_at) and attrs.last_heartbeat_at >= 0 and
         attrs.last_heartbeat_at <= now_ms do
      :ok
    else
      {:error, :invalid_registration}
    end
  end

  defp validate_registration(_role, _attrs, _now_ms), do: {:error, :invalid_registration}

  defp present_registration_field?(value) when is_integer(value), do: value >= 0
  defp present_registration_field?(value), do: present_string?(value)

  defp session_available(state, token, session_id) do
    case Map.get(state.sessions, session_id) do
      nil ->
        :ok

      %{issue_id: issue_id, generation: generation}
      when issue_id == token.issue_id and generation == token.generation ->
        :ok

      _ ->
        {:error, :session_owned_elsewhere}
    end
  end

  defp worker_available(execution, :worker, session_id) do
    case Enum.find(execution.leases, fn {id, lease} ->
           id != session_id and lease.role == :worker and lease.status == :active
         end) do
      nil -> :ok
      _ -> {:error, :worker_already_registered}
    end
  end

  defp worker_available(_execution, :reviewer, _session_id), do: :ok

  defp same_registration?(left, right) do
    Map.take(left, [
      :issue_id,
      :repository,
      :generation,
      :role,
      :session_id,
      :process_id,
      :branch,
      :worktree
    ]) ==
      Map.take(right, [
        :issue_id,
        :repository,
        :generation,
        :role,
        :session_id,
        :process_id,
        :branch,
        :worktree
      ])
  end

  defp put_lease(state, execution, lease) do
    execution = %{execution | leases: Map.put(execution.leases, lease.session_id, lease)}

    state
    |> put_execution(execution)
    |> put_in([:sessions, lease.session_id], lease)
  end

  defp put_execution(state, execution),
    do: put_in(state, [:executions, execution.issue_id], execution)

  defp validate_terminal(attrs, now_ms)
       when is_map(attrs) and is_integer(now_ms) and now_ms >= 0 do
    if present_string?(Map.get(attrs, :terminal_state)) and
         present_string?(Map.get(attrs, :accepted_head)) and
         optional_string?(Map.get(attrs, :merge_identity)) and valid_failure_evidence?(attrs, :terminal_state) do
      :ok
    else
      {:error, :invalid_terminal_observation}
    end
  end

  defp validate_terminal(_attrs, _now_ms), do: {:error, :invalid_terminal_observation}

  defp same_terminal?(left, right) do
    Map.take(left, [:state, :accepted_head, :merge_identity, :failure_evidence_ref]) ==
      Map.take(right, [:state, :accepted_head, :merge_identity, :failure_evidence_ref])
  end

  defp same_triage_incident?(record, execution, expected_head) do
    Map.take(record, [:type, :issue_id, :repository, :generation, :branch, :worktree, :expected_head]) ==
      %{
        type: :post_terminal_head_divergence,
        issue_id: execution.issue_id,
        repository: execution.repository,
        generation: execution.generation,
        branch: execution.branch,
        worktree: execution.worktree,
        expected_head: expected_head
      }
  end

  defp triage_id(execution), do: triage_id_for(execution.issue_id, execution.generation)

  defp triage_id_for(issue_id, generation),
    do: "#{issue_id}:#{generation}:post-terminal-head-divergence"

  defp canonical_observations(observations) do
    observations
    |> Enum.reduce_while({:ok, %{}}, &canonical_observation_step/2)
    |> case do
      {:ok, observations_by_key} ->
        {:ok, observations_by_key |> Map.values() |> Enum.sort_by(&observation_sort_key/1)}

      error ->
        error
    end
  end

  defp canonical_observation_step(observation, {:ok, seen}) do
    case validate_observation(observation) do
      :ok -> put_canonical_observation(seen, observation)
      {:error, reason} -> {:halt, {:error, reason}}
    end
  end

  defp put_canonical_observation(seen, observation) do
    key = {observation.issue_id, observation.generation, observation.session_id}

    case Map.get(seen, key) do
      nil -> {:cont, {:ok, Map.put(seen, key, observation)}}
      ^observation -> {:cont, {:ok, seen}}
      _other -> {:halt, {:error, {:contradictory_observation, observation.session_id}}}
    end
  end

  defp validate_observation(observation) when is_map(observation) do
    required = [
      :issue_id,
      :repository,
      :generation,
      :session_id,
      :process_id,
      :branch,
      :worktree,
      :last_heartbeat_at,
      :linear_state,
      :pr_state,
      :head
    ]

    if Enum.all?(required, &present_observation_field?(Map.get(observation, &1))) and
         observation.role in @roles and is_integer(observation.generation) and
         observation.generation > 0 do
      :ok
    else
      {:error, :invalid_session_observation}
    end
  end

  defp validate_observation(_observation), do: {:error, :invalid_session_observation}

  defp present_observation_field?(value) when is_integer(value), do: value >= 0
  defp present_observation_field?(value), do: present_string?(value)

  defp observation_sort_key(observation),
    do: {observation.issue_id, observation.generation, observation.session_id}

  defp reset_ownership(executions) do
    Map.new(executions, fn {issue_id, execution} ->
      ownership = if termination_unconfirmed?(execution), do: :unknown, else: :reconciled
      {issue_id, %{execution | ownership: ownership}}
    end)
  end

  defp termination_unconfirmed?(execution) do
    Map.get(execution, :termination_unconfirmed, false) == true or
      execution_requires_termination?(execution)
  end

  defp execution_requires_termination?(execution) when is_map(execution) do
    Enum.any?(Map.get(execution, :leases, %{}), fn {_session_id, lease} ->
      termination_pending?(lease)
    end)
  end

  defp execution_requires_termination?(_execution), do: false

  defp termination_pending?(lease) when is_map(lease) do
    Map.get(lease, :termination_required, false) == true and
      is_nil(Map.get(lease, :termination_confirmed_at_ms))
  end

  defp termination_pending?(_lease), do: false

  defp termination_confirmation_required(lease) do
    if Map.get(lease, :termination_required, false) do
      :ok
    else
      {:error, :termination_not_required}
    end
  end

  defp add_unconfirmed_reasons(summary, state) do
    Enum.reduce(state.executions, summary, fn {_issue_id, execution}, summary_acc ->
      if termination_unconfirmed?(execution) do
        Enum.reduce(execution.leases, summary_acc, fn {session_id, lease}, inner_summary ->
          if termination_pending?(lease),
            do: add_reason(inner_summary, :unknown, session_id),
            else: inner_summary
        end)
      else
        summary_acc
      end
    end)
  end

  defp empty_summary do
    %{unknown: [], contradictory: [], expired: [], stale: []}
  end

  defp reconcile_observation(state, summary, seen, observation, now_ms, ttl_ms) do
    case Map.get(state.executions, observation.issue_id) do
      nil ->
        {state, add_reason(summary, :unknown, observation.session_id), seen}

      execution when execution.generation != observation.generation ->
        contradictory_generation(state, summary, seen, execution, observation)

      execution ->
        reconcile_known_observation(state, summary, seen, execution, observation, now_ms, ttl_ms)
    end
  end

  defp contradictory_generation(state, summary, seen, execution, observation) do
    blocked_state = mark_ownership(state, execution.issue_id, :contradictory)
    blocked_summary = add_reason(summary, :contradictory, observation.session_id)
    {blocked_state, blocked_summary, seen}
  end

  defp reconcile_known_observation(state, summary, seen, execution, observation, now_ms, ttl_ms) do
    case reconcile_observation_precondition(execution, observation, state, summary, seen, now_ms) do
      {:blocked, next} ->
        next

      :continue ->
        lease = execution.leases[observation.session_id]
        reconcile_known_lease(state, summary, seen, execution, observation, lease, now_ms, ttl_ms)
    end
  end

  defp reconcile_observation_precondition(execution, observation, state, summary, seen, now_ms) do
    cond do
      not matching_scope?(execution, observation) ->
        {:blocked, contradictory_observation(state, summary, seen, execution, observation)}

      execution.status == :terminal and observation.linear_state != execution.terminal.state ->
        {:blocked, contradictory_observation(state, summary, seen, execution, observation)}

      observation.last_heartbeat_at > now_ms ->
        {:blocked, contradictory_observation(state, summary, seen, execution, observation)}

      is_nil(Map.get(execution.leases, observation.session_id)) ->
        {:blocked, unknown_observation(state, summary, seen, execution, observation)}

      true ->
        :continue
    end
  end

  defp contradictory_observation(state, summary, seen, execution, observation) do
    blocked_state = mark_ownership(state, execution.issue_id, :contradictory)
    blocked_summary = add_reason(summary, :contradictory, observation.session_id)
    {blocked_state, blocked_summary, mark_seen(seen, execution, observation)}
  end

  defp unknown_observation(state, summary, seen, execution, observation) do
    unknown_state = mark_ownership(state, execution.issue_id, :unknown)
    unknown_summary = add_reason(summary, :unknown, observation.session_id)
    {unknown_state, unknown_summary, seen}
  end

  defp reconcile_known_lease(state, summary, seen, execution, observation, lease, now_ms, ttl_ms) do
    cond do
      lease.status != :active or not same_registration?(lease, observation) ->
        contradictory_observation(state, summary, seen, execution, observation)

      now_ms - max(lease.last_heartbeat_at, observation.last_heartbeat_at) >= ttl_ms ->
        expire_observed_lease(state, summary, seen, execution, observation)

      now_ms - observation.last_heartbeat_at >= ttl_ms ->
        stale_observed_lease(state, summary, seen, execution, observation)

      true ->
        refreshed =
          Map.merge(
            lease,
            Map.take(observation, [:last_heartbeat_at, :linear_state, :pr_state, :head])
          )

        {put_lease(state, execution, refreshed), summary, mark_seen(seen, execution, observation)}
    end
  end

  defp expire_observed_lease(state, summary, seen, execution, observation) do
    expired_state = expire_lease(state, execution, observation.session_id)
    blocked_state = mark_termination_unconfirmed(expired_state, execution.issue_id)
    expired_summary = add_reason(summary, :expired, observation.session_id)
    {blocked_state, add_reason(expired_summary, :unknown, observation.session_id), seen}
  end

  defp stale_observed_lease(state, summary, seen, execution, observation) do
    stale_state = mark_ownership(state, execution.issue_id, :unknown)
    stale_summary = add_reason(summary, :stale, observation.session_id)
    {stale_state, stale_summary, mark_seen(seen, execution, observation)}
  end

  defp matching_scope?(execution, observation) do
    execution.repository == observation.repository and execution.branch == observation.branch and
      execution.worktree == observation.worktree
  end

  defp mark_seen(seen, execution, observation) do
    MapSet.put(seen, {execution.issue_id, execution.generation, observation.session_id})
  end

  defp expire_or_block_missing_leases(state, summary, seen, now_ms, ttl_ms) do
    Enum.reduce(state.executions, {state, summary}, fn {issue_id, execution}, accumulator ->
      Enum.reduce(execution.leases, accumulator, fn {session_id, lease}, inner ->
        reconcile_missing_lease(
          inner,
          issue_id,
          execution,
          session_id,
          lease,
          seen,
          now_ms,
          ttl_ms
        )
      end)
    end)
  end

  defp reconcile_missing_lease(
         {state, summary},
         issue_id,
         execution,
         session_id,
         lease,
         seen,
         now_ms,
         ttl_ms
       ) do
    key = {issue_id, execution.generation, session_id}

    cond do
      lease.status != :active or MapSet.member?(seen, key) ->
        {state, summary}

      now_ms - lease.last_heartbeat_at >= ttl_ms ->
        expired_state = expire_lease(state, execution, session_id)

        blocked_state =
          expired_state
          |> mark_termination_unconfirmed(issue_id)
          |> mark_ownership(issue_id, :unknown)

        blocked_summary = add_reason(summary, :unknown, session_id)
        {blocked_state, add_reason(blocked_summary, :expired, session_id)}

      true ->
        unknown_state = mark_ownership(state, issue_id, :unknown)
        {unknown_state, add_reason(summary, :unknown, session_id)}
    end
  end

  defp expire_lease(state, execution, session_id) do
    state
    |> put_in([:executions, execution.issue_id, :leases, session_id, :status], :expired)
    |> put_in([:executions, execution.issue_id, :leases, session_id, :termination_required], true)
    |> put_in([:sessions, session_id, :status], :expired)
    |> put_in([:sessions, session_id, :termination_required], true)
  end

  defp mark_termination_unconfirmed(state, issue_id),
    do: put_in(state, [:executions, issue_id, :termination_unconfirmed], true)

  defp mark_ownership(state, issue_id, :contradictory),
    do: put_in(state, [:executions, issue_id, :ownership], :contradictory)

  defp mark_ownership(state, issue_id, :unknown) do
    if get_in(state, [:executions, issue_id, :ownership]) == :contradictory do
      state
    else
      put_in(state, [:executions, issue_id, :ownership], :unknown)
    end
  end

  defp add_reason(summary, key, value) do
    update_in(summary, [key], fn values -> Enum.uniq([value | values]) end)
  end

  defp finalize_summary(summary) do
    summary = Map.new(summary, fn {key, values} -> {key, Enum.sort(values)} end)

    status =
      if summary.unknown == [] and summary.contradictory == [] and summary.stale == [],
        do: :reconciled,
        else: :blocked

    Map.put(summary, :status, status)
  end

  defp active_lease_ids(execution) do
    execution.leases
    |> Enum.filter(fn {_session_id, lease} -> lease.status == :active end)
    |> Enum.map(&elem(&1, 0))
    |> Enum.sort()
  end

  defp token(issue_id, generation), do: %{issue_id: issue_id, generation: generation}

  defp present_string?(value) when is_binary(value) do
    trimmed = String.trim(value)

    trimmed != "" and byte_size(trimmed) <= 512 and
      not String.contains?(trimmed, ["\n", "\r", <<0>>])
  end

  defp present_string?(_value), do: false

  defp optional_string?(nil), do: true
  defp optional_string?(value), do: present_string?(value)
end
