defmodule SymphonyElixir.Orchestrator do
  @moduledoc """
  Polls the configured issue tracker and dispatches repository copies to Codex-backed workers.
  """

  use GenServer
  require Logger
  import Bitwise, only: [<<<: 2]

  alias SymphonyElixir.{
    AgentRunner,
    Config,
    ExecutionFence,
    ExecutionSupervisor,
    GlobalPause,
    ResponsibilityGraph,
    ReviewHandoff,
    ReviewHandoffEvidence,
    RuntimeIdentity,
    StartupMaintenance,
    StatusDashboard,
    TerminalOutcome,
    Tracker,
    WorkPackageClaim,
    WorkPackageCleanupReceipt,
    WorkPackageRuntime,
    Workspace
  }

  alias SymphonyElixir.Codex.Progress
  alias SymphonyElixir.ExecutionFence.Persistence
  alias SymphonyElixir.ManagedCheckout.Checkpoint
  alias SymphonyElixir.ManagedResponsibility.Admission, as: ManagedAdmission
  alias SymphonyElixir.ManagedTokenBudget.Runtime, as: ManagedBudget
  alias SymphonyElixir.ManagedTokenBudget.Stop, as: ManagedBudgetStop
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: ResponsibilityPersistence
  alias SymphonyElixir.ResponsibilityGraph.ReviewCompletion
  alias SymphonyElixir.Tracker.Issue
  alias SymphonyElixir.WorkPackageClaim.{Journal, Unsubmitted}
  alias SymphonyElixir.WorkPackageClaim.Recovery, as: ClaimRecovery

  @continuation_retry_delay_ms 1_000
  @failure_retry_base_ms 10_000
  @execution_fence_lease_ttl_ms 300_000
  # Poll and reconciliation calls may spend several seconds in the Linear
  # client. Keep worker authorization bounded, but do not let the default
  # 5-second GenServer.call timeout race a healthy scheduler poll.
  @execution_authorization_timeout_ms 60_000
  # Terminal tracker states always dominate dynamic labels and local workflow
  # configuration. A stale ready label must never re-admit completed work.
  @mandatory_terminal_states ["closed", "cancelled", "canceled", "duplicate", "done"]
  # Slightly above the dashboard render interval so "checking now…" can render.
  @poll_transition_render_delay_ms 20
  # A poll may retry a bounded number of durable cleanup receipts. The
  # GenServer serializes this reconciliation with dispatch, so this remains a
  # singleflight retry path without introducing another timer or scheduler.
  @cleanup_receipt_replay_limit 8
  @empty_codex_totals %{
    input_tokens: 0,
    output_tokens: 0,
    total_tokens: 0,
    seconds_running: 0
  }

  defmodule State do
    @moduledoc """
    Runtime state for the orchestrator polling loop.
    """

    defstruct [
      :poll_interval_ms,
      :max_concurrent_agents,
      :next_poll_due_at_ms,
      :poll_check_in_progress,
      :tick_timer_ref,
      :tick_token,
      task_supervisor: SymphonyElixir.TaskSupervisor,
      running: %{},
      completed: MapSet.new(),
      claimed: MapSet.new(),
      blocked: %{},
      retry_attempts: %{},
      execution_fence: ExecutionFence.new(),
      execution_fence_path: nil,
      responsibility_graph: ResponsibilityGraph.new(),
      responsibility_graph_path: nil,
      execution_supervisor: nil,
      work_package_runtime: nil,
      review_handoff_evidence: nil,
      review_issue_fetcher: nil,
      review_handoff_cursor: 0,
      execution_termination_fun: nil,
      stall_restarts: %{},
      codex_totals: nil,
      codex_issue_totals: %{},
      managed_token_budget: nil,
      managed_token_budget_error: nil,
      codex_rate_limits: nil,
      startup_maintenance: nil,
      cleanup_receipt_quarantine: MapSet.new()
    ]
  end

  @doc false
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    case Config.settings() do
      {:ok, config} ->
        case load_execution_fence(config.execution_fence.state_path) do
          {:ok, fence_state} ->
            graph_path = Config.responsibility_graph_state_path()

            case load_responsibility_graph(graph_path) do
              {:ok, responsibility_graph} ->
                state = %State{
                  poll_interval_ms: config.polling.interval_ms,
                  max_concurrent_agents: config.agent.max_concurrent_agents,
                  next_poll_due_at_ms: nil,
                  poll_check_in_progress: false,
                  tick_timer_ref: nil,
                  tick_token: nil,
                  task_supervisor: Keyword.get(opts, :task_supervisor, SymphonyElixir.TaskSupervisor),
                  execution_fence: fence_state,
                  execution_fence_path: config.execution_fence.state_path,
                  responsibility_graph: responsibility_graph,
                  responsibility_graph_path: graph_path,
                  execution_supervisor: Keyword.get(opts, :execution_supervisor),
                  work_package_runtime: Keyword.get(opts, :work_package_runtime),
                  review_handoff_evidence: Keyword.get(opts, :review_handoff_evidence),
                  review_issue_fetcher: Keyword.get(opts, :review_issue_fetcher),
                  execution_termination_fun: Keyword.get(opts, :execution_termination_fun),
                  codex_totals: @empty_codex_totals,
                  codex_issue_totals: %{},
                  codex_rate_limits: nil
                }

                start_with_managed_budget(state, opts)

              {:error, reason} ->
                {:stop, {:responsibility_graph_unavailable, reason}}
            end

          {:error, reason} ->
            {:stop, {:execution_fence_unavailable, reason}}
        end

      {:error, reason} ->
        {:stop, reason}
    end
  end

  @impl true
  def handle_info({:tick, tick_token}, %{tick_token: tick_token} = state)
      when is_reference(tick_token) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info({:tick, _tick_token}, state), do: {:noreply, state}

  def handle_info(:tick, state) do
    state = refresh_runtime_config(state)

    state = %{
      state
      | poll_check_in_progress: true,
        next_poll_due_at_ms: nil,
        tick_timer_ref: nil,
        tick_token: nil
    }

    notify_dashboard()
    :ok = schedule_poll_cycle_start()
    {:noreply, state}
  end

  def handle_info(:run_poll_cycle, state) do
    state = refresh_runtime_config(state)
    state = reconcile_review_handoffs(state)
    state = replay_persisted_cleanup_receipts(state)
    state = maybe_dispatch(state)
    state = schedule_tick(state, state.poll_interval_ms)
    state = %{state | poll_check_in_progress: false}

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:startup_maintenance_timeout, ref}, %{startup_maintenance: %{task_ref: ref}} = state)
      when is_reference(ref) do
    maintenance = state.startup_maintenance

    if is_pid(maintenance[:task_pid]) and Process.alive?(maintenance.task_pid) do
      Task.Supervisor.terminate_child(state.task_supervisor, maintenance.task_pid)
    end

    Logger.warning("Startup terminal workspace cleanup timed out; scheduler and dashboard remain healthy")

    state =
      state
      |> Map.put(:startup_maintenance, StartupMaintenance.timeout(maintenance))
      |> schedule_tick(0)

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:startup_maintenance_timeout, _ref}, state), do: {:noreply, state}

  def handle_info({:startup_cleanup_fence_updated, fence_state}, state) do
    case ExecutionFence.validate(fence_state) do
      :ok ->
        notify_dashboard()
        {:noreply, %{state | execution_fence: fence_state}}

      {:error, reason} ->
        Logger.error("Ignoring invalid startup cleanup fence update: #{inspect(reason)}")
        {:noreply, state}
    end
  end

  def handle_info({ref, {:ok, result}}, %{startup_maintenance: %{task_ref: ref}} = state)
      when is_reference(ref) do
    Process.demonitor(ref, [:flush])

    state =
      state
      |> Map.put(:startup_maintenance, StartupMaintenance.complete(state.startup_maintenance, result))
      |> schedule_tick(0)

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, %{startup_maintenance: %{task_ref: ref}} = state)
      when is_reference(ref) do
    Logger.warning("Startup terminal workspace cleanup failed; scheduler and dashboard remain healthy")

    state =
      state
      |> Map.put(:startup_maintenance, StartupMaintenance.fail(state.startup_maintenance, reason))
      |> schedule_tick(0)

    notify_dashboard()
    {:noreply, state}
  end

  def handle_info(
        {:DOWN, ref, :process, _pid, reason},
        %{running: running} = state
      ) do
    case find_issue_id_for_ref(running, ref) do
      nil ->
        {:noreply, state}

      issue_id ->
        {running_entry, state} = pop_running_entry(state, issue_id)
        running_entry = Map.put(running_entry, :terminal_outcome, terminal_outcome_for(running_entry, reason))
        state = record_session_completion_totals(state, running_entry)
        session_id = running_entry_session_id(running_entry)
        state = finish_agent_down(state, issue_id, running_entry, session_id, reason)

        Logger.info("Agent task finished for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}")

        notify_dashboard()
        {:noreply, state}
    end
  end

  def handle_info({:worker_runtime_info, issue_id, runtime_info}, %{running: running} = state)
      when is_binary(issue_id) and is_map(runtime_info) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        if runtime_info_belongs_to_entry?(runtime_info, running_entry) do
          {state, observed_head} = observe_worker_runtime_head(state, running_entry, runtime_info)

          updated_running_entry =
            running_entry
            |> maybe_put_runtime_value(:worker_host, runtime_info[:worker_host])
            |> maybe_put_runtime_value(:workspace_path, runtime_info[:workspace_path])
            |> maybe_put_runtime_value(:accepted_head, observed_head)

          notify_dashboard()
          {:noreply, %{state | running: Map.put(state.running, issue_id, updated_running_entry)}}
        else
          Logger.warning("Ignoring stale worker runtime information for issue_id=#{issue_id}")
          {:noreply, state}
        end
    end
  end

  def handle_info(
        {:codex_worker_update, issue_id, %{event: _, timestamp: _} = update},
        %{running: running} = state
      ) do
    case Map.get(running, issue_id) do
      nil ->
        {:noreply, state}

      running_entry ->
        if runtime_info_belongs_to_entry?(update, running_entry) do
          handle_current_codex_update(state, issue_id, running_entry, update)
        else
          Logger.warning("Ignoring stale Codex worker update for issue_id=#{issue_id}")
          {:noreply, state}
        end
    end
  end

  def handle_info({:codex_worker_update, _issue_id, _update}, state), do: {:noreply, state}

  def handle_info({:retry_issue, issue_id, retry_token}, state) do
    result =
      case pop_retry_attempt_state(state, issue_id, retry_token) do
        {:ok, attempt, metadata, state} -> handle_retry_issue(state, issue_id, attempt, metadata)
        :missing -> {:noreply, state}
      end

    notify_dashboard()
    result
  end

  def handle_info({:retry_issue, _issue_id}, state), do: {:noreply, state}

  def handle_info(msg, state) do
    Logger.debug("Orchestrator ignored message: #{inspect(msg)}")
    {:noreply, state}
  end

  defp start_with_managed_budget(state, opts) do
    case ManagedBudget.load(state) do
      {:ok, state} -> {:ok, start_startup_maintenance(state, opts)}
      {:error, reason} -> {:stop, {:managed_token_budget_unavailable, reason}}
    end
  end

  defp finish_agent_down(%{managed_token_budget_error: error} = state, issue_id, entry, _session_id, _reason)
       when not is_nil(error) do
    block_issue_from_entry(state, issue_id, entry, "managed token accounting requires reconciliation")
  end

  defp finish_agent_down(state, issue_id, entry, session_id, reason) do
    state = release_execution_lease(state, entry, reason)
    state = maybe_confirm_execution_supervisor(state, entry)
    handle_agent_down(reason, state, issue_id, entry, session_id)
  end

  defp handle_agent_down(:normal, state, issue_id, running_entry, session_id) do
    if input_required_blocker?(running_entry) do
      block_input_required_agent_down(state, issue_id, running_entry, session_id, :normal)
    else
      Logger.info("Agent task completed for issue_id=#{issue_id} session_id=#{session_id}; scheduling active-state continuation check")

      state
      |> complete_issue(issue_id)
      |> schedule_issue_retry(issue_id, 1, %{
        identifier: running_entry.identifier,
        issue_url: running_entry.issue.url,
        delay_type: :continuation,
        worker_host: Map.get(running_entry, :worker_host),
        workspace_path: Map.get(running_entry, :workspace_path),
        execution_token: Map.get(running_entry, :execution_token),
        execution_session_id: Map.get(running_entry, :execution_session_id),
        accepted_head: Map.get(running_entry, :accepted_head),
        merge_identity: Map.get(running_entry, :merge_identity)
      })
    end
  end

  defp handle_agent_down(reason, state, issue_id, running_entry, session_id) do
    if input_required_blocker?(running_entry) do
      block_input_required_agent_down(state, issue_id, running_entry, session_id, reason)
    else
      retry_agent_down(state, issue_id, running_entry, session_id, reason)
    end
  end

  defp block_input_required_agent_down(state, issue_id, running_entry, session_id, reason) do
    error = blocker_error(running_entry, "agent exited: #{inspect(reason)}")

    Logger.warning("Agent task blocked for issue_id=#{issue_id} issue_identifier=#{running_entry.identifier} session_id=#{session_id}: #{error}")

    block_issue_from_entry(state, issue_id, running_entry, error)
  end

  defp retry_agent_down(state, issue_id, running_entry, session_id, reason) do
    Logger.warning("Agent task exited for issue_id=#{issue_id} session_id=#{session_id} reason=#{inspect(reason)}; scheduling retry")

    next_attempt = next_retry_attempt_from_running(running_entry)

    schedule_issue_retry(state, issue_id, next_attempt, %{
      identifier: running_entry.identifier,
      issue_url: running_entry.issue.url,
      error: "agent exited: #{inspect(reason)}",
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      execution_token: Map.get(running_entry, :execution_token),
      execution_session_id: Map.get(running_entry, :execution_session_id),
      accepted_head: Map.get(running_entry, :accepted_head),
      merge_identity: Map.get(running_entry, :merge_identity)
    })
  end

  defp maybe_dispatch(%State{} = state) do
    state =
      state
      |> reconcile_running_issues()
      |> reconcile_blocked_issues()
      |> hold_review_handoff_claims()

    if GlobalPause.paused?() do
      Logger.debug("Global mutable admission is paused; skipping new worker dispatch")
      state
    else
      with :ok <- Config.validate!(),
           {:ok, issues} <- Tracker.fetch_issues_by_states(Config.settings!().tracker.active_states) do
        choose_issues(refresh_pending_claim_issues(state, issues), state)
      else
        {:error, :missing_linear_api_token} ->
          Logger.error("Tracker API token missing in WORKFLOW.md")
          state

        {:error, :missing_linear_project_slug} ->
          Logger.error("Tracker project scope missing in WORKFLOW.md")
          state

        {:error, :missing_tracker_kind} ->
          Logger.error("Tracker kind missing in WORKFLOW.md")

          state

        {:error, {:unsupported_tracker_kind, kind}} ->
          Logger.error("Unsupported tracker kind in WORKFLOW.md: #{inspect(kind)}")

          state

        {:error, {:invalid_workflow_config, message}} ->
          Logger.error("Invalid WORKFLOW.md config: #{message}")
          state

        {:error, {:missing_workflow_file, path, reason}} ->
          Logger.error("Missing WORKFLOW.md at #{path}: #{inspect(reason)}")
          state

        {:error, :workflow_front_matter_not_a_map} ->
          Logger.error("Failed to parse WORKFLOW.md: workflow front matter must decode to a map")
          state

        {:error, {:workflow_parse_error, reason}} ->
          Logger.error("Failed to parse WORKFLOW.md: #{inspect(reason)}")
          state

        {:error, reason} ->
          Logger.error("Failed to fetch from issue tracker: #{inspect(reason)}")
          state
      end
    end
  end

  defp reconcile_review_handoffs(%State{work_package_runtime: nil} = state), do: state

  defp reconcile_review_handoffs(state) do
    pending = ReviewHandoff.pending_executions(state.execution_fence, Map.keys(state.running))
    offset = if pending == [], do: 0, else: rem(state.review_handoff_cursor, length(pending))
    batch = Enum.drop(pending, offset) ++ Enum.take(pending, offset)
    ids = batch |> Enum.take(@cleanup_receipt_replay_limit) |> Enum.map(&elem(&1, 0))
    state = %{state | review_handoff_cursor: offset + length(ids)}

    case ids do
      [] ->
        state

      _ ->
        case fetch_review_issues(state, ids) do
          {:ok, issues} ->
            reconcile_review_handoff_issues(state, issues)

          {:error, reason} ->
            Logger.warning("Review handoff tracker reconciliation unavailable: #{inspect(reason)}")
            state
        end
    end
  end

  defp hold_review_handoff_claims(%State{work_package_runtime: nil} = state), do: state

  defp hold_review_handoff_claims(state) do
    ids = ReviewHandoff.pending_executions(state.execution_fence, Map.keys(state.running)) |> Enum.map(&elem(&1, 0))
    %{state | claimed: MapSet.union(state.claimed, MapSet.new(ids))}
  end

  @doc false
  @spec reconcile_review_handoff_issues_for_test(map(), [Issue.t()]) :: map()
  def reconcile_review_handoff_issues_for_test(state, issues) do
    fetcher = state.review_issue_fetcher || fn _ids -> {:ok, issues} end
    result = reconcile_review_handoff_issues(%{state | review_issue_fetcher: fetcher}, issues)
    %{result | review_issue_fetcher: state.review_issue_fetcher}
  end

  defp fetch_review_issues(state, ids) do
    fetcher = state.review_issue_fetcher || (&Tracker.fetch_issues_by_ids/1)
    fetcher.(ids)
  end

  defp reconcile_review_handoff_issues(state, issues) do
    pending = Map.new(ReviewHandoff.pending_executions(state.execution_fence, Map.keys(state.running)))

    Enum.reduce(issues, state, fn issue, current ->
      case Map.get(pending, issue.id) do
        nil -> current
        execution -> reconcile_review_handoff(current, execution, issue)
      end
    end)
  end

  defp reconcile_review_handoff(state, execution, issue) do
    with {:ok, entry} <- ReviewHandoff.entry(execution, issue),
         {:ok, entry} <- bind_review_handoff_responsibility(state, execution, entry) do
      state = maybe_confirm_execution_supervisor(state, entry)

      if terminal_issue_state?(issue.state, terminal_state_set()) and
           get_in(state.execution_fence, [:executions, issue.id, :ownership]) == :reconciled do
        finish_review_handoff(state, execution, entry)
      else
        state
      end
    else
      {:error, reason} ->
        Logger.warning("Review handoff remains held for #{issue_context(issue)}: #{inspect(reason)}")
        state
    end
  end

  defp bind_review_handoff_responsibility(state, execution, entry) do
    runtime = state.work_package_runtime
    profile = runtime.managed_project_profile_id
    key = Journal.reservation_key(execution.issue_id, profile, execution.repository, execution.generation)
    delegations = state.responsibility_graph.delegations
    expected = review_reservation_identity(runtime, execution, entry)

    with {:ok, journal} <- Journal.load(runtime.journal_path),
         reservation when is_map(reservation) <- Map.get(journal.reservations, key),
         true <- Map.take(reservation, Map.keys(expected)) == expected,
         delegation when is_map(delegation) <- Map.get(delegations, reservation.responsible_delegation_id),
         true <-
           delegation.role == :responsible and delegation.scope.repository == execution.repository and
             delegation.scope.issue_id in [execution.issue_id, entry.issue.identifier] do
      {:ok, entry |> Map.put(:responsibility_delegation_id, delegation.id) |> Map.put(:review_reservation, reservation)}
    else
      _ -> {:error, :review_responsibility_identity_unavailable}
    end
  end

  defp review_reservation_identity(runtime, execution, entry) do
    %{
      issue_id: execution.issue_id,
      repository_ref: execution.repository,
      managed_project_profile_id: runtime.managed_project_profile_id,
      runner_id: runtime.runner_id,
      generation: execution.generation,
      session_id: entry.execution_session_id,
      process_id: entry.process_id,
      execution_fence_token: "#{execution.issue_id}:#{execution.generation}",
      runtime_lease_id: entry.execution_session_id
    }
  end

  defp finish_review_handoff(state, execution, entry) do
    observe = state.review_handoff_evidence || (&ReviewHandoffEvidence.observe/1)

    with {:ok, %{accepted_head: head, merge_identity: merge}} <- observe.(execution),
         :ok <- recheck_review_issue(state, entry.issue) do
      evidence = %{
        accepted_head: head,
        merge_identity: merge,
        terminal_outcome: TerminalOutcome.for_tracker_state(entry.issue.state),
        review_merge_verified: true
      }

      entry = Map.merge(entry, evidence)
      state = maybe_fence_terminal_execution(state, entry, true)
      cleanup_review_handoff(state, entry)
    else
      {:error, reason} ->
        Logger.warning("Review handoff awaits accepted merge for #{issue_context(entry.issue)}: #{inspect(reason)}")
        state

      _ ->
        Logger.warning("Review handoff received malformed merge evidence for #{issue_context(entry.issue)}")
        state
    end
  end

  defp recheck_review_issue(state, issue) do
    with {:ok, [%Issue{} = current]} <- fetch_review_issues(state, [issue.id]),
         true <- current.id == issue.id and current.state == issue.state and current.updated_at == issue.updated_at do
      :ok
    else
      _ -> {:error, :review_native_state_changed}
    end
  end

  defp cleanup_review_handoff(state, entry) do
    status = get_in(state.responsibility_graph, [:delegations, entry.responsibility_delegation_id, :status])
    ready = ExecutionFence.validate_cleanup(state.execution_fence, entry.execution_token, entry.accepted_head)

    if status == :completed and ready == :ok do
      state = maybe_confirm_execution_supervisor(state, entry)
      state = cleanup_fenced_workspace_or_legacy(state, entry.issue, entry)
      release_cleaned_review_claim(state, entry.issue.id)
    else
      state
    end
  end

  defp release_cleaned_review_claim(state, issue_id) do
    if get_in(state.execution_fence, [:executions, issue_id, :cleanup]) == :cleaned do
      release_issue_claim(state, issue_id)
    else
      state
    end
  end

  defp reconcile_running_issues(%State{} = state) do
    state = reconcile_stalled_running_issues(state)
    running_ids = Map.keys(state.running)

    if running_ids == [] do
      state
    else
      case Tracker.fetch_issues_by_ids(running_ids) do
        {:ok, issues} ->
          issues
          |> reconcile_running_issue_states(
            state,
            active_state_set(),
            terminal_state_set()
          )
          |> reconcile_missing_running_issue_ids(running_ids, issues)

        {:error, reason} ->
          Logger.debug("Failed to refresh running issue states: #{inspect(reason)}; keeping active workers")

          state
      end
    end
  end

  defp reconcile_blocked_issues(%State{} = state) do
    blocked_ids = Map.keys(state.blocked)

    if blocked_ids == [] do
      state
    else
      case Tracker.fetch_issues_by_ids(blocked_ids) do
        {:ok, issues} ->
          issues
          |> reconcile_blocked_issue_states(
            state,
            active_state_set(),
            terminal_state_set()
          )
          |> reconcile_missing_blocked_issue_ids(blocked_ids, issues)

        {:error, reason} ->
          Logger.debug("Failed to refresh blocked issue states: #{inspect(reason)}; keeping blocked issues")

          state
      end
    end
  end

  @doc false
  @spec reconcile_issue_states_for_test([Issue.t()], term()) :: term()
  def reconcile_issue_states_for_test(issues, %State{} = state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  def reconcile_issue_states_for_test(issues, state) when is_list(issues) do
    reconcile_running_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec reconcile_blocked_issue_states_for_test([Issue.t()], term()) :: term()
  def reconcile_blocked_issue_states_for_test(issues, %State{} = state) when is_list(issues) do
    reconcile_blocked_issue_states(issues, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec handle_retry_issue_lookup_for_test(Issue.t(), term(), String.t(), non_neg_integer(), map()) ::
          term()
  def handle_retry_issue_lookup_for_test(%Issue{} = issue, %State{} = state, issue_id, attempt, metadata)
      when is_binary(issue_id) and is_integer(attempt) and attempt >= 0 and is_map(metadata) do
    {:noreply, updated_state} = handle_retry_issue_lookup(issue, state, issue_id, attempt, metadata)
    updated_state
  end

  @doc false
  @spec should_dispatch_issue_for_test(Issue.t(), term()) :: boolean()
  def should_dispatch_issue_for_test(%Issue{} = issue, %State{} = state) do
    should_dispatch_issue?(issue, state, active_state_set(), terminal_state_set())
  end

  @doc false
  @spec admit_execution_for_test(term(), Issue.t(), String.t() | nil) ::
          {:ok, term(), map(), String.t(), String.t() | nil, map()} | {:error, term()}
  def admit_execution_for_test(%State{} = state, %Issue{} = issue, worker_host) do
    admit_execution(state, issue, worker_host)
  end

  @doc false
  @spec handle_claim_failure_for_test(term(), Issue.t(), term(), map()) :: term()
  def handle_claim_failure_for_test(%State{} = state, %Issue{} = issue, reason, entry) do
    handle_claim_failure(state, issue, reason, entry)
  end

  @doc false
  @spec revalidate_issue_for_dispatch_for_test(Issue.t(), ([String.t()] -> term())) ::
          {:ok, Issue.t()} | {:skip, Issue.t() | :missing} | {:error, term()}
  def revalidate_issue_for_dispatch_for_test(%Issue{} = issue, issue_fetcher)
      when is_function(issue_fetcher, 1) do
    revalidate_issue_for_dispatch(issue, issue_fetcher, terminal_state_set())
  end

  @doc false
  @spec sort_issues_for_dispatch_for_test([Issue.t()]) :: [Issue.t()]
  def sort_issues_for_dispatch_for_test(issues) when is_list(issues) do
    sort_issues_for_dispatch(issues)
  end

  @doc false
  @spec select_worker_host_for_test(term(), String.t() | nil) :: String.t() | nil | :no_worker_capacity
  def select_worker_host_for_test(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host)
  end

  defp reconcile_running_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_running_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_running_issue_states(
      rest,
      reconcile_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        state |> refresh_running_issue_state(issue) |> terminate_running_issue(issue.id, true)

      !issue_routable?(issue) ->
        Logger.info("Issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; stopping active agent")

        state |> refresh_running_issue_state(issue) |> terminate_running_issue(issue.id, false)

      active_issue_state?(issue.state, active_states) ->
        refresh_running_issue_state(state, issue)

      true ->
        Logger.info("Issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; stopping active agent")

        state |> refresh_running_issue_state(issue) |> terminate_running_issue(issue.id, false)
    end
  end

  defp reconcile_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp reconcile_blocked_issue_states([], state, _active_states, _terminal_states), do: state

  defp reconcile_blocked_issue_states([issue | rest], state, active_states, terminal_states) do
    reconcile_blocked_issue_states(
      rest,
      reconcile_blocked_issue_state(issue, state, active_states, terminal_states),
      active_states,
      terminal_states
    )
  end

  defp reconcile_blocked_issue_state(%Issue{} = issue, state, active_states, terminal_states) do
    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Blocked issue moved to terminal state: #{issue_context(issue)} state=#{issue.state}; releasing block")
        blocked_entry = Map.get(state.blocked, issue.id, %{})
        state = maybe_fence_terminal_execution(state, Map.put(blocked_entry, :issue, issue), true)
        state = cleanup_fenced_workspace_or_legacy(state, issue, blocked_entry)
        release_terminal_block_after_cleanup(state, issue, blocked_entry)

      !issue_routable?(issue) ->
        Logger.info("Blocked issue no longer routed to this worker: #{issue_context(issue)} assignee=#{inspect(issue.assignee_id)}; releasing block")
        release_issue_claim(state, issue.id)

      active_issue_state?(issue.state, active_states) ->
        refresh_blocked_issue_state(state, issue)

      true ->
        Logger.info("Blocked issue moved to non-active state: #{issue_context(issue)} state=#{issue.state}; retaining cleanup authority")

        refresh_blocked_issue_state(state, issue)
    end
  end

  defp reconcile_blocked_issue_state(_issue, state, _active_states, _terminal_states), do: state

  defp reconcile_missing_running_issue_ids(%State{} = state, requested_issue_ids, issues)
       when is_list(requested_issue_ids) and is_list(issues) do
    visible_issue_ids =
      issues
      |> Enum.flat_map(fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _ -> []
      end)
      |> MapSet.new()

    Enum.reduce(requested_issue_ids, state, fn issue_id, state_acc ->
      if MapSet.member?(visible_issue_ids, issue_id) do
        state_acc
      else
        log_missing_running_issue(state_acc, issue_id)
        terminate_running_issue(state_acc, issue_id, false)
      end
    end)
  end

  defp reconcile_missing_running_issue_ids(state, _requested_issue_ids, _issues), do: state

  defp reconcile_missing_blocked_issue_ids(%State{} = state, requested_issue_ids, issues)
       when is_list(requested_issue_ids) and is_list(issues) do
    visible_issue_ids =
      issues
      |> Enum.flat_map(fn
        %Issue{id: issue_id} when is_binary(issue_id) -> [issue_id]
        _ -> []
      end)
      |> MapSet.new()

    Enum.reduce(requested_issue_ids, state, fn issue_id, state_acc ->
      if MapSet.member?(visible_issue_ids, issue_id) do
        state_acc
      else
        Logger.info("Blocked issue no longer visible during state refresh: issue_id=#{issue_id}; releasing block")
        release_issue_claim(state_acc, issue_id)
      end
    end)
  end

  defp reconcile_missing_blocked_issue_ids(state, _requested_issue_ids, _issues), do: state

  defp log_missing_running_issue(%State{} = state, issue_id) when is_binary(issue_id) do
    case Map.get(state.running, issue_id) do
      %{identifier: identifier} ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id} issue_identifier=#{identifier}; stopping active agent")

      _ ->
        Logger.info("Issue no longer visible during running-state refresh: issue_id=#{issue_id}; stopping active agent")
    end
  end

  defp log_missing_running_issue(_state, _issue_id), do: :ok

  defp refresh_running_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.running, issue.id) do
      %{issue: _} = running_entry ->
        %{state | running: Map.put(state.running, issue.id, %{running_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp refresh_blocked_issue_state(%State{} = state, %Issue{} = issue) do
    case Map.get(state.blocked, issue.id) do
      %{issue: _} = blocked_entry ->
        %{state | blocked: Map.put(state.blocked, issue.id, %{blocked_entry | issue: issue})}

      _ ->
        state
    end
  end

  defp release_terminal_block_after_cleanup(%State{work_package_runtime: nil} = state, issue, _entry),
    do: release_issue_claim(state, issue.id)

  defp release_terminal_block_after_cleanup(%State{} = state, issue, %{execution_token: token}) do
    execution = get_in(state.execution_fence, [:executions, token.issue_id])

    receipts_accepted? =
      is_map(execution) and execution.generation == token.generation and execution.cleanup == :cleaned and
        not cleanup_receipt_pending?(state.work_package_runtime, token, "repository_cleanup_verified") and
        Enum.all?(execution.leases, fn {_session_id, lease} ->
          is_integer(Map.get(lease, :termination_confirmed_at_ms))
        end) and
        not cleanup_receipt_pending?(state.work_package_runtime, token, "termination_confirmed")

    if receipts_accepted?,
      do: release_issue_claim(state, issue.id),
      else: refresh_blocked_issue_state(state, issue)
  end

  defp release_terminal_block_after_cleanup(%State{} = state, issue, _entry),
    do: release_issue_claim(state, issue.id)

  defp terminate_running_issue(%State{} = state, issue_id, cleanup_workspace) do
    case Map.get(state.running, issue_id) do
      nil ->
        release_issue_claim(state, issue_id)

      %{pid: pid, ref: ref, identifier: identifier} = running_entry ->
        running_entry = Map.put(running_entry, :terminal_outcome, terminal_outcome_for(running_entry, :orchestrator_stop))
        state = record_session_completion_totals(state, running_entry)
        # Persist the lease release before stopping. A crash after process exit
        # must leave the exact generation discoverable by terminal reconciliation.
        state = release_execution_lease(state, running_entry, :orchestrator_stop)
        state = maybe_fence_stopped_execution(state, running_entry, cleanup_workspace)

        stop_running_task(pid, ref, state.task_supervisor)
        {state, running_entry} = drain_stopped_managed_usage(state, issue_id, running_entry)

        finish_stopped_issue(state, issue_id, running_entry, identifier, cleanup_workspace)

      _ ->
        release_issue_claim(state, issue_id)
    end
  end

  defp finish_stopped_issue(%{managed_token_budget_error: error} = state, issue_id, entry, _identifier, _cleanup)
       when not is_nil(error) do
    block_issue_from_entry(state, issue_id, entry, "managed token accounting requires reconciliation")
  end

  defp finish_stopped_issue(state, issue_id, running_entry, identifier, cleanup_workspace) do
    state = release_execution_lease(state, running_entry, :orchestrator_stop)
    state = maybe_confirm_execution_supervisor(state, running_entry)

    state =
      if cleanup_workspace and is_nil(state.work_package_runtime) do
        cleanup_fenced_workspace_or_legacy(state, Map.get(running_entry, :issue, identifier), running_entry)
      else
        state
      end

    %{
      state
      | running: Map.delete(state.running, issue_id),
        claimed: MapSet.delete(state.claimed, issue_id),
        blocked: Map.delete(state.blocked, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id)
    }
  end

  defp maybe_fence_stopped_execution(%State{work_package_runtime: nil} = state, entry, cleanup),
    do: maybe_fence_terminal_execution(state, entry, cleanup)

  # Managed completion needs a fresh accepted merge, not the worker's initial
  # checkout head. The persisted generation is reconciled on the ordinary poll.
  defp maybe_fence_stopped_execution(state, _entry, _cleanup), do: state

  defp reconcile_stalled_running_issues(%State{} = state) do
    timeout_ms = Config.settings!().codex.stall_timeout_ms
    max_no_progress_tokens = Config.settings!().codex.max_no_progress_tokens
    max_total_tokens = Config.settings!().codex.max_total_tokens
    unmanaged? = is_nil(state.work_package_runtime)

    cond do
      unmanaged? and timeout_ms <= 0 and max_no_progress_tokens <= 0 and max_total_tokens <= 0 ->
        state

      map_size(state.running) == 0 ->
        state

      true ->
        now = DateTime.utc_now()
        now_ms = monotonic_now_ms()

        Enum.reduce(state.running, state, fn {issue_id, running_entry}, state_acc ->
          maybe_restart_stalled_issue(state_acc, issue_id, running_entry, now, now_ms, timeout_ms)
        end)
    end
  end

  defp maybe_restart_stalled_issue(state, issue_id, running_entry, now, now_ms, timeout_ms) do
    if Map.has_key?(state.blocked, issue_id) do
      state
    else
      restart_stalled_issue(state, issue_id, running_entry, now, now_ms, timeout_ms)
    end
  end

  defp restart_stalled_issue(state, issue_id, running_entry, now, now_ms, timeout_ms) do
    case ManagedBudget.effective_limit(state, issue_id) do
      {:ok, limit} ->
        restart_stalled_issue(state, issue_id, running_entry, now, now_ms, timeout_ms, limit)

      {:error, reason} ->
        stop_for_invalid_token_budget(state, issue_id, running_entry, reason)
    end
  end

  defp restart_stalled_issue(state, issue_id, running_entry, now, now_ms, timeout_ms, max_total_tokens) do
    elapsed_ms = stall_elapsed_ms(running_entry, now_ms)
    max_no_progress_tokens = Config.settings!().codex.max_no_progress_tokens
    no_progress_tokens = no_progress_token_count(running_entry)
    no_durable_progress_tokens = no_durable_progress_token_count(running_entry)
    issue_total_tokens = issue_token_total(state, issue_id)
    time_stall? = stalled_for_time?(timeout_ms, elapsed_ms)
    total_token_budget_exhausted? = token_budget_exhausted?(max_total_tokens, issue_total_tokens)
    command_token_stall? = no_progress_token_stall?(max_no_progress_tokens, no_progress_tokens)
    durable_token_stall? = no_progress_token_stall?(max_no_progress_tokens, no_durable_progress_tokens)

    token_stall? = command_token_stall? or durable_token_stall?

    if time_stall? or token_stall? or total_token_budget_exhausted? do
      restart_count = Map.get(state.stall_restarts, issue_id, 0) + 1

      diagnostic =
        stall_diagnostic(
          running_entry,
          now,
          elapsed_ms,
          timeout_ms,
          restart_count,
          token_stall?,
          durable_token_stall?,
          no_progress_tokens,
          no_durable_progress_tokens,
          max_no_progress_tokens
        )

      context = %{
        identifier: Map.get(running_entry, :identifier, issue_id),
        session_id: running_entry_session_id(running_entry),
        elapsed_ms: elapsed_ms,
        restart_count: restart_count,
        diagnostic: diagnostic,
        total_token_budget_exhausted?: total_token_budget_exhausted?,
        issue_total_tokens: issue_total_tokens,
        max_total_tokens: max_total_tokens,
        token_stall?: token_stall?,
        no_progress_tokens: no_progress_tokens,
        no_durable_progress_tokens: no_durable_progress_tokens,
        max_no_progress_tokens: max_no_progress_tokens
      }

      handle_stalled_issue(state, issue_id, running_entry, context)
    else
      state
    end
  end

  defp stalled_for_time?(timeout_ms, elapsed_ms),
    do: timeout_ms > 0 and is_integer(elapsed_ms) and elapsed_ms > timeout_ms

  defp token_budget_exhausted?(max_total_tokens, issue_total_tokens),
    do: is_integer(max_total_tokens) and max_total_tokens > 0 and issue_total_tokens >= max_total_tokens

  defp no_progress_token_stall?(max_no_progress_tokens, observed_tokens),
    do: max_no_progress_tokens > 0 and observed_tokens >= max_no_progress_tokens

  defp handle_stalled_issue(state, issue_id, running_entry, context) do
    cond do
      input_required_blocker?(running_entry) ->
        error = blocker_error(running_entry, "stalled for #{context.elapsed_ms}ms after Codex requested operator input")

        Logger.warning(
          "Issue blocked: issue_id=#{issue_id} issue_identifier=#{context.identifier} " <>
            "session_id=#{context.session_id} elapsed_ms=#{context.elapsed_ms}; #{error}"
        )

        state
        |> record_session_completion_totals(running_entry)
        |> stop_and_block_issue(issue_id, running_entry, error)

      context.total_token_budget_exhausted? ->
        error = total_token_budget_error(context.issue_total_tokens, context.max_total_tokens)

        state
        |> record_session_completion_totals(running_entry)
        |> stop_and_block_issue(
          issue_id,
          Map.put(
            running_entry,
            :stall_diagnostic,
            total_token_budget_diagnostic(running_entry, context.issue_total_tokens, context.max_total_tokens)
          ),
          error
        )

      context.restart_count > Config.settings!().codex.max_stall_retries ->
        error = "codex stalled #{context.restart_count} consecutive times; automatic recovery exhausted"

        Logger.warning(
          "Issue stopped after repeated stalls: issue_id=#{issue_id} " <>
            "issue_identifier=#{context.identifier} session_id=#{context.session_id} " <>
            "restart_count=#{context.restart_count} elapsed_ms=#{context.elapsed_ms}"
        )

        state
        |> record_session_completion_totals(running_entry)
        |> put_stall_restart_count(issue_id, context.restart_count)
        |> stop_and_block_issue(issue_id, Map.put(running_entry, :stall_diagnostic, context.diagnostic), error)

      true ->
        Logger.warning(
          "Issue stalled: issue_id=#{issue_id} issue_identifier=#{context.identifier} " <>
            "session_id=#{context.session_id} elapsed_ms=#{context.elapsed_ms}; restarting with backoff"
        )

        next_attempt = next_retry_attempt_from_running(running_entry)

        state
        |> put_stall_restart_count(issue_id, context.restart_count)
        |> terminate_running_issue(issue_id, false)
        |> put_issue_token_total(issue_id, context.issue_total_tokens)
        |> reserve_issue_claim_for_retry(issue_id)
        |> schedule_issue_retry(issue_id, next_attempt, %{
          identifier: context.identifier,
          issue_url: running_entry.issue.url,
          error:
            stall_retry_error(
              context.elapsed_ms,
              context.token_stall?,
              context.no_progress_tokens,
              context.no_durable_progress_tokens,
              context.max_no_progress_tokens
            ),
          stall_diagnostic: context.diagnostic
        })
    end
  end

  defp stall_elapsed_ms(running_entry, now_ms) when is_integer(now_ms) do
    case last_activity_monotonic_ms(running_entry) do
      timestamp_ms when is_integer(timestamp_ms) -> max(0, now_ms - timestamp_ms)
      _ -> nil
    end
  end

  defp last_activity_monotonic_ms(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :codex_last_activity_monotonic_ms) ||
      Map.get(running_entry, :started_monotonic_ms)
  end

  defp last_activity_monotonic_ms(_running_entry), do: nil

  defp stall_retry_error(
         elapsed_ms,
         true,
         no_progress_tokens,
         no_durable_progress_tokens,
         threshold
       ) do
    "token progress guard reached #{threshold} tokens " <>
      "(meaningful=#{no_progress_tokens}, durable=#{no_durable_progress_tokens}, " <>
      "qualifying_activity_silence_ms=#{inspect(elapsed_ms)})"
  end

  defp stall_retry_error(elapsed_ms, false, _no_progress, _no_durable_progress, _threshold) do
    "stalled for #{elapsed_ms}ms without qualifying codex activity"
  end

  defp no_progress_token_count(running_entry) do
    max(
      0,
      Map.get(running_entry, :codex_total_tokens, 0) -
        Map.get(running_entry, :codex_progress_token_baseline, 0)
    )
  end

  defp no_durable_progress_token_count(running_entry) do
    max(
      0,
      Map.get(running_entry, :codex_total_tokens, 0) -
        Map.get(running_entry, :codex_durable_progress_token_baseline, 0)
    )
  end

  defp stall_diagnostic(
         running_entry,
         now,
         elapsed_ms,
         timeout_ms,
         restart_count,
         token_stall?,
         durable_token_stall?,
         no_progress_tokens,
         no_durable_progress_tokens,
         max_no_progress_tokens
       ) do
    %{
      reason:
        if(token_stall?,
          do: "codex_token_growth_without_meaningful_progress",
          else: "codex_no_activity"
        ),
      observed_at: now,
      elapsed_ms: elapsed_ms,
      threshold_ms: timeout_ms,
      restart_count: restart_count,
      no_progress_tokens: no_progress_tokens,
      no_durable_progress_tokens: no_durable_progress_tokens,
      no_progress_token_threshold: max_no_progress_tokens,
      durable_progress_token_threshold: max_no_progress_tokens,
      durable_token_stall: durable_token_stall?,
      last_progress_at: Map.get(running_entry, :codex_last_progress_timestamp),
      last_progress_method: Map.get(running_entry, :codex_last_progress_method),
      last_durable_progress_at: Map.get(running_entry, :codex_last_durable_progress_timestamp),
      last_durable_progress_method: Map.get(running_entry, :codex_last_durable_progress_method),
      session_id: running_entry_session_id(running_entry),
      turn_count: Map.get(running_entry, :turn_count, 0),
      last_event: Map.get(running_entry, :last_codex_event),
      last_event_at: Map.get(running_entry, :last_codex_timestamp),
      last_qualifying_activity_class: Map.get(running_entry, :codex_last_activity_method) || "worker_started",
      worker_process_alive: worker_process_alive?(Map.get(running_entry, :pid)),
      codex_app_server_pid: Map.get(running_entry, :codex_app_server_pid),
      token_totals: %{
        input_tokens: Map.get(running_entry, :codex_input_tokens, 0),
        output_tokens: Map.get(running_entry, :codex_output_tokens, 0),
        total_tokens: Map.get(running_entry, :codex_total_tokens, 0)
      }
    }
  end

  defp worker_process_alive?(pid) when is_pid(pid), do: Process.alive?(pid)
  defp worker_process_alive?(_pid), do: false

  defp put_stall_restart_count(%State{} = state, issue_id, count) do
    %{state | stall_restarts: Map.put(state.stall_restarts, issue_id, count)}
  end

  defp reserve_issue_claim_for_retry(%State{} = state, issue_id) do
    %{state | claimed: MapSet.put(state.claimed, issue_id)}
  end

  defp input_required_blocker?(running_entry) when is_map(running_entry) do
    Map.get(running_entry, :last_codex_event) in [:turn_input_required, :approval_required] or
      not is_nil(input_required_completion_outcome(Map.get(running_entry, :completion))) or
      codex_message_method(Map.get(running_entry, :last_codex_message)) ==
        "mcpServer/elicitation/request"
  end

  defp input_required_blocker?(_running_entry), do: false

  defp input_required_completion_outcome(completion) when is_map(completion) do
    outcome = Map.get(completion, :outcome) || Map.get(completion, "outcome")
    normalize_input_required_outcome(outcome)
  end

  defp input_required_completion_outcome(_completion), do: nil

  defp normalize_input_required_outcome(outcome)
       when outcome in [:input_required, :needs_input, :approval_required],
       do: outcome

  defp normalize_input_required_outcome(outcome) when is_binary(outcome) do
    case outcome do
      "input_required" -> :input_required
      "needs_input" -> :needs_input
      "approval_required" -> :approval_required
      _ -> nil
    end
  end

  defp normalize_input_required_outcome(_outcome), do: nil

  defp blocker_error(running_entry, fallback) when is_map(running_entry) do
    codex_event_blocker_error(Map.get(running_entry, :last_codex_event)) ||
      completion_blocker_error(Map.get(running_entry, :completion)) ||
      codex_message_blocker_error(Map.get(running_entry, :last_codex_message)) ||
      fallback
  end

  defp blocker_error(_running_entry, fallback), do: fallback

  defp codex_event_blocker_error(:turn_input_required), do: "codex turn requires operator input"
  defp codex_event_blocker_error(:approval_required), do: "codex turn requires approval"
  defp codex_event_blocker_error(_event), do: nil

  defp completion_blocker_error(completion) do
    case input_required_completion_outcome(completion) do
      outcome when outcome in [:input_required, :needs_input] -> "codex turn requires operator input"
      :approval_required -> "codex turn requires approval"
      nil -> nil
    end
  end

  defp codex_message_blocker_error(message) do
    if codex_message_method(message) == "mcpServer/elicitation/request" do
      "codex MCP elicitation requires operator input"
    end
  end

  defp codex_message_method(%{message: %{"method" => method}}) when is_binary(method), do: method
  defp codex_message_method(%{message: %{method: method}}) when is_binary(method), do: method
  defp codex_message_method(%{"method" => method}) when is_binary(method), do: method
  defp codex_message_method(%{method: method}) when is_binary(method), do: method
  defp codex_message_method(_message), do: nil

  defp terminate_task(pid, task_supervisor) when is_pid(pid) do
    case Task.Supervisor.terminate_child(task_supervisor, pid) do
      :ok ->
        :ok

      {:error, :not_found} ->
        Process.exit(pid, :shutdown)
    end
  end

  defp terminate_task(_pid, _task_supervisor), do: :ok

  defp stop_running_task(pid, ref, task_supervisor) do
    if is_pid(pid) do
      stop_ref = Process.monitor(pid)
      terminate_task(pid, task_supervisor)

      receive do
        {:DOWN, ^stop_ref, :process, ^pid, _reason} -> :ok
      after
        5_000 -> Process.demonitor(stop_ref, [:flush])
      end
    end

    if is_reference(ref) do
      Process.demonitor(ref, [:flush])
    end

    :ok
  end

  defp stop_and_block_issue(%State{} = state, issue_id, running_entry, error) do
    stop_running_task(
      Map.get(running_entry, :pid),
      Map.get(running_entry, :ref),
      state.task_supervisor
    )

    {state, running_entry} = drain_stopped_managed_usage(state, issue_id, running_entry)
    block_issue_from_entry(state, issue_id, running_entry, error)
  end

  defp drain_stopped_managed_usage(state, issue_id, entry) do
    ManagedBudgetStop.drain(state, issue_id, entry, fn current, current_entry, update ->
      {next_entry, delta} = integrate_codex_update(current_entry, update)
      current = current |> apply_codex_token_delta(delta) |> ManagedBudget.observe(issue_id, next_entry, update)
      {%{current | running: Map.put(current.running, issue_id, next_entry)}, next_entry}
    end)
  end

  defp block_issue_from_entry(%State{} = state, issue_id, running_entry, error) do
    blocked_entry = %{
      issue_id: issue_id,
      identifier: Map.get(running_entry, :identifier, issue_id),
      issue: Map.get(running_entry, :issue),
      worker_host: Map.get(running_entry, :worker_host),
      workspace_path: Map.get(running_entry, :workspace_path),
      session_id: running_entry_session_id(running_entry),
      error: error,
      blocked_at: DateTime.utc_now(),
      last_codex_message: Map.get(running_entry, :last_codex_message),
      last_codex_event: Map.get(running_entry, :last_codex_event),
      last_codex_timestamp: Map.get(running_entry, :last_codex_timestamp),
      execution_token: Map.get(running_entry, :execution_token),
      execution_session_id: Map.get(running_entry, :execution_session_id),
      accepted_head: Map.get(running_entry, :accepted_head),
      merge_identity: Map.get(running_entry, :merge_identity),
      stall_diagnostic: Map.get(running_entry, :stall_diagnostic)
    }

    %{
      state
      | running: Map.delete(state.running, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id),
        claimed: MapSet.put(state.claimed, issue_id),
        blocked: Map.put(state.blocked, issue_id, blocked_entry)
    }
  end

  defp choose_issues(issues, state) do
    active_states = active_state_set()
    terminal_states = terminal_state_set()

    issues
    |> sort_issues_for_dispatch()
    |> Enum.reduce(state, fn issue, state_acc ->
      if should_dispatch_issue?(issue, state_acc, active_states, terminal_states) do
        dispatch_issue(state_acc, issue)
      else
        state_acc
      end
    end)
  end

  defp sort_issues_for_dispatch(issues) when is_list(issues) do
    Enum.sort_by(issues, fn
      %Issue{} = issue ->
        {priority_rank(issue.priority), issue_created_at_sort_key(issue), issue.identifier || issue.id || ""}

      _ ->
        {priority_rank(nil), issue_created_at_sort_key(nil), ""}
    end)
  end

  defp priority_rank(priority) when is_integer(priority) and priority in 1..4, do: priority
  defp priority_rank(_priority), do: 5

  defp issue_created_at_sort_key(%Issue{created_at: %DateTime{} = created_at}) do
    DateTime.to_unix(created_at, :microsecond)
  end

  defp issue_created_at_sort_key(%Issue{}), do: 9_223_372_036_854_775_807
  defp issue_created_at_sort_key(_issue), do: 9_223_372_036_854_775_807

  defp should_dispatch_issue?(
         %Issue{} = issue,
         %State{running: running, claimed: claimed, blocked: blocked} = state,
         active_states,
         terminal_states
       ) do
    candidate_issue?(issue, active_states, terminal_states) and
      ManagedBudget.admission(state, issue.id) == :ok and
      !MapSet.member?(claimed, issue.id) and
      !Map.has_key?(running, issue.id) and
      !Map.has_key?(blocked, issue.id) and
      (available_slots(state) > 0 or retained_claim_slot?(state, issue.id)) and
      state_slots_available?(issue, running) and
      worker_slots_available?(state)
  end

  defp should_dispatch_issue?(_issue, _state, _active_states, _terminal_states), do: false

  defp state_slots_available?(%Issue{state: issue_state}, running) when is_map(running) do
    limit = Config.max_concurrent_agents_for_state(issue_state)
    used = running_issue_count_for_state(running, issue_state)
    limit > used
  end

  defp state_slots_available?(_issue, _running), do: false

  defp running_issue_count_for_state(running, issue_state) when is_map(running) do
    normalized_state = normalize_issue_state(issue_state)

    Enum.count(running, fn
      {_id, %{issue: %Issue{state: state_name}}} ->
        normalize_issue_state(state_name) == normalized_state

      _ ->
        false
    end)
  end

  defp candidate_issue?(
         %Issue{
           id: id,
           identifier: identifier,
           title: title,
           state: state_name
         } = issue,
         active_states,
         terminal_states
       )
       when is_binary(id) and is_binary(identifier) and is_binary(title) and is_binary(state_name) do
    Enum.all?([id, identifier, title, state_name], &present_string?/1) and
      issue_routable?(issue) and
      active_issue_state?(state_name, active_states) and
      !terminal_issue_state?(state_name, terminal_states)
  end

  defp candidate_issue?(_issue, _active_states, _terminal_states), do: false

  defp issue_routable?(%Issue{} = issue) do
    Issue.routable?(issue, Config.settings!().tracker.required_labels)
  end

  defp terminal_issue_state?(state_name, terminal_states) when is_binary(state_name) do
    MapSet.member?(terminal_states, normalize_issue_state(state_name))
  end

  defp terminal_issue_state?(_state_name, _terminal_states), do: false

  defp present_string?(value) when is_binary(value), do: String.trim(value) != ""
  defp present_string?(_value), do: false

  defp active_issue_state?(state_name, active_states) when is_binary(state_name) do
    MapSet.member?(active_states, normalize_issue_state(state_name))
  end

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    String.downcase(String.trim(state_name))
  end

  defp terminal_state_set do
    (@mandatory_terminal_states ++ Config.settings!().tracker.terminal_states)
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp active_state_set do
    Config.settings!().tracker.active_states
    |> Enum.map(&normalize_issue_state/1)
    |> Enum.filter(&(&1 != ""))
    |> MapSet.new()
  end

  defp dispatch_issue(%State{} = state, issue, attempt \\ nil, preferred_worker_host \\ nil) do
    case refresh_issue_for_dispatch(issue) do
      {:ok, %Issue{} = refreshed_issue} ->
        do_dispatch_issue(state, refreshed_issue, attempt, preferred_worker_host)

      {:skip, _reason} ->
        state

      {:error, _reason} ->
        state
    end
  end

  defp refresh_issue_for_dispatch(issue) do
    case revalidate_issue_for_dispatch(issue, &Tracker.fetch_issues_by_ids/1, terminal_state_set()) do
      {:ok, %Issue{} = refreshed_issue} ->
        {:ok, refreshed_issue}

      {:skip, :missing} ->
        Logger.info("Skipping dispatch; issue no longer active or visible: #{issue_context(issue)}")
        {:skip, :missing}

      {:skip, %Issue{} = refreshed_issue} ->
        Logger.info("Skipping stale dispatch after issue refresh: #{issue_context(refreshed_issue)} state=#{inspect(refreshed_issue.state)} blocked_by=#{length(refreshed_issue.blocked_by)}")

        {:skip, refreshed_issue}

      {:error, reason} ->
        Logger.warning("Skipping dispatch; issue refresh failed for #{issue_context(issue)}: #{inspect(reason)}")
        {:error, reason}
    end
  end

  defp do_dispatch_issue(%State{} = state, issue, attempt, preferred_worker_host) do
    if GlobalPause.paused?() do
      Logger.debug("Global mutable admission paused before worker selection for #{issue_context(issue)}")
      state
    else
      recipient = self()

      case select_worker_host(state, preferred_worker_host) do
        :no_worker_capacity ->
          Logger.debug("No SSH worker slots available for #{issue_context(issue)} preferred_worker_host=#{inspect(preferred_worker_host)}")
          state

        worker_host ->
          spawn_issue_on_worker_host(state, issue, attempt, recipient, worker_host)
      end
    end
  end

  defp spawn_issue_on_worker_host(%State{} = state, issue, attempt, recipient, worker_host) do
    if GlobalPause.paused?() do
      Logger.debug("Global mutable admission paused before execution-fence admission for #{issue_context(issue)}")
      state
    else
      case admit_execution(state, issue, worker_host, attempt) do
        {:ok, state, token, session_id, responsibility_delegation_id, runtime_lease} ->
          case claim_work_package(state, issue, token, worker_host) do
            {:ok, state} ->
              spawn_fenced_issue(
                state,
                issue,
                attempt,
                recipient,
                worker_host,
                token,
                session_id,
                responsibility_delegation_id,
                runtime_lease
              )

            {:error, reason} ->
              Logger.warning("Skipping mutable dispatch without a durable work-package claim for #{issue_context(issue)}: #{inspect(reason)}")

              handle_claim_failure(state, issue, reason, %{
                execution_token: token,
                execution_session_id: session_id,
                responsibility_delegation_id: responsibility_delegation_id,
                responsibility_runtime_lease: runtime_lease
              })
          end

        {:error, reason} ->
          handle_claim_admission_failure(state, issue, reason)
      end
    end
  end

  defp spawn_fenced_issue(
         %State{} = state,
         issue,
         attempt,
         recipient,
         worker_host,
         token,
         session_id,
         responsibility_delegation_id,
         runtime_lease
       ) do
    supervisor_identity = execution_supervisor_identity(state, issue, token, session_id, worker_host)
    runtime = if is_map(state.work_package_runtime), do: state.work_package_runtime, else: %{}

    if GlobalPause.paused?() do
      Logger.debug("Global mutable admission paused immediately before worker spawn for #{issue_context(issue)}")

      if is_map(state.work_package_runtime) do
        state
      else
        release_execution_lease(
          state,
          %{
            execution_token: token,
            execution_session_id: session_id,
            responsibility_delegation_id: responsibility_delegation_id,
            responsibility_runtime_lease: runtime_lease
          },
          :global_pause
        )
      end
    else
      case start_claimed_worker(state, issue, fn ->
             AgentRunner.run(issue, recipient,
               attempt: attempt,
               managed_model_route: is_map(get_in(state.work_package_runtime || %{}, [:managed_delegations])),
               managed_model_runtime: managed_worker_model_runtime(state.work_package_runtime),
               worker_host: worker_host,
               execution_token: token,
               execution_session_id: session_id,
               execution_checkout: managed_execution_checkout(state, token, session_id),
               execution_checkout_checkpoint: fn checkpoint ->
                 GenServer.call(
                   recipient,
                   {:execution_checkout_progress, issue.id, checkpoint},
                   @execution_authorization_timeout_ms
                 )
               end,
               execution_supervisor: supervisor_identity,
               secret_environment_names: Map.get(runtime, :secret_environment_names, []),
               execution_supervisor_recorder: fn identity ->
                 GenServer.call(
                   recipient,
                   {:execution_fence_supervisor, token, session_id, identity},
                   @execution_authorization_timeout_ms
                 )
               end,
               execution_fence_guard: fn ->
                 GenServer.call(
                   recipient,
                   {:execution_authorize, token, responsibility_delegation_id, :state_mutation},
                   @execution_authorization_timeout_ms
                 )
               end
             )
           end) do
        {:ok, pid} ->
          ref = Process.monitor(pid)

          Logger.info("Dispatching issue to agent: #{issue_context(issue)} pid=#{inspect(pid)} attempt=#{inspect(attempt)} worker_host=#{worker_host || "local"}")

          running =
            Map.put(state.running, issue.id, %{
              pid: pid,
              ref: ref,
              identifier: issue.identifier,
              issue: issue,
              worker_host: worker_host,
              workspace_path: nil,
              session_id: nil,
              execution_token: token,
              execution_session_id: session_id,
              responsibility_delegation_id: responsibility_delegation_id,
              responsibility_runtime_lease: runtime_lease,
              last_codex_message: nil,
              last_codex_timestamp: nil,
              last_codex_event: nil,
              codex_app_server_pid: nil,
              codex_input_tokens: 0,
              codex_output_tokens: 0,
              codex_total_tokens: 0,
              codex_last_reported_input_tokens: 0,
              codex_last_reported_output_tokens: 0,
              codex_last_reported_total_tokens: 0,
              codex_progress_token_baseline: 0,
              codex_durable_progress_token_baseline: 0,
              codex_last_progress_timestamp: nil,
              codex_last_progress_method: nil,
              codex_last_activity_monotonic_ms: nil,
              codex_last_activity_method: nil,
              turn_count: 0,
              retry_attempt: normalize_retry_attempt(attempt),
              started_at: DateTime.utc_now(),
              started_monotonic_ms: monotonic_now_ms()
            })

          %{
            state
            | running: running,
              claimed: MapSet.put(state.claimed, issue.id),
              retry_attempts: Map.delete(state.retry_attempts, issue.id)
          }

        {:error, reason} ->
          Logger.error("Unable to spawn agent for #{issue_context(issue)}: #{inspect(reason)}")

          handle_claim_spawn_failure(state, issue, attempt, reason, %{
            worker_host: worker_host,
            execution_token: token,
            execution_session_id: session_id,
            responsibility_delegation_id: responsibility_delegation_id,
            responsibility_runtime_lease: runtime_lease
          })
      end
    end
  end

  defp managed_worker_model_runtime(%{managed_delegations: %{repository_ref: repository_ref}} = runtime) do
    %{
      journal_path: Map.get(runtime, :journal_path),
      managed_project_profile_id: Map.get(runtime, :managed_project_profile_id),
      repository_ref: repository_ref
    }
  end

  defp managed_worker_model_runtime(_runtime), do: nil

  defp execution_supervisor_identity(%State{execution_supervisor: :systemd_user}, issue, token, session_id, nil) do
    ExecutionSupervisor.identity(issue.id, token.generation, session_id, session_id, execution_fence_now_ms())
  end

  defp execution_supervisor_identity(_state, _issue, _token, _session_id, _worker_host), do: nil

  defp claim_work_package(%State{work_package_runtime: nil, execution_supervisor: nil} = state, _issue, _token, _worker_host),
    do: {:ok, state}

  defp claim_work_package(%State{work_package_runtime: nil}, _issue, _token, _worker_host),
    do: {:error, :work_package_runtime_required}

  defp claim_work_package(%State{work_package_runtime: runtime, execution_supervisor: :systemd_user} = state, issue, _token, nil)
       when is_map(runtime) do
    input = claim_input(state, issue)

    claim_opts =
      []
      |> maybe_claim_option(runtime, :request_fun)
      |> maybe_claim_option(runtime, :now_fun)

    with :ok <- ExecutionSupervisor.available?(),
         {:ok, _claim} <- WorkPackageClaim.claim(input, claim_opts) do
      {:ok, state}
    else
      {:error, _reason} = error -> error
    end
  end

  defp claim_work_package(%State{work_package_runtime: _runtime}, _issue, _token, _worker_host),
    do: {:error, :execution_supervisor_required_for_claim}

  defp maybe_claim_option(options, runtime, key) do
    case Map.get(runtime, key) do
      value when is_function(value) -> Keyword.put(options, key, value)
      _ -> options
    end
  end

  defp admit_execution(state, issue, worker_host, attempt \\ nil)

  defp admit_execution(%State{} = state, %Issue{id: issue_id} = issue, worker_host, attempt)
       when is_binary(issue_id) do
    with :ok <- ManagedBudget.admission(state, issue_id) do
      recover_or_admit_execution(state, issue, worker_host, attempt)
    end
  end

  defp admit_execution(_state, _issue, _worker_host, _attempt), do: {:error, :invalid_issue}

  defp recover_or_admit_execution(state, issue, worker_host, attempt) do
    case prepare_claim_recovery(state, issue, attempt) do
      {:new, fence, graph} ->
        with {:ok, state} <- persist_responsibility_graph(state, graph),
             {:ok, state} <- persist_execution_fence(state, fence) do
          admit_new_execution(state, issue, worker_host, attempt)
        end

      {:new, graph} ->
        admit_new_execution(%{state | responsibility_graph: graph}, issue, worker_host, attempt)

      :new ->
        admit_new_execution(state, issue, worker_host, attempt)

      {:ok, fence, graph, recovered} ->
        with :ok <- ManagedBudget.generation(state, recovered.token),
             {:ok, state} <- persist_responsibility_graph(state, graph),
             {:ok, state} <- persist_execution_fence(state, fence) do
          {:ok, state, recovered.token, recovered.session_id, recovered.delegation_id, recovered.runtime_lease}
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp admit_new_execution(%State{} = state, %Issue{id: issue_id} = issue, worker_host, attempt) do
    now_ms = execution_fence_now_ms()
    attrs = execution_attributes(issue, worker_host)
    session_id_for_generation = fn generation -> execution_session_id(issue_id, generation) end

    manifest = get_in(state.work_package_runtime || %{}, [:managed_delegations])

    with {:ok, graph} <-
           ManagedAdmission.prepare(
             state.responsibility_graph,
             state.execution_fence,
             manifest,
             issue,
             attempt,
             now_ms,
             state.work_package_runtime
           ),
         state = %{state | responsibility_graph: graph},
         {:ok, fence_state, token} <- ExecutionFence.admit(state.execution_fence, attrs, now_ms),
         :ok <- ManagedBudget.generation(state, token),
         session_id = session_id_for_generation.(token.generation),
         runtime_lease = execution_runtime_lease(issue_id, token, session_id),
         {:ok, fence_state, _result} <-
           ExecutionFence.register(
             fence_state,
             token,
             :worker,
             execution_session_attributes(attrs, session_id),
             now_ms
           ),
         {:ok, next_state, responsibility_delegation_id} <-
           bind_execution_responsibility(state, issue, runtime_lease, now_ms),
         {:ok, next_state} <- persist_execution_fence(next_state, fence_state) do
      {:ok, next_state, token, session_id, responsibility_delegation_id, runtime_lease}
    end
  end

  defp prepare_claim_recovery(%State{work_package_runtime: nil}, _issue, _attempt), do: :new

  defp prepare_claim_recovery(state, issue, attempt) do
    ClaimRecovery.prepare(
      state.work_package_runtime,
      state.execution_fence,
      state.responsibility_graph,
      issue,
      attempt,
      execution_fence_now_ms()
    )
  end

  defp claim_input(state, issue) do
    Map.merge(state.work_package_runtime, %{
      issue_id: issue.id,
      issue_identifier: issue.identifier,
      repository_ref: get_in(state.execution_fence, [:executions, issue.id, :repository]),
      fence_state: state.execution_fence,
      responsibility_graph: state.responsibility_graph
    })
  end

  defp start_claimed_worker(%State{work_package_runtime: nil} = state, _issue, worker),
    do: Task.Supervisor.start_child(state.task_supervisor, worker)

  defp start_claimed_worker(state, issue, worker) do
    with :ok <- WorkPackageClaim.begin_spawn(claim_input(state, issue)) do
      if GlobalPause.paused?(),
        do: {:error, :global_pause},
        else: Task.Supervisor.start_child(state.task_supervisor, worker)
    end
  end

  defp handle_claim_failure(state, _issue, {:claim_indeterminate, _reason}, _entry), do: state

  defp handle_claim_failure(state, issue, reason, entry) do
    if retain_claim_error?(state, issue, reason),
      do: block_claim_recovery(state, issue, reason),
      else: release_execution_lease(state, entry, :claim_not_submitted)
  end

  defp handle_claim_admission_failure(state, _issue, :claim_recovery_backoff), do: state

  defp handle_claim_admission_failure(state, issue, reason) do
    Logger.warning("Skipping fenced dispatch for #{issue_context(issue)}: #{inspect(reason)}")
    if is_map(state.work_package_runtime) and Map.has_key?(state.execution_fence.executions, issue.id), do: block_claim_recovery(state, issue, reason), else: state
  end

  defp handle_claim_spawn_failure(%State{work_package_runtime: nil} = state, issue, attempt, reason, entry) do
    state = release_execution_lease(state, entry, :spawn_failed)
    next_attempt = if is_integer(attempt), do: attempt + 1, else: nil

    schedule_issue_retry(
      state,
      issue.id,
      next_attempt,
      Map.merge(entry, %{
        identifier: issue.identifier,
        issue_url: issue.url,
        error: "failed to spawn agent: #{inspect(reason)}"
      })
    )
  end

  defp handle_claim_spawn_failure(state, issue, _attempt, reason, _entry),
    do: block_claim_recovery(state, issue, reason)

  defp retain_claim_error?(_state, _issue, {:claim_indeterminate, _reason}), do: true
  defp retain_claim_error?(%State{work_package_runtime: nil}, _issue, _reason), do: false

  defp retain_claim_error?(state, issue, :reservation_not_ready),
    do: Unsubmitted.claim_may_exist?(state.work_package_runtime, state.execution_fence, issue.id)

  defp retain_claim_error?(_state, _issue, _reason), do: true

  defp retained_claim_slot?(%State{work_package_runtime: nil}, _issue_id), do: false
  defp retained_claim_slot?(state, issue_id), do: ClaimRecovery.held?(state.execution_fence, issue_id)

  defp block_claim_recovery(state, issue, reason) do
    block_issue_from_entry(state, issue.id, %{issue: issue, identifier: issue.identifier}, "Claim recovery requires reconciliation: #{inspect(reason)}")
  end

  defp refresh_pending_claim_issues(%State{work_package_runtime: nil}, issues), do: issues

  defp refresh_pending_claim_issues(state, issues) do
    visible = MapSet.new(issues, & &1.id)

    missing =
      for {id, _execution} <- state.execution_fence.executions,
          retained_claim_slot?(state, id) and not MapSet.member?(visible, id) and
            not Map.has_key?(state.running, id),
          do: id

    case missing do
      [] ->
        issues

      ids ->
        case Tracker.fetch_issues_by_ids(ids) do
          {:ok, refreshed} -> issues ++ refreshed
          {:error, _reason} -> issues
        end
    end
  end

  @doc false
  @spec repository_identity() :: String.t()
  def repository_identity do
    case {System.get_env("SYMPHONY_POOL_KEY"), System.get_env("SYMPHONY_REPOSITORY_REF")} do
      {nil, nil} ->
        "openai/symphony"

      {pool_key, repository_ref}
      when is_binary(pool_key) and pool_key != "" and is_binary(repository_ref) and
             repository_ref != "" ->
        if Regex.match?(~r/\A[a-zA-Z0-9_.-]+\/[a-zA-Z0-9_.-]+\z/, repository_ref) do
          repository_ref
        else
          raise ArgumentError, "invalid Symphony pool repository reference"
        end

      _ ->
        raise ArgumentError, "Symphony pool repository identity requires both pool and repository"
    end
  end

  defp managed_execution_checkout(%State{work_package_runtime: nil}, _token, _session_id), do: nil

  defp managed_execution_checkout(state, token, session_id) do
    state.execution_fence.executions[token.issue_id]
    |> Map.take([:issue_id, :generation, :repository, :branch, :worktree])
    |> Map.put(:session_id, session_id)
  end

  defp execution_attributes(%Issue{id: issue_id, identifier: identifier, branch_name: branch_name}, worker_host) do
    workspace_root =
      case worker_host do
        nil -> Config.local_workspace_root()
        _ -> Config.settings!().workspace.root
      end

    workspace = Path.join(workspace_root, Workspace.workspace_key(identifier || issue_id))

    %{
      issue_id: issue_id,
      repository: repository_identity(),
      worker_host: worker_host,
      branch: branch_name || "codex/#{Workspace.workspace_key(identifier || issue_id)}",
      worktree: workspace
    }
  end

  defp execution_session_attributes(attrs, session_id) do
    Map.merge(attrs, %{
      session_id: session_id,
      process_id: session_id,
      role: :worker,
      linear_state: "admitted",
      pr_state: "unopened",
      head: "unobserved",
      last_heartbeat_at: 0
    })
  end

  defp execution_session_id(issue_id, generation), do: "worker:#{issue_id}:#{generation}"

  defp execution_runtime_lease(issue_id, %{generation: generation}, session_id) do
    %{
      issue_id: issue_id,
      repository: repository_identity(),
      generation: generation,
      session_id: session_id,
      process_id: session_id
    }
  end

  defp bind_execution_responsibility(%State{} = state, %Issue{} = issue, runtime_lease, now_ms) do
    if ResponsibilityGraph.enforced?(state.responsibility_graph) do
      with {:ok, delegation} <-
             ResponsibilityGraph.admission_delegation(
               state.responsibility_graph,
               issue.id,
               issue.identifier,
               runtime_lease.repository
             ),
           {:ok, graph_state} <-
             ResponsibilityGraph.bind_runtime_lease(
               state.responsibility_graph,
               delegation.id,
               runtime_lease,
               now_ms
             ),
           {:ok, next_state} <- persist_responsibility_graph(state, graph_state) do
        {:ok, next_state, delegation.id}
      end
    else
      {:ok, state, nil}
    end
  end

  defp release_execution_lease(%State{} = state, running_entry, reason) do
    with %{execution_token: token, execution_session_id: session_id} <- running_entry,
         {:ok, fence_state, _result} <-
           ExecutionFence.release(state.execution_fence, token, session_id, normalize_release_reason(reason)) do
      case persist_execution_fence(state, fence_state) do
        {:ok, next_state} ->
          release_responsibility_lease(next_state, running_entry)

        {:error, persist_reason} ->
          Logger.error("Execution-fence lease release was not persisted: #{inspect(persist_reason)}")
          state
      end
    else
      _ ->
        state
    end
  end

  defp maybe_confirm_execution_supervisor(%State{} = state, running_entry) do
    token = Map.get(running_entry, :execution_token)
    session_id = Map.get(running_entry, :execution_session_id)

    case supervisor_identity_for(state.execution_fence, token, session_id) do
      identity when is_map(identity) ->
        case terminate_execution(state, identity) do
          {:ok, evidence} ->
            now_ms = execution_fence_now_ms()

            case ExecutionFence.confirm_termination(state.execution_fence, token, session_id, evidence, now_ms) do
              {:ok, fence_state, _result} ->
                case persist_execution_fence(state, fence_state) do
                  {:ok, next_state} ->
                    submit_termination_cleanup_receipt(next_state, running_entry, evidence)

                  {:error, reason} ->
                    Logger.error("Execution supervisor proof could not be persisted: #{inspect(reason)}")
                    state
                end

              {:error, reason} ->
                Logger.warning("Execution supervisor proof rejected: #{inspect(reason)}")
                state
            end

          {:error, reason} ->
            Logger.warning("Execution supervisor termination could not be proven: #{inspect(reason)}")
            state
        end

      _ ->
        state
    end
  end

  defp terminate_execution(state, identity) do
    terminate = state.execution_termination_fun || (&ExecutionSupervisor.terminate/1)
    terminate.(identity)
  end

  defp supervisor_identity_for(fence_state, %{issue_id: issue_id, generation: generation}, session_id)
       when is_binary(issue_id) and is_integer(generation) and is_binary(session_id) do
    case get_in(fence_state, [:executions, issue_id]) do
      %{generation: ^generation, leases: leases} ->
        case Map.get(leases, session_id) do
          %{generation: ^generation, session_id: ^session_id, supervisor_identity: identity} -> identity
          _ -> nil
        end

      _ ->
        nil
    end
  end

  defp supervisor_identity_for(_fence_state, _token, _session_id), do: nil

  defp submit_termination_cleanup_receipt(%State{work_package_runtime: nil} = state, _entry, _evidence),
    do: state

  defp submit_termination_cleanup_receipt(%State{work_package_runtime: runtime} = state, entry, evidence)
       when is_map(runtime) and is_map(evidence) do
    issue_id = Map.get(entry.execution_token, :issue_id) || entry.issue.id
    execution = get_in(state.execution_fence, [:executions, issue_id])
    accepted_head = get_in(execution, [:terminal, :accepted_head])

    if is_map(execution) and is_binary(accepted_head) and accepted_head != "unobserved" do
      input =
        runtime
        |> Map.merge(%{
          issue_id: issue_id,
          repository_ref: execution.repository,
          fence_state: state.execution_fence
        })

      attrs = %{terminal_outcome: cleanup_terminal_outcome(state, entry.execution_token), accepted_head: accepted_head}
      opts = [] |> maybe_claim_option(runtime, :request_fun) |> maybe_claim_option(runtime, :now_fun)

      case WorkPackageCleanupReceipt.termination_confirmed(input, attrs, opts) do
        {:ok, _result} ->
          state

        {:error, reason} ->
          Logger.warning("Trusted termination receipt was not accepted for #{issue_context(entry.issue)}: #{inspect(reason)}")
          state
      end
    else
      Logger.warning("Trusted termination receipt was not sent without an exact accepted head for #{issue_context(entry.issue)}")
      state
    end
  end

  defp submit_termination_cleanup_receipt(state, _entry, _evidence), do: state

  defp release_responsibility_lease(
         %State{} = state,
         %{responsibility_delegation_id: delegation_id, responsibility_runtime_lease: runtime_lease}
       )
       when is_binary(delegation_id) and is_map(runtime_lease) do
    case ResponsibilityGraph.release_runtime_lease(
           state.responsibility_graph,
           delegation_id,
           runtime_lease,
           execution_fence_now_ms()
         ) do
      {:ok, graph_state, _result} ->
        case persist_responsibility_graph(state, graph_state) do
          {:ok, next_state} ->
            next_state

          {:error, reason} ->
            Logger.error("Responsibility runtime-lease release was not persisted: #{inspect(reason)}")
            state
        end

      {:error, reason} ->
        Logger.error("Responsibility runtime-lease release was rejected: #{inspect(reason)}")
        state
    end
  end

  defp release_responsibility_lease(state, _running_entry), do: state

  defp normalize_release_reason(reason) when reason in [:spawn_failed, :global_pause, :claim_not_submitted], do: reason
  defp normalize_release_reason(_reason), do: :orchestrator_stop

  defp terminal_outcome_for(entry, reason) do
    tracker_state = Map.get(Map.get(entry, :issue) || %{}, :state)

    terminal? =
      case tracker_state do
        state when is_binary(state) ->
          try do
            terminal_issue_state?(state, terminal_state_set())
          rescue
            _ -> MapSet.member?(MapSet.new(@mandatory_terminal_states), normalize_issue_state(state))
          end

        _ ->
          false
      end

    cond do
      terminal? and reason == :normal -> TerminalOutcome.for_tracker_state(tracker_state)
      reason in [:orchestrator_stop, :global_pause] -> :blocked
      true -> :failed
    end
  end

  defp cleanup_terminal_outcome(state, token) do
    case get_in(state.execution_fence, [:executions, token.issue_id]) do
      %{cleanup_receipt: %{terminal_outcome: outcome}} when outcome in [:completed, :failed, :blocked] ->
        outcome

      %{terminal: %{state: terminal_state}} when is_binary(terminal_state) ->
        try do
          if terminal_issue_state?(terminal_state, terminal_state_set()),
            do: TerminalOutcome.for_tracker_state(terminal_state),
            else: :blocked
        rescue
          _ -> :blocked
        end

      _ ->
        :blocked
    end
  rescue
    _ -> :blocked
  end

  defp maybe_fence_terminal_execution(state, _running_entry, false), do: state

  defp maybe_fence_terminal_execution(state, entry, true) do
    if is_nil(state.work_package_runtime) or Map.get(entry, :review_merge_verified, false),
      do: fence_terminal_execution(state, entry),
      else: state
  end

  defp fence_terminal_execution(state, %{execution_token: token, issue: %Issue{state: issue_state} = issue} = entry)
       when is_binary(issue_state) do
    terminal_head = Map.get(entry, :accepted_head, "unobserved")
    terminal_attrs = %{terminal_state: issue_state, accepted_head: terminal_head, merge_identity: Map.get(entry, :merge_identity)}

    case ExecutionFence.fence(state.execution_fence, token, terminal_attrs, execution_fence_now_ms()) do
      {:ok, fence_state, _result} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} ->
            complete_responsibility_execution(next_state, entry, terminal_attrs)

          {:error, persist_reason} ->
            Logger.error("Terminal execution fence was not persisted: #{inspect(persist_reason)}")
            state
        end

      {:error, reason} ->
        Logger.warning("Terminal execution fence rejected for #{issue_context(issue)}: #{inspect(reason)}")
        state
    end
  end

  defp fence_terminal_execution(state, _entry), do: state

  defp complete_responsibility_execution(
         %State{} = state,
         %{responsibility_delegation_id: delegation_id} = entry,
         terminal_attrs
       )
       when is_binary(delegation_id) and is_map(terminal_attrs) do
    result =
      if Map.get(entry, :review_merge_verified, false) do
        ReviewCompletion.complete(
          state.responsibility_graph,
          state.execution_fence,
          entry,
          terminal_attrs,
          execution_fence_now_ms()
        )
      else
        now_ms = execution_fence_now_ms()
        ResponsibilityGraph.complete(state.responsibility_graph, delegation_id, terminal_attrs, now_ms)
      end

    case result do
      {:ok, graph_state, _impact} ->
        case persist_responsibility_graph(state, graph_state) do
          {:ok, next_state} ->
            next_state

          {:error, reason} ->
            Logger.error("Terminal responsibility completion was not persisted: #{inspect(reason)}")
            state
        end

      {:error, reason} ->
        Logger.warning("Terminal responsibility completion rejected: #{inspect(reason)}")
        state
    end
  end

  defp complete_responsibility_execution(state, _entry, _terminal_attrs), do: state

  defp cleanup_fenced_workspace_or_legacy(state, issue_or_identifier, entry) do
    if is_nil(state.work_package_runtime) or Map.get(entry, :review_merge_verified, false),
      do: cleanup_verified_workspace(state, issue_or_identifier, entry),
      else: state
  end

  defp cleanup_verified_workspace(%State{} = state, issue_or_identifier, %{execution_token: token} = entry) do
    case Map.get(entry, :accepted_head) do
      head when is_binary(head) and head != "" and head != "unobserved" ->
        case execution_workspace_path(state.execution_fence, token, entry) do
          {:ok, workspace_path} ->
            case cleanup_receipt_status(state.execution_fence, token, head) do
              :removal_started ->
                case Workspace.path_exists?(workspace_path, Map.get(entry, :worker_host)) do
                  {:ok, false} ->
                    persist_fenced_cleanup(state, token, head, execution_fence_now_ms(), Map.get(entry, :terminal_outcome))

                  {:ok, true} ->
                    verify_and_cleanup_fenced_workspace(
                      state,
                      issue_or_identifier,
                      Map.put(entry, :workspace_path, workspace_path),
                      token,
                      head
                    )

                  {:error, reason} ->
                    Logger.warning("Preserving fenced workspace because cleanup replay could not verify path absence: #{inspect(reason)}")
                    state
                end

              _ ->
                verify_and_cleanup_fenced_workspace(
                  state,
                  issue_or_identifier,
                  Map.put(entry, :workspace_path, workspace_path),
                  token,
                  head
                )
            end

          {:error, reason} ->
            Logger.warning("Preserving fenced workspace because its path could not be resolved: #{inspect(reason)}")
            state
        end

      _ ->
        Logger.warning("Preserving fenced workspace until an exact terminal head is observed")
        state
    end
  end

  defp cleanup_verified_workspace(state, issue_or_identifier, entry) do
    cleanup_issue_workspace(issue_or_identifier, entry)
    state
  end

  defp verify_and_cleanup_fenced_workspace(state, issue_or_identifier, entry, token, head) do
    workspace_path = Map.get(entry, :workspace_path)

    case Workspace.current_head(workspace_path, Map.get(entry, :worker_host)) do
      {:ok, ^head} ->
        cleanup_fenced_execution(state, issue_or_identifier, entry, token, head)

      {:ok, observed_head} ->
        record_head_divergence(state, token, head, observed_head)

      {:error, reason} ->
        Logger.warning("Preserving fenced workspace because exact head could not be observed: #{inspect(reason)}")
        state
    end
  end

  defp cleanup_receipt_status(fence_state, %{issue_id: issue_id, generation: generation}, expected_head) do
    case Map.get(fence_state.executions, issue_id) do
      %{generation: ^generation, cleanup_receipt: %{phase: phase, expected_head: ^expected_head}} -> phase
      _ -> nil
    end
  end

  defp cleanup_receipt_status(_fence_state, _token, _expected_head), do: nil

  defp cleanup_fenced_execution(state, issue_or_identifier, entry, token, head) do
    now_ms = execution_fence_now_ms()

    case ExecutionFence.prepare_cleanup(
           state.execution_fence,
           token,
           head,
           now_ms,
           Map.get(entry, :terminal_outcome, :blocked)
         ) do
      {:ok, prepared_fence, _result} ->
        case persist_execution_fence(state, prepared_fence) do
          {:ok, prepared_state} ->
            case prepare_cleanup_evidence(prepared_state, entry, token, head, now_ms) do
              {:ok, evidence_state} ->
                case cleanup_issue_workspace(issue_or_identifier, entry) do
                  :ok ->
                    finalize_fenced_workspace_cleanup(evidence_state, entry, token, head, now_ms)

                  {:ok, _removed} ->
                    finalize_fenced_workspace_cleanup(evidence_state, entry, token, head, now_ms)

                  {:error, reason, _path} ->
                    Logger.warning("Preserving fenced workspace after cleanup failure: #{inspect(reason)}")
                    evidence_state

                  {:error, reason} ->
                    Logger.warning("Preserving fenced workspace after cleanup failure: #{inspect(reason)}")
                    evidence_state
                end

              {:error, reason} ->
                Logger.warning("Preserving fenced workspace without independent archive evidence: #{inspect(reason)}")
                prepared_state
            end

          {:error, reason} ->
            Logger.error("Execution-fence cleanup intent was not persisted: #{inspect(reason)}")
            state
        end

      {:error, reason} ->
        Logger.warning("Preserving fenced workspace after cleanup rejection: #{inspect(reason)}")
        state
    end
  end

  defp prepare_cleanup_evidence(%State{work_package_runtime: nil} = state, _entry, _token, _head, _now_ms),
    do: {:ok, state}

  defp prepare_cleanup_evidence(
         %State{work_package_runtime: runtime} = state,
         entry,
         token,
         head,
         now_ms
       )
       when is_map(runtime) do
    case Map.get(runtime, :cleanup_prepare_fun) do
      preparer when is_function(preparer, 4) ->
        case preparer.(state, token, head, entry) do
          {:ok, evidence_ref} when is_binary(evidence_ref) and evidence_ref != "" ->
            case ExecutionFence.record_cleanup_evidence(
                   state.execution_fence,
                   token,
                   head,
                   evidence_ref,
                   now_ms
                 ) do
              {:ok, fence_state} -> persist_execution_fence(state, fence_state)
              {:error, reason} -> {:error, reason}
            end

          {:error, _reason} = error ->
            error

          _ ->
            {:error, :invalid_cleanup_evidence}
        end

      _ ->
        {:error, :cleanup_preservation_verifier_missing}
    end
  rescue
    error -> {:error, {:cleanup_preservation_verifier_failed, error}}
  end

  defp prepare_cleanup_evidence(_state, _entry, _token, _head, _now_ms),
    do: {:error, :invalid_cleanup_runtime}

  defp finalize_fenced_workspace_cleanup(state, entry, token, head, now_ms) do
    workspace_path = Map.get(entry, :workspace_path)
    worker_host = Map.get(entry, :worker_host)

    case Workspace.path_exists?(workspace_path, worker_host) do
      {:ok, false} ->
        persist_fenced_cleanup(state, token, head, now_ms, Map.get(entry, :terminal_outcome))

      {:ok, true} ->
        Logger.warning("Preserving fenced workspace because its path remains present after cleanup")
        state

      {:error, reason} ->
        Logger.warning("Preserving fenced workspace because path absence could not be verified: #{inspect(reason)}")
        state
    end
  end

  defp persist_fenced_cleanup(state, token, head, now_ms, terminal_outcome) do
    case ExecutionFence.cleanup(state.execution_fence, token, head, now_ms) do
      {:ok, fence_state, result} when result in [:cleaned, :already_cleaned] ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} ->
            submit_repository_cleanup_receipt(next_state, token, head, terminal_outcome || cleanup_terminal_outcome(next_state, token))

          {:error, reason} ->
            Logger.error("Execution-fence cleanup state was not persisted: #{inspect(reason)}")
            state
        end

      {:error, reason} ->
        Logger.warning("Preserving fenced workspace after cleanup rejection: #{inspect(reason)}")
        state
    end
  end

  defp submit_repository_cleanup_receipt(%State{work_package_runtime: runtime} = state, token, head, terminal_outcome)
       when is_map(runtime) do
    case cleanup_evidence_ref(runtime, state, token, head) do
      {:ok, evidence_ref} ->
        execution = get_in(state.execution_fence, [:executions, token.issue_id])

        input =
          runtime
          |> Map.merge(%{
            issue_id: token.issue_id,
            repository_ref: execution.repository,
            fence_state: state.execution_fence
          })

        attrs = %{terminal_outcome: terminal_outcome, accepted_head: head, evidence_ref: evidence_ref}
        opts = [] |> maybe_claim_option(runtime, :request_fun) |> maybe_claim_option(runtime, :now_fun)

        case WorkPackageCleanupReceipt.repository_cleanup_verified(input, attrs, opts) do
          {:ok, _result} ->
            state

          {:error, reason} ->
            Logger.warning("Trusted repository cleanup receipt was not accepted for issue_id=#{token.issue_id}: #{inspect(reason)}")
            state
        end

      {:error, reason} ->
        quarantine? = cleanup_evidence_missing_archive?(reason)

        Logger.warning(
          "Repository cleanup evidence was not sent without an independent verification record " <>
            "for issue_id=#{token.issue_id}: #{inspect(reason)}" <>
            if(quarantine?, do: "; quarantining replay until restart", else: "")
        )

        if quarantine?,
          do: quarantine_cleanup_receipt(state, token, "repository_cleanup_verified"),
          else: state
    end
  end

  defp submit_repository_cleanup_receipt(state, _token, _head, _terminal_outcome), do: state

  defp cleanup_evidence_missing_archive?({:cleanup_manifest_unreadable, :enoent}), do: true
  defp cleanup_evidence_missing_archive?(:cleanup_archive_missing_workspace), do: true
  defp cleanup_evidence_missing_archive?(_reason), do: false

  defp cleanup_evidence_ref(runtime, state, token, head) do
    case Map.get(runtime, :cleanup_evidence_fun) do
      verifier when is_function(verifier, 3) ->
        case verifier.(state, token, head) do
          {:ok, evidence_ref} when is_binary(evidence_ref) and evidence_ref != "" -> {:ok, evidence_ref}
          {:error, _reason} = error -> error
          _ -> {:error, :invalid_cleanup_evidence}
        end

      _ ->
        {:error, :cleanup_evidence_verifier_missing}
    end
  rescue
    error -> {:error, {:cleanup_evidence_verifier_failed, error}}
  end

  defp record_head_divergence(state, token, expected_head, observed_head) do
    case ExecutionFence.record_head_divergence(
           state.execution_fence,
           token,
           expected_head,
           observed_head,
           execution_fence_now_ms()
         ) do
      {:ok, fence_state, :recorded} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} ->
            Logger.warning("Preserving fenced workspace after head divergence expected=#{expected_head} observed=#{observed_head}; triage recorded")
            next_state

          {:error, reason} ->
            Logger.error("Head-divergence triage was not persisted: #{inspect(reason)}")
            state
        end

      {:ok, _fence_state, :already_recorded} ->
        Logger.warning("Preserving fenced workspace after previously recorded head divergence expected=#{expected_head} observed=#{observed_head}")
        state

      {:error, reason} ->
        Logger.error("Head-divergence triage was rejected: #{inspect(reason)}")
        state
    end
  end

  defp execution_workspace_path(fence_state, %{issue_id: issue_id, generation: generation}, entry) do
    case Map.get(fence_state.executions, issue_id) do
      %{generation: ^generation, worktree: worktree} when is_binary(worktree) ->
        case Map.get(entry, :workspace_path) do
          path when is_binary(path) and path != "" and path == worktree ->
            {:ok, worktree}

          path when is_binary(path) and path != "" ->
            {:error, :execution_workspace_mismatch}

          _ ->
            {:ok, worktree}
        end

      _ ->
        {:error, :unknown_execution_workspace}
    end
  end

  defp execution_workspace_path(_fence_state, _token, _entry),
    do: {:error, :invalid_execution_token}

  defp runtime_info_belongs_to_entry?(runtime_info, entry) do
    Map.get(runtime_info, :execution_token) == Map.get(entry, :execution_token) and
      Map.get(runtime_info, :execution_session_id) == Map.get(entry, :execution_session_id)
  end

  defp observe_worker_runtime_head(state, entry, runtime_info) do
    case Map.get(runtime_info, :head) do
      head when is_binary(head) ->
        token = Map.get(entry, :execution_token)
        session_id = Map.get(entry, :execution_session_id)

        case ExecutionFence.observe_session_head(
               state.execution_fence,
               token,
               session_id,
               head,
               execution_fence_now_ms()
             ) do
          {:ok, fence_state} ->
            case persist_execution_fence(state, fence_state) do
              {:ok, next_state} ->
                {next_state, head}

              {:error, reason} ->
                Logger.error("Worker head observation was not persisted: #{inspect(reason)}")
                {state, nil}
            end

          {:error, reason} ->
            Logger.warning("Worker head observation rejected: #{inspect(reason)}")
            {state, nil}
        end

      _ ->
        {state, nil}
    end
  end

  defp revalidate_issue_for_dispatch(%Issue{id: issue_id}, issue_fetcher, terminal_states)
       when is_binary(issue_id) and is_function(issue_fetcher, 1) do
    case issue_fetcher.([issue_id]) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        if retry_candidate_issue?(refreshed_issue, terminal_states) do
          {:ok, refreshed_issue}
        else
          {:skip, refreshed_issue}
        end

      {:ok, []} ->
        {:skip, :missing}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp revalidate_issue_for_dispatch(issue, _issue_fetcher, _terminal_states), do: {:ok, issue}

  defp complete_issue(%State{} = state, issue_id) do
    %{
      state
      | completed: MapSet.put(state.completed, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id),
        stall_restarts: Map.delete(state.stall_restarts, issue_id)
    }
  end

  defp schedule_issue_retry(%State{} = state, issue_id, attempt, metadata)
       when is_binary(issue_id) and is_map(metadata) do
    previous_retry = Map.get(state.retry_attempts, issue_id, %{attempt: 0})
    next_attempt = if is_integer(attempt), do: attempt, else: previous_retry.attempt + 1
    delay_ms = retry_delay(next_attempt, metadata)
    old_timer = Map.get(previous_retry, :timer_ref)
    retry_token = make_ref()
    due_at_ms = System.monotonic_time(:millisecond) + delay_ms
    identifier = pick_retry_identifier(issue_id, previous_retry, metadata)
    issue_url = pick_retry_issue_url(previous_retry, metadata)
    error = pick_retry_error(previous_retry, metadata)
    worker_host = pick_retry_worker_host(previous_retry, metadata)
    workspace_path = pick_retry_workspace_path(previous_retry, metadata)
    stall_diagnostic = metadata[:stall_diagnostic] || Map.get(previous_retry, :stall_diagnostic)

    if is_reference(old_timer) do
      Process.cancel_timer(old_timer)
    end

    timer_ref = Process.send_after(self(), {:retry_issue, issue_id, retry_token}, delay_ms)

    error_suffix = if is_binary(error), do: " error=#{error}", else: ""

    Logger.warning("Retrying issue_id=#{issue_id} issue_identifier=#{identifier} in #{delay_ms}ms (attempt #{next_attempt})#{error_suffix}")

    %{
      state
      | retry_attempts:
          Map.put(state.retry_attempts, issue_id, %{
            attempt: next_attempt,
            timer_ref: timer_ref,
            retry_token: retry_token,
            due_at_ms: due_at_ms,
            identifier: identifier,
            issue_url: issue_url,
            error: error,
            worker_host: worker_host,
            workspace_path: workspace_path,
            execution_token: Map.get(metadata, :execution_token),
            execution_session_id: Map.get(metadata, :execution_session_id),
            accepted_head: Map.get(metadata, :accepted_head),
            merge_identity: Map.get(metadata, :merge_identity),
            stall_diagnostic: stall_diagnostic
          })
    }
  end

  defp pop_retry_attempt_state(%State{} = state, issue_id, retry_token) when is_reference(retry_token) do
    case Map.get(state.retry_attempts, issue_id) do
      %{attempt: attempt, retry_token: ^retry_token} = retry_entry ->
        metadata = %{
          identifier: Map.get(retry_entry, :identifier),
          issue_url: Map.get(retry_entry, :issue_url),
          error: Map.get(retry_entry, :error),
          worker_host: Map.get(retry_entry, :worker_host),
          workspace_path: Map.get(retry_entry, :workspace_path),
          execution_token: Map.get(retry_entry, :execution_token),
          execution_session_id: Map.get(retry_entry, :execution_session_id),
          accepted_head: Map.get(retry_entry, :accepted_head),
          merge_identity: Map.get(retry_entry, :merge_identity),
          stall_diagnostic: Map.get(retry_entry, :stall_diagnostic)
        }

        {:ok, attempt, metadata, %{state | retry_attempts: Map.delete(state.retry_attempts, issue_id)}}

      _ ->
        :missing
    end
  end

  defp handle_retry_issue(%State{} = state, issue_id, attempt, metadata) do
    if GlobalPause.paused?() do
      Logger.debug("Global mutable admission is paused; retaining retry for issue_id=#{issue_id}")

      {:noreply,
       schedule_issue_retry(
         state,
         issue_id,
         attempt,
         Map.merge(metadata, %{error: "global mutable admission paused"})
       )}
    else
      case Tracker.fetch_issues_by_ids([issue_id]) do
        {:ok, issues} ->
          issues
          |> find_issue_by_id(issue_id)
          |> handle_retry_issue_lookup(state, issue_id, attempt, metadata)

        {:error, reason} ->
          Logger.warning("Retry poll failed for issue_id=#{issue_id} issue_identifier=#{metadata[:identifier] || issue_id}: #{inspect(reason)}")

          {:noreply,
           schedule_issue_retry(
             state,
             issue_id,
             attempt + 1,
             Map.merge(metadata, %{error: "retry poll failed: #{inspect(reason)}"})
           )}
      end
    end
  end

  defp handle_retry_issue_lookup(%Issue{} = issue, state, issue_id, attempt, metadata) do
    terminal_states = terminal_state_set()

    cond do
      terminal_issue_state?(issue.state, terminal_states) ->
        Logger.info("Issue state is terminal: issue_id=#{issue_id} issue_identifier=#{issue.identifier} state=#{issue.state}; removing associated workspace")

        state = maybe_fence_terminal_execution(state, Map.put(metadata, :issue, issue), true)
        state = cleanup_fenced_workspace_or_legacy(state, issue, metadata)
        {:noreply, release_issue_claim(state, issue_id)}

      retry_candidate_issue?(issue, terminal_states) ->
        handle_active_retry(state, issue, attempt, metadata)

      true ->
        Logger.debug("Issue left active states, removing claim issue_id=#{issue_id} issue_identifier=#{issue.identifier}")

        {:noreply, release_issue_claim(state, issue_id)}
    end
  end

  defp handle_retry_issue_lookup(nil, state, issue_id, _attempt, _metadata) do
    Logger.debug("Issue no longer visible, removing claim issue_id=#{issue_id}")
    {:noreply, release_issue_claim(state, issue_id)}
  end

  defp cleanup_issue_workspace(identifier, worker_host)

  defp cleanup_issue_workspace(issue_or_identifier, metadata) when is_map(metadata) do
    case Map.get(metadata, :workspace_path) do
      workspace_path when is_binary(workspace_path) and workspace_path != "" ->
        Workspace.remove_recorded(workspace_path, Map.get(metadata, :worker_host))

      _ ->
        cleanup_issue_workspace(issue_or_identifier, Map.get(metadata, :worker_host))
    end
  end

  defp cleanup_issue_workspace(%Issue{} = issue, worker_host) do
    Workspace.remove_issue_workspaces(issue, worker_host)
  end

  defp cleanup_issue_workspace(identifier, worker_host) when is_binary(identifier) do
    Workspace.remove_issue_workspaces(identifier, worker_host)
  end

  defp cleanup_issue_workspace(_issue_or_identifier, _worker_host), do: :ok

  defp start_startup_maintenance(%State{} = state, opts) do
    metadata = StartupMaintenance.start()
    task_supervisor = state.task_supervisor
    owner = self()

    startup_cleanup_fun =
      Keyword.get_lazy(opts, :startup_cleanup_fun, fn ->
        fn issue -> startup_workspace_cleanup(state, issue, owner) end
      end)

    maintenance_work =
      case Keyword.get(opts, :startup_maintenance_fun) do
        fun when is_function(fun, 0) ->
          fun

        _ ->
          fn ->
            StartupMaintenance.run(
              &Tracker.fetch_issues_by_states/1,
              startup_cleanup_fun,
              Config.settings!().tracker
            )
          end
      end

    maintenance_fun = fn ->
      replay_persisted_cleanup_receipts(state)
      maintenance_work.()
    end

    task =
      Task.Supervisor.async_nolink(task_supervisor, maintenance_fun)

    Process.send_after(self(), {:startup_maintenance_timeout, task.ref}, StartupMaintenance.timeout_ms())

    %{
      state
      | startup_maintenance:
          metadata
          |> Map.put(:task_ref, task.ref)
          |> Map.put(:task_pid, task.pid)
    }
  end

  defp replay_persisted_cleanup_receipts(%State{work_package_runtime: nil} = state), do: state

  defp replay_persisted_cleanup_receipts(%State{work_package_runtime: runtime} = state)
       when is_map(runtime) do
    actions =
      Enum.flat_map(state.execution_fence.executions, fn {issue_id, execution} ->
        token = %{issue_id: issue_id, generation: execution.generation, repository_ref: execution.repository}

        termination_actions =
          Enum.flat_map(execution.leases, fn {_session_id, lease} ->
            case {execution.terminal, Map.get(lease, :termination_confirmed_at_ms), Map.get(lease, :termination_evidence)} do
              {terminal, at_ms, evidence} when is_map(terminal) and is_integer(at_ms) and is_map(evidence) ->
                [
                  {:termination, token, lease.session_id, evidence, %Issue{id: issue_id, identifier: issue_id}, cleanup_terminal_outcome(state, token)}
                ]

              _ ->
                []
            end
          end)

        repository_actions =
          if execution.cleanup == :cleaned do
            case {get_in(execution, [:terminal, :accepted_head]), cleanup_terminal_outcome(state, token)} do
              {head, outcome} when is_binary(head) and head != "" -> [{:repository, token, head, outcome}]
              _ -> []
            end
          else
            []
          end

        termination_actions ++ repository_actions
      end)

    actions
    |> Enum.reject(&cleanup_receipt_quarantined?(state, &1))
    |> Enum.filter(&cleanup_receipt_pending?(runtime, &1))
    |> Enum.take(@cleanup_receipt_replay_limit)
    |> Enum.reduce(state, fn
      {:termination, token, session_id, evidence, issue, outcome}, current_state ->
        submit_termination_cleanup_receipt(
          current_state,
          %{execution_token: token, execution_session_id: session_id, issue: issue, terminal_outcome: outcome},
          evidence
        )

      {:repository, token, head, outcome}, current_state ->
        submit_repository_cleanup_receipt(current_state, token, head, outcome)
    end)
  rescue
    error ->
      Logger.warning("Persisted cleanup receipt replay failed: #{inspect(error)}")
      state
  end

  defp replay_persisted_cleanup_receipts(state), do: state

  defp cleanup_receipt_quarantined?(%State{} = state, action) do
    case cleanup_receipt_replay_key(action) do
      nil -> false
      key -> MapSet.member?(state.cleanup_receipt_quarantine, key)
    end
  end

  defp cleanup_receipt_replay_key({:termination, token, _session_id, _evidence, _issue, _outcome}),
    do: cleanup_receipt_replay_key(token, "termination_confirmed")

  defp cleanup_receipt_replay_key({:repository, token, _head, _outcome}),
    do: cleanup_receipt_replay_key(token, "repository_cleanup_verified")

  defp cleanup_receipt_replay_key(_action), do: nil

  defp cleanup_receipt_replay_key(token, receipt_kind)
       when is_map(token) and is_binary(receipt_kind) do
    case {Map.get(token, :issue_id), Map.get(token, :generation)} do
      {issue_id, generation} when is_binary(issue_id) and is_integer(generation) and generation > 0 ->
        {issue_id, generation, receipt_kind}

      _ ->
        nil
    end
  end

  defp quarantine_cleanup_receipt(%State{} = state, token, receipt_kind) do
    case cleanup_receipt_replay_key(token, receipt_kind) do
      nil -> state
      key -> %{state | cleanup_receipt_quarantine: MapSet.put(state.cleanup_receipt_quarantine, key)}
    end
  end

  # Acknowledged receipts are durable and need no further provider call.  Filter
  # them before taking the bounded replay batch; otherwise a large historical
  # journal can permanently starve a newer pending receipt.  An unreadable or
  # malformed journal/ack deliberately remains pending so the normal submit
  # path logs the concrete validation error instead of silently hiding it.
  defp cleanup_receipt_pending?(runtime, {kind, token, _session_id, _evidence, _issue, _outcome})
       when kind == :termination do
    cleanup_receipt_pending?(runtime, token, "termination_confirmed")
  end

  defp cleanup_receipt_pending?(runtime, {kind, token, _head, _outcome}) when kind == :repository do
    cleanup_receipt_pending?(runtime, token, "repository_cleanup_verified")
  end

  defp cleanup_receipt_pending?(_runtime, _action), do: true

  defp cleanup_receipt_pending?(runtime, token, receipt_kind)
       when is_map(runtime) and is_map(token) and is_binary(receipt_kind) do
    with issue_id when is_binary(issue_id) <- Map.get(token, :issue_id),
         generation when is_integer(generation) and generation > 0 <- Map.get(token, :generation),
         profile_id when is_binary(profile_id) <- Map.get(runtime, :managed_project_profile_id),
         repository_ref when is_binary(repository_ref) <- Map.get(token, :repository_ref),
         path when is_binary(path) <- Map.get(runtime, :journal_path),
         {:ok, journal} <- load_cleanup_journal(path),
         key <- Journal.reservation_key(issue_id, profile_id, repository_ref, generation),
         reservation when is_map(reservation) <- Map.get(journal.reservations, key),
         {:ok, semantic} <- Journal.cleanup_receipt(journal, key, receipt_kind),
         {:ok, acknowledgement} <- Journal.cleanup_receipt_ack(journal, key, receipt_kind),
         true <- valid_replayed_ack?(semantic, acknowledgement, reservation, receipt_kind) do
      false
    else
      _ -> true
    end
  end

  defp load_cleanup_journal(path) do
    case Journal.load(path) do
      :missing -> {:ok, Journal.new()}
      {:ok, journal} -> {:ok, journal}
      {:error, _reason} = error -> error
    end
  end

  defp valid_replayed_ack?(semantic, acknowledgement, reservation, receipt_kind)
       when is_map(semantic) and is_map(acknowledgement) and is_map(reservation) do
    expected = %{
      projection_id: Map.get(reservation, :projection_id),
      reservation_id: Map.get(reservation, :reservation_id),
      receipt_id: Map.get(semantic, :receipt_id),
      receipt_kind: receipt_kind,
      generation: Map.get(reservation, :generation),
      evidence_ref: Map.get(semantic, :evidence_ref),
      accepted_head: Map.get(semantic, :accepted_head),
      execution_capacity_state: "released"
    }

    semantic_complete? =
      Enum.all?([:receipt_id, :evidence_ref, :accepted_head], &present_string?(Map.get(semantic, &1)))

    semantic_identity? =
      Map.get(semantic, :receipt_kind) == receipt_kind and
        Map.get(semantic, :generation) == Map.get(reservation, :generation)

    acknowledgement_complete? = Enum.all?(expected, fn {_field, value} -> not is_nil(value) end)
    values_match? = Enum.all?(expected, fn {field, value} -> Map.get(acknowledgement, field) == value end)

    state_match? =
      case receipt_kind do
        "termination_confirmed" ->
          {Map.get(acknowledgement, :scope_state), Map.get(acknowledgement, :reservation_state)} in [{"held", "claimed"}, {"released", "released"}]

        "repository_cleanup_verified" ->
          {Map.get(acknowledgement, :scope_state), Map.get(acknowledgement, :reservation_state)} ==
            {"released", "released"}

        _ ->
          false
      end

    semantic_complete? and semantic_identity? and acknowledgement_complete? and values_match? and state_match? and
      is_boolean(Map.get(acknowledgement, :replayed))
  end

  defp valid_replayed_ack?(_semantic, _acknowledgement, _reservation, _receipt_kind), do: false

  defp defer_startup_workspace_cleanup(%Issue{} = issue) do
    Logger.info("Deferring startup cleanup for terminal issue #{issue_context(issue)} until its persisted execution fence is reconciled")
    {:error, :execution_fence_reconciliation_required}
  end

  defp startup_workspace_cleanup(%State{} = state, %Issue{id: issue_id} = issue, owner)
       when is_pid(owner) do
    executions = state.execution_fence.executions

    case Map.get(executions, issue_id) do
      nil ->
        Workspace.remove_issue_workspaces_for_startup(issue)

      %{cleanup: :cleaned} ->
        Workspace.remove_issue_workspaces_for_startup(issue)

      %{
        cleanup: :pending,
        cleanup_receipt: %{phase: :removal_started, expected_head: expected_head},
        generation: generation,
        worktree: workspace,
        worker_host: worker_host
      } ->
        replay_persisted_cleanup(state, owner, issue, generation, workspace, worker_host, expected_head)

      _execution ->
        defer_startup_workspace_cleanup(issue)
    end
  end

  defp startup_workspace_cleanup(_state, %Issue{} = issue, _owner),
    do: defer_startup_workspace_cleanup(issue)

  defp replay_persisted_cleanup(state, owner, issue, generation, workspace, worker_host, expected_head) do
    token = %{issue_id: issue.id, generation: generation}

    case Workspace.path_exists?(workspace, worker_host) do
      {:ok, false} ->
        fence_state =
          case Persistence.load(state.execution_fence_path) do
            {:ok, persisted_state} -> persisted_state
            _ -> state.execution_fence
          end

        case ExecutionFence.cleanup(fence_state, token, expected_head, execution_fence_now_ms()) do
          {:ok, fence_state, _result} ->
            case Persistence.save(state.execution_fence_path, fence_state) do
              :ok ->
                replay_persisted_cleanup_receipts(%{state | execution_fence: fence_state})
                send(owner, {:startup_cleanup_fence_updated, fence_state})
                :ok

              {:error, reason} ->
                {:error, {:execution_fence_persistence_failed, reason}}
            end

          {:error, reason} ->
            {:error, reason}
        end

      {:ok, true} ->
        defer_startup_workspace_cleanup(issue)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp notify_dashboard do
    StatusDashboard.notify_update()
  end

  defp handle_active_retry(state, issue, attempt, metadata) do
    if retry_candidate_issue?(issue, terminal_state_set()) and
         dispatch_slots_available?(issue, state) and
         worker_slots_available?(state, metadata[:worker_host]) do
      case refresh_issue_for_dispatch(issue) do
        {:ok, %Issue{} = refreshed_issue} ->
          {:noreply, do_dispatch_issue(state, refreshed_issue, attempt, metadata[:worker_host])}

        {:skip, :missing} ->
          {:noreply, release_issue_claim(state, issue.id)}

        {:skip, %Issue{} = refreshed_issue} ->
          handle_retry_issue_lookup(refreshed_issue, state, issue.id, attempt, metadata)

        {:error, reason} ->
          {:noreply,
           schedule_issue_retry(
             state,
             issue.id,
             attempt + 1,
             Map.merge(metadata, %{
               identifier: issue.identifier,
               error: "retry dispatch refresh failed: #{inspect(reason)}"
             })
           )}
      end
    else
      Logger.debug("No available slots for retrying #{issue_context(issue)}; retrying again")

      {:noreply,
       schedule_issue_retry(
         state,
         issue.id,
         attempt + 1,
         Map.merge(metadata, %{
           identifier: issue.identifier,
           error: "no available orchestrator slots"
         })
       )}
    end
  end

  defp release_issue_claim(%State{} = state, issue_id) do
    %{
      state
      | claimed: MapSet.delete(state.claimed, issue_id),
        blocked: Map.delete(state.blocked, issue_id),
        retry_attempts: Map.delete(state.retry_attempts, issue_id),
        stall_restarts: Map.delete(state.stall_restarts, issue_id),
        codex_issue_totals: ManagedBudget.release_totals(state, issue_id)
    }
  end

  defp retry_delay(attempt, metadata) when is_integer(attempt) and attempt > 0 and is_map(metadata) do
    if metadata[:delay_type] == :continuation and attempt == 1 do
      @continuation_retry_delay_ms
    else
      failure_retry_delay(attempt)
    end
  end

  defp failure_retry_delay(attempt) do
    max_delay_power = min(attempt - 1, 10)
    min(@failure_retry_base_ms * (1 <<< max_delay_power), Config.settings!().agent.max_retry_backoff_ms)
  end

  defp normalize_retry_attempt(attempt) when is_integer(attempt) and attempt > 0, do: attempt
  defp normalize_retry_attempt(_attempt), do: 0

  defp next_retry_attempt_from_running(running_entry) do
    case Map.get(running_entry, :retry_attempt) do
      attempt when is_integer(attempt) and attempt > 0 -> attempt + 1
      _ -> nil
    end
  end

  defp pick_retry_identifier(issue_id, previous_retry, metadata) do
    metadata[:identifier] || Map.get(previous_retry, :identifier) || issue_id
  end

  defp pick_retry_issue_url(previous_retry, metadata) do
    metadata[:issue_url] || Map.get(previous_retry, :issue_url)
  end

  defp pick_retry_error(previous_retry, metadata) do
    metadata[:error] || Map.get(previous_retry, :error)
  end

  defp pick_retry_worker_host(previous_retry, metadata) do
    metadata[:worker_host] || Map.get(previous_retry, :worker_host)
  end

  defp pick_retry_workspace_path(previous_retry, metadata) do
    metadata[:workspace_path] || Map.get(previous_retry, :workspace_path)
  end

  defp maybe_put_runtime_value(running_entry, _key, nil), do: running_entry

  defp maybe_put_runtime_value(running_entry, key, value) when is_map(running_entry) do
    Map.put(running_entry, key, value)
  end

  defp select_worker_host(%State{} = state, preferred_worker_host) do
    case Config.settings!().worker.ssh_hosts do
      [] ->
        nil

      hosts ->
        available_hosts = Enum.filter(hosts, &worker_host_slots_available?(state, &1))

        cond do
          available_hosts == [] ->
            :no_worker_capacity

          preferred_worker_host_available?(preferred_worker_host, available_hosts) ->
            preferred_worker_host

          true ->
            least_loaded_worker_host(state, available_hosts)
        end
    end
  end

  defp preferred_worker_host_available?(preferred_worker_host, hosts)
       when is_binary(preferred_worker_host) and is_list(hosts) do
    preferred_worker_host != "" and preferred_worker_host in hosts
  end

  defp preferred_worker_host_available?(_preferred_worker_host, _hosts), do: false

  defp least_loaded_worker_host(%State{} = state, hosts) when is_list(hosts) do
    hosts
    |> Enum.with_index()
    |> Enum.min_by(fn {host, index} ->
      {running_worker_host_count(state.running, host), index}
    end)
    |> elem(0)
  end

  defp running_worker_host_count(running, worker_host) when is_map(running) and is_binary(worker_host) do
    Enum.count(running, fn
      {_issue_id, %{worker_host: ^worker_host}} -> true
      _ -> false
    end)
  end

  defp worker_slots_available?(%State{} = state) do
    select_worker_host(state, nil) != :no_worker_capacity
  end

  defp worker_slots_available?(%State{} = state, preferred_worker_host) do
    select_worker_host(state, preferred_worker_host) != :no_worker_capacity
  end

  defp worker_host_slots_available?(%State{} = state, worker_host) when is_binary(worker_host) do
    case Config.settings!().worker.max_concurrent_agents_per_host do
      limit when is_integer(limit) and limit > 0 ->
        running_worker_host_count(state.running, worker_host) < limit

      _ ->
        true
    end
  end

  defp find_issue_by_id(issues, issue_id) when is_binary(issue_id) do
    Enum.find(issues, fn
      %Issue{id: ^issue_id} ->
        true

      _ ->
        false
    end)
  end

  defp find_issue_id_for_ref(running, ref) do
    running
    |> Enum.find_value(fn {issue_id, %{ref: running_ref}} ->
      if running_ref == ref, do: issue_id
    end)
  end

  defp running_entry_session_id(%{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp running_entry_session_id(_running_entry), do: "n/a"

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp available_slots(%State{} = state) do
    pending =
      if is_map(state.work_package_runtime) do
        Enum.count(state.execution_fence.executions, fn {id, _execution} ->
          not Map.has_key?(state.running, id) and ClaimRecovery.held?(state.execution_fence, id)
        end)
      else
        0
      end

    max(
      (state.max_concurrent_agents || Config.settings!().agent.max_concurrent_agents) -
        map_size(state.running) - pending,
      0
    )
  end

  @spec request_refresh() :: map() | :unavailable
  def request_refresh do
    request_refresh(__MODULE__)
  end

  @spec request_refresh(GenServer.server()) :: map() | :unavailable
  def request_refresh(server) do
    if Process.whereis(server) do
      GenServer.call(server, :request_refresh)
    else
      :unavailable
    end
  end

  @spec snapshot() :: map() | :timeout | :unavailable
  def snapshot, do: snapshot(__MODULE__, 15_000)

  @spec snapshot(GenServer.server(), timeout()) :: map() | :timeout | :unavailable
  def snapshot(server, timeout) do
    if Process.whereis(server) do
      try do
        GenServer.call(server, :snapshot, timeout)
      catch
        :exit, {:timeout, _} -> :timeout
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc "Reconciles an explicit sanitized execution-session observation snapshot."
  @spec reconcile_execution_fence([map()], non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def reconcile_execution_fence(observations, now_ms) when is_list(observations) do
    reconcile_execution_fence(__MODULE__, observations, now_ms, @execution_fence_lease_ttl_ms)
  end

  @spec reconcile_execution_fence([map()], non_neg_integer(), pos_integer()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def reconcile_execution_fence(observations, now_ms, ttl_ms) when is_list(observations) do
    reconcile_execution_fence(__MODULE__, observations, now_ms, ttl_ms)
  end

  @spec reconcile_execution_fence(GenServer.server(), [map()], non_neg_integer(), pos_integer()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def reconcile_execution_fence(server, observations, now_ms, ttl_ms)
      when is_list(observations) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:execution_fence_reconcile, observations, now_ms, ttl_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc false
  @spec register_execution_session(GenServer.server(), map(), :worker | :reviewer, map(), non_neg_integer()) ::
          {:ok, :registered | :already_registered} | {:error, term()} | :unavailable
  def register_execution_session(server, token, role, attrs, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:execution_fence_register, token, role, attrs, now_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc false
  @spec record_execution_supervisor(GenServer.server(), map(), String.t(), map()) ::
          {:ok, :recorded} | {:error, term()} | :unavailable
  def record_execution_supervisor(server, token, session_id, identity) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:execution_fence_supervisor, token, session_id, identity})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc false
  @spec heartbeat_execution_session(GenServer.server(), map(), String.t(), non_neg_integer()) ::
          {:ok, :persisted} | {:error, term()} | :unavailable
  def heartbeat_execution_session(server, token, session_id, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:execution_fence_heartbeat, token, session_id, now_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc false
  @spec release_execution_session(GenServer.server(), map(), String.t(), atom()) ::
          {:ok, :released | :already_released} | {:error, term()} | :unavailable
  def release_execution_session(server, token, session_id, reason \\ :released) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:execution_fence_release, token, session_id, reason})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc false
  @spec confirm_execution_termination(GenServer.server(), map(), String.t(), map(), non_neg_integer()) ::
          {:ok, :confirmed | :already_confirmed} | {:error, term()} | :unavailable
  def confirm_execution_termination(server, token, session_id, evidence, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:execution_fence_confirm_termination, token, session_id, evidence, now_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc false
  @spec fence_execution(GenServer.server(), map(), map(), non_neg_integer()) ::
          {:ok, :fenced | :already_fenced} | {:error, term()} | :unavailable
  def fence_execution(server, token, attrs, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:execution_fence_terminal, token, attrs, now_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc "Returns the read-only responsibility/delegation projection."
  @spec responsibility_snapshot() :: map() | :unavailable
  def responsibility_snapshot, do: responsibility_snapshot(__MODULE__)

  @spec responsibility_snapshot(GenServer.server()) :: map() | :unavailable
  def responsibility_snapshot(server) do
    if server_available?(server) do
      try do
        GenServer.call(server, :responsibility_snapshot)
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc "Activates machine-enforced responsibility admission after the graph is proven."
  @spec activate_responsibility_graph(non_neg_integer()) ::
          {:ok, :activated | :already_activated} | {:error, term()} | :unavailable
  def activate_responsibility_graph(now_ms),
    do: activate_responsibility_graph(__MODULE__, now_ms)

  @spec activate_responsibility_graph(GenServer.server(), non_neg_integer()) ::
          {:ok, :activated | :already_activated} | {:error, term()} | :unavailable
  def activate_responsibility_graph(server, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:responsibility_activate, now_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc "Registers a typed responsibility delegation and persists it."
  @spec delegate_responsibility(map(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def delegate_responsibility(attrs, now_ms), do: delegate_responsibility(__MODULE__, attrs, now_ms)

  @spec delegate_responsibility(GenServer.server(), map(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def delegate_responsibility(server, attrs, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:responsibility_delegate, attrs, now_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc "Renews a responsibility lease heartbeat."
  @spec heartbeat_responsibility(String.t(), non_neg_integer()) ::
          {:ok, :persisted} | {:error, term()} | :unavailable
  def heartbeat_responsibility(delegation_id, now_ms),
    do: heartbeat_responsibility(__MODULE__, delegation_id, now_ms)

  @spec heartbeat_responsibility(GenServer.server(), String.t(), non_neg_integer()) ::
          {:ok, :persisted} | {:error, term()} | :unavailable
  def heartbeat_responsibility(server, delegation_id, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:responsibility_heartbeat, delegation_id, now_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc "Reconciles a restart-blocked delegation against its HGS-294 lease reference."
  @spec reconcile_responsibility(String.t(), map() | nil, non_neg_integer()) ::
          {:ok, :persisted} | {:error, term()} | :unavailable
  def reconcile_responsibility(delegation_id, runtime_lease, now_ms),
    do: reconcile_responsibility(__MODULE__, delegation_id, runtime_lease, now_ms)

  @spec reconcile_responsibility(GenServer.server(), String.t(), map() | nil, non_neg_integer()) ::
          {:ok, :persisted} | {:error, term()} | :unavailable
  def reconcile_responsibility(server, delegation_id, runtime_lease, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:responsibility_reconcile, delegation_id, runtime_lease, now_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc "Authorizes a graph action and its HGS-294 runtime lease together."
  @spec authorize_responsibility(String.t(), atom()) :: {:ok, map()} | {:error, term()} | :unavailable
  def authorize_responsibility(delegation_id, action),
    do: authorize_responsibility(__MODULE__, delegation_id, action)

  @spec authorize_responsibility(GenServer.server(), String.t(), atom()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def authorize_responsibility(server, delegation_id, action) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:responsibility_authorize, delegation_id, action})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc "Revokes a responsibility tree and fences its referenced HGS-294 generations."
  @spec revoke_responsibility(String.t(), term(), map(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def revoke_responsibility(delegation_id, reason, terminal_attrs, now_ms),
    do: revoke_responsibility(__MODULE__, delegation_id, reason, terminal_attrs, now_ms)

  @spec revoke_responsibility(GenServer.server(), String.t(), term(), map(), non_neg_integer()) ::
          {:ok, map()} | {:error, term()} | :unavailable
  def revoke_responsibility(server, delegation_id, reason, terminal_attrs, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(server, {:responsibility_revoke, delegation_id, reason, terminal_attrs, now_ms})
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  @doc false
  @spec record_execution_head_divergence(GenServer.server(), map(), String.t(), String.t(), non_neg_integer()) ::
          {:ok, :recorded | :already_recorded} | {:error, term()} | :unavailable
  def record_execution_head_divergence(server, token, expected_head, observed_head, now_ms) do
    if server_available?(server) do
      try do
        GenServer.call(
          server,
          {:execution_fence_head_divergence, token, expected_head, observed_head, now_ms}
        )
      catch
        :exit, _ -> :unavailable
      end
    else
      :unavailable
    end
  end

  defp persist_managed_failed_turn(state, issue_id, update, sender) do
    runtime = state.work_package_runtime
    entry = Map.get(state.running, issue_id)

    with %{pid: ^sender, codex_session_identity: %{thread_id: thread_id, turn_id: turn_id}} <- entry,
         true <- runtime_info_belongs_to_entry?(update, entry),
         true <- is_binary(thread_id) and thread_id != "" and is_binary(turn_id) and turn_id != "",
         %{
           journal_path: path,
           managed_project_profile_id: profile_id,
           managed_delegations: %{repository_ref: repository_ref}
         } <- runtime,
         true <- is_binary(path) and is_binary(profile_id) and is_binary(repository_ref),
         %{issue_id: ^issue_id, generation: generation} <- entry.execution_token,
         %{status: :active, generation: ^generation, repository: ^repository_ref, leases: leases} <-
           Map.get(state.execution_fence.executions, issue_id),
         %{status: :active} <- Map.get(leases, entry.execution_session_id),
         {:ok, journal} <- Journal.load(path),
         key <- Journal.reservation_key(issue_id, profile_id, repository_ref, generation),
         %{session_id: session_id, responsible_delegation_id: delegation_id, execution_fence_token: fence_token} <-
           Map.get(journal.reservations, key),
         true <-
           session_id == entry.execution_session_id and delegation_id == entry.responsibility_delegation_id and
             fence_token == "#{issue_id}:#{generation}",
         %{
           "method" => "turn/failed",
           "params" => %{"threadId" => ^thread_id, "turn" => %{"id" => ^turn_id}}
         } <-
           Map.get(update, :payload),
         {:ok, encoded} <- Jason.encode(Map.get(update, :payload)),
         evidence <- %{
           thread_id: thread_id,
           turn_id: turn_id,
           observed_at_ms: execution_fence_now_ms(),
           payload_sha256: Base.encode16(:crypto.hash(:sha256, encoded), case: :lower)
         },
         {:ok, next_journal} <- Journal.put_failed_worker_turn(journal, key, "#{thread_id}:#{turn_id}", evidence),
         :ok <- Journal.save(path, next_journal) do
      :ok
    else
      {:error, _reason} = error -> error
      _ -> {:error, :managed_failed_turn_identity_mismatch}
    end
  end

  @impl true
  def handle_call({:managed_failed_turn, issue_id, %{event: :turn_failed} = update}, {sender, _tag}, %State{} = state) do
    reply = persist_managed_failed_turn(state, issue_id, update, sender)
    {:reply, reply, state}
  end

  def handle_call({:execution_checkout_progress, issue_id, checkpoint}, {sender, _tag}, %State{} = state) do
    case Checkpoint.accept(state, issue_id, sender, checkpoint, DateTime.utc_now()) do
      {:ok, entry} -> {:reply, :ok, %{state | running: Map.put(state.running, issue_id, entry)}}
      {:error, _} = error -> {:reply, error, state}
    end
  end

  def handle_call({:execution_fence_authorize, token, action}, _from, %State{} = state) do
    {:reply, ExecutionFence.authorize(state.execution_fence, token, action), state}
  end

  def handle_call({:execution_authorize, token, nil, action}, _from, %State{} = state) do
    {:reply, ExecutionFence.authorize(state.execution_fence, token, action), state}
  end

  def handle_call({:execution_authorize, token, delegation_id, action}, _from, %State{} = state)
      when is_binary(delegation_id) do
    reply =
      if ResponsibilityGraph.enforced?(state.responsibility_graph) do
        ResponsibilityGraph.authorize_with_execution_fence(
          state.responsibility_graph,
          delegation_id,
          action,
          state.execution_fence
        )
      else
        ExecutionFence.authorize(state.execution_fence, token, action)
      end

    {:reply, reply, state}
  end

  def handle_call({:execution_fence_register, token, role, attrs, now_ms}, _from, %State{} = state) do
    case ExecutionFence.register(state.execution_fence, token, role, attrs, now_ms) do
      {:ok, fence_state, result} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} -> {:reply, {:ok, result}, next_state}
          {:error, reason} -> {:reply, {:error, {:execution_fence_persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execution_fence_supervisor, token, session_id, identity}, _from, %State{} = state) do
    with {:ok, captured_identity} <- ExecutionSupervisor.capture(identity, timeout_ms: 1_000),
         {:ok, fence_state} <- ExecutionFence.record_supervisor(state.execution_fence, token, session_id, captured_identity) do
      case persist_execution_fence(state, fence_state) do
        {:ok, next_state} -> {:reply, {:ok, :recorded}, next_state}
        {:error, reason} -> {:reply, {:error, {:execution_fence_persistence_failed, reason}}, state}
      end
    else
      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execution_fence_heartbeat, token, session_id, now_ms}, _from, %State{} = state) do
    case ExecutionFence.heartbeat(state.execution_fence, token, session_id, now_ms) do
      {:ok, fence_state} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} -> {:reply, {:ok, :persisted}, next_state}
          {:error, reason} -> {:reply, {:error, {:execution_fence_persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execution_fence_release, token, session_id, reason}, _from, %State{} = state) do
    case ExecutionFence.release(state.execution_fence, token, session_id, reason) do
      {:ok, fence_state, result} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} ->
            {:reply, {:ok, result}, next_state}

          {:error, persist_reason} ->
            {:reply, {:error, {:execution_fence_persistence_failed, persist_reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(
        {:execution_fence_confirm_termination, token, session_id, evidence, now_ms},
        _from,
        %State{} = state
      ) do
    case ExecutionFence.confirm_termination(
           state.execution_fence,
           token,
           session_id,
           evidence,
           now_ms
         ) do
      {:ok, fence_state, result} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} ->
            {:reply, {:ok, result}, next_state}

          {:error, reason} ->
            {:reply, {:error, {:execution_fence_persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execution_fence_terminal, token, attrs, now_ms}, _from, %State{} = state) do
    case ExecutionFence.fence(state.execution_fence, token, attrs, now_ms) do
      {:ok, fence_state, result} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} -> {:reply, {:ok, result}, next_state}
          {:error, reason} -> {:reply, {:error, {:execution_fence_persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(
        {:execution_fence_head_divergence, token, expected_head, observed_head, now_ms},
        _from,
        %State{} = state
      ) do
    case ExecutionFence.record_head_divergence(
           state.execution_fence,
           token,
           expected_head,
           observed_head,
           now_ms
         ) do
      {:ok, fence_state, result} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} -> {:reply, {:ok, result}, next_state}
          {:error, reason} -> {:reply, {:error, {:execution_fence_persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:execution_fence_reconcile, observations, now_ms, ttl_ms}, _from, %State{} = state) do
    result =
      with {:ok, claims} <- ClaimRecovery.unstarted_claims(state.work_package_runtime, state.execution_fence) do
        ExecutionFence.reconcile_claim_sessions(state.execution_fence, observations, claims, now_ms, ttl_ms)
      end

    case result do
      {:ok, fence_state, summary} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, next_state} ->
            reply = {:ok, %{summary: summary, execution_fence: execution_fence_snapshot(fence_state)}}
            {:reply, reply, next_state}

          {:error, reason} ->
            {:reply, {:error, {:execution_fence_persistence_failed, reason}}, state}
        end

      {:error, reason} = error ->
        Logger.warning("Execution-fence reconciliation rejected: #{inspect(reason)}")
        {:reply, error, state}
    end
  end

  def handle_call(:responsibility_snapshot, _from, %State{} = state) do
    {:reply, ResponsibilityGraph.snapshot(state.responsibility_graph), state}
  end

  def handle_call({:responsibility_activate, now_ms}, _from, %State{} = state) do
    case ResponsibilityGraph.activate(state.responsibility_graph, now_ms) do
      {:ok, graph_state, result} ->
        case persist_responsibility_graph(state, graph_state) do
          {:ok, next_state} -> {:reply, {:ok, result}, next_state}
          {:error, reason} -> {:reply, {:error, {:responsibility_persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:responsibility_delegate, attrs, now_ms}, _from, %State{} = state) do
    case ResponsibilityGraph.delegate(state.responsibility_graph, attrs, now_ms) do
      {:ok, graph_state, delegation} ->
        case persist_responsibility_graph(state, graph_state) do
          {:ok, next_state} -> {:reply, {:ok, delegation}, next_state}
          {:error, reason} -> {:reply, {:error, {:responsibility_persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:responsibility_heartbeat, delegation_id, now_ms}, _from, %State{} = state) do
    case ResponsibilityGraph.heartbeat(state.responsibility_graph, delegation_id, now_ms) do
      {:ok, graph_state} ->
        case persist_responsibility_graph(state, graph_state) do
          {:ok, next_state} -> {:reply, {:ok, :persisted}, next_state}
          {:error, reason} -> {:reply, {:error, {:responsibility_persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:responsibility_reconcile, delegation_id, runtime_lease, now_ms}, _from, %State{} = state) do
    case ResponsibilityGraph.reconcile_delegation(
           state.responsibility_graph,
           delegation_id,
           runtime_lease,
           now_ms
         ) do
      {:ok, graph_state} ->
        case persist_responsibility_graph(state, graph_state) do
          {:ok, next_state} -> {:reply, {:ok, :persisted}, next_state}
          {:error, reason} -> {:reply, {:error, {:responsibility_persistence_failed, reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:responsibility_authorize, delegation_id, action}, _from, %State{} = state) do
    {:reply,
     ResponsibilityGraph.authorize_with_execution_fence(
       state.responsibility_graph,
       delegation_id,
       action,
       state.execution_fence
     ), state}
  end

  def handle_call(
        {:responsibility_revoke, delegation_id, reason, terminal_attrs, now_ms},
        _from,
        %State{} = state
      ) do
    case ResponsibilityGraph.revoke_and_fence(
           state.responsibility_graph,
           state.execution_fence,
           delegation_id,
           reason,
           terminal_attrs,
           now_ms
         ) do
      {:ok, graph_state, fence_state, impact} ->
        case persist_execution_fence(state, fence_state) do
          {:ok, fence_persisted_state} ->
            case persist_responsibility_graph(fence_persisted_state, graph_state) do
              {:ok, next_state} ->
                {:reply, {:ok, impact}, next_state}

              {:error, persist_reason} ->
                {:reply, {:error, {:responsibility_persistence_failed, persist_reason}}, fence_persisted_state}
            end

          {:error, persist_reason} ->
            {:reply, {:error, {:execution_fence_persistence_failed, persist_reason}}, state}
        end

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(:snapshot, _from, state) do
    state = refresh_runtime_config(state)
    now = DateTime.utc_now()
    now_ms = System.monotonic_time(:millisecond)

    identity_snapshot =
      RuntimeIdentity.snapshot(
        state.execution_fence,
        state.responsibility_graph,
        managed_pool?: WorkPackageRuntime.managed_pool?(),
        managed_runtime_configured?: is_map(state.work_package_runtime),
        managed_delegation_manifest: get_in(state.work_package_runtime || %{}, [:managed_delegations])
      )

    running =
      state.running
      |> Enum.map(fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: metadata.identifier,
          issue_url: metadata.issue.url,
          state: metadata.issue.state,
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: metadata.session_id,
          codex_app_server_pid: metadata.codex_app_server_pid,
          codex_input_tokens: metadata.codex_input_tokens,
          codex_output_tokens: metadata.codex_output_tokens,
          codex_total_tokens: metadata.codex_total_tokens,
          codex_issue_total_tokens: issue_token_total(state, issue_id),
          codex_max_total_tokens: Config.settings!().codex.max_total_tokens,
          turn_count: Map.get(metadata, :turn_count, 0),
          started_at: metadata.started_at,
          last_codex_timestamp: metadata.last_codex_timestamp,
          last_codex_message: metadata.last_codex_message,
          last_codex_event: metadata.last_codex_event,
          configured_stall_timeout_ms: Config.settings!().codex.stall_timeout_ms,
          qualifying_activity_silence_ms: stall_elapsed_ms(metadata, now_ms),
          last_qualifying_activity_class: Map.get(metadata, :codex_last_activity_method) || "worker_started",
          checkout_progress: %{
            head: Map.get(metadata, :checkout_head),
            sequence: Map.get(metadata, :checkout_progress_sequence),
            committed: Map.get(metadata, :checkout_commit_checkpoint)
          },
          runtime_seconds: running_seconds(metadata.started_at, now)
        }
      end)

    retrying =
      state.retry_attempts
      |> Enum.map(fn {issue_id, %{attempt: attempt, due_at_ms: due_at_ms} = retry} ->
        %{
          issue_id: issue_id,
          attempt: attempt,
          due_in_ms: max(0, due_at_ms - now_ms),
          identifier: Map.get(retry, :identifier),
          issue_url: Map.get(retry, :issue_url),
          error: Map.get(retry, :error),
          worker_host: Map.get(retry, :worker_host),
          workspace_path: Map.get(retry, :workspace_path),
          stall_diagnostic: Map.get(retry, :stall_diagnostic)
        }
      end)

    blocked =
      state.blocked
      |> Enum.map(fn {issue_id, metadata} ->
        %{
          issue_id: issue_id,
          identifier: Map.get(metadata, :identifier),
          issue_url: blocked_issue_url(metadata),
          state: blocked_issue_state(metadata),
          worker_host: Map.get(metadata, :worker_host),
          workspace_path: Map.get(metadata, :workspace_path),
          session_id: Map.get(metadata, :session_id),
          error: Map.get(metadata, :error),
          blocked_at: Map.get(metadata, :blocked_at),
          last_codex_timestamp: Map.get(metadata, :last_codex_timestamp),
          last_codex_message: Map.get(metadata, :last_codex_message),
          last_codex_event: Map.get(metadata, :last_codex_event),
          stall_diagnostic: Map.get(metadata, :stall_diagnostic)
        }
      end)

    {:reply,
     %{
       running: running,
       retrying: retrying,
       blocked: blocked,
       execution_fence: execution_fence_snapshot(state.execution_fence),
       responsibility_graph: ResponsibilityGraph.snapshot(state.responsibility_graph),
       codex_totals: state.codex_totals,
       codex_issue_totals: state.codex_issue_totals,
       rate_limits: Map.get(state, :codex_rate_limits),
       pause_gate: GlobalPause.snapshot(),
       startup_maintenance: StartupMaintenance.snapshot(state.startup_maintenance),
       runtime_identity: identity_snapshot.runtime_identity,
       execution_authority: identity_snapshot.execution_authority,
       managed_work_package: identity_snapshot.managed_work_package,
       readiness: identity_snapshot.readiness,
       polling: %{
         checking?: state.poll_check_in_progress == true,
         next_poll_in_ms: next_poll_in_ms(state.next_poll_due_at_ms, now_ms),
         poll_interval_ms: state.poll_interval_ms
       }
     }, state}
  end

  def handle_call(:request_refresh, _from, state) do
    now_ms = System.monotonic_time(:millisecond)
    already_due? = is_integer(state.next_poll_due_at_ms) and state.next_poll_due_at_ms <= now_ms
    coalesced = state.poll_check_in_progress == true or already_due?
    state = if coalesced, do: state, else: schedule_tick(state, 0)

    {:reply,
     %{
       queued: true,
       coalesced: coalesced,
       requested_at: DateTime.utc_now(),
       operations: ["poll", "reconcile"]
     }, state}
  end

  defp blocked_issue_state(%{issue: %Issue{state: state}}), do: state
  defp blocked_issue_state(_metadata), do: nil

  defp blocked_issue_url(%{issue: %Issue{url: url}}), do: url
  defp blocked_issue_url(_metadata), do: nil

  defp execution_fence_snapshot(fence_state) do
    case ExecutionFence.snapshot(fence_state) do
      snapshot when is_map(snapshot) -> snapshot
      {:error, reason} -> %{schema_version: 1, status: :invalid, error: reason}
    end
  end

  defp load_execution_fence(path) when is_binary(path) do
    case Persistence.load(path) do
      :missing ->
        fence_state = ExecutionFence.new()

        case Persistence.save(path, fence_state) do
          :ok -> {:ok, fence_state}
          {:error, reason} -> {:error, {:initial_persistence_failed, reason}}
        end

      {:ok, fence_state} ->
        with {:ok, restarted_state} <- ExecutionFence.mark_unreconciled_after_restart(fence_state),
             {:ok, reconciled_state} <- reconcile_persisted_supervisors(restarted_state),
             :ok <- Persistence.save(path, reconciled_state) do
          {:ok, reconciled_state}
        else
          {:error, reason} -> {:error, {:restart_reconciliation_persistence_failed, reason}}
          other -> {:error, {:restart_reconciliation_persistence_failed, other}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_execution_fence(path), do: {:error, {:invalid_state_path, path}}

  @doc false
  @spec reconcile_persisted_supervisors_for_test(map(), keyword()) :: {:ok, map()}
  def reconcile_persisted_supervisors_for_test(state, opts \\ []) when is_map(state) and is_list(opts) do
    reconcile_persisted_supervisors(state, opts)
  end

  defp reconcile_persisted_supervisors(state, opts \\ []) do
    now_ms = Keyword.get(opts, :now_ms, execution_fence_now_ms())

    reconciled_state =
      Enum.reduce(state.executions, state, fn {issue_id, execution}, state_acc ->
        token = %{issue_id: issue_id, generation: execution.generation}

        Enum.reduce(execution.leases, state_acc, fn {session_id, lease}, lease_state ->
          reconcile_persisted_supervisor(lease_state, token, session_id, lease, now_ms, opts)
        end)
      end)

    {:ok, reconciled_state}
  end

  defp reconcile_persisted_supervisor(state, token, session_id, lease, now_ms, opts) do
    case Map.get(lease, :supervisor_identity) do
      identity when is_map(identity) ->
        state = release_restarted_supervisor_lease(state, token, session_id)

        if is_nil(Map.get(lease, :termination_confirmed_at_ms)) do
          terminate = Keyword.get(opts, :termination_fun, &ExecutionSupervisor.terminate/2)

          case terminate.(identity, opts) do
            {:ok, evidence} ->
              confirmation_now_ms = max(now_ms, Map.get(evidence, :observed_at_ms, now_ms))

              case ExecutionFence.confirm_termination(
                     state,
                     token,
                     session_id,
                     evidence,
                     confirmation_now_ms
                   ) do
                {:ok, fence_state, _result} ->
                  case ExecutionFence.validate(fence_state) do
                    :ok ->
                      fence_state

                    {:error, reason} ->
                      Logger.error("Restart termination proof produced invalid fence state: #{inspect(reason)}")
                      state
                  end

                {:error, reason} ->
                  Logger.warning("Restart termination proof was rejected for session_id=#{session_id}: #{inspect(reason)}")
                  state
              end

            {:error, reason} ->
              Logger.warning("Restart termination could not be proven for session_id=#{session_id}: #{inspect(reason)}")
              state
          end
        else
          state
        end

      _ ->
        state
    end
  end

  defp release_restarted_supervisor_lease(state, token, session_id) do
    case ExecutionFence.release(state, token, session_id, :orchestrator_stop) do
      {:ok, fence_state, _result} ->
        fence_state

      {:error, reason} ->
        Logger.warning("Restart supervisor lease could not be marked for termination session_id=#{session_id}: #{inspect(reason)}")
        state
    end
  end

  defp load_responsibility_graph(path) when is_binary(path) do
    case ResponsibilityPersistence.load(path) do
      :missing ->
        graph_state = ResponsibilityGraph.new()

        case ResponsibilityPersistence.save(path, graph_state) do
          :ok -> {:ok, graph_state}
          {:error, reason} -> {:error, {:initial_persistence_failed, reason}}
        end

      {:ok, graph_state} ->
        with {:ok, restarted_state} <- ResponsibilityGraph.mark_unreconciled_after_restart(graph_state),
             :ok <- ResponsibilityPersistence.save(path, restarted_state) do
          {:ok, restarted_state}
        else
          {:error, reason} -> {:error, {:restart_reconciliation_persistence_failed, reason}}
          other -> {:error, {:restart_reconciliation_persistence_failed, other}}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp load_responsibility_graph(path), do: {:error, {:invalid_state_path, path}}

  defp persist_execution_fence(%State{execution_fence_path: nil} = state, fence_state) do
    {:ok, %{state | execution_fence: fence_state}}
  end

  defp persist_execution_fence(%State{execution_fence_path: path} = state, fence_state)
       when is_binary(path) do
    case Persistence.save(path, fence_state) do
      :ok -> {:ok, %{state | execution_fence: fence_state}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_execution_fence(_state, _fence_state), do: {:error, :invalid_state_path}

  defp persist_responsibility_graph(%State{responsibility_graph_path: nil} = state, graph_state) do
    {:ok, %{state | responsibility_graph: graph_state}}
  end

  defp persist_responsibility_graph(%State{responsibility_graph_path: path} = state, graph_state)
       when is_binary(path) do
    case ResponsibilityPersistence.save(path, graph_state) do
      :ok -> {:ok, %{state | responsibility_graph: graph_state}}
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_responsibility_graph(_state, _graph_state), do: {:error, :invalid_state_path}

  defp execution_fence_now_ms, do: max(0, System.system_time(:millisecond))

  defp server_available?(server) when is_pid(server), do: Process.alive?(server)
  defp server_available?(server) when is_atom(server), do: is_pid(Process.whereis(server))
  defp server_available?(_server), do: false

  defp handle_current_codex_update(state, issue_id, running_entry, update) do
    {updated_running_entry, token_delta} = integrate_codex_update(running_entry, update)

    state =
      state
      |> apply_codex_token_delta(token_delta)
      |> apply_codex_issue_token_delta(issue_id, token_delta)
      |> ManagedBudget.observe(issue_id, updated_running_entry, update)
      |> apply_codex_rate_limits(update)

    updated_running_entry =
      Map.put(
        updated_running_entry,
        :codex_issue_total_tokens,
        issue_token_total(state, issue_id)
      )

    if state.managed_token_budget_error do
      error = "managed token accounting requires reconciliation"
      Logger.error("Managed usage persistence failed: issue_id=#{issue_id} issue_identifier=#{running_entry.identifier} session_id=#{running_entry_session_id(updated_running_entry)}")
      {:noreply, stop_and_block_issue(state, issue_id, updated_running_entry, error)}
    else
      enforce_total_token_budget(state, issue_id, updated_running_entry)
    end
  end

  defp enforce_total_token_budget(state, issue_id, updated_running_entry) do
    case ManagedBudget.effective_limit(state, issue_id) do
      {:ok, threshold} ->
        enforce_total_token_budget(state, issue_id, updated_running_entry, threshold)

      {:error, reason} ->
        state = stop_for_invalid_token_budget(state, issue_id, updated_running_entry, reason)
        notify_dashboard()
        {:noreply, state}
    end
  end

  defp enforce_total_token_budget(state, issue_id, updated_running_entry, threshold) do
    if is_integer(threshold) and threshold > 0 and issue_token_total(state, issue_id) >= threshold do
      total_tokens = issue_token_total(state, issue_id)
      diagnostic = total_token_budget_diagnostic(updated_running_entry, total_tokens, threshold)
      error = total_token_budget_error(total_tokens, threshold)

      Logger.warning(
        "Issue stopped after cumulative Codex token budget: issue_id=#{issue_id} issue_identifier=#{Map.get(updated_running_entry, :identifier, issue_id)} total_tokens=#{total_tokens} threshold=#{threshold}"
      )

      state =
        state
        |> record_session_completion_totals(updated_running_entry)
        |> stop_and_block_issue(
          issue_id,
          Map.put(updated_running_entry, :stall_diagnostic, diagnostic),
          error
        )

      notify_dashboard()
      {:noreply, state}
    else
      notify_dashboard()
      {:noreply, %{state | running: Map.put(state.running, issue_id, updated_running_entry)}}
    end
  end

  defp integrate_codex_update(running_entry, %{event: event, timestamp: timestamp} = update) do
    {file_change_result, seen_file_change_ids} =
      Progress.accept_file_change(
        update,
        running_entry
        |> Map.get(:codex_session_identity, %{})
        |> Map.put(:id, running_entry.session_id),
        Map.get(running_entry, :workspace_path),
        Map.get(running_entry, :seen_file_change_ids, MapSet.new())
      )

    token_delta = extract_token_delta(running_entry, update)
    codex_input_tokens = Map.get(running_entry, :codex_input_tokens, 0)
    codex_output_tokens = Map.get(running_entry, :codex_output_tokens, 0)
    codex_total_tokens = Map.get(running_entry, :codex_total_tokens, 0)
    codex_app_server_pid = Map.get(running_entry, :codex_app_server_pid)
    last_reported_input = Map.get(running_entry, :codex_last_reported_input_tokens, 0)
    last_reported_output = Map.get(running_entry, :codex_last_reported_output_tokens, 0)
    last_reported_total = Map.get(running_entry, :codex_last_reported_total_tokens, 0)
    turn_count = Map.get(running_entry, :turn_count, 0)

    updated_running_entry =
      Map.merge(running_entry, %{
        seen_file_change_ids: seen_file_change_ids,
        codex_session_identity: codex_session_identity_for_update(running_entry, update),
        last_codex_timestamp: timestamp,
        last_codex_message: summarize_codex_update(update),
        session_id: session_id_for_update(running_entry.session_id, update),
        last_codex_event: event,
        codex_app_server_pid: codex_app_server_pid_for_update(codex_app_server_pid, update),
        codex_input_tokens: codex_input_tokens + token_delta.input_tokens,
        codex_output_tokens: codex_output_tokens + token_delta.output_tokens,
        codex_total_tokens: codex_total_tokens + token_delta.total_tokens,
        codex_last_reported_input_tokens: max(last_reported_input, token_delta.input_reported),
        codex_last_reported_output_tokens: max(last_reported_output, token_delta.output_reported),
        codex_last_reported_total_tokens: max(last_reported_total, token_delta.total_reported),
        turn_count: turn_count_for_update(turn_count, running_entry.session_id, update)
      })

    updated_running_entry =
      if file_change_result == :accepted or meaningful_progress_update?(update) do
        updated_running_entry
        |> Map.put(:codex_progress_token_baseline, updated_running_entry.codex_total_tokens)
        |> Map.put(:codex_last_progress_timestamp, timestamp)
        |> Map.put(:codex_last_progress_method, codex_update_method(update))
      else
        updated_running_entry
      end

    updated_running_entry =
      if qualifying_liveness_update?(update) do
        updated_running_entry
        |> Map.put(:codex_last_activity_monotonic_ms, monotonic_now_ms())
        |> Map.put(:codex_last_activity_method, codex_update_method(update))
      else
        updated_running_entry
      end

    updated_running_entry =
      if file_change_result == :accepted or durable_progress_update?(update) do
        updated_running_entry
        |> Map.put(:codex_durable_progress_token_baseline, updated_running_entry.codex_total_tokens)
        |> Map.put(:codex_last_durable_progress_timestamp, timestamp)
        |> Map.put(:codex_last_durable_progress_method, codex_update_method(update))
      else
        updated_running_entry
      end

    {updated_running_entry, token_delta}
  end

  defp codex_session_identity_for_update(_entry, %{
         event: :session_started,
         thread_id: thread_id,
         turn_id: turn_id
       }),
       do: %{thread_id: thread_id, turn_id: turn_id}

  defp codex_session_identity_for_update(entry, _update),
    do: Map.get(entry, :codex_session_identity, %{})

  defp meaningful_progress_update?(%{event: event})
       when event in [
              :session_started,
              :turn_completed,
              :turn_failed,
              :turn_cancelled,
              :tool_call_completed
            ],
       do: true

  defp meaningful_progress_update?(update) do
    case codex_update_method(update) do
      method when is_binary(method) ->
        String.contains?(method, "commandExecution") or
          String.contains?(method, "/tool/") or
          String.contains?(method, "fileChange")

      _ ->
        false
    end
  end

  defp qualifying_liveness_update?(%{event: event})
       when event in [
              :session_started,
              :turn_completed,
              :turn_failed,
              :turn_cancelled,
              :tool_call_completed,
              :turn_input_required,
              :approval_required
            ],
       do: true

  defp qualifying_liveness_update?(update) do
    case codex_update_method(update) do
      method when is_binary(method) ->
        String.starts_with?(method, "item/") or
          String.contains?(method, "commandExecution") or
          String.contains?(method, "/tool/") or
          String.contains?(method, "fileChange") or
          method == "mcpServer/elicitation/request"

      _ ->
        false
    end
  end

  defp durable_progress_update?(%{event: event})
       when event in [:session_started, :turn_completed, :turn_failed, :turn_cancelled],
       do: true

  defp durable_progress_update?(_update), do: false

  defp codex_update_method(%{payload: %{"method" => method}}) when is_binary(method), do: method
  defp codex_update_method(%{payload: %{method: method}}) when is_binary(method), do: method
  defp codex_update_method(%{event: event}) when is_atom(event), do: Atom.to_string(event)
  defp codex_update_method(_update), do: nil

  defp monotonic_now_ms do
    case Application.get_env(:symphony_elixir, :monotonic_now_ms) do
      now_ms when is_integer(now_ms) -> now_ms
      _ -> System.monotonic_time(:millisecond)
    end
  end

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_binary(pid),
       do: pid

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid})
       when is_integer(pid),
       do: Integer.to_string(pid)

  defp codex_app_server_pid_for_update(_existing, %{codex_app_server_pid: pid}) when is_list(pid),
    do: to_string(pid)

  defp codex_app_server_pid_for_update(existing, _update), do: existing

  defp session_id_for_update(_existing, %{session_id: session_id}) when is_binary(session_id),
    do: session_id

  defp session_id_for_update(existing, _update), do: existing

  defp turn_count_for_update(existing_count, existing_session_id, %{
         event: :session_started,
         session_id: session_id
       })
       when is_integer(existing_count) and is_binary(session_id) do
    if session_id == existing_session_id do
      existing_count
    else
      existing_count + 1
    end
  end

  defp turn_count_for_update(existing_count, _existing_session_id, _update)
       when is_integer(existing_count),
       do: existing_count

  defp turn_count_for_update(_existing_count, _existing_session_id, _update), do: 0

  defp summarize_codex_update(update) do
    %{
      event: update[:event],
      message: update[:payload] || update[:raw],
      timestamp: update[:timestamp]
    }
  end

  defp schedule_tick(%State{} = state, delay_ms) when is_integer(delay_ms) and delay_ms >= 0 do
    if is_reference(state.tick_timer_ref) do
      Process.cancel_timer(state.tick_timer_ref)
    end

    tick_token = make_ref()
    timer_ref = Process.send_after(self(), {:tick, tick_token}, delay_ms)

    %{
      state
      | tick_timer_ref: timer_ref,
        tick_token: tick_token,
        next_poll_due_at_ms: System.monotonic_time(:millisecond) + delay_ms
    }
  end

  defp schedule_poll_cycle_start do
    :timer.send_after(@poll_transition_render_delay_ms, self(), :run_poll_cycle)
    :ok
  end

  defp next_poll_in_ms(nil, _now_ms), do: nil

  defp next_poll_in_ms(next_poll_due_at_ms, now_ms) when is_integer(next_poll_due_at_ms) do
    max(0, next_poll_due_at_ms - now_ms)
  end

  defp pop_running_entry(state, issue_id) do
    {Map.get(state.running, issue_id), %{state | running: Map.delete(state.running, issue_id)}}
  end

  defp record_session_completion_totals(state, running_entry) when is_map(running_entry) do
    runtime_seconds = running_seconds(running_entry.started_at, DateTime.utc_now())

    codex_totals =
      apply_token_delta(
        state.codex_totals,
        %{
          input_tokens: 0,
          output_tokens: 0,
          total_tokens: 0,
          seconds_running: runtime_seconds
        }
      )

    %{state | codex_totals: codex_totals}
  end

  defp record_session_completion_totals(state, _running_entry), do: state

  defp refresh_runtime_config(%State{} = state) do
    config = Config.settings!()

    %{
      state
      | poll_interval_ms: config.polling.interval_ms,
        max_concurrent_agents: config.agent.max_concurrent_agents
    }
  end

  defp retry_candidate_issue?(%Issue{} = issue, terminal_states) do
    candidate_issue?(issue, active_state_set(), terminal_states)
  end

  defp dispatch_slots_available?(%Issue{} = issue, %State{} = state) do
    available_slots(state) > 0 and state_slots_available?(issue, state.running)
  end

  defp apply_codex_token_delta(
         %{codex_totals: codex_totals} = state,
         %{input_tokens: input, output_tokens: output, total_tokens: total} = token_delta
       )
       when is_integer(input) and is_integer(output) and is_integer(total) do
    %{state | codex_totals: apply_token_delta(codex_totals, token_delta)}
  end

  defp apply_codex_token_delta(state, _token_delta), do: state

  defp apply_codex_issue_token_delta(%State{work_package_runtime: runtime} = state, _issue_id, _delta)
       when not is_nil(runtime), do: state

  defp apply_codex_issue_token_delta(
         %State{} = state,
         issue_id,
         %{total_tokens: total_tokens}
       )
       when is_binary(issue_id) and is_integer(total_tokens) do
    totals = state.codex_issue_totals || %{}
    next_total = issue_token_total(state, issue_id) + max(total_tokens, 0)
    %{state | codex_issue_totals: Map.put(totals, issue_id, next_total)}
  end

  defp apply_codex_issue_token_delta(state, _issue_id, _token_delta), do: state

  defp put_issue_token_total(%State{} = state, issue_id, total_tokens)
       when is_binary(issue_id) and is_integer(total_tokens) do
    %{state | codex_issue_totals: Map.put(state.codex_issue_totals || %{}, issue_id, max(total_tokens, issue_token_total(state, issue_id)))}
  end

  defp put_issue_token_total(state, _issue_id, _total_tokens), do: state

  defp issue_token_total(%State{} = state, issue_id) when is_binary(issue_id) do
    Map.get(state.codex_issue_totals || %{}, issue_id, 0)
  end

  defp issue_token_total(_state, _issue_id), do: 0

  defp stop_for_invalid_token_budget(state, issue_id, running_entry, reason) do
    state
    |> stop_and_block_issue(issue_id, running_entry, "managed token budget authority requires reconciliation")
    |> ManagedBudget.latch(reason)
  end

  defp total_token_budget_error(total_tokens, threshold) do
    "codex total token budget exhausted at #{total_tokens} tokens (threshold #{threshold})"
  end

  defp total_token_budget_diagnostic(running_entry, total_tokens, threshold) do
    %{
      reason: "codex_total_token_budget_exhausted",
      observed_at: DateTime.utc_now(),
      issue_total_tokens: total_tokens,
      total_token_threshold: threshold,
      current_attempt_total_tokens: Map.get(running_entry, :codex_total_tokens, 0),
      session_id: running_entry_session_id(running_entry),
      turn_count: Map.get(running_entry, :turn_count, 0),
      last_event: Map.get(running_entry, :last_codex_event),
      last_event_at: Map.get(running_entry, :last_codex_timestamp),
      worker_process_alive: worker_process_alive?(Map.get(running_entry, :pid)),
      codex_app_server_pid: Map.get(running_entry, :codex_app_server_pid),
      token_totals: %{
        input_tokens: Map.get(running_entry, :codex_input_tokens, 0),
        output_tokens: Map.get(running_entry, :codex_output_tokens, 0),
        total_tokens: Map.get(running_entry, :codex_total_tokens, 0)
      }
    }
  end

  defp apply_codex_rate_limits(%State{} = state, update) when is_map(update) do
    case extract_rate_limits(update) do
      %{} = rate_limits ->
        %{state | codex_rate_limits: rate_limits}

      _ ->
        state
    end
  end

  defp apply_codex_rate_limits(state, _update), do: state

  defp apply_token_delta(codex_totals, token_delta) do
    input_tokens = Map.get(codex_totals, :input_tokens, 0) + token_delta.input_tokens
    output_tokens = Map.get(codex_totals, :output_tokens, 0) + token_delta.output_tokens
    total_tokens = Map.get(codex_totals, :total_tokens, 0) + token_delta.total_tokens

    seconds_running =
      Map.get(codex_totals, :seconds_running, 0) + Map.get(token_delta, :seconds_running, 0)

    %{
      input_tokens: max(0, input_tokens),
      output_tokens: max(0, output_tokens),
      total_tokens: max(0, total_tokens),
      seconds_running: max(0, seconds_running)
    }
  end

  defp extract_token_delta(running_entry, %{event: _, timestamp: _} = update) do
    usage = extract_token_usage(update)

    {
      compute_token_delta(
        running_entry,
        :input,
        usage,
        :codex_last_reported_input_tokens
      ),
      compute_token_delta(
        running_entry,
        :output,
        usage,
        :codex_last_reported_output_tokens
      ),
      compute_token_delta(
        running_entry,
        :total,
        usage,
        :codex_last_reported_total_tokens
      )
    }
    |> Tuple.to_list()
    |> then(fn [input, output, total] ->
      %{
        input_tokens: input.delta,
        output_tokens: output.delta,
        total_tokens: total.delta,
        input_reported: input.reported,
        output_reported: output.reported,
        total_reported: total.reported
      }
    end)
  end

  defp compute_token_delta(running_entry, token_key, usage, reported_key) do
    next_total = get_token_usage(usage, token_key)
    prev_reported = Map.get(running_entry, reported_key, 0)

    delta =
      if is_integer(next_total) and next_total >= prev_reported do
        next_total - prev_reported
      else
        0
      end

    %{
      delta: max(delta, 0),
      reported: if(is_integer(next_total), do: next_total, else: prev_reported)
    }
  end

  defp extract_token_usage(update) do
    payloads = [
      update[:usage],
      Map.get(update, "usage"),
      Map.get(update, :usage),
      update[:payload],
      Map.get(update, "payload"),
      update
    ]

    Enum.find_value(payloads, &absolute_token_usage_from_payload/1) ||
      Enum.find_value(payloads, &turn_completed_usage_from_payload/1) ||
      %{}
  end

  defp extract_rate_limits(update) do
    rate_limits_from_payload(update[:rate_limits]) ||
      rate_limits_from_payload(Map.get(update, "rate_limits")) ||
      rate_limits_from_payload(Map.get(update, :rate_limits)) ||
      rate_limits_from_payload(update[:payload]) ||
      rate_limits_from_payload(Map.get(update, "payload")) ||
      rate_limits_from_payload(update)
  end

  defp absolute_token_usage_from_payload(payload) when is_map(payload) do
    absolute_paths = [
      ["params", "msg", "payload", "info", "total_token_usage"],
      [:params, :msg, :payload, :info, :total_token_usage],
      ["params", "msg", "info", "total_token_usage"],
      [:params, :msg, :info, :total_token_usage],
      ["params", "tokenUsage", "total"],
      [:params, :tokenUsage, :total],
      ["tokenUsage", "total"],
      [:tokenUsage, :total]
    ]

    explicit_map_at_paths(payload, absolute_paths)
  end

  defp absolute_token_usage_from_payload(_payload), do: nil

  defp turn_completed_usage_from_payload(payload) when is_map(payload) do
    method = Map.get(payload, "method") || Map.get(payload, :method)

    if method in ["turn/completed", :turn_completed] do
      direct =
        Map.get(payload, "usage") ||
          Map.get(payload, :usage) ||
          map_at_path(payload, ["params", "usage"]) ||
          map_at_path(payload, [:params, :usage])

      if is_map(direct) and integer_token_map?(direct), do: direct
    end
  end

  defp turn_completed_usage_from_payload(_payload), do: nil

  defp rate_limits_from_payload(payload) when is_map(payload) do
    direct = Map.get(payload, "rate_limits") || Map.get(payload, :rate_limits)

    cond do
      rate_limits_map?(direct) ->
        direct

      rate_limits_map?(payload) ->
        payload

      true ->
        rate_limit_payloads(payload)
    end
  end

  defp rate_limits_from_payload(payload) when is_list(payload) do
    rate_limit_payloads(payload)
  end

  defp rate_limits_from_payload(_payload), do: nil

  defp rate_limit_payloads(payload) when is_map(payload) do
    Map.values(payload)
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limit_payloads(payload) when is_list(payload) do
    payload
    |> Enum.reduce_while(nil, fn
      value, nil ->
        case rate_limits_from_payload(value) do
          nil -> {:cont, nil}
          rate_limits -> {:halt, rate_limits}
        end

      _value, result ->
        {:halt, result}
    end)
  end

  defp rate_limits_map?(payload) when is_map(payload) do
    limit_id =
      Map.get(payload, "limit_id") ||
        Map.get(payload, :limit_id) ||
        Map.get(payload, "limit_name") ||
        Map.get(payload, :limit_name)

    has_buckets =
      Enum.any?(
        ["primary", :primary, "secondary", :secondary, "credits", :credits],
        &Map.has_key?(payload, &1)
      )

    !is_nil(limit_id) and has_buckets
  end

  defp rate_limits_map?(_payload), do: false

  defp explicit_map_at_paths(payload, paths) when is_map(payload) and is_list(paths) do
    Enum.find_value(paths, fn path ->
      value = map_at_path(payload, path)

      if is_map(value) and integer_token_map?(value), do: value
    end)
  end

  defp explicit_map_at_paths(_payload, _paths), do: nil

  defp map_at_path(payload, path) when is_map(payload) and is_list(path) do
    Enum.reduce_while(path, payload, fn key, acc ->
      if is_map(acc) and Map.has_key?(acc, key) do
        {:cont, Map.get(acc, key)}
      else
        {:halt, nil}
      end
    end)
  end

  defp map_at_path(_payload, _path), do: nil

  defp integer_token_map?(payload) do
    token_fields = [
      :input_tokens,
      :output_tokens,
      :total_tokens,
      :prompt_tokens,
      :completion_tokens,
      :inputTokens,
      :outputTokens,
      :totalTokens,
      :promptTokens,
      :completionTokens,
      "input_tokens",
      "output_tokens",
      "total_tokens",
      "prompt_tokens",
      "completion_tokens",
      "inputTokens",
      "outputTokens",
      "totalTokens",
      "promptTokens",
      "completionTokens"
    ]

    token_fields
    |> Enum.any?(fn field ->
      value = payload_get(payload, field)
      !is_nil(integer_like(value))
    end)
  end

  defp get_token_usage(usage, :input),
    do:
      payload_get(usage, [
        "input_tokens",
        "prompt_tokens",
        :input_tokens,
        :prompt_tokens,
        :input,
        "promptTokens",
        :promptTokens,
        "inputTokens",
        :inputTokens
      ])

  defp get_token_usage(usage, :output),
    do:
      payload_get(usage, [
        "output_tokens",
        "completion_tokens",
        :output_tokens,
        :completion_tokens,
        :output,
        :completion,
        "outputTokens",
        :outputTokens,
        "completionTokens",
        :completionTokens
      ])

  defp get_token_usage(usage, :total),
    do:
      payload_get(usage, [
        "total_tokens",
        "total",
        :total_tokens,
        :total,
        "totalTokens",
        :totalTokens
      ])

  defp payload_get(payload, fields) when is_list(fields) do
    Enum.find_value(fields, fn field -> map_integer_value(payload, field) end)
  end

  defp payload_get(payload, field), do: map_integer_value(payload, field)

  defp map_integer_value(payload, field) do
    if is_map(payload) do
      value = Map.get(payload, field)
      integer_like(value)
    else
      nil
    end
  end

  defp running_seconds(%DateTime{} = started_at, %DateTime{} = now) do
    max(0, DateTime.diff(now, started_at, :second))
  end

  defp running_seconds(_started_at, _now), do: 0

  defp integer_like(value) when is_integer(value) and value >= 0, do: value

  defp integer_like(value) when is_binary(value) do
    case Integer.parse(String.trim(value)) do
      {num, _} when num >= 0 -> num
      _ -> nil
    end
  end

  defp integer_like(_value), do: nil
end
