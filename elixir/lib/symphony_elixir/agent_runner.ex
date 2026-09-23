defmodule SymphonyElixir.AgentRunner do
  @moduledoc """
  Executes a single tracker work item in its workspace with Codex.
  """

  require Logger
  alias SymphonyElixir.Codex.{AppServer, ModelRouter}
  alias SymphonyElixir.{Config, ManagedCheckout, PromptBuilder, Tracker, Workspace}
  alias SymphonyElixir.ManagedCheckout.Progress, as: CheckoutProgress
  alias SymphonyElixir.Tracker.Issue

  @type worker_host :: String.t() | nil

  @doc false
  @spec continue_with_issue_for_test(Issue.t(), ([String.t()] -> term())) ::
          {:continue, Issue.t()} | {:done, Issue.t()} | {:error, term()}
  def continue_with_issue_for_test(%Issue{} = issue, issue_state_fetcher)
      when is_function(issue_state_fetcher, 1) do
    continue_with_issue?(issue, issue_state_fetcher)
  end

  @spec run(map(), pid() | nil, keyword()) :: :ok | no_return()
  def run(issue, codex_update_recipient \\ nil, opts \\ []) do
    # The orchestrator owns host retries so one worker lifetime never hops machines.
    worker_host = selected_worker_host(Keyword.get(opts, :worker_host), Config.settings!().worker.ssh_hosts)

    Logger.info("Starting agent run for #{issue_context(issue)} worker_host=#{worker_host_for_log(worker_host)}")

    case run_on_worker_host(issue, codex_update_recipient, opts, worker_host) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.error("Agent run failed for #{issue_context(issue)}: #{inspect(reason)}")
        raise RuntimeError, "Agent run failed for #{issue_context(issue)}: #{inspect(reason)}"
    end
  end

  defp run_on_worker_host(issue, codex_update_recipient, opts, worker_host) do
    Logger.info("Starting worker attempt for #{issue_context(issue)} worker_host=#{worker_host_for_log(worker_host)}")

    with :ok <- execution_fence_preflight(opts) do
      case create_workspace(issue, worker_host, opts) do
        {:ok, workspace} ->
          opts = CheckoutProgress.attach(workspace, opts)
          opts = checkout_guarded_options(workspace, opts)

          try do
            send_worker_runtime_info(codex_update_recipient, issue, worker_host, workspace, opts)

            with :ok <- execution_fence_preflight(opts),
                 :ok <- Workspace.run_before_run_hook(workspace, issue, worker_host) do
              run_codex_turns(workspace, issue, codex_update_recipient, opts, worker_host)
            end
          after
            try do
              run_after_run_hook_if_authorized(workspace, issue, worker_host, opts)
              send_worker_runtime_info(codex_update_recipient, issue, worker_host, workspace, opts)
              require_checkout_progress!(issue, opts)
            after
              CheckoutProgress.clear(opts)
            end
          end

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp create_workspace(issue, worker_host, opts) do
    case Keyword.get(opts, :execution_checkout) do
      nil ->
        Workspace.create_for_issue(issue, worker_host)

      identity ->
        guard = fn -> execution_fence_preflight(opts) end
        Workspace.create_for_execution(issue, identity, worker_host, guard)
    end
  end

  defp require_checkout_progress!(issue, opts) do
    case CheckoutProgress.status(opts) do
      :ok -> :ok
      {:error, reason} -> raise RuntimeError, "Checkout progress failed: #{inspect(reason)} (#{issue_context(issue)})"
    end
  end

  defp checkout_guarded_options(workspace, opts) do
    case Keyword.get(opts, :execution_checkout) do
      nil ->
        opts

      identity ->
        Keyword.put(opts, :execution_fence_guard, fn -> checkout_preflight(workspace, identity, opts) end)
    end
  end

  defp checkout_preflight(workspace, identity, opts) do
    with :ok <- CheckoutProgress.status(opts),
         :ok <- execution_fence_preflight(opts),
         {:ok, observed} <- ManagedCheckout.verify(workspace, identity),
         :ok <- execution_fence_preflight(opts) do
      CheckoutProgress.observe(opts, observed)
    else
      {:error, reason} -> CheckoutProgress.fail(opts, reason)
    end
  end

  defp execution_fence_preflight(opts) do
    case Keyword.get(opts, :execution_fence_guard) do
      nil ->
        :ok

      guard when is_function(guard, 0) ->
        case guard.() do
          :ok -> :ok
          {:ok, _metadata} -> :ok
          {:error, _reason} = error -> error
          _other -> {:error, :invalid_execution_fence_guard_result}
        end

      _invalid ->
        {:error, :invalid_execution_fence_guard}
    end
  end

  defp codex_message_handler(recipient, issue, opts) do
    identity = %{
      execution_token: Keyword.get(opts, :execution_token),
      execution_session_id: Keyword.get(opts, :execution_session_id)
    }

    fn message ->
      send_codex_update(recipient, issue, Map.merge(message, identity))
    end
  end

  defp send_codex_update(recipient, %Issue{id: issue_id}, message)
       when is_binary(issue_id) and is_pid(recipient) do
    send(recipient, {:codex_worker_update, issue_id, message})
    :ok
  end

  defp send_codex_update(_recipient, _issue, _message), do: :ok

  defp send_worker_runtime_info(recipient, %Issue{id: issue_id}, worker_host, workspace, opts)
       when is_binary(issue_id) and is_pid(recipient) and is_binary(workspace) and is_list(opts) do
    if execution_fence_preflight(opts) == :ok do
      send_authorized_worker_runtime_info(recipient, issue_id, worker_host, workspace, opts)
    end

    :ok
  end

  defp send_worker_runtime_info(_recipient, _issue, _worker_host, _workspace, _opts), do: :ok

  defp send_authorized_worker_runtime_info(recipient, issue_id, worker_host, workspace, opts) do
    with {:ok, observed} <- observed_repository_info(workspace, worker_host, opts),
         :ok <- execution_fence_preflight(opts) do
      runtime_info =
        Map.merge(observed, %{
          worker_host: worker_host,
          workspace_path: workspace,
          execution_token: Keyword.get(opts, :execution_token),
          execution_session_id: Keyword.get(opts, :execution_session_id)
        })

      send(recipient, {:worker_runtime_info, issue_id, runtime_info})
    end

    :ok
  end

  defp observed_repository_info(workspace, worker_host, opts) do
    case Keyword.get(opts, :execution_checkout) do
      nil -> {:ok, maybe_put_runtime_head(%{}, Workspace.current_head(workspace, worker_host))}
      identity -> ManagedCheckout.verify(workspace, identity)
    end
  end

  defp maybe_put_runtime_head(runtime_info, {:ok, head}),
    do: Map.put(runtime_info, :head, head)

  defp maybe_put_runtime_head(runtime_info, _result), do: runtime_info

  defp run_codex_turns(workspace, issue, codex_update_recipient, opts, worker_host) do
    route_result =
      if Keyword.get(opts, :managed_model_route, false),
        do: ModelRouter.resolve_managed(issue, Keyword.get(opts, :attempt)),
        else: {:ok, ModelRouter.resolve(issue, Keyword.get(opts, :attempt))}

    case route_result do
      {:ok, route} -> start_codex_turns(workspace, issue, codex_update_recipient, opts, worker_host, route)
      {:error, _reason} = error -> error
    end
  end

  defp start_codex_turns(workspace, issue, codex_update_recipient, opts, worker_host, route) do
    max_turns = Keyword.get(opts, :max_turns, Config.settings!().agent.max_turns)
    issue_state_fetcher = Keyword.get(opts, :issue_state_fetcher, &Tracker.fetch_issues_by_ids/1)

    Logger.info(
      "Codex model route selected for #{issue_context(issue)} model=#{route.model} tier=#{route.tier} effort=#{route.effort} attempt=#{route.attempt} escalated=#{route.escalated} reason=#{inspect(route.reason)}"
    )

    session_opts = [
      worker_host: worker_host,
      model_route: route,
      execution_fence_guard: Keyword.get(opts, :execution_fence_guard),
      execution_checkout: Keyword.get(opts, :execution_checkout),
      execution_supervisor: Keyword.get(opts, :execution_supervisor),
      execution_supervisor_recorder: Keyword.get(opts, :execution_supervisor_recorder),
      secret_environment_names: Keyword.get(opts, :secret_environment_names, [])
    ]

    case AppServer.start_session(workspace, session_opts) do
      {:ok, session} ->
        try do
          do_run_codex_turns(session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, 1, max_turns)
        after
          AppServer.stop_session(session)
        end

      {:error, _reason} = error ->
        error
    end
  end

  defp do_run_codex_turns(app_session, workspace, issue, codex_update_recipient, opts, issue_state_fetcher, turn_number, max_turns) do
    prompt = build_turn_prompt(issue, opts, turn_number, max_turns, app_session.model_route)

    with {:ok, turn_session} <-
           AppServer.run_turn(
             app_session,
             prompt,
             issue,
             on_message: codex_message_handler(codex_update_recipient, issue, opts),
             execution_fence_guard: Keyword.get(opts, :execution_fence_guard)
           ) do
      Logger.info("Completed agent run for #{issue_context(issue)} session_id=#{turn_session[:session_id]} workspace=#{workspace} turn=#{turn_number}/#{max_turns}")

      case continue_with_issue?(issue, issue_state_fetcher) do
        {:continue, refreshed_issue} when turn_number < max_turns ->
          Logger.info("Continuing agent run for #{issue_context(refreshed_issue)} after normal turn completion turn=#{turn_number}/#{max_turns}")

          do_run_codex_turns(
            app_session,
            workspace,
            refreshed_issue,
            codex_update_recipient,
            opts,
            issue_state_fetcher,
            turn_number + 1,
            max_turns
          )

        {:continue, refreshed_issue} ->
          Logger.info("Reached agent.max_turns for #{issue_context(refreshed_issue)} with issue still active; returning control to orchestrator")

          :ok

        {:done, _refreshed_issue} ->
          :ok

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp build_turn_prompt(issue, opts, 1, _max_turns, route) do
    """
    Codex routing evidence: model=#{route.model}; tier=#{route.tier}; effort=#{route.effort}; attempt=#{route.attempt}; escalated=#{route.escalated}; reason=#{route.reason}.
    Include this routing evidence in the Linear implementation handoff comment.

    #{PromptBuilder.build_prompt(issue, opts)}

    #{managed_checkout_guidance(opts)}
    """
  end

  defp build_turn_prompt(_issue, opts, turn_number, max_turns, _route) do
    """
    Continuation guidance:

    - The previous Codex turn completed normally, but the tracker work item is still in an active state.
    - This is continuation turn ##{turn_number} of #{max_turns} for the current agent run.
    - Resume from the current workspace and workpad state instead of restarting from scratch.
    - The original task instructions and prior turn context are already present in this thread, so do not restate them before acting.
    - Focus on the remaining ticket work and do not end the turn while the issue stays active unless you are truly blocked.

    #{managed_checkout_guidance(opts)}
    """
  end

  defp managed_checkout_guidance(opts) do
    case Keyword.get(opts, :execution_checkout) do
      nil ->
        ""

      identity ->
        """
        Managed checkout authority for this execution:
        The runtime prepared and verified repository #{identity.repository}, workspace #{identity.worktree},
        branch #{identity.branch}, issue #{identity.issue_id}, generation #{identity.generation}.
        Work only in that workspace and branch. Branch creation, switching and retry workspace replacement
        belong to the runtime; generic workflow instructions to create or change a branch do not apply.
        Preserve .git/symphony-execution.json. If checkout identity disagrees, stop and report the mismatch.
        Commit and push useful changes on the prepared branch. A closed or merged PR requires runtime
        reconciliation; do not create a replacement branch or repair another generation in place.
        """
    end
  end

  defp continue_with_issue?(%Issue{id: issue_id} = issue, issue_state_fetcher) when is_binary(issue_id) do
    case issue_state_fetcher.([issue_id]) do
      {:ok, [%Issue{} = refreshed_issue | _]} ->
        if active_issue_state?(refreshed_issue.state) and issue_routable?(refreshed_issue) do
          {:continue, refreshed_issue}
        else
          {:done, refreshed_issue}
        end

      {:ok, []} ->
        {:done, issue}

      {:error, reason} ->
        {:error, {:issue_state_refresh_failed, reason}}
    end
  end

  defp continue_with_issue?(issue, _issue_state_fetcher), do: {:done, issue}

  defp active_issue_state?(state_name) when is_binary(state_name) do
    normalized_state = normalize_issue_state(state_name)

    Config.settings!().tracker.active_states
    |> Enum.any?(fn active_state -> normalize_issue_state(active_state) == normalized_state end)
  end

  defp active_issue_state?(_state_name), do: false

  defp issue_routable?(%Issue{} = issue) do
    Issue.routable?(issue, Config.settings!().tracker.required_labels)
  end

  defp selected_worker_host(nil, []), do: nil

  defp selected_worker_host(preferred_host, configured_hosts) when is_list(configured_hosts) do
    hosts =
      configured_hosts
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()

    case preferred_host do
      host when is_binary(host) and host != "" -> host
      _ when hosts == [] -> nil
      _ -> List.first(hosts)
    end
  end

  defp worker_host_for_log(nil), do: "local"
  defp worker_host_for_log(worker_host), do: worker_host

  defp normalize_issue_state(state_name) when is_binary(state_name) do
    state_name
    |> String.trim()
    |> String.downcase()
  end

  defp issue_context(%Issue{id: issue_id, identifier: identifier}) do
    "issue_id=#{issue_id} issue_identifier=#{identifier}"
  end

  defp run_after_run_hook_if_authorized(workspace, issue, worker_host, opts) do
    case execution_fence_preflight(opts) do
      :ok ->
        Workspace.run_after_run_hook(workspace, issue, worker_host)

      {:error, reason} ->
        Logger.warning("Skipping after-run hook after execution fence rejection for #{issue_context(issue)}: #{inspect(reason)}")
        :ok
    end
  end
end
