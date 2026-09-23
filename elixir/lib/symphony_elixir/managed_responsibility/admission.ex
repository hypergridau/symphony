defmodule SymphonyElixir.ManagedResponsibility.Admission do
  @moduledoc """
  Applies operator authorization within the existing fenced admission transaction.
  It returns a graph candidate; only the orchestrator persists a bound delegation.
  """

  alias SymphonyElixir.{Config, ExecutionFence, ManagedResponsibility, ResponsibilityGraph}

  alias SymphonyElixir.Codex.ModelRouter
  alias SymphonyElixir.ManagedTokenBudget.Limit
  alias SymphonyElixir.WorkPackageClaim.Unsubmitted

  @efforts ~w(none minimal low medium high xhigh max ultra)

  @spec prepare(map(), map(), map() | nil, map(), non_neg_integer() | nil, non_neg_integer()) ::
          {:ok, map()} | {:error, term()}
  def prepare(graph, fence, manifest, issue, attempt, now_ms) do
    prepare(graph, fence, manifest, issue, attempt, now_ms, nil)
  end

  @spec prepare(map(), map(), map() | nil, map(), non_neg_integer() | nil, non_neg_integer(), map() | nil) ::
          {:ok, map()} | {:error, term()}
  def prepare(graph, _fence, nil, issue, attempt, _now_ms, _runtime) do
    if ModelRouter.resolve(issue, attempt).model == "gpt-6-luna",
      do: {:error, :managed_responsibility_required_for_gpt6_luna},
      else: {:ok, graph}
  end

  def prepare(graph, fence, manifest, issue, attempt, now_ms, runtime) do
    with true <- ResponsibilityGraph.enforced?(graph),
         :ok <- ExecutionFence.validate(fence),
         :ok <- prior_repository_cleanup(fence, graph, manifest.repository_ref, issue.id, runtime, now_ms),
         {:ok, next_graph} <- ManagedResponsibility.admit(graph, manifest, issue, now_ms, %{runtime: runtime, fence: fence}),
         {:ok, delegation} <- ResponsibilityGraph.admission_delegation(next_graph, issue.id, issue.identifier, manifest.repository_ref),
         :ok <- matching_budget_modes(next_graph, delegation),
         :ok <- route_budget(delegation.budget, ModelRouter.resolve(issue, attempt)) do
      {:ok, next_graph}
    else
      false -> {:error, :managed_responsibility_requires_enforcement}
      {:error, _reason} = error -> error
    end
  end

  defp prior_repository_cleanup(%{executions: executions} = fence, graph, repository, issue_id, runtime, now_ms) do
    held =
      Enum.any?(executions, fn {other_id, execution} ->
        other_id != issue_id and execution.repository == repository and
          not (cleaned_execution?(fence, other_id, execution) or
                 Unsubmitted.released_without_workspace?(runtime, fence, graph, execution, now_ms))
      end)

    if held, do: {:error, :previous_repository_cleanup_required}, else: :ok
  end

  defp cleaned_execution?(fence, issue_id, %{cleanup: :cleaned, status: :terminal, terminal: %{accepted_head: head}} = execution)
       when is_binary(head) do
    ExecutionFence.validate_cleanup(fence, %{issue_id: issue_id, generation: execution.generation}, head) == :ok
  end

  defp cleaned_execution?(_fence, _issue_id, _execution), do: false

  defp route_budget(budget, route) do
    configured_limit = Config.settings!().codex.max_total_tokens
    selected_rank = Enum.find_index(@efforts, &(&1 == route.effort))
    maximum_rank = Enum.find_index(@efforts, &(&1 == Atom.to_string(budget.effort)))

    if route.model == budget.model and is_integer(selected_rank) and is_integer(maximum_rank) and
         selected_rank <= maximum_rank and match?({:ok, _}, Limit.bounded(configured_limit, %{budget: budget})) do
      :ok
    else
      {:error, :managed_responsibility_budget_exceeded}
    end
  end

  defp matching_budget_modes(_graph, %{parent_delegation_id: nil}), do: :ok

  defp matching_budget_modes(graph, %{parent_delegation_id: parent_id, budget: budget}) do
    with %{budget: parent_budget} <- Map.get(graph.delegations, parent_id),
         {:ok, parent_mode} <- Limit.budget_mode(parent_budget),
         {:ok, child_mode} <- Limit.budget_mode(budget),
         true <- parent_mode == child_mode do
      :ok
    else
      _ -> {:error, :managed_responsibility_budget_mode_mismatch}
    end
  end
end
