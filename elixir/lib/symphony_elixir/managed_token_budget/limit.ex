defmodule SymphonyElixir.ManagedTokenBudget.Limit do
  @moduledoc "Resolves the local ceiling and one explicit managed grant."

  @error {:error, :managed_token_budget_unavailable_or_exhausted}
  @modes [:finite, :progress_scoped]
  @progress_models ["gpt-5.6-luna", "gpt-6-luna"]
  @type error :: {:error, :managed_token_budget_unavailable_or_exhausted}
  @type result :: {:ok, non_neg_integer() | :unbounded, map() | nil} | error()
  @type mode :: :finite | :progress_scoped

  @spec bounded(term(), term()) :: {:ok, pos_integer() | :unbounded} | error()
  def bounded(configured, %{budget: budget}) when is_map(budget) do
    with true <- is_integer(configured) and configured > 0,
         {:ok, mode} <- budget_mode(budget),
         {:ok, maximum} <- maximum_for_mode(mode, budget),
         :ok <- valid_progress_model(mode, budget) do
      {:ok, if(mode == :progress_scoped, do: :unbounded, else: min(configured, maximum))}
    else
      _ -> @error
    end
  end

  def bounded(_, _), do: @error

  @doc "Returns the typed mode of a managed budget; legacy maps remain finite."
  @spec budget_mode(term()) :: {:ok, mode()} | error()
  def budget_mode(budget) when is_map(budget) do
    case Map.fetch(budget, :mode) do
      :error -> {:ok, :finite}
      {:ok, value} when value in @modes -> {:ok, value}
      _ -> @error
    end
  end

  def budget_mode(_budget), do: @error

  @spec resolve(term(), term(), term()) :: result()
  def resolve(configured, nil, _issue_id) when is_integer(configured) and configured >= 0,
    do: {:ok, configured, nil}

  def resolve(configured, %{managed_delegations: %{entries: entries}}, issue_id)
      when is_integer(configured) and configured > 0 and is_list(entries) do
    with true <- is_binary(issue_id) and byte_size(issue_id) > 0,
         {:ok, matches} <- validate_entries(entries, issue_id),
         {:ok, responsible} <- exactly_one(matches),
         {:ok, limit} <- bounded(configured, responsible) do
      {:ok, limit, responsible}
    else
      _ -> @error
    end
  end

  def resolve(_, _, _), do: @error

  defp validate_entries(entries, requested) do
    Enum.reduce_while(entries, {:ok, MapSet.new(), MapSet.new(), []}, &collect_entry(&1, &2, requested))
    |> case do
      {:ok, _seen, _grants, matches} -> {:ok, matches}
      error -> error
    end
  end

  defp collect_entry(entry, {:ok, seen, grants, matches}, requested) do
    with {:ok, id, grant} <- valid_entry(entry),
         false <- MapSet.member?(seen, id) or MapSet.member?(grants, grant.id) do
      found = if id == requested, do: [grant | matches], else: matches
      {:cont, {:ok, MapSet.put(seen, id), MapSet.put(grants, grant.id), found}}
    else
      _ -> {:halt, @error}
    end
  end

  defp valid_entry(%{issue_id: issue_id, responsible: %{id: id, budget: budget} = grant})
       when is_binary(issue_id) and byte_size(issue_id) > 0 and is_binary(id) and byte_size(id) > 0 and
              is_map(budget) do
    with {:ok, mode} <- budget_mode(budget),
         {:ok, _} <- maximum_for_mode(mode, budget),
         :ok <- valid_progress_model(mode, budget) do
      {:ok, issue_id, grant}
    else
      _ -> :error
    end
  end

  defp valid_entry(_), do: :error
  defp exactly_one([grant]), do: {:ok, grant}
  defp exactly_one(_), do: @error

  defp positive_maximum(%{max_tokens: maximum}) when is_integer(maximum) and maximum > 0,
    do: {:ok, maximum}

  defp positive_maximum(_budget), do: @error

  defp maximum_for_mode(:progress_scoped, %{max_tokens: nil}), do: {:ok, :unbounded}
  defp maximum_for_mode(:progress_scoped, _budget), do: @error
  defp maximum_for_mode(:finite, budget), do: positive_maximum(budget)

  defp valid_progress_model(:progress_scoped, %{model: model}) when model in @progress_models, do: :ok
  defp valid_progress_model(:progress_scoped, _budget), do: @error
  defp valid_progress_model(:finite, _budget), do: :ok
end
