defmodule SymphonyElixir.Linear.Adapter do
  @moduledoc """
  Linear-backed tracker adapter.
  """

  @behaviour SymphonyElixir.Tracker

  alias SymphonyElixir.Linear.{AgentTool, Client}
  alias SymphonyElixir.Tracker.Issue

  @spec validate_config(map()) :: :ok | {:error, term()}
  def validate_config(tracker_settings) do
    cond do
      not present_string?(tracker_settings.endpoint) ->
        {:error, :invalid_linear_endpoint}

      not present_string?(tracker_settings.api_key) ->
        {:error, :missing_linear_api_token}

      not present_string?(tracker_settings.project_slug) ->
        {:error, :missing_linear_project_slug}

      not valid_project_slugs?(tracker_settings) ->
        {:error, :invalid_linear_project_slugs}

      not is_nil(tracker_settings.assignee) and not present_string?(tracker_settings.assignee) ->
        {:error, :invalid_linear_assignee}

      true ->
        :ok
    end
  end

  defp valid_project_slugs?(tracker_settings) do
    provider = tracker_settings.provider

    case if(is_map(provider), do: Map.get(provider, "project_slugs"), else: :invalid) do
      nil ->
        true

      slugs when is_list(slugs) ->
        slugs == ["224f14ff3a24", "frigga-712f8a45bf36"] and
          tracker_settings.project_slug == "224f14ff3a24"

      _ ->
        false
    end
  end

  @spec fetch_issues_by_states([String.t()]) :: {:ok, [Issue.t()]} | {:error, term()}
  def fetch_issues_by_states(states), do: client_module().fetch_issues_by_states(states)

  @spec fetch_issues_by_ids([String.t()]) :: {:ok, [Issue.t()]} | {:error, term()}
  def fetch_issues_by_ids(issue_ids), do: client_module().fetch_issues_by_ids(issue_ids)

  @spec agent_tool_specs() :: [map()]
  def agent_tool_specs, do: AgentTool.tool_specs()

  @spec execute_agent_tool(String.t(), term(), keyword()) :: map()
  def execute_agent_tool(tool, arguments, opts) do
    AgentTool.execute(tool, arguments, opts)
  end

  @spec secret_environment_names(map()) :: [String.t()]
  def secret_environment_names(tracker_settings), do: tracker_settings.secret_environment_names

  defp client_module do
    Application.get_env(:symphony_elixir, :linear_client_module, Client)
  end

  defp present_string?(value) when is_binary(value), do: String.trim(value) != ""
  defp present_string?(_value), do: false
end
