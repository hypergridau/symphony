defmodule SymphonyElixir.Codex.ModelRouter do
  @moduledoc """
  Selects the least-capable Codex model admitted for a Linear issue and retry.

  The legacy retry ladder is Luna high -> Luna xhigh -> Luna max -> Sol xhigh.
  An explicit `model:gpt-6-luna` label selects a separate GPT-6 Luna-only ladder.
  Failed worker attempts escalate one rung at a time and never wrap around the
  selected ladder.
  """

  alias SymphonyElixir.Tracker.Issue

  @legacy_ladder [
    %{tier: "luna-high", model: "gpt-5.6-luna", effort: "high"},
    %{tier: "luna-xhigh", model: "gpt-5.6-luna", effort: "xhigh"},
    %{tier: "luna-max", model: "gpt-5.6-luna", effort: "max"},
    %{tier: "sol-xhigh", model: "gpt-5.6-sol", effort: "xhigh"}
  ]
  @gpt6_luna_ladder [
    %{tier: "gpt6-luna-high", model: "gpt-6-luna", effort: "high"},
    %{tier: "gpt6-luna-xhigh", model: "gpt-6-luna", effort: "xhigh"},
    %{tier: "gpt6-luna-max", model: "gpt-6-luna", effort: "max"}
  ]
  @explicit_labels %{
    "model:luna" => "luna-high",
    "model:luna-high" => "luna-high",
    "model:luna-xhigh" => "luna-xhigh",
    "model:luna-max" => "luna-max",
    "model:terra" => "luna-xhigh",
    "model:sol" => "sol-xhigh",
    "model:sol-xhigh" => "sol-xhigh"
  }
  @retired_labels MapSet.new(["model:spark"])
  @luna_xhigh_labels MapSet.new(["architecture", "security", "high-consequence", "cross-repository", "production"])

  @spec resolve(Issue.t(), non_neg_integer() | nil) :: map()
  def resolve(%Issue{labels: labels}, attempt) do
    normalized_labels = labels |> List.wrap() |> Enum.map(&normalize_label/1) |> MapSet.new()
    gpt6_luna? = MapSet.member?(normalized_labels, "model:gpt-6-luna")
    ladder = if gpt6_luna?, do: @gpt6_luna_ladder, else: @legacy_ladder

    {base_tier, reason} =
      if gpt6_luna?,
        do: {"gpt6-luna-high", "explicit model:gpt-6-luna label"},
        else: base_route(normalized_labels)

    base_index = Enum.find_index(ladder, &(&1.tier == base_tier)) || 0
    retry_count = if is_integer(attempt) and attempt > 0, do: attempt, else: 0
    selected_index = min(base_index + retry_count, length(ladder) - 1)
    route = Enum.at(ladder, selected_index)

    Map.merge(route, %{
      base_tier: base_tier,
      attempt: retry_count,
      escalated: selected_index > base_index,
      reason: if(selected_index > base_index, do: "#{reason}; escalated after worker attempt #{retry_count}", else: reason)
    })
  end

  defp base_route(labels) do
    explicit = Enum.find(@explicit_labels, fn {label, _tier} -> MapSet.member?(labels, label) end)

    cond do
      explicit ->
        {elem(explicit, 1), "explicit #{elem(explicit, 0)} label"}

      Enum.any?(@retired_labels, &MapSet.member?(labels, &1)) ->
        {"luna-high", "retired model:spark label fell back to luna-high"}

      Enum.any?(@luna_xhigh_labels, &MapSet.member?(labels, &1)) ->
        {"luna-xhigh", "high-complexity Linear label routed to luna-xhigh"}

      true ->
        {"luna-high", "default cost-sensitive coding route"}
    end
  end

  defp normalize_label(label) when is_binary(label), do: label |> String.trim() |> String.downcase()
  defp normalize_label(label), do: label |> to_string() |> normalize_label()
end
