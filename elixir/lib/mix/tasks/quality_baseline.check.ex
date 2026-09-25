defmodule Mix.Tasks.QualityBaseline.Check do
  use Mix.Task

  @moduledoc """
  Rejects new Credo or Dialyzer findings relative to the pinned, reviewed main baseline.

  Line numbers are excluded from identities so nearby edits do not conceal or create
  findings. Duplicate findings retain their counts, and improvements are allowed.
  """

  @shortdoc "Fail when source quality regresses beyond the pinned baseline"
  @baseline_path "config/quality-baseline.json"
  @source_commit "b2ca242e99c5f8c4fc3e474b298c57fc9004fd7d"

  @impl Mix.Task
  def run(args) do
    kind =
      case args do
        ["--credo"] -> "credo"
        ["--dialyzer"] -> "dialyzer"
        _ -> Mix.raise("usage: mix quality_baseline.check --credo|--dialyzer")
      end

    baseline = load_baseline!()
    current = if kind == "credo", do: credo_findings!(), else: dialyzer_findings!()
    allowed = baseline |> Map.fetch!(kind) |> Map.new(fn %{"identity" => key, "count" => count} -> {key, count} end)

    regressions = regressions(current, allowed)

    case regressions do
      [] ->
        Mix.shell().info("quality baseline #{kind}: no new findings (#{Enum.sum(Map.values(current))} current)")

      _ ->
        Enum.each(regressions, fn {key, count} ->
          Mix.shell().error("#{kind}: #{key} (#{count} current, #{Map.get(allowed, key, 0)} allowed)")
        end)

        Mix.raise("quality baseline #{kind}: #{length(regressions)} new or increased finding(s)")
    end
  end

  @doc "Returns only new or increased warning identities; improved findings are allowed."
  @spec regressions(%{optional(String.t()) => non_neg_integer()}, %{optional(String.t()) => non_neg_integer()}) ::
          [{String.t(), pos_integer()}]
  def regressions(current, allowed) when is_map(current) and is_map(allowed) do
    current
    |> Enum.filter(fn {key, count} -> count > Map.get(allowed, key, 0) end)
    |> Enum.sort()
  end

  defp load_baseline! do
    baseline = @baseline_path |> File.read!() |> Jason.decode!()

    unless baseline["schemaVersion"] == 1 and baseline["sourceCommit"] == @source_commit and
             valid_entries?(baseline["credo"]) and valid_entries?(baseline["dialyzer"]) do
      Mix.raise("quality baseline is malformed or not pinned to the reviewed source commit")
    end

    baseline
  end

  defp valid_entries?(entries) when is_list(entries) do
    Enum.all?(entries, fn
      %{"identity" => key, "count" => count} when is_binary(key) and key != "" and is_integer(count) and count > 0 ->
        true

      _ ->
        false
    end) and length(entries) == length(Enum.uniq_by(entries, & &1["identity"]))
  end

  defp valid_entries?(_entries), do: false

  defp credo_findings! do
    {output, status} = System.cmd("mix", ["credo", "--strict", "--format", "json"], stderr_to_stdout: true)

    unless status in [0, 1, 12, 14] do
      Mix.shell().error(output)
      Mix.raise("Credo failed before producing findings (exit #{status})")
    end

    case output |> String.split("{", parts: 2) do
      [_prefix, json] ->
        case Jason.decode("{" <> json) do
          {:ok, %{"issues" => issues}} when is_list(issues) ->
            Enum.frequencies_by(issues, fn issue ->
              Jason.encode!([issue["filename"], issue["check"], issue["scope"], issue["message"]])
            end)

          _ ->
            Mix.raise("Credo did not produce a valid findings document")
        end

      _ ->
        Mix.raise("Credo did not produce a findings document")
    end
  end

  defp dialyzer_findings! do
    {output, status} = System.cmd("mix", ["dialyzer", "--format", "short"], stderr_to_stdout: true)

    unless status in [0, 2] do
      Mix.raise("Dialyzer failed before producing findings (exit #{status})")
    end

    findings =
      output
      |> String.split("\n")
      |> Enum.flat_map(fn line ->
        case Regex.run(~r/^(.+?\.ex):\d+(?::\d+)?:([a-z_]+) (.+)$/, String.trim(line)) do
          [_, file, category, message] -> [file <> ":" <> category <> " " <> message]
          _ -> []
        end
      end)

    case Regex.run(~r/Total errors: (\d+)/, output) do
      [_, count] ->
        if length(findings) == String.to_integer(count),
          do: Enum.frequencies(findings),
          else: Mix.raise("Dialyzer warning count did not match the reported total")

      _ when status == 0 and findings == [] ->
        %{}

      _ ->
        Mix.raise("Dialyzer output could not be fully accounted for")
    end
  end
end
