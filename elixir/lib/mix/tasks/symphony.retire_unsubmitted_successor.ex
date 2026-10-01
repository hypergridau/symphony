defmodule Mix.Tasks.Symphony.RetireUnsubmittedSuccessor do
  use Mix.Task

  @moduledoc "Runs the bounded unsubmitted-successor retirement using Mix runtime configuration."

  alias SymphonyElixir.UnsubmittedSuccessorRetirement

  @shortdoc "Retires one owner-authorized never-submitted delegation predecessor"

  @impl Mix.Task
  @spec run([String.t()]) :: :ok | no_return()
  def run(args) do
    case OptionParser.parse(args, strict: [workflow: :string]) do
      {opts, [identifier], []} ->
        case UnsubmittedSuccessorRetirement.execute(identifier, Keyword.get(opts, :workflow)) do
          {:ok, result} ->
            Mix.shell().info("unsubmitted successor retirement #{result} identifier=#{identifier}")
            :ok

          {:error, reason} ->
            Mix.raise("unsubmitted successor retirement held closed: #{safe_reason(reason)}")
        end

      _ ->
        Mix.raise(
          "usage: mix symphony.retire_unsubmitted_successor " <>
            "--workflow <trusted-absolute-path> <issue-identifier>"
        )
    end
  end

  defp safe_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp safe_reason({reason, _detail}) when is_atom(reason), do: Atom.to_string(reason)
  defp safe_reason(_reason), do: "invalid_authority_or_evidence"
end
