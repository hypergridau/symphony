defmodule SymphonyElixir.Worker.Command do
  @moduledoc "Runs a fixed argv with EOF on stdin; no caller input is appended to a Codex prompt."

  @spec run(String.t(), [String.t()], keyword()) :: {term(), non_neg_integer()}
  def run(executable, args, options) do
    {discard_stderr, options} = Keyword.pop(options, :discard_stderr, false)

    shell =
      case discard_stderr do
        false -> "exec \"$@\" </dev/null"
        true -> "exec \"$@\" </dev/null 2>/dev/null"
      end

    System.cmd("/bin/sh", ["-c", shell, "symphony-command", executable | args], options)
  end
end
