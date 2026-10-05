defmodule SymphonyElixir.Worker.Command do
  @moduledoc "Runs a fixed argv with EOF on stdin; no caller input is appended to a Codex prompt."

  @input_limit 524_288
  @input_output_limit 8_192
  @input_timeout 5_000

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

  @doc false
  @spec run_with_input(String.t(), [String.t()], binary(), keyword()) ::
          {binary(), non_neg_integer()} | {:error, atom()}
  def run_with_input(executable, args, input, options) when is_binary(input) and byte_size(input) <= @input_limit do
    size = byte_size(input)
    script = "/usr/bin/head -c #{size} | exec \"$@\""

    port_options = [
      :binary,
      :exit_status,
      :use_stdio,
      :stderr_to_stdout,
      {:args, ["-c", script, "symphony-validator", executable | args]}
    ]

    port_options =
      case Keyword.get(options, :env) do
        nil -> port_options
        env -> [{:env, env} | port_options]
      end

    port = Port.open({:spawn_executable, "/bin/sh"}, port_options)
    Port.command(port, input)
    collect_input_command(port, <<>>, Keyword.get(options, :output_limit, @input_output_limit))
  rescue
    _ -> {:error, :command_failed}
  end

  def run_with_input(_executable, _args, _input, _options), do: {:error, :input_limit}

  defp collect_input_command(port, output, limit) do
    receive do
      {^port, {:data, data}} when byte_size(output) + byte_size(data) <= limit ->
        collect_input_command(port, output <> data, limit)

      {^port, {:data, _data}} ->
        Port.close(port)
        {:error, :output_limit}

      {^port, {:exit_status, status}} ->
        {output, status}

      {^port, :closed} ->
        {:error, :command_closed}
    after
      @input_timeout ->
        Port.close(port)
        {:error, :command_timeout}
    end
  end
end
