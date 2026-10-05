defmodule SymphonyElixir.Worker.Command do
  @moduledoc "Runs a fixed argv with EOF on stdin; no caller input is appended to a Codex prompt."

  @input_limit 524_288
  @input_output_limit 8_192
  @input_timeout 5_000
  @timeout_cleanup_margin 300
  @input_chunk_size 16_384

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
    timeout_ms = Keyword.get(options, :timeout_ms, @input_timeout)

    if is_integer(timeout_ms) and timeout_ms > @timeout_cleanup_margin and timeout_ms <= @input_timeout do
      deadline = monotonic_ms() + timeout_ms
      run_in_port_owner(executable, args, input, options, timeout_ms, deadline)
    else
      {:error, :command_failed}
    end
  rescue
    _ -> {:error, :command_failed}
  end

  def run_with_input(_executable, _args, _input, _options), do: {:error, :input_limit}

  defp run_in_port_owner(executable, args, input, options, timeout_ms, deadline) do
    parent = self()
    reference = make_ref()

    {pid, monitor} =
      spawn_monitor(fn ->
        Process.flag(:trap_exit, true)

        result = run_input_port(executable, args, input, options, timeout_ms, deadline)
        send(parent, {reference, result})
      end)

    receive do
      {^reference, result} ->
        Process.demonitor(monitor, [:flush])
        result

      {:DOWN, ^monitor, :process, ^pid, _reason} ->
        {:error, :command_failed}
    after
      remaining_ms(deadline) ->
        Process.exit(pid, :kill)
        Process.demonitor(monitor, [:flush])
        {:error, :command_timeout}
    end
  end

  defp run_input_port(executable, args, input, options, timeout_ms, deadline) do
    kill_after_ms = min(250, div(timeout_ms, 4))
    command_timeout_ms = timeout_ms - kill_after_ms - 50
    size = byte_size(input)
    script = "/usr/bin/head -c #{size} | exec \"$@\""

    port_options = [
      :binary,
      :exit_status,
      :use_stdio,
      :stderr_to_stdout,
      {:args,
       [
         "--signal=TERM",
         "--kill-after=#{duration(kill_after_ms)}",
         duration(command_timeout_ms),
         "/bin/sh",
         "-c",
         script,
         "symphony-validator",
         executable | args
       ]}
    ]

    port_options =
      case Keyword.get(options, :env) do
        nil -> port_options
        env -> [{:env, env} | port_options]
      end

    port = Port.open({:spawn_executable, "/usr/bin/timeout"}, port_options)

    send_input(
      port,
      input,
      0,
      <<>>,
      false,
      Keyword.get(options, :output_limit, @input_output_limit),
      deadline
    )
  end

  defp send_input(port, input, offset, output, overflow?, limit, deadline) do
    cond do
      remaining_ms(deadline) == 0 ->
        Port.close(port)
        {:error, :command_timeout}

      overflow? ->
        await_exit(port, output, true, offset, byte_size(input), limit, deadline)

      offset >= byte_size(input) ->
        await_exit(port, output, false, offset, byte_size(input), limit, deadline)

      true ->
        chunk_size = min(@input_chunk_size, byte_size(input) - offset)
        chunk = binary_part(input, offset, chunk_size)

        case try_port_command(port, chunk) do
          :sent -> send_input(port, input, offset + chunk_size, output, false, limit, deadline)
          :busy -> await_input_space(port, input, offset, output, false, limit, deadline)
          :closed -> await_exit(port, output, false, offset, byte_size(input), limit, deadline)
        end
    end
  end

  defp await_input_space(port, input, offset, output, overflow?, limit, deadline) do
    receive do
      {^port, {:data, data}} ->
        {next_output, next_overflow?} = append_bounded(output, data, overflow?, limit)
        send_input(port, input, offset, next_output, next_overflow?, limit, deadline)

      {^port, {:exit_status, status}} ->
        command_result(output, overflow?, status, offset, byte_size(input))

      {:EXIT, ^port, _reason} ->
        await_exit(port, output, overflow?, offset, byte_size(input), limit, deadline)

      {^port, :closed} ->
        {:error, :command_closed}
    after
      min(remaining_ms(deadline), 10) ->
        send_input(port, input, offset, output, overflow?, limit, deadline)
    end
  end

  defp await_exit(port, output, overflow?, offset, input_size, limit, deadline) do
    receive do
      {^port, {:data, data}} ->
        {next_output, next_overflow?} = append_bounded(output, data, overflow?, limit)
        await_exit(port, next_output, next_overflow?, offset, input_size, limit, deadline)

      {^port, {:exit_status, status}} ->
        command_result(output, overflow?, status, offset, input_size)

      {:EXIT, ^port, _reason} ->
        await_exit(port, output, overflow?, offset, input_size, limit, deadline)

      {^port, :closed} ->
        {:error, :command_closed}
    after
      remaining_ms(deadline) ->
        Port.close(port)
        {:error, :command_timeout}
    end
  end

  defp append_bounded(output, data, overflow?, limit) do
    available = max(limit - byte_size(output), 0)
    retained = min(available, byte_size(data))
    next_output = if retained == 0, do: output, else: output <> binary_part(data, 0, retained)
    {next_output, overflow? or retained < byte_size(data)}
  end

  defp try_port_command(port, chunk) do
    if Port.command(port, chunk, [:nosuspend]), do: :sent, else: :busy
  rescue
    _ -> :closed
  catch
    :exit, _reason -> :closed
  end

  defp command_result(_output, true, _status, _offset, _input_size), do: {:error, :output_limit}

  defp command_result(_output, _overflow?, status, _offset, _input_size) when status in [124, 137],
    do: {:error, :command_timeout}

  defp command_result(_output, _overflow?, _status, offset, input_size) when offset < input_size,
    do: {:error, :input_write_failed}

  defp command_result(output, false, status, _offset, _input_size), do: {output, status}

  defp remaining_ms(deadline), do: max(deadline - monotonic_ms(), 0)
  defp monotonic_ms, do: System.monotonic_time(:millisecond)

  defp duration(milliseconds) do
    seconds = milliseconds / 1_000
    :erlang.float_to_binary(seconds, decimals: 3) <> "s"
  end
end
