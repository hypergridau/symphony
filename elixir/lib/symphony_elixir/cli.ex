defmodule SymphonyElixir.CLI do
  @moduledoc """
  Escript entrypoint for running Symphony with an explicit WORKFLOW.md path.
  """

  alias SymphonyElixir.{LogFile, ResponsibilityBootstrap}
  alias SymphonyElixir.UnsubmittedSuccessorRetirement
  alias SymphonyElixir.Worker.CLI, as: WorkerCLI

  @acknowledgement_switch :i_understand_that_this_will_be_running_without_the_usual_guardrails
  @activation_switch :activate_responsibility_graph
  @switches [
    {@acknowledgement_switch, :boolean},
    {@activation_switch, :boolean},
    logs_root: :string,
    port: :integer
  ]

  @type ensure_started_result :: {:ok, [atom()]} | {:error, term()}
  @type deps :: %{
          required(:file_regular?) => (String.t() -> boolean()),
          required(:set_workflow_file_path) => (String.t() -> :ok | {:error, term()}),
          required(:set_logs_root) => (String.t() -> :ok | {:error, term()}),
          required(:set_server_port_override) => (non_neg_integer() | nil -> :ok | {:error, term()}),
          required(:ensure_all_started) => (-> ensure_started_result()),
          optional(:activate_responsibility_graph) => (non_neg_integer() -> :ok | {:error, term()})
        }

  @spec main([String.t()]) :: no_return()
  def main(args) do
    if "--retire-unsubmitted-successor" in args do
      dispatch_or_start(args)
    else
      dispatch_regular(args)
    end
  end

  defp dispatch_regular(args) do
    case args do
      ["--assignment-json" | _worker_args] ->
        WorkerCLI.main(args)

      ["--verify-auth-cache"] ->
        WorkerCLI.main(args)

      _ ->
        main(args, fn -> Application.ensure_all_started(:symphony_elixir) end)
    end
  end

  @doc false
  @spec dispatch_unsubmitted_successor_retirement(
          [String.t()],
          (-> ensure_started_result()),
          (String.t(), String.t() -> {:ok, :retired | :already_retired} | {:error, term()})
        ) ::
          {:retirement, {:ok, :retired | :already_retired} | {:error, String.t()}}
          | {:normal, (-> ensure_started_result())}
  def dispatch_unsubmitted_successor_retirement(args, ensure_all_started, execute)
      when is_list(args) and is_function(ensure_all_started, 0) and is_function(execute, 2) do
    if "--retire-unsubmitted-successor" in args do
      {:retirement, evaluate_unsubmitted_successor_retirement(args, execute)}
    else
      {:normal, ensure_all_started}
    end
  end

  defp dispatch_or_start(args) do
    case dispatch_unsubmitted_successor_retirement(
           args,
           fn -> Application.ensure_all_started(:symphony_elixir) end,
           &UnsubmittedSuccessorRetirement.execute/2
         ) do
      {:retirement, {:ok, result}} ->
        IO.puts("unsubmitted successor retirement #{result}")
        System.halt(0)

      {:retirement, {:error, reason}} ->
        IO.puts(:stderr, "unsubmitted successor retirement held closed: #{reason}")
        System.halt(1)

      {:normal, ensure_all_started} ->
        main(args, ensure_all_started)
    end
  end

  @doc false
  @spec evaluate_unsubmitted_successor_retirement(
          [String.t()],
          (String.t(), String.t() -> {:ok, :retired | :already_retired} | {:error, term()})
        ) :: {:ok, :retired | :already_retired} | {:error, String.t()}
  def evaluate_unsubmitted_successor_retirement(
        ["--retire-unsubmitted-successor", "--workflow", workflow_path, identifier],
        execute
      )
      when is_function(execute, 2) do
    case execute.(identifier, workflow_path) do
      {:ok, result} when result in [:retired, :already_retired] -> {:ok, result}
      {:error, reason} -> {:error, safe_retirement_reason(reason)}
      _ -> {:error, retirement_usage_message()}
    end
  end

  def evaluate_unsubmitted_successor_retirement(_args, _execute),
    do: {:error, retirement_usage_message()}

  defp retirement_usage_message do
    "Usage: symphony --retire-unsubmitted-successor --workflow <trusted-absolute-path> <issue-identifier>"
  end

  defp safe_retirement_reason(reason) when is_atom(reason), do: Atom.to_string(reason)
  defp safe_retirement_reason({reason, _detail}) when is_atom(reason), do: Atom.to_string(reason)
  defp safe_retirement_reason(_reason), do: "invalid_authority_or_evidence"

  @doc false
  @spec main([String.t()], (-> ensure_started_result())) :: no_return()
  def main(args, ensure_all_started) do
    case evaluate(args, runtime_deps(ensure_all_started)) do
      :ok ->
        wait_for_shutdown()

      {:error, message} ->
        IO.puts(:stderr, message)
        System.halt(1)
    end
  end

  @spec evaluate([String.t()], deps()) :: :ok | {:error, String.t()}
  def evaluate(args, deps \\ runtime_deps()) do
    case OptionParser.parse(args, strict: @switches) do
      {opts, [], []} ->
        with :ok <- require_guardrails_acknowledgement(opts),
             :ok <- maybe_set_logs_root(opts, deps),
             :ok <- maybe_set_server_port(opts, deps) do
          run(Path.expand("WORKFLOW.md"), deps, opts)
        end

      {opts, [workflow_path], []} ->
        with :ok <- require_guardrails_acknowledgement(opts),
             :ok <- maybe_set_logs_root(opts, deps),
             :ok <- maybe_set_server_port(opts, deps) do
          run(workflow_path, deps, opts)
        end

      _ ->
        {:error, usage_message()}
    end
  end

  @spec run(String.t(), deps()) :: :ok | {:error, String.t()}
  def run(workflow_path, deps), do: run(workflow_path, deps, [])

  @spec run(String.t(), deps(), keyword()) :: :ok | {:error, String.t()}
  def run(workflow_path, deps, opts) do
    expanded_path = Path.expand(workflow_path)

    if deps.file_regular?.(expanded_path) do
      :ok = deps.set_workflow_file_path.(expanded_path)

      case deps.ensure_all_started.() do
        {:ok, _started_apps} ->
          maybe_activate_responsibility_graph(opts, deps)

        {:error, reason} ->
          {:error, "Failed to start Symphony with workflow #{expanded_path}: #{inspect(reason)}"}
      end
    else
      {:error, "Workflow file not found: #{expanded_path}"}
    end
  end

  @spec usage_message() :: String.t()
  defp usage_message do
    "Usage: symphony [--logs-root <path>] [--port <port>] [--activate-responsibility-graph] [path-to-WORKFLOW.md]"
  end

  @spec runtime_deps() :: deps()
  defp runtime_deps(ensure_all_started \\ fn -> Application.ensure_all_started(:symphony_elixir) end) do
    %{
      file_regular?: &File.regular?/1,
      set_workflow_file_path: &SymphonyElixir.Workflow.set_workflow_file_path/1,
      set_logs_root: &set_logs_root/1,
      set_server_port_override: &set_server_port_override/1,
      activate_responsibility_graph: &ResponsibilityBootstrap.activate/1,
      ensure_all_started: ensure_all_started
    }
  end

  defp maybe_activate_responsibility_graph(opts, deps) do
    case {Keyword.get(opts, @activation_switch, false), Map.get(deps, :activate_responsibility_graph)} do
      {false, _callback} ->
        :ok

      {true, callback} when is_function(callback, 1) ->
        case callback.(System.system_time(:millisecond)) do
          :ok ->
            :ok

          {:error, reason} ->
            {:error, "Failed to activate responsibility graph: #{inspect(reason)}"}

          result ->
            {:error, "Failed to activate responsibility graph: #{inspect(result)}"}
        end

      {true, _callback} ->
        {:error, "Responsibility graph activation is unavailable"}
    end
  end

  defp maybe_set_logs_root(opts, deps) do
    case Keyword.get_values(opts, :logs_root) do
      [] ->
        :ok

      values ->
        logs_root = values |> List.last() |> String.trim()

        if logs_root == "" do
          {:error, usage_message()}
        else
          :ok = deps.set_logs_root.(Path.expand(logs_root))
        end
    end
  end

  defp require_guardrails_acknowledgement(opts) do
    if Keyword.get(opts, @acknowledgement_switch, false) do
      :ok
    else
      {:error, acknowledgement_banner()}
    end
  end

  @spec acknowledgement_banner() :: String.t()
  defp acknowledgement_banner do
    lines = [
      "This Symphony implementation is a low key engineering preview.",
      "Codex will run without any guardrails.",
      "SymphonyElixir is not a supported product and is presented as-is.",
      "To proceed, start with `--i-understand-that-this-will-be-running-without-the-usual-guardrails` CLI argument"
    ]

    width = Enum.max(Enum.map(lines, &String.length/1))
    border = String.duplicate("─", width + 2)
    top = "╭" <> border <> "╮"
    bottom = "╰" <> border <> "╯"
    spacer = "│ " <> String.duplicate(" ", width) <> " │"

    content =
      [
        top,
        spacer
        | Enum.map(lines, fn line ->
            "│ " <> String.pad_trailing(line, width) <> " │"
          end)
      ] ++ [spacer, bottom]

    [
      IO.ANSI.red(),
      IO.ANSI.bright(),
      Enum.join(content, "\n"),
      IO.ANSI.reset()
    ]
    |> IO.iodata_to_binary()
  end

  defp set_logs_root(logs_root) do
    Application.put_env(:symphony_elixir, :log_file, LogFile.default_log_file(logs_root))
    :ok
  end

  defp maybe_set_server_port(opts, deps) do
    case Keyword.get_values(opts, :port) do
      [] ->
        :ok

      values ->
        port = List.last(values)

        if is_integer(port) and port >= 0 do
          :ok = deps.set_server_port_override.(port)
        else
          {:error, usage_message()}
        end
    end
  end

  defp set_server_port_override(port) when is_integer(port) and port >= 0 do
    Application.put_env(:symphony_elixir, :server_port_override, port)
    :ok
  end

  @spec wait_for_shutdown() :: no_return()
  defp wait_for_shutdown do
    case Process.whereis(SymphonyElixir.Supervisor) do
      nil ->
        IO.puts(:stderr, "Symphony supervisor is not running")
        System.halt(1)

      pid ->
        ref = Process.monitor(pid)

        receive do
          {:DOWN, ^ref, :process, ^pid, reason} ->
            case reason do
              :normal -> System.halt(0)
              _ -> System.halt(1)
            end
        end
    end
  end
end
