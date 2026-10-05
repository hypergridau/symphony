defmodule SymphonyElixir.AgentRuntimeSupervisor do
  @moduledoc """
  Supervises the scheduler authority together with its agent tasks.
  """

  use Supervisor
  require Logger
  alias SymphonyElixir.WorkPackageRuntime

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    Supervisor.start_link(__MODULE__, opts, name: name)
  end

  @impl true
  def init(opts) do
    task_supervisor_name =
      Keyword.get(opts, :task_supervisor_name, SymphonyElixir.TaskSupervisor)

    orchestrator_name = Keyword.get(opts, :orchestrator_name, SymphonyElixir.Orchestrator)

    orchestrator_opts = [name: orchestrator_name, task_supervisor: task_supervisor_name]

    orchestrator_opts =
      case WorkPackageRuntime.configuration() do
        :disabled ->
          install_abort_prepare_roots(nil)

          if WorkPackageRuntime.managed_pool?() do
            Logger.error("Managed Symphony pool requires the complete work-package runtime configuration")
            raise ArgumentError, "managed Symphony pool work-package runtime is not configured"
          else
            orchestrator_opts
          end

        {:ok, runtime} ->
          install_abort_prepare_roots(runtime.disposable_rke2_host_config)
          Keyword.merge(orchestrator_opts, execution_supervisor: :systemd_user, work_package_runtime: runtime)

        {:error, reason} ->
          Logger.error("Managed work-package runtime configuration is incomplete: #{inspect(reason)}")
          raise ArgumentError, "invalid managed work-package runtime configuration: #{inspect(reason)}"
      end

    children = [
      Supervisor.child_spec(
        {Task.Supervisor, name: task_supervisor_name},
        id: task_supervisor_name
      ),
      Supervisor.child_spec(
        {SymphonyElixir.Orchestrator, orchestrator_opts},
        id: orchestrator_name
      )
    ]

    Supervisor.init(children, strategy: :one_for_all)
  end

  defp install_abort_prepare_roots(%{abort_journal_root: journal_root, workspace_root: workspace_root, result_journal_root: result_root}) do
    Application.put_env(:symphony_elixir, :abort_prepare_journal_root, journal_root)
    Application.put_env(:symphony_elixir, :abort_prepare_workspace_root, workspace_root)
    Application.put_env(:symphony_elixir, :abort_result_journal_root, result_root)
  end

  defp install_abort_prepare_roots(_) do
    Application.delete_env(:symphony_elixir, :abort_prepare_journal_root)
    Application.delete_env(:symphony_elixir, :abort_prepare_workspace_root)
    Application.delete_env(:symphony_elixir, :abort_result_journal_root)
  end
end
