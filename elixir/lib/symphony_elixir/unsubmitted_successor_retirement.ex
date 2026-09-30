defmodule SymphonyElixir.UnsubmittedSuccessorRetirement do
  @moduledoc """
  Applies one owner-signed never-submitted retirement while the managed
  mutable-admission gate is paused and the pool service is stopped.
  """

  import Bitwise, only: [band: 2]

  alias SymphonyElixir.{Config, GlobalPause, ManagedLauncherLock, Workflow}
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.WorkPackageClaim.UnsubmittedSuccessor
  alias SymphonyElixir.WorkPackageRuntime

  @spec execute(String.t(), String.t() | nil) :: {:ok, :retired | :already_retired} | {:error, term()}
  def execute(identifier, workflow_path) when is_binary(identifier) and is_binary(workflow_path) do
    with :ok <- require_paused_gate(),
         {:ok, _started} <- Application.ensure_all_started(:crypto),
         {:ok, workflow_sha256} <- trusted_workflow_file(workflow_path),
         :ok <- Workflow.set_workflow_file_path(workflow_path),
         {:ok, runtime} <- WorkPackageRuntime.configuration(),
         runtime <-
           Map.merge(runtime, %{
             workflow_sha256: workflow_sha256,
             execution_fence_path: Config.execution_fence_state_path(),
             responsibility_graph_path: Config.responsibility_graph_state_path()
           }),
         {:ok, entry} <- select_entry(runtime, identifier),
         {:ok, lock_path} <- ManagedLauncherLock.pool_lock_path(runtime.journal_path, runtime.pool_key),
         {:ok, result} <-
           ManagedLauncherLock.with_exclusive_lock(lock_path, fn -> retire_locked(runtime, entry) end) do
      {:ok, result}
    else
      :missing -> {:error, :required_state_missing}
      :disabled -> {:error, :managed_runtime_disabled}
      {:error, _reason} = error -> error
      other -> {:error, {:unexpected_result, other}}
    end
  rescue
    _error -> {:error, :operator_migration_failed}
  catch
    _kind, _reason -> {:error, :operator_migration_failed}
  end

  def execute(_identifier, nil), do: {:error, :trusted_workflow_path_required}
  def execute(_identifier, _workflow_path), do: {:error, :invalid_issue_identifier_or_workflow_path}

  defp retire_locked(runtime, entry) do
    with :ok <- ManagedLauncherLock.require_service_stopped(runtime.pool_key),
         :ok <- require_paused_gate(),
         :ok <- trusted_state_paths(runtime, entry),
         fence_path = Config.execution_fence_state_path(),
         graph_path = Config.responsibility_graph_state_path(),
         {:ok, fence} <- FencePersistence.load(fence_path),
         {:ok, graph} <- GraphPersistence.load(graph_path),
         now_ms = System.system_time(:millisecond),
         {:ok, next_fence, next_graph, result} <-
           UnsubmittedSuccessor.prepare(runtime, fence, graph, entry, now_ms),
         :ok <- require_paused_gate(),
         :ok <- ManagedLauncherLock.require_service_stopped(runtime.pool_key),
         :ok <- GraphPersistence.save(graph_path, next_graph),
         :ok <- require_paused_gate(),
         :ok <- ManagedLauncherLock.require_service_stopped(runtime.pool_key),
         :ok <- FencePersistence.save(fence_path, next_fence),
         {:ok, persisted_fence} <- FencePersistence.load(fence_path),
         {:ok, persisted_graph} <- GraphPersistence.load(graph_path),
         {:ok, _same_fence, _same_graph, :already_retired} <-
           UnsubmittedSuccessor.prepare(
             runtime,
             persisted_fence,
             persisted_graph,
             entry,
             System.system_time(:millisecond)
           ) do
      {:ok, result}
    else
      {:error, _reason} = error -> error
    end
  end

  defp require_paused_gate do
    case GlobalPause.snapshot() do
      %{configured?: true, paused?: true} -> :ok
      _ -> {:error, :global_gate_must_be_configured_and_paused}
    end
  end

  defp select_entry(%{managed_delegations: %{schema_version: 2, entries: entries}}, identifier)
       when is_list(entries) do
    case Enum.filter(entries, &(&1.identifier == identifier)) do
      [entry] ->
        if is_map(entry[:prior_unsubmitted_authority]) and is_map(entry[:unsubmitted_observation]),
          do: {:ok, entry},
          else: {:error, :signed_unsubmitted_successor_missing}

      _ ->
        {:error, :signed_successor_entry_not_unique}
    end
  end

  defp select_entry(_runtime, _identifier), do: {:error, :signed_successor_manifest_unavailable}

  defp trusted_state_paths(runtime, entry) do
    observation = entry.unsubmitted_observation

    with true <- is_map(observation),
         true <- observation["workflow_sha256"] == runtime.workflow_sha256,
         true <- observation["journal_path"] == runtime.journal_path,
         true <- observation["execution_fence_path"] == runtime.execution_fence_path,
         true <- observation["responsibility_graph_path"] == runtime.responsibility_graph_path,
         true <- Path.basename(Config.local_workspace_root()) == runtime.pool_key,
         :ok <-
           ManagedLauncherLock.trusted_state_files(
             runtime.journal_path,
             runtime.execution_fence_path,
             runtime.responsibility_graph_path,
             runtime.pool_key
           ) do
      :ok
    else
      _ -> {:error, :signed_pool_state_paths_do_not_match_runtime}
    end
  end

  defp trusted_workflow_file(path) when is_binary(path) do
    with true <- Path.type(path) == :absolute and Path.expand(path) == path,
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1}} <- File.lstat(path),
         true <- band(mode, 0o022) == 0,
         :ok <- plain_trusted_ancestors(Path.dirname(path)),
         {:ok, bytes} <- File.read(path) do
      {:ok, digest(bytes)}
    else
      _ -> {:error, :untrusted_workflow_file}
    end
  end

  defp digest(bytes) when is_binary(bytes) do
    bytes
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end

  defp plain_trusted_ancestors(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when band(mode, 0o022) == 0 ->
        parent = Path.dirname(path)
        if parent == path, do: :ok, else: plain_trusted_ancestors(parent)

      _ ->
        {:error, :untrusted_workflow_ancestor}
    end
  end
end
