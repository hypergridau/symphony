defmodule SymphonyElixir.ManagedLauncherLock do
  import Bitwise, only: [band: 2]

  @moduledoc """
  Holds the managed pool's host launcher lock during the bounded successor-retirement task.

  The lock path is derived from the exact trusted host journal root and pool key carried by the
  signed observation. Only the fixed system `flock` executable is used; platforms without it fail
  closed.
  """

  @flock_path "/usr/bin/flock"
  @systemctl_path "/usr/bin/systemctl"
  @flock_ready_marker "symphony-unsubmitted-successor-lock-ready"
  @flock_busy_status 75
  @trusted_pools_root "/srv/dahlia-runner-state/run/pools"
  @recovery_pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)
  @managed_owner 1001

  @doc "Derives the adjacent pool launcher lock from the canonical journal path."
  @spec pool_lock_path(Path.t(), String.t()) :: {:ok, Path.t()} | {:error, term()}
  def pool_lock_path(journal_path, pool_key) when is_binary(journal_path) and is_binary(pool_key) do
    expected_journal = Path.join([@trusted_pools_root, pool_key, "work-package.json"])

    with true <- Regex.match?(~r/\A[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}\z/, pool_key),
         true <- Path.type(journal_path) == :absolute and Path.expand(journal_path) == journal_path,
         true <- journal_path == expected_journal do
      {:ok, Path.join(@trusted_pools_root, pool_key <> ".lock")}
    else
      _ -> {:error, :untrusted_pool_launcher_lock_path}
    end
  end

  def pool_lock_path(_journal_path, _pool_key), do: {:error, :untrusted_pool_launcher_lock_path}

  @doc "Requires existing journal, fence, and graph files under canonical plain directories."
  @spec trusted_state_files(Path.t(), Path.t(), Path.t(), String.t()) :: :ok | {:error, term()}
  def trusted_state_files(journal_path, fence_path, graph_path, pool_key)
      when is_binary(journal_path) and is_binary(fence_path) and is_binary(graph_path) and is_binary(pool_key) do
    expected_fence =
      Path.join(["/srv/dahlia-runner-state", "workspaces", "pools", pool_key, ".symphony", "execution-fence.json"])

    expected_graph = Path.join(Path.dirname(expected_fence), "responsibility-graph.json")

    with true <- Regex.match?(~r/\A[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}\z/, pool_key),
         true <- journal_path == Path.join([@trusted_pools_root, pool_key, "work-package.json"]),
         true <- fence_path == expected_fence,
         true <- graph_path == expected_graph,
         {:ok, runner_uid} <- effective_uid(),
         :ok <- trusted_regular_file(journal_path, runner_uid),
         :ok <- trusted_regular_file(fence_path, runner_uid),
         :ok <- trusted_regular_file(graph_path, runner_uid) do
      :ok
    else
      _ -> {:error, :untrusted_pool_state_path}
    end
  end

  def trusted_state_files(_journal_path, _fence_path, _graph_path, _pool_key),
    do: {:error, :untrusted_pool_state_path}

  @doc false
  @spec trusted_regular_metadata?(File.Stat.t(), non_neg_integer()) :: boolean()
  def trusted_regular_metadata?(%File.Stat{type: :regular, uid: uid, mode: mode, links: 1}, uid)
      when band(mode, 0o077) == 0,
      do: true

  def trusted_regular_metadata?(_stat, _runner_uid), do: false

  @doc false
  @spec trusted_directory_metadata?(File.Stat.t(), non_neg_integer()) :: boolean()
  def trusted_directory_metadata?(%File.Stat{type: :directory, uid: uid, mode: mode}, runner_uid)
      when uid in [0, runner_uid] and band(mode, 0o022) == 0,
      do: true

  def trusted_directory_metadata?(%File.Stat{type: :directory, uid: 0, mode: mode}, _runner_uid)
      when band(mode, 0o1000) != 0,
      do: true

  def trusted_directory_metadata?(_stat, _runner_uid), do: false

  @doc "Runs a callback while the derived existing pool lock is held exclusively by the OS."
  @spec with_exclusive_lock(Path.t(), (-> term())) :: term()
  def with_exclusive_lock(path, callback) when is_binary(path) and is_function(callback, 0) do
    with {:ok, owner} <- effective_uid(), do: with_owner_lock(path, owner, callback)
  end

  def with_exclusive_lock(_path, _callback), do: {:error, :pool_launcher_lock_unavailable}

  @doc "Root recovery holds the existing managed UID 1001 lock without changing its ownership."
  @spec with_root_recovery_lock(Path.t(), String.t(), (-> term())) :: term()
  def with_root_recovery_lock(journal, pool, callback) when pool in @recovery_pools and is_function(callback, 0) do
    with {:ok, 0} <- effective_uid(),
         {:ok, path} <- pool_lock_path(journal, pool) do
      with_owner_lock(path, @managed_owner, callback)
    else
      {:error, _reason} = error -> error
      _ -> {:error, :pool_launcher_lock_file_untrusted}
    end
  end

  def with_root_recovery_lock(_journal, _pool, _callback), do: {:error, :pool_launcher_lock_file_untrusted}

  defp with_owner_lock(path, owner, callback) do
    with :ok <- linux_only(),
         :ok <- trusted_flock_executable(),
         :ok <- trusted_lock_file(path, owner),
         {:ok, before} <- File.lstat(path) do
      case open_lock_port(path) do
        {:ok, port} ->
          try do
            with :ok <- await_ready(port, <<>>),
                 :ok <- verify_held_inode(port, path, owner, before) do
              result = callback.()
              with :ok <- verify_held_inode(port, path, owner, before), do: result
            end
          after
            close_lock_port(port)
          end

        {:error, _reason} = error ->
          error
      end
    else
      {:error, _reason} = error -> error
    end
  rescue
    _error -> {:error, :pool_launcher_lock_unavailable}
  catch
    _kind, _reason -> {:error, :pool_launcher_lock_unavailable}
  end

  defp verify_held_inode(port, path, owner, before) do
    with :ok <- trusted_lock_file(path, owner),
         {:ok, after_stat} <- File.lstat(path),
         true <- lock_identity(before) == lock_identity(after_stat),
         {:os_pid, pid} <- Port.info(port, :os_pid),
         directory <- "/proc/#{pid}/fd",
         {:ok, entries} <- File.ls(directory),
         true <- Enum.any?(entries, &held_inode?(Path.join(directory, &1), before)) do
      :ok
    else
      _ -> {:error, :pool_launcher_lock_inode_changed}
    end
  end

  defp held_inode?(path, expected) do
    case File.stat(path) do
      {:ok, actual} -> lock_identity(actual) == lock_identity(expected)
      _ -> false
    end
  end

  defp lock_identity(stat), do: Map.take(stat, [:major_device, :minor_device, :inode, :type, :uid, :gid, :mode, :links])

  @doc "Requires the exact pool's systemd unit to be loaded and fully inactive."
  @spec require_service_stopped(String.t()) :: :ok | {:error, term()}
  def require_service_stopped(pool_key) when is_binary(pool_key) do
    with true <- Regex.match?(~r/\A[a-zA-Z0-9][a-zA-Z0-9._-]{0,63}\z/, pool_key),
         :ok <- linux_only(),
         :ok <- trusted_systemctl_executable(),
         {output, 0} <-
           System.cmd(
             @systemctl_path,
             [
               "show",
               "--property=LoadState,ActiveState,ControlGroup,MainPID",
               "dahlia-symphony@#{pool_key}.service"
             ],
             stderr_to_stdout: true
           ),
         true <- stopped_unit_properties?(output) do
      :ok
    else
      _ -> {:error, :pool_service_not_proven_stopped}
    end
  rescue
    _error -> {:error, :pool_service_not_proven_stopped}
  end

  def require_service_stopped(_pool_key), do: {:error, :pool_service_not_proven_stopped}

  @doc false
  @spec stopped_unit_properties?(String.t()) :: boolean()
  def stopped_unit_properties?(output) when is_binary(output) do
    allowed = ["LoadState", "ActiveState", "ControlGroup", "MainPID"]

    with {:ok, properties} <- parse_unit_properties(String.split(output, "\n", trim: true), allowed),
         true <- Map.has_key?(properties, "LoadState"),
         true <- Map.has_key?(properties, "ActiveState"),
         true <- Map.has_key?(properties, "ControlGroup"),
         true <- properties["LoadState"] == "loaded",
         true <- properties["ActiveState"] == "inactive",
         true <- properties["ControlGroup"] == "",
         true <- properties["MainPID"] in [nil, "0"] do
      true
    else
      _ -> false
    end
  end

  def stopped_unit_properties?(_output), do: false

  defp parse_unit_properties(lines, allowed) do
    Enum.reduce_while(lines, {:ok, %{}}, &put_unit_property(&1, &2, allowed))
  end

  defp put_unit_property(line, {:ok, properties}, allowed) do
    with [key, value] <- String.split(line, "=", parts: 2),
         true <- key in allowed,
         false <- Map.has_key?(properties, key) do
      {:cont, {:ok, Map.put(properties, key, value)}}
    else
      _ -> {:halt, :invalid}
    end
  end

  defp linux_only do
    if :os.type() == {:unix, :linux}, do: :ok, else: {:error, :pool_launcher_lock_unsupported}
  end

  defp trusted_flock_executable do
    case File.lstat(@flock_path) do
      {:ok, %File.Stat{type: :regular, uid: 0, mode: mode}} when band(mode, 0o022) == 0 -> :ok
      _ -> {:error, :pool_launcher_lock_unsupported}
    end
  end

  defp trusted_systemctl_executable do
    case File.lstat(@systemctl_path) do
      {:ok, %File.Stat{type: :regular, uid: 0, mode: mode}} when band(mode, 0o022) == 0 -> :ok
      _ -> {:error, :pool_service_probe_unsupported}
    end
  end

  defp trusted_lock_file(path, runner_uid) do
    if Path.type(path) == :absolute and Path.expand(path) == path and not String.starts_with?(path, ["//", "\\\\"]) do
      with {:ok, stat} <- File.lstat(path),
           true <- trusted_regular_metadata?(stat, runner_uid),
           :ok <- trusted_directory_ancestors(Path.dirname(path), runner_uid) do
        :ok
      else
        _ -> {:error, :pool_launcher_lock_file_untrusted}
      end
    else
      {:error, :pool_launcher_lock_file_untrusted}
    end
  end

  defp trusted_regular_file(path, runner_uid) do
    if Path.type(path) == :absolute and Path.expand(path) == path do
      with {:ok, stat} <- File.lstat(path),
           true <- trusted_regular_metadata?(stat, runner_uid),
           :ok <- trusted_directory_ancestors(Path.dirname(path), runner_uid) do
        :ok
      else
        _ -> {:error, :untrusted_pool_state_path}
      end
    else
      {:error, :untrusted_pool_state_path}
    end
  end

  defp trusted_directory_ancestors(path, runner_uid) do
    case File.lstat(path) do
      {:ok, stat} ->
        if trusted_directory_metadata?(stat, runner_uid) do
          trusted_directory_parent(path, runner_uid)
        else
          {:error, :pool_launcher_lock_file_untrusted}
        end

      _ ->
        {:error, :pool_launcher_lock_file_untrusted}
    end
  end

  defp trusted_directory_parent(path, runner_uid) do
    parent = Path.dirname(path)
    if parent == path, do: :ok, else: trusted_directory_ancestors(parent, runner_uid)
  end

  defp effective_uid do
    case File.stat("/proc/self") do
      {:ok, %File.Stat{uid: uid}} when is_integer(uid) and uid >= 0 -> {:ok, uid}
      _ -> {:error, :pool_launcher_lock_unsupported}
    end
  end

  defp open_lock_port(path) do
    command = "printf '#{@flock_ready_marker}'; while read _line; do :; done"

    try do
      {:ok,
       Port.open(
         {:spawn_executable, @flock_path},
         [:binary, :exit_status, {:args, ["-F", "-x", "-n", "-E", Integer.to_string(@flock_busy_status), path, "-c", command]}]
       )}
    rescue
      error -> {:error, {:pool_launcher_lock_open_failed, error}}
    end
  end

  defp await_ready(port, buffer) do
    receive do
      {^port, {:data, data}} when is_binary(data) ->
        next = buffer <> data

        if String.starts_with?(next, @flock_ready_marker),
          do: :ok,
          else: await_ready(port, next)

      {^port, {:exit_status, @flock_busy_status}} ->
        {:error, :pool_launcher_lock_busy}

      {^port, {:exit_status, _status}} ->
        {:error, :pool_launcher_lock_unavailable}
    after
      5_000 -> {:error, :pool_launcher_lock_timeout}
    end
  end

  defp close_lock_port(port) do
    if Port.info(port), do: Port.close(port)
  rescue
    _error -> :ok
  end
end
