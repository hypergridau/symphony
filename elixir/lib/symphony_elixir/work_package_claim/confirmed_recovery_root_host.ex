defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost do
  @moduledoc """
  Fixed Linux host adapter for HGS-740 root authority, signer, process checks and IO.

  Core owns recovery policy. This module supplies a verified context and explicit
  callbacks for host operations, keeping production paths and the signer pinned.
  """

  import Bitwise, only: [band: 2]

  alias SymphonyElixir.{Config, ManagedLauncherLock, Workflow}
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.WorkPackageClaim.{ConfirmedRecoveryContext, ConfirmedRecoveryEvidence}
  alias SymphonyElixir.WorkPackageClaim.Journal

  @state_root "/srv/dahlia-runner-state"
  @evidence_root "/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery"
  @identity_root "/srv/dahlia-runner-state/identity/claim-recovery-hgs485"
  @pause_path "/srv/dahlia-runner-state/control/global-mutable-pause.state"
  @provider_receipt_root "/etc/dahlia-managed-claim-recovery/hgs485-20260909"
  @signer_fingerprint "903b66d70e23219ee947bdbbdd738b29851302a24985edd4a69abc8a2875d8e6"
  @pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)
  @systemd_properties ~w(ActiveState ControlGroup MainPID)

  @type result :: {:ok, ConfirmedRecoveryContext.t()} | {:error, term()}

  @doc false
  @spec authorize_apply(String.t(), String.t(), String.t(), String.t()) :: result()
  def authorize_apply(issue_id, pool, workflow_path, nonce) do
    with :ok <- require_root(),
         :ok <- require_pool(pool),
         :ok <- require_issue_id(issue_id),
         :ok <- require_paused_gate(),
         :ok <- trusted_workflow_file(workflow_path, pool),
         :ok <- Workflow.set_workflow_file_path(workflow_path),
         {:ok, runtime} <- runtime_paths(pool) do
      verified_context(issue_id, pool, nonce, workflow_path, runtime)
    end
  end

  @doc false
  @spec authorize_completion(String.t(), String.t(), String.t()) :: result()
  def authorize_completion(issue_id, pool, workflow_path) do
    with :ok <- require_root(),
         :ok <- require_pool(pool),
         :ok <- require_issue_id(issue_id),
         :ok <- trusted_workflow_file(workflow_path, pool),
         :ok <- Workflow.set_workflow_file_path(workflow_path),
         {:ok, runtime} <- runtime_paths(pool) do
      verified_context(issue_id, pool, issue_id, workflow_path, runtime)
    end
  end

  @doc false
  @spec authorize_startup(String.t(), String.t()) :: result()
  def authorize_startup(workflow_path, pool) do
    with :ok <- require_root(),
         :ok <- require_pool(pool),
         :ok <- trusted_workflow_file(workflow_path, pool),
         :ok <- Workflow.set_workflow_file_path(workflow_path),
         {:ok, runtime} <- fixed_runtime_paths(pool) do
      verified_context("f77e349e-21d9-4bdf-bad3-ce08b302e7e8", pool, "", workflow_path, runtime)
    else
      {:error, _reason} = error -> error
      _ -> {:error, :hgs740_startup_held_closed}
    end
  end

  @doc false
  @spec with_pool_lock(ConfirmedRecoveryContext.t(), (-> term())) :: term()
  def with_pool_lock(%ConfirmedRecoveryContext{runtime: runtime, pool: pool}, fun) when is_function(fun, 0) do
    with {:ok, lock_path} <- ManagedLauncherLock.pool_lock_path(runtime.journal_path, pool) do
      ManagedLauncherLock.with_exclusive_lock(lock_path, fun)
    end
  end

  @doc false
  @spec operations() :: map()
  def operations do
    %{
      paths: %{
        state_root: @state_root,
        evidence_root: @evidence_root,
        provider_receipt_root: @provider_receipt_root
      },
      marker_directory: &marker_directory/1,
      fixed_runtime_paths: &fixed_runtime_paths/1,
      require_service_stopped: &ManagedLauncherLock.require_service_stopped/1,
      require_services_quiescent: &require_services_quiescent/0,
      require_mutation_quiescent: &require_mutation_quiescent/2,
      no_processes_for_uid: &no_processes_for_uid/1,
      require_paused_gate: &require_paused_gate/0,
      read_public_key: &read_public_key/0,
      verify_signed_evidence: &verify_signed_evidence/2,
      save_state: &save_state/3,
      lstat: &File.lstat/1,
      lstat_posix: &lstat_posix/1,
      stat: &File.stat/1,
      read: &File.read/1,
      ls: &File.ls/1,
      open: &File.open/2,
      close: &File.close/1,
      chmod: &File.chmod/2,
      rename: &File.rename/2,
      remove: &File.rm/1,
      change_owner: &change_owner/3,
      raw_open: &raw_open/2,
      raw_read: &:file.read/2,
      raw_write: &:file.write/2,
      raw_sync: &:file.sync/1,
      raw_close: &:file.close/1,
      read_file_info: &read_file_info/2,
      system_cmd: &system_cmd/1,
      now_ms: fn -> System.system_time(:millisecond) end,
      unique_integer: fn -> System.unique_integer([:positive]) end
    }
  end

  @doc false
  @spec marker_directory(String.t()) :: Path.t()
  def marker_directory(issue_id), do: Path.join([@evidence_root, issue_id, "generation-2"])

  @doc false
  @spec fixed_runtime_paths(String.t()) :: {:ok, map()} | {:error, :invalid_pool}
  def fixed_runtime_paths(pool) do
    journal_path = Path.join([@state_root, "run", "pools", pool, "work-package.json"])
    state_dir = Path.join([@state_root, "workspaces", "pools", pool, ".symphony"])

    with :ok <- require_pool(pool) do
      {:ok,
       %{
         pool_key: pool,
         journal_path: journal_path,
         execution_fence_path: Path.join(state_dir, "execution-fence.json"),
         responsibility_graph_path: Path.join(state_dir, "responsibility-graph.json")
       }}
    end
  end

  @doc false
  @spec require_services_quiescent() :: :ok | {:error, :managed_services_not_quiescent}
  def require_services_quiescent do
    units = Enum.map(@pools, &"dahlia-symphony@#{&1}.service") ++ ["dahlia-claim-witness.service", "dahlia-claim-witness.socket"]

    with :ok <- trusted_systemctl(),
         true <- Enum.all?(units, &unit_quiescent?/1) do
      :ok
    else
      _ -> {:error, :managed_services_not_quiescent}
    end
  end

  @doc false
  @spec require_mutation_quiescent(map(), non_neg_integer()) :: :ok | {:error, :pool_state_owner_not_quiescent}
  def require_mutation_quiescent(runtime, uid) do
    with :ok <- require_services_quiescent(),
         :ok <- trusted_runtime_directories(runtime, runtime.pool_key, uid),
         :ok <- user_manager_quiescent(uid),
         :ok <- no_processes_for_uid(uid) do
      :ok
    else
      _ -> {:error, :pool_state_owner_not_quiescent}
    end
  end

  @doc false
  @spec verify_signed_evidence(binary(), map()) :: {:ok, map()} | {:error, term()}
  def verify_signed_evidence(bytes, bindings) do
    with {:ok, key} <- read_public_key(),
         do: ConfirmedRecoveryEvidence.verify(bytes, key, bindings)
  end

  defp verified_context(issue_id, pool, nonce, workflow_path, runtime) do
    {:ok,
     %ConfirmedRecoveryContext{
       issue_id: issue_id,
       pool: pool,
       nonce: nonce,
       workflow_path: workflow_path,
       runtime: runtime,
       host_ops: operations()
     }}
  end

  defp require_pool(pool) when pool in @pools, do: :ok
  defp require_pool(_pool), do: {:error, :invalid_pool}

  defp require_issue_id(issue_id) do
    if is_binary(issue_id) and Regex.match?(~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, issue_id),
      do: :ok,
      else: {:error, :invalid_issue_id}
  end

  defp require_root do
    case File.stat("/proc/self") do
      {:ok, %File.Stat{uid: 0}} -> :ok
      _ -> {:error, :root_privilege_required}
    end
  end

  defp require_paused_gate do
    transition_path = Path.join(Path.dirname(@pause_path), "global-mutable-pause.transition")

    with :ok <- trusted_root_directory(Path.dirname(@pause_path)),
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1}} <- File.lstat(@pause_path),
         true <- band(mode, 0o777) == 0o640,
         {:ok, "paused\n"} <- File.read(@pause_path),
         {:error, :enoent} <- File.lstat(transition_path) do
      :ok
    else
      _ -> {:error, :global_gate_must_be_configured_and_paused}
    end
  end

  defp trusted_workflow_file(path, pool) do
    expected_path = Path.join([@state_root, "dahlia", "config", "symphony", "workflows", pool <> ".md"])

    with true <- Path.type(path) == :absolute and Path.expand(path) == path,
         true <- path == expected_path,
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1}} <- File.lstat(path),
         true <- band(mode, 0o022) == 0,
         :ok <- trusted_root_directory(Path.dirname(path)) do
      :ok
    else
      _ -> {:error, :untrusted_workflow_file}
    end
  end

  defp runtime_paths(pool) do
    with {:ok, paths} <- fixed_runtime_paths(pool),
         true <- Config.execution_fence_state_path() == paths.execution_fence_path do
      {:ok, paths}
    else
      _ -> {:error, :configured_state_path_mismatch}
    end
  end

  defp read_public_key do
    pem_path = Path.join([@identity_root, "public.pem"])
    metadata_path = Path.join([@identity_root, "public.json"])

    with {:ok, pem} <- read_root_file(pem_path, 16_384),
         {:ok, metadata} <- read_root_file(metadata_path, 4_096),
         {:ok, %{"providerFingerprint" => fingerprint}} <- Jason.decode(metadata),
         true <- fingerprint == @signer_fingerprint,
         {:ok, public_key} <- decode_public_key(pem),
         true <- public_key_fingerprint(public_key) == fingerprint do
      {:ok, public_key}
    else
      _ -> {:error, :untrusted_recovery_key}
    end
  end

  defp read_root_file(path, max_bytes) do
    with :ok <- trusted_root_directory(Path.dirname(path)),
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1, size: size}} <- File.lstat(path),
         true <- band(mode, 0o022) == 0 and size <= max_bytes,
         {:ok, bytes} <- File.read(path) do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_root_file}
    end
  end

  defp decode_public_key(pem) do
    prefix = <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>>

    with [{:SubjectPublicKeyInfo, der, :not_encrypted}] <- :public_key.pem_decode(pem),
         <<^prefix::binary, key::binary-size(32)>> <- der do
      {:ok, key}
    else
      _ -> {:error, :untrusted_recovery_key}
    end
  rescue
    _ -> {:error, :untrusted_recovery_key}
  end

  defp public_key_fingerprint(key) do
    prefix = <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>>
    Base.encode64(prefix <> key) |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
  end

  defp trusted_root_directory(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when band(mode, 0o022) == 0 ->
        parent = Path.dirname(path)
        if parent == path, do: :ok, else: trusted_root_directory(parent)

      _ ->
        {:error, :untrusted_root_directory}
    end
  end

  defp trusted_runtime_directories(runtime, pool, owner) do
    expected_journal = Path.join([@state_root, "run", "pools", pool, "work-package.json"])
    expected_state = Path.join([@state_root, "workspaces", "pools", pool, ".symphony"])

    expected = [expected_journal, Path.join(expected_state, "execution-fence.json"), Path.join(expected_state, "responsibility-graph.json")]

    if [runtime.journal_path, runtime.execution_fence_path, runtime.responsibility_graph_path] == expected and
         Enum.all?(expected, &trusted_state_ancestors?(Path.dirname(&1), owner)),
       do: :ok,
       else: {:error, :untrusted_pool_state_directory}
  end

  defp trusted_state_ancestors?(path, owner) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, uid: uid, mode: mode}} when uid in [0, owner] and band(mode, 0o022) == 0 ->
        parent = Path.dirname(path)
        parent == path or trusted_state_ancestors?(parent, owner)

      _ ->
        false
    end
  end

  defp no_processes_for_uid(uid) do
    with {:ok, entries} <- File.ls("/proc"),
         true <- Enum.all?(entries, &proc_entry_not_owned_by?(&1, uid)) do
      :ok
    else
      _ -> {:error, :state_owner_process_present}
    end
  end

  defp proc_entry_not_owned_by?(entry, uid) do
    if Regex.match?(~r/\A[0-9]+\z/, entry) do
      case File.lstat(Path.join("/proc", entry)) do
        {:ok, %File.Stat{uid: ^uid}} -> false
        {:ok, %File.Stat{}} -> true
        {:error, :enoent} -> true
        _ -> false
      end
    else
      true
    end
  end

  defp user_manager_quiescent(uid) do
    with :ok <- trusted_systemctl(),
         {properties, 0} <- system_cmd(["show", "--property=ActiveState,ControlGroup,MainPID", "user@#{uid}.service"]),
         {:ok, values} <- parse_systemd_properties(properties),
         true <- values["ActiveState"] in ["inactive", "failed"],
         true <- values["ControlGroup"] == "",
         true <- values["MainPID"] in ["0", nil] do
      :ok
    else
      _ -> {:error, :user_manager_not_quiescent}
    end
  rescue
    _ -> {:error, :user_manager_not_quiescent}
  end

  defp unit_quiescent?(unit) do
    with {enabled, 1} <- system_cmd(["is-enabled", unit]),
         true <- String.trim(enabled) == "masked",
         {properties, 0} <- system_cmd(["show", "--property=ActiveState,ControlGroup,MainPID", unit]),
         {:ok, values} <- parse_systemd_properties(properties),
         true <- values["ActiveState"] in ["inactive", "failed"],
         true <- values["ControlGroup"] == "",
         true <- values["MainPID"] in ["0", nil] do
      true
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  defp parse_systemd_properties(output) do
    Enum.reduce_while(String.split(output, "\n", trim: true), {:ok, %{}}, fn line, {:ok, values} ->
      case String.split(line, "=", parts: 2) do
        [key, value] when key in @systemd_properties and not is_map_key(values, key) -> {:cont, {:ok, Map.put(values, key, value)}}
        _ -> {:halt, :invalid}
      end
    end)
    |> case do
      {:ok, values} when map_size(values) == length(@systemd_properties) -> {:ok, values}
      _ -> {:error, :invalid_systemd_properties}
    end
  end

  defp trusted_systemctl do
    case File.lstat("/usr/bin/systemctl") do
      {:ok, %File.Stat{type: :regular, uid: 0, mode: mode}} when band(mode, 0o022) == 0 -> :ok
      _ -> {:error, :untrusted_systemctl}
    end
  end

  if Mix.env() == :test do
    @doc false
    @spec systemctl_invocation_for_test([String.t()]) :: {String.t(), [String.t()], keyword()}
    def systemctl_invocation_for_test(args), do: systemctl_invocation(args)
  end

  defp system_cmd(args) do
    {executable, arguments, options} = systemctl_invocation(args)
    System.cmd(executable, arguments, options)
  end

  defp systemctl_invocation(args) do
    {"/usr/bin/systemctl", ["--system" | args],
     [
       stderr_to_stdout: true,
       env: [
         {"DBUS_SYSTEM_BUS_ADDRESS", nil},
         {"DBUS_SESSION_BUS_ADDRESS", nil},
         {"SYSTEMD_BUS_ADDRESS", nil},
         {"XDG_RUNTIME_DIR", nil}
       ]
     ]}
  end

  defp save_state(:claim_journal, path, state), do: Journal.save(path, state)
  defp save_state(:fence, path, state), do: FencePersistence.save(path, state)
  defp save_state(:responsibility_graph, path, state), do: GraphPersistence.save(path, state)
  defp lstat_posix(path), do: File.lstat(path, time: :posix)
  defp change_owner(path, uid, gid), do: :file.change_owner(String.to_charlist(path), uid, gid)
  defp raw_open(path, modes), do: :file.open(String.to_charlist(path), modes)
  defp read_file_info(io, opts), do: :file.read_file_info(io, opts)
end
