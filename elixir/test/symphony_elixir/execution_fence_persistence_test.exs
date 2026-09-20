defmodule SymphonyElixir.ExecutionFencePersistenceTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ExecutionFence.Persistence

  @issue "HGS-294"

  setup do
    root =
      Path.join(
        System.tmp_dir!(),
        "symphony-execution-fence-#{System.unique_integer([:positive])}"
      )

    path = Path.join(root, "state.json")
    File.mkdir_p!(root)

    on_exit(fn -> File.rm_rf(root) end)
    {:ok, path: path}
  end

  test "persists and rehydrates the complete generation and lease registry", %{path: path} do
    {:ok, state, token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)

    {:ok, state, :registered} =
      ExecutionFence.register(state, token, :worker, session("worker-1"), 100)

    {:ok, state, :registered} =
      ExecutionFence.register(state, token, :reviewer, session("reviewer-1"), 100)

    assert :ok = Persistence.save(path, state)
    assert {:ok, restored} = Persistence.load(path)
    assert restored == state
    assert {:ok, restored} = ExecutionFence.mark_unreconciled_after_restart(restored)
    assert restored.executions[@issue].ownership == :unknown
    assert restored.executions[@issue].leases["worker-1"].status == :active
    assert restored.executions[@issue].leases["reviewer-1"].status == :active
  end

  test "a malformed durable snapshot fails closed", %{path: path} do
    File.write!(path, ~s({"schema_version":1,"executions":[],"sessions":{},"history":[]}))

    assert {:error, {:invalid_snapshot, _reason}} = Persistence.load(path)
  end

  test "exact-byte decoding retains the same validator and never consults recovery files", %{path: path} do
    state = ExecutionFence.new()
    assert :ok = Persistence.save(path, state)
    bytes = File.read!(path)
    assert {:ok, ^state} = Persistence.decode_bytes(bytes)
    File.write!(path, "{broken}")
    assert {:ok, ^state} = Persistence.decode_bytes(bytes)
    assert File.read!(path) == "{broken}"

    for invalid <- [nil, 7, "{broken}", ~s({"schema_version":1,"executions":[],"sessions":{},"history":[]})] do
      assert {:error, {:invalid_snapshot, _}} = Persistence.decode_bytes(invalid)
    end
  end

  test "save creates the parent directory and replaces an existing snapshot", %{path: path} do
    {:ok, state, _token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)
    assert :ok = Persistence.save(path, state)

    {:ok, next_state, _next_token} = ExecutionFence.admit(state, admission("next"), 200)
    assert :ok = Persistence.save(path, next_state)
    assert {:ok, restored} = Persistence.load(path)
    assert Map.has_key?(restored.executions, "next")
  end

  test "recovers a valid snapshot left beside a missing primary file", %{path: path} do
    {:ok, state, _token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)
    assert :ok = Persistence.save(path, state)

    recovery_path = "#{path}.previous-recovery"
    assert :ok = File.rename(path, recovery_path)

    assert {:ok, ^state} = Persistence.load(path)
  end

  test "persists and rehydrates the exactly-once divergence triage record", %{path: path} do
    {:ok, state, token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)
    {:ok, state, :fenced} = ExecutionFence.fence(state, token, terminal(), 110)

    {:ok, state, :recorded} =
      ExecutionFence.record_head_divergence(state, token, "abc123", "def456", 120)

    assert :ok = Persistence.save(path, state)
    assert {:ok, restored} = Persistence.load(path)
    assert restored == state
    assert [%{observed_head: "def456"}] = Map.values(restored.triage_records)
  end

  test "persists the termination safety flag and cleanup receipt", %{path: path} do
    {:ok, state, token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)

    {:ok, state, :registered} =
      ExecutionFence.register(state, token, :worker, session("worker-1"), 100)

    {:ok, state, %{status: :blocked, expired: ["worker-1"]}} =
      ExecutionFence.reconcile_sessions(state, [], 200, 50)

    assert :ok = Persistence.save(path, state)
    assert {:ok, state} = Persistence.load(path)
    assert state.executions[@issue].termination_unconfirmed

    {:ok, state, :fenced} = ExecutionFence.fence(state, token, terminal(), 210)

    evidence = %{
      session_id: "worker-1",
      process_id: "process-worker-1",
      process_tree: :terminated,
      evidence_ref: "process-tree-check-1",
      observed_at_ms: 220
    }

    {:ok, state, :confirmed} =
      ExecutionFence.confirm_termination(state, token, "worker-1", evidence, 220)

    {:ok, state, :prepared} = ExecutionFence.prepare_cleanup(state, token, "abc123", 230)

    assert :ok = Persistence.save(path, state)
    assert {:ok, restored} = Persistence.load(path)
    assert restored == state
    refute restored.executions[@issue].termination_unconfirmed
    assert restored.executions[@issue].cleanup_receipt.phase == :removal_started
    assert restored.executions[@issue].leases["worker-1"].termination_confirmed_at_ms == 220
    assert restored.executions[@issue].leases["worker-1"].termination_evidence_ref == "process-tree-check-1"
    assert restored.executions[@issue].leases["worker-1"].termination_evidence == evidence
  end

  test "persists released lease termination uncertainty through restart", %{path: path} do
    {:ok, state, token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)

    {:ok, state, :registered} =
      ExecutionFence.register(state, token, :worker, session("worker-1"), 100)

    {:ok, state, :released} =
      ExecutionFence.release(state, token, "worker-1", :orchestrator_stop)

    assert :ok = Persistence.save(path, state)
    assert {:ok, restored} = Persistence.load(path)
    assert restored.executions[@issue].termination_unconfirmed
    assert restored.executions[@issue].leases["worker-1"].termination_required

    assert {:ok, restored} = ExecutionFence.mark_unreconciled_after_restart(restored)
    assert restored.executions[@issue].ownership == :unknown

    evidence = %{
      session_id: "worker-1",
      process_id: "process-worker-1",
      process_tree: :terminated,
      evidence_ref: "process-tree-check-restart",
      observed_at_ms: 120
    }

    assert {:ok, restored, :confirmed} =
             ExecutionFence.confirm_termination(restored, token, "worker-1", evidence, 120)

    refute restored.executions[@issue].termination_unconfirmed
    assert restored.executions[@issue].ownership == :reconciled
    assert :ok = Persistence.save(path, restored)
    assert {:ok, reloaded} = Persistence.load(path)
    assert reloaded.executions[@issue].leases["worker-1"].termination_required
    assert reloaded.executions[@issue].leases["worker-1"].termination_confirmed_at_ms == 120
  end

  test "cold persistence loads full termination evidence without preloaded fence atoms", %{path: path} do
    {:ok, state, token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)
    {:ok, state, :registered} = ExecutionFence.register(state, token, :worker, session("worker-1"), 100)
    assert :ok = Persistence.save(path, state)

    unknown = "unknown_termination_#{System.unique_integer([:positive])}"

    evidence = %{
      "process_tree" => "terminated",
      "supervisor" => "systemd_user",
      "session_id" => "worker-1",
      "process_id" => "process-worker-1",
      "unit" => "symphony-exec-cold.scope",
      "pre_active_state" => nil,
      "pre_control_group" => "/qualification/symphony-exec-cold.scope",
      "pre_processes" => [123, 124],
      "main_pid" => nil,
      "active_state" => "inactive",
      "control_group" => nil,
      "remaining_processes" => 0,
      "observed_at_ms" => 120,
      "evidence_ref" => "cold-load-proof",
      unknown => "ignored"
    }

    payload = path |> File.read!() |> Jason.decode!()
    lease = Map.put(payload["sessions"]["worker-1"], "termination_evidence", evidence)

    payload =
      payload
      |> put_in(["sessions", "worker-1"], lease)
      |> put_in(["executions", @issue, "leases", "worker-1"], lease)

    snapshot = Jason.encode!(payload)
    File.write!(path, snapshot)

    # This script contains no termination-field atom literals. Its VM has not
    # loaded the modules that previously happened to intern the decoder's keys.
    script = ~S"""
    [path, unknown] = System.argv()
    false = :code.is_loaded(SymphonyElixir.ExecutionFence)
    false = :code.is_loaded(SymphonyElixir.ExecutionSupervisor)
    {:ok, restored} = SymphonyElixir.ExecutionFence.Persistence.load(path)
    actual = Map.new(restored.sessions["worker-1"].termination_evidence, fn {key, value} ->
      value = if is_atom(value) and not is_nil(value), do: Atom.to_string(value), else: value
      {Atom.to_string(key), value}
    end)
    expected = path |> File.read!() |> Jason.decode!() |> get_in(["sessions", "worker-1", "termination_evidence"]) |> Map.delete(unknown)
    true = actual == expected
    try do
      String.to_existing_atom(unknown)
      raise "unknown termination field was interned"
    rescue
      ArgumentError -> :ok
    end
    IO.puts("cold termination evidence loaded")
    """

    {output, status} = cold_process(script, [path, unknown])

    assert status == 0, output
    assert output =~ "cold termination evidence loaded"
    assert File.read!(path) == snapshot
  end

  test "cold cleanup outcome decoding retains its closed value set", %{path: path} do
    {:ok, state, token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)
    {:ok, state, :fenced} = ExecutionFence.fence(state, token, terminal(), 110)
    {:ok, state, :prepared} = ExecutionFence.prepare_cleanup(state, token, "abc123", 120)
    assert :ok = Persistence.save(path, state)
    payload = path |> File.read!() |> Jason.decode!()

    script = ~S"""
    [path, issue, expected] = System.argv()
    false = :code.is_loaded(SymphonyElixir.ExecutionFence)
    result = SymphonyElixir.ExecutionFence.Persistence.load(path)
    if expected == "invalid" do
      {:error, {:invalid_snapshot, :invalid_cleanup_terminal_outcome}} = result
    else
      {:ok, restored} = result
      true = Atom.to_string(restored.executions[issue].cleanup_receipt.terminal_outcome) == expected
    end
    IO.puts("cold cleanup outcome checked")
    """

    for outcome <- ["completed", "failed", "blocked", "invalid"] do
      snapshot = payload |> put_in(["executions", @issue, "cleanup_receipt", "terminal_outcome"], outcome) |> Jason.encode!()
      File.write!(path, snapshot)
      {output, status} = cold_process(script, [path, @issue, outcome])
      assert status == 0, output
      assert output =~ "cold cleanup outcome checked"
      assert File.read!(path) == snapshot
    end
  end

  test "legacy snapshots with expired leases migrate to unconfirmed termination", %{path: path} do
    {:ok, state, token} = ExecutionFence.admit(ExecutionFence.new(), admission(), 100)

    {:ok, state, :registered} =
      ExecutionFence.register(state, token, :worker, session("worker-1"), 100)

    {:ok, state, _summary} = ExecutionFence.reconcile_sessions(state, [], 200, 50)
    assert :ok = Persistence.save(path, state)

    payload =
      path
      |> File.read!()
      |> Jason.decode!()

    legacy_execution = Map.delete(payload["executions"][@issue], "termination_unconfirmed")
    legacy_payload = put_in(payload, ["executions", @issue], legacy_execution)

    File.write!(path, Jason.encode!(legacy_payload))

    assert {:ok, restored} = Persistence.load(path)
    assert restored.executions[@issue].termination_unconfirmed
    refute Map.has_key?(restored.executions[@issue].leases["worker-1"], :termination_confirmed_at_ms)
  end

  defp cold_process(script, args) do
    code_paths = Enum.flat_map(:code.get_path(), &["-pa", List.to_string(&1)])
    executable = System.find_executable("elixir") || raise "elixir executable unavailable"
    script_path = Path.join(System.tmp_dir!(), "symphony-cold-process-#{System.unique_integer([:positive])}.exs")

    try do
      File.write!(script_path, script)
      System.cmd(executable, ["--erl", "+S 2:2"] ++ code_paths ++ [script_path | args], stderr_to_stdout: true)
    after
      File.rm(script_path)
    end
  end

  defp admission(issue_id \\ @issue) do
    %{
      issue_id: issue_id,
      repository: "openai/symphony",
      branch: "codex/#{String.downcase(issue_id)}",
      worktree: "C:/code/hypergrid.au/_worktrees/#{String.downcase(issue_id)}"
    }
  end

  defp session(session_id) do
    Map.merge(admission(), %{
      generation: 1,
      role: :worker,
      session_id: session_id,
      process_id: "process-#{session_id}",
      linear_state: "In Progress",
      pr_state: "OPEN",
      head: "abc123",
      last_heartbeat_at: 100
    })
  end

  defp terminal do
    %{terminal_state: "Done", accepted_head: "abc123"}
  end
end
