defmodule SymphonyElixir.UnsubmittedSuccessorRetirementTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.Symphony.RetireUnsubmittedSuccessor
  alias SymphonyElixir.UnsubmittedSuccessorRetirement

  test "requires an explicit trusted workflow path" do
    assert {:error, :trusted_workflow_path_required} =
             UnsubmittedSuccessorRetirement.execute("HGS-736", nil)
  end

  test "holds closed when the global pause gate is missing" do
    with_global_pause(nil, fn ->
      assert {:error, :global_gate_must_be_configured_and_paused} =
               UnsubmittedSuccessorRetirement.execute("HGS-736", "/trusted/workflow.md")
    end)
  end

  test "holds closed when the global pause gate is running" do
    with_global_pause("running\n", fn ->
      assert {:error, :global_gate_must_be_configured_and_paused} =
               UnsubmittedSuccessorRetirement.execute("HGS-736", "/trusted/workflow.md")
    end)
  end

  @tag skip: :os.type() != {:unix, :linux}
  test "rejects a caller-owned workflow before reading runtime or state" do
    workflow_path = Path.join(System.tmp_dir!(), "symphony-workflow-#{System.unique_integer([:positive])}.md")
    File.write!(workflow_path, "---\nworkspace:\n  root: /tmp/workspace\n---\n")
    File.chmod!(workflow_path, 0o600)

    on_exit(fn ->
      File.rm(workflow_path)
    end)

    with_global_pause("paused\n", fn ->
      assert UnsubmittedSuccessorRetirement.execute("HGS-736", workflow_path) in [
               {:error, :untrusted_workflow_file},
               {:error, :managed_runtime_disabled}
             ]
    end)
  end

  test "Mix task rejects missing workflow instead of starting the service" do
    assert_raise Mix.Error, ~r/trusted_workflow_path_required/, fn ->
      RetireUnsubmittedSuccessor.run(["HGS-736"])
    end
  end

  test "Mix task reports malformed arguments with usage" do
    assert_raise Mix.Error, ~r/usage: mix symphony\.retire_unsubmitted_successor/, fn ->
      RetireUnsubmittedSuccessor.run([])
    end
  end

  defp with_global_pause(contents, callback) do
    key = "SYMPHONY_GLOBAL_PAUSE_FILE"
    previous_path = System.get_env(key)
    pause_path = Path.join(System.tmp_dir!(), "symphony-pause-#{System.unique_integer([:positive])}")

    if is_binary(contents), do: File.write!(pause_path, contents), else: File.rm(pause_path)
    if is_binary(contents), do: System.put_env(key, pause_path), else: System.delete_env(key)

    try do
      callback.()
    after
      restore_env(key, previous_path)
      File.rm(pause_path)
    end
  end

  defp restore_env(key, nil), do: System.delete_env(key)
  defp restore_env(key, value), do: System.put_env(key, value)
end
