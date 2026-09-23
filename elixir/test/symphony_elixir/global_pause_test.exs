defmodule SymphonyElixir.GlobalPauseTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.GlobalPause

  setup do
    previous_path = System.get_env("SYMPHONY_GLOBAL_PAUSE_FILE")
    root = Path.join(System.tmp_dir!(), "symphony-global-pause-#{System.unique_integer([:positive])}")
    File.mkdir_p!(root)
    path = Path.join(root, "global-mutable-pause.state")
    System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", path)

    on_exit(fn ->
      if is_binary(previous_path) do
        System.put_env("SYMPHONY_GLOBAL_PAUSE_FILE", previous_path)
      else
        System.delete_env("SYMPHONY_GLOBAL_PAUSE_FILE")
      end

      File.rm_rf(root)
    end)

    %{path: path, transition: Path.join(root, "global-mutable-pause.transition")}
  end

  test "missing state is configured and paused", %{path: path} do
    assert GlobalPause.snapshot() == %{
             configured?: true,
             paused?: true,
             state: "paused",
             path: Path.expand(path),
             reason: "enoent"
           }
  end

  test "only exact running state permits admission", %{path: path} do
    File.write!(path, "paused\n")
    assert GlobalPause.paused?()

    File.write!(path, "running\n")
    refute GlobalPause.paused?()
    assert GlobalPause.snapshot().state == "running"

    File.write!(path, "running-now\n")
    assert GlobalPause.paused?()
    assert GlobalPause.snapshot().reason == "invalid_pause_file_state"
  end

  test "a root transition epoch pauses admission and is echoed by the barrier", %{path: path, transition: transition} do
    epoch = String.duplicate("a", 32)
    File.write!(path, "running\n")
    File.write!(transition, "pausing:#{epoch}\n")

    assert %{
             configured?: true,
             paused?: true,
             state: "paused",
             reason: "pause_transition",
             transition_epoch: ^epoch
           } = GlobalPause.snapshot()

    File.write!(path, "paused\n")
    assert GlobalPause.snapshot().transition_epoch == epoch
    File.rm!(transition)
    assert GlobalPause.snapshot().reason == "operator_paused"
  end

  test "malformed or non-regular transition markers fail closed", %{path: path, transition: transition} do
    File.write!(path, "running\n")
    File.write!(transition, "pausing:wrong\n")
    assert GlobalPause.snapshot().reason == "invalid_pause_transition"
    assert GlobalPause.paused?()

    File.rm!(transition)
    File.mkdir!(transition)
    assert GlobalPause.snapshot().reason == "invalid_pause_transition"
    assert GlobalPause.paused?()

    if match?({:unix, _}, :os.type()) do
      File.rmdir!(transition)
      File.ln_s!(path, transition)
      assert GlobalPause.snapshot().reason == "invalid_pause_transition"
      assert GlobalPause.paused?()
    end
  end

  test "unset path is explicitly reported as unconfigured" do
    System.delete_env("SYMPHONY_GLOBAL_PAUSE_FILE")

    assert GlobalPause.snapshot() == %{
             configured?: false,
             paused?: false,
             state: "unconfigured",
             path: nil,
             reason: "missing_pause_file_path"
           }
  end
end
