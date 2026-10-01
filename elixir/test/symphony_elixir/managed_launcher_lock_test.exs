defmodule SymphonyElixir.ManagedLauncherLockTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.ManagedLauncherLock

  @tag skip: :os.type() != {:unix, :linux}
  test "derives the launcher lock only from the canonical pool journal path" do
    assert {:ok, "/srv/dahlia-runner-state/run/pools/hypergrid-gitops.lock"} =
             ManagedLauncherLock.pool_lock_path(
               "/srv/dahlia-runner-state/run/pools/hypergrid-gitops/work-package.json",
               "hypergrid-gitops"
             )

    for {journal, pool} <- [
          {"/srv/dahlia-runner-state/run/pools/other/work-package.json", "hypergrid-gitops"},
          {"/srv/dahlia-runner-state/run/pools/hypergrid-gitops/claims.json", "hypergrid-gitops"},
          {"relative/pools/hypergrid-gitops/work-package.json", "hypergrid-gitops"},
          {"/srv/dahlia-runner-state/run/pools/hypergrid-gitops/work-package.json", "../other"}
        ] do
      assert {:error, :untrusted_pool_launcher_lock_path} = ManagedLauncherLock.pool_lock_path(journal, pool)
    end
  end

  test "accepts only a loaded inactive service with no cgroup or main process" do
    assert ManagedLauncherLock.stopped_unit_properties?("LoadState=loaded\nActiveState=inactive\nControlGroup=\nMainPID=0\n")

    assert ManagedLauncherLock.stopped_unit_properties?("LoadState=loaded\nActiveState=inactive\nControlGroup=\n")

    refute ManagedLauncherLock.stopped_unit_properties?("LoadState=loaded\nActiveState=active\nControlGroup=/system.slice/example\nMainPID=42\n")

    refute ManagedLauncherLock.stopped_unit_properties?("LoadState=loaded\nActiveState=inactive\nControlGroup=/system.slice/example\nMainPID=0\n")

    refute ManagedLauncherLock.stopped_unit_properties?("LoadState=not-found\nActiveState=inactive\nControlGroup=\n")

    refute ManagedLauncherLock.stopped_unit_properties?("LoadState=loaded\nActiveState=inactive\nControlGroup=\nMainPID=0\nUnexpected=value\n")

    refute ManagedLauncherLock.stopped_unit_properties?("LoadState=loaded\nActiveState=inactive\nControlGroup=\nMainPID=0\nMainPID=0\n")
  end

  @tag skip: :os.type() != {:unix, :linux}
  test "state custody requires runner-owned private single-link files and protected directories" do
    root = Path.expand(System.tmp_dir!())
    directory = Path.join(root, "symphony-custody-#{System.unique_integer([:positive])}")
    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf(directory) end)

    path = Path.join(directory, "state.json")
    File.write!(path, "{}")
    File.chmod!(path, 0o600)
    stat = File.lstat!(path)
    runner_uid = stat.uid

    assert ManagedLauncherLock.trusted_regular_metadata?(stat, runner_uid)
    refute ManagedLauncherLock.trusted_regular_metadata?(stat, runner_uid + 1)

    File.chmod!(path, 0o640)
    refute ManagedLauncherLock.trusted_regular_metadata?(File.lstat!(path), runner_uid)
    File.chmod!(path, 0o600)

    linked_path = Path.join(directory, "state-hardlink.json")
    File.ln!(path, linked_path)
    refute ManagedLauncherLock.trusted_regular_metadata?(File.lstat!(path), runner_uid)

    directory_stat = File.lstat!(directory)
    assert ManagedLauncherLock.trusted_directory_metadata?(directory_stat, runner_uid)
    File.chmod!(directory, 0o777)
    refute ManagedLauncherLock.trusted_directory_metadata?(File.lstat!(directory), runner_uid)
  end

  if :os.type() == {:unix, :linux} and File.regular?("/usr/bin/flock") do
    test "the OS lock excludes a second migration and releases after completion" do
      root = Path.expand(System.tmp_dir!())
      directory = Path.join(root, "symphony-launcher-lock-#{System.unique_integer([:positive])}")
      File.mkdir_p!(directory)
      on_exit(fn -> File.rm_rf(directory) end)
      path = Path.join(directory, "pool.lock")
      File.write!(path, "")
      File.chmod!(path, 0o600)
      parent = self()

      holder =
        Task.async(fn ->
          ManagedLauncherLock.with_exclusive_lock(path, fn ->
            send(parent, :launcher_lock_held)

            receive do
              :release_launcher_lock -> :released
            after
              5_000 -> :timeout
            end
          end)
        end)

      assert_receive :launcher_lock_held, 2_000
      assert {:error, _reason} = ManagedLauncherLock.with_exclusive_lock(path, fn -> flunk("second lock entered") end)
      send(holder.pid, :release_launcher_lock)
      assert :released = Task.await(holder, 2_000)
      assert {:ok, :applied} = ManagedLauncherLock.with_exclusive_lock(path, fn -> {:ok, :applied} end)
      assert :ok = ManagedLauncherLock.with_exclusive_lock(path, fn -> :ok end)

      File.chmod!(path, 0o644)

      assert {:error, :pool_launcher_lock_file_untrusted} =
               ManagedLauncherLock.with_exclusive_lock(path, fn -> flunk("unsafe lock file entered") end)
    end
  end
end
