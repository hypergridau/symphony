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

  if :os.type() == {:unix, :linux} and File.regular?("/usr/bin/flock") do
    test "the OS lock excludes a second migration and releases after completion" do
      root = Path.expand(System.tmp_dir!())
      directory = Path.join(root, "symphony-launcher-lock-#{System.unique_integer([:positive])}")
      File.mkdir_p!(directory)
      on_exit(fn -> File.rm_rf(directory) end)
      path = Path.join(directory, "pool.lock")
      File.write!(path, "")
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
      assert :ok = ManagedLauncherLock.with_exclusive_lock(path, fn -> :ok end)
    end
  end
end
