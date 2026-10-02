defmodule SymphonyElixir.WorkPackageClaim.DirectoryDurabilityTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryCore, as: Core
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost, as: RootHost

  test "Core writes and replaces a real marker using file sync and directory-specific fsync" do
    if :os.type() == {:unix, :linux} do
      {directory, runtime} = real_directory_fixture()
      marker = Path.join(directory, "transaction.json")
      assert :ok = Core.persist_initial_marker_with_test_context(marker, %{"status" => "applying"}, runtime)
      assert Jason.decode!(File.read!(marker)) == %{"status" => "applying"}
      applied_bytes = Jason.encode!(%{"status" => "local_applied"})
      assert :ok = Core.replace_marker_with_test_context(marker, applied_bytes, runtime)
      assert Jason.decode!(File.read!(marker)) == %{"status" => "local_applied"}
      assert File.ls!(directory) == ["transaction.json"]
      assert Bitwise.band(File.stat!(marker).mode, 0o777) == 0o600
    end
  end

  test "directory operation rejects files, missing paths and symlink redirection" do
    if :os.type() == {:unix, :linux} do
      {directory, _runtime} = real_directory_fixture()
      file = Path.join(directory, "regular-file")
      File.write!(file, "retained bytes")
      directory_link = Path.join(directory, "directory-link")
      file_link = Path.join(directory, "file-link")
      File.ln_s!(directory, directory_link)
      File.ln_s!(file, file_link)

      for path <- [file, Path.join(directory, "missing"), directory_link, file_link] do
        assert {:error, {:directory_sync_failed, _reason}} = RootHost.operations().sync_directory.(path)
      end

      assert File.read!(file) == "retained bytes"
    end
  end

  test "Core retains a file-synced marker on directory failure without raw-directory fallback" do
    if :os.type() == {:unix, :linux} do
      for reason <- [:eacces, :eio] do
        {directory, runtime} = real_directory_fixture()
        marker = Path.join(directory, "transaction.json")
        denied_sync = fn ^directory -> {:error, {:directory_sync_failed, reason}} end
        failing = put_in(runtime.host_ops.sync_directory, denied_sync)
        result = Core.persist_initial_marker_with_test_context(marker, %{"status" => "applying"}, failing)
        assert {:error, {:directory_sync_failed, ^reason}} = result
        assert Jason.decode!(File.read!(marker)) == %{"status" => "applying"}
        assert :ok = runtime.host_ops.sync_directory.(directory)
      end
    end
  end

  test "missing or malformed directory capability fails closed after retaining the marker" do
    if :os.type() == {:unix, :linux} do
      for callback <- [:missing, :invalid] do
        {directory, runtime} = real_directory_fixture()
        marker = Path.join(directory, "transaction.json")

        ops =
          case callback do
            :missing -> Map.delete(runtime.host_ops, :sync_directory)
            :invalid -> Map.put(runtime.host_ops, :sync_directory, :invalid)
          end

        invalid_runtime = %{runtime | host_ops: ops}
        result = Core.persist_initial_marker_with_test_context(marker, %{"status" => "applying"}, invalid_runtime)
        assert {:error, :invalid_host_operations} = result
        assert Jason.decode!(File.read!(marker)) == %{"status" => "applying"}
      end
    end
  end

  defp real_directory_fixture do
    root = Path.join(System.tmp_dir!(), "hgs740-directory-sync-#{System.unique_integer([:positive])}")
    directory = Path.join(root, "claim/generation-2")
    File.mkdir_p!(directory)
    on_exit(fn -> File.rm_rf!(root) end)
    operations = RootHost.operations()

    # Only custody is synthetic for an unprivileged test runner. The production
    # file write/sync/rename and directory open/fsync callbacks are unchanged.
    lstat = fn path ->
      case File.lstat(path) do
        {:ok, %File.Stat{type: :directory} = stat} -> {:ok, %{stat | uid: 0, gid: 0, mode: 0o700}}
        other -> other
      end
    end

    operations = %{operations | paths: %{operations.paths | evidence_root: root}}
    {directory, %{host_ops: Map.put(operations, :lstat, lstat)}}
  end
end
