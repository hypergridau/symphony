defmodule SymphonyElixir.RetainedGrantProofTrustedFileTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RetainedGrantProof.TrustedFile

  @fixture System.get_env("HGS600_TRUSTED_FILE_FIXTURE")
  @acl_fixture System.get_env("HGS600_ACL_FIXTURE")
  @metadata [:major_device, :minor_device, :inode, :uid, :gid, :mode, :links, :size, :mtime, :ctime]

  test "invalid path and missing configuration never return bytes" do
    for path <- [nil, 7, "relative", "//root/config", "/root/../root/config", "/root//config", "/root\\config", "/root/" <> <<0>>, <<255>>] do
      assert {:error, _} = TrustedFile.load(path, String.duplicate("0", 64), 4096)
    end

    for {hash, limit} <- [{nil, 4096}, {"", 4096}, {<<255>>, 4096}, {String.duplicate("0", 64), 0}] do
      assert {:error, _} = TrustedFile.load("/missing-synthetic-config", hash, limit)
    end
  end

  test "secret seed loading fails closed for invalid installed bindings" do
    for path <- [nil, "relative", "/root/../root/seed", <<255>>] do
      assert {:error, :untrusted_retained_grant_file} = TrustedFile.load_seed(path, String.duplicate("0", 64))
    end
  end

  @tag skip: is_nil(@fixture)
  test "public root fixture never qualifies as a confidential seed" do
    path = Path.join(@fixture, "valid")
    before = metadata(path)
    assert {:error, :untrusted_retained_grant_file} = TrustedFile.load_seed(path, digest(File.read!(path)))
    assert metadata(path) == before
  end

  @tag skip: is_nil(@fixture)
  test "actual root-owned fixture accepts an exact boundary without metadata or byte changes" do
    path = Path.join(@fixture, "valid")
    bytes = File.read!(path)
    before = metadata(path)
    assert {:ok, ^bytes} = TrustedFile.load(path, digest(bytes), byte_size(bytes))
    assert {:error, _} = TrustedFile.load(path, digest(bytes), byte_size(bytes) - 1)
    assert metadata(path) == before
    assert File.read!(path) == bytes
  end

  @tag skip: is_nil(@fixture)
  test "actual ownership, mode and link fixtures fail closed" do
    hash = @fixture |> Path.join("valid") |> File.read!() |> digest()

    for name <- ["missing", "fifo", "symlink", "linked-parent/valid", "writable", "worker", "empty", "oversize", "hardlinked", "write-parent/config", "worker-parent/config"] do
      assert {:error, _} = TrustedFile.load(Path.join(@fixture, name), hash, 4096)
    end
  end

  @tag skip: is_nil(@acl_fixture)
  test "actual POSIX ACL masks reflected in group mode reject effective file and ancestor writes" do
    hash = @acl_fixture |> Path.join("file-masked") |> File.read!() |> digest()
    assert {:error, _} = TrustedFile.load(Path.join(@acl_fixture, "file-write"), hash, 4096)
    assert {:ok, _} = TrustedFile.load(Path.join(@acl_fixture, "file-masked"), hash, 4096)
    assert {:error, _} = TrustedFile.load(Path.join(@acl_fixture, "dir-write/config"), hash, 4096)
    assert {:ok, _} = TrustedFile.load(Path.join(@acl_fixture, "dir-masked/config"), hash, 4096)
  end

  defp metadata(path) do
    {:ok, stat} = File.lstat(path, time: :posix)
    Map.take(stat, @metadata)
  end

  defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end
