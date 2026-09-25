Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.ManagedResponsibilityManifestTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedResponsibility.Manifest
  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture

  test "absent configuration preserves legacy delegation and partial configuration fails" do
    assert {:ok, nil} = Manifest.load(%{}, 1)

    assert {:error, :managed_delegation_manifest_required} =
             Manifest.load(%{"SYMPHONY_POOL_KEY" => "test-pool"}, 1)

    assert {:error, :managed_delegation_manifest_required} =
             Manifest.load(%{"SYMPHONY_REPOSITORY_REF" => "openai/symphony"}, 1)

    assert {:error, :incomplete_managed_delegation_config} =
             Manifest.load(%{"DAHLIA_MANAGED_DELEGATION_PATH" => "/manifest.json"}, 1)

    assert {:error, :incomplete_managed_delegation_config} =
             Manifest.load(%{"DAHLIA_MANAGED_DELEGATION_SHA256" => String.duplicate("0", 64)}, 1)

    assert {:error, :managed_delegation_manifest_required} =
             Manifest.load(%{"DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519" => String.duplicate("0", 64)}, 1)
  end

  test "Ed25519 authorization binds exact bytes, trusted key and signing purpose" do
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519)
    {other_public, _} = :crypto.generate_key(:eddsa, :ed25519)
    bytes = ~s({"schema_version":1,"entries":[]})
    signature = sign(bytes, private_key)

    assert {:ok, digest(public_key)} == Manifest.verify_signature(bytes, signature, hex(public_key))

    assert {:error, :invalid_managed_delegation_signature} ==
             Manifest.verify_signature(bytes <> " ", signature, hex(public_key))

    assert {:error, :invalid_managed_delegation_signature} ==
             Manifest.verify_signature(bytes, signature, hex(other_public))

    wrong_purpose = :crypto.sign(:eddsa, :none, "other-purpose\0" <> bytes, [private_key, :ed25519])

    assert {:error, :invalid_managed_delegation_signature} ==
             Manifest.verify_signature(bytes, hex(wrong_purpose), hex(public_key))

    assert {:error, :invalid_managed_delegation_signature} ==
             Manifest.verify_signature(bytes, String.duplicate("0", 128), hex(public_key))

    assert {:error, :invalid_managed_delegation_signature} ==
             Manifest.verify_signature(bytes, signature, "not-a-key")
  end

  describe "root-owned Linux file qualification" do
    @describetag skip: System.get_env("SYMPHONY_TEST_ROOT_MANIFEST_FILES") != "1"

    setup do
      assert {:unix, :linux} = :os.type()
      assert {"0\n", 0} = System.cmd("id", ["-u"])
      root = Path.join(System.tmp_dir!(), "managed-manifest-#{System.unique_integer([:positive])}")
      File.mkdir_p!(root)
      path = Path.join(root, "manifest.json")
      now = System.system_time(:millisecond)
      bytes = Jason.encode!(Fixture.payload(now))
      {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519)
      File.write!(path, bytes)
      File.chmod!(path, 0o644)
      assert File.stat!(path).uid == 0

      env = %{
        "DAHLIA_MANAGED_DELEGATION_PATH" => path,
        "DAHLIA_MANAGED_DELEGATION_SHA256" => digest(bytes),
        "DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519" => sign(bytes, private_key),
        "DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519" => hex(public_key),
        "SYMPHONY_POOL_KEY" => "test-pool",
        "DAHLIA_RUNNER_ID" => "runner-test",
        "SYMPHONY_REPOSITORY_REF" => "openai/symphony",
        "DAHLIA_MANAGED_PROJECT_PROFILE_ID" => "profile-test"
      }

      on_exit(fn -> File.rm_rf!(root) end)
      %{root: root, path: path, env: env, now: now, private_key: private_key}
    end

    test "loads a pinned regular file without activating queued grants", %{env: env, now: now} do
      assert {:ok, manifest} = Manifest.load(env, now)
      assert manifest.source_sha256 == env["DAHLIA_MANAGED_DELEGATION_SHA256"]
      assert manifest.signer_key_sha256 == digest(Base.decode16!(env["DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519"], case: :lower))
      assert length(manifest.entries) == 2
      refute Map.has_key?(manifest, :delegations)
    end

    test "rejects changed bytes and group-writable authorization", %{path: path, env: env, now: now} do
      File.write!(path, " ", [:append])
      assert {:error, _} = Manifest.load(env, now)
      matching = Map.put(env, "DAHLIA_MANAGED_DELEGATION_SHA256", digest(File.read!(path)))
      File.chmod!(path, 0o664)
      assert {:error, _} = Manifest.load(matching, now)
    end

    test "rejects missing, altered, wrong-key and wrong-context signatures", %{path: path, env: env, now: now, private_key: private_key} do
      assert {:error, :incomplete_managed_delegation_config} =
               Manifest.load(Map.delete(env, "DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519"), now)

      assert {:error, _} =
               Manifest.load(Map.put(env, "DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519", String.duplicate("0", 128)), now)

      {wrong_public, _} = :crypto.generate_key(:eddsa, :ed25519)

      assert {:error, _} =
               Manifest.load(Map.put(env, "DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519", hex(wrong_public)), now)

      bytes = File.read!(path)
      wrong_context = :crypto.sign(:eddsa, :none, "other-purpose\0" <> bytes, [private_key, :ed25519])

      assert {:error, _} =
               Manifest.load(Map.put(env, "DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519", hex(wrong_context)), now)

      changed = bytes <> " "
      File.write!(path, changed)

      assert {:error, _} =
               Manifest.load(Map.put(env, "DAHLIA_MANAGED_DELEGATION_SHA256", digest(changed)), now)
    end

    test "rejects file and directory symlinks", %{root: root, path: path, env: env, now: now} do
      link = Path.join(root, "link.json")
      File.ln_s!(path, link)
      assert {:error, _} = Manifest.load(Map.put(env, "DAHLIA_MANAGED_DELEGATION_PATH", link), now)
      directory = Path.join(root, "linked-directory")
      File.ln_s!(root, directory)
      linked_path = Path.join(directory, "manifest.json")
      assert {:error, _} = Manifest.load(Map.put(env, "DAHLIA_MANAGED_DELEGATION_PATH", linked_path), now)
    end

    test "rejects oversize, malformed JSON and missing files", %{path: path, env: env, now: now} do
      for bytes <- [String.duplicate(" ", 262_145), "not-json"] do
        File.write!(path, bytes)
        matching = Map.put(env, "DAHLIA_MANAGED_DELEGATION_SHA256", digest(bytes))
        assert {:error, _} = Manifest.load(matching, now)
      end

      missing = Map.put(env, "DAHLIA_MANAGED_DELEGATION_PATH", path <> ".missing")
      assert {:error, _} = Manifest.load(missing, now)
    end
  end

  defp digest(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
  defp hex(bytes), do: Base.encode16(bytes, case: :lower)

  defp sign(bytes, private_key) do
    :crypto.sign(:eddsa, :none, "hypergrid.symphony.managed-delegation.v1\0" <> bytes, [private_key, :ed25519]) |> hex()
  end
end
