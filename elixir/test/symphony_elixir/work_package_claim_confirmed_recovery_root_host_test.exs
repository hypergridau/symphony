defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHostTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost

  test "decodes an Ed25519 PKCS8 signer seed without accepting another key algorithm" do
    seed = :binary.copy(<<7>>, 32)
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519, seed)

    der =
      <<0x30, 0x2E, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x04, 0x22, 0x04, 0x20, seed::binary>>

    pem = "-----BEGIN PRIVATE KEY-----\n" <> Base.encode64(der) <> "\n-----END PRIVATE KEY-----\n"

    assert {:ok, decoded_private_key} = ConfirmedRecoveryRootHost.decode_private_key_for_test(pem)
    message = "HGS-740 root signer test"
    signature = :crypto.sign(:eddsa, :none, message, [decoded_private_key, :ed25519])

    assert decoded_private_key == private_key
    assert :crypto.verify(:eddsa, :none, message, signature, [public_key, :ed25519])
    assert {:error, :untrusted_recovery_key} = ConfirmedRecoveryRootHost.decode_private_key_for_test("not a key")
  end

  test "syncs the evidence directory after immutable output creation" do
    if :os.type() == {:unix, :linux} do
      root = Path.expand("../../..", __DIR__)

      assert :ok = ConfirmedRecoveryRootHost.sync_directory_for_test(root)

      assert {:error, {:directory_sync_failed, _reason}} =
               ConfirmedRecoveryRootHost.sync_directory_for_test(Path.join(System.tmp_dir!(), "hgs740-missing-directory"))
    end
  end

  test "writes issuer output exclusively with private mode and synced bytes" do
    if :os.type() == {:unix, :linux} do
      directory = Path.join(System.tmp_dir!(), "hgs740-issuer-write-#{System.unique_integer([:positive])}")
      path = Path.join(directory, "candidate.json")
      bytes = "canonical candidate bytes"
      File.mkdir!(directory)
      on_exit(fn -> File.rm_rf!(directory) end)

      result = ConfirmedRecoveryRootHost.exclusive_durable_write_for_test(path, bytes)

      expected_result =
        if match?({:ok, %File.Stat{uid: 0}}, File.stat("/proc/self")),
          do: :ok,
          else: {:error, :issuer_output_conflict}

      assert result == expected_result
      assert {:ok, %File.Stat{type: :regular, mode: mode, links: 1, size: size}} = File.lstat(path)
      assert Bitwise.band(mode, 0o777) == 0o600
      assert size == byte_size(bytes)
      assert File.read!(path) == bytes

      assert {:error, :issuer_output_conflict} =
               ConfirmedRecoveryRootHost.exclusive_durable_write_for_test(path, "replacement bytes")

      assert File.read!(path) == bytes
    end
  end

  test "checks trusted Linux ancestors without following symlinks" do
    if :os.type() == {:unix, :linux} do
      assert :ok = ConfirmedRecoveryRootHost.trusted_root_directory_for_test("/usr")
      assert {:error, :untrusted_root_directory} = ConfirmedRecoveryRootHost.trusted_root_directory_for_test("/tmp")

      link = Path.join(System.tmp_dir!(), "hgs740-untrusted-link-#{System.unique_integer([:positive])}")
      File.ln_s!("/usr", link)
      on_exit(fn -> File.rm!(link) end)

      assert {:error, :untrusted_root_directory} = ConfirmedRecoveryRootHost.trusted_root_directory_for_test(link)
    end
  end

  test "accepts trusted state ancestors and rejects a state path below a writable ancestor" do
    if :os.type() == {:unix, :linux} do
      assert ConfirmedRecoveryRootHost.trusted_state_ancestors_for_test("/usr", 0)

      directory = Path.join(System.tmp_dir!(), "hgs740-state-ancestor-#{System.unique_integer([:positive])}")
      File.mkdir!(directory)
      File.chmod!(directory, 0o700)
      on_exit(fn -> File.rm_rf!(directory) end)
      assert {:ok, %File.Stat{uid: owner}} = File.stat(directory)

      refute ConfirmedRecoveryRootHost.trusted_state_ancestors_for_test(directory, owner)
      refute ConfirmedRecoveryRootHost.trusted_state_ancestors_for_test(Path.join(directory, "missing"), owner)
    end

    assert {:error, :untrusted_pool_state_directory} =
             ConfirmedRecoveryRootHost.trusted_runtime_directories_for_test(
               %{journal_path: "/untrusted", execution_fence_path: "/untrusted", responsibility_graph_path: "/untrusted"},
               "midgard",
               1001
             )
  end

  test "process ownership and systemd parsing fail closed on ambiguous host state" do
    if :os.type() == {:unix, :linux} do
      operations = ConfirmedRecoveryRootHost.operations()
      assert {:ok, %File.Stat{uid: current_uid}} = File.stat("/proc/self")
      assert {:error, :state_owner_process_present} = operations.no_processes_for_uid.(current_uid)
      assert :ok = operations.no_processes_for_uid.(4_294_967_294)
    end

    assert {:ok, %{"ActiveState" => "inactive", "ControlGroup" => "", "MainPID" => "0"}} =
             ConfirmedRecoveryRootHost.parse_systemd_properties_for_test("ActiveState=inactive\nControlGroup=\nMainPID=0\n")

    assert {:error, :invalid_systemd_properties} =
             ConfirmedRecoveryRootHost.parse_systemd_properties_for_test("ActiveState=inactive\nActiveState=failed\nControlGroup=\nMainPID=0\n")

    assert {:error, :invalid_systemd_properties} =
             ConfirmedRecoveryRootHost.parse_systemd_properties_for_test("ActiveState=inactive\nControlGroup=\nUnknown=value\nMainPID=0\n")

    assert {:error, :invalid_systemd_properties} =
             ConfirmedRecoveryRootHost.parse_systemd_properties_for_test("ActiveState=inactive\n")
  end

  test "wires root host operations and rejects invalid issuer identities before IO" do
    operations = ConfirmedRecoveryRootHost.operations()

    assert operations.paths.evidence_root == "/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery"
    assert is_function(operations.read_issuer_bundle, 2)
    assert is_function(operations.persist_issuer_outputs, 3)
    assert is_function(operations.sign_recovery_payload, 1)
    assert is_function(operations.verify_signed_evidence, 2)
    assert is_function(operations.require_mutation_quiescent, 2)

    assert {:ok, runtime} = ConfirmedRecoveryRootHost.fixed_runtime_paths("midgard")
    assert runtime.pool_key == "midgard"
    assert runtime.journal_path == "/srv/dahlia-runner-state/run/pools/midgard/work-package.json"
    assert {:error, :invalid_pool} = ConfirmedRecoveryRootHost.fixed_runtime_paths("unknown")

    assert {:error, :invalid_issue_id} = ConfirmedRecoveryRootHost.read_issuer_bundle("invalid", "/not/read")
    assert {:error, :invalid_issue_id} = ConfirmedRecoveryRootHost.persist_issuer_outputs("invalid", "candidate", "envelope")
  end

  test "accepts collector inputs and private retained evidence but rejects denial and output markers" do
    issue_id = "33333333-3333-4333-8333-333333333333"
    operations = issuer_directory_operations(issue_id, [])

    assert :ok = ConfirmedRecoveryRootHost.issuer_input_directory_for_test(issue_id, operations)

    denial_operations = issuer_directory_operations(issue_id, ["provider-held-denial.json"])

    assert {:error, :provider_readback_denied} =
             ConfirmedRecoveryRootHost.issuer_input_directory_for_test(issue_id, denial_operations)

    replay_operations = issuer_directory_operations(issue_id, ["candidate.json"])

    assert {:error, :issuer_output_conflict} =
             ConfirmedRecoveryRootHost.issuer_input_directory_for_test(issue_id, replay_operations)

    unsafe_operations = issuer_directory_operations(issue_id, ["foreign.json"], unsafe: true)

    assert {:error, :untrusted_issuer_bundle} =
             ConfirmedRecoveryRootHost.issuer_input_directory_for_test(issue_id, unsafe_operations)
  end

  test "reads only the exact root issuer-input path with exact owner, link and mode bounds" do
    if :os.type() == {:unix, :linux} do
      issue_id = "66666666-6666-4666-8666-666666666666"
      directory = ConfirmedRecoveryRootHost.marker_directory(issue_id)
      path = Path.join(directory, "issuer-input.json")
      operations = issuer_directory_operations(issue_id, [])
      input_bytes = String.duplicate("x", 64)

      read_operations =
        Map.merge(operations, %{
          validate_directory: fn ^issue_id -> :ok end,
          read: fn ^path -> {:ok, input_bytes} end
        })

      assert {:ok, ^input_bytes} =
               ConfirmedRecoveryRootHost.read_issuer_bundle_for_test(issue_id, path, read_operations)

      assert {:error, :untrusted_issuer_bundle} =
               ConfirmedRecoveryRootHost.read_issuer_bundle_for_test(
                 issue_id,
                 Path.join(directory, "foreign.json"),
                 read_operations
               )
    end
  end

  test "retains an exclusive candidate when the second immutable output fails" do
    issue_id = "77777777-7777-4777-8777-777777777777"
    directory = ConfirmedRecoveryRootHost.marker_directory(issue_id)
    parent = self()

    operations = %{
      validate_issue: fn ^issue_id -> :ok end,
      validate_directory: fn ^issue_id -> :ok end,
      write: fn path, bytes ->
        send(parent, {:attempted_output, Path.basename(path), bytes})
        if Path.basename(path) == "candidate.json", do: :ok, else: {:error, :exclusive_create_failed}
      end,
      sync_directory: fn ^directory ->
        send(parent, {:directory_synced, directory})
        :ok
      end
    }

    assert {:error, :exclusive_create_failed} =
             ConfirmedRecoveryRootHost.persist_issuer_outputs_for_test(issue_id, "candidate", "envelope", operations)

    assert_received {:attempted_output, "candidate.json", "candidate"}
    assert_received {:attempted_output, "confirmed-root-envelope.json", "envelope"}
    refute_received {:directory_synced, _path}
  end

  test "persists both immutable outputs before syncing their directory" do
    issue_id = "88888888-8888-4888-8888-888888888888"
    directory = ConfirmedRecoveryRootHost.marker_directory(issue_id)
    Process.put(:issuer_persistence_events, [])

    operations = %{
      validate_issue: fn ^issue_id -> :ok end,
      validate_directory: fn ^issue_id -> :ok end,
      write: fn path, bytes ->
        record_issuer_persistence_event({:write, Path.basename(path), bytes})
        :ok
      end,
      sync_directory: fn ^directory ->
        record_issuer_persistence_event({:sync, directory})
        :ok
      end
    }

    assert :ok =
             ConfirmedRecoveryRootHost.persist_issuer_outputs_for_test(
               issue_id,
               "canonical candidate bytes",
               "canonical envelope bytes",
               operations
             )

    assert Process.get(:issuer_persistence_events) == [
             {:write, "candidate.json", "canonical candidate bytes"},
             {:write, "confirmed-root-envelope.json", "canonical envelope bytes"},
             {:sync, directory}
           ]
  end

  test "does not create an envelope or sync after first output denial" do
    issue_id = "99999999-9999-4999-8999-999999999999"
    directory = ConfirmedRecoveryRootHost.marker_directory(issue_id)
    parent = self()

    operations = %{
      validate_issue: fn ^issue_id -> :ok end,
      validate_directory: fn ^issue_id -> :ok end,
      write: fn path, _bytes ->
        send(parent, {:attempted_first_output, Path.basename(path)})
        {:error, :exclusive_create_failed}
      end,
      sync_directory: fn ^directory ->
        send(parent, :unexpected_sync)
        :ok
      end
    }

    assert {:error, :exclusive_create_failed} =
             ConfirmedRecoveryRootHost.persist_issuer_outputs_for_test(issue_id, "candidate", "envelope", operations)

    assert_received {:attempted_first_output, "candidate.json"}
    refute_received {:attempted_first_output, "confirmed-root-envelope.json"}
    refute_received :unexpected_sync
  end

  test "normalizes an unexpected host callback result to a closed issuer conflict" do
    issue_id = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"

    operations = %{
      validate_issue: fn ^issue_id -> :unexpected_result end,
      validate_directory: fn _issue_id -> flunk("directory validation must not run") end,
      write: fn _path, _bytes -> flunk("issuer outputs must not be written") end,
      sync_directory: fn _directory -> flunk("issuer directory must not be synced") end
    }

    assert {:error, :issuer_output_conflict} =
             ConfirmedRecoveryRootHost.persist_issuer_outputs_for_test(issue_id, "candidate", "envelope", operations)
  end

  test "reports directory sync failure after retaining both immutable outputs" do
    issue_id = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
    directory = ConfirmedRecoveryRootHost.marker_directory(issue_id)
    parent = self()

    operations = %{
      validate_issue: fn ^issue_id -> :ok end,
      validate_directory: fn ^issue_id -> :ok end,
      write: fn path, bytes ->
        send(parent, {:retained_output, Path.basename(path), bytes})
        :ok
      end,
      sync_directory: fn ^directory -> {:error, {:directory_sync_failed, :eio}} end
    }

    assert {:error, {:directory_sync_failed, :eio}} =
             ConfirmedRecoveryRootHost.persist_issuer_outputs_for_test(issue_id, "candidate", "envelope", operations)

    assert_received {:retained_output, "candidate.json", "candidate"}
    assert_received {:retained_output, "confirmed-root-envelope.json", "envelope"}
  end

  defp issuer_directory_operations(issue_id, additional_entries, options \\ []) do
    directory = ConfirmedRecoveryRootHost.marker_directory(issue_id)
    issue_directory = Path.dirname(directory)

    entries =
      ["reviewed-preflight.json", "provider-held-readback.json", "issuer-input.json", "dahlia-controls"] ++
        additional_entries

    lstat = &issuer_entry_stat(&1, issue_directory, directory, additional_entries, options)

    %{
      lstat: lstat,
      ls: fn ^directory -> {:ok, entries} end,
      trusted_root_directory: fn ^directory -> :ok end
    }
  end

  defp record_issuer_persistence_event(event) do
    Process.put(:issuer_persistence_events, Process.get(:issuer_persistence_events) ++ [event])
  end

  defp issuer_entry_stat(path, issue_directory, directory, additional_entries, options) do
    name = Path.basename(path)

    cond do
      path == issue_directory or path == directory -> {:ok, %File.Stat{type: :directory, uid: 0, mode: 0o700}}
      Path.dirname(path) != directory -> {:error, :enoent}
      name == "dahlia-controls" -> {:ok, %File.Stat{type: :directory, uid: 0, mode: 0o755}}
      name in ["reviewed-preflight.json", "provider-held-readback.json", "issuer-input.json"] -> issuer_input_stat()
      name in additional_entries -> additional_entry_stat(Keyword.get(options, :unsafe, false))
      true -> {:error, :enoent}
    end
  end

  defp issuer_input_stat, do: {:ok, %File.Stat{type: :regular, uid: 0, gid: 0, mode: 0o600, links: 1, size: 64}}

  defp additional_entry_stat(true) do
    {:ok, %File.Stat{type: :symlink, uid: 0, gid: 0, mode: 0o777, links: 1, size: 64}}
  end

  defp additional_entry_stat(false) do
    {:ok, %File.Stat{type: :regular, uid: 0, gid: 0, mode: 0o644, links: 1, size: 64}}
  end
end
