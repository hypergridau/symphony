defmodule SymphonyElixir.ManagedExecutorAbortResultJournalTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedExecutor.AbortResultJournal

  setup do
    if match?({:win32, _}, :os.type()), do: Process.put(:abort_result_journal_windows_test_only, true)

    root = Path.join(File.cwd!(), ".tmp-abort-result-#{System.unique_integer([:positive])}")
    :ok = File.mkdir_p(root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(root, 0o700)
    on_exit(fn -> File.rm_rf(root) end)
    %{root: root}
  end

  test "stores and replays the exact blocked result bytes", %{root: root} do
    bytes = ~s({"outcome":"blocked","summary":"private"}) <> <<0, 255>>
    expected_hash = sha256(bytes)

    assert {:ok, %{reference: "blocked-result-1", sha256: ^expected_hash}} =
             AbortResultJournal.record(root, "blocked-result-1", authority(), bytes)

    assert {:ok, %{reference: "blocked-result-1", sha256: ^expected_hash}} =
             AbortResultJournal.record(root, "blocked-result-1", authority(), bytes)

    assert {:ok, ^bytes} = AbortResultJournal.load(root, "blocked-result-1", authority(), expected_hash)
  end

  test "holds changed bytes or authority under an existing reference", %{root: root} do
    bytes = "blocked result"
    assert {:ok, %{sha256: digest}} = AbortResultJournal.record(root, "same-ref", authority(), bytes)

    assert {:held, :abort_result_journal_conflict} =
             AbortResultJournal.record(root, "same-ref", authority(), "changed result")

    variants = [
      %{authority() | assignment_digest: String.duplicate("c", 64)},
      %{authority() | issue_uuid: "c60d9711-d8ed-4a69-8910-570d0b4bbe7a"},
      %{authority() | generation: 9},
      %{authority() | allocation_id: "allocation-9"}
    ]

    for changed <- variants do
      assert {:held, :abort_result_journal_invalid} = AbortResultJournal.load(root, "same-ref", changed, digest)
    end
  end

  test "detects tampering and a mismatched caller hash", %{root: root} do
    bytes = "blocked result"
    assert {:ok, %{sha256: digest}} = AbortResultJournal.record(root, "tamper-ref", authority(), bytes)
    path = path(root, "tamper-ref")
    :ok = File.write(path, "tampered")
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(path, 0o600)

    assert {:held, :abort_result_journal_invalid} = AbortResultJournal.load(root, "tamper-ref", authority(), digest)
    assert {:held, :abort_result_journal_invalid} = AbortResultJournal.load(root, "tamper-ref", authority(), sha256("wrong"))
  end

  test "rejects duplicate JSON keys in a stored record", %{root: root} do
    assert {:ok, %{sha256: digest}} = AbortResultJournal.record(root, "duplicate-key", authority(), "blocked")
    path = path(root, "duplicate-key")
    {:ok, original} = File.read(path)
    duplicated = String.replace(original, "\"schema_version\":1", "\"schema_version\":1,\"schema_version\":1", global: false)
    :ok = File.write(path, duplicated)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(path, 0o600)

    assert {:held, :abort_result_journal_invalid} = AbortResultJournal.load(root, "duplicate-key", authority(), digest)
  end

  test "retains a partial write as a recovery hold", %{root: root} do
    path = path(root, "partial-ref")
    :ok = File.write(path, "{")
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(path, 0o600)

    assert {:held, :abort_result_journal_invalid} =
             AbortResultJournal.load(root, "partial-ref", authority(), sha256("blocked"))

    assert {:held, :abort_result_journal_conflict} =
             AbortResultJournal.record(root, "partial-ref", authority(), "blocked")

    assert {:ok, "{"} = File.read(path)
  end

  test "rejects a linked record and an unsafe root", %{root: root} do
    if match?({:unix, _}, :os.type()) do
      target = Path.join(root, "outside.json")
      :ok = File.write(target, "private")
      :ok = File.ln_s(target, path(root, "linked-ref"))

      assert {:held, :abort_result_journal_read_unavailable} =
               AbortResultJournal.record(root, "linked-ref", authority(), "blocked")

      assert {:held, :abort_result_journal_read_unavailable} =
               AbortResultJournal.load(root, "linked-ref", authority(), sha256("blocked"))

      assert {:ok, "private"} = File.read(target)

      assert {:error, :invalid_abort_result_journal_root} =
               AbortResultJournal.record(Path.join(root, "missing"), "new-ref", authority(), "blocked")

      root_link = root <> "-link"
      :ok = File.ln_s(root, root_link)

      assert {:error, :invalid_abort_result_journal_root} =
               AbortResultJournal.record(root_link, "new-ref", authority(), "blocked")

      :ok = File.rm(root_link)
    end

    assert {:error, :invalid_abort_result_journal_root} =
             AbortResultJournal.record("relative", "new-ref", authority(), "blocked")
  end

  test "rejects a journal below a group or world writable ancestor" do
    if match?({:unix, _}, :os.type()) do
      parent = Path.join(File.cwd!(), ".tmp-abort-result-parent-#{System.unique_integer([:positive])}")
      root = Path.join(parent, "private")
      :ok = File.mkdir_p(root)
      :ok = File.chmod(parent, 0o777)
      :ok = File.chmod(root, 0o700)
      on_exit(fn -> File.rm_rf(parent) end)

      assert {:error, :invalid_abort_result_journal_root} =
               AbortResultJournal.record(root, "unsafe-ancestor", authority(), "blocked")
    end
  end

  test "rejects incomplete authority identities", %{root: root} do
    invalid = %{authority() | allocation_id: nil}

    assert {:error, :invalid_abort_result_journal_record} =
             AbortResultJournal.record(root, "valid-ref", invalid, "blocked")

    assert {:error, :invalid_abort_result_journal_record} =
             AbortResultJournal.record(root, "../escape", authority(), "blocked")
  end

  test "holds missing, oversized, and insecure records", %{root: root} do
    assert :missing = AbortResultJournal.load(root, "missing-record", authority(), sha256("blocked"))

    assert {:error, :invalid_abort_result_journal_record} =
             AbortResultJournal.record(root, "empty-result", authority(), "")

    oversized = String.duplicate("x", 65_537)

    assert {:error, :invalid_abort_result_journal_record} =
             AbortResultJournal.record(root, "oversized-result", authority(), oversized)

    assert {:ok, %{sha256: digest}} = AbortResultJournal.record(root, "insecure-record", authority(), "blocked")

    if match?({:unix, _}, :os.type()) do
      :ok = File.chmod(path(root, "insecure-record"), 0o644)

      assert {:held, :abort_result_journal_read_unavailable} =
               AbortResultJournal.load(root, "insecure-record", authority(), digest)

      assert {:held, :abort_result_journal_read_unavailable} =
               AbortResultJournal.record(root, "insecure-record", authority(), "blocked")

      :ok = File.chmod(root, 0o755)

      assert {:error, :invalid_abort_result_journal_root} =
               AbortResultJournal.record(root, "non-private-root", authority(), "blocked")
    end
  end

  defp authority do
    %{
      assignment_digest: String.duplicate("a", 64),
      issue_uuid: "b60d9711-d8ed-4a69-8910-570d0b4bbe7a",
      generation: 8,
      allocation_id: "allocation-8"
    }
  end

  defp path(root, reference), do: Path.join(root, sha256(reference) <> ".abort-result.json")
  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
