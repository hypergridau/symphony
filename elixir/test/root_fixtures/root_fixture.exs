defmodule SymphonyElixir.RootFixtures.ConfirmedRecoveryTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransaction, as: Transaction
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWAL

  @issue_id "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"
  @pool "midgard"
  @workflow "/srv/dahlia-runner-state/dahlia/config/symphony/workflows/midgard.md"
  @evidence_root "/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery"
  @fixture_root "/srv/hgs740-root-fixture"
  @owner 1001

  setup_all do
    assert System.get_env("HGS740_ROOT_FIXTURE") == "1"
    assert match?({:ok, %File.Stat{uid: 0}}, File.stat("/proc/self"))
    assert {:ok, %File.Stat{type: :directory}} = File.stat("/srv")
    File.mkdir_p!(@fixture_root)
    :ok
  end

  test "production WAL replays every on-disk crash prefix and preserves exact bytes" do
    names = ~w(claimJournal fence responsibilityGraph)

    for prefix <- 0..length(names) do
      directory = Path.join(@fixture_root, "prefix-#{prefix}")
      File.mkdir_p!(directory)
      images = images(directory, names)

      for image <- images, do: write_owned(image.name, image.preimage_bytes)

      {first_result, written} = replay(images, prefix)

      if prefix == length(names) do
        assert first_result == :ok
      else
        assert first_result == {:error, :synthetic_crash}
      end

      assert written == prefix
      assert :ok == elem(replay(images, :infinity), 0)
      assert :ok == elem(replay(images, :infinity), 0)

      for image <- images do
        assert File.read!(image.name) == image.postimage_bytes
        assert_owned(image.name)
      end
    end
  end

  test "a contradictory or missing target stops disk replay before later images" do
    directory = Path.join(@fixture_root, "conflict")
    File.mkdir_p!(directory)
    [first, second] = images(directory, ~w(first second))
    write_owned(first.name, "changed-by-another-writer")
    write_owned(second.name, second.preimage_bytes)

    assert {:error, :transaction_target_conflict} == elem(replay([first, second], :infinity), 0)
    assert File.read!(first.name) == "changed-by-another-writer"
    assert File.read!(second.name) == second.preimage_bytes

    File.rm!(first.name)
    assert {:error, :enoent} == elem(replay([first, second], :infinity), 0)
    assert File.read!(second.name) == second.preimage_bytes
  end

  test "exact postimage replay repairs ownership after a save before metadata restore" do
    directory = Path.join(@fixture_root, "ownership")
    File.mkdir_p!(directory)
    [image] = images(directory, ["claimJournal"])
    write_owned(image.name, image.preimage_bytes)

    assert {:error, :synthetic_crash_after_save} ==
             ConfirmedRecoveryWAL.apply_images(
               [wal_image(image)],
               &read_current/1,
               fn path, bytes, already_applied? ->
                 refute already_applied?
                 File.write!(path, bytes)
                 File.chown!(path, 0)
                 File.chgrp!(path, 0)
                 {:error, :synthetic_crash_after_save}
               end
             )

    assert File.read!(image.name) == image.postimage_bytes
    assert {:ok, %File.Stat{uid: 0, gid: 0}} = File.stat(image.name)

    assert :ok ==
             ConfirmedRecoveryWAL.apply_images(
               [wal_image(image)],
               &read_current/1,
               fn path, _bytes, already_applied? ->
                 assert already_applied?
                 File.chown!(path, @owner)
                 File.chgrp!(path, @owner)
                 File.chmod!(path, 0o600)
                 :ok
               end
             )

    assert File.read!(image.name) == image.postimage_bytes
    assert_owned(image.name)
  end

  test "completion marker is durable before custody release and survives failures" do
    directory = Path.join(@fixture_root, "completion")
    File.mkdir_p!(directory)
    marker = Path.join(directory, "transaction.json")
    custody = Path.join(directory, "custody")
    File.mkdir_p!(custody)

    assert {:error, :synthetic_commit_failure} ==
             ConfirmedRecoveryWAL.commit_then_release(
               fn -> {:error, :synthetic_commit_failure} end,
               fn -> flunk("custody release ran before commit") end
             )

    assert {:error, :enoent} = File.read(marker)

    commit = fn ->
      File.write!(marker, "complete\n", [:sync])
      :ok
    end

    assert {:error, :synthetic_release_failure} ==
             ConfirmedRecoveryWAL.commit_then_release(commit, fn ->
               assert File.read!(marker) == "complete\n"
               {:error, :synthetic_release_failure}
             end)

    assert File.read!(marker) == "complete\n"
    assert {:ok, %File.Stat{uid: 0}} = File.stat(custody)

    assert :ok ==
             ConfirmedRecoveryWAL.commit_then_release(commit, fn ->
               assert File.read!(marker) == "complete\n"
               File.chown!(custody, @owner)
               File.chgrp!(custody, @owner)
               :ok
             end)

    assert {:ok, %File.Stat{uid: @owner, gid: @owner}} = File.stat(custody)
  end

  test "public root entry points deny untrusted paths, unpaused apply, and markerless startup" do
    assert {:error, :invalid_pool} = Transaction.apply(@issue_id, "unknown", @workflow, @issue_id)

    assert {:error, :global_gate_must_be_configured_and_paused} =
             Transaction.apply(@issue_id, @pool, @workflow, @issue_id)

    assert {:error, :untrusted_workflow_file} = Transaction.complete(@issue_id, @pool, @workflow)
    assert {:error, :untrusted_workflow_file} = Transaction.verify_startup(@workflow, @pool)

    File.mkdir_p!(Path.dirname(@workflow))
    File.write!(@workflow, "---\n---\n", [:exclusive])

    assert {:error, :untrusted_workflow_file} =
             Transaction.complete(@issue_id, @pool, Path.join(@fixture_root, "forged.md"))

    assert {:error, :enoent} = File.lstat(@evidence_root)
    assert :ok = Transaction.verify_startup(@workflow, @pool)

    File.mkdir_p!(@evidence_root)
    File.chmod!(@evidence_root, 0o700)
    File.mkdir_p!(Path.join(@evidence_root, @issue_id))
    File.chmod!(Path.join(@evidence_root, @issue_id), 0o700)
    File.mkdir_p!(Transaction.marker_directory(@issue_id))
    File.chmod!(Transaction.marker_directory(@issue_id), 0o700)

    assert [] == File.ls!(Transaction.marker_directory(@issue_id))
    assert {:error, :hgs740_transaction_marker_missing} = Transaction.no_marker_startup_policy(@issue_id, false)
    # The public verifier intentionally collapses issue-level denials to one startup hold.
    assert {:error, :hgs740_startup_held_closed} = Transaction.verify_startup(@workflow, @pool)
    assert {:error, :configured_state_path_mismatch} = Transaction.complete(@issue_id, @pool, @workflow)

    assert {:error, :untrusted_workflow_file} =
             Transaction.verify_startup(Path.join(@fixture_root, "forged.md"), @pool)
  end

  defp images(directory, names) do
    Enum.map(names, fn name ->
      preimage = "#{name}:before\n"

      %{
        name: Path.join(directory, name <> ".json"),
        preimage_bytes: preimage,
        preimage_sha256: sha256(preimage),
        postimage_bytes: "#{name}:after\n"
      }
    end)
  end

  defp replay(images, allowed_writes) do
    counter = :counters.new(1, [])

    result =
      ConfirmedRecoveryWAL.apply_images(
        Enum.map(images, &wal_image/1),
        &read_current/1,
        &persist_replay_image(&1, &2, &3, counter, allowed_writes)
      )

    {result, :counters.get(counter, 1)}
  end

  defp persist_replay_image(path, bytes, true, _counter, _allowed_writes) do
    assert File.read!(path) == bytes
    restore_owned_metadata(path)
    :ok
  end

  defp persist_replay_image(path, bytes, false, counter, allowed_writes) do
    written = :counters.get(counter, 1)

    if allowed_writes == :infinity or written < allowed_writes do
      write_owned(path, bytes)
      :counters.add(counter, 1, 1)
      :ok
    else
      {:error, :synthetic_crash}
    end
  end

  defp wal_image(image), do: Map.take(image, [:name, :preimage_sha256, :postimage_bytes])

  defp read_current(path) do
    case File.read(path) do
      {:ok, bytes} -> bytes
      {:error, _reason} = error -> error
    end
  end

  defp write_owned(path, bytes) do
    File.write!(path, bytes)
    restore_owned_metadata(path)
  end

  defp restore_owned_metadata(path) do
    File.chown!(path, @owner)
    File.chgrp!(path, @owner)
    File.chmod!(path, 0o600)
  end

  defp assert_owned(path) do
    assert {:ok, %File.Stat{uid: @owner, gid: @owner, mode: mode}} = File.stat(path)
    assert Bitwise.band(mode, 0o777) == 0o600
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
