defmodule SymphonyElixir.RootFixtures.ConfirmedRecoveryTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliationHost, as: EpochHost
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost, as: RootHost
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransaction, as: Transaction
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWAL

  @issue_id "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"
  @issuer_issue_id "33333333-3333-4333-8333-333333333333"
  @issuer_denial_issue_id "44444444-4444-4444-8444-444444444444"
  @issuer_replay_issue_id "55555555-5555-4555-8555-555555555555"
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

  test "epoch issuance survives every publication crash prefix without replacing its signature" do
    for prefix <- 0..3 do
      issue_id = "66666666-6666-4666-8666-66666666666#{prefix}"
      directory = RootHost.marker_directory(issue_id)
      epoch = Path.join(directory, "reconciliation/epoch-1")
      File.mkdir_p!(epoch)
      for path <- [Path.dirname(directory), directory, Path.dirname(epoch), epoch], do: File.chmod!(path, 0o700)
      Process.put(:epoch_writes, 0)

      write = fn path, bytes ->
        count = Process.get(:epoch_writes)

        if count == prefix do
          {:error, :synthetic_crash}
        else
          Process.put(:epoch_writes, count + 1)
          RootHost.exclusive_durable_write_for_test(path, bytes)
        end
      end

      sync = fn _directory -> :ok end
      first = EpochHost.persist(issue_id, "candidate", "envelope", write, sync)
      assert first == if(prefix == 3, do: :ok, else: {:error, :issuer_output_conflict})
      assert :ok = EpochHost.persist(issue_id, "candidate", "envelope", &RootHost.exclusive_durable_write_for_test/2, sync)
      assert :ok = EpochHost.persist(issue_id, "candidate", "envelope", &RootHost.exclusive_durable_write_for_test/2, sync)
      assert File.read!(Path.join(epoch, "issued-envelope.json")) == "envelope"
      assert File.read!(Path.join(directory, "candidate.json")) == "candidate"
      assert File.read!(Path.join(directory, "confirmed-root-envelope.json")) == "envelope"

      assert {:error, :issuer_output_conflict} =
               EpochHost.persist(issue_id, "candidate", "different-envelope", &RootHost.exclusive_durable_write_for_test/2, sync)

      assert File.read!(Path.join(epoch, "issued-envelope.json")) == "envelope"
    end
  end

  test "epoch private reader rejects writable files and symlinks on the actual filesystem" do
    directory = Path.join(@fixture_root, "epoch-private-reader")
    File.mkdir_p!(directory)
    File.chmod!(directory, 0o700)
    path = Path.join(directory, "evidence.json")
    write_root_private(path, "{}")
    assert {:ok, "{}"} = EpochHost.read_private(path, 10)
    File.chmod!(path, 0o644)
    assert {:error, :untrusted_reconciliation_file} = EpochHost.read_private(path, 10)
    link = Path.join(directory, "redirect.json")
    File.ln_s!(path, link)
    assert {:error, :untrusted_reconciliation_file} = EpochHost.read_private(link, 10)
    File.chmod!(path, 0o600)
    assert {:error, :untrusted_reconciliation_file} = EpochHost.read_private(path, 1)
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

  test "issuer accepts collector inputs in its existing directory and writes immutable outputs" do
    directory = Transaction.marker_directory(@issuer_issue_id)
    File.mkdir_p!(directory)
    File.chmod!(Path.dirname(directory), 0o700)
    File.chmod!(directory, 0o700)

    write_root_private(Path.join(directory, "reviewed-preflight.json"), "{}\n")
    write_root_private(Path.join(directory, "provider-held-readback.json"), "{\"held\":true}\n")
    write_root_private(Path.join(directory, "issuer-input.json"), "{\"bundle\":true}\n")

    input_path = Path.join(directory, "issuer-input.json")
    assert {:ok, "{\"bundle\":true}\n"} = RootHost.read_issuer_bundle(@issuer_issue_id, input_path)
    assert {:error, :untrusted_issuer_bundle} = RootHost.read_issuer_bundle(@issuer_issue_id, Path.join(directory, "foreign-input.json"))

    candidate = "{\"candidate\":true}\n"
    envelope = "{\"envelope\":true}\n"

    assert :ok = RootHost.persist_issuer_outputs(@issuer_issue_id, candidate, envelope)
    assert File.read!(Path.join(directory, "candidate.json")) == candidate
    assert File.read!(Path.join(directory, "confirmed-root-envelope.json")) == envelope
    assert_owned_root(Path.join(directory, "candidate.json"))
    assert_owned_root(Path.join(directory, "confirmed-root-envelope.json"))
  end

  test "issuer preserves collector denial without writing outputs" do
    directory = Transaction.marker_directory(@issuer_denial_issue_id)
    File.mkdir_p!(directory)
    File.chmod!(Path.dirname(directory), 0o700)
    File.chmod!(directory, 0o700)
    write_root_private(Path.join(directory, "reviewed-preflight.json"), "{}\n")
    write_root_private(Path.join(directory, "provider-held-readback.json"), "{\"held\":true}\n")
    write_root_private(Path.join(directory, "issuer-input.json"), "{\"bundle\":true}\n")
    write_root_private(Path.join(directory, "provider-held-denial.json"), "{\"denied\":true}\n")

    assert {:error, :provider_readback_denied} =
             RootHost.persist_issuer_outputs(@issuer_denial_issue_id, "candidate", "envelope")

    refute File.exists?(Path.join(directory, "candidate.json"))
    refute File.exists?(Path.join(directory, "confirmed-root-envelope.json"))
  end

  test "issuer refuses a transaction marker already present in the collector directory" do
    directory = Transaction.marker_directory(@issuer_replay_issue_id)
    File.mkdir_p!(directory)
    File.chmod!(Path.dirname(directory), 0o700)
    File.chmod!(directory, 0o700)
    write_root_private(Path.join(directory, "reviewed-preflight.json"), "{}\n")
    write_root_private(Path.join(directory, "provider-held-readback.json"), "{\"held\":true}\n")
    write_root_private(Path.join(directory, "issuer-input.json"), "{\"bundle\":true}\n")
    write_root_private(Path.join(directory, "transaction.json"), "{\"state\":\"applying\"}\n")

    assert {:error, :issuer_output_conflict} =
             RootHost.persist_issuer_outputs(@issuer_replay_issue_id, "candidate", "envelope")

    refute File.exists?(Path.join(directory, "candidate.json"))
    refute File.exists?(Path.join(directory, "confirmed-root-envelope.json"))
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

    case File.lstat(@evidence_root) do
      {:error, :enoent} ->
        assert :ok = Transaction.verify_startup(@workflow, @pool)

      {:ok, %File.Stat{type: :directory, uid: 0}} ->
        assert {:error, :hgs740_startup_held_closed} = Transaction.verify_startup(@workflow, @pool)
    end

    File.mkdir_p!(@evidence_root)
    File.chmod!(@evidence_root, 0o700)
    assert {:ok, %File.Stat{type: :directory, uid: 0, mode: evidence_mode}} = File.lstat(@evidence_root)
    assert Bitwise.band(evidence_mode, 0o777) == 0o700
    File.mkdir_p!(Path.join(@evidence_root, @issue_id))
    File.chmod!(Path.join(@evidence_root, @issue_id), 0o700)
    File.mkdir_p!(Transaction.marker_directory(@issue_id))
    File.chmod!(Transaction.marker_directory(@issue_id), 0o700)

    assert [] == File.ls!(Transaction.marker_directory(@issue_id))
    assert {:error, :hgs740_transaction_marker_missing} = Transaction.no_marker_startup_policy(@issue_id, false)
    # The public verifier intentionally collapses issue-level denials to one startup hold.
    assert {:error, :hgs740_startup_held_closed} = Transaction.verify_startup(@workflow, @pool)
    assert {:error, :untrusted_workflow_file} = Transaction.complete(@issue_id, @pool, @workflow)

    assert {:error, :untrusted_workflow_file} =
             Transaction.verify_startup(Path.join(@fixture_root, "forged.md"), @pool)

    recovery = install_recovery_workflows()
    assert {:ok, context} = RootHost.authorize_completion(@issue_id, @pool, recovery)

    assert context.runtime.execution_fence_path ==
             "/srv/dahlia-runner-state/workspaces/pools/midgard/.symphony/execution-fence.json"

    other = Path.join(Path.dirname(recovery), "grid.md")
    File.chown!(other, @owner)
    assert {:error, :untrusted_workflow_file} = RootHost.authorize_completion(@issue_id, @pool, recovery)
    File.chown!(other, 0)
    File.chmod!(other, 0o666)
    assert {:error, :untrusted_workflow_file} = RootHost.authorize_completion(@issue_id, @pool, recovery)
    File.chmod!(other, 0o644)
    bytes = File.read!(other)
    File.rm!(other)
    File.ln_s!(recovery, other)
    assert {:error, :untrusted_workflow_file} = RootHost.authorize_completion(@issue_id, @pool, recovery)
    File.rm!(other)
    File.write!(other, bytes)
    assert {:ok, _context} = RootHost.authorize_completion(@issue_id, @pool, recovery)
    install_recovery_workflows("")
    assert {:error, :missing_tracker_kind} = RootHost.authorize_completion(@issue_id, @pool, recovery)
  end

  defp install_recovery_workflows(tracker_config \\ "tracker:\n  kind: memory\n") do
    root = "/srv/dahlia-runner-state/dahlia"
    output = "config/symphony/recovery-workflows"
    pools = ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)
    renderer_path = "scripts/symphony/linux-workflow.mjs"
    renderer = workflow_entry(root, renderer_path, "synthetic canonical renderer")

    {outputs, sources} =
      Enum.map(pools, fn pool ->
        workspace = "/srv/dahlia-runner-state/workspaces/pools/" <> pool
        source = workflow_entry(root, "config/symphony/workflows/" <> pool <> ".md", "synthetic source " <> pool)
        bytes = "---\n" <> tracker_config <> "workspace:\n  root: \"" <> workspace <> "\"\n---\n"
        derived = workflow_entry(root, output <> "/" <> pool <> ".md", bytes) |> Map.delete("blob")
        {Map.merge(derived, %{"pool" => pool, "source" => source, "workspaceRoot" => workspace}), source}
      end)
      |> Enum.unzip()

    commit = String.duplicate("a", 40)
    controls = %{"schemaVersion" => 1, "sourceCommit" => commit, "files" => [renderer | sources]}

    receipt = %{
      "schemaVersion" => 1,
      "derivation" => "canonical-linux-workflow-v1",
      "sourceCommit" => commit,
      "runtimeRoot" => "/srv/dahlia-runner-state",
      "renderer" => renderer,
      "files" => outputs
    }

    workflow_entry(root, "linux-control-receipt.json", Jason.encode!(controls))
    workflow_entry(root, output <> "/recovery-workflow-receipt.json", Jason.encode!(receipt))
    Path.join([root, output, @pool <> ".md"])
  end

  defp workflow_entry(root, relative, bytes) do
    path = Path.join(root, relative)
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, bytes)
    File.chmod!(path, 0o644)
    blob = :crypto.hash(:sha, ["blob ", Integer.to_string(byte_size(bytes)), <<0>>, bytes])
    %{"path" => relative, "bytes" => byte_size(bytes), "mode" => "100644", "sha256" => sha256(bytes), "blob" => Base.encode16(blob, case: :lower)}
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

  defp write_root_private(path, bytes) do
    File.write!(path, bytes, [:binary, :exclusive])
    File.chown!(path, 0)
    File.chgrp!(path, 0)
    File.chmod!(path, 0o600)
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

  defp assert_owned_root(path) do
    assert {:ok, %File.Stat{uid: 0, gid: 0, mode: mode, links: 1}} = File.lstat(path)
    assert Bitwise.band(mode, 0o777) == 0o600
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
