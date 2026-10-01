defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliationTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedEpoch, as: FailedEpoch
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuance, as: Issuance
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliation, as: Epoch
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliationHost, as: EpochHost
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost, as: Host

  @directory "/synthetic/generation-2"
  @issue "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"

  test "successor custody refuses signed predecessors and any existing or unreadable successor on downgrade" do
    inputs = ~w(started.json reviewed-preflight.json provider-held-readback.json manifest.json issuer-input.json)

    operations = %{
      trusted: fn _ -> :ok end,
      lstat: fn _ -> {:ok, %File.Stat{mode: 0o700}} end,
      ls: fn _ -> {:ok, inputs} end
    }

    assert :ok = EpochHost.successor_custody_for_test("epoch-2", operations)
    signed = %{operations | ls: fn _ -> {:ok, ["issued-envelope.json" | inputs]} end}
    assert {:error, :invalid_reconciliation_epoch} = EpochHost.successor_custody_for_test("epoch-2", signed)
    inaccessible = %{operations | trusted: fn _ -> {:error, :untrusted} end}
    assert {:error, :invalid_reconciliation_epoch} = EpochHost.successor_custody_for_test("epoch-2", inaccessible)
    assert {:error, :reconciliation_epoch_downgrade} = EpochHost.successor_custody_for_test("epoch-1", operations)
    absent = %{operations | lstat: fn _ -> {:error, :enoent} end}
    assert :ok = EpochHost.successor_custody_for_test("epoch-1", absent)
    uncertain = %{operations | lstat: fn _ -> {:error, :eacces} end}
    assert {:error, :reconciliation_epoch_downgrade} = EpochHost.successor_custody_for_test("epoch-1", uncertain)
    write = fn _, _ -> flunk("invalid candidate published") end
    sync = fn _ -> flunk("invalid candidate synced") end
    assert {:error, :issuer_output_conflict} = EpochHost.persist(@issue, "invalid", "unused", write, sync)
    assert {:error, :issuer_output_conflict} = EpochHost.persist(@issue, "{}", "unused", write, sync)
  end

  test "successor binding permits only the pinned unsigned predecessor and failure seal" do
    malformed = %{"reconciliation" => %{"epoch" => "epoch-3"}}
    assert {:error, :invalid_reconciliation_epoch} = EpochHost.verify(malformed)
    assert {:error, :invalid_reconciliation_epoch} = EpochHost.verify_envelope(malformed, "unused")
    {observation, files, hashes} = fixture()
    metadata = observation["reconciliation"]

    successor =
      metadata
      |> Map.put("epoch", "epoch-2")
      |> Map.put("contractVersion", "hgs740-reconciliation-observation.v2")
      |> Map.put("predecessorEpoch", FailedEpoch.predecessor_binding())

    observation = Map.put(observation, "reconciliation", successor)
    assert FailedEpoch.valid?(successor)
    refute FailedEpoch.valid?(Map.put(successor, "predecessorEpoch", %{}))

    assert {:error, :invalid_reconciliation_epoch} =
             Epoch.verify_test_epoch(observation, @directory, fn path, _ -> Map.fetch(files, path) end, hashes)

    for change <- [%{"epoch" => "epoch-3"}, %{"contractVersion" => "hgs740-reconciliation-observation.v1"}, %{"predecessorEpoch" => %{}}, %{"replacement" => true}] do
      changed = Map.put(observation, "reconciliation", Map.merge(successor, change))
      assert {:error, :invalid_reconciliation_epoch} = Epoch.validate(changed)
    end

    pinned = FailedEpoch.predecessor_binding()["evidenceSHA256"]
    synthetic = Map.new(pinned, fn {name, _hash} -> {name, "synthetic " <> name} end)
    pin_hashes = Map.new(synthetic, fn {name, bytes} -> {name, digest(bytes)} end)
    seal = "synthetic failure seal"
    binding = %{"epoch" => "epoch-1", "evidenceSHA256" => pin_hashes, "failureSealSHA256" => digest(seal)}
    metadata = %{"predecessorEpoch" => binding}
    read = fn path, _maximum -> {:ok, Map.get(synthetic, Path.basename(path), seal)} end
    assert :ok = FailedEpoch.verify_for_test(metadata, @directory, read, pin_hashes, digest(seal))
    assert {:error, _} = FailedEpoch.verify_for_test(%{}, @directory, read, pin_hashes, digest(seal))
    assert {:error, _} = FailedEpoch.verify(metadata, @directory, read)

    for name <- Map.keys(synthetic) do
      changed = fn path, maximum ->
        if Path.basename(path) == name, do: {:ok, "changed"}, else: read.(path, maximum)
      end

      missing = fn path, maximum ->
        if Path.basename(path) == name, do: {:error, :enoent}, else: read.(path, maximum)
      end

      assert {:error, _} = FailedEpoch.verify_for_test(metadata, @directory, changed, pin_hashes, digest(seal))
      assert {:error, _} = FailedEpoch.verify_for_test(metadata, @directory, missing, pin_hashes, digest(seal))
    end

    changed_seal = fn _, _ -> {:ok, "changed seal"} end
    assert {:error, _} = FailedEpoch.verify_for_test(metadata, @directory, changed_seal, pin_hashes, digest(seal))
  end

  test "issuer output barriers preserve collector denial before transaction conflict" do
    trusted = fn _ -> :ok end
    assert :ok = Host.issuer_output_barriers_for_test(%{trusted: trusted, ls: fn _ -> {:ok, []} end})

    assert :ok =
             Host.issuer_output_barriers_for_test(%{
               trusted: trusted,
               ls: fn _ -> {:ok, ~w(candidate.json confirmed-root-envelope.json)} end
             })

    assert {:error, :issuer_output_conflict} =
             Host.issuer_output_barriers_for_test(%{
               trusted: trusted,
               ls: fn _ -> {:ok, ["transaction.json"]} end
             })

    assert {:error, :provider_readback_denied} =
             Host.issuer_output_barriers_for_test(%{
               trusted: trusted,
               ls: fn _ -> {:ok, ~w(provider-held-denial.json transaction.json)} end
             })
  end

  test "issuance resumes only after quiescence and never reads new signing input on replay" do
    context = %{
      runtime: %{journal_path: "/fixed/journal"},
      host_ops: %{
        lstat: fn _ -> {:ok, %File.Stat{type: :regular, uid: 1001, links: 1}} end,
        require_mutation_quiescent: fn _, uid ->
          assert uid == 1001
          :ok
        end,
        resume_reconciliation: fn _, path ->
          assert path == "/fixed/input"
          :ok
        end,
        read_issuer_bundle: fn _, _ -> flunk("replay must not obtain another signing input") end
      }
    }

    assert :ok = Issuance.issue_locked_for_test(context, "/fixed/input")
    expired = put_in(context, [:host_ops, :resume_reconciliation], fn _, _ -> {:error, :expired} end)
    assert {:error, :expired} = Issuance.issue_locked_for_test(expired, "/fixed/input")
    active = put_in(context, [:host_ops, :require_mutation_quiescent], fn _, _ -> {:error, :active} end)
    assert {:error, :active} = Issuance.issue_locked_for_test(active, "/fixed/input")
  end

  test "fixed directory requires every input and allows only the saved envelope afterward" do
    required = ~w(started.json reviewed-preflight.json provider-held-readback.json manifest.json issuer-input.json)

    operations = %{
      trusted: fn _ -> :ok end,
      lstat: fn _ -> {:ok, %File.Stat{mode: 0o700}} end,
      ls: fn _ -> {:ok, required} end
    }

    assert :ok = EpochHost.require_directory_for_test(operations)
    assert :ok = EpochHost.require_directory_for_test(%{operations | ls: fn _ -> {:ok, ["issued-envelope.json" | required]} end})

    for name <- required do
      missing = %{operations | ls: fn _ -> {:ok, List.delete(required, name)} end}
      assert {:error, :invalid_reconciliation_epoch} = EpochHost.require_directory_for_test(missing)
    end

    unexpected = %{operations | ls: fn _ -> {:ok, ["replacement.json" | required]} end}
    exposed = %{operations | lstat: fn _ -> {:ok, %File.Stat{mode: 0o755}} end}
    assert {:error, :invalid_reconciliation_epoch} = EpochHost.require_directory_for_test(unexpected)
    assert {:error, :invalid_reconciliation_epoch} = EpochHost.require_directory_for_test(exposed)
  end

  test "root composition rejects invalid candidate or absent issuer custody before any write" do
    issue = "00000000-0000-4000-8000-000000000740"
    assert {:error, _} = Host.persist_issuer_outputs("invalid", "{}", "unused")
    assert {:error, _} = Host.persist_issuer_outputs(issue, "invalid", "unused")
    assert {:error, _} = Host.persist_issuer_outputs(issue, "[]", "unused")
    assert {:error, _} = Host.persist_issuer_outputs(issue, "{}", "unused")
    path = Path.join(Epoch.epoch_directory(Host.marker_directory(issue)), "issuer-input.json")
    assert {:error, _} = Host.read_issuer_bundle(issue, path)
  end

  test "saved envelope identity prevents replay substitution and incomplete publication" do
    operations = %{
      lstat: fn _ -> {:ok, %File.Stat{}} end,
      read: fn _, _ -> {:ok, "signed"} end,
      exists: fn _ -> false end
    }

    assert :ok = EpochHost.verify_saved_envelope_for_test("signed", operations)
    assert {:error, :epoch_issuance_conflict} = EpochHost.verify_saved_envelope_for_test("replacement", operations)
    missing = %{operations | lstat: fn _ -> {:error, :enoent} end}
    assert :ok = EpochHost.verify_saved_envelope_for_test("signed", missing)

    for blocked <- ~w(candidate.json confirmed-root-envelope.json transaction.json) do
      conflict = %{missing | exists: fn path -> Path.basename(path) == blocked end}
      assert {:error, :epoch_issuance_conflict} = EpochHost.verify_saved_envelope_for_test("signed", conflict)
    end

    unreadable = %{operations | lstat: fn _ -> {:error, :eacces} end}
    assert {:error, :epoch_issuance_conflict} = EpochHost.verify_saved_envelope_for_test("signed", unreadable)
    assert {:error, _} = EpochHost.read_private("/etc/passwd", 1_048_576)
  end

  test "protected reads reject custody changes and races after the initial stat" do
    info = %File.Stat{type: :regular, uid: 0, gid: 0, mode: 0o600, links: 1, size: 3}
    operations = %{trusted: fn _ -> :ok end, lstat: fn _ -> {:ok, info} end, read: fn _ -> {:ok, "abc"} end}
    assert {:ok, "abc"} = EpochHost.read_private_for_test("/fixed/input", 3, operations)

    invalid_stats = [%{type: :symlink}, %{uid: 1001}, %{gid: 1001}, %{mode: 0o644}, %{links: 2}, %{size: 4}, %{size: 0}]

    for change <- invalid_stats do
      denied = Map.put(operations, :lstat, fn _ -> {:ok, struct(info, change)} end)
      assert {:error, :untrusted_reconciliation_file} = EpochHost.read_private_for_test("/fixed/input", 3, denied)
    end

    calls = :counters.new(1, [])

    raced =
      Map.put(operations, :lstat, fn _ ->
        :counters.add(calls, 1, 1)
        {:ok, if(:counters.get(calls, 1) == 1, do: info, else: %{info | inode: 2})}
      end)

    assert {:error, :untrusted_reconciliation_file} = EpochHost.read_private_for_test("/fixed/input", 3, raced)
    truncated = %{operations | read: fn _ -> {:ok, "ab"} end}
    assert {:error, :untrusted_reconciliation_file} = EpochHost.read_private_for_test("/fixed/input", 3, truncated)
  end

  test "resume revalidates the saved envelope and publishes only the same signed payload" do
    {observation, _files, _hashes} = fixture()
    payload = %{"observation" => observation, "reservationId" => observation["expected"]["reservationId"]}
    encoded = Base.url_encode64(Evidence.canonical_json(payload), padding: false)
    envelope = Evidence.canonical_json(%{"payload" => encoded, "signature" => "saved"})
    parent = self()

    context = %{
      issue_id: @issue,
      pool: "hypergrid-gitops",
      nonce: "original",
      host_ops: %{
        now_ms: fn -> 1000 end,
        verify_signed_evidence: fn bytes, bindings ->
          assert bytes == envelope
          assert bindings.issue_id == @issue and bindings.nonce == "original" and bindings.now_ms == 1000
          {:ok, payload}
        end,
        persist_issuer_outputs: fn issue, candidate, signed ->
          send(parent, {:published, issue, candidate, signed})
          :ok
        end
      }
    }

    operations = %{lstat: fn _ -> {:ok, %File.Stat{}} end, read: fn _, _ -> {:ok, envelope} end}
    assert :ok = EpochHost.resume_saved_for_test(context, "/fixed/saved", operations)
    assert_received {:published, @issue, candidate, ^envelope}
    assert candidate == Evidence.canonical_json(observation)

    denied = put_in(context, [:host_ops, :verify_signed_evidence], fn _, _ -> {:error, :expired} end)
    denied_result = EpochHost.resume_saved_for_test(denied, "/fixed/saved", operations)
    assert {:error, :saved_reconciliation_issuance_invalid} = denied_result
    refute_received {:published, _, _, _}
    invalid = %{operations | read: fn _, _ -> {:ok, "invalid"} end}
    unreadable = %{operations | lstat: fn _ -> {:error, :eacces} end}
    assert {:error, :saved_reconciliation_issuance_invalid} = EpochHost.resume_saved_for_test(context, "/fixed/saved", invalid)
    assert {:error, :saved_reconciliation_issuance_invalid} = EpochHost.resume_saved_for_test(context, "/fixed/saved", unreadable)
  end

  test "the retained claim cannot drop its epoch binding even without a directory" do
    {observation, _files, _hashes} = fixture()
    assert {:error, :reconciliation_binding_missing} = EpochHost.verify(Map.delete(observation, "reconciliation"))
    assert :ok = EpochHost.verify(%{})
    assert {:error, :invalid_reconciliation_epoch} = EpochHost.verify(nil)
    assert :ok = EpochHost.verify_envelope(%{}, "unused")
    assert {:error, :invalid_reconciliation_epoch} = EpochHost.verify(observation)
  end

  test "publication stops at every failed durable operation and preserves the write order" do
    issue = "00000000-0000-4000-8000-000000000740"
    candidate = "{\"reconciliation\":{\"epoch\":\"epoch-1\"}}"

    for fail_at <- 1..5 do
      counter = :counters.new(1, [])

      operation = fn _path ->
        :counters.add(counter, 1, 1)
        if :counters.get(counter, 1) == fail_at, do: {:error, :injected_io_failure}, else: :ok
      end

      write = fn path, _bytes -> operation.(path) end
      assert {:error, :issuer_output_conflict} = EpochHost.persist(issue, candidate, "envelope", write, operation)
      assert :counters.get(counter, 1) == fail_at
    end

    parent = self()

    write = fn path, bytes ->
      send(parent, {:write, Path.basename(path), bytes})
      :ok
    end

    sync = fn path ->
      send(parent, {:sync, Path.basename(path)})
      :ok
    end

    assert :ok = EpochHost.persist(issue, candidate, "envelope", write, sync)
    assert_received {:write, "issued-envelope.json", "envelope"}
    assert_received {:sync, "epoch-1"}
    assert_received {:write, "candidate.json", ^candidate}
    assert_received {:write, "confirmed-root-envelope.json", "envelope"}
    assert_received {:sync, "generation-2"}
  end

  test "absent or untrusted root paths never become resumable evidence" do
    issue = "00000000-0000-4000-8000-000000000740"
    directory = Host.marker_directory(issue)
    assert :not_issued = EpochHost.resume(%{issue_id: issue}, "/unrelated/input")

    assert {:error, :invalid_reconciliation_epoch} =
             EpochHost.resume(%{issue_id: issue}, Path.join(Epoch.epoch_directory(directory), "issuer-input.json"))

    assert {:error, :invalid_reconciliation_epoch} = EpochHost.require_directory(issue)
    assert {:error, _} = EpochHost.read_private("/missing-reconciliation-test/input", 10)
    assert {:error, _} = EpochHost.read_private(System.tmp_dir!(), 10)
  end

  test "fixed epoch validates historical custody and exact evidence on restart" do
    {observation, files, hashes} = fixture()
    read = fn path, _maximum -> Map.fetch(files, path) end
    assert :ok = Epoch.verify_test_epoch(observation, @directory, read, hashes)
    assert :ok = Epoch.verify_test_epoch(observation, @directory, read, hashes)
    assert Enum.count(files) == 7
  end

  test "missing, replaced and noncanonical historical or epoch artifacts fail closed" do
    {observation, files, hashes} = fixture()

    for path <- Map.keys(files) do
      missing = Map.delete(files, path)
      changed = Map.put(files, path, "{\"different\":true}")

      assert {:error, :invalid_reconciliation_epoch} =
               Epoch.verify_test_epoch(observation, @directory, fn p, _ -> Map.fetch(missing, p) end, hashes)

      assert {:error, :invalid_reconciliation_epoch} =
               Epoch.verify_test_epoch(observation, @directory, fn p, _ -> Map.fetch(changed, p) end, hashes)
    end
  end

  test "each identity, historical binding and fresh evidence hash denies substitution" do
    {observation, files, hashes} = fixture()
    read = fn path, _ -> Map.fetch(files, path) end

    for field <- Map.keys(observation["expected"]) do
      changed = put_in(observation, ["expected", field], "changed")
      assert {:error, :invalid_reconciliation_epoch} = Epoch.verify_test_epoch(changed, @directory, read, hashes)
    end

    for field <- Map.keys(observation["reconciliation"]) do
      changed = put_in(observation, ["reconciliation", field], "changed")
      assert {:error, :invalid_reconciliation_epoch} = Epoch.verify_test_epoch(changed, @directory, read, hashes)
    end

    for field <- ~w(fenceSHA256 claimJournalSHA256 responsibilityGraphSHA256 predecessorRetirement) do
      changed = Map.put(observation, field, "changed")
      assert {:error, :invalid_reconciliation_epoch} = Epoch.verify_test_epoch(changed, @directory, read, hashes)
    end
  end

  defp fixture do
    observation = %{
      "expected" => %{
        "issueId" => @issue,
        "generation" => 2,
        "reservationId" => "workpkgreservation_e19008ccb2764fe79ca68bf500d20a1f",
        "projectionId" => "workpkg_4446a7d851764ecf9bf62bfbae26d1cc",
        "repositoryRef" => "repo",
        "scopeKeys" => ["scope"]
      },
      "localGenerationMax" => 2,
      "fenceSHA256" => String.duplicate("a", 64),
      "claimJournalSHA256" => String.duplicate("b", 64),
      "responsibilityGraphSHA256" => String.duplicate("c", 64),
      "predecessorRetirement" => %{"retired" => true},
      "witnesses" => [],
      "witnessLogSHA256" => %{},
      "hostIdentity" => "host",
      "bootId" => "boot",
      "dispatchPhase" => "confirmed",
      "observedAt" => "2026-10-01T00:00:00Z"
    }

    bundle = %{"assignmentSnapshotState" => "absent", "assignmentSHA256" => nil, "predecessorClaimState" => "unsubmitted", "observation" => observation, "providerHeld" => %{}}
    historical = %{"issuer-input.json" => Evidence.canonical_json(bundle), "reviewed-preflight.json" => "{}", "provider-held-readback.json" => "{}"}
    hashes = Map.new(historical, fn {name, bytes} -> {name, digest(bytes)} end)

    metadata = %{
      "contractVersion" => "hgs740-reconciliation-observation.v1",
      "epoch" => "epoch-1",
      "historicalSHA256" => hashes,
      "reviewedPreflightSHA256" => digest("{}"),
      "providerReadbackSHA256" => digest("{}"),
      "providerHeldSHA256" => digest("{}"),
      "issuerInputSHA256" => digest(Evidence.canonical_json(bundle)),
      "observedAt" => observation["observedAt"]
    }

    observation = Map.put(observation, "reconciliation", metadata)
    files = Map.new(historical, fn {name, bytes} -> {Path.join(@directory, name), bytes} end)
    epoch = Epoch.epoch_directory(@directory)

    files =
      Map.merge(files, %{
        Path.join(epoch, "reviewed-preflight.json") => "{}",
        Path.join(epoch, "provider-held-readback.json") => "{}",
        Path.join(epoch, "manifest.json") => Evidence.canonical_json(metadata),
        Path.join(epoch, "issuer-input.json") => Evidence.canonical_json(Map.put(bundle, "observation", observation))
      })

    {observation, files, hashes}
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
