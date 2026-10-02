defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFifthEpochTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedEpoch, as: First
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedFourthEpoch, as: Fourth
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedSecondEpoch, as: Second
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedSignedEpoch, as: Third
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliation, as: Epoch
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliationHost, as: Host

  @base "/synthetic/generation-2"
  @seal "/srv/dahlia-runner-state/evidence/hgs740-reconciliation-20261001/failed-signed-epoch-4-seal-v1.json"

  test "reserved fifth input has no fallback and prohibits all previous signing paths" do
    fifth = Epoch.epoch_directory(@base, "epoch-5")
    assert {:ok, ^fifth} = Epoch.input_directory(@base, fn ^fifth -> {:ok, %File.Stat{type: :directory}} end)

    for result <- [{:ok, %File.Stat{type: :symlink}}, {:ok, %File.Stat{type: :regular}}, {:error, :eacces}] do
      assert {:error, :invalid_reconciliation_epoch} = Epoch.input_directory(@base, fn ^fifth -> result end)
    end

    assert :ok = Host.successor_downgrade_for_test("epoch-5", fn _ -> flunk("no earlier selector") end)

    for epoch <- ~w(epoch-1 epoch-2 epoch-3 epoch-4) do
      directory = fn _ -> {:ok, %File.Stat{type: :directory}} end
      assert {:error, :reconciliation_epoch_downgrade} = Host.successor_downgrade_for_test(epoch, directory)
      unreadable = fn _ -> {:error, :eacces} end
      assert {:error, :reconciliation_epoch_downgrade} = Host.successor_downgrade_for_test(epoch, unreadable)
    end

    assert_raise FunctionClauseError, fn -> Epoch.epoch_directory(@base, "epoch-6") end
  end

  test "fourth chain verifier rejects every historical byte and seal substitution" do
    ancestor = chain()
    manifest = Evidence.canonical_json(ancestor)
    files = Map.put(Map.new(Map.keys(Fourth.predecessor_binding()["evidenceSHA256"]), &{&1, "fixed:" <> &1}), "manifest.json", manifest)
    base = %{"candidate.json" => "old candidate", "confirmed-root-envelope.json" => "old envelope"}
    binding = %{"epoch" => "epoch-4", "evidenceSHA256" => hashes(files), "baseSignedOutputSHA256" => hashes(base), "failureSealSHA256" => digest("failed seal")}
    metadata = Map.put(ancestor, "signedPredecessorEpoch4", binding)
    paths = Map.new(files, fn {name, bytes} -> {Path.join([@base, "reconciliation", "epoch-4", name]), bytes} end)
    paths = Map.merge(paths, Map.new(base, fn {name, bytes} -> {Path.join(@base, name), bytes} end)) |> Map.put(@seal, "failed seal")
    ancestor_check = fn ^metadata, @base, _ -> :ok end
    verify = fn values, selected -> Fourth.verify_for_test(selected, @base, reader(values), binding, ancestor_check) end
    assert :ok = verify.(paths, metadata)

    for path <- Map.keys(paths) do
      assert {:error, :invalid_reconciliation_epoch} = verify.(Map.put(paths, path, "changed"), metadata)
      assert {:error, :invalid_reconciliation_epoch} = verify.(Map.delete(paths, path), metadata)
    end

    changed_ancestor = fn _, _, _ -> {:error, :changed_ancestor} end
    result = Fourth.verify_for_test(metadata, @base, reader(paths), binding, changed_ancestor)
    assert {:error, :invalid_reconciliation_epoch} = result
    refute Fourth.valid?(%{})
    assert Fourth.valid?(Map.put(ancestor, "signedPredecessorEpoch4", Fourth.predecessor_binding()))
  end

  test "fifth custody requires unsigned ancestors, signed third and complete signed fourth" do
    inputs = ~w(started.json reviewed-preflight.json provider-held-readback.json manifest.json issuer-input.json)
    fourth = ~w(candidate.json confirmed-root-envelope.json issued-envelope.json) ++ inputs

    operations = %{
      trusted: fn _ -> :ok end,
      lstat: fn _ -> {:ok, %File.Stat{mode: 0o700}} end,
      ls: fn path ->
        case Path.basename(path) do
          "epoch-3" -> {:ok, ["issued-envelope.json" | inputs]}
          "epoch-4" -> {:ok, fourth}
          _ -> {:ok, inputs}
        end
      end
    }

    assert :ok = Host.successor_custody_for_test("epoch-5", operations)

    for entries <- [inputs, fourth ++ ["extra.json"], Enum.drop(fourth, 1)] do
      changed = fn path ->
        if Path.basename(path) == "epoch-4", do: {:ok, entries}, else: operations.ls.(path)
      end

      assert {:error, :invalid_reconciliation_epoch} =
               Host.successor_custody_for_test("epoch-5", %{operations | ls: changed})
    end

    assert {:error, :invalid_reconciliation_epoch} = Fourth.verify(%{}, @base, fn _, _ -> flunk("invalid chain") end)
    missing = fn _, _ -> {:error, :enoent} end
    no_key = fn -> flunk("no candidate") end
    assert {:error, :invalid_reconciliation_epoch} = Fourth.verify_historical_signature(@base, missing, no_key)
  end

  test "historical signature uses only recorded issuance and exact candidate bytes" do
    observation = %{"claimJournalSHA256" => "journal", "fenceSHA256" => "fence", "responsibilityGraphSHA256" => "graph"}
    candidate = Evidence.canonical_json(observation)
    payload = %{"issuedAt" => "2026-10-01T18:45:40Z", "observation" => observation}
    envelope = Evidence.canonical_json(%{"payload" => Base.url_encode64(Evidence.canonical_json(payload), padding: false)})
    files = %{"candidate.json" => candidate, "issued-envelope.json" => envelope}
    paths = Map.new(files, fn {name, bytes} -> {Path.join([@base, "reconciliation", "epoch-4", name]), bytes} end)

    verify = fn ^envelope, "root public key", bindings ->
      assert bindings.now_ms == DateTime.to_unix(~U[2026-10-01 18:45:40Z], :millisecond)
      assert bindings.generation == 2 and bindings.claim_journal_sha256 == "journal"
      assert bindings.assignment_sha256 == nil and bindings.assignment_snapshot_state == "absent"
      {:ok, payload}
    end

    check = fn values, key, verification ->
      Fourth.historical_signature_for_test(@base, reader(values), key, hashes(files), verification)
    end

    key = fn -> {:ok, "root public key"} end
    denied_key = fn -> {:error, :untrusted_key} end
    denied_signature = fn _, _, _ -> {:error, :invalid_signature} end
    assert :ok = check.(paths, key, verify)
    assert {:error, :invalid_reconciliation_epoch} = check.(paths, denied_key, verify)
    assert {:error, :invalid_reconciliation_epoch} = check.(paths, key, denied_signature)

    for path <- Map.keys(paths) do
      assert {:error, :invalid_reconciliation_epoch} = check.(Map.put(paths, path, "changed"), key, verify)
    end
  end

  defp chain, do: %{"ancestorEpoch1" => First.predecessor_binding(), "predecessorEpoch2" => Second.predecessor_binding(), "signedPredecessorEpoch3" => Third.predecessor_binding()}
  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp hashes(files), do: Map.new(files, fn {name, bytes} -> {name, digest(bytes)} end)

  defp reader(files),
    do: fn path, _maximum ->
      Map.fetch(files, path)
      |> case do
        {:ok, bytes} -> {:ok, bytes}
        :error -> {:error, :enoent}
      end
    end
end
