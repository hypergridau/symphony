defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedSecondEpochTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedEpoch, as: First
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedSecondEpoch, as: Second
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliation, as: Epoch

  test "third epoch accepts only the fixed V3 references and rejects arbitrary successor numbering" do
    metadata = %{
      "contractVersion" => "hgs740-reconciliation-observation.v3",
      "epoch" => "epoch-3",
      "historicalSHA256" => Epoch.historical_hashes(),
      "observedAt" => "2026-10-01T14:37:00Z",
      "predecessorEpoch2" => Second.predecessor_binding(),
      "ancestorEpoch1" => First.predecessor_binding()
    }

    metadata = Enum.reduce(~w(reviewedPreflightSHA256 providerReadbackSHA256 providerHeldSHA256 issuerInputSHA256), metadata, &Map.put(&2, &1, String.duplicate("a", 64)))

    observation = %{
      "reconciliation" => metadata,
      "observedAt" => metadata["observedAt"],
      "expected" => %{
        "issueId" => "f77e349e-21d9-4bdf-bad3-ce08b302e7e8",
        "generation" => 2,
        "reservationId" => "workpkgreservation_e19008ccb2764fe79ca68bf500d20a1f",
        "projectionId" => "workpkg_4446a7d851764ecf9bf62bfbae26d1cc"
      }
    }

    assert Second.valid?(metadata)
    assert :ok = Epoch.validate(observation)

    for change <- [
          %{"epoch" => "epoch-4"},
          %{"contractVersion" => "hgs740-reconciliation-observation.v2"},
          %{"ancestorEpoch1" => %{}},
          %{"predecessorEpoch2" => %{}},
          %{"predecessorEpoch" => First.predecessor_binding()}
        ] do
      changed = Map.merge(metadata, change)
      assert {:error, :invalid_reconciliation_epoch} = Epoch.validate(Map.put(observation, "reconciliation", changed))
    end

    refute Second.valid?(%{})
    assert {:error, :invalid_reconciliation_epoch} = Second.verify(%{}, "/fixed", fn _, _ -> flunk("read before binding") end)
  end

  test "both unsigned histories and seals must remain exact, including the second manifest ancestor" do
    manifest = Evidence.canonical_json(%{"predecessorEpoch" => First.predecessor_binding()})
    files = Map.new(Second.predecessor_binding()["evidenceSHA256"], fn {name, _} -> {name, if(name == "manifest.json", do: manifest, else: "synthetic " <> name)} end)
    seal = "synthetic unsigned second seal"
    files = Map.put(files, "failed-epoch-2-seal-v1.json", seal)
    hashes = Map.new(Map.delete(files, "failed-epoch-2-seal-v1.json"), fn {name, bytes} -> {name, digest(bytes)} end)
    metadata = %{"ancestorEpoch1" => First.predecessor_binding(), "predecessorEpoch2" => %{"epoch" => "epoch-2", "evidenceSHA256" => hashes, "failureSealSHA256" => digest(seal)}}

    ancestor = fn %{"predecessorEpoch" => binding}, "/fixed", _ ->
      assert binding == First.predecessor_binding()
      :ok
    end

    verify = fn changed, pins, ancestor_callback ->
      read = fn path, _ -> Map.fetch(changed, Path.basename(path)) end
      Second.verify_for_test(pins, "/fixed", read, hashes, digest(seal), ancestor_callback)
    end

    assert :ok = verify.(files, metadata, ancestor)
    assert {:error, _} = verify.(files, metadata, fn _, _, _ -> {:error, :changed_ancestor} end)

    for name <- Map.keys(files) do
      assert {:error, _} = verify.(Map.put(files, name, "changed"), metadata, ancestor)
      assert {:error, _} = verify.(Map.delete(files, name), metadata, ancestor)
    end

    assert {:error, _} = verify.(files, Map.put(metadata, "ancestorEpoch1", %{}), ancestor)
    changed = Map.put(files, "manifest.json", Evidence.canonical_json(%{"predecessorEpoch" => %{}}))
    changed_hashes = Map.put(hashes, "manifest.json", digest(changed["manifest.json"]))
    changed_metadata = put_in(metadata, ["predecessorEpoch2", "evidenceSHA256"], changed_hashes)
    read = fn path, _ -> Map.fetch(changed, Path.basename(path)) end
    assert {:error, _} = Second.verify_for_test(changed_metadata, "/fixed", read, changed_hashes, digest(seal), ancestor)
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
