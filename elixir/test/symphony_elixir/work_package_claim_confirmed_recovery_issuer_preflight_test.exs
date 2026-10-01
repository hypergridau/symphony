defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuerPreflightTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuerPreflight, as: Preflight

  @history_files ~w(issuer-input.json reviewed-preflight.json provider-held-readback.json)
  @cluster %{"apiServer" => "https://10.0.14.10:6443", "caSha256" => String.duplicate("b", 64)}

  test "pins all three historical files before observing the exact claim and cluster" do
    assert :ok =
             invoke(Evidence.canonical_json(bundle()), %{}, fn claim, cluster ->
               assert claim == expected_claim()
               assert cluster == @cluster
               send(self(), :observed)
               {:ok, %{}}
             end)

    names =
      for _ <- 1..3 do
        assert_receive {:read, name}
        name
      end

    assert Enum.sort(names) == Enum.sort(@history_files)
    assert_receive :observed
  end

  test "every changed or missing historical file denies before observation" do
    for name <- @history_files, change <- ["tampered", :missing] do
      assert_denied(Evidence.canonical_json(bundle()), %{name => change})
    end
  end

  test "noncanonical, malformed or assignment-bearing input denies before observation" do
    input = bundle()

    for bytes <- [
          " " <> Evidence.canonical_json(input),
          "{",
          Evidence.canonical_json(Map.put(input, "assignmentSHA256", String.duplicate("a", 64))),
          Evidence.canonical_json(Map.put(input, "assignmentSnapshotState", "present")),
          Evidence.canonical_json(Map.delete(input, "observation")),
          Evidence.canonical_json(put_in(input, ["observation", "expected"], nil))
        ] do
      assert_denied(bytes, %{})
    end
  end

  test "native observer errors, exceptions and exits all keep preflight closed" do
    for observer <- [fn _, _ -> {:error, :offline} end, fn _, _ -> raise "offline" end, fn _, _ -> exit(:offline) end] do
      assert {:error, :kubernetes_observation_unavailable} = invoke(Evidence.canonical_json(bundle()), %{}, observer)
    end
  end

  defp assert_denied(bytes, changes) do
    reference = make_ref()

    assert {:error, :kubernetes_observation_unavailable} =
             invoke(bytes, changes, fn _, _ ->
               send(self(), {:unexpected, reference})
               {:ok, %{}}
             end)

    refute_received {:unexpected, ^reference}
  end

  defp invoke(input, changes, observe) do
    files = %{"issuer-input.json" => input, "reviewed-preflight.json" => "synthetic reviewed", "provider-held-readback.json" => "synthetic held"}
    current = Map.merge(files, changes)
    hashes = Map.new(files, fn {name, bytes} -> {name, :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)} end)

    read = fn path, maximum ->
      assert maximum == 1_048_576
      name = Path.basename(path)
      send(self(), {:read, name})

      case Map.fetch!(current, name) do
        :missing -> {:error, :enoent}
        bytes -> {:ok, bytes}
      end
    end

    Preflight.verify_for_test("/fixed", hashes, read, observe)
  end

  defp bundle do
    %{
      "assignmentSHA256" => nil,
      "assignmentSnapshotState" => "absent",
      "observation" => %{
        "expected" => %{"issueId" => "f77e349e-21d9-4bdf-bad3-ce08b302e7e8", "generation" => 2, "projectionId" => "synthetic"},
        "kubernetes" => %{"cluster" => @cluster}
      }
    }
  end

  defp expected_claim do
    bundle()["observation"]["expected"] |> Map.put("assignmentSHA256", nil) |> Map.put("assignmentSnapshotState", "absent")
  end
end
