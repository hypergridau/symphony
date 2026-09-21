defmodule SymphonyElixir.QualityBaselineTest do
  use ExUnit.Case, async: true

  alias Mix.Tasks.QualityBaseline.Check

  test "only additions and increased duplicate findings are regressions" do
    assert Check.regressions(%{"existing" => 2, "new" => 1, "fixed" => 0}, %{"existing" => 1, "fixed" => 2}) ==
             [{"existing", 2}, {"new", 1}]
  end

  test "the pinned baseline is complete and duplicate-free" do
    baseline = "config/quality-baseline.json" |> File.read!() |> Jason.decode!()

    assert baseline["schemaVersion"] == 1
    assert baseline["sourceCommit"] == "b2ca242e99c5f8c4fc3e474b298c57fc9004fd7d"
    assert Enum.sum(Enum.map(baseline["credo"], & &1["count"])) == 122
    assert Enum.sum(Enum.map(baseline["dialyzer"], & &1["count"])) == 26

    for kind <- ["credo", "dialyzer"] do
      identities = Enum.map(baseline[kind], & &1["identity"])
      assert length(identities) == length(Enum.uniq(identities))
    end
  end
end
