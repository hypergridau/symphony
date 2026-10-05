defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWorkflowTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWorkflow

  @root "/srv/dahlia-runner-state/dahlia/"
  @output "config/symphony/recovery-workflows/"
  @pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)

  test "accepts only the fixed selected path and a complete source-linked six-pool export" do
    {files, _receipt, _controls} = fixture()
    assert :ok = verify(files)

    for path <- [@root <> "config/symphony/workflows/midgard.md", "/tmp/midgard.md", @root <> @output <> "./midgard.md", @root <> @output <> "grid.md"] do
      assert {:error, :untrusted_workflow_file} =
               ConfirmedRecoveryWorkflow.verify(path, "midgard", fn _, _ -> flunk("bad path must not read") end)
    end
  end

  test "checks unselected pools and rejects incomplete or malformed export receipts" do
    {files, receipt, _controls} = fixture()
    assert_denied(Map.put(files, @root <> @output <> "grid.md", "changed unselected output"))

    for entries <- [Enum.drop(receipt["files"], 1), [false | tl(receipt["files"])], List.duplicate(hd(receipt["files"]), 6)] do
      assert_denied(put_receipt(files, %{receipt | "files" => entries}))
    end

    assert_denied(Map.delete(files, @root <> @output <> "recovery-workflow-receipt.json"))
    assert_denied(Map.put(files, @root <> @output <> "recovery-workflow-receipt.json", "partial JSON"))
  end

  test "rejects oversized declared and actual fixed-path artifacts" do
    {files, receipt, _controls} = fixture()
    [first | rest] = receipt["files"]
    oversized = Map.put(first, "bytes", 524_289)
    assert_denied(put_receipt(files, %{receipt | "files" => [oversized | rest]}))
    assert_denied(Map.put(files, @root <> @output <> "grid.md", :binary.copy("x", 524_289)))
  end

  test "accepts the 137-file abort export, preserves predecessors and rejects a 138th entry" do
    {files, _receipt, controls} = fixture()
    extra = for number <- 1..130, do: entry("scripts/symphony/bounded-#{number}.py", "control")
    complete = %{controls | "files" => controls["files"] ++ extra}
    assert length(complete["files"]) == 137
    assert :ok = verify(Map.put(files, @root <> "linux-control-receipt.json", Jason.encode!(complete)))

    for count <- [130, 135] do
      predecessor = %{complete | "files" => Enum.take(complete["files"], count)}
      assert :ok = verify(Map.put(files, @root <> "linux-control-receipt.json", Jason.encode!(predecessor)))
    end

    oversized = %{complete | "files" => complete["files"] ++ [entry("scripts/symphony/overflow.py", "control")]}
    assert_denied(Map.put(files, @root <> "linux-control-receipt.json", Jason.encode!(oversized)))
  end

  test "binds source commit, canonical receipt entries, blob identities and actual bytes" do
    {files, receipt, controls} = fixture()
    assert_denied(put_receipt(files, %{receipt | "sourceCommit" => String.duplicate("b", 40)}))
    assert_denied(Map.put(files, @root <> "scripts/symphony/linux-workflow.mjs", "changed renderer"))
    assert_denied(Map.put(files, @root <> "config/symphony/workflows/grid.md", "changed source"))
    renderer = %{receipt["renderer"] | "blob" => String.duplicate("0", 40)}
    changed = put_receipt(files, %{receipt | "renderer" => renderer})
    changed_controls = %{controls | "files" => [renderer | tl(controls["files"])]}
    assert_denied(Map.put(changed, @root <> "linux-control-receipt.json", Jason.encode!(changed_controls)))
  end

  test "rejects redirected output, workspace, mode and byte count" do
    {files, receipt, _controls} = fixture()
    [first | rest] = receipt["files"]

    for changed <- [
          Map.put(first, "path", "../outside.md"),
          Map.put(first, "workspaceRoot", "/tmp"),
          Map.put(first, "mode", "100755"),
          Map.put(first, "bytes", first["bytes"] + 1),
          Map.put(first, "source", nil)
        ] do
      assert_denied(put_receipt(files, %{receipt | "files" => [changed | rest]}))
    end
  end

  defp verify(files) do
    ConfirmedRecoveryWorkflow.verify(@root <> @output <> "midgard.md", "midgard", fn path, bound ->
      case Map.fetch(files, path) do
        {:ok, bytes} when byte_size(bytes) <= bound -> {:ok, bytes}
        _ -> {:error, :untrusted_root_file}
      end
    end)
  end

  defp assert_denied(files), do: assert({:error, :untrusted_workflow_file} = verify(files))

  defp put_receipt(files, receipt),
    do: Map.put(files, @root <> @output <> "recovery-workflow-receipt.json", Jason.encode!(receipt))

  defp fixture do
    renderer_path = "scripts/symphony/linux-workflow.mjs"
    renderer = entry(renderer_path, "canonical renderer")
    initial = %{(@root <> renderer_path) => "canonical renderer"}

    {outputs, sources, files} =
      Enum.reduce(@pools, {[], [], initial}, fn pool, {outputs, sources, files} ->
        source_path = "config/symphony/workflows/" <> pool <> ".md"
        output_path = @output <> pool <> ".md"
        source_bytes = "canonical " <> pool
        output_bytes = "Linux " <> pool
        source = entry(source_path, source_bytes)
        output = entry(output_path, output_bytes) |> Map.delete("blob")
        output = Map.merge(output, %{"pool" => pool, "source" => source, "workspaceRoot" => "/srv/dahlia-runner-state/workspaces/pools/" <> pool})
        files = files |> Map.put(@root <> source_path, source_bytes) |> Map.put(@root <> output_path, output_bytes)
        {outputs ++ [output], sources ++ [source], files}
      end)

    commit = String.duplicate("a", 40)
    controls = %{"schemaVersion" => 1, "sourceCommit" => commit, "files" => [renderer | sources]}
    receipt = %{"schemaVersion" => 1, "derivation" => "canonical-linux-workflow-v1", "sourceCommit" => commit, "runtimeRoot" => "/srv/dahlia-runner-state", "renderer" => renderer, "files" => outputs}
    files = files |> Map.put(@root <> "linux-control-receipt.json", Jason.encode!(controls)) |> put_receipt(receipt)
    {files, receipt, controls}
  end

  defp entry(path, bytes) do
    %{
      "path" => path,
      "bytes" => byte_size(bytes),
      "mode" => "100644",
      "sha256" => hash(:sha256, bytes),
      "blob" => hash(:sha, ["blob ", Integer.to_string(byte_size(bytes)), <<0>>, bytes])
    }
  end

  defp hash(algorithm, bytes), do: algorithm |> :crypto.hash(bytes) |> Base.encode16(case: :lower)
end
