defmodule SymphonyElixir.WorkPackageClaimRecoveryCLIBootstrapTest do
  use ExUnit.Case, async: true

  test "cold recovery initializes YAML and parses a workflow without starting orchestration" do
    executable = System.find_executable("elixir") || raise "elixir executable unavailable"
    paths = Enum.flat_map(:code.get_path(), &["-pa", List.to_string(&1)])
    workflow = Path.join(System.tmp_dir!(), "hgs740-cold-cli-#{System.unique_integer([:positive])}.md")
    File.write!(workflow, "---\ntracker:\n  kind: memory\nworkspace:\n  root: /tmp/fixture-workspaces\n---\nRead-only fixture\n", [:exclusive])
    on_exit(fn -> File.rm(workflow) end)

    code = """
    spawn(fn -> Process.sleep(20_000); System.halt(70) end)
    started? = fn app -> Enum.any?(Application.started_applications(), fn {name, _, _} -> name == app end) end
    false = started?.(:yaml_elixir)
    false = started?.(:symphony_elixir)
    :ok = SymphonyElixir.CLI.prepare_hgs740_runtime()
    :ok = SymphonyElixir.CLI.prepare_hgs740_runtime()
    true = started?.(:yaml_elixir)
    {:ok, %{config: %{"tracker" => %{"kind" => "memory"}}, prompt: "Read-only fixture"}} =
      SymphonyElixir.Workflow.load(#{inspect(workflow)})
    false = started?.(:symphony_elixir)
    false = started?.(:phoenix)
    false = started?.(:req)
    nil = Process.whereis(SymphonyElixir.Orchestrator)
    nil = Process.whereis(SymphonyElixir.WorkflowStore)
    IO.puts("cold YAML recovery ready; orchestration absent")
    """

    {output, status} = System.cmd(executable, paths ++ ["-e", code], env: [{"ERL_FLAGS", "+S 2:2"}], stderr_to_stdout: true)
    assert status == 0, output
    assert output =~ "cold YAML recovery ready; orchestration absent"
  end
end
