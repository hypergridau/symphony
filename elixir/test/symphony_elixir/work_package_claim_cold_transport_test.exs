defmodule SymphonyElixir.WorkPackageClaimColdTransportTest do
  use ExUnit.Case, async: true

  test "cold admitted readback starts HTTP dependencies without Mix or orchestration" do
    executable = System.find_executable("elixir") || raise "elixir executable unavailable"
    paths = Enum.flat_map(:code.get_path(), &["-pa", List.to_string(&1)])

    code = """
    spawn(fn -> Process.sleep(20_000); System.halt(70) end)
    alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseTransport, as: Transport
    started? = fn app -> Enum.any?(Application.started_applications(), fn {name, _, _} -> name == app end) end
    nil = Process.whereis(Mix.State)
    false = started?.(:mix)
    false = started?.(:req)
    count = :atomics.new(1, [])
    Req.default_options(adapter: fn request ->
      :atomics.add(count, 1, 1)
      true = request.url.scheme == "https" and request.url.host == "dahlia.hypergrid.au"
      true = request.url.path == "/provider/v1/work-packages/claim-recovery/release-only/confirmation-readback"
      true = request.method == :post
      %{"bundle" => %{"retained" => true}, "providerApprovalId" => "synthetic-provider"} = Jason.decode!(request.body)
      {request, %Req.Response{status: 200, headers: %{"cache-control" => ["no-store"]}, body: ~s({"data":{"confirmed":true}})}}
    end)
    {:error, :hgs740_confirmation_readback_not_admitted} =
      Transport.confirmed_readback(%{}, "synthetic-provider", %{})
    {:error, :hgs740_provider_transport_held_closed} =
      Transport.confirmed_readback(%{}, "synthetic-provider", %{historical_readback: true})
    false = started?.(:req)
    0 = :atomics.get(count, 1)
    context = %{historical_readback: true, runner_token: "synthetic-runner", admin_token: "synthetic-admin"}
    for _ <- 1..2 do
      {:ok, %{"confirmed" => true}} = Transport.confirmed_readback(%{"retained" => true}, "synthetic-provider", context)
    end
    2 = :atomics.get(count, 1)
    true = started?.(:req)
    nil = Process.whereis(Mix.State)
    false = started?.(:mix)
    false = started?.(:symphony_elixir)
    false = started?.(:phoenix)
    nil = Process.whereis(SymphonyElixir.Orchestrator)
    nil = Process.whereis(SymphonyElixir.WorkflowStore)
    IO.puts("cold confirmed readback ready; Mix and orchestration absent")
    """

    {output, status} = System.cmd(executable, paths ++ ["-e", code], env: [{"ERL_FLAGS", "+S 2:2"}], stderr_to_stdout: true)
    assert status == 0, output
    assert output =~ "cold confirmed readback ready; Mix and orchestration absent"
  end
end
