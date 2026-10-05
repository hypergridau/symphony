defmodule SymphonyElixir.RootFixtures.AbortInputPublisherBoundaryTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.RKE2Job.RootAbortInputPublisher

  @socket "/run/dahlia-pre-execution-abort-input-publisher.sock"

  test "fixed socket accepts a maximum-sized selector echo larger than one kilobyte" do
    assert System.get_env("HGS740_ROOT_FIXTURE") == "1"
    assert match?({:ok, %{uid: 0}}, File.stat("/proc/self"))
    assert File.lstat(@socket) == {:error, :enoent}
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, packet: :line, ip: {:local, String.to_charlist(@socket)}])

    on_exit(fn ->
      :gen_tcp.close(listener)
      File.rm(@socket)
    end)

    request = %{
      "schemaVersion" => 1,
      "operation" => "verify_pre_execution_abort_disposal",
      "claimSHA256" => String.duplicate("a", 64),
      "assignmentDigest" => String.duplicate("b", 64),
      "allocationId" => String.duplicate("q", 1024),
      "resultReference" => String.duplicate("r", 512)
    }

    response =
      Map.merge(Map.take(request, ~w(claimSHA256 assignmentDigest allocationId resultReference)), %{
        "status" => "pre-execution-abort-disposal-verified",
        "resultSHA256" => String.duplicate("c", 64),
        "prepareId" => "11111111-2222-4333-8444-555555555599",
        "prepareRequestSHA256" => String.duplicate("d", 64),
        "observedAt" => DateTime.utc_now() |> DateTime.to_iso8601()
      })

    bytes = Jason.encode!(%{"ok" => true, "result" => response}) <> "\n"
    assert byte_size(bytes) > 1024 and byte_size(bytes) < 4096

    server =
      Task.async(fn ->
        {:ok, socket} = :gen_tcp.accept(listener, 5000)

        try do
          {:ok, raw} = :gen_tcp.recv(socket, 0, 5000)
          assert Jason.decode!(raw) == request
          :gen_tcp.send(socket, bytes)
        after
          :gen_tcp.close(socket)
        end
      end)

    assert {:ok, ^response} = RootAbortInputPublisher.verify_disposal(request)
    assert :ok = Task.await(server, 5000)
  end
end
