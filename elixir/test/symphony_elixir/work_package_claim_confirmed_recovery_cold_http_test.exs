defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryColdHTTPTest do
  use ExUnit.Case, async: true

  test "cold recovery starts only HTTP dependencies after validating context and CA" do
    for mode <- ["valid", "wrong-ca"] do
      assert {output, 0} = cold_process(mode)
      assert output =~ "cold-recovery-#{mode}-passed"
    end
  end

  defp cold_process(mode) do
    paths = Enum.flat_map(:code.get_path(), &["-pa", List.to_string(&1)])
    executable = System.find_executable("elixir") || raise "elixir executable unavailable"
    path = Path.join(System.tmp_dir!(), "symphony-cold-http-#{System.unique_integer([:positive])}.exs")

    try do
      File.write!(path, script())
      System.cmd(executable, ["--erl", "+S 2:2"] ++ paths ++ [path, mode], stderr_to_stdout: true)
    after
      File.rm(path)
    end
  end

  defp script do
    ~S'''
    alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryKubernetes, as: Kubernetes
    spawn(fn -> Process.sleep(20_000); System.halt(70) end)
    [mode] = System.argv()
    started? = fn app -> Enum.any?(Application.started_applications(), fn {name, _, _} -> name == app end) end
    false = started?.(:req)
    false = started?.(:symphony_elixir)
    nil = Process.whereis(SymphonyElixir.Orchestrator)
    {:ok, listener} = :gen_tcp.listen(0, [:binary, active: false, ip: {127, 0, 0, 1}])
    {:ok, {_, port}} = :inet.sockname(listener)
    parent = self()
    spawn(fn ->
      Enum.each(1..3, fn _ ->
        {:ok, socket} = :gen_tcp.accept(listener, 5_000)
        read_headers = fn read, bytes ->
          if String.contains?(bytes, "\r\n\r\n") do
            :ok
          else
            {:ok, chunk} = :gen_tcp.recv(socket, 0, 5_000)
            read.(read, bytes <> chunk)
          end
        end
        :ok = read_headers.(read_headers, "")
        :ok = :gen_tcp.send(socket, "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}")
        :ok = :gen_tcp.close(socket)
        send(parent, :http_read)
      end)
    end)
    claim = %{"issueId" => "11111111-2222-3333-4444-555555555555", "generation" => 2,
      "assignmentSHA256" => nil, "assignmentSnapshotState" => "absent"}
    ca = String.duplicate("a", 64)
    cluster = %{"apiServer" => "https://10.0.14.10:6443", "caSha256" => ca}
    context = fn ^claim -> {:ok, :validated_context, if(mode == "valid", do: ca, else: String.duplicate("b", 64))} end
    list = fn "frigga", :validated_context ->
      {:ok, %{status: 200}} = Req.get("http://127.0.0.1:#{port}/", retry: false, receive_timeout: 5_000)
      {:ok, %{items: [], resource_version: "42"}}
    end
    result = Kubernetes.observe_without_assignment_snapshot_with_test_adapter(claim, cluster, context, list, list)
    if mode == "valid" do
      {:ok, %{"jobs" => %{"itemCount" => 0}, "pods" => %{"itemCount" => 0}}} = result
      true = started?.(:req)
      Enum.each(1..3, fn _ -> receive do :http_read -> :ok after 5_000 -> raise "missing HTTP read" end end)
    else
      {:error, :kubernetes_observation_unavailable} = result
      false = started?.(:req)
    end
    false = started?.(:symphony_elixir)
    nil = Process.whereis(SymphonyElixir.Orchestrator)
    :ok = :gen_tcp.close(listener)
    IO.puts("cold-recovery-#{mode}-passed")
    '''
  end
end
