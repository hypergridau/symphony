defmodule SymphonyElixir.RKE2JobDahliaAssignmentBindingTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.DahliaAssignmentBinding

  @digest String.duplicate("b", 64)
  @origin "https://assignment-broker.example"

  test "posts the verified manifest's exact bytes and detached signature with the host token" do
    assignment = assignment()
    binding = claim_binding(assignment)
    config = config(assignment)
    caller = self()

    config =
      Map.put(config, :assignment_bind_post_fun, fn url, options ->
        send(caller, {:request, url, options})
        {:ok, %Req.Response{status: 200, body: bound_response(assignment)}}
      end)

    assert {:ok, %{assignment_digest: @digest, branch_ref: branch_ref}} =
             DahliaAssignmentBinding.bind(assignment, binding, config)

    assert branch_ref == "refs/heads/" <> assignment.branch
    assert_receive {:request, url, options}
    assert url == @origin <> "/v1/host/assignments/reservation-one/bind"
    assert options[:headers] == [{"authorization", "Bearer host-token"}]
    assert options[:json] |> Map.keys() |> Enum.sort() == [:issueIdentifier, :manifestBase64, :signatureHex]
    assert options[:json].issueIdentifier == "HGS-734"
    assert Base.decode64!(options[:json].manifestBase64) == config.managed_delegations.source_bytes
    assert options[:json].signatureHex == config.managed_delegations.source_signature_hex
    refute Map.has_key?(options[:json], :issueId)
    refute Map.has_key?(options[:json], :manifest)
  end

  test "denial, unavailable transport, proof mismatch, and source pin drift all fail closed" do
    assignment = assignment()
    binding = claim_binding(assignment)
    config = config(assignment)

    denied_response = {:ok, %Req.Response{status: 409, body: %{"status" => "denied"}}}
    denied = Map.put(config, :assignment_bind_post_fun, fn _, _ -> denied_response end)
    assert {:held, :managed_assignment_binding_denied} = DahliaAssignmentBinding.bind(assignment, binding, denied)

    unavailable = Map.put(config, :assignment_bind_post_fun, fn _, _ -> {:error, :timeout} end)
    assert {:held, :managed_assignment_binding_unverified} = DahliaAssignmentBinding.bind(assignment, binding, unavailable)

    mismatched_source = put_in(config, [:managed_delegations, :source_sha256], String.duplicate("0", 64))
    refute_called = Map.put(mismatched_source, :assignment_bind_post_fun, fn _, _ -> flunk("invalid proof must not be sent") end)
    assert {:held, :managed_assignment_binding_unverified} = DahliaAssignmentBinding.bind(assignment, binding, refute_called)

    source_drift =
      Map.put(config, :assignment_bind_post_fun, fn _, _ ->
        {:ok, %Req.Response{status: 200, body: Map.put(bound_response(assignment), "branchRef", "refs/heads/codex/other")}}
      end)

    assert {:held, :managed_assignment_binding_unverified} = DahliaAssignmentBinding.bind(assignment, binding, source_drift)
  end

  test "a request accepted before a host crash replays the same idempotent binding" do
    assignment = assignment()
    binding = claim_binding(assignment)
    config = config(assignment)
    {:ok, server} = Agent.start_link(fn -> %{calls: [], committed: nil} end)

    replaying =
      Map.put(config, :assignment_bind_post_fun, fn url, options ->
        request = {url, options[:json]}

        Agent.get_and_update(server, fn state ->
          calls = [request | state.calls]

          case {state.committed, length(calls)} do
            {nil, 1} ->
              result = {:error, :response_lost_after_commit}
              updated = %{state | calls: calls, committed: {request, bound_response(assignment)}}
              {result, updated}

            {{^request, response}, _} ->
              {{:ok, %Req.Response{status: 200, body: response}}, %{state | calls: calls}}

            _ ->
              {{:error, :idempotency_conflict}, %{state | calls: calls}}
          end
        end)
      end)

    assert {:held, :managed_assignment_binding_unverified} = DahliaAssignmentBinding.bind(assignment, binding, replaying)
    assert {:ok, %{assignment_digest: @digest}} = DahliaAssignmentBinding.bind(assignment, binding, replaying)
    assert %{calls: [first, second]} = Agent.get(server, & &1)
    assert first == second
  end

  test "the binding origin is independent and must use HTTPS" do
    assignment = assignment()
    binding = claim_binding(assignment)
    config = config(assignment) |> Map.put(:assignment_bind_origin, "http://assignment-broker.example")
    assert {:held, :managed_assignment_binding_unverified} = DahliaAssignmentBinding.bind(assignment, binding, config)
  end

  defp config(assignment) do
    bytes = Jason.encode!(%{"schema_version" => 1, "synthetic" => true})
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519)
    signature = :crypto.sign(:eddsa, :none, "hypergrid.symphony.managed-delegation.v1\0" <> bytes, [private_key, :ed25519])
    public_hex = Base.encode16(public_key, case: :lower)

    manifest = %{
      schema_version: 1,
      repository_ref: assignment.repository_ref,
      entries: [%{issue_id: assignment.lease.issue_id, identifier: "HGS-734"}],
      source_bytes: bytes,
      source_signature_hex: Base.encode16(signature, case: :lower),
      source_public_key_hex: public_hex,
      source_sha256: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
      signer_key_sha256: Base.encode16(:crypto.hash(:sha256, public_key), case: :lower)
    }

    %{assignment_bind_origin: @origin, runner_token: "host-token", managed_delegations: manifest}
  end

  defp bound_response(assignment) do
    %{
      "status" => "bound",
      "assignmentDigest" => @digest,
      "branchRef" => "refs/heads/" <> assignment.branch
    }
  end

  defp claim_binding(assignment) do
    %{
      issue_id: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      repository_ref: assignment.repository_ref,
      runner_id: assignment.seat,
      reservation_id: "reservation-one"
    }
  end

  defp assignment do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run one signed assignment"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs734-binding",
        seat: "runner-17",
        lease: %{
          issue_id: "issue-1",
          repository: "hypergridau/symphony",
          generation: 4,
          session_id: "worker:issue-1:4",
          process_id: "worker:issue-1:4"
        },
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Bound assignment", evidence: "Dahlia digest and source pin"},
        context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end
end
