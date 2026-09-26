defmodule SymphonyElixir.HostWitnessAbortTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.HostWitness

  defp input(witness) do
    %{
      pool_key: "midgard",
      issue_id: "11111111-2222-3333-4444-555555555555",
      runner_id: "runner-one",
      managed_project_profile_id: "profile-one",
      repository_ref: "hypergridau/dahlia",
      host_witness_fun: witness
    }
  end

  defp reservation do
    %{
      projection_id: "projection-one",
      reservation_id: "reservation-one",
      workspace_id: "workspace-one",
      company_id: "company-one",
      issue_id: "11111111-2222-3333-4444-555555555555",
      runner_id: "runner-one",
      managed_project_profile_id: "profile-one",
      repository_ref: "hypergridau/dahlia",
      scope_keys: ["repo:hypergridau/dahlia"],
      generation: 1,
      session_id: "session-one",
      process_id: "process-one",
      responsible_delegation_id: "delegation-one",
      execution_fence_token: "11111111-2222-3333-4444-555555555555:1",
      runtime_lease_id: "session-one",
      reservation_nonce: "private-nonce"
    }
  end

  defp proof do
    %{
      "proofId" => "proof-one",
      "proofSHA256" => String.duplicate("a", 64),
      "receiptId" => "receipt-one",
      "receiptSHA256" => String.duplicate("b", 64),
      "assignmentDigest" => String.duplicate("c", 64),
      "allocationId" => "allocation-one",
      "abortResultRef" => "blocked-result-one"
    }
  end

  test "submits the exact v2 abort references without exposing the reservation nonce" do
    parent = self()

    witness = fn request ->
      send(parent, {:root_request, request})

      {:ok,
       %{
         "ok" => true,
         "receipt" => %{"version" => 1, "sequence" => 3, "hash" => String.duplicate("d", 64), "replayed" => false}
       }}
    end

    assert :ok = HostWitness.record_abort(input(witness), reservation(), proof())

    assert_receive {:root_request, request}
    assert request["version"] == 2
    assert request["pool"] == "midgard"
    assert request["operation"] == "abort_cleanup_intent"
    assert request["abortProof"] == proof()
    assert request["claim"]["reservationId"] == "reservation-one"

    assert request["claim"]["nonceHash"] ==
             :crypto.hash(:sha256, "private-nonce") |> Base.encode16(case: :lower)

    refute inspect(request) =~ "private-nonce"
  end

  test "rejects malformed references and changed claim identity before the root call" do
    parent = self()
    witness = fn request -> send(parent, {:unexpected_root_call, request}) end

    for invalid <- [
          Map.put(proof(), "proofSHA256", "not-a-digest"),
          Map.put(proof(), "allocationId", ""),
          Map.put(proof(), "executionStarted", false)
        ] do
      assert {:error, :invalid_abort_proof_reference} =
               HostWitness.record_abort(input(witness), reservation(), invalid)
    end

    assert {:error, :host_witness_claim_incomplete} =
             HostWitness.record_abort(input(witness), %{reservation() | issue_id: "another-issue"}, proof())

    refute_receive {:unexpected_root_call, _}
  end
end
