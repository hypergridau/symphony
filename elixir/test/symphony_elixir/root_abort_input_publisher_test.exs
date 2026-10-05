defmodule SymphonyElixir.RKE2Job.RootAbortInputPublisherTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RKE2Job.RootAbortInputPublisher

  test "accepts only the bounded selector and binding contract" do
    assert :ok = RootAbortInputPublisher.validate_request(request())
  end

  test "rejects payloads that try to send root-private evidence or paths" do
    assert {:error, :invalid_root_abort_input_request} =
             RootAbortInputPublisher.validate_request(Map.put(request(), "schemaVersion", 1.0))

    assert {:error, :invalid_root_abort_input_request} =
             RootAbortInputPublisher.validate_request(Map.put(request(), "checkpoints", %{}))

    assert {:error, :invalid_root_abort_input_request} =
             RootAbortInputPublisher.validate_request(Map.put(request(), "journalRoot", "/tmp/private"))

    assert {:error, :invalid_root_abort_input_request} =
             RootAbortInputPublisher.validate_request(Map.put(request(), "resultReference", "../claim.json"))
  end

  test "accepts only an exact publication acknowledgement for the selected claim" do
    request = request()

    assert :ok =
             RootAbortInputPublisher.validate_response(
               %{
                 "status" => "root-abort-inputs-published",
                 "claimSHA256" => request["claimSHA256"]
               },
               request
             )

    assert {:error, :invalid_root_abort_input_acknowledgement} =
             RootAbortInputPublisher.validate_response(
               %{
                 "status" => "root-abort-inputs-published",
                 "claimSHA256" => String.duplicate("f", 64)
               },
               request
             )
  end

  test "validates exact pre-delete eligibility and disposal bindings" do
    eligibility = request("verify_pre_execution_abort_eligibility")
    response = Map.take(eligibility, ~w(claimSHA256 assignmentDigest allocationId resultReference))
    response = Map.put(response, "status", "pre-execution-abort-eligible")

    assert :ok = RootAbortInputPublisher.validate_eligibility_response(response, eligibility)
    assert {:error, :invalid_root_abort_input_eligibility} =
             RootAbortInputPublisher.validate_eligibility_response(Map.put(response, "extra", true), eligibility)

    disposal = disposal_request()
    disposal_response = Map.take(disposal, ~w(claimSHA256 assignmentDigest allocationId resultReference resultSHA256 prepareId prepareRequestSHA256 observedAt))
    disposal_response = Map.put(disposal_response, "status", "pre-execution-abort-disposal-verified")

    assert :ok = RootAbortInputPublisher.validate_disposal_request(disposal)
    assert :ok = RootAbortInputPublisher.validate_disposal_response(disposal_response, disposal)
    assert {:error, :invalid_root_abort_disposal_acknowledgement} =
             RootAbortInputPublisher.validate_disposal_response(Map.put(disposal_response, "allocationId", "other"), disposal)
  end

  test "holds malformed, stale, future and expanded disposal requests" do
    request = disposal_request()

    for changed <- [
          Map.put(request, "observedAt", "not-a-timestamp"),
          Map.put(request, "observedAt", DateTime.add(DateTime.utc_now(), 301, :second) |> DateTime.to_iso8601()),
          Map.put(request, "observedAt", DateTime.add(DateTime.utc_now(), -301, :second) |> DateTime.to_iso8601()),
          Map.put(request, "prepareId", "not-a-uuid"),
          Map.put(request, "resultSHA256", "bad"),
          Map.put(request, "extra", true)
        ] do
      assert {:error, :invalid_root_abort_disposal_request} = RootAbortInputPublisher.validate_disposal_request(changed)
    end
  end

  defp request(operation \\ "publish_pre_execution_abort_inputs") do
    %{
      "schemaVersion" => 1,
      "operation" => operation,
      "claimSHA256" => String.duplicate("a", 64),
      "assignmentDigest" => String.duplicate("b", 64),
      "allocationId" => "allocation-one",
      "resultReference" => "managed-abort-result:v1:one"
    }
  end

  defp disposal_request do
    request("verify_pre_execution_abort_disposal")
    |> Map.merge(%{
      "resultSHA256" => String.duplicate("c", 64),
      "prepareId" => "11111111-2222-4333-8444-555555555599",
      "prepareRequestSHA256" => String.duplicate("d", 64),
      "observedAt" => DateTime.utc_now() |> DateTime.to_iso8601()
    })
  end
end
