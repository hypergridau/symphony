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

  defp request do
    %{
      "schemaVersion" => 1,
      "operation" => "publish_pre_execution_abort_inputs",
      "claimSHA256" => String.duplicate("a", 64),
      "assignmentDigest" => String.duplicate("b", 64),
      "allocationId" => "allocation-one",
      "resultReference" => "managed-abort-result:v1:one"
    }
  end
end
