defmodule SymphonyElixir.RKE2JobAllocationRegistrationTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RKE2Job.JobAllocationRegistration

  test "posts only the allocation ID under the trusted host token and verifies the UID response" do
    caller = self()

    post = fn url, opts ->
      send(caller, {:post, url, opts})
      {:ok, %Req.Response{status: 201, body: %{"data" => %{"jobUid" => "job-uid-1", "replayed" => false}}}}
    end

    context = %{base_url: "https://dahlia.example/", runner_token: "host-only-token", post_fun: post}
    allocation_id = allocation_id("job-uid-1")
    assert :ok = JobAllocationRegistration.register(allocation_id, "job-uid-1", "workpkgreservation_1", context)

    assert_receive {:post, "https://dahlia.example/runner/v1/verified-assignments/workpkgreservation_1/job-allocation", opts}
    assert opts[:json] == %{allocationId: allocation_id}
    assert opts[:headers] == [{"authorization", "Bearer host-only-token"}]
    assert opts[:retry] == false
    assert opts[:redirect] == false
  end

  test "holds a mismatched response, provider denial, and missing trusted configuration" do
    response = fn _url, _opts ->
      {:ok, %Req.Response{status: 200, body: %{"data" => %{"jobUid" => "other-uid", "replayed" => true}}}}
    end

    config = %{base_url: "https://dahlia.example", runner_token: "host-only-token", post_fun: response}

    assert {:held, :job_allocation_registration_unverified} =
             JobAllocationRegistration.register(allocation_id("job-uid-1"), "job-uid-1", "reservation-1", config)

    denied = %{config | post_fun: fn _url, _opts -> {:ok, %Req.Response{status: 409}} end}

    assert {:held, :job_allocation_registration_unverified} =
             JobAllocationRegistration.register(allocation_id("job-uid-1"), "job-uid-1", "reservation-1", denied)

    assert {:held, :job_allocation_registration_unverified} =
             JobAllocationRegistration.register(allocation_id("job-uid-1"), "job-uid-1", "reservation-1", %{})

    assert {:held, :job_allocation_registration_unverified} =
             JobAllocationRegistration.register(allocation_id("other-uid"), "job-uid-1", "reservation-1", config)

    long_uid = "job:" <> String.duplicate("a", 140)

    replayed = %{
      config
      | post_fun: fn _url, _opts ->
          {:ok, %Req.Response{status: 200, body: %{"data" => %{"jobUid" => long_uid, "replayed" => true}}}}
        end
    }

    assert :ok = JobAllocationRegistration.register(allocation_id(long_uid), long_uid, "reservation-1", replayed)
  end

  defp allocation_id(uid) do
    payload = [1, "symphony-beta", "symphony-job", uid, String.duplicate("a", 64)]
    "rke2job:v1:" <> (Jason.encode!(payload) |> Base.url_encode64(padding: false))
  end
end
