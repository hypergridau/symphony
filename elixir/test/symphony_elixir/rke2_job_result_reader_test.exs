Code.require_file("../support/rke2_job_fake_client.exs", __DIR__)

defmodule SymphonyElixir.RKE2JobResultReaderTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{JobSpec, ResultReader}
  alias SymphonyElixir.Worker.CLI

  defmodule Client do
    def get_job(_namespace, _name, context), do: context.job
    def list_pods_snapshot(_namespace, context), do: context.pods
  end

  test "reads the one exact terminated preflight Pod and returns Kubernetes versions" do
    {assignment, config, job, pod, uid} = fixture()

    assert {:ok, observation} = read(assignment, config, job, [pod], uid)
    assert observation.job_uid == uid
    assert observation.job_resource_version == "job-rv-7"
    assert observation.pod_uid == "pod-uid-1"
    assert observation.pod_resource_version == "pod-rv-8"
    assert observation.pod_list_resource_version == "list-rv-9"
    assert observation.exit_code == 0
    assert observation.result["status"] == "preflight_passed"
  end

  test "reads a completed Codex receipt only when the Job, Pod, and PR identity agree" do
    {assignment, config, job, pod, uid} = fixture(:codex)

    result =
      receipt(assignment)
      |> Map.merge(%{
        status: "completed",
        reason: "pull_request_created",
        checkout_lease_id: "checkout-1",
        checkout_revocation: "confirmed",
        broker_lease_id: "publish-2",
        revocation: "confirmed",
        codex_exit_code: 0,
        base_oid: String.duplicate("a", 40),
        branch_head_oid: String.duplicate("b", 40),
        head_oid: String.duplicate("c", 40),
        changed_files: 2,
        pull_request_number: 123,
        pull_request_url: "https://github.com/hypergridau/symphony/pull/123"
      })

    completed = put_in(pod, ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"], Jason.encode!(result))
    assert {:ok, observation} = read(assignment, config, job, [completed], uid)
    assert observation.result["pull_request_number"] == 123

    forged = put_in(completed, ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"], Jason.encode!(%{result | pull_request_url: "https://github.com/other/repo/pull/123"}))
    assert {:held, :job_result_pod_or_receipt_unverified} = read(assignment, config, job, [forged], uid)

    unrevoked = put_in(completed, ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"], Jason.encode!(%{result | revocation: "held"}))
    assert {:held, :job_result_pod_or_receipt_unverified} = read(assignment, config, job, [unrevoked], uid)
  end

  test "holds a Job UID mismatch or unverified terminal state" do
    {assignment, config, job, pod, uid} = fixture()

    assert {:held, :job_result_identity_or_terminal_unverified} =
             read(assignment, config, put_in(job, ["metadata", "uid"], "other-job"), [pod], uid)

    assert {:held, :job_result_identity_or_terminal_unverified} =
             read(assignment, config, put_in(job, ["status", "conditions"], []), [pod], uid)
  end

  test "holds unavailable Job and Pod readback without inferring a terminal result" do
    {assignment, config, job, pod, uid} = fixture()

    assert {:held, :job_result_read_unavailable} =
             ResultReader.read(assignment, uid,
               client: Client,
               client_context: %{job: {:error, :timeout}, pods: {:ok, %{items: [pod], resource_version: "list-rv-9"}}},
               config: config
             )

    assert {:held, :job_pod_result_read_unavailable} =
             ResultReader.read(assignment, uid,
               client: Client,
               client_context: %{job: {:ok, job}, pods: {:error, :timeout}},
               config: config
             )

    assert {:error, :invalid_result_reader_request} = read(assignment, config, job, [pod], "")
  end

  test "holds incomplete or malformed Pod snapshots before cleanup" do
    {assignment, config, job, pod, uid} = fixture()
    missing_version = %{items: [pod], resource_version: nil}
    malformed_pod = put_in(pod, ["metadata", "uid"], nil)

    snapshots = [
      missing_version,
      %{items: [malformed_pod], resource_version: "list-rv-9"},
      %{items: [], resource_version: "list-rv-9"}
    ]

    for snapshot <- snapshots do
      assert {:held, _reason} =
               ResultReader.read(assignment, uid,
                 client: Client,
                 client_context: %{job: {:ok, job}, pods: {:ok, snapshot}},
                 config: config
               )
    end
  end

  test "holds a forged owner, ambiguous Pods, and mismatched assignment receipt" do
    {assignment, config, job, pod, uid} = fixture()

    forged = put_in(pod, ["metadata", "ownerReferences", Access.at(0), "uid"], "other-job")
    assert {:held, :job_result_pod_or_receipt_unverified} = read(assignment, config, job, [forged], uid)

    second = put_in(pod, ["metadata", "uid"], "pod-uid-2")
    assert {:held, :job_result_pod_ambiguous} = read(assignment, config, job, [pod, second], uid)

    wrong = put_in(pod, ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"], Jason.encode!(Map.put(receipt(assignment), :assignment_digest, String.duplicate("0", 64))))
    assert {:held, :job_result_pod_or_receipt_unverified} = read(assignment, config, job, [wrong], uid)
  end

  test "holds malformed or extra receipt content before it can enter a journal" do
    {assignment, config, job, pod, uid} = fixture()

    extra = receipt(assignment) |> Map.put(:auth_json, "secret") |> Jason.encode!()
    extra_pod = put_in(pod, ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"], extra)
    assert {:held, :job_result_pod_or_receipt_unverified} = read(assignment, config, job, [extra_pod], uid)

    empty_pod = put_in(pod, ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"], "")
    assert {:held, :job_result_pod_or_receipt_unverified} = read(assignment, config, job, [empty_pod], uid)

    sensitive_reason = receipt(assignment) |> Map.put(:reason, "token=secret") |> Jason.encode!()
    sensitive_pod = put_in(pod, ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"], sensitive_reason)
    assert {:held, :job_result_pod_or_receipt_unverified} = read(assignment, config, job, [sensitive_pod], uid)
  end

  test "holds a success claim when Kubernetes reports failure" do
    {assignment, config, job, pod, uid} = fixture()
    failed_job = put_in(job, ["status", "conditions"], [%{"type" => "Failed", "status" => "True"}])
    failed_pod = put_in(pod, ["status", "phase"], "Failed")
    assert {:held, :job_result_pod_or_receipt_unverified} = read(assignment, config, failed_job, [failed_pod], uid)
  end

  test "binds failed and held receipts to the worker CLI exit codes" do
    {assignment, config, job, pod, uid} = fixture(:codex)
    failed_job = put_in(job, ["status", "conditions"], [%{"type" => "Failed", "status" => "True"}])
    failed_pod = put_in(pod, ["status", "phase"], "Failed")
    result = receipt(assignment) |> Map.merge(%{status: "held", reason: "broker_uncertain"})

    held_pod =
      failed_pod
      |> put_in(["status", "containerStatuses", Access.at(0), "state", "terminated", "exitCode"], 2)
      |> put_in(["status", "containerStatuses", Access.at(0), "state", "terminated", "message"], Jason.encode!(result))

    assert {:ok, observation} = read(assignment, config, failed_job, [held_pod], uid)
    assert observation.exit_code == 2

    held_with_head =
      put_in(
        held_pod,
        ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"],
        Jason.encode!(%{result | head_oid: String.duplicate("a", 40)})
      )

    assert {:held, :job_result_pod_or_receipt_unverified} =
             read(assignment, config, failed_job, [held_with_head], uid)

    wrong_exit = put_in(held_pod, ["status", "containerStatuses", Access.at(0), "state", "terminated", "exitCode"], 1)
    assert {:held, :job_result_pod_or_receipt_unverified} = read(assignment, config, failed_job, [wrong_exit], uid)

    failed_result = Jason.encode!(%{result | status: "failed"})
    failed = put_in(wrong_exit, ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"], failed_result)
    assert {:ok, %{exit_code: 1}} = read(assignment, config, failed_job, [failed], uid)

    for changed <- [
          %{result | head_oid: String.duplicate("a", 40)},
          %{result | broker_lease_id: "publish-1"},
          %{result | checkout_revocation: "confirmed"},
          %{result | revocation: "confirmed"}
        ] do
      forged =
        put_in(
          failed,
          ["status", "containerStatuses", Access.at(0), "state", "terminated", "message"],
          Jason.encode!(%{changed | status: "failed"})
        )

      assert {:held, :job_result_pod_or_receipt_unverified} =
               read(assignment, config, failed_job, [forged], uid)
    end
  end

  defp read(assignment, config, job, pods, uid) do
    ResultReader.read(assignment, uid,
      client: Client,
      client_context: %{job: {:ok, job}, pods: {:ok, %{items: pods, resource_version: "list-rv-9"}}},
      config: config
    )
  end

  defp fixture(mode \\ :preflight) do
    assignment = assignment()

    config = %{
      namespace: "symphony-beta",
      image: "registry.example/symphony-worker@sha256:" <> String.duplicate("a", 64),
      repository_id: "123456789"
    }

    config =
      if mode == :codex do
        slot = %{
          slot_id: "slot-1",
          claim_name: "codex-auth-slot-1",
          claim_uid: "pvc-uid-one",
          lease_id: "auth-lease-1",
          assignment_sha256: assignment.sha256,
          seat: assignment.seat
        }

        Map.merge(config, %{auth_slot: slot, auth_slot_catalog: %{"slot-1" => "codex-auth-slot-1"}})
      else
        config
      end

    {:ok, expected} = JobSpec.compile(assignment, config)

    {:ok, agent} = Agent.start_link(fn -> %{jobs: %{}, creates: 0, create_error: nil, create_commit?: true} end)
    {:ok, created} = SymphonyElixir.RKE2JobFakeClient.create_job(config.namespace, expected, agent)
    Agent.stop(agent)
    uid = created["metadata"]["uid"]

    job =
      created
      |> put_in(["metadata", "resourceVersion"], "job-rv-7")
      |> put_in(["spec", "suspend"], false)
      |> Map.put("status", %{"conditions" => [%{"type" => "Complete", "status" => "True"}]})

    pod = %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "namespace" => config.namespace,
        "name" => expected["metadata"]["name"] <> "-a",
        "uid" => "pod-uid-1",
        "resourceVersion" => "pod-rv-8",
        "labels" => %{"batch.kubernetes.io/job-name" => expected["metadata"]["name"]},
        "ownerReferences" => [%{"apiVersion" => "batch/v1", "kind" => "Job", "name" => expected["metadata"]["name"], "uid" => uid, "controller" => true}]
      },
      "status" => %{
        "phase" => "Succeeded",
        "containerStatuses" => [
          %{
            "name" => "symphony-worker",
            "ready" => false,
            "state" => %{"terminated" => %{"exitCode" => 0, "message" => Jason.encode!(receipt(assignment))}}
          }
        ]
      }
    }

    {assignment, config, job, pod, uid}
  end

  defp receipt(assignment) do
    CLI.base_result(
      %{
        subject: %{
          assignmentDigest: assignment.sha256,
          issueUuid: assignment.lease.issue_id,
          generation: assignment.lease.generation,
          repositoryRef: assignment.repository_ref,
          branchRef: "refs/heads/" <> assignment.branch
        }
      },
      "preflight_passed",
      "auth_slot_required"
    )
  end

  defp assignment do
    {:ok, assignment} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-result-reader",
        seat: "runner-17",
        lease: %{issue_id: "issue-1", repository: "hypergridau/symphony", generation: 4, session_id: "worker:issue-1:4", process_id: "worker:issue-1:4"},
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Assignment bundle", evidence: "Focused test coverage"},
        context_secret_refs: [],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    assignment
  end
end
