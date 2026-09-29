defmodule SymphonyElixir.RKE2Job.TerminalLeaseTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{ExecutionFence, ManagedAssignmentBundle}
  alias SymphonyElixir.RKE2Job.TerminalLease

  test "finalized result releases the exact worker and records replayable termination proof" do
    {fence, reservation, assignment, observation} = fixture()

    assert {:ok, confirmed, evidence} = TerminalLease.confirm(fence, reservation, assignment, observation, 100)
    execution = confirmed.executions[reservation.issue_id]
    lease = execution.leases[reservation.session_id]

    assert lease.status == :released
    assert lease.termination_required
    assert lease.termination_evidence == evidence
    assert execution.ownership == :reconciled
    refute execution.termination_unconfirmed
    assert evidence.job_uid == "job-uid-1"
    assert evidence.pod_uid == "pod-uid-1"

    expected_ref =
      :crypto.hash(:sha256, assignment.sha256 <> "\0job-uid-1")
      |> Base.encode16(case: :lower)

    assert evidence.evidence_ref == "sha256:#{expected_ref}"

    assert {:ok, ^confirmed, _} = TerminalLease.confirm(confirmed, reservation, assignment, observation, 101)

    assert {:error, :disposable_terminal_lease_unverified} =
             TerminalLease.confirm(
               confirmed,
               reservation,
               assignment,
               Map.put(observation, "job_uid", "another-job"),
               101
             )
  end

  test "stale assignment, result and lease cannot terminate another generation" do
    {fence, reservation, assignment, observation} = fixture()

    for changed <- [
          %{reservation | session_id: "another"},
          %{reservation | process_id: "another"},
          %{reservation | generation: 2},
          %{reservation | execution_fence_token: "issue-1:2"}
        ] do
      assert {:error, _} = TerminalLease.confirm(fence, changed, assignment, observation, 100)
    end

    assert {:error, _} =
             TerminalLease.confirm(
               fence,
               reservation,
               assignment,
               put_in(observation, ["result", "assignment_digest"], String.duplicate("e", 64)),
               100
             )

    assert {:error, _} =
             TerminalLease.confirm(fence, reservation, assignment, Map.put(observation, "job_uid", "bad/uid"), 100)

    assert fence.executions[reservation.issue_id].leases[reservation.session_id].status == :active
  end

  defp fixture do
    issue_id = "issue-1"
    repository = "hypergridau/symphony"
    session_id = "worker:issue-1:1"
    process_id = "process-1"

    {:ok, admitted, token} =
      ExecutionFence.admit(
        ExecutionFence.new(),
        %{
          issue_id: issue_id,
          repository: repository,
          branch: "codex/hgs729-disposable",
          worktree: "/synthetic/issue-1"
        },
        0
      )

    {:ok, fence, :registered} =
      ExecutionFence.register(
        admitted,
        token,
        :worker,
        %{
          session_id: session_id,
          process_id: process_id,
          branch: "codex/hgs729-disposable",
          worktree: "/synthetic/issue-1",
          linear_state: "In Progress",
          pr_state: "none",
          head: "unobserved",
          last_heartbeat_at: 0
        },
        0
      )

    {:ok, assignment} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run one disposable worker"},
        repository_ref: repository,
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-disposable",
        seat: "runner-17",
        lease: %{
          issue_id: issue_id,
          repository: repository,
          generation: 1,
          session_id: session_id,
          process_id: process_id
        },
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Pull request", evidence: "Exact terminal result"},
        context_secret_refs: [],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    reservation = %{
      issue_id: issue_id,
      repository_ref: repository,
      generation: 1,
      session_id: session_id,
      process_id: process_id,
      runtime_lease_id: session_id,
      execution_fence_token: "issue-1:1"
    }

    observation = %{
      "job_uid" => "job-uid-1",
      "pod_uid" => "pod-uid-1",
      "exit_code" => 2,
      "result" => %{
        "schema_version" => 1,
        "status" => "held",
        "reason" => "worker_held",
        "assignment_digest" => assignment.sha256,
        "issue_uuid" => issue_id,
        "generation" => 1,
        "repository_ref" => repository,
        "branch_ref" => "refs/heads/" <> assignment.branch,
        "checkout_lease_id" => nil,
        "checkout_revocation" => "confirmed",
        "broker_lease_id" => nil,
        "revocation" => "confirmed",
        "codex_exit_code" => nil,
        "head_oid" => nil,
        "branch_head_oid" => nil,
        "base_oid" => nil,
        "changed_files" => nil,
        "pull_request_number" => nil,
        "pull_request_url" => nil
      }
    }

    {fence, reservation, assignment, observation}
  end
end
