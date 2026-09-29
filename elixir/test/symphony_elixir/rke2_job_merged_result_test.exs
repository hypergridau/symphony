defmodule SymphonyElixir.RKE2Job.MergedResultTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{MergedResult, MergedResultEvidence}

  @head String.duplicate("a", 40)
  @prior String.duplicate("b", 40)
  @base String.duplicate("c", 40)
  @merge String.duplicate("d", 40)

  test "accepts only the merged PR from the journaled completed assignment" do
    assignment = assignment()
    observation = observation(assignment)
    pr = pr(assignment)

    assert {:ok, %{accepted_head: @head, merge_identity: @merge}} =
             MergedResult.accept(assignment, observation, pr)

    for changed <- [
          Map.put(pr, "state", "OPEN"),
          Map.put(pr, "headRefOid", @prior),
          Map.put(pr, "headRefName", "another-branch"),
          Map.put(pr, "baseRefName", "another-base"),
          Map.put(pr, "number", 8),
          Map.put(pr, "url", "https://github.com/other/repo/pull/7"),
          Map.put(pr, "mergedAt", nil),
          Map.put(pr, "mergeCommit", nil)
        ] do
      assert {:error, :disposable_merge_unavailable} = MergedResult.accept(assignment, observation, changed)
    end
  end

  test "rejects failed, stale and malformed result observations" do
    assignment = assignment()
    observation = observation(assignment)
    pr = pr(assignment)

    for changed <- [
          put_in(observation, ["result", "status"], "failed"),
          put_in(observation, ["result", "generation"], assignment.lease.generation + 1),
          put_in(observation, ["result", "assignment_digest"], String.duplicate("e", 64)),
          put_in(observation, ["result", "head_oid"], @prior),
          Map.put(observation, "exit_code", 1),
          %{}
        ] do
      assert {:error, :disposable_merge_unavailable} = MergedResult.accept(assignment, changed, pr)
    end
  end

  test "reads the exact PR number without a local worktree" do
    assignment = assignment()
    observation = observation(assignment)
    expected = ["pr", "view", "7", "--repo", assignment.repository_ref, "--json", "number,url,state,headRefName,headRefOid,baseRefName,mergeCommit,mergedAt"]

    runner = fn executable, args ->
      assert executable == "gh"
      assert args == expected
      {Jason.encode!(pr(assignment)), 0}
    end

    assert {:ok, %{accepted_head: @head}} =
             MergedResultEvidence.observe(assignment, observation, command_runner: runner)

    assert {:error, :disposable_merge_evidence_unavailable} =
             MergedResultEvidence.observe(assignment, observation, command_runner: fn _, _ -> {"", 1} end)
  end

  defp assignment do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run one disposable worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-disposable",
        seat: "runner-17",
        lease: %{issue_id: "issue-1", repository: "hypergridau/symphony", generation: 4, session_id: "worker:issue-1:4", process_id: "worker:issue-1:4"},
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Pull request", evidence: "Exact merged result"},
        context_secret_refs: [],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end

  defp observation(assignment) do
    %{
      "exit_code" => 0,
      "result" => %{
        "schema_version" => 1,
        "status" => "completed",
        "reason" => "pull_request_created",
        "assignment_digest" => assignment.sha256,
        "issue_uuid" => assignment.lease.issue_id,
        "generation" => assignment.lease.generation,
        "repository_ref" => assignment.repository_ref,
        "branch_ref" => "refs/heads/" <> assignment.branch,
        "checkout_lease_id" => "checkout-1",
        "checkout_revocation" => "confirmed",
        "broker_lease_id" => "broker-1",
        "revocation" => "confirmed",
        "codex_exit_code" => 0,
        "head_oid" => @head,
        "branch_head_oid" => @prior,
        "base_oid" => @base,
        "changed_files" => 1,
        "pull_request_number" => 7,
        "pull_request_url" => "https://github.com/#{assignment.repository_ref}/pull/7"
      }
    }
  end

  defp pr(assignment) do
    %{
      "number" => 7,
      "url" => "https://github.com/#{assignment.repository_ref}/pull/7",
      "state" => "MERGED",
      "headRefName" => assignment.branch,
      "headRefOid" => @head,
      "baseRefName" => "main",
      "mergeCommit" => %{"oid" => @merge},
      "mergedAt" => "2026-09-29T10:00:00Z"
    }
  end
end
