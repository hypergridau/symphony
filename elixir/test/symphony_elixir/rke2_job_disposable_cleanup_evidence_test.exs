defmodule SymphonyElixir.RKE2JobDisposableCleanupEvidenceTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.{ExecutionFence, ManagedAssignmentBundle, Orchestrator, WorkPackageClaim}
  alias SymphonyElixir.ExecutionFence.Persistence
  alias SymphonyElixir.RKE2Job.{DisposableCleanupEvidence, ResultJournal, TerminalLease}
  alias SymphonyElixir.Worker.CLI

  @issue "issue-1"
  @repository "hypergridau/symphony"
  @branch "codex/hgs729-disposable"
  @head String.duplicate("a", 40)
  @merge String.duplicate("b", 40)

  setup do
    if match?({:win32, _}, :os.type()), do: Process.put(:result_journal_windows_test_only, true)
    root = Path.join(System.tmp_dir!(), "disposable-cleanup-#{System.unique_integer([:positive])}")
    :ok = File.mkdir_p(root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(root, 0o700)
    on_exit(fn -> File.rm_rf(root) end)
    %{root: root}
  end

  test "finalized disposable teardown yields stable exact cleanup evidence", %{root: root} do
    {runtime, fence, token, assignment, observation} = fixture(root)

    assert :missing = ResultJournal.load_finalization(assignment, "job-uid-1", root)

    assert {:error, :disposable_cleanup_evidence_unverified} =
             DisposableCleanupEvidence.verify(runtime, fence, token, @head)

    assert {:ok, _} = ResultJournal.record_finalization(assignment, "job-uid-1", root)
    assert {:ok, evidence_ref} = DisposableCleanupEvidence.verify(runtime, fence, token, @head)
    assert {:ok, ^evidence_ref} = DisposableCleanupEvidence.verify(runtime, fence, token, @head)
    assert String.starts_with?(evidence_ref, "sha256:")
    assert byte_size(evidence_ref) == 71
    assert {:ok, reservation} = DisposableCleanupEvidence.reservation(runtime, token)
    assert reservation.assignment_snapshot

    assert {:error, :disposable_cleanup_evidence_unverified} =
             DisposableCleanupEvidence.verify(runtime, fence, token, @merge)

    assert {:error, :disposable_cleanup_evidence_unverified} =
             DisposableCleanupEvidence.verify(runtime, fence, %{token | generation: 2}, @head)

    marker_path = Path.join(root, assignment.sha256 <> "-job-uid-1.json.finalized")
    changed = Jason.decode!(File.read!(marker_path)) |> Map.put("pod_uid", "another-pod")
    :ok = File.write(marker_path, Jason.encode!(changed))

    assert {:error, :disposable_cleanup_evidence_unverified} =
             DisposableCleanupEvidence.verify(runtime, fence, token, @head)

    assert {:ok, _} = ResultJournal.load(assignment, observation.job_uid, root)

    assert {:ok, journal} = WorkPackageClaim.Journal.load(runtime.journal_path)
    key = WorkPackageClaim.Journal.reservation_key(@issue, "profile-1", @repository, 1)
    suspended = put_in(journal.reservations[key], [:dispatch, :phase], "allocation_suspended")
    {:ok, suspended_journal} = WorkPackageClaim.Journal.put(journal, key, suspended)
    assert :ok = WorkPackageClaim.Journal.save(runtime.journal_path, suspended_journal)
    assert {:error, :disposable_cleanup_not_started} = DisposableCleanupEvidence.reservation(runtime, token)

    no_snapshot = Map.delete(journal.reservations[key], :assignment_snapshot)
    {:ok, ambiguous_journal} = WorkPackageClaim.Journal.put(journal, key, no_snapshot)
    assert :ok = WorkPackageClaim.Journal.save(runtime.journal_path, ambiguous_journal)

    assert {:error, :disposable_cleanup_assignment_missing} =
             DisposableCleanupEvidence.reservation(runtime, token)

    local = put_in(no_snapshot, [:dispatch, :allocation_id], nil)
    {:ok, local_journal} = WorkPackageClaim.Journal.put(ambiguous_journal, key, local)

    assert :ok = WorkPackageClaim.Journal.save(runtime.journal_path, local_journal)
    assert :local = DisposableCleanupEvidence.reservation(runtime, token)
  end

  test "orchestrator projects both existing provider receipts and replays without a guest archive", %{root: root} do
    {runtime, fence, token, assignment, _observation} = fixture(root)
    assert {:ok, _} = ResultJournal.record_finalization(assignment, "job-uid-1", root)
    parent = self()

    request = fn _url, options ->
      payload = Keyword.fetch!(options, :json)
      kind = payload["receiptKind"]
      send(parent, {:cleanup_receipt_posted, kind})

      released = kind == "repository_cleanup_verified"

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "data" => %{
             "projectionId" => "projection-1",
             "reservationId" => payload["reservationId"],
             "receiptId" => payload["receiptId"],
             "receiptKind" => kind,
             "executionCapacityState" => "released",
             "scopeState" => if(released, do: "released", else: "held"),
             "reservationState" => if(released, do: "released", else: "claimed"),
             "generation" => 1,
             "evidenceRef" => payload["evidenceRef"],
             "acceptedHead" => payload["acceptedHead"],
             "replayed" => false
           }
         }
       }}
    end

    runtime =
      Map.merge(runtime, %{
        base_url: "http://provider.test",
        runner_token: "runner-token",
        attestation_key: "attestation-key",
        runner_id: "runner-17",
        request_fun: request,
        now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end,
        cleanup_evidence_fun: fn _, _, _ -> flunk("disposable cleanup must not use the guest archive") end
      })

    state = %Orchestrator.State{
      execution_fence: fence,
      execution_fence_path: Path.join(root, "fence.json"),
      responsibility_graph: %{delegations: %{"delegation-1" => %{status: :completed}}},
      work_package_runtime: runtime
    }

    entry = %{
      issue: %SymphonyElixir.Tracker.Issue{id: @issue, identifier: "HGS-729", state: "Done"},
      execution_token: token,
      execution_session_id: "worker:issue-1:1",
      responsibility_delegation_id: "delegation-1",
      terminal_outcome: :completed
    }

    cleaned = Orchestrator.reconcile_disposable_cleanup_for_test(state, entry, assignment, @head)
    assert cleaned.execution_fence.executions[@issue].cleanup == :cleaned
    assert cleaned.execution_fence.executions[@issue].cleanup_receipt.phase == :verified
    assert_receive {:cleanup_receipt_posted, "termination_confirmed"}
    assert_receive {:cleanup_receipt_posted, "repository_cleanup_verified"}
    assert {:ok, persisted_fence} = Persistence.load(state.execution_fence_path)
    assert persisted_fence.executions[@issue].cleanup == :cleaned
    assert persisted_fence.executions[@issue].leases["worker:issue-1:1"].termination_evidence.job_uid == "job-uid-1"

    contradictory =
      put_in(persisted_fence, [:executions, @issue, :cleanup_receipt, :evidence_ref], "sha256:changed")

    assert {:error, :disposable_cleanup_evidence_unverified} =
             DisposableCleanupEvidence.verify(runtime, contradictory, token, @head)

    restarted = %{cleaned | execution_fence: persisted_fence}
    replayed = Orchestrator.reconcile_disposable_cleanup_for_test(restarted, entry, assignment, @head)
    assert replayed.execution_fence == persisted_fence
    refute_receive {:cleanup_receipt_posted, _}

    assert {:ok, journal} = WorkPackageClaim.Journal.load(runtime.journal_path)
    key = WorkPackageClaim.Journal.reservation_key(@issue, "profile-1", @repository, 1)

    assert {:ok, %{scope_state: "held"}} =
             WorkPackageClaim.Journal.cleanup_receipt_ack(journal, key, "termination_confirmed")

    assert {:ok, %{scope_state: "released", reservation_state: "released"}} =
             WorkPackageClaim.Journal.cleanup_receipt_ack(journal, key, "repository_cleanup_verified")
  end

  test "lost provider acknowledgement retains the semantic and holds cleanup", %{root: root} do
    {runtime, fence, token, assignment, _observation} = fixture(root)
    assert {:ok, _} = ResultJournal.record_finalization(assignment, "job-uid-1", root)

    runtime =
      Map.merge(runtime, %{
        base_url: "http://provider.test",
        runner_token: "runner-token",
        attestation_key: "attestation-key",
        runner_id: "runner-17",
        request_fun: fn _, _ -> {:error, :lost_response} end,
        now_fun: fn -> ~U[2026-09-06 10:00:00.000Z] end
      })

    state = %Orchestrator.State{
      execution_fence: fence,
      responsibility_graph: %{delegations: %{"delegation-1" => %{status: :completed}}},
      work_package_runtime: runtime
    }

    entry = %{
      issue: %SymphonyElixir.Tracker.Issue{id: @issue, identifier: "HGS-729", state: "Done"},
      execution_token: token,
      execution_session_id: "worker:issue-1:1",
      responsibility_delegation_id: "delegation-1",
      terminal_outcome: :completed
    }

    held = Orchestrator.reconcile_disposable_cleanup_for_test(state, entry, assignment, @head)
    assert held.execution_fence.executions[@issue].cleanup == :pending

    assert {:ok, journal} = WorkPackageClaim.Journal.load(runtime.journal_path)
    key = WorkPackageClaim.Journal.reservation_key(@issue, "profile-1", @repository, 1)

    assert {:ok, %{receipt_kind: "termination_confirmed"}} =
             WorkPackageClaim.Journal.cleanup_receipt(journal, key, "termination_confirmed")

    assert :missing = WorkPackageClaim.Journal.cleanup_receipt_ack(journal, key, "termination_confirmed")
    assert :missing = WorkPackageClaim.Journal.cleanup_receipt(journal, key, "repository_cleanup_verified")
  end

  defp fixture(root) do
    {:ok, assignment} = assignment()
    {:ok, snapshot} = ManagedAssignmentBundle.snapshot(assignment)
    reservation = reservation(snapshot)
    key = WorkPackageClaim.Journal.reservation_key(@issue, "profile-1", @repository, 1)
    {:ok, journal} = WorkPackageClaim.Journal.put(WorkPackageClaim.Journal.new(), key, reservation)
    journal_path = Path.join(root, "claims.json")
    :ok = WorkPackageClaim.Journal.save(journal_path, journal)

    runtime = %{
      journal_path: journal_path,
      managed_project_profile_id: "profile-1",
      disposable_rke2_host_config: %{result_journal_root: root}
    }

    observation = observation(assignment)
    {:ok, _} = ResultJournal.record(assignment, observation, root)

    {:ok, admitted, token} =
      ExecutionFence.admit(
        ExecutionFence.new(),
        %{issue_id: @issue, repository: @repository, branch: @branch, worktree: "/ephemeral/issue-1"},
        0
      )

    {:ok, registered, :registered} =
      ExecutionFence.register(
        admitted,
        token,
        :worker,
        %{
          session_id: "worker:issue-1:1",
          process_id: "process-1",
          branch: @branch,
          worktree: "/ephemeral/issue-1",
          linear_state: "In Progress",
          pr_state: "unopened",
          head: "unobserved",
          last_heartbeat_at: 0
        },
        0
      )

    assert {:ok, confirmed, _} = TerminalLease.confirm(registered, reservation, assignment, observation, 100)

    assert {:ok, fenced, :fenced} =
             ExecutionFence.fence(
               confirmed,
               token,
               %{terminal_state: "Done", accepted_head: @head, merge_identity: @merge},
               101
             )

    {runtime, fenced, Map.put(token, :repository_ref, @repository), assignment, observation}
  end

  defp reservation(snapshot) do
    %{
      issue_id: @issue,
      managed_project_profile_id: "profile-1",
      repository_ref: @repository,
      workspace_id: "workspace-1",
      company_id: "company-1",
      projection_id: "projection-1",
      reservation_id: "reservation-1",
      reservation_nonce: "nonce-1",
      runner_id: "runner-17",
      session_id: "worker:issue-1:1",
      process_id: "process-1",
      responsible_delegation_id: "delegation-1",
      execution_fence_token: "issue-1:1",
      runtime_lease_id: "worker:issue-1:1",
      generation: 1,
      scope_keys: ["repo:hypergridau/symphony"],
      assignment_snapshot: snapshot,
      dispatch: %{
        phase: "spawn_started",
        attempts: 1,
        retry_at_ms: 0,
        authority_digest: String.duplicate("c", 64),
        allocation_id: "rke2job:v1:allocation"
      }
    }
  end

  defp observation(assignment) do
    result =
      CLI.base_result(
        %{
          subject: %{
            assignmentDigest: assignment.sha256,
            issueUuid: @issue,
            generation: 1,
            repositoryRef: @repository,
            branchRef: "refs/heads/" <> @branch
          }
        },
        "completed",
        "pull_request_created"
      )
      |> Map.merge(%{
        checkout_lease_id: "checkout-1",
        checkout_revocation: "confirmed",
        broker_lease_id: "broker-1",
        revocation: "confirmed",
        codex_exit_code: 0,
        head_oid: @head,
        branch_head_oid: @head,
        base_oid: @merge,
        changed_files: 1,
        pull_request_number: 1,
        pull_request_url: "https://github.com/hypergridau/symphony/pull/1"
      })

    %{
      job_uid: "job-uid-1",
      job_resource_version: "job-rv-1",
      pod_uid: "pod-uid-1",
      pod_resource_version: "pod-rv-1",
      pod_list_resource_version: "pods-rv-1",
      exit_code: 0,
      result: Map.new(result, fn {key, value} -> {Atom.to_string(key), value} end)
    }
  end

  defp assignment do
    ManagedAssignmentBundle.build(%{
      objective: %{id: "objective-1", identity: "objective-1", content: "Run one disposable worker"},
      repository_ref: @repository,
      base_ref: "refs/remotes/origin/main",
      branch: @branch,
      seat: "runner-17",
      lease: %{
        issue_id: @issue,
        repository: @repository,
        generation: 1,
        session_id: "worker:issue-1:1",
        process_id: "process-1"
      },
      intent_ancestry: ["objective-root", "delegation-1"],
      acceptance: %{deliverable: "Pull request", evidence: "Terminal result"},
      context_secret_refs: [],
      platform: "linux-x86_64",
      environment_classification: "repository",
      environment_constraints: ["repository", "no-production-workload"],
      placement: :internal_beta,
      target_environment: :rke2
    })
  end
end
