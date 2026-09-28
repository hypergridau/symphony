defmodule SymphonyElixir.ManagedExecutorAbortResultPublisherTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.ManagedExecutor.{AbortResultJournal, AbortResultPublisher}

  setup do
    if match?({:win32, _}, :os.type()), do: Process.put(:abort_result_journal_windows_test_only, true)

    root = Path.join(File.cwd!(), ".tmp-abort-publisher-#{System.unique_integer([:positive])}")
    :ok = File.mkdir_p(root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(root, 0o700)
    on_exit(fn -> File.rm_rf(root) end)
    %{root: root}
  end

  test "publishes exact typed bytes and returns the same reference on replay", %{root: root} do
    assignment = assignment()
    allocation = %{id: "allocation-8", status: :ready}
    result = blocked_result(assignment)
    key = assignment.sha256 <> ":abort-result"
    reference = reference(key)
    context = context(root, assignment)

    assert {:ok, ^reference} =
             AbortResultPublisher.publish_or_reconcile_abort_result(allocation, assignment, result, key, context)

    expected_bytes = blocked_wire_bytes(assignment, allocation, result, reference)
    expected_digest = sha256(expected_bytes)
    binding = binding(assignment, allocation)

    assert {:ok, ^expected_bytes} = AbortResultJournal.load(root, reference, binding, expected_digest)

    assert {:ok, ^reference} =
             AbortResultPublisher.publish_or_reconcile_abort_result(allocation, assignment, result, key, context)

    assert {:ok, ^expected_bytes} = AbortResultJournal.load(root, reference, binding, expected_digest)
  end

  test "holds a changed allocation under the same stable reference", %{root: root} do
    assignment = assignment()
    result = blocked_result(assignment)
    key = assignment.sha256 <> ":abort-result"
    original = %{id: "allocation-8", status: :ready}
    changed = %{id: "allocation-9", status: :ready}
    context = context(root, assignment)
    reference = reference(key)

    assert {:ok, ^reference} =
             AbortResultPublisher.publish_or_reconcile_abort_result(original, assignment, result, key, context)

    assert {:error, :abort_result_journal_conflict} =
             AbortResultPublisher.publish_or_reconcile_abort_result(changed, assignment, result, key, context)

    bytes = blocked_wire_bytes(assignment, original, result, reference)
    assert {:ok, ^bytes} = AbortResultJournal.load(root, reference, binding(assignment, original), sha256(bytes))
  end

  test "rejects an unsupported abort reason before creating a journal record", %{root: root} do
    assignment = assignment()
    allocation = %{id: "allocation-8", status: :ready}
    key = assignment.sha256 <> ":abort-result"
    reference = reference(key)

    result = %{
      assignment_digest: assignment.sha256,
      abort_reason: :other,
      outcome: :blocked,
      summary: "Assignment was blocked before execution.",
      evidence_ref: "managed-executor:#{assignment.sha256}:other"
    }

    assert {:error, :pre_execution_result_invalid} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               assignment,
               result,
               key,
               context(root, assignment)
             )

    assert :missing =
             AbortResultJournal.load(
               root,
               reference,
               binding(assignment, allocation),
               sha256(blocked_wire_bytes(assignment, allocation, result, reference))
             )
  end

  test "missing and relative roots fail closed without creating records", %{root: root} do
    assignment = assignment()
    allocation = %{id: "allocation-8", status: :ready}
    result = blocked_result(assignment)
    key = assignment.sha256 <> ":abort-result"
    reference = reference(key)

    assert {:error, :abort_result_journal_root_unavailable} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               assignment,
               result,
               key,
               %{claim_binding: claim_binding(assignment)}
             )

    assert {:error, :invalid_abort_result_journal_root} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               assignment,
               result,
               key,
               context("relative-root", assignment)
             )

    assert :missing =
             AbortResultJournal.load(
               root,
               reference,
               binding(assignment, allocation),
               sha256(blocked_wire_bytes(assignment, allocation, result, reference))
             )
  end

  test "rejects mismatched key, assignment, allocation and typed result before writing", %{root: root} do
    assignment = assignment()
    allocation = %{id: "allocation-8", status: :ready}
    result = blocked_result(assignment)
    key = assignment.sha256 <> ":abort-result"
    context = context(root, assignment)

    assert {:error, :abort_result_idempotency_key_mismatch} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               assignment,
               result,
               "wrong-key",
               context
             )

    assert {:error, :abort_result_claim_binding_unavailable} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               assignment,
               result,
               key,
               %{abort_result_journal_root: root}
             )

    wrong_claim = put_in(context, [:claim_binding, :reservation_id], "")

    assert {:error, :abort_result_claim_binding_invalid} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               assignment,
               result,
               key,
               wrong_claim
             )

    wrong_runner = put_in(context, [:claim_binding, :runner_id], "other-runner")

    assert {:error, :abort_result_claim_binding_invalid} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               assignment,
               result,
               key,
               wrong_runner
             )

    assert {:error, :pre_execution_result_invalid} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               assignment,
               Map.put(result, :summary, "changed"),
               key,
               context
             )

    assert {:error, :allocation_invalid} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               %{id: "", status: :ready},
               assignment,
               result,
               key,
               context
             )

    invalid_assignment = Map.put(assignment, :sha256, String.duplicate("0", 64))

    assert {:error, :assignment_bundle_digest_mismatch} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               invalid_assignment,
               result,
               key,
               context
             )

    assert :missing =
             AbortResultJournal.load(
               root,
               reference(key),
               binding(assignment, allocation),
               sha256(blocked_wire_bytes(assignment, allocation, result, reference(key)))
             )
  end

  defp assignment do
    {:ok, assignment} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-hgs733", identity: "objective-hgs733", content: "Record exact blocked bytes."},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs733-abort-result",
        seat: "runner-fixture",
        lease: %{
          issue_id: "b60d9711-d8ed-4a69-8910-570d0b4bbe7a",
          repository: "hypergridau/symphony",
          generation: 8,
          session_id: "worker:hgs733:8",
          process_id: "worker:hgs733:8"
        },
        intent_ancestry: ["objective-root", "delegation-fixture"],
        acceptance: %{deliverable: "One blocked-result journal record", evidence: "Exact bytes and binding"},
        context_secret_refs: [],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["synthetic-test-only"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    assignment
  end

  defp blocked_result(assignment) do
    %{
      assignment_digest: assignment.sha256,
      abort_reason: :checkout_preparation_failed,
      outcome: :blocked,
      summary: "Checkout preparation failed before execution.",
      evidence_ref: "managed-executor:#{assignment.sha256}:checkout_preparation_failed"
    }
  end

  defp binding(assignment, allocation) do
    %{
      assignment_digest: assignment.sha256,
      issue_uuid: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      allocation_id: allocation.id
    }
  end

  defp context(root, assignment),
    do: %{abort_result_journal_root: root, claim_binding: claim_binding(assignment)}

  defp claim_binding(assignment) do
    %{
      projection_id: "projection-8",
      reservation_id: "reservation-8",
      runner_id: "runner-fixture",
      issue_id: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      repository_ref: assignment.repository_ref
    }
  end

  defp blocked_wire_bytes(assignment, allocation, result, reference) do
    claim = claim_binding(assignment)

    Jason.encode!(%{
      "status" => "blocked",
      "reference" => reference,
      "projectionId" => claim.projection_id,
      "reservationId" => claim.reservation_id,
      "issueId" => claim.issue_id,
      "runnerId" => claim.runner_id,
      "generation" => claim.generation,
      "assignmentDigest" => assignment.sha256,
      "allocationId" => allocation.id,
      "abortReason" => Atom.to_string(result.abort_reason)
    })
  end

  defp reference(key) do
    digest = :crypto.hash(:sha256, key) |> Base.encode16(case: :lower)
    "managed-abort-result:v1:" <> digest
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
