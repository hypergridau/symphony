defmodule SymphonyElixir.WorkerAssignmentTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.Worker.Assignment

  test "decodes a validated Job argument into the exact broker subject" do
    bundle = bundle()
    assert {:ok, decoded} = Assignment.decode(Jason.encode!(bundle), bundle.sha256, "123456789")
    assert decoded.bundle == bundle

    assert decoded.subject == %{
             assignmentDigest: bundle.sha256,
             issueUuid: bundle.lease.issue_id,
             generation: bundle.lease.generation,
             runnerId: bundle.seat,
             repositoryId: "123456789",
             repositoryRef: bundle.repository_ref,
             branchRef: "refs/heads/" <> bundle.branch
           }
  end

  test "rejects changed assignment bytes, env digest, and repository selector" do
    bundle = bundle()
    json = Jason.encode!(bundle)
    changed = Jason.encode!(Map.put(bundle, :branch, "codex/other"))

    assert invalid(changed, bundle.sha256, "123456789")
    assert invalid(json, String.duplicate("0", 64), "123456789")
    assert invalid(json, bundle.sha256, "0")
    assert invalid(json, bundle.sha256, "01")
    assert invalid(json, bundle.sha256, "1x")
    assert invalid(json, bundle.sha256, String.duplicate("1", 21))
  end

  test "rejects unknown keys, malformed environment, and oversized input" do
    bundle = bundle()
    json = Jason.encode!(bundle)

    assert invalid(Jason.encode!(Map.put(bundle, :unexpected, true)), bundle.sha256, "123456789")
    assert invalid(Jason.encode!(put_in(bundle, [:environment, :placement], "other")), bundle.sha256, "123456789")
    assert invalid("{", bundle.sha256, "123456789")
    assert invalid(String.duplicate(" ", 32_769), bundle.sha256, "123456789")
    assert invalid(json, bundle.sha256, nil)
  end

  defp invalid(json, digest, repository_id),
    do: Assignment.decode(json, digest, repository_id) == {:error, :invalid_worker_assignment}

  defp bundle do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Add a bounded canary file"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-canary",
        seat: "runner-17",
        lease: %{
          issue_id: "937400ab-b95e-4ddb-8adf-e28bf13c3852",
          repository: "hypergridau/symphony",
          generation: 4,
          session_id: "worker:hgs729:4",
          process_id: "worker:hgs729:4"
        },
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Canary file", evidence: "Focused test coverage"},
        context_secret_refs: [],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end
end
