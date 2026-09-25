Code.require_file("../support/rke2_job_fake_client.exs", __DIR__)

defmodule SymphonyElixir.RKE2JobProviderTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{JobSpec, Provider}

  setup do
    {:ok, client} = Agent.start_link(fn -> %{jobs: %{}, creates: 0, deletes: []} end)
    %{client: client}
  end

  test "compiles only a validated internal beta RKE2 assignment into a deterministic restricted Job" do
    assignment = assignment()
    config = config()

    assert {:ok, job} = JobSpec.compile(assignment, config)
    assert {:ok, ^job} = JobSpec.compile(assignment, config)
    assert {:ok, next_generation} = JobSpec.compile(assignment(5), config)
    refute next_generation["metadata"]["name"] == job["metadata"]["name"]
    assert job["metadata"]["name"] =~ ~r/\Asymphony-[a-f0-9]{24}\z/
    assert job["metadata"]["annotations"]["symphony.hypergrid.au/assignment-sha256"] == assignment.sha256

    container = get_in(job, ["spec", "template", "spec", "containers"]) |> hd()
    pod = get_in(job, ["spec", "template", "spec"])
    assert container["image"] == config.image
    assert container["command"] == ["/usr/local/bin/symphony-worker"]
    assert container["securityContext"]["allowPrivilegeEscalation"] == false
    assert container["securityContext"]["readOnlyRootFilesystem"] == true
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
    assert pod["automountServiceAccountToken"] == false

    assert pod["volumes"]
           |> Enum.map(&(Map.keys(&1) |> MapSet.new()))
           |> Enum.all?(&(&1 == MapSet.new(["name", "emptyDir"])))

    refute inspect(job) =~ "DAHLIA_WORK_PACKAGE_RUNNER_TOKEN_VALUE"
    refute inspect(job) =~ "hostPath"
    refute inspect(job) =~ "persistentVolumeClaim"
    refute inspect(job) =~ "serviceAccountName"
  end

  test "rejects wrong placement, changed assignment, and unpinned or invalid trusted config" do
    assignment = assignment()

    wrong_target = %{assignment | environment: %{assignment.environment | target_environment: :lke}}
    assert {:error, :assignment_bundle_environment_invalid} = JobSpec.compile(wrong_target, config())

    assert {:error, :assignment_bundle_digest_mismatch} =
             JobSpec.compile(%{assignment | branch: "codex/changed"}, config())

    assert {:error, :assignment_bundle_digest_mismatch} =
             JobSpec.compile(Map.put(assignment, :command, ["/bin/sh", "-c", "echo unsafe"]), config())

    assert {:error, :rke2_job_trusted_config_invalid} = JobSpec.compile(assignment, %{config() | image: "worker:latest"})
    assert {:error, :rke2_job_trusted_config_invalid} = JobSpec.compile(assignment, %{config() | namespace: "Prod"})
  end

  test "create replay reads and verifies the deterministic existing Job", %{client: client} do
    assignment = assignment()
    opts = opts(client)

    assert {:ok, created} = Provider.ensure(assignment, opts)
    assert {:ok, ^created} = Provider.ensure(assignment, opts)
    assert Agent.get(client, & &1.creates) == 1
  end

  test "rejects a tampered bundle before the client can create anything", %{client: client} do
    assert {:error, :assignment_bundle_digest_mismatch} =
             Provider.ensure(%{assignment() | branch: "codex/tampered"}, opts(client))

    assert Agent.get(client, & &1.creates) == 0
  end

  test "holds a colliding Job with a different identity or spec", %{client: client} do
    assignment = assignment()
    {:ok, expected} = JobSpec.compile(assignment, config())
    foreign = put_in(expected, ["spec", "backoffLimit"], 7)
    Agent.update(client, &put_in(&1, [:jobs, {config().namespace, expected["metadata"]["name"]}], foreign))

    assert {:held, :job_identity_or_spec_mismatch} = Provider.ensure(assignment, opts(client))
  end

  test "delete requires the exact assignment identity and server UID", %{client: client} do
    assignment = assignment()
    assert {:ok, job} = Provider.ensure(assignment, opts(client))
    stored = put_in(job, ["metadata", "uid"], "uid-exact")
    key = {config().namespace, job["metadata"]["name"]}
    Agent.update(client, &put_in(&1, [:jobs, key], stored))

    assert :ok = Provider.delete(assignment, opts(client))
    assert Agent.get(client, & &1.deletes) == ["uid-exact"]
    assert {:held, :job_not_found_for_delete} = Provider.delete(assignment, opts(client))
  end

  test "delete holds an existing Job whose recorded spec no longer matches", %{client: client} do
    assignment = assignment()
    assert {:ok, job} = Provider.ensure(assignment, opts(client))
    key = {config().namespace, job["metadata"]["name"]}
    foreign = job |> put_in(["metadata", "uid"], "uid-foreign") |> put_in(["spec", "backoffLimit"], 1)
    Agent.update(client, &put_in(&1, [:jobs, key], foreign))

    assert {:held, :job_identity_or_spec_mismatch} = Provider.delete(assignment, opts(client))
    assert Agent.get(client, & &1.deletes) == []
  end

  defp assignment(generation \\ 4) do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-rke2-job-provider",
        seat: "runner-17",
        lease: %{issue_id: "issue-1", repository: "hypergridau/symphony", generation: generation, session_id: "worker:issue-1:4", process_id: "worker:issue-1:4"},
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Assignment bundle", evidence: "Focused test coverage"},
        context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end

  defp config, do: %{namespace: "symphony-beta", image: "registry.example/symphony-worker@sha256:" <> String.duplicate("a", 64)}
  defp opts(client), do: [client: SymphonyElixir.RKE2JobFakeClient, client_context: client, config: config()]
end
