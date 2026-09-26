Code.require_file("../support/rke2_job_fake_client.exs", __DIR__)

defmodule SymphonyElixir.RKE2JobProviderTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{JobSpec, Provider}

  setup do
    {:ok, client} =
      Agent.start_link(fn ->
        %{
          jobs: %{},
          creates: 0,
          deletes: [],
          create_error: nil,
          create_commit?: true,
          get_error: nil,
          delete_error: nil,
          delete_commit?: false
        }
      end)

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
    assert job["spec"]["suspend"] == true

    container = get_in(job, ["spec", "template", "spec", "containers"]) |> hd()
    pod = get_in(job, ["spec", "template", "spec"])
    assert container["image"] == config.image
    assert container["command"] == ["/usr/local/bin/symphony-worker"]
    assert container["securityContext"]["allowPrivilegeEscalation"] == false
    assert container["securityContext"]["readOnlyRootFilesystem"] == true
    assert container["securityContext"]["capabilities"]["drop"] == ["ALL"]
    assert pod["automountServiceAccountToken"] == false
    assert pod["serviceAccountName"] == "disposable-worker"

    assert Enum.find(container["volumeMounts"], &(&1["name"] == "broker-identity")) ==
             %{"name" => "broker-identity", "mountPath" => "/var/run/secrets/frigga-broker", "readOnly" => true}

    assert Enum.find(pod["volumes"], &(&1["name"] == "broker-identity")) == %{
             "name" => "broker-identity",
             "projected" => %{
               "sources" => [
                 %{
                   "serviceAccountToken" => %{
                     "audience" => "hypergrid-runner-broker",
                     "expirationSeconds" => 600,
                     "path" => "token"
                   }
                 }
               ]
             }
           }

    assert pod["volumes"]
           |> Enum.filter(&Map.has_key?(&1, "emptyDir"))
           |> length() == 2

    refute inspect(job) =~ "DAHLIA_WORK_PACKAGE_RUNNER_TOKEN_VALUE"
    refute inspect(job) =~ "hostPath"
    refute inspect(job) =~ "persistentVolumeClaim"
    refute inspect(job) =~ "GITHUB_TOKEN"
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

    attrs = %{assignment_attrs() | placement: :hosted_production, target_environment: :lke}
    assert {:ok, hosted} = ManagedAssignmentBundle.build(attrs)
    assert {:error, :rke2_job_target_not_authorized} = JobSpec.compile(hosted, config())
  end

  test "rejects invalid compiler arguments and oversized assignment payloads" do
    assert {:error, :invalid_rke2_job_assignment} = JobSpec.compile(nil, config())
    assert {:error, :invalid_rke2_job_assignment} = JobSpec.compile(assignment(), nil)

    refs = for index <- 1..32, do: "SECRET_#{index}_" <> String.duplicate("A", 2_000)
    assert {:ok, oversized} = ManagedAssignmentBundle.build(%{assignment_attrs() | context_secret_refs: refs})
    assert {:error, :rke2_job_assignment_too_large} = JobSpec.compile(oversized, config())
  end

  test "create replay reads and verifies the deterministic existing Job", %{client: client} do
    assignment = assignment()
    opts = opts(client)

    assert {:ok, created} = Provider.ensure(assignment, opts)
    assert {:ok, ^created} = Provider.ensure(assignment, opts)
    assert Agent.get(client, & &1.creates) == 1
  end

  test "reconciles a create timeout by reading the defaulted Job", %{client: client} do
    Agent.update(client, &%{&1 | create_error: :timeout})

    assert {:ok, created} = Provider.ensure(assignment(), opts(client))
    assert get_in(created, ["spec", "template", "spec", "dnsPolicy"]) == "ClusterFirst"
    assert Agent.get(client, & &1.creates) == 1
  end

  test "holds a create timeout when read-back proves no Job exists", %{client: client} do
    Agent.update(client, &%{&1 | create_error: :timeout, create_commit?: false})

    assert {:held, {:job_create_outcome_uncertain, :timeout}} = Provider.ensure(assignment(), opts(client))
    assert Agent.get(client, & &1.creates) == 1
    assert Agent.get(client, &map_size(&1.jobs)) == 0
  end

  test "reads back after malformed create response and holds when read-back fails", %{client: client} do
    Agent.update(client, &%{&1 | create_error: :invalid_response})
    assert {:ok, _job} = Provider.ensure(assignment(), opts(client))

    {:ok, other_client} =
      Agent.start_link(fn ->
        %{
          jobs: %{},
          creates: 0,
          deletes: [],
          create_error: :timeout,
          create_commit?: false,
          get_error: {:error, :timeout},
          delete_error: nil,
          delete_commit?: false
        }
      end)

    assert {:held, {:job_create_and_read_uncertain, :timeout, :timeout}} =
             Provider.ensure(assignment(), opts(other_client))
  end

  test "validates provider options before effect calls and fails closed for missing ports" do
    assert {:error, :invalid_rke2_job_request} = Provider.ensure(nil, [])
    assert {:error, :invalid_rke2_job_request} = Provider.delete(nil, [])

    assert {:error, :rke2_job_client_missing} =
             Provider.ensure(assignment(), config: config(), client: String)

    assert {:error, :rke2_job_trusted_config_missing} =
             Provider.ensure(assignment(), client: SymphonyElixir.RKE2JobFakeClient)
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

  test "holds unresolved existing-Job reads and delete adapter failures", %{client: client} do
    assignment = assignment()
    {:ok, expected} = JobSpec.compile(assignment, config())
    key = {config().namespace, expected["metadata"]["name"]}
    Agent.update(client, &put_in(&1, [:jobs, key], expected))
    Agent.update(client, &%{&1 | get_error: {:error, :timeout}})

    assert {:held, {:job_read_failed, :timeout}} = Provider.ensure(assignment, opts(client))

    Agent.update(client, &%{&1 | get_error: :malformed_response})
    assert {:held, :invalid_job_read_response} = Provider.ensure(assignment, opts(client))

    Agent.update(client, &%{&1 | get_error: nil})
    Agent.update(client, &%{&1 | jobs: Map.delete(&1.jobs, key)})
    assert {:ok, job} = Provider.ensure(assignment, opts(client))
    Agent.update(client, &%{&1 | get_error: {:error, :timeout}})
    assert {:held, {:job_read_failed, :timeout}} = Provider.delete(assignment, opts(client))

    Agent.update(client, &%{&1 | get_error: nil, delete_error: :timeout})
    assert {:held, {:job_delete_outcome_uncertain, :timeout}} = Provider.delete(assignment, opts(client))
    assert Agent.get(client, &Map.has_key?(&1.jobs, key))
    assert get_in(job, ["metadata", "uid"]) == get_in(Agent.get(client, &Map.fetch!(&1.jobs, key)), ["metadata", "uid"])
  end

  test "holds server-defaulted Jobs with security-relevant drift", %{client: client} do
    assignment = assignment()
    assert {:ok, created} = Provider.ensure(assignment, opts(client))
    key = {config().namespace, created["metadata"]["name"]}

    tampered = [
      put_in(created, ["spec", "template", "spec", "automountServiceAccountToken"], true),
      put_in(created, ["spec", "template", "spec", "serviceAccountName"], "default"),
      put_in(created, ["spec", "template", "spec", "volumes", Access.at(2), "projected", "sources", Access.at(0), "serviceAccountToken", "audience"], "kubernetes"),
      put_in(created, ["spec", "template", "spec", "hostNetwork"], true),
      put_in(created, ["spec", "template", "spec", "containers", Access.at(0), "securityContext", "allowPrivilegeEscalation"], true),
      put_in(created, ["spec", "template", "spec", "containers", Access.at(0), "image"], "other@sha256:" <> String.duplicate("b", 64))
    ]

    for job <- tampered do
      Agent.update(client, &put_in(&1, [:jobs, key], job))
      assert {:held, :job_identity_or_spec_mismatch} = Provider.ensure(assignment, opts(client))
    end
  end

  test "delete requires the exact assignment identity and server UID", %{client: client} do
    assignment = assignment()
    assert {:ok, job} = Provider.ensure(assignment, opts(client))
    uid = get_in(job, ["metadata", "uid"])

    assert :ok = Provider.delete(assignment, opts(client))
    assert Agent.get(client, & &1.deletes) == [uid]
    assert {:held, :job_not_found_for_delete} = Provider.delete(assignment, opts(client))
  end

  test "an accepted delete stays pending until the exact Job is absent", %{client: client} do
    assignment = assignment()
    assert {:ok, job} = Provider.ensure(assignment, opts(client))
    uid = get_in(job, ["metadata", "uid"])
    Agent.update(client, &Map.put(&1, :delete_pending?, true))

    assert {:held, :job_delete_pending} = Provider.delete_owned(assignment, uid, opts(client))
    assert Agent.get(client, & &1.deletes) == [uid]
    assert Agent.get(client, &(&1.jobs != %{}))
    {:ok, expected} = JobSpec.compile(assignment, config())
    key = {config().namespace, job["metadata"]["name"]}
    terminating = Agent.get(client, &Map.fetch!(&1.jobs, key))
    refute JobSpec.owned_job?(terminating, expected)
    assert JobSpec.owned_job_for_cleanup?(terminating, expected)
    refute JobSpec.owned_job_for_cleanup?(put_in(terminating, ["metadata", "finalizers"], ["foreign"]), expected)

    Agent.update(client, &Map.put(&1, :delete_pending?, false))
    assert :ok = Provider.delete_owned(assignment, uid, opts(client))
    assert Agent.get(client, & &1.jobs) == %{}
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
    attrs = assignment_attrs()
    attrs = %{attrs | lease: %{attrs.lease | generation: generation}}

    {:ok, bundle} =
      ManagedAssignmentBundle.build(attrs)

    bundle
  end

  defp assignment_attrs do
    %{
      objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
      repository_ref: "hypergridau/symphony",
      base_ref: "refs/remotes/origin/main",
      branch: "codex/hgs729-rke2-job-provider",
      seat: "runner-17",
      lease: %{
        issue_id: "issue-1",
        repository: "hypergridau/symphony",
        generation: 4,
        session_id: "worker:issue-1:4",
        process_id: "worker:issue-1:4"
      },
      intent_ancestry: ["objective-root", "delegation-1"],
      acceptance: %{deliverable: "Assignment bundle", evidence: "Focused test coverage"},
      context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
      platform: "linux-x86_64",
      environment_classification: "repository",
      environment_constraints: ["repository", "no-production-workload"],
      placement: :internal_beta,
      target_environment: :rke2
    }
  end

  defp config, do: %{namespace: "symphony-beta", image: "registry.example/symphony-worker@sha256:" <> String.duplicate("a", 64)}
  defp opts(client), do: [client: SymphonyElixir.RKE2JobFakeClient, client_context: client, config: config()]
end
