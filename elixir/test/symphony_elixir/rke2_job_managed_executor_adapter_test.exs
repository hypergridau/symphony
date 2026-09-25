Code.require_file("../support/rke2_job_fake_client.exs", __DIR__)

defmodule SymphonyElixir.RKE2JobMalformedClientContext do
  def client_context(_assignment, _operation, _key, _context), do: :not_a_context_response
end

defmodule SymphonyElixir.RKE2JobRaisingClientContext do
  def client_context(_assignment, _operation, _key, _context), do: raise("context provider failed")
end

defmodule SymphonyElixir.RKE2JobFakeClientContext do
  @behaviour SymphonyElixir.RKE2Job.ClientContext

  @impl true
  def client_context(assignment, operation, idempotency_key, agent) do
    Agent.get_and_update(agent, fn state ->
      event = {operation, assignment.sha256, idempotency_key}

      if state.denied do
        {{:error, :synthetic_credential_denial}, %{state | events: [event | state.events]}}
      else
        {{:ok, state.client_context}, %{state | events: [event | state.events]}}
      end
    end)
  end
end

defmodule SymphonyElixir.RKE2JobManagedExecutorAdapterTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{JobSpec, ManagedExecutorAdapter}
  alias SymphonyElixir.RKE2JobFakeClient

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

    {:ok, credentials} =
      Agent.start_link(fn -> %{client_context: client, events: [], denied: false} end)

    %{client: client, credentials: credentials}
  end

  test "allocates through the provider and records exact namespace, name, digest, and UID", context do
    assignment = assignment()

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(
               assignment,
               key(assignment, :allocation),
               adapter_context(context)
             )

    assert allocation.status == :ready
    assert {:ok, uid} = allocation_uid(allocation)
    assert is_binary(uid)

    assert [{:allocate, digest, idempotency_key}] = Agent.get(context.credentials, &Enum.reverse(&1.events))
    assert digest == assignment.sha256
    assert idempotency_key == key(assignment, :allocation)
  end

  test "reconciles an ambiguous create by exact provider readback", context do
    Agent.update(context.client, &%{&1 | create_error: :timeout})
    assignment = assignment()

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(
               assignment,
               key(assignment, :allocation),
               adapter_context(context)
             )

    assert {:ok, _uid} = allocation_uid(allocation)
    assert Agent.get(context.client, & &1.creates) == 1
  end

  test "release deletes only the read-back Job with the recorded UID", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert :ok = ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)
    assert Agent.get(context.client, & &1.deletes) == [elem(allocation_uid(allocation), 1)]
    assert :ok = ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)
  end

  test "allocation stays suspended and only the cleanup path accepts an activated owned Job", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(
               assignment,
               key(assignment, :allocation),
               opts
             )

    [1, namespace, name, _uid, _digest] = allocation_payload(allocation)
    job_key = {namespace, name}
    Agent.update(context.client, &put_in(&1, [:jobs, job_key, "spec", "suspend"], false))

    assert {:held, :job_identity_or_spec_mismatch} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert :ok = ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)
  end

  test "reconciles a DELETE timeout after readback proves the allocated Job is absent", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    Agent.update(context.client, &%{&1 | delete_error: :timeout, delete_commit?: true})

    assert :ok = ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)
    assert Agent.get(context.client, &map_size(&1.jobs)) == 0
  end

  test "holds a replacement Job UID even when the replacement still matches the assignment", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:ok, expected} = JobSpec.compile(assignment, config())
    name = expected["metadata"]["name"]
    replacement = store_defaulted_job(expected, "uid-replacement-123")
    Agent.update(context.client, &put_in(&1, [:jobs, {config().namespace, name}], replacement))

    assert {:held, :job_allocation_identity_mismatch} =
             ManagedExecutorAdapter.delete_owned(allocation, assignment, key(assignment, :delete), opts)

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "rejects a forged allocation and fails closed when client context cannot be issued", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    [version, namespace, name, uid, _digest] = allocation_payload(allocation)
    forged = encode_allocation([version, namespace, name, uid, String.duplicate("0", 64)])

    assert {:error, :job_allocation_identity_mismatch} =
             ManagedExecutorAdapter.delete_owned(forged, assignment, key(assignment, :delete), opts)

    Agent.update(context.credentials, &%{&1 | denied: true})

    assert {:error, :rke2_job_client_context_unavailable} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)
  end

  test "rejects invalid assignments, keys, and adapter ports", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:error, :invalid_rke2_job_assignment} =
             ManagedExecutorAdapter.allocate_or_reconcile(nil, key(assignment, :allocation), opts)

    assert {:error, :invalid_rke2_job_idempotency_key} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, "wrong-key", opts)

    assert {:error, :rke2_job_adapter_ports_invalid} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), nil)
  end

  test "fails closed for malformed and raising client-context providers", context do
    assignment = assignment()
    key = key(assignment, :allocation)

    malformed = Map.put(adapter_context(context), :client_context_provider, SymphonyElixir.RKE2JobMalformedClientContext)
    raising = Map.put(adapter_context(context), :client_context_provider, SymphonyElixir.RKE2JobRaisingClientContext)

    assert {:error, :invalid_rke2_job_client_context_response} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key, malformed)

    assert {:error, :rke2_job_client_context_unavailable} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key, raising)
  end

  test "rejects malformed and invalid-UID allocation identifiers", context do
    assignment = assignment()
    opts = adapter_context(context)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, key(assignment, :allocation), opts)

    assert {:error, :job_allocation_identity_mismatch} =
             ManagedExecutorAdapter.delete_owned(%{id: "rke2job:v1:%%%", status: :ready}, assignment, key(assignment, :delete), opts)

    [version, namespace, name, _uid, digest] = allocation_payload(allocation)
    invalid_uid = encode_allocation([version, namespace, name, "invalid uid", digest])

    assert {:error, :job_allocation_identity_mismatch} =
             ManagedExecutorAdapter.delete_owned(invalid_uid, assignment, key(assignment, :delete), opts)
  end

  defp adapter_context(context) do
    %{
      client: RKE2JobFakeClient,
      client_context_provider: SymphonyElixir.RKE2JobFakeClientContext,
      client_context_provider_context: context.credentials,
      config: config()
    }
  end

  defp allocation_uid(allocation) do
    [1, _namespace, _name, uid, _digest] = allocation_payload(allocation)
    {:ok, uid}
  end

  defp allocation_payload(%{id: "rke2job:v1:" <> encoded, status: :ready}) do
    {:ok, payload} = Base.url_decode64(encoded, padding: false)
    {:ok, values} = Jason.decode(payload)
    values
  end

  defp encode_allocation(payload), do: %{id: "rke2job:v1:" <> (Jason.encode!(payload) |> Base.url_encode64(padding: false)), status: :ready}

  defp assignment do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-rke2-provider-adapter",
        seat: "runner-17",
        lease: %{
          issue_id: "issue-1",
          repository: "hypergridau/symphony",
          generation: 4,
          session_id: "worker:issue-1:4",
          process_id: "worker:issue-1:4"
        },
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "RKE2 Job allocation", evidence: "Fake transport tests"},
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
  defp key(assignment, stage), do: assignment.sha256 <> ":" <> Atom.to_string(stage)

  defp store_defaulted_job(job, uid) do
    name = job["metadata"]["name"]
    generated = %{"batch.kubernetes.io/controller-uid" => uid, "batch.kubernetes.io/job-name" => name}

    job
    |> put_in(["metadata", "uid"], uid)
    |> put_in(["metadata", "resourceVersion"], "18")
    |> put_in(["metadata", "generation"], 1)
    |> put_in(["metadata", "creationTimestamp"], "2026-09-26T00:00:00Z")
    |> put_in(["metadata", "managedFields"], [%{"manager" => "kube-controller-manager", "operation" => "Update"}])
    |> put_in(["metadata", "labels"], Map.merge(job["metadata"]["labels"], generated))
    |> put_in(["metadata", "annotations"], Map.put(job["metadata"]["annotations"], "batch.kubernetes.io/job-tracking", ""))
    |> put_in(["spec", "completions"], 1)
    |> put_in(["spec", "parallelism"], 1)
    |> put_in(["spec", "completionMode"], "NonIndexed")
    |> put_in(["spec", "manualSelector"], false)
    |> put_in(["spec", "suspend"], true)
    |> put_in(["spec", "podReplacementPolicy"], "TerminatingOrFailed")
    |> put_in(["spec", "selector"], %{"matchLabels" => %{"batch.kubernetes.io/controller-uid" => uid}})
    |> put_in(["spec", "template", "metadata", "creationTimestamp"], nil)
    |> put_in(["spec", "template", "metadata", "labels"], Map.merge(job["spec"]["template"]["metadata"]["labels"], generated))
    |> put_in(["spec", "template", "spec", "dnsPolicy"], "ClusterFirst")
    |> put_in(["spec", "template", "spec", "schedulerName"], "default-scheduler")
    |> put_in(["spec", "template", "spec", "terminationGracePeriodSeconds"], 30)
    |> put_in(["spec", "template", "spec", "enableServiceLinks"], true)
    |> put_in(["spec", "template", "spec", "preemptionPolicy"], "PreemptLowerPriority")
    |> put_in(["spec", "template", "spec", "serviceAccountName"], "default")
    |> put_in(["spec", "template", "spec", "containers", Access.at(0), "terminationMessagePath"], "/dev/termination-log")
    |> put_in(["spec", "template", "spec", "containers", Access.at(0), "terminationMessagePolicy"], "File")
  end
end
