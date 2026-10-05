defmodule SymphonyElixir.RKE2JobPreSpawnAuthorizationTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{JobSpec, ManagedExecutorAdapter}

  defmodule Client do
    def create_job(_namespace, _job, _context) do
      send(self(), :adapter_create)
      {:error, :unexpected}
    end

    def get_job(_namespace, _name, _context), do: {:error, :unexpected}
    def list_pods(_namespace, _context), do: {:error, :unexpected}

    def activate_job(_namespace, _name, _uid, _resource_version, _context) do
      send(self(), :adapter_activation)
      {:error, :unexpected}
    end

    def delete_job(_namespace, _name, _uid, _context) do
      send(self(), :adapter_delete)
      {:error, :unexpected}
    end
  end

  defmodule ClientContext do
    def client_context(_assignment, _operation, _key, _context), do: {:ok, :unused}
  end

  defmodule SlotGuard do
    def authorize_pre_spawn(slot, assignment, allocation, {owner, result}) do
      send(owner, {:slot_preflight, slot, assignment.sha256, allocation.id})
      result
    end
  end

  @slot %{
    slot_id: "slot-one",
    claim_name: "codex-home-one",
    claim_uid: "pvc-uid-one",
    lease_id: "12345678-1234-4123-8123-123456789abc",
    assignment_sha256: nil,
    seat: "runner-17"
  }

  test "preflight checks the exact allocation and propagates only the typed slot denial" do
    assignment = assignment()
    slot = slot(assignment)
    allocation = allocation(assignment, config(slot))
    context = context(config(slot), {self(), {:denied, :codex_auth_slot_denied}})
    key = assignment.sha256 <> ":preflight"

    assert :ok =
             ManagedExecutorAdapter.preflight_owned(
               allocation,
               assignment,
               key,
               context(config(slot), {self(), :ok})
             )

    assert_receive {:slot_preflight, ^slot, digest, allocation_id}
    assert digest == assignment.sha256
    assert allocation_id == allocation.id

    assert {:denied, :codex_auth_slot_denied} =
             ManagedExecutorAdapter.preflight_owned(allocation, assignment, key, context)

    assert_receive {:slot_preflight, ^slot, digest, allocation_id}
    assert digest == assignment.sha256
    assert allocation_id == allocation.id
    refute_receive :adapter_create
    refute_receive :adapter_activation
    refute_receive :adapter_delete
  end

  test "preflight holds missing guards and invalid identity without activation or deletion" do
    assignment = assignment()
    slot = slot(assignment)
    allocation = allocation(assignment, config(slot))
    context = context(config(slot), {self(), :ok})

    assert {:error, :invalid_rke2_job_idempotency_key} =
             ManagedExecutorAdapter.preflight_owned(
               allocation,
               assignment,
               assignment.sha256 <> ":activate",
               context
             )

    assert {:error, :job_allocation_identity_mismatch} =
             ManagedExecutorAdapter.preflight_owned(
               %{allocation | id: "rke2job:v1:forged"},
               assignment,
               assignment.sha256 <> ":preflight",
               context
             )

    assert {:held, :auth_slot_lease_guard_missing} =
             ManagedExecutorAdapter.preflight_owned(
               allocation,
               assignment,
               assignment.sha256 <> ":preflight",
               Map.delete(context, :auth_slot_lease_guard)
             )

    refute_receive {:slot_preflight, _, _, _}
    refute_receive {:adapter_activation, _}
  end

  test "unslotted allocations pass the preflight without a slot guard" do
    assignment = assignment()
    config = config(nil)
    allocation = allocation(assignment, config)

    assert :ok =
             ManagedExecutorAdapter.preflight_owned(
               allocation,
               assignment,
               assignment.sha256 <> ":preflight",
               context(config, {self(), :ok}) |> Map.delete(:auth_slot_lease_guard)
             )
  end

  defp context(config, slot_result) do
    %{
      client: Client,
      client_context_provider: ClientContext,
      client_context_provider_context: nil,
      auth_slot_lease_guard: SlotGuard,
      auth_slot_lease_guard_context: slot_result,
      config: config
    }
  end

  defp slot(assignment), do: %{@slot | assignment_sha256: assignment.sha256}

  defp config(slot) do
    %{
      namespace: "frigga",
      image: "registry.example/symphony-worker@sha256:" <> String.duplicate("a", 64),
      repository_id: "123456789",
      auth_slot: slot,
      auth_slot_catalog: if(is_map(slot), do: %{slot.slot_id => slot.claim_name}, else: nil)
    }
  end

  defp allocation(assignment, config) do
    {:ok, expected} = JobSpec.compile(assignment, config)
    namespace = expected["metadata"]["namespace"]
    name = expected["metadata"]["name"]
    digest = expected["metadata"]["annotations"]["symphony.hypergrid.au/assignment-sha256"]
    encoded = Jason.encode!([1, namespace, name, "job-uid-one", digest]) |> Base.url_encode64(padding: false)
    %{id: "rke2job:v1:" <> encoded, status: :ready}
  end

  defp assignment do
    {:ok, assignment} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run one bounded task"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs733-pre-spawn-guard",
        seat: "runner-17",
        lease: %{
          issue_id: "issue-1",
          repository: "hypergridau/symphony",
          generation: 4,
          session_id: "worker:issue-1:4",
          process_id: "worker:issue-1:4"
        },
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Pre-spawn guard", evidence: "Synthetic source tests"},
        context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    assignment
  end
end
