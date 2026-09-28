defmodule SymphonyElixir.RKE2JobHostAllocationContextTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{HostAllocationContext, JobSpec, ResultJournal, TerminalOwner}
  alias SymphonyElixir.Worker.CLI

  defmodule ReadOnlySlotGuard do
    def verify_claim_uid(_slot, _context) do
      send(self(), :claim_uid_verified)
      :ok
    end

    def verify_bound(_slot, _assignment, _allocation, _context) do
      send(self(), :lease_binding_verified)
      :ok
    end
  end

  defmodule TerminalAdapter do
    def finalize_terminal_owned(allocation, assignment, key, context) do
      send(self(), {:terminal_finalization, allocation.id, assignment.sha256, key, context.config.auth_slot})
      {:held, :terminal_result_pending}
    end
  end

  @image "ghcr.io/hypergridau/symphony-worker@sha256:" <> String.duplicate("a", 64)
  @lease_id "12345678-1234-4123-8123-123456789abc"
  @env %{
    "SYMPHONY_RKE2_API_SERVER" => "https://10.0.14.10:6443",
    "SYMPHONY_RKE2_CREDENTIAL_ROOT" => "/etc/symphony/frigga-kubernetes",
    "SYMPHONY_RKE2_WORKER_IMAGE" => @image,
    "SYMPHONY_RKE2_REPOSITORY_ID" => "123456789",
    "SYMPHONY_RKE2_AUTH_SLOT_ID" => "slot-one",
    "SYMPHONY_RKE2_AUTH_CLAIM_NAME" => "codex-oauth-slot-1",
    "SYMPHONY_RKE2_RESULT_JOURNAL_ROOT" => "/private/symphony/job-results"
  }

  test "host settings are all-or-nothing and pinned to a manifest repository" do
    manifest = %{repository_ref: "hypergridau/symphony"}
    assert :disabled = HostAllocationContext.configuration(%{}, manifest, "https://provider.example", "host-token")

    assert {:error, {:incomplete_rke2_host_context, ["SYMPHONY_RKE2_AUTH_CLAIM_NAME"]}} =
             HostAllocationContext.configuration(Map.delete(@env, "SYMPHONY_RKE2_AUTH_CLAIM_NAME"), manifest, "https://provider.example", "host-token")

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(@env, nil, "https://provider.example", "host-token")

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(Map.put(@env, "SYMPHONY_RKE2_WORKER_IMAGE", "worker:latest"), manifest, "https://provider.example", "host-token")

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(Map.put(@env, "SYMPHONY_RKE2_RESULT_JOURNAL_ROOT", "relative/results"), manifest, "https://provider.example", "host-token")

    assert {:ok, config} = HostAllocationContext.configuration(@env, manifest, "https://provider.example", "host-token")
    assert config.image == @image
    assert config.repository_ref == "hypergridau/symphony"
    assert config.slot_id == "slot-one"
    assert config.result_journal_root == "/private/symphony/job-results"
  end

  test "prepares one exact slot-bound Job context without storing credentials in the assignment" do
    assignment = assignment()

    binding = %{
      issue_id: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      repository_ref: assignment.repository_ref,
      runner_id: assignment.seat,
      reservation_id: "reservation-one"
    }

    {:ok, base} =
      HostAllocationContext.configuration(@env, %{repository_ref: assignment.repository_ref}, "https://provider.example", "host-token")

    caller = self()

    config =
      base
      |> Map.put(:client_context_fun, fn seen, operation, key, _config ->
        send(caller, {:kube_context_requested, seen.sha256, operation, key})
        {:ok, %{synthetic: true}}
      end)
      |> Map.put(:pvc_read_fun, fn "frigga", "codex-oauth-slot-1", %{synthetic: true} ->
        {:ok,
         %{
           "apiVersion" => "v1",
           "kind" => "PersistentVolumeClaim",
           "metadata" => %{"namespace" => "frigga", "name" => "codex-oauth-slot-1", "uid" => "pvc-uid-one"},
           "status" => %{"phase" => "Bound"}
         }}
      end)
      |> Map.put(:post_fun, fn url, opts ->
        send(caller, {:slot_post, url, opts[:json]})

        {:ok,
         %Req.Response{
           status: 200,
           body: %{"data" => %{"slotId" => "slot-one", "claimName" => "codex-oauth-slot-1", "claimUid" => "pvc-uid-one", "leaseId" => @lease_id, "replayed" => false}}
         }}
      end)

    assert {:ok, context} = HostAllocationContext.prepare(assignment, binding, config)
    assert_receive {:kube_context_requested, digest, :allocate, key}
    assert digest == assignment.sha256
    assert key == digest <> ":allocation"
    assert_receive {:slot_post, url, %{assignmentDigest: ^digest, slotId: "slot-one", claimUid: "pvc-uid-one"}}
    assert String.ends_with?(url, "/reservation-one/codex-auth-slots/reserve")
    assert context.config.auth_slot.lease_id == @lease_id
    assert context.config.auth_slot.claim_uid == "pvc-uid-one"
    assert context.config.repository_id == "123456789"
    assert context.claim_binding == binding
    assert context.result_journal_root == "/private/symphony/job-results"
    assert context.auth_slot_lease_guard_context.result_journal_root == context.result_journal_root
    refute Map.has_key?(assignment, :runner_token)

    assert {:held, _} =
             context.auth_slot_lease_guard_context.cleanup_receipt_fun.(
               context.config.auth_slot,
               assignment,
               %{id: "invalid-allocation", status: :ready}
             )

    assert_receive {:kube_context_requested, ^digest, :finalize, finalize_key}
    assert finalize_key == digest <> ":finalize"

    assert {:held, :rke2_host_allocation_context_unavailable} =
             HostAllocationContext.prepare(assignment, %{binding | issue_id: "another-issue"}, config)

    refute_receive {:slot_post, _, _}
  end

  test "reattaches only the exact suspended Job and bound OAuth slot without reserving a lease" do
    assignment = assignment()
    binding = claim_binding(assignment)
    {:ok, base} = HostAllocationContext.configuration(@env, %{repository_ref: assignment.repository_ref}, "https://provider.example", "host-token")

    slot = %{
      slot_id: base.slot_id,
      claim_name: base.claim_name,
      claim_uid: "pvc-uid-one",
      lease_id: @lease_id,
      assignment_sha256: assignment.sha256,
      seat: assignment.seat
    }

    {:ok, expected} =
      JobSpec.compile(assignment, %{
        namespace: "frigga",
        image: base.image,
        repository_id: base.repository_id,
        auth_slot: slot,
        auth_slot_catalog: %{base.slot_id => base.claim_name}
      })

    generated = %{
      "batch.kubernetes.io/controller-uid" => "job-uid-one",
      "batch.kubernetes.io/job-name" => expected["metadata"]["name"]
    }

    job =
      expected
      |> put_in(["metadata", "uid"], "job-uid-one")
      |> put_in(["metadata", "labels"], Map.merge(expected["metadata"]["labels"], generated))
      |> put_in(["spec", "selector"], %{"matchLabels" => %{"batch.kubernetes.io/controller-uid" => "job-uid-one"}})
      |> put_in(["spec", "template", "metadata", "labels"], Map.merge(expected["spec"]["template"]["metadata"]["labels"], generated))

    assert JobSpec.owned_job?(job, expected)

    allocation_id =
      "rke2job:v1:" <>
        Base.url_encode64(Jason.encode!([1, "frigga", expected["metadata"]["name"], "job-uid-one", assignment.sha256]), padding: false)

    caller = self()

    config =
      base
      |> Map.put(:slot_guard, ReadOnlySlotGuard)
      |> Map.put(:adapter, TerminalAdapter)
      |> Map.put(:client_context_fun, fn _assignment, :allocate, _key, _config -> {:ok, %{synthetic: true}} end)
      |> Map.put(:job_read_fun, fn "frigga", name, %{synthetic: true} ->
        send(caller, {:job_read, name})
        {:ok, Process.get(:retained_job)}
      end)

    Process.put(:retained_job, job)
    assert {:ok, context} = HostAllocationContext.reattach(assignment, binding, allocation_id, config)
    assert_receive {:job_read, _name}
    assert_receive :claim_uid_verified
    assert_receive :lease_binding_verified
    assert context.config.auth_slot == slot
    assert context.claim_binding == binding
    assert context.result_journal_root == "/private/symphony/job-results"
    assert context.auth_slot_lease_guard_context.result_journal_root == context.result_journal_root
    assert is_function(context.auth_slot_lease_guard_context.cleanup_receipt_fun, 3)

    Process.put(:retained_job, put_in(job, ["metadata", "uid"], "replacement-uid"))

    assert {:held, :rke2_retained_allocation_unverified} =
             HostAllocationContext.reattach(assignment, binding, allocation_id, config)

    refute_receive :claim_uid_verified
    refute_receive :lease_binding_verified

    Process.put(:retained_job, put_in(job, ["spec", "suspend"], false))

    assert {:held, :rke2_retained_allocation_unverified} =
             HostAllocationContext.reattach(assignment, binding, allocation_id, config)

    refute_receive :claim_uid_verified
    refute_receive :lease_binding_verified

    assert {:ok, started_context} =
             HostAllocationContext.reattach_started(assignment, binding, allocation_id, config)

    assert started_context.config.auth_slot == slot
    assert_receive :claim_uid_verified
    assert_receive :lease_binding_verified

    assert {:held, :terminal_result_pending} = TerminalOwner.reconcile(assignment, binding, allocation_id, config)
    assert_receive {:terminal_finalization, ^allocation_id, assignment_sha256, finalize_key, ^slot}
    assert assignment_sha256 == assignment.sha256
    assert finalize_key == assignment.sha256 <> ":finalize"
    assert_receive :claim_uid_verified
    assert_receive :lease_binding_verified

    Process.put(:retained_job, put_in(job, ["metadata", "uid"], "replacement-uid"))

    assert {:held, :rke2_retained_allocation_unverified} =
             HostAllocationContext.reattach_started(assignment, binding, allocation_id, config)

    refute_receive :claim_uid_verified
    refute_receive :lease_binding_verified

    assert {:held, :rke2_terminal_allocation_unverified} =
             TerminalOwner.reconcile(assignment, binding, allocation_id, config)

    refute_receive {:terminal_finalization, _, _, _, _}
  end

  test "reattaches terminal cleanup after Job deletion and OAuth lease release" do
    assignment = assignment()
    binding = claim_binding(assignment)
    {:ok, base} = HostAllocationContext.configuration(@env, %{repository_ref: assignment.repository_ref}, "https://provider.example", "host-token")
    root = Path.join(System.tmp_dir!(), "symphony-terminal-reattach-#{System.unique_integer([:positive])}")
    :ok = File.mkdir_p(root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(root, 0o700)
    if match?({:win32, _}, :os.type()), do: Process.put(:result_journal_windows_test_only, true)
    on_exit(fn -> File.rm_rf(root) end)

    slot = %{
      slot_id: base.slot_id,
      claim_name: base.claim_name,
      claim_uid: "pvc-uid-one",
      lease_id: @lease_id,
      assignment_sha256: assignment.sha256,
      seat: assignment.seat
    }

    result =
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
      |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)

    observation = %{
      job_uid: "job-uid-one",
      job_resource_version: "job-rv-one",
      pod_uid: "pod-uid-one",
      pod_resource_version: "pod-rv-one",
      pod_list_resource_version: "list-rv-one",
      exit_code: 0,
      result: result
    }

    assert {:ok, _path} = ResultJournal.record(assignment, observation, root, slot)
    {:ok, expected} = JobSpec.compile(assignment, %{namespace: "frigga", image: base.image, repository_id: base.repository_id})

    allocation_id =
      "rke2job:v1:" <>
        Base.url_encode64(Jason.encode!([1, "frigga", expected["metadata"]["name"], observation.job_uid, assignment.sha256]), padding: false)

    caller = self()

    config =
      base
      |> Map.put(:result_journal_root, root)
      |> Map.put(:slot_guard, ReadOnlySlotGuard)
      |> Map.put(:adapter, TerminalAdapter)
      |> Map.put(:client_context_fun, fn _assignment, :finalize, key, _config ->
        send(caller, {:finalize_context, key})
        {:ok, %{synthetic: true}}
      end)
      |> Map.put(:job_read_fun, fn _, _, _ -> flunk("terminal replay must not require the deleted Job") end)

    assert {:ok, context} = HostAllocationContext.reattach_terminal(assignment, binding, allocation_id, config)
    assert context.config.auth_slot == slot
    assert context.result_journal_root == root
    assert_receive {:finalize_context, finalize_key}
    assert finalize_key == assignment.sha256 <> ":finalize"
    assert_receive :claim_uid_verified
    refute_receive :lease_binding_verified

    assert {:held, :terminal_result_pending} = TerminalOwner.reconcile(assignment, binding, allocation_id, config)
    assert_receive {:terminal_finalization, ^allocation_id, assignment_sha256, finalize_key, ^slot}
    assert assignment_sha256 == assignment.sha256
    assert finalize_key == assignment.sha256 <> ":finalize"
    assert_receive :claim_uid_verified
    refute_receive :lease_binding_verified

    altered_id = String.replace(allocation_id, "rke2job:v1:", "rke2job:v2:")

    assert {:held, :rke2_terminal_allocation_unverified} =
             HostAllocationContext.reattach_terminal(assignment, binding, altered_id, config)

    refute_receive :claim_uid_verified
    refute_receive :lease_binding_verified
  end

  defp claim_binding(assignment) do
    %{
      issue_id: assignment.lease.issue_id,
      generation: assignment.lease.generation,
      repository_ref: assignment.repository_ref,
      runner_id: assignment.seat,
      reservation_id: "reservation-one"
    }
  end

  defp assignment do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-host-context",
        seat: "runner-17",
        lease: %{
          issue_id: "issue-1",
          repository: "hypergridau/symphony",
          generation: 4,
          session_id: "worker:issue-1:4",
          process_id: "worker:issue-1:4"
        },
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "RKE2 Job allocation", evidence: "Synthetic host context test"},
        context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end
end
