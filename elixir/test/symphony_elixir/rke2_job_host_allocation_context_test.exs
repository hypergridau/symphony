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
    "SYMPHONY_RKE2_CREDENTIAL_ROOT" => Path.expand("/etc/symphony/frigga-kubernetes"),
    "SYMPHONY_RKE2_WORKER_IMAGE" => @image,
    "SYMPHONY_RKE2_REPOSITORY_ID" => "123456789",
    "SYMPHONY_RKE2_AUTH_SLOT_ID" => "slot-one",
    "SYMPHONY_RKE2_AUTH_CLAIM_NAME" => "codex-oauth-slot-1",
    "SYMPHONY_RKE2_RESULT_JOURNAL_ROOT" => Path.expand("/private/symphony/job-results"),
    "SYMPHONY_RKE2_ABORT_JOURNAL_ROOT" => Path.expand("/private/symphony/abort-prepares"),
    "SYMPHONY_RKE2_WORKSPACE_ROOT" => Path.expand("/private/symphony/workspaces"),
    "SYMPHONY_DAHLIA_ASSIGNMENT_BIND_ORIGIN" => "https://assignment-broker.example"
  }
  @binding_digest String.duplicate("b", 64)

  test "host settings are all-or-nothing and pinned to a manifest repository" do
    manifest = %{repository_ref: "hypergridau/symphony"}
    assert :disabled = HostAllocationContext.configuration(%{}, manifest, "https://provider.example", "host-token")

    assert {:error, {:incomplete_rke2_host_context, ["SYMPHONY_RKE2_AUTH_CLAIM_NAME"]}} =
             HostAllocationContext.configuration(Map.delete(@env, "SYMPHONY_RKE2_AUTH_CLAIM_NAME"), manifest, "https://provider.example", "host-token")

    assert {:error, {:incomplete_rke2_host_context, ["SYMPHONY_RKE2_ABORT_JOURNAL_ROOT"]}} =
             HostAllocationContext.configuration(Map.delete(@env, "SYMPHONY_RKE2_ABORT_JOURNAL_ROOT"), manifest, "https://provider.example", "host-token")

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(@env, nil, "https://provider.example", "host-token")

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(Map.put(@env, "SYMPHONY_RKE2_WORKER_IMAGE", "worker:latest"), manifest, "https://provider.example", "host-token")

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(Map.put(@env, "SYMPHONY_RKE2_RESULT_JOURNAL_ROOT", "relative/results"), manifest, "https://provider.example", "host-token")

    assert {:error, {:incomplete_rke2_host_context, ["SYMPHONY_DAHLIA_ASSIGNMENT_BIND_ORIGIN"]}} =
             HostAllocationContext.configuration(Map.delete(@env, "SYMPHONY_DAHLIA_ASSIGNMENT_BIND_ORIGIN"), manifest, "https://provider.example", "host-token")

    assert {:ok, config} =
             HostAllocationContext.configuration(@env, manifest, "https://provider.example", "host-token")

    assert config.image == @image
    assert config.repository_ref == "hypergridau/symphony"
    assert config.slot_id == "slot-one"
    assert config.result_journal_root == @env["SYMPHONY_RKE2_RESULT_JOURNAL_ROOT"]
    assert config.abort_journal_root == @env["SYMPHONY_RKE2_ABORT_JOURNAL_ROOT"]
    assert config.workspace_root == @env["SYMPHONY_RKE2_WORKSPACE_ROOT"]

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(
               Map.put(@env, "SYMPHONY_RKE2_ABORT_JOURNAL_ROOT", "/private/symphony/workspaces/abort"),
               manifest,
               "https://provider.example",
               "host-token"
             )
  end

  @tag skip: match?({:win32, _}, :os.type())
  test "abort journal cannot resolve through a link into the workspace" do
    root = Path.join(System.tmp_dir!(), "hgs733-abort-roots-#{System.unique_integer([:positive])}")
    workspace = Path.join(root, "workspaces")
    alias_path = Path.join(root, "alias")
    File.mkdir_p!(workspace)
    File.ln_s!(workspace, alias_path)
    on_exit(fn -> File.rm_rf!(root) end)

    env =
      @env
      |> Map.put("SYMPHONY_RKE2_WORKSPACE_ROOT", workspace)
      |> Map.put("SYMPHONY_RKE2_ABORT_JOURNAL_ROOT", Path.join(alias_path, "abort"))

    assert {:error, :invalid_rke2_host_context} =
             HostAllocationContext.configuration(env, %{repository_ref: "hypergridau/symphony"}, "https://provider.example", "host-token")
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

    {:ok, base} = host_configuration(assignment)

    caller = self()

    config =
      base
      |> Map.put(:client_context_fun, fn seen, operation, key, _config ->
        send(caller, {:kube_context_requested, seen.sha256, operation, key})
        {:ok, %{synthetic: true}}
      end)
      |> Map.put(:assignment_bind_post_fun, bind_post_fun(assignment, caller))
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
    assert_receive {:assignment_bind, bind_url, bind_body}
    assert String.ends_with?(bind_url, "/v1/host/assignments/reservation-one/bind")
    assert bind_body.issueIdentifier == "HGS-734"
    assert Base.decode64!(bind_body.manifestBase64) == base.managed_delegations.source_bytes
    assert bind_body.signatureHex == base.managed_delegations.source_signature_hex
    assert_receive {:kube_context_requested, digest, :allocate, key}
    assert digest == assignment.sha256
    assert key == digest <> ":allocation"
    assert_receive {:slot_post, url, %{assignmentDigest: @binding_digest, slotId: "slot-one", claimUid: "pvc-uid-one"}}
    assert String.ends_with?(url, "/reservation-one/codex-auth-slots/reserve")
    assert context.config.auth_slot.lease_id == @lease_id
    assert context.config.auth_slot.binding_sha256 == @binding_digest
    assert context.config.assignment_binding_digest == @binding_digest
    assert {:ok, job_spec} = JobSpec.compile(assignment, context.config)
    assert job_spec["metadata"]["annotations"]["symphony.hypergrid.au/assignment-binding-sha256"] == @binding_digest
    assert context.config.auth_slot.claim_uid == "pvc-uid-one"
    assert context.config.repository_id == "123456789"
    assert context.claim_binding == binding
    assert context.result_journal_root == @env["SYMPHONY_RKE2_RESULT_JOURNAL_ROOT"]
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

  test "a denied assignment bind stops before Kubernetes credentials or OAuth slot reservation" do
    assignment = assignment()
    binding = claim_binding(assignment)
    {:ok, base} = host_configuration(assignment)
    caller = self()

    config =
      Map.put(base, :assignment_bind_post_fun, fn url, options ->
        send(caller, {:assignment_bind, url, options[:json]})
        {:ok, %Req.Response{status: 409, body: %{"status" => "denied"}}}
      end)

    assert {:held, :rke2_host_allocation_context_unavailable} =
             HostAllocationContext.prepare(assignment, binding, config)

    assert_receive {:assignment_bind, _, _}
    refute_receive {:kube_context_requested, _, _, _}
    refute_receive {:slot_post, _, _}
  end

  test "reattaches only the exact suspended Job and bound OAuth slot without reserving a lease" do
    assignment = assignment()
    binding = claim_binding(assignment)
    {:ok, base} = host_configuration(assignment)

    slot = %{
      slot_id: base.slot_id,
      claim_name: base.claim_name,
      claim_uid: "pvc-uid-one",
      lease_id: @lease_id,
      assignment_sha256: assignment.sha256,
      binding_sha256: @binding_digest,
      seat: assignment.seat
    }

    {:ok, expected} =
      JobSpec.compile(assignment, %{
        namespace: "frigga",
        image: base.image,
        repository_id: base.repository_id,
        auth_slot: slot,
        auth_slot_catalog: %{base.slot_id => base.claim_name},
        assignment_binding_digest: @binding_digest
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
      |> Map.put(:assignment_bind_post_fun, bind_post_fun(assignment, caller))
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
    assert context.result_journal_root == @env["SYMPHONY_RKE2_RESULT_JOURNAL_ROOT"]
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

  test "reattaches terminal cleanup from its saved binding after current authority expires" do
    assignment = assignment()
    binding = claim_binding(assignment)
    {:ok, base} = host_configuration(assignment)
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
      binding_sha256: @binding_digest,
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
      |> Map.put(:assignment_bind_post_fun, fn url, opts ->
        send(caller, {:assignment_bind_denied, url, opts[:json]})
        {:ok, %Req.Response{status: 409, body: %{"status" => "denied"}}}
      end)
      |> Map.put(:slot_guard, ReadOnlySlotGuard)
      |> Map.put(:adapter, TerminalAdapter)
      |> Map.put(:client_context_fun, fn _assignment, :finalize, key, _config ->
        send(caller, {:finalize_context, key})
        {:ok, %{synthetic: true}}
      end)
      |> Map.put(:job_read_fun, fn _, _, _ -> flunk("terminal replay must not require the deleted Job") end)

    assert {:ok, context} = HostAllocationContext.reattach_terminal(assignment, binding, allocation_id, config)
    assert context.config.auth_slot == slot
    assert context.config.assignment_binding_digest == @binding_digest
    assert context.result_journal_root == root
    assert_receive {:finalize_context, finalize_key}
    assert finalize_key == assignment.sha256 <> ":finalize"
    assert_receive :claim_uid_verified
    refute_receive :lease_binding_verified
    refute_receive {:assignment_bind_denied, _, _}

    assert {:held, :terminal_result_pending} = TerminalOwner.reconcile(assignment, binding, allocation_id, config)
    assert_receive {:terminal_finalization, ^allocation_id, assignment_sha256, finalize_key, ^slot}
    assert assignment_sha256 == assignment.sha256
    assert finalize_key == assignment.sha256 <> ":finalize"
    assert_receive :claim_uid_verified
    refute_receive :lease_binding_verified
    assert_receive {:assignment_bind_denied, _, _}
    refute_receive {:assignment_bind_denied, _, _}

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

  defp host_configuration(assignment) do
    HostAllocationContext.configuration(@env, signed_manifest(assignment), "https://provider.example", "host-token")
  end

  defp bind_post_fun(assignment, caller) do
    fn url, opts ->
      send(caller, {:assignment_bind, url, opts[:json]})

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "status" => "bound",
           "assignmentDigest" => @binding_digest,
           "branchRef" => "refs/heads/" <> assignment.branch
         }
       }}
    end
  end

  defp signed_manifest(assignment) do
    bytes = Jason.encode!(%{"schema_version" => 1, "synthetic" => true})
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519)
    signature = :crypto.sign(:eddsa, :none, "hypergrid.symphony.managed-delegation.v1\0" <> bytes, [private_key, :ed25519])
    public_hex = Base.encode16(public_key, case: :lower)
    signature_hex = Base.encode16(signature, case: :lower)

    %{
      repository_ref: assignment.repository_ref,
      schema_version: 1,
      entries: [%{issue_id: assignment.lease.issue_id, identifier: "HGS-734"}],
      source_bytes: bytes,
      source_signature_hex: signature_hex,
      source_public_key_hex: public_hex,
      source_sha256: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
      signer_key_sha256: Base.encode16(:crypto.hash(:sha256, public_key), case: :lower)
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
