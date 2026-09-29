defmodule SymphonyElixir.WorkerOneShotTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.JobSpec
  alias SymphonyElixir.Worker.Assignment
  alias SymphonyElixir.Worker.BoundedOutput
  alias SymphonyElixir.Worker.CLI
  alias SymphonyElixir.Worker.OneShot

  @hgs736_issue_uuid "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"
  @hgs736_no_checkout_constraint "qualification/hgs-736/started-no-checkout/" <>
                                   @hgs736_issue_uuid <> "/generation-1"

  test "bounds command output through System.cmd's collectable sink" do
    {sink, 0} = System.cmd("git", ["--version"], stderr_to_stdout: true, into: struct(BoundedOutput, limit: 4))
    assert byte_size(sink.output) == 4
    assert sink.truncated?
  end

  test "accepts only the JobSpec arguments, identity, digest, and fixed auth home" do
    assignment = assignment()
    env = environment(assignment)
    json = Jason.encode!(assignment)

    assert {:ok, decoded} = CLI.decode_assignment(json, env)
    assert decoded.bundle.sha256 == assignment.sha256
    changed = Jason.decode!(json) |> Map.put("branch", "codex/other") |> Jason.encode!()
    assert {:error, :invalid_job_assignment, nil} = CLI.decode_assignment(changed, env)
    assert {:error, :invalid_job_assignment, nil} = CLI.decode_assignment(json, Map.put(env, "SYMPHONY_ASSIGNMENT_ID", "wrong"))
    assert {:error, :invalid_job_assignment, nil} = CLI.decode_assignment(json, Map.put(env, "CODEX_HOME", "/workspace/.codex"))

    assert %{exit_code: 1, result: %{reason: "invalid_job_arguments"}} =
             CLI.run(["--assignment-json", json, "extra"], env)
  end

  test "runs a JIT checkout and mediated additions-only pull request without exposing the Git token" do
    assignment = assignment()
    test_pid = self()
    token = "github-installation-secret"

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      create_askpass: fn -> :ok end,
      now: fn -> ~U[2026-09-27 00:00:00Z] end,
      broker_issue: fn subject, use, key, _now, ttl, _ctx ->
        send(test_pid, {:issue, subject, use, key, ttl})
        lease_id = if use == :git_checkout, do: "lease-1", else: "publish-lease"
        {:ok, %{"leaseId" => lease_id, "notAfter" => "2026-09-27T00:10:00Z", "repositoryRef" => subject.repositoryRef}}
      end,
      broker_checkout: fn "lease-1", _repository_ref, _cutoff, _ctx ->
        {:ok, %{installation_token: token}}
      end,
      read_additions: fn ["created.txt"] -> {:ok, [%{path: "created.txt", contents: "proof\n"}]} end,
      broker_branch: fn "publish-lease", _base, "refs/heads/codex/hgs729-canary", _ctx ->
        {:ok, String.duplicate("b", 40)}
      end,
      broker_commit: fn "publish-lease", _head, _message, [%{path: "created.txt", contents: "proof\n"}], _ctx ->
        {:ok, String.duplicate("c", 40)}
      end,
      broker_pull_request: fn "publish-lease", "hypergridau/symphony", _ctx ->
        {:ok, %{number: 123, url: "https://github.com/hypergridau/symphony/pull/123"}}
      end,
      broker_revoke: fn lease_id, _ctx ->
        send(test_pid, {:revoke, lease_id})
        :ok
      end,
      command: fn executable, args, opts ->
        send(test_pid, {:command, executable, args, opts})

        case {executable, args} do
          {"git", ["-c", "credential.helper=", "clone", "--branch", "main", "--single-branch", "--no-tags", "--", "https://github.com/hypergridau/symphony.git", "/workspace"]} ->
            {"", 0}

          {"git", ["-C", "/workspace", "switch", "--create", "--", "codex/hgs729-canary"]} ->
            {"", 0}

          {"codex", _args} ->
            {"{\"type\":\"turn.completed\"}\n", 0}

          {"git", ["-C", "/workspace", "rev-parse", "HEAD"]} ->
            {String.duplicate("a", 40) <> "\n", 0}

          {"git", ["-C", "/workspace", "status", "--porcelain=v1", "-z", "--untracked-files=all"]} ->
            {"?? created.txt\0", 0}

          _ ->
            flunk("unexpected worker command: #{inspect({executable, args})}")
        end
      end
    }

    assert %{exit_code: 0, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.status == "completed"
    assert result.revocation == "confirmed"
    assert result.checkout_lease_id == "lease-1"
    assert result.checkout_revocation == "confirmed"
    assert result.broker_lease_id == "publish-lease"
    assert result.changed_files == 1
    assert result.pull_request_url == "https://github.com/hypergridau/symphony/pull/123"
    assert_receive {:revoke, "lease-1"}
    assert_receive {:revoke, "publish-lease"}
    assert_receive {:issue, subject, :git_checkout, key, 600}
    assert subject.assignmentDigest == assignment.sha256
    assert key == assignment.sha256 <> ":worker-checkout"

    assert_receive {:command, "git", clone_args, clone_opts}
    refute Enum.any?(clone_args, &String.contains?(&1, token))
    assert {"SYMPHONY_GITHUB_TOKEN", ^token} = Enum.find(clone_opts[:env], &match?({"SYMPHONY_GITHUB_TOKEN", _}, &1))

    assert_receive {:command, "codex", codex_args, codex_opts}
    refute Enum.any?(codex_args, &String.contains?(&1, token))
    refute Enum.any?(codex_opts[:env], &match?({"SYMPHONY_GITHUB_TOKEN", _}, &1))
    assert Enum.any?(codex_args, &(&1 == "gpt-6-luna"))
    assert Enum.any?(codex_args, &(&1 == "model_reasoning_effort=high"))
    assert Enum.any?(codex_args, &(&1 == "workspace-write"))
    assert Enum.any?(codex_args, &(&1 == "never"))
    assert Enum.any?(codex_args, &(&1 == "--ephemeral"))
    prompt = List.last(codex_args)
    refute String.contains?(prompt, token)
    refute String.contains?(prompt, "context_secret_refs")
  end

  test "signed HGS-736 assignment revokes the checkout lease before requiring broker denial" do
    assignment = hgs736_assignment()
    test_pid = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      now: fn -> ~U[2026-09-27 00:00:00Z] end,
      broker_issue: fn subject, use, key, _now, ttl, _ctx ->
        send(test_pid, {:broker, :issue, use, key, ttl, subject})
        {:ok, %{"leaseId" => "hgs736-checkout", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_revoke: fn lease_id, _ctx ->
        send(test_pid, {:broker, :revoke, lease_id})
        :ok
      end,
      broker_checkout_denial: fn lease_id, repository_ref, cutoff, _ctx ->
        send(test_pid, {:broker, :checkout_denial, lease_id, repository_ref, cutoff})
        :confirmed_denied
      end,
      command: fn executable, args, _opts ->
        flunk("checkout-denial qualification must not invoke #{executable}: #{inspect(args)}")
      end
    }

    assert %{exit_code: 1, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.status == "failed"
    assert result.reason == "credential_checkout_denied"
    assert result.checkout_lease_id == "hgs736-checkout"
    assert result.checkout_revocation == "confirmed"
    assert result.revocation == "confirmed"
    assert result.broker_lease_id == nil
    assert result.codex_exit_code == nil
    assert result.head_oid == nil
    assert_receive {:broker, :issue, :git_checkout, key, 600, subject}
    assert key == assignment.sha256 <> ":worker-checkout"
    assert subject.assignmentDigest == assignment.sha256
    assert subject.issueUuid == @hgs736_issue_uuid
    assert subject.generation == 1
    assert subject.runnerId == assignment.seat
    assert_receive {:broker, :revoke, "hgs736-checkout"}
    assert_receive {:broker, :checkout_denial, "hgs736-checkout", "hypergridau/symphony", "2026-09-27T00:10:00Z"}
    assert_receive {:broker, :revoke, "hgs736-checkout"}
    refute_receive {:command, _, _}
  end

  test "reserved HGS-736 intent fails closed before any worker or broker operation" do
    invalid_assignments = [
      assignment(
        "00000000-0000-4000-8000-000000000000",
        1,
        ["repository", "no-production-workload", @hgs736_no_checkout_constraint]
      ),
      assignment(@hgs736_issue_uuid, 2, ["repository", "no-production-workload", @hgs736_no_checkout_constraint]),
      assignment(
        @hgs736_issue_uuid,
        1,
        ["repository", "no-production-workload", @hgs736_no_checkout_constraint <> "-typo"]
      ),
      assignment(
        @hgs736_issue_uuid,
        1,
        ["repository", "no-production-workload", @hgs736_no_checkout_constraint, "qualification/hgs-736/duplicate"]
      )
    ]

    for assignment <- invalid_assignments do
      deps = no_operation_deps()

      assert %{exit_code: 1, result: %{status: "failed", reason: "invalid_hgs736_checkout_qualification"}} =
               CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)
    end
  end

  test "HGS-736 intent fails closed when the signed subject is misbound" do
    assignment = hgs736_assignment()

    {:ok, decoded} = Assignment.decode(Jason.encode!(assignment), assignment.sha256, "123456789")

    misbound = put_in(decoded.subject.runnerId, "other-runner")

    assert {:error, "invalid_hgs736_checkout_qualification", _result} =
             OneShot.run(misbound, environment(assignment), no_operation_deps())

    mismatched_job = Map.put(environment(assignment), "SYMPHONY_ASSIGNMENT_ID", "other-job")

    assert {:error, "invalid_hgs736_checkout_qualification", _result} =
             OneShot.run(decoded, mismatched_job, no_operation_deps())
  end

  test "HGS-736 intent fails closed on revocation uncertainty and never requests checkout" do
    assignment = hgs736_assignment()
    test_pid = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, :git_checkout, _key, _now, _ttl, _ctx ->
        send(test_pid, {:broker, :issue})
        {:ok, %{"leaseId" => "hgs736-checkout", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_revoke: fn _lease_id, _ctx ->
        send(test_pid, {:broker, :revoke})
        {:held, :broker_uncertain}
      end,
      broker_checkout_denial: fn _lease_id, _repository_ref, _cutoff, _ctx ->
        flunk("checkout after uncertain HGS-736 revocation")
      end
    }

    assert %{
             exit_code: 2,
             result: %{
               status: "held",
               reason: "credential_revocation_unconfirmed",
               checkout_lease_id: "hgs736-checkout",
               checkout_revocation: "held",
               revocation: "held"
             }
           } =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert_receive {:broker, :issue}
    assert_receive {:broker, :revoke}
    refute_receive {:broker, :revoke}
  end

  test "HGS-736 holds an unconfirmed checkout response even when both revocations are confirmed" do
    assignment = hgs736_assignment()
    test_pid = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, :git_checkout, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "hgs736-checkout", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_revoke: fn _lease_id, _ctx ->
        send(test_pid, {:broker, :revoke})
        :ok
      end,
      broker_checkout_denial: fn _lease_id, _repository_ref, _cutoff, _ctx ->
        send(test_pid, {:broker, :checkout_uncertain})
        {:held, :broker_uncertain}
      end
    }

    assert %{
             exit_code: 2,
             result: %{
               status: "held",
               reason: "credential_checkout_uncertain",
               checkout_lease_id: "hgs736-checkout",
               checkout_revocation: "confirmed",
               revocation: "confirmed"
             }
           } =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert_receive {:broker, :revoke}
    assert_receive {:broker, :checkout_uncertain}
    assert_receive {:broker, :revoke}
  end

  test "HGS-736 intent never clones if a revoked lease unexpectedly returns a checkout token" do
    assignment = hgs736_assignment()
    test_pid = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, :git_checkout, _key, _now, _ttl, _ctx ->
        send(test_pid, {:broker, :issue})
        {:ok, %{"leaseId" => "hgs736-checkout", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_revoke: fn _lease_id, _ctx ->
        send(test_pid, {:broker, :revoke})
        :ok
      end,
      broker_checkout_denial: fn _lease_id, _repository_ref, _cutoff, _ctx ->
        send(test_pid, {:broker, :unexpected_checkout_success})
        :unexpected_issue
      end,
      command: fn executable, args, _opts ->
        flunk("unexpected token must not reach #{executable}: #{inspect(args)}")
      end
    }

    assert %{
             exit_code: 2,
             result: %{
               status: "held",
               reason: "qualification_checkout_denial_failed",
               checkout_lease_id: "hgs736-checkout",
               checkout_revocation: "confirmed",
               revocation: "confirmed"
             }
           } =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert_receive {:broker, :issue}
    assert_receive {:broker, :revoke}
    assert_receive {:broker, :unexpected_checkout_success}
    assert_receive {:broker, :revoke}
    refute_receive {:command, _, _}
  end

  test "does not retry uncertain issuance or start checkout" do
    assignment = assignment()
    test_pid = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        send(test_pid, :issue)
        {:held, :broker_uncertain}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> flunk("checkout after uncertain issue") end
    }

    assert %{exit_code: 2, result: %{status: "held", reason: "credential_issuance_uncertain"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert_receive :issue
    refute_receive :issue
  end

  test "fails closed before issuing a lease when the auth slot is unavailable" do
    assignment = assignment()

    deps = %{
      auth_slot_ready: fn -> false end,
      workspace_ready: fn -> flunk("workspace check after auth slot failure") end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx -> flunk("lease after auth slot failure") end
    }

    assert %{exit_code: 1, result: %{reason: "codex_auth_slot_unavailable", checkout_lease_id: nil}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)
  end

  test "fails closed on an invalid OneShot input and malformed auth readiness result" do
    assert {:error, "invalid_worker_assignment", %{}} = OneShot.run(nil, %{}, %{})

    assignment = assignment()

    deps = %{
      auth_slot_ready: fn -> :unexpected end,
      workspace_ready: fn -> flunk("workspace after invalid auth readiness result") end
    }

    assert %{exit_code: 1, result: %{reason: "codex_auth_slot_unavailable"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)
  end

  test "fails closed on unavailable workspace and denied checkout lease issuance" do
    assignment = assignment()

    workspace_deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> false end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx -> flunk("lease with unavailable workspace") end
    }

    assert %{exit_code: 1, result: %{reason: "workspace_not_empty"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), workspace_deps)

    issue_deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, :git_checkout, _key, _now, _ttl, _ctx -> {:error, :denied} end
    }

    assert %{exit_code: 1, result: %{reason: "credential_issuance_denied", checkout_lease_id: nil}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), issue_deps)
  end

  test "uses the default workspace check for empty, occupied, and unavailable paths" do
    assignment = assignment()
    workspace = Path.join(System.tmp_dir!(), "symphony-workspace-#{System.unique_integer([:positive])}")
    File.mkdir_p!(workspace)
    on_exit(fn -> File.rm_rf!(workspace) end)

    deps = %{
      workspace_path: workspace,
      auth_slot_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx -> {:error, :denied} end
    }

    assert %{result: %{reason: "credential_issuance_denied"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    File.write!(Path.join(workspace, "leftover"), "stale")

    assert %{exit_code: 1, result: %{reason: "workspace_not_empty"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    File.rm!(Path.join(workspace, "leftover"))
    File.rm_rf!(workspace)

    assert %{exit_code: 1, result: %{reason: "workspace_unavailable"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)
  end

  test "holds an invalid checkout lease response without guessing a lease ID" do
    assignment = assignment()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx -> {:ok, %{"leaseId" => "unconfirmed"}} end
    }

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "broker_lease_metadata_invalid"
    assert result.checkout_lease_id == nil
    assert result.checkout_revocation == "not_started"
  end

  test "fails closed when the broker issuer raises without retrying or checking out" do
    assignment = assignment()
    parent = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        send(parent, :issue_attempted)
        raise "broker adapter failed"
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> flunk("checkout after broker issue failure") end
    }

    assert %{exit_code: 1, result: %{reason: "credential_issuance_denied", checkout_lease_id: nil}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert_receive :issue_attempted
    refute_receive :issue_attempted
  end

  test "retains a confirmed checkout revoke receipt after checkout uncertainty" do
    assignment = assignment()
    parent = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "checkout-uncertain", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:held, :timeout} end,
      broker_revoke: fn lease, _ctx ->
        send(parent, {:revoked, lease})
        :ok
      end
    }

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "credential_checkout_uncertain"
    assert result.checkout_lease_id == "checkout-uncertain"
    assert result.checkout_revocation == "confirmed"
    assert_receive {:revoked, "checkout-uncertain"}
  end

  test "holds checkout uncertainty when its revoke reply is malformed" do
    assignment = assignment()
    parent = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "checkout-revoke-unknown", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:held, :timeout} end,
      broker_revoke: fn lease, _ctx ->
        send(parent, {:revoked, lease})
        :unexpected_revoke_reply
      end
    }

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "credential_checkout_uncertain"
    assert result.checkout_lease_id == "checkout-revoke-unknown"
    assert result.checkout_revocation == "held"
    assert_receive {:revoked, "checkout-revoke-unknown"}
  end

  test "revokes checkout lease when askpass setup is denied" do
    assignment = assignment()
    parent = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "checkout-askpass", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:ok, %{installation_token: "secret"}} end,
      create_askpass: fn -> {:error, :denied} end,
      broker_revoke: fn lease, _ctx ->
        send(parent, {:revoked, lease})
        :ok
      end
    }

    assert %{exit_code: 1, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "repository_checkout_failed"
    assert result.checkout_lease_id == "checkout-askpass"
    assert result.checkout_revocation == "confirmed"
    assert_receive {:revoked, "checkout-askpass"}
  end

  test "reports Codex command failure as an execution failure with checkout receipt" do
    assignment = assignment()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      create_askpass: fn -> :ok end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "checkout-codex-error", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:ok, %{installation_token: "secret"}} end,
      broker_revoke: fn _lease, _ctx -> :ok end,
      command: fn
        "git", ["-c", "credential.helper=", "clone" | _args], _opts -> {"", 0}
        "git", ["-C", "/workspace", "switch", "--create", "--", _branch], _opts -> {"", 0}
        "codex", _args, _opts -> :unexpected_command_result
      end
    }

    assert %{exit_code: 1, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "codex_execution_failed"
    assert result.checkout_lease_id == "checkout-codex-error"
    assert result.checkout_revocation == "confirmed"
  end

  test "does not publish when Codex exits unsuccessfully and retains checkout receipt" do
    assignment = assignment()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "checkout-only", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:ok, %{installation_token: "secret"}} end,
      broker_revoke: fn _lease, _ctx -> :ok end,
      command: fn
        "git", ["-c", "credential.helper=", "clone" | _args], _opts -> {"", 0}
        "git", ["-C", "/workspace", "switch", "--create", "--", _branch], _opts -> {"", 0}
        "codex", _args, _opts -> {"", 7}
      end
    }

    assert %{exit_code: 1, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "codex_failed_or_no_additions"
    assert result.checkout_lease_id == "checkout-only"
    assert result.checkout_revocation == "confirmed"
    assert result.revocation == "confirmed"
    assert result.broker_lease_id == nil
  end

  test "holds a checkout whose local branch setup fails and records uncertain revocation" do
    assignment = assignment()
    parent = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "checkout-switch", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:ok, %{installation_token: "secret"}} end,
      broker_revoke: fn lease, _ctx ->
        send(parent, {:revoked, lease})
        {:held, :broker_uncertain}
      end,
      command: fn
        "git", ["-c", "credential.helper=", "clone" | _args], _opts -> {"", 0}
        "git", ["-C", "/workspace", "switch", "--create", "--", _branch], _opts -> {"", 1}
      end
    }

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "repository_checkout_failed"
    assert result.checkout_lease_id == "checkout-switch"
    assert result.checkout_revocation == "held"
    assert_receive {:revoked, "checkout-switch"}
  end

  test "holds a completed run when lease revocation cannot be confirmed" do
    assignment = assignment()
    deps = successful_dependencies(self(), {:held, :broker_uncertain})

    assert %{exit_code: 2, result: %{status: "held", reason: "credential_revocation_unconfirmed", revocation: "held"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)
  end

  test "retains confirmed PR identity when publish lease revocation is uncertain" do
    assignment = assignment()
    parent = self()
    deps = publish_failure_dependencies(assignment, parent, :pr_success)

    deps =
      Map.put(deps, :broker_revoke, fn lease, _ctx ->
        send(parent, {:revoked, lease})
        if lease == "checkout-lease", do: :ok, else: {:held, :broker_uncertain}
      end)

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "credential_revocation_unconfirmed"
    assert result.checkout_lease_id == "checkout-lease"
    assert result.checkout_revocation == "confirmed"
    assert result.broker_lease_id == "publish-lease"
    assert result.revocation == "held"
    assert result.head_oid == String.duplicate("c", 40)
    assert result.pull_request_number == 1
    assert result.pull_request_url == "https://github.com/hypergridau/symphony/pull/1"
    assert_receive {:revoked, "checkout-lease"}
    assert_receive {:revoked, "publish-lease"}
  end

  test "holds a denied pull request result when publish lease revocation is uncertain" do
    assignment = assignment()
    parent = self()
    deps = publish_failure_dependencies(assignment, parent, :pr_denied)

    deps =
      Map.put(deps, :broker_revoke, fn lease, _ctx ->
        send(parent, {:revoked, lease})
        if lease == "checkout-lease", do: :ok, else: {:held, :broker_uncertain}
      end)

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "pull_request_denied"
    assert result.checkout_lease_id == "checkout-lease"
    assert result.checkout_revocation == "confirmed"
    assert result.broker_lease_id == "publish-lease"
    assert result.revocation == "held"
    assert result.head_oid == String.duplicate("c", 40)
    assert result.pull_request_number == nil
    assert_receive {:revoked, "publish-lease"}
  end

  test "no-slot Jobs run only explicit assignment preflight" do
    assignment = assignment()
    env = environment(assignment) |> Map.delete("CODEX_HOME") |> Map.put("SYMPHONY_WORKER_MODE", "preflight")

    assert %{exit_code: 0, result: %{status: "preflight_passed", reason: "auth_slot_required"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], env)

    assert {:error, :invalid_job_assignment, nil} =
             CLI.decode_assignment(Jason.encode!(assignment), Map.put(env, "CODEX_HOME", "/workspace/.codex"))
  end

  test "revokes checkout credentials after checkout denial and clone failure" do
    assignment = assignment()
    parent = self()

    for failure <- [:checkout_denied, :clone_failed] do
      deps = %{
        auth_slot_ready: fn -> true end,
        workspace_ready: fn -> true end,
        create_askpass: fn -> :ok end,
        broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
          {:ok, %{"leaseId" => "lease-1", "notAfter" => "2026-09-27T00:10:00Z"}}
        end,
        broker_checkout: fn _lease, _repo, _cutoff, _ctx ->
          if failure == :checkout_denied, do: {:error, :broker_denied}, else: {:ok, %{installation_token: "secret"}}
        end,
        broker_revoke: fn "lease-1", _ctx ->
          send(parent, {:revoked, failure})
          :ok
        end,
        command: fn "git", _args, _opts -> {"", 1} end
      }

      assert %{exit_code: 1, result: %{checkout_lease_id: "lease-1", checkout_revocation: "confirmed"}} =
               CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

      assert_receive {:revoked, ^failure}
    end
  end

  test "retains checkout lease identity when its revocation is unconfirmed" do
    assignment = assignment()
    deps = successful_dependencies(self(), {:held, :broker_uncertain})

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.status == "held"
    assert result.reason == "credential_revocation_unconfirmed"
    assert result.checkout_lease_id == "lease-1"
    assert result.checkout_revocation == "held"
    refute_receive {:command, "codex", _args}
  end

  test "revokes an uncertain commit lease and holds rather than retrying" do
    assignment = assignment()
    parent = self()
    deps = publish_failure_dependencies(assignment, parent, :commit_uncertain)

    assert %{exit_code: 2, result: %{status: "held"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert_receive {:revoked, "checkout-lease"}
    assert_receive {:commit, 1}
    assert_receive {:revoked, "publish-lease"}
    refute_receive {:commit, _}
  end

  test "does not open a pull request after a denied broker commit" do
    assignment = assignment()
    parent = self()
    deps = publish_failure_dependencies(assignment, parent, :commit_denied)

    assert %{exit_code: 1, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "broker_commit_denied"
    assert result.checkout_lease_id == "checkout-lease"
    assert result.head_oid == nil
    assert_receive {:commit, 1}
    assert_receive {:revoked, "publish-lease"}
    refute_receive {:pull_request, _}
  end

  test "does not commit after a denied branch creation and retains checkout receipt" do
    assignment = assignment()
    parent = self()
    deps = publish_failure_dependencies(assignment, parent, :branch_denied)

    assert %{exit_code: 1, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "broker_branch_denied"
    assert result.checkout_lease_id == "checkout-lease"
    assert result.checkout_revocation == "confirmed"
    assert_receive {:revoked, "checkout-lease"}
    assert_receive {:revoked, "publish-lease"}
    refute_receive {:commit, _}
  end

  test "holds an unrecognized branch response and revokes the publish lease without committing" do
    assignment = assignment()
    parent = self()
    deps = publish_failure_dependencies(assignment, parent, :branch_uncertain)

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "broker_branch_uncertain"
    assert result.checkout_lease_id == "checkout-lease"
    assert result.checkout_revocation == "confirmed"
    assert result.broker_lease_id == "publish-lease"
    assert result.revocation == "confirmed"
    assert_receive {:revoked, "publish-lease"}
    refute_receive {:commit, _}
  end

  test "does not modify the broker when Codex changes an existing workspace file" do
    assignment = assignment()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "checkout-workspace", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:ok, %{installation_token: "secret"}} end,
      broker_revoke: fn _lease, _ctx -> :ok end,
      broker_branch: fn _lease, _base, _ref, _ctx -> flunk("branch after unsupported workspace change") end,
      command: fn
        "git", ["-c", "credential.helper=", "clone" | _args], _opts -> {"", 0}
        "git", ["-C", "/workspace", "switch", "--create", "--", _branch], _opts -> {"", 0}
        "codex", _args, _opts -> {"{}", 0}
        "git", ["-C", "/workspace", "rev-parse", "HEAD"], _opts -> {String.duplicate("a", 40), 0}
        "git", ["-C", "/workspace", "status", "--porcelain=v1", "-z", "--untracked-files=all"], _opts -> {" M tracked.txt\0", 0}
      end
    }

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "unsupported_workspace_changes"
    assert result.checkout_lease_id == "checkout-workspace"
    assert result.checkout_revocation == "confirmed"
    assert result.broker_lease_id == nil
  end

  test "rejects untracked paths outside the workspace before reading files" do
    assignment = assignment()
    test_pid = self()

    deps = %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "checkout-path", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:ok, %{installation_token: "secret"}} end,
      broker_revoke: fn lease, _ctx ->
        send(test_pid, {:revoked, lease})
        :ok
      end,
      command: fn
        "git", ["-c", "credential.helper=", "clone" | _args], _opts -> {"", 0}
        "git", ["-C", "/workspace", "switch", "--create", "--", _branch], _opts -> {"", 0}
        "codex", _args, _opts -> {"{}", 0}
        "git", ["-C", "/workspace", "rev-parse", "HEAD"], _opts -> {String.duplicate("a", 40), 0}
        "git", ["-C", "/workspace", "status", "--porcelain=v1", "-z", "--untracked-files=all"], _opts -> {"?? ../outside.txt\0", 0}
      end
    }

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "unsupported_workspace_changes"
    assert_receive {:revoked, "checkout-path"}
    refute_receive {:revoked, "publish-lease"}
  end

  test "reads additions from regular files under the workspace and rejects missing files" do
    assignment = assignment()
    parent = self()
    workspace = Path.join(System.tmp_dir!(), "symphony-worker-#{System.unique_integer([:positive])}")
    File.mkdir_p!(Path.join(workspace, "nested"))
    File.write!(Path.join(workspace, "nested/added.txt"), "proof\n")
    on_exit(fn -> File.rm_rf!(workspace) end)

    deps = %{
      workspace_path: workspace,
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      broker_issue: fn _subject, use, _key, _now, _ttl, _ctx ->
        if use == :git_checkout,
          do: {:ok, %{"leaseId" => "checkout-files", "notAfter" => "2026-09-27T00:10:00Z"}},
          else: {:error, :denied}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:ok, %{installation_token: "secret"}} end,
      broker_revoke: fn lease, _ctx ->
        send(parent, {:revoked, lease})
        :ok
      end,
      command: fn
        "git", ["-c", "credential.helper=", "clone" | _args], _opts -> {"", 0}
        "git", ["-C", ^workspace, "switch", "--create", "--", _branch], _opts -> {"", 0}
        "codex", _args, _opts -> {"{}", 0}
        "git", ["-C", ^workspace, "rev-parse", "HEAD"], _opts -> {String.duplicate("a", 40), 0}
        "git", ["-C", ^workspace, "status", "--porcelain=v1", "-z", "--untracked-files=all"], _opts -> {"?? nested/added.txt\0", 0}
      end
    }

    assert %{exit_code: 1, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "credential_issuance_denied"
    assert result.checkout_lease_id == "checkout-files"
    assert_receive {:revoked, "checkout-files"}

    File.write!(Path.join(workspace, "nested/added.txt"), :binary.copy("x", 512 * 1024 + 1))

    assert %{exit_code: 2, result: %{reason: "unsupported_workspace_changes"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    File.write!(Path.join(workspace, "nested/added.txt"), <<255>>)

    assert %{exit_code: 2, result: %{reason: "unsupported_workspace_changes"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    missing_deps =
      Map.put(deps, :command, fn
        "git", ["-c", "credential.helper=", "clone" | _args], _opts -> {"", 0}
        "git", ["-C", ^workspace, "switch", "--create", "--", _branch], _opts -> {"", 0}
        "codex", _args, _opts -> {"{}", 0}
        "git", ["-C", ^workspace, "rev-parse", "HEAD"], _opts -> {String.duplicate("a", 40), 0}
        "git", ["-C", ^workspace, "status", "--porcelain=v1", "-z", "--untracked-files=all"], _opts -> {"?? nested/missing.txt\0", 0}
      end)

    assert %{exit_code: 2, result: missing_result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), missing_deps)

    assert missing_result.reason == "unsupported_workspace_changes"
    refute_receive {:revoked, "publish-lease"}
  end

  test "retains checkout receipt when publish lease issuance is denied" do
    assignment = assignment()
    parent = self()
    deps = publish_failure_dependencies(assignment, parent, :publish_issue_denied)

    deps =
      Map.put(deps, :broker_issue, fn _subject, use, _key, _now, _ttl, _ctx ->
        if use == :git_checkout,
          do: {:ok, %{"leaseId" => "checkout-lease", "notAfter" => "2026-09-27T00:10:00Z"}},
          else: {:error, :denied}
      end)

    assert %{exit_code: 1, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "credential_issuance_denied"
    assert result.checkout_lease_id == "checkout-lease"
    assert result.checkout_revocation == "confirmed"
    assert result.broker_lease_id == nil
    assert_receive {:revoked, "checkout-lease"}
    refute_receive {:commit, _}
  end

  test "holds an uncertain pull request result with the confirmed commit OID" do
    assignment = assignment()
    parent = self()
    deps = publish_failure_dependencies(assignment, parent, :pr_uncertain)

    assert %{exit_code: 2, result: result} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert result.reason == "pull_request_uncertain"
    assert result.checkout_lease_id == "checkout-lease"
    assert result.checkout_revocation == "confirmed"
    assert result.head_oid == String.duplicate("c", 40)
    assert result.broker_lease_id == "publish-lease"
    assert_receive {:revoked, "publish-lease"}
  end

  test "revokes a denied pull request lease after the single commit" do
    assignment = assignment()
    parent = self()
    deps = publish_failure_dependencies(assignment, parent, :pr_denied)

    assert %{exit_code: 1, result: %{status: "failed"}} =
             CLI.run(["--assignment-json", Jason.encode!(assignment)], environment(assignment), deps)

    assert_receive {:revoked, "publish-lease"}
  end

  defp publish_failure_dependencies(assignment, parent, failure) do
    %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      create_askpass: fn -> :ok end,
      now: fn -> ~U[2026-09-27 00:00:00Z] end,
      broker_issue: fn _subject, use, _key, _now, _ttl, _ctx ->
        id = if use == :git_checkout, do: "checkout-lease", else: "publish-lease"
        {:ok, %{"leaseId" => id, "notAfter" => "2026-09-27T00:10:00Z", "repositoryRef" => assignment.repository_ref}}
      end,
      broker_checkout: fn _lease, _repo, _cutoff, _ctx -> {:ok, %{installation_token: "secret"}} end,
      read_additions: fn ["added.txt"] -> {:ok, [%{path: "added.txt", contents: "proof\n"}]} end,
      broker_branch: fn "publish-lease", _base, "refs/heads/codex/hgs729-canary", _ctx ->
        branch_result(failure)
      end,
      broker_revoke: fn lease, _ctx ->
        send(parent, {:revoked, lease})
        :ok
      end,
      broker_commit: fn "publish-lease", _head, _message, _additions, _ctx ->
        send(parent, {:commit, 1})
        commit_result(failure)
      end,
      broker_pull_request: fn "publish-lease", _repo, _ctx ->
        send(parent, {:pull_request, 1})
        pull_request_result(failure)
      end,
      command: &publish_failure_command/3
    }
  end

  defp branch_result(:branch_denied), do: {:error, :broker_denied}
  defp branch_result(:branch_uncertain), do: :unexpected_branch_result
  defp branch_result(_failure), do: {:ok, String.duplicate("b", 40)}

  defp commit_result(:commit_uncertain), do: {:held, :broker_uncertain}
  defp commit_result(:commit_denied), do: {:error, :broker_denied}
  defp commit_result(_failure), do: {:ok, String.duplicate("c", 40)}

  defp pull_request_result(:pr_denied), do: {:error, :broker_denied}
  defp pull_request_result(:pr_uncertain), do: {:held, :broker_uncertain}
  defp pull_request_result(_failure), do: {:ok, %{number: 1, url: "https://github.com/hypergridau/symphony/pull/1"}}

  defp publish_failure_command(executable, args, _opts) do
    case {executable, args} do
      {"git", ["-c", "credential.helper=", "clone", "--branch", "main", "--single-branch", "--no-tags", "--", _, "/workspace"]} -> {"", 0}
      {"git", ["-C", "/workspace", "switch", "--create", "--", "codex/hgs729-canary"]} -> {"", 0}
      {"codex", _args} -> {"{}", 0}
      {"git", ["-C", "/workspace", "rev-parse", "HEAD"]} -> {String.duplicate("b", 40) <> "\n", 0}
      {"git", ["-C", "/workspace", "status", "--porcelain=v1", "-z", "--untracked-files=all"]} -> {"?? added.txt\0", 0}
      _ -> flunk("unexpected worker command: #{inspect({executable, args})}")
    end
  end

  defp successful_dependencies(test_pid, revoke_result) do
    token = "github-installation-secret"

    %{
      auth_slot_ready: fn -> true end,
      workspace_ready: fn -> true end,
      create_askpass: fn -> :ok end,
      now: fn -> ~U[2026-09-27 00:00:00Z] end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        {:ok, %{"leaseId" => "lease-1", "notAfter" => "2026-09-27T00:10:00Z"}}
      end,
      broker_checkout: fn _lease, _repository_ref, _cutoff, _ctx -> {:ok, %{installation_token: token}} end,
      broker_revoke: fn _lease, _ctx -> revoke_result end,
      command: fn executable, args, _opts ->
        send(test_pid, {:command, executable, args})

        case {executable, args} do
          {"git", ["-c", "credential.helper=", "clone", "--branch", "main", "--single-branch", "--no-tags", "--", _, "/workspace"]} -> {"", 0}
          {"git", ["-C", "/workspace", "switch", "--create", "--", "codex/hgs729-canary"]} -> {"", 0}
          {"codex", _args} -> {"{}", 0}
          {"git", ["-C", "/workspace", "rev-parse", "HEAD"]} -> {String.duplicate("b", 40) <> "\n", 0}
          {"git", ["-C", "/workspace", "status", "--porcelain=v1", "-z", "--untracked-files=all"]} -> {"", 0}
          _ -> flunk("unexpected worker command: #{inspect({executable, args})}")
        end
      end
    }
  end

  defp environment(assignment) do
    %{
      "SYMPHONY_ASSIGNMENT_SHA256" => assignment.sha256,
      "SYMPHONY_ASSIGNMENT_ID" => JobSpec.identity(assignment),
      "SYMPHONY_REPOSITORY_ID" => "123456789",
      "SYMPHONY_WORKER_MODE" => "codex",
      "CODEX_HOME" => "/var/lib/frigga-codex-home"
    }
  end

  defp no_operation_deps do
    %{
      auth_slot_ready: fn -> flunk("invalid HGS-736 intent must fail before auth check") end,
      workspace_ready: fn -> flunk("invalid HGS-736 intent must fail before workspace check") end,
      broker_issue: fn _subject, _use, _key, _now, _ttl, _ctx ->
        flunk("invalid HGS-736 intent must not issue a lease")
      end,
      broker_revoke: fn _lease, _ctx -> flunk("invalid HGS-736 intent must not revoke a lease") end,
      broker_checkout_denial: fn _lease, _repo, _cutoff, _ctx ->
        flunk("invalid HGS-736 intent must not check out")
      end,
      command: fn executable, args, _opts ->
        flunk("invalid HGS-736 intent must not invoke #{executable}: #{inspect(args)}")
      end
    }
  end

  defp assignment do
    assignment(
      "937400ab-b95e-4ddb-8adf-e28bf13c3852",
      4,
      ["repository", "no-production-workload"]
    )
  end

  defp hgs736_assignment do
    assignment(
      @hgs736_issue_uuid,
      1,
      ["repository", "no-production-workload", @hgs736_no_checkout_constraint]
    )
  end

  defp assignment(issue_id, generation, constraints) do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Add a bounded canary file"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-canary",
        seat: "runner-17",
        lease: %{
          issue_id: issue_id,
          repository: "hypergridau/symphony",
          generation: generation,
          session_id: "worker:assignment:#{generation}",
          process_id: "worker:assignment:#{generation}"
        },
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Canary file", evidence: "Focused test coverage"},
        context_secret_refs: [],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: constraints,
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end
end
