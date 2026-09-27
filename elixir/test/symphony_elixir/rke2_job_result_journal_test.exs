defmodule SymphonyElixir.RKE2JobResultJournalTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.ResultJournal
  alias SymphonyElixir.Worker.CLI

  setup do
    if match?({:win32, _}, :os.type()), do: Process.put(:result_journal_windows_test_only, true)
    root = Path.join(System.tmp_dir!(), "symphony-result-journal-#{System.unique_integer([:positive])}")
    :ok = File.mkdir_p(root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(root, 0o700)
    on_exit(fn -> File.rm_rf(root) end)
    %{root: root}
  end

  test "journals one exact receipt and replays the same bytes", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)

    assert {:ok, path} = ResultJournal.record(assignment, observation, root)
    assert {:ok, ^path} = ResultJournal.record(assignment, observation, root)
    assert {:ok, loaded} = ResultJournal.load(assignment, observation.job_uid, root)
    assert loaded["job_uid"] == observation.job_uid
    assert loaded["result"]["assignment_digest"] == assignment.sha256
  end

  test "holds a conflicting observation for the same Job UID", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)
    assert {:ok, _path} = ResultJournal.record(assignment, observation, root)

    changed = %{observation | pod_resource_version: "pod-rv-10"}
    assert {:held, :job_result_journal_conflict} = ResultJournal.record(assignment, changed, root)
    assert {:ok, loaded} = ResultJournal.load(assignment, observation.job_uid, root)
    assert loaded["pod_resource_version"] == "pod-rv-8"
  end

  test "retains an incomplete record and holds recovery", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)
    path = Path.join(root, assignment.sha256 <> "-" <> observation.job_uid <> ".json")
    :ok = File.write(path, "{")
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(path, 0o600)

    assert {:held, :job_result_journal_conflict} = ResultJournal.record(assignment, observation, root)
    assert {:held, :job_result_journal_invalid} = ResultJournal.load(assignment, observation.job_uid, root)
    assert {:ok, "{"} = File.read(path)
  end

  test "rejects an existing symbolic-link journal record", %{root: root} do
    if match?({:unix, _}, :os.type()) do
      assignment = assignment()
      observation = observation(assignment)
      target = Path.join(root, "other.json")
      path = Path.join(root, assignment.sha256 <> "-" <> observation.job_uid <> ".json")
      :ok = File.write(target, "private")
      :ok = File.ln_s(target, path)

      assert {:held, :job_result_journal_read_unavailable} = ResultJournal.record(assignment, observation, root)
      assert {:held, :job_result_journal_read_unavailable} = ResultJournal.load(assignment, observation.job_uid, root)
      assert {:ok, "private"} = File.read(target)
    end
  end

  test "rejects unverified fields and a non-private journal root", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)

    extra = put_in(observation, [:result, "auth_json"], "secret")
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(assignment, extra, root)

    forged_completion =
      put_in(observation, [:result, "status"], "completed")

    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(assignment, forged_completion, root)
    assert {:error, :invalid_job_result_journal_root} = ResultJournal.record(assignment, observation, "relative")
    assert :missing = ResultJournal.load(assignment, observation.job_uid, root)

    if match?({:unix, _}, :os.type()) do
      :ok = File.chmod(root, 0o755)
      assert {:error, :invalid_job_result_journal_root} = ResultJournal.record(assignment, observation, root)
    end
  end

  test "accepts a matching held outcome with string-keyed readback and rejects changed status", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)
    result = %{observation.result | "status" => "held", "reason" => "broker_uncertain"}

    held =
      observation
      |> Map.put(:exit_code, 2)
      |> Map.put(:result, result)
      |> Map.new(fn {key, value} -> {Atom.to_string(key), value} end)

    assert {:ok, path} = ResultJournal.record(assignment, held, root)
    assert {:ok, ^path} = ResultJournal.record(assignment, held, root)
    assert {:ok, %{"exit_code" => 2}} = ResultJournal.load(assignment, held["job_uid"], root)

    false_completion = put_in(held, ["result", "status"], "completed")
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(assignment, false_completion, root)
  end

  test "holds oversized and insecure existing records without replacing them", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)
    path = Path.join(root, assignment.sha256 <> "-" <> observation.job_uid <> ".json")
    oversized = String.duplicate("x", 8_193)
    :ok = File.write(path, oversized)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(path, 0o600)

    assert {:held, :job_result_journal_invalid} = ResultJournal.load(assignment, observation.job_uid, root)
    assert {:held, :job_result_journal_conflict} = ResultJournal.record(assignment, observation, root)
    assert {:ok, ^oversized} = File.read(path)

    if match?({:unix, _}, :os.type()) do
      :ok = File.chmod(path, 0o644)
      assert {:held, :job_result_journal_read_unavailable} = ResultJournal.load(assignment, observation.job_uid, root)
      assert {:held, :job_result_journal_read_unavailable} = ResultJournal.record(assignment, observation, root)
    end
  end

  test "rejects a missing Job identity and a changed assignment", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)

    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(assignment, %{observation | job_uid: nil}, root)
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.load(assignment, "", root)
    assert :missing = ResultJournal.load(assignment, observation.job_uid, root)

    changed_assignment = %{assignment | sha256: String.duplicate("0", 64)}
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(changed_assignment, observation, root)
  end

  test "holds a validly encoded record whose assignment or Job UID was changed", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)
    assert {:ok, path} = ResultJournal.record(assignment, observation, root)

    original = File.read!(path)
    tampered = Jason.decode!(original)
    tampered = put_in(tampered, ["observation", "result", "assignment_digest"], String.duplicate("0", 64))
    :ok = File.write(path, Jason.encode!(tampered))
    assert {:held, :job_result_journal_invalid} = ResultJournal.load(assignment, observation.job_uid, root)

    tampered = Jason.decode!(original)
    tampered = put_in(tampered, ["observation", "job_uid"], "different-job")
    :ok = File.write(path, Jason.encode!(tampered))
    assert {:held, :job_result_journal_invalid} = ResultJournal.load(assignment, observation.job_uid, root)

    :ok = File.write(path, original)
    assert {:ok, _} = ResultJournal.load(assignment, observation.job_uid, root)
  end

  test "rejects missing, non-directory and linked roots", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)
    missing = Path.join(root, "missing")
    assert {:error, :invalid_job_result_journal_root} = ResultJournal.record(assignment, observation, missing)

    file = Path.join(root, "ordinary-file")
    :ok = File.write(file, "content")
    assert {:error, :invalid_job_result_journal_root} = ResultJournal.record(assignment, observation, file)

    if match?({:unix, _}, :os.type()) do
      link = Path.join(root, "linked")
      :ok = File.ln_s(root, link)
      assert {:error, :invalid_job_result_journal_root} = ResultJournal.record(assignment, observation, link)
      assert {:error, :invalid_job_result_journal_root} = ResultJournal.load(assignment, observation.job_uid, link)
    end
  end

  test "rejects malformed observations and does not create a record", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(assignment, nil, root)
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(assignment, %{observation | exit_code: -1}, root)
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(assignment, %{observation | pod_uid: "../pod"}, root)
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(assignment, %{observation | job_resource_version: ""}, root)
    assert :missing = ResultJournal.load(assignment, observation.job_uid, root)
  end

  test "holds non-file records and malformed persisted schemas", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)
    path = Path.join(root, assignment.sha256 <> "-" <> observation.job_uid <> ".json")

    :ok = File.mkdir(path)
    assert {:held, _} = ResultJournal.record(assignment, observation, root)
    assert {:held, :job_result_journal_read_unavailable} = ResultJournal.load(assignment, observation.job_uid, root)
    :ok = File.rmdir(path)

    :ok = File.write(path, Jason.encode!(%{"schema_version" => 2, "observation" => %{}}))
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(path, 0o600)
    assert {:held, :job_result_journal_invalid} = ResultJournal.load(assignment, observation.job_uid, root)

    :ok = File.write(path, Jason.encode!(%{"schema_version" => 1, "observation" => %{}, "secret" => "unexpected"}))
    assert {:held, :job_result_journal_invalid} = ResultJournal.load(assignment, observation.job_uid, root)
  end

  test "rejects invalid request types before filesystem access", %{root: root} do
    assignment = assignment()
    observation = observation(assignment)
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(nil, observation, root)
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.load(nil, observation.job_uid, root)
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.load(assignment, nil, root)
    assert {:error, :invalid_job_result_journal_root} = ResultJournal.record(assignment, observation, nil)
    assert {:error, :invalid_job_result_journal_root} = ResultJournal.load(assignment, observation.job_uid, nil)
    assert {:error, :invalid_job_result_journal_record} = ResultJournal.record(assignment, %{observation | result: nil}, root)
  end

  defp observation(assignment) do
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

    %{
      job_uid: "job-uid-1",
      job_resource_version: "job-rv-7",
      pod_uid: "pod-uid-1",
      pod_resource_version: "pod-rv-8",
      pod_list_resource_version: "list-rv-9",
      exit_code: 0,
      result: result
    }
  end

  defp assignment do
    {:ok, assignment} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs729-result-journal",
        seat: "runner-17",
        lease: %{issue_id: "issue-1", repository: "hypergridau/symphony", generation: 4, session_id: "worker:issue-1:4", process_id: "worker:issue-1:4"},
        intent_ancestry: ["objective-root", "delegation-1"],
        acceptance: %{deliverable: "Assignment bundle", evidence: "Focused test coverage"},
        context_secret_refs: [],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    assignment
  end
end
