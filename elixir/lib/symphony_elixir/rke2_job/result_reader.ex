defmodule SymphonyElixir.RKE2Job.ResultReader do
  @moduledoc """
  Reads one worker termination receipt before deleting its exact owned Job.

  This is a read-only source port. The caller must durably journal a successful
  observation before cleanup; a receipt alone does not release an OAuth slot.
  """

  alias SymphonyElixir.RKE2Job.{HTTPClient, JobSpec}

  @max_message_bytes 3_500
  @hex40 ~r/\A[a-f0-9]{40}\z/
  @hex64 ~r/\A[a-f0-9]{64}\z/
  @safe_reason ~r/\A[a-z][a-z0-9_]{0,127}\z/
  @safe_id ~r/\A[A-Za-z0-9][A-Za-z0-9._:-]*\z/
  @result_keys ~w(schema_version status reason assignment_digest issue_uuid generation repository_ref branch_ref checkout_lease_id checkout_revocation broker_lease_id revocation codex_exit_code head_oid branch_head_oid base_oid changed_files pull_request_number pull_request_url)
  @pre_checkout_denials ~w(codex_auth_slot_unavailable workspace_not_empty workspace_unavailable credential_issuance_denied)
  @checkout_failures ~w(credential_checkout_denied repository_checkout_failed)

  @spec read(map(), String.t(), keyword()) :: {:ok, map()} | {:held, atom()} | {:error, atom()}
  def read(assignment, job_uid, opts) when is_map(assignment) and is_binary(job_uid) and is_list(opts) do
    client = Keyword.get(opts, :client, HTTPClient)
    context = Keyword.get(opts, :client_context)

    with true <- valid_uid?(job_uid),
         true <-
           is_atom(client) and Code.ensure_loaded?(client) and
             function_exported?(client, :get_job, 3) and
             function_exported?(client, :list_pods_snapshot, 2),
         {:ok, expected} <- JobSpec.compile(assignment, Keyword.get(opts, :config)) do
      namespace = get_in(expected, ["metadata", "namespace"])
      name = get_in(expected, ["metadata", "name"])

      case client.get_job(namespace, name, context) do
        {:ok, job} -> read_pods(client, context, namespace, job_uid, expected, job)
        _ -> {:held, :job_result_read_unavailable}
      end
    else
      false -> {:error, :invalid_result_reader_request}
      {:error, _} -> {:error, :invalid_result_reader_request}
    end
  rescue
    _ -> {:held, :job_result_read_unavailable}
  end

  def read(_assignment, _job_uid, _opts), do: {:error, :invalid_result_reader_request}

  @doc "Checks the bounded worker schema and exact CLI status/exit pairing for host journaling."
  @spec valid_receipt_outcome?(map(), integer()) :: boolean()
  def valid_receipt_outcome?(result, exit_code) when is_map(result) and is_integer(exit_code) do
    mode = if result["status"] == "preflight_passed", do: "preflight", else: "codex"
    env = [%{"name" => "SYMPHONY_WORKER_MODE", "value" => mode}]

    valid_result_fields?(result, env) and
      case result["status"] do
        "preflight_passed" -> exit_code == 0
        "completed" -> exit_code == 0
        "failed" -> exit_code == 1
        "held" -> exit_code == 2
        _ -> false
      end
  end

  def valid_receipt_outcome?(_result, _exit_code), do: false

  @doc "Classifies a failed worker result reporting no Git head or publish lease."
  @spec no_checkout_failure?(map(), integer()) :: boolean()
  def no_checkout_failure?(result, 1) when is_map(result) do
    no_git_artifacts? =
      Enum.all?(
        ~w(broker_lease_id codex_exit_code head_oid branch_head_oid base_oid changed_files pull_request_number pull_request_url),
        &is_nil(result[&1])
      )

    valid_receipt_outcome?(result, 1) and result["status"] == "failed" and no_git_artifacts? and
      no_checkout_lease_state?(result)
  end

  def no_checkout_failure?(_result, _exit_code), do: false

  defp no_checkout_lease_state?(%{"checkout_lease_id" => nil} = result),
    do:
      result["reason"] in @pre_checkout_denials and
        result["checkout_revocation"] == "not_started" and result["revocation"] == "not_started"

  defp no_checkout_lease_state?(result),
    do:
      result["reason"] in @checkout_failures and
        result["checkout_revocation"] == "confirmed" and result["revocation"] == "confirmed"

  defp read_pods(client, context, namespace, uid, expected, job) do
    if JobSpec.owned_job_for_cleanup?(job, expected) and get_in(job, ["metadata", "uid"]) == uid and
         terminal_job?(job) and valid_version?(get_in(job, ["metadata", "resourceVersion"])) do
      case client.list_pods_snapshot(namespace, context) do
        {:ok, snapshot} -> parse_snapshot(expected, job, uid, snapshot)
        _ -> {:held, :job_pod_result_read_unavailable}
      end
    else
      {:held, :job_result_identity_or_terminal_unverified}
    end
  end

  defp parse_snapshot(expected, job, uid, %{items: items, resource_version: list_version})
       when is_list(items) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])
    digest = get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-sha256"])

    if valid_version?(list_version) and Enum.all?(items, &valid_pod?(&1, namespace)) do
      candidates = Enum.filter(items, &candidate?(&1, name, uid, digest))

      case candidates do
        [pod] -> parse_pod(expected, job, uid, pod, list_version)
        _ -> {:held, :job_result_pod_ambiguous}
      end
    else
      {:held, :job_pod_result_read_invalid}
    end
  end

  defp parse_snapshot(_expected, _job, _uid, _snapshot), do: {:held, :job_pod_result_read_invalid}

  defp parse_pod(expected, job, uid, pod, list_version) do
    name = get_in(expected, ["metadata", "name"])
    owners = get_in(pod, ["metadata", "ownerReferences"])
    status = pod["status"]
    worker = get_in(expected, ["spec", "template", "spec", "containers"]) |> List.first()
    statuses = if is_map(status), do: status["containerStatuses"], else: nil

    with true <- owned_pod?(owners, name, uid),
         {:ok, terminated} <- terminated_worker(worker, statuses),
         true <- valid_version?(get_in(pod, ["metadata", "resourceVersion"])),
         {:ok, result} <- parse_message(terminated["message"], expected),
         true <- consistent_outcome?(job, status, terminated["exitCode"], result) do
      {:ok,
       %{
         job_uid: uid,
         job_resource_version: get_in(job, ["metadata", "resourceVersion"]),
         pod_uid: get_in(pod, ["metadata", "uid"]),
         pod_resource_version: get_in(pod, ["metadata", "resourceVersion"]),
         pod_list_resource_version: list_version,
         exit_code: terminated["exitCode"],
         result: result
       }}
    else
      _ -> {:held, :job_result_pod_or_receipt_unverified}
    end
  end

  defp owned_pod?(owners, name, uid) when is_list(owners) do
    controller = Enum.filter(owners, &(&1["controller"] == true))

    case controller do
      [%{"apiVersion" => "batch/v1", "kind" => "Job", "name" => ^name, "uid" => ^uid}] -> true
      _ -> false
    end
  end

  defp owned_pod?(_owners, _name, _uid), do: false

  defp terminated_worker(%{"name" => name}, [%{"name" => name, "ready" => false, "state" => %{"terminated" => terminated}}])
       when is_map(terminated) do
    if is_integer(terminated["exitCode"]) and terminated["exitCode"] in 0..255 and
         is_binary(terminated["message"]),
       do: {:ok, terminated},
       else: :error
  end

  defp terminated_worker(_worker, _statuses), do: :error

  defp parse_message(message, expected) when byte_size(message) <= @max_message_bytes do
    case Jason.decode(message) do
      {:ok, %{"schema_version" => 1} = result} ->
        digest = get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-sha256"])
        issue = get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-issue-id"])
        generation = get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-generation"])
        env = get_in(expected, ["spec", "template", "spec", "containers"]) |> List.first() |> Map.get("env", [])
        repo = expected_repo(expected)
        branch = expected_branch(expected)

        expected_identity = %{
          "assignment_digest" => digest,
          "issue_uuid" => issue,
          "generation" => String.to_integer(generation),
          "repository_ref" => repo,
          "branch_ref" => branch
        }

        if is_binary(digest) and Regex.match?(@hex64, digest) and
             Map.take(result, Map.keys(expected_identity)) == expected_identity and valid_result_fields?(result, env) do
          {:ok, result}
        else
          {:held, :job_result_identity_mismatch}
        end

      _ ->
        {:held, :job_result_message_invalid}
    end
  rescue
    _ -> {:held, :job_result_message_invalid}
  end

  defp parse_message(_message, _expected), do: {:held, :job_result_message_invalid}

  defp valid_result_fields?(result, env) do
    mode = Enum.find_value(env, fn entry -> if entry["name"] == "SYMPHONY_WORKER_MODE", do: entry["value"] end)
    status = result["status"]

    Enum.all?([
      Enum.sort(Map.keys(result)) == Enum.sort(@result_keys),
      valid_reason?(result["reason"]),
      optional_id?(result["checkout_lease_id"]),
      optional_id?(result["broker_lease_id"]),
      result["checkout_revocation"] in ["not_started", "confirmed", "held"],
      result["revocation"] in ["not_started", "confirmed", "held"],
      optional_oid?(result["base_oid"]),
      optional_oid?(result["branch_head_oid"]),
      optional_oid?(result["head_oid"]),
      optional_count?(result["changed_files"], 1..20),
      optional_count?(result["codex_exit_code"], 0..255),
      valid_pull_request?(result),
      valid_status?(status, mode, result)
    ])
  end

  defp valid_status?("preflight_passed", "preflight", result),
    do:
      Map.take(result, ~w(reason checkout_lease_id broker_lease_id codex_exit_code head_oid branch_head_oid base_oid changed_files checkout_revocation revocation pull_request_number pull_request_url)) ==
        %{
          "reason" => "auth_slot_required",
          "checkout_lease_id" => nil,
          "broker_lease_id" => nil,
          "codex_exit_code" => nil,
          "head_oid" => nil,
          "branch_head_oid" => nil,
          "base_oid" => nil,
          "changed_files" => nil,
          "checkout_revocation" => "not_started",
          "revocation" => "not_started",
          "pull_request_number" => nil,
          "pull_request_url" => nil
        }

  defp valid_status?("completed", "codex", result),
    do:
      Map.take(result, ~w(reason checkout_revocation revocation codex_exit_code)) ==
        %{"reason" => "pull_request_created", "checkout_revocation" => "confirmed", "revocation" => "confirmed", "codex_exit_code" => 0} and
        Enum.all?(
          ~w(checkout_lease_id broker_lease_id base_oid branch_head_oid head_oid pull_request_number changed_files),
          &(not is_nil(result[&1]))
        )

  defp valid_status?(status, "codex", %{"checkout_lease_id" => nil} = result)
       when status in ["failed", "held"] do
    Map.take(
      result,
      ~w(checkout_revocation broker_lease_id revocation codex_exit_code head_oid branch_head_oid base_oid changed_files pull_request_number pull_request_url)
    ) == %{
      "checkout_revocation" => "not_started",
      "broker_lease_id" => nil,
      "revocation" => "not_started",
      "codex_exit_code" => nil,
      "head_oid" => nil,
      "branch_head_oid" => nil,
      "base_oid" => nil,
      "changed_files" => nil,
      "pull_request_number" => nil,
      "pull_request_url" => nil
    }
  end

  defp valid_status?(status, "codex", _result) when status in ["failed", "held"], do: true
  defp valid_status?(_status, _mode, _result), do: false

  defp consistent_outcome?(job, pod_status, exit_code, result) do
    complete = condition?(job, "Complete")
    failed = condition?(job, "Failed")
    success = result["status"] in ["completed", "preflight_passed"]

    if success do
      complete and not failed and pod_status["phase"] == "Succeeded" and exit_code == 0
    else
      failed and not complete and pod_status["phase"] == "Failed" and
        exit_code == if(result["status"] == "held", do: 2, else: 1)
    end
  end

  defp condition?(job, kind) do
    job
    |> get_in(["status", "conditions"])
    |> List.wrap()
    |> Enum.any?(&(&1["type"] == kind and &1["status"] == "True"))
  end

  defp candidate?(pod, name, uid, digest) do
    metadata = pod["metadata"]
    labels = metadata["labels"] || %{}
    owners = metadata["ownerReferences"] || []

    Enum.any?(owners, &(&1["uid"] == uid or (&1["kind"] == "Job" and &1["name"] == name))) or
      labels["batch.kubernetes.io/controller-uid"] == uid or
      labels["batch.kubernetes.io/job-name"] == name or
      labels["symphony.hypergrid.au/assignment-sha256"] == digest or
      String.starts_with?(metadata["name"], name <> "-")
  end

  defp valid_pod?(%{"apiVersion" => "v1", "kind" => "Pod", "metadata" => metadata}, namespace)
       when is_map(metadata),
       do:
         Enum.all?([
           metadata["namespace"] == namespace,
           valid_uid?(metadata["uid"]),
           valid_version?(metadata["resourceVersion"]),
           is_binary(metadata["name"]),
           metadata["name"] != "",
           is_map(metadata["labels"] || %{}),
           valid_owners?(metadata["ownerReferences"] || [])
         ])

  defp valid_pod?(_pod, _namespace), do: false
  defp valid_owners?(owners) when is_list(owners), do: Enum.all?(owners, &is_map/1)
  defp valid_owners?(_owners), do: false

  defp terminal_job?(job), do: condition?(job, "Complete") != condition?(job, "Failed")

  defp expected_repo(expected) do
    expected |> get_in(["spec", "template", "spec", "containers"]) |> List.first() |> Map.get("args", []) |> Enum.at(1) |> Jason.decode!() |> Map.fetch!("repository_ref")
  end

  defp expected_branch(expected),
    do: expected |> get_in(["spec", "template", "spec", "containers"]) |> List.first() |> Map.get("args", []) |> Enum.at(1) |> Jason.decode!() |> Map.fetch!("branch") |> then(&("refs/heads/" <> &1))

  defp optional_id?(nil), do: true
  defp optional_id?(value) when is_binary(value), do: byte_size(value) <= 256 and Regex.match?(@safe_id, value)
  defp optional_id?(_value), do: false
  defp valid_reason?(value) when is_binary(value), do: Regex.match?(@safe_reason, value)
  defp valid_reason?(_value), do: false
  defp optional_count?(nil, _range), do: true
  defp optional_count?(value, range) when is_integer(value), do: value in range
  defp optional_count?(_value, _range), do: false

  defp valid_pull_request?(%{"pull_request_number" => nil, "pull_request_url" => nil}), do: true

  defp valid_pull_request?(%{"pull_request_number" => number, "pull_request_url" => url, "repository_ref" => repo})
       when is_integer(number) and number > 0 and is_binary(repo) and is_binary(url),
       do: url == "https://github.com/" <> repo <> "/pull/" <> Integer.to_string(number)

  defp valid_pull_request?(_result), do: false
  defp optional_oid?(nil), do: true
  defp optional_oid?(value) when is_binary(value), do: Regex.match?(@hex40, value)
  defp optional_oid?(_value), do: false
  defp valid_version?(value), do: valid_uid?(value)
  defp valid_uid?(value) when is_binary(value), do: byte_size(value) in 1..256 and Regex.match?(~r/\A[a-zA-Z0-9][a-zA-Z0-9._:-]*\z/, value)
  defp valid_uid?(_value), do: false
end
