defmodule SymphonyElixir.WorkPackageStartedNoCheckoutReceipt do
  @moduledoc """
  Replays the trusted host's finalized started Job failure to Dahlia.

  The immutable receipt is saved in the retained claim journal before the
  provider request. A lost response can be retried with a fresh HMAC timestamp.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{DisposableCleanupEvidence, ResultJournal}
  alias SymphonyElixir.WorkPackageClaim.Journal

  @kind "started_no_checkout_cleanup_verified"
  @version "work-package-started-no-checkout-receipt.v1"
  @timeout_ms 10_000
  @fields [
    {:contract_version, "contractVersion"},
    {:receipt_id, "receiptId"},
    {:receipt_kind, "receiptKind"},
    {:assignment_digest, "assignmentDigest"},
    {:job_namespace, "jobNamespace"},
    {:job_name, "jobName"},
    {:job_uid, "jobUid"},
    {:pod_uid, "podUid"},
    {:slot_lease_id, "slotLeaseId"},
    {:checkout_lease_id, "checkoutLeaseId"},
    {:result_sha256, "resultSHA256"},
    {:execution_started, "executionStarted"},
    {:checkout_accepted, "checkoutAccepted"},
    {:job_and_pods_absent, "jobAndPodsAbsent"},
    {:oauth_slot_released, "oauthSlotReleased"},
    {:credential_leases_terminal, "credentialLeasesTerminal"},
    {:observed_at, "observedAt"},
    {:evidence_ref, "evidenceRef"},
    {:runner_id, "runnerId"},
    {:managed_project_profile_id, "managedProjectProfileId"},
    {:reservation_id, "reservationId"},
    {:reservation_nonce, "reservationNonce"},
    {:issue_id, "issueId"},
    {:generation, "generation"},
    {:session_id, "sessionId"},
    {:process_id, "processId"},
    {:responsible_delegation_id, "responsibleDelegationId"},
    {:execution_fence_token, "executionFenceToken"},
    {:runtime_lease_id, "runtimeLeaseId"},
    {:repository_ref, "repositoryRef"},
    {:scope_keys, "scopeKeys"},
    {:attested_at, "attestedAt"}
  ]

  @doc "Submits one exact finalized failure, retaining its semantic receipt and provider acknowledgement."
  @spec submit(map(), map(), map()) :: {:ok, map()} | {:error, term()}
  def submit(runtime, fence, token), do: submit(runtime, fence, token, [])

  @spec submit(map(), map(), map(), keyword()) :: {:ok, map()} | {:error, term()}
  def submit(runtime, fence, token, opts)
      when is_map(runtime) and is_map(fence) and is_map(token) and is_list(opts) do
    with {:ok, evidence_ref} <- DisposableCleanupEvidence.verify_no_checkout_failure(runtime, fence, token),
         {:ok, context} <- context(runtime, fence, token, evidence_ref),
         {:ok, now} <- current_time(Keyword.get(opts, :now_fun, &DateTime.utc_now/0)),
         {:ok, semantic, journal} <- retained_semantic(context, now),
         :ok <- Journal.save(runtime.journal_path, journal) do
      case stored_ack(semantic, context) do
        {:ok, ack} -> {:ok, ack}
        :missing -> submit_unacknowledged(context, journal, semantic, now, opts)
        {:error, _} = error -> error
      end
    end
  rescue
    _ -> {:error, :started_no_checkout_receipt_unavailable}
  end

  def submit(_runtime, _fence, _token, _opts), do: {:error, :started_no_checkout_receipt_invalid}

  @doc "Encodes the exact provider HMAC field order, including sorted scope keys."
  @spec canonical_json(map()) :: {:ok, String.t()} | {:error, term()}
  def canonical_json(receipt) when is_map(receipt) do
    if Enum.all?(@fields, fn {key, _wire} -> Map.has_key?(receipt, key) end) and
         is_list(receipt.scope_keys) do
      fields =
        Enum.map(@fields, fn
          {:scope_keys, wire} -> {wire, Enum.sort(receipt.scope_keys)}
          {key, wire} -> {wire, Map.fetch!(receipt, key)}
        end)

      {:ok,
       fields
       |> Enum.map(fn {key, value} -> [Jason.encode!(key), ":", Jason.encode!(value)] end)
       |> Enum.intersperse(",")
       |> then(&["{", &1, "}"])
       |> IO.iodata_to_binary()}
    else
      {:error, :started_no_checkout_receipt_invalid}
    end
  end

  def canonical_json(_receipt), do: {:error, :started_no_checkout_receipt_invalid}

  @doc "Signs the immutable semantic tuple with a fresh attestation timestamp."
  @spec sign(map(), String.t()) :: {:ok, String.t()} | {:error, term()}
  def sign(receipt, key) when is_map(receipt) and is_binary(key) and byte_size(key) > 0 do
    with {:ok, canonical} <- canonical_json(receipt) do
      {:ok, :crypto.mac(:hmac, :sha256, key, canonical) |> Base.url_encode64(padding: false)}
    end
  end

  def sign(_receipt, _key), do: {:error, :started_no_checkout_attestation_unavailable}

  defp context(runtime, fence, token, evidence_ref) do
    result_root = get_in(runtime, [:disposable_rke2_host_config, :result_journal_root])

    with path when is_binary(path) <- Map.get(runtime, :journal_path),
         profile when is_binary(profile) <- Map.get(runtime, :managed_project_profile_id),
         repository when is_binary(repository) <- Map.get(token, :repository_ref),
         issue when is_binary(issue) <- Map.get(token, :issue_id),
         generation when is_integer(generation) <- Map.get(token, :generation),
         {:ok, journal} <- Journal.load(path),
         key = Journal.reservation_key(issue, profile, repository, generation),
         saved when is_map(saved) <- Map.get(journal.reservations, key),
         %{dispatch: %{phase: "spawn_started", allocation_id: allocation_id}} <- saved,
         {:ok, assignment} <- ManagedAssignmentBundle.from_snapshot(saved.assignment_snapshot),
         %{environment: %{target_environment: :rke2}} <- assignment,
         lease when is_map(lease) <- get_in(fence, [:executions, issue, :leases, saved.session_id]),
         %{job_uid: job_uid, pod_uid: pod_uid, terminal_status: "failed"} <- Map.get(lease, :termination_evidence),
         {:ok, observation, slot} <- ResultJournal.load_with_slot(assignment, job_uid, result_root),
         %{lease_id: slot_lease_id} <- slot,
         {:ok, marker} <- ResultJournal.load_finalization(assignment, job_uid, result_root),
         true <- marker["pod_uid"] == pod_uid and observation["pod_uid"] == pod_uid,
         {:ok, namespace, job_name} <- job_identity(allocation_id, assignment.sha256, job_uid),
         true <-
           saved.runner_id == runtime.runner_id and saved.repository_ref == repository and
             saved.generation == generation and saved.issue_id == issue do
      {:ok,
       %{
         runtime: runtime,
         journal: journal,
         key: key,
         saved: saved,
         assignment: assignment,
         observation: observation,
         marker: marker,
         slot_lease_id: slot_lease_id,
         namespace: namespace,
         job_name: job_name,
         job_uid: job_uid,
         pod_uid: pod_uid,
         evidence_ref: evidence_ref
       }}
    else
      _ -> {:error, :started_no_checkout_authority_unverified}
    end
  end

  defp job_identity(allocation_id, digest, uid) do
    with "rke2job:v1:" <> encoded <- allocation_id,
         {:ok, bytes} <- Base.url_decode64(encoded, padding: false),
         {:ok, [1, namespace, name, ^uid, ^digest]} <- Jason.decode(bytes),
         true <- is_binary(namespace) and is_binary(name) and namespace != "" and name != "" do
      {:ok, namespace, name}
    else
      _ -> {:error, :started_no_checkout_job_mismatch}
    end
  end

  defp retained_semantic(context, now) do
    current = get_in(context.journal, [:reservations, context.key, :cleanup_receipts, @kind])
    observed_at = if is_map(current), do: current.observed_at, else: DateTime.to_iso8601(now)
    expected = semantic(context, observed_at)

    cond do
      is_map(current) and Map.drop(current, [:acknowledgement]) == expected ->
        {:ok, current, context.journal}

      is_map(current) ->
        {:error, :started_no_checkout_receipt_conflict}

      true ->
        case Journal.put_cleanup_receipt(context.journal, context.key, @kind, expected) do
          {:ok, journal} -> {:ok, expected, journal}
          {:error, _} = error -> error
        end
    end
  end

  defp semantic(context, observed_at) do
    saved = context.saved
    marker = context.marker
    result = context.observation["result"]

    seed =
      saved.reservation_id <>
        "\0" <>
        context.assignment.sha256 <>
        "\0" <>
        context.job_uid <> "\0" <> marker["result_sha256"]

    receipt_id = "hgs736-" <> (:crypto.hash(:sha256, seed) |> Base.encode16(case: :lower))

    %{
      contract_version: @version,
      receipt_id: receipt_id,
      receipt_kind: @kind,
      assignment_digest: context.assignment.sha256,
      job_namespace: context.namespace,
      job_name: context.job_name,
      job_uid: context.job_uid,
      pod_uid: context.pod_uid,
      slot_lease_id: context.slot_lease_id,
      checkout_lease_id: result["checkout_lease_id"],
      result_sha256: marker["result_sha256"],
      execution_started: true,
      checkout_accepted: false,
      job_and_pods_absent: true,
      oauth_slot_released: true,
      credential_leases_terminal: true,
      observed_at: observed_at,
      evidence_ref: context.evidence_ref,
      runner_id: saved.runner_id,
      managed_project_profile_id: saved.managed_project_profile_id,
      reservation_id: saved.reservation_id,
      reservation_nonce: saved.reservation_nonce,
      issue_id: saved.issue_id,
      generation: saved.generation,
      session_id: saved.session_id,
      process_id: saved.process_id,
      responsible_delegation_id: saved.responsible_delegation_id,
      execution_fence_token: saved.execution_fence_token,
      runtime_lease_id: saved.runtime_lease_id,
      repository_ref: saved.repository_ref,
      scope_keys: Enum.sort(saved.scope_keys)
    }
  end

  defp stored_ack(semantic, context) do
    case Map.get(semantic, :acknowledgement) do
      nil -> :missing
      ack when is_map(ack) -> validate_ack(ack, semantic, context)
      _ -> {:error, :started_no_checkout_ack_invalid}
    end
  end

  defp submit_unacknowledged(context, journal, semantic, now, opts) do
    receipt = Map.put(semantic, :attested_at, DateTime.to_iso8601(now))
    request_fun = Keyword.get(opts, :request_fun, &Req.post/2)

    with {:ok, signature} <- sign(receipt, context.runtime.attestation_key),
         wire = wire_receipt(Map.put(receipt, :signature, signature)),
         {:ok, data} <- request(context, wire, request_fun),
         {:ok, ack} <- validate_ack(data, semantic, context),
         {:ok, updated} <- Journal.put_cleanup_receipt_ack(journal, context.key, @kind, ack),
         :ok <- Journal.save(context.runtime.journal_path, updated) do
      {:ok, ack}
    end
  end

  defp request(context, wire, request_fun) when is_function(request_fun, 2) do
    url =
      String.trim_trailing(context.runtime.base_url, "/") <>
        "/runner/v1/work-packages/" <> context.saved.projection_id <> "/started-no-checkout-receipt"

    options = [
      headers: [{"authorization", "Bearer #{context.runtime.runner_token}"}],
      json: wire,
      connect_options: [timeout: @timeout_ms],
      receive_timeout: @timeout_ms,
      retry: false
    ]

    case request_fun.(url, options) do
      {:ok, %Req.Response{status: status, body: body}} when status in 200..299 ->
        case body do
          %{"data" => data} when is_map(data) -> {:ok, data}
          data when is_map(data) -> {:ok, data}
          _ -> {:error, :started_no_checkout_provider_payload_invalid}
        end

      {:ok, %Req.Response{status: status}} ->
        {:error, {:started_no_checkout_provider_status, status}}

      {:error, reason} ->
        {:error, {:started_no_checkout_provider_request, reason}}

      _ ->
        {:error, :started_no_checkout_provider_response_invalid}
    end
  end

  defp wire_receipt(receipt) do
    Enum.reduce(@fields, %{}, fn {atom, wire}, acc -> Map.put(acc, wire, Map.fetch!(receipt, atom)) end)
    |> Map.put("signature", receipt.signature)
  end

  defp validate_ack(data, semantic, context) when is_map(data) do
    wire =
      Map.new(data, fn {key, value} ->
        {if(is_atom(key), do: ack_wire_key(key), else: key), value}
      end)

    expected = %{
      "projectionId" => context.saved.projection_id,
      "reservationId" => context.saved.reservation_id,
      "receiptId" => semantic.receipt_id,
      "receiptKind" => @kind,
      "executionCapacityState" => "released",
      "scopeState" => "released",
      "reservationState" => "released",
      "generation" => semantic.generation,
      "evidenceRef" => semantic.evidence_ref
    }

    if Enum.all?(expected, fn {key, value} -> Map.get(wire, key) == value end) and
         is_boolean(Map.get(wire, "replayed")) and not Map.has_key?(wire, "acceptedHead") do
      {:ok,
       %{
         projection_id: wire["projectionId"],
         reservation_id: wire["reservationId"],
         receipt_id: wire["receiptId"],
         receipt_kind: wire["receiptKind"],
         execution_capacity_state: wire["executionCapacityState"],
         scope_state: wire["scopeState"],
         reservation_state: wire["reservationState"],
         generation: wire["generation"],
         evidence_ref: wire["evidenceRef"],
         replayed: wire["replayed"]
       }}
    else
      {:error, :started_no_checkout_ack_invalid}
    end
  end

  defp ack_wire_key(:projection_id), do: "projectionId"
  defp ack_wire_key(:reservation_id), do: "reservationId"
  defp ack_wire_key(:receipt_id), do: "receiptId"
  defp ack_wire_key(:receipt_kind), do: "receiptKind"
  defp ack_wire_key(:execution_capacity_state), do: "executionCapacityState"
  defp ack_wire_key(:scope_state), do: "scopeState"
  defp ack_wire_key(:reservation_state), do: "reservationState"
  defp ack_wire_key(:generation), do: "generation"
  defp ack_wire_key(:evidence_ref), do: "evidenceRef"
  defp ack_wire_key(:replayed), do: "replayed"
  defp ack_wire_key(key), do: Atom.to_string(key)

  defp current_time(fun) when is_function(fun, 0) do
    case fun.() do
      %DateTime{} = now -> {:ok, DateTime.truncate(now, :millisecond)}
      _ -> {:error, :started_no_checkout_clock_invalid}
    end
  end

  defp current_time(_), do: {:error, :started_no_checkout_clock_invalid}
end
