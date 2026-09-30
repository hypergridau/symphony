defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransaction do
  @moduledoc """
  Root-only write-ahead transition for the paused HGS-740 confirmed claim.

  The signed proof is verified against exact persisted preimages before a marker is
  written. The marker contains the three validated postimages and is durable before
  the first state replacement, allowing exact crash replay without editing JSON by
  hand. Startup verification keeps admission closed while a transition is partial.
  """

  import Bitwise, only: [band: 2]

  alias SymphonyElixir.{Config, ManagedLauncherLock, Workflow}
  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence

  alias SymphonyElixir.WorkPackageClaim.{
    ConfirmedRecoveryEvidence,
    ConfirmedRecoveryKubernetes,
    ConfirmedRecoveryWAL,
    Dispatch,
    Journal
  }

  @evidence_root "/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery"
  @runtime_state_root "/srv/dahlia-runner-state"
  @issue_id "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"
  @identity_root "/srv/dahlia-runner-state/identity/claim-recovery-hgs485"
  @pause_path "/srv/dahlia-runner-state/control/global-mutable-pause.state"
  @provider_receipt_root "/etc/dahlia-managed-claim-recovery/hgs485-20260909"
  @provider_claim_fields ~w(projectionId reservationId workspaceId companyId issueId runnerId managedProjectProfileId repositoryRef scopeKeys generation sessionId processId responsibleDelegationId executionFenceToken runtimeLeaseId nonceHash)
  @provider_receipt_fields ~w(recoveryId projectionId fenceRevision oldTupleDigest oldNonceHash nextGenerationFloor confirmedAt proofDigest projectionState reservationState executionCapacityState scopeState)
  @provider_proof_fields ~w(contractVersion recoveryId projectionId fenceRevision oldTupleDigest runnerId hostIdentity bootId observedAt evidenceRef globalPause runnerStopped neverSpawned supervisedWorkerAbsent processCount workspaceAbsent localGenerationMax fenceSHA256 claimJournalSHA256)
  @local_receipt_fields ~w(assignmentDigest assignmentSHA256 completedAt contractVersion evidenceRef expected generation issueId nonce observationSHA256 pool postconditions postimages preimages proofSHA256 reservationId transactionId)
  @state_file_metadata [:major_device, :minor_device, :inode, :uid, :gid, :mode, :links, :size, :mtime, :ctime]
  @transaction_writable_roots [
    "/srv/dahlia-runner-state/run",
    "/srv/dahlia-runner-state/workspaces"
  ]
  @marker_version "work-package-hgs740-local-transition.v1"
  @receipt_version "work-package-hgs740-local-transition-receipt.v1"
  @receipt_domain "hypergrid-work-package-recovery:hgs740-local-transition-receipt.v1\0"
  @pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)
  @state_names ~w(claimJournal fence responsibilityGraph)
  @systemd_properties ~w(ActiveState ControlGroup MainPID)

  @type result :: {:ok, :applied | :already_applied} | {:error, term()}

  @doc "Applies or resumes the exact signed confirmed-claim transition as root."
  @spec apply(String.t(), String.t(), String.t(), String.t()) :: result()
  def apply(issue_id, pool, workflow_path, nonce)
      when is_binary(issue_id) and is_binary(pool) and is_binary(workflow_path) and is_binary(nonce) do
    with :ok <- require_root(),
         :ok <- require_pool(pool),
         :ok <- require_issue_id(issue_id),
         :ok <- require_paused_gate(),
         :ok <- trusted_workflow_file(workflow_path, pool),
         :ok <- Workflow.set_workflow_file_path(workflow_path),
         {:ok, runtime} <- runtime_paths(pool),
         {:ok, lock_path} <- ManagedLauncherLock.pool_lock_path(runtime.journal_path, pool),
         {:ok, {:ok, result}} <-
           ManagedLauncherLock.with_exclusive_lock(lock_path, fn ->
             apply_locked(issue_id, pool, nonce, runtime)
           end) do
      {:ok, result}
    else
      {:error, _reason} = error -> error
      _ -> {:error, :confirmed_recovery_held_closed}
    end
  rescue
    _ -> {:error, :confirmed_recovery_held_closed}
  catch
    _, _ -> {:error, :confirmed_recovery_held_closed}
  end

  def apply(_issue_id, _pool, _workflow_path, _nonce), do: {:error, :invalid_confirmed_recovery_request}

  @doc "Completes a locally applied recovery after exact provider release and fresh no-Job/no-Pod readback."
  @spec complete(String.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def complete(issue_id, pool, workflow_path)
      when is_binary(issue_id) and is_binary(pool) and is_binary(workflow_path) do
    with :ok <- require_root(),
         :ok <- require_pool(pool),
         :ok <- require_issue_id(issue_id),
         :ok <- trusted_workflow_file(workflow_path, pool),
         :ok <- Workflow.set_workflow_file_path(workflow_path),
         {:ok, runtime} <- runtime_paths(pool),
         {:ok, lock_path} <- ManagedLauncherLock.pool_lock_path(runtime.journal_path, pool),
         {:ok, {:ok, :complete}} <-
           ManagedLauncherLock.with_exclusive_lock(lock_path, fn ->
             complete_locked(issue_id, pool, runtime)
           end) do
      :ok
    else
      {:error, _reason} = error -> error
      _ -> {:error, :hgs740_completion_held_closed}
    end
  rescue
    _ -> {:error, :hgs740_completion_held_closed}
  catch
    _, _ -> {:error, :hgs740_completion_held_closed}
  end

  def complete(_issue_id, _pool, _workflow_path), do: {:error, :invalid_hgs740_completion_request}

  @doc "Verifies all durable HGS-740 markers before a pool service starts."
  @spec verify_startup(String.t(), String.t()) :: :ok | {:error, term()}
  def verify_startup(workflow_path, pool) when is_binary(workflow_path) and is_binary(pool) do
    with :ok <- require_root(),
         :ok <- require_pool(pool),
         :ok <- trusted_workflow_file(workflow_path, pool),
         :ok <- Workflow.set_workflow_file_path(workflow_path),
         :ok <- verify_all_markers(nil) do
      :ok
    else
      {:error, _reason} = error -> error
      _ -> {:error, :hgs740_startup_held_closed}
    end
  rescue
    _ -> {:error, :hgs740_startup_held_closed}
  catch
    _, _ -> {:error, :hgs740_startup_held_closed}
  end

  def verify_startup(_workflow_path, _pool), do: {:error, :invalid_hgs740_startup_request}

  @doc false
  @spec marker_directory(String.t()) :: Path.t()
  def marker_directory(issue_id), do: Path.join([@evidence_root, issue_id, "generation-2"])

  @doc false
  @spec classify_image(binary(), String.t(), binary()) ::
          :write | :already_applied | {:error, :transaction_target_conflict}
  def classify_image(current_bytes, preimage_sha256, postimage_bytes)
      when is_binary(current_bytes) and is_binary(preimage_sha256) and is_binary(postimage_bytes) do
    current_sha256 = digest(current_bytes)

    cond do
      current_sha256 == digest(postimage_bytes) -> :already_applied
      current_sha256 == preimage_sha256 -> :write
      true -> {:error, :transaction_target_conflict}
    end
  end

  def classify_image(_current_bytes, _preimage_sha256, _postimage_bytes),
    do: {:error, :transaction_target_conflict}

  @doc false
  @spec apply_image(binary() | nil, String.t(), binary(), (binary() -> :ok | {:error, term()})) ::
          :ok | {:error, term()}
  def apply_image(current_bytes, preimage_sha256, postimage_bytes, persist)
      when (is_binary(current_bytes) or is_nil(current_bytes)) and is_binary(preimage_sha256) and
             is_binary(postimage_bytes) and is_function(persist, 1) do
    case classify_image(current_bytes, preimage_sha256, postimage_bytes) do
      :already_applied -> persist.(postimage_bytes)
      :write -> persist.(postimage_bytes)
      {:error, _reason} = error -> error
    end
  end

  def apply_image(_current_bytes, _preimage_sha256, _postimage_bytes, _persist),
    do: {:error, :transaction_target_conflict}

  @doc false
  @spec pre_marker_startup_policy(boolean()) :: :ok | {:error, :hgs740_transaction_marker_missing}
  def pre_marker_startup_policy(false), do: :ok
  def pre_marker_startup_policy(true), do: {:error, :hgs740_transaction_marker_missing}

  @doc false
  @spec no_marker_startup_policy(String.t(), boolean()) :: :ok | {:error, :hgs740_transaction_marker_missing}
  def no_marker_startup_policy(@issue_id, _evidence_directory_exists),
    do: {:error, :hgs740_transaction_marker_missing}

  def no_marker_startup_policy(_issue_id, evidence_directory_exists),
    do: pre_marker_startup_policy(evidence_directory_exists)

  @doc false
  @spec local_receipt_payload(map()) :: map()
  def local_receipt_payload(fields) when is_map(fields) do
    Map.merge(fields, %{"contractVersion" => @receipt_version})
  end

  @doc false
  @spec decode_candidate_bytes(binary()) :: {:ok, map()} | {:error, :invalid_candidate_json}
  def decode_candidate_bytes(bytes) when is_binary(bytes) do
    with {:ok, ordered} <- Jason.decode(bytes, objects: :ordered_objects),
         :ok <- validate_ordered_json(ordered),
         true <- Jason.encode!(ordered) == bytes,
         converted when is_map(converted) <- ordered_json_to_term(ordered) do
      {:ok, converted}
    else
      _ -> {:error, :invalid_candidate_json}
    end
  rescue
    _ -> {:error, :invalid_candidate_json}
  end

  @doc false
  @spec local_receipt_bytes_valid?(binary(), binary(), map()) :: boolean()
  def local_receipt_bytes_valid?(payload_bytes, candidate_bytes, marker)
      when is_binary(payload_bytes) and is_binary(candidate_bytes) and is_map(marker) do
    with {:ok, payload} when is_map(payload) <- Jason.decode(payload_bytes),
         true <- ConfirmedRecoveryEvidence.canonical_json(payload) == payload_bytes,
         true <- Enum.sort(Map.keys(payload)) == Enum.sort(@local_receipt_fields),
         true <- payload_bytes == candidate_bytes,
         true <- timestamp?(payload["completedAt"]),
         true <- payload["completedAt"] == marker["completedAt"] do
      true
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  def local_receipt_bytes_valid?(_payload_bytes, _candidate_bytes, _marker), do: false

  @doc false
  @spec decode_provider_response(binary()) :: {:ok, map()} | {:error, :invalid_provider_response_json}
  def decode_provider_response(bytes) when is_binary(bytes) do
    with {:ok, %Jason.OrderedObject{} = ordered} <- Jason.decode(bytes, objects: :ordered_objects),
         :ok <- validate_ordered_json(ordered),
         converted when is_map(converted) <- ordered_json_to_term(ordered),
         data when is_map(data) <- converted["data"] do
      {:ok, %{"data" => data}}
    else
      _ -> {:error, :invalid_provider_response_json}
    end
  rescue
    _ -> {:error, :invalid_provider_response_json}
  end

  def decode_provider_response(_bytes), do: {:error, :invalid_provider_response_json}

  @doc false
  @spec validate_hgs719_receipt_binding(map(), map(), String.t(), map(), String.t(), String.t(), binary()) ::
          :ok | {:error, :provider_confirmation_receipt_mismatch}
  def validate_hgs719_receipt_binding(receipt, confirmed, recovery_id, expected, old_tuple_digest, fence_revision, proof_bytes)
      when is_map(receipt) and is_map(confirmed) and is_map(expected) and is_binary(proof_bytes) do
    with true <- exact_keys?(confirmed, @provider_receipt_fields),
         true <- confirmed == receipt,
         true <- receipt["recoveryId"] == recovery_id,
         true <- receipt["projectionId"] == expected["projectionId"],
         true <- receipt["oldTupleDigest"] == old_tuple_digest,
         true <- receipt["fenceRevision"] == fence_revision,
         true <- receipt["proofDigest"] == digest(proof_bytes) do
      :ok
    else
      _ -> {:error, :provider_confirmation_receipt_mismatch}
    end
  end

  def validate_hgs719_receipt_binding(_receipt, _confirmed, _recovery_id, _expected, _old_tuple_digest, _fence_revision, _proof_bytes),
    do: {:error, :provider_confirmation_receipt_mismatch}

  defp apply_locked(issue_id, pool, nonce, runtime) do
    marker_path = marker_path(issue_id)

    with :ok <- ManagedLauncherLock.require_service_stopped(pool),
         :ok <- require_services_quiescent(),
         :ok <- require_paused_gate(),
         {:ok, marker} <- load_or_create_marker(issue_id, pool, nonce, runtime, marker_path),
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         {:ok, applied_marker} <- apply_marker(marker, marker_path, runtime),
         :ok <- require_paused_gate(),
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         :ok <- ManagedLauncherLock.require_service_stopped(pool),
         :ok <- publish_local_candidate(applied_marker, marker_path) do
      {:ok, if(marker["status"] == "local_applied", do: :already_applied, else: :applied)}
    end
  end

  defp complete_locked(issue_id, pool, runtime) do
    marker_path = marker_path(issue_id)

    with :ok <- ManagedLauncherLock.require_service_stopped(pool),
         :ok <- require_services_quiescent(),
         :ok <- require_paused_gate(),
         {:ok, marker_bytes} <- read_trusted_evidence(marker_path),
         {:ok, marker} when is_map(marker) <- Jason.decode(marker_bytes),
         :ok <- validate_marker_identity(marker, issue_id, pool, marker["nonce"]),
         true <- marker["status"] == "local_applied",
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         :ok <- freeze_state_directories(marker, runtime),
         :ok <- verify_signed_local_receipt(marker, issue_id),
         :ok <- verify_postimages(marker, runtime),
         {:ok, provider_proof, provider_payload} <- verify_provider_final_proof(marker, runtime),
         {:ok, candidate} <- read_candidate(issue_id, marker),
         {:ok, k8s_readback} <-
           ConfirmedRecoveryKubernetes.observe(
             Map.put(marker["expected"], "assignmentSHA256", marker["assignmentSHA256"]),
             candidate["kubernetes"]["cluster"]
           ),
         :ok <- final_local_release_invariants(marker, runtime, provider_payload),
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         :ok <- ManagedLauncherLock.require_service_stopped(pool),
         :ok <- complete_marker(marker, marker_path, provider_proof, provider_payload, k8s_readback, runtime) do
      {:ok, :complete}
    else
      _ -> {:error, :hgs740_completion_held_closed}
    end
  end

  defp read_candidate(issue_id, marker) do
    directory = marker_directory(issue_id)

    with {:ok, observation_bytes} <- read_trusted_evidence(Path.join(directory, "candidate.json")),
         {:ok, proof_bytes} <- read_trusted_evidence(Path.join(directory, "confirmed-root-envelope.json")),
         true <- digest(observation_bytes) == marker["observationSHA256"],
         true <- digest(proof_bytes) == marker["proofSHA256"],
         {:ok, observation} when is_map(observation) <- decode_candidate_bytes(observation_bytes),
         true <- "sha256:" <> digest(observation_bytes) == marker["evidenceRef"] do
      {:ok, observation}
    else
      _ -> {:error, :confirmed_recovery_candidate_changed}
    end
  end

  defp verify_provider_final_proof(marker, runtime, require_current_journal? \\ true) do
    path = Path.join(@provider_receipt_root, marker["issueId"] <> ".json")

    with {:ok, bytes} <- read_root_file(path, 1_048_576),
         {:ok, envelope} when is_map(envelope) <- Jason.decode(bytes),
         true <- Enum.sort(Map.keys(envelope)) == ["payload", "signature"],
         {:ok, payload_bytes} <- Base.url_decode64(envelope["payload"], padding: false),
         {:ok, signature} <- Base.url_decode64(envelope["signature"], padding: false),
         {:ok, public_key} <- read_public_key(),
         true <- Jason.encode!(envelope) == bytes,
         true <- :crypto.verify(:eddsa, :none, payload_bytes, signature, [public_key, :ed25519]),
         {:ok, payload} when is_map(payload) <- Jason.decode(payload_bytes),
         true <- canonical_provider_final_payload(payload) == payload_bytes,
         :ok <- validate_provider_final_payload(payload, marker, runtime, require_current_journal?),
         :ok <- verify_hgs719_operation(marker, payload, bytes, public_key) do
      {:ok, bytes, payload}
    else
      _ -> {:error, :provider_final_proof_invalid}
    end
  rescue
    _ -> {:error, :provider_final_proof_invalid}
  end

  defp validate_provider_final_payload(payload, marker, runtime, require_current_journal?) do
    with {:ok, journal_sha256} <- provider_journal_sha256(marker, runtime, require_current_journal?),
         :ok <- validate_provider_final_payload_shape(payload),
         :ok <- validate_provider_final_claim(payload, marker, journal_sha256),
         :ok <- validate_provider_release_receipt(payload["receipt"], marker) do
      :ok
    else
      _ -> {:error, :provider_final_proof_invalid}
    end
  end

  defp provider_journal_sha256(marker, _runtime, false),
    do: {:ok, marker["postimages"]["claimJournal"]["sha256"]}

  defp provider_journal_sha256(marker, runtime, true) do
    read_state_file(runtime.journal_path, marker["stateOwnership"]["claimJournal"]["uid"])
    |> case do
      {:ok, bytes} -> {:ok, digest(bytes)}
      _ -> {:error, :provider_journal_unavailable}
    end
  end

  defp validate_provider_final_payload_shape(payload) do
    if Enum.sort(Map.keys(payload)) == Enum.sort(~w(contractVersion expected receipt localGenerationMax journalSHA256 neverSpawned)) and
         payload["contractVersion"] == "work-package-pre-spawn-recovery.v1" do
      :ok
    else
      {:error, :provider_final_proof_invalid}
    end
  end

  defp validate_provider_final_claim(payload, marker, journal_sha256) do
    if payload["expected"] == marker["expected"] and payload["localGenerationMax"] == 2 and
         payload["neverSpawned"] == true and digest?(journal_sha256) and payload["journalSHA256"] == journal_sha256 do
      :ok
    else
      {:error, :provider_final_proof_invalid}
    end
  end

  defp validate_provider_release_receipt(receipt, marker) when is_map(receipt) do
    expected = marker["expected"]

    with true <- Enum.sort(Map.keys(receipt)) == Enum.sort(@provider_receipt_fields),
         true <- receipt["oldTupleDigest"] == ConfirmedRecoveryEvidence.tuple_digest(expected),
         true <- receipt["oldNonceHash"] == expected["nonceHash"],
         true <- receipt["projectionId"] == expected["projectionId"],
         true <- receipt["projectionState"] == "queued",
         true <- Enum.all?(~w(reservationState executionCapacityState scopeState), &(receipt[&1] == "released")),
         true <- receipt["nextGenerationFloor"] == 3,
         true <- Enum.all?(~w(recoveryId fenceRevision), &text?(receipt[&1])),
         true <- digest?(receipt["proofDigest"]),
         true <- timestamp?(receipt["confirmedAt"]) do
      :ok
    else
      _ -> {:error, :provider_final_proof_invalid}
    end
  end

  defp validate_provider_release_receipt(_receipt, _marker),
    do: {:error, :provider_final_proof_invalid}

  defp verify_hgs719_operation(marker, payload, final_envelope_bytes, public_key) do
    expected = marker["expected"]
    old_tuple_digest = ConfirmedRecoveryEvidence.tuple_digest(expected)
    recovery_id = "hgs719-#{marker["pool"]}-#{old_tuple_digest}"

    directory =
      Path.join([
        "/srv/dahlia-runner-state",
        "evidence",
        "claim-recovery-hgs719",
        marker["pool"],
        recovery_id
      ])

    with :ok <- exact_private_operation_directory(directory),
         {:ok, files} <- read_hgs719_operation_files(directory),
         :ok <- validate_hgs719_observations(files, expected, payload),
         :ok <- validate_hgs719_prepare(files, recovery_id, expected, old_tuple_digest),
         {:ok, proof_bytes} <- canonical_hgs719_proof(files.confirmation["proof"]),
         :ok <-
           validate_hgs719_confirmation(
             files,
             recovery_id,
             expected,
             old_tuple_digest,
             proof_bytes,
             public_key,
             marker
           ),
         :ok <-
           validate_hgs719_receipt(
             files,
             payload["receipt"],
             recovery_id,
             expected,
             old_tuple_digest,
             proof_bytes
           ),
         true <- files.signed_envelope_bytes == final_envelope_bytes do
      :ok
    else
      _ -> {:error, :provider_final_operation_mismatch}
    end
  rescue
    _ -> {:error, :provider_final_operation_mismatch}
  end

  defp read_hgs719_operation_files(directory) do
    names = %{
      prepare_observation: "prepare-observation.json",
      prepare_request: "prepare-request.json",
      prepare_response: "prepare-response.json",
      confirm_observation: "confirm-observation.json",
      confirmation: "confirm-request.json",
      confirm_response: "confirm-response.json",
      signed_envelope: "signed-envelope.json"
    }

    Enum.reduce_while(names, {:ok, %{}}, fn {key, name}, {:ok, files} ->
      with {:ok, bytes} <- read_private_operation_file(Path.join(directory, name)),
           {:ok, value} <- decode_operation_file(key, bytes) do
        {:cont, {:ok, Map.put(files, key, value) |> Map.put(:"#{key}_bytes", bytes)}}
      else
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, files} -> {:ok, files}
      error -> error
    end
  end

  defp decode_operation_file(key, bytes) when key in [:prepare_response, :confirm_response],
    do: decode_provider_response(bytes)

  defp decode_operation_file(_key, bytes), do: decode_candidate_bytes(bytes)

  defp validate_hgs719_observations(files, expected, payload) do
    with true <- files.prepare_observation["expected"] == expected,
         true <- files.confirm_observation["expected"] == expected,
         true <- files.prepare_observation["localGenerationMax"] == 2,
         true <- files.confirm_observation["localGenerationMax"] == 2,
         true <- files.confirm_observation["claimJournalSHA256"] == payload["journalSHA256"],
         true <- timestamp?(files.prepare_observation["observedAt"]),
         true <- timestamp?(files.confirm_observation["observedAt"]) do
      :ok
    else
      _ -> {:error, :provider_operation_observation_mismatch}
    end
  end

  defp validate_hgs719_prepare(files, recovery_id, expected, old_tuple_digest) do
    request = files.prepare_request
    response = files.prepare_response["data"]

    with true <- exact_keys?(request, ~w(recoveryId expected reason evidenceRef)),
         true <- request["recoveryId"] == recovery_id and request["expected"] == expected,
         true <- request["evidenceRef"] == "sha256:" <> digest(files.prepare_observation_bytes),
         true <-
           exact_keys?(
             response,
             ~w(recoveryId projectionId fenceRevision oldTupleDigest preparedAt state reservationState executionCapacityState scopeState)
           ),
         true <- response["recoveryId"] == recovery_id and response["projectionId"] == expected["projectionId"],
         true <- response["oldTupleDigest"] == old_tuple_digest,
         true <- response["state"] == "prepared" and response["reservationState"] == "claimed",
         true <- response["executionCapacityState"] == "held" and response["scopeState"] == "held",
         true <- text?(response["fenceRevision"]) and timestamp?(response["preparedAt"]) do
      :ok
    else
      _ -> {:error, :provider_prepare_mismatch}
    end
  end

  defp canonical_hgs719_proof(proof) when is_map(proof) do
    with :ok <- exact_keys_result(proof, @provider_proof_fields),
         {:ok, bytes} <- encode_ordered_object(proof, @provider_proof_fields) do
      {:ok, bytes}
    else
      _ -> {:error, :provider_proof_invalid}
    end
  end

  defp canonical_hgs719_proof(_proof), do: {:error, :provider_proof_invalid}

  defp validate_hgs719_confirmation(files, recovery_id, expected, old_tuple_digest, proof_bytes, public_key, marker) do
    confirmation = files.confirmation
    proof = confirmation["proof"]
    prepared = files.prepare_response["data"]

    with true <- exact_keys?(confirmation, ~w(recoveryId proof signature)),
         true <- confirmation["recoveryId"] == recovery_id,
         true <- proof["recoveryId"] == recovery_id and proof["projectionId"] == expected["projectionId"],
         true <- proof["fenceRevision"] == prepared["fenceRevision"],
         true <- proof["oldTupleDigest"] == old_tuple_digest and proof["runnerId"] == expected["runnerId"],
         true <- proof["localGenerationMax"] == 2,
         true <- proof["fenceSHA256"] == marker["postimages"]["fence"]["sha256"],
         true <- proof["claimJournalSHA256"] == marker["postimages"]["claimJournal"]["sha256"],
         true <- proof["globalPause"] == true and proof["runnerStopped"] == true and proof["neverSpawned"] == true,
         true <- proof["supervisedWorkerAbsent"] == true and proof["workspaceAbsent"] == true and proof["processCount"] == 0,
         true <- proof["evidenceRef"] == "sha256:" <> digest(files.confirm_observation_bytes),
         {:ok, signature} <- Base.url_decode64(confirmation["signature"], padding: false),
         true <- byte_size(signature) == 64,
         true <- :crypto.verify(:eddsa, :none, proof_bytes, signature, [public_key, :ed25519]) do
      :ok
    else
      _ -> {:error, :provider_confirmation_mismatch}
    end
  end

  defp validate_hgs719_receipt(files, receipt, recovery_id, expected, old_tuple_digest, proof_bytes) do
    confirmed = files.confirm_response["data"]

    validate_hgs719_receipt_binding(
      receipt,
      confirmed,
      recovery_id,
      expected,
      old_tuple_digest,
      files.prepare_response["data"]["fenceRevision"],
      proof_bytes
    )
  end

  defp exact_keys?(value, keys) when is_map(value), do: Enum.sort(Map.keys(value)) == Enum.sort(keys)
  defp exact_keys?(_value, _keys), do: false

  defp exact_keys_result(value, keys) do
    if exact_keys?(value, keys), do: :ok, else: {:error, :invalid_exact_keyset}
  end

  defp exact_private_operation_directory(path) do
    root = Path.join(["/srv/dahlia-runner-state", "evidence", "claim-recovery-hgs719"])

    with true <- String.starts_with?(path, root <> "/"),
         :ok <- trusted_root_directory(Path.dirname(root)),
         :ok <- exact_private_directories(path, Path.dirname(root)) do
      :ok
    else
      _ -> {:error, :untrusted_provider_operation_directory}
    end
  end

  defp exact_private_directories(path, base) do
    relative = Path.relative_to(path, base)

    Enum.reduce_while(Path.split(relative), base, fn part, parent ->
      current = Path.join(parent, part)

      case File.lstat(current) do
        {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when band(mode, 0o777) == 0o700 ->
          {:cont, current}

        _ ->
          {:halt, {:error, :untrusted_provider_operation_directory}}
      end
    end)
    |> case do
      ^path -> :ok
      _ -> {:error, :untrusted_provider_operation_directory}
    end
  end

  defp read_private_operation_file(path) do
    with {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1, size: size}} <- File.lstat(path),
         true <- band(mode, 0o777) == 0o600 and size in 1..262_144,
         {:ok, bytes} <- File.read(path),
         true <- byte_size(bytes) == size do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_provider_operation_file}
    end
  end

  defp canonical_provider_final_payload(payload) do
    with {:ok, expected} <- encode_ordered_object(payload["expected"], @provider_claim_fields),
         {:ok, receipt} <- encode_ordered_object(payload["receipt"], @provider_receipt_fields),
         fields = [
           {"contractVersion", payload["contractVersion"]},
           {"expected", {:raw, expected}},
           {"receipt", {:raw, receipt}},
           {"localGenerationMax", payload["localGenerationMax"]},
           {"journalSHA256", payload["journalSHA256"]},
           {"neverSpawned", payload["neverSpawned"]}
         ],
         {:ok, bytes} <- encode_ordered_fields(fields) do
      bytes
    else
      _ -> nil
    end
  end

  defp encode_ordered_object(value, fields) when is_map(value) do
    if Enum.sort(Map.keys(value)) == Enum.sort(fields) do
      fields
      |> Enum.map(fn key -> {key, value[key]} end)
      |> encode_ordered_fields()
    else
      {:error, :invalid_ordered_object}
    end
  end

  defp encode_ordered_object(_value, _fields), do: {:error, :invalid_ordered_object}

  defp encode_ordered_fields(fields) do
    fields
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, parts} ->
      encoded =
        case value do
          {:raw, raw_json} when is_binary(raw_json) -> {:ok, raw_json}
          other -> Jason.encode(other)
        end

      case encoded do
        {:ok, json} -> {:cont, {:ok, [parts, Jason.encode!(key), ":", json, ","]}}
        _ -> {:halt, {:error, :invalid_ordered_value}}
      end
    end)
    |> case do
      {:ok, parts} -> {:ok, ["{", String.trim_trailing(IO.iodata_to_binary(parts), ","), "}"] |> IO.iodata_to_binary()}
      error -> error
    end
  end

  defp final_local_release_invariants(marker, runtime, provider_payload) do
    with true <- provider_payload["journalSHA256"] == marker["postimages"]["claimJournal"]["sha256"],
         :ok <- verify_postimages(marker, runtime),
         :ok <- release_state_invariants(marker, runtime) do
      :ok
    else
      _ -> {:error, :local_release_invariants_changed}
    end
  end

  defp release_state_invariants(marker, runtime) do
    with {:ok, paths} <- read_state_preimages(runtime),
         expected = marker["expected"],
         issue_id = marker["issueId"],
         key = Journal.reservation_key(issue_id, expected["managedProjectProfileId"], expected["repositoryRef"], 2),
         reservation when is_map(reservation) <- paths.journal.state.reservations[key],
         %{dispatch: %{phase: "recovery_pending", allocation_id: nil}} <- reservation,
         true <- current_claim(paths.journal.state, expected) == expected,
         :ok <- released_fence_lease(paths.fence.state, issue_id, expected),
         :ok <- released_graph_lease(paths.graph.state, expected) do
      :ok
    else
      _ -> {:error, :local_release_invariants_changed}
    end
  end

  defp released_fence_lease(fence, issue_id, expected) do
    case get_in(fence, [:executions, issue_id, :leases, expected["sessionId"]]) do
      %{process_id: process_id, status: :released, release_reason: :spawn_failed, termination_required: false} ->
        if process_id == expected["processId"], do: :ok, else: {:error, :execution_lease_not_released}

      _ ->
        {:error, :execution_lease_not_released}
    end
  end

  defp released_graph_lease(graph, expected) do
    case Map.get(graph.delegations, expected["responsibleDelegationId"]) do
      %{status: :active, runtime_lease: nil} -> :ok
      _ -> {:error, :responsibility_lease_not_released}
    end
  end

  defp complete_marker(marker, path, provider_proof, provider_payload, k8s_readback, runtime) do
    with {:ok, completion_postimages} <- state_hashes(runtime, marker["stateOwnership"]),
         completed =
           marker
           |> Map.put("status", "complete")
           |> Map.put("providerFinalProofSHA256", digest(provider_proof))
           |> Map.put("providerJournalSHA256", provider_payload["journalSHA256"])
           |> Map.put("completionObservation", k8s_readback)
           |> Map.put("completionPostimages", completion_postimages)
           |> Map.put("providerReceipt", provider_payload["receipt"])
           |> Map.put("completedAt", DateTime.utc_now() |> DateTime.to_iso8601()),
         true <- completed["completionPostimages"]["claimJournalSHA256"] == marker["postimages"]["claimJournal"]["sha256"],
         true <- completed["completionPostimages"]["fenceSHA256"] == marker["postimages"]["fence"]["sha256"],
         true <- completed["completionPostimages"]["responsibilityGraphSHA256"] == marker["postimages"]["responsibilityGraph"]["sha256"],
         :ok <- restore_state_directories(marker, runtime),
         :ok <- validate_state_directories(marker, runtime, :original),
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         :ok <- release_state_invariants(marker, runtime),
         :ok <- durable_replace(path, Jason.encode!(completed)) do
      :ok
    else
      _ -> {:error, :hgs740_completion_held_closed}
    end
  end

  defp state_hashes(runtime, ownership) do
    with {:ok, journal} <- read_state_file(runtime.journal_path, ownership["claimJournal"]["uid"]),
         {:ok, fence} <- read_state_file(runtime.execution_fence_path, ownership["fence"]["uid"]),
         {:ok, graph} <- read_state_file(runtime.responsibility_graph_path, ownership["responsibilityGraph"]["uid"]) do
      {:ok,
       %{
         "claimJournalSHA256" => digest(journal),
         "fenceSHA256" => digest(fence),
         "responsibilityGraphSHA256" => digest(graph)
       }}
    else
      _ -> {:error, :state_hash_read_failed}
    end
  end

  defp load_or_create_marker(issue_id, pool, nonce, runtime, marker_path) do
    case read_trusted_evidence(marker_path) do
      {:ok, bytes} ->
        with {:ok, marker} <- Jason.decode(bytes),
             :ok <- validate_marker_identity(marker, issue_id, pool, nonce),
             :ok <- marker_postimages_valid(marker),
             :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
             :ok <- freeze_state_directories(marker, runtime),
             :ok <- validate_resumable_marker(marker, runtime) do
          {:ok, marker}
        else
          _ -> {:error, :existing_hgs740_marker_conflict}
        end

      {:error, :enoent} ->
        create_marker(issue_id, pool, nonce, runtime, marker_path)

      {:error, _reason} = error ->
        error
    end
  end

  defp create_marker(issue_id, pool, nonce, runtime, marker_path) do
    with :ok <- trusted_runtime_files(runtime, pool),
         {:ok, ownership} <- state_ownership(runtime_state_paths(runtime)),
         :ok <- require_mutation_quiescent(runtime, ownership["claimJournal"]["uid"]),
         {:ok, paths} <- read_state_preimages(runtime),
         :ok <- require_mutation_quiescent(runtime, paths.ownership["claimJournal"]["uid"]),
         {:ok, observation, observation_bytes, proof_bytes, proof_payload, proof_now_ms} <-
           verify_signed_proof(issue_id, pool, nonce, paths),
         :ok <- verify_local_claim(paths, proof_payload, runtime),
         {:ok, postimages} <- prepare_postimages(paths, proof_payload, runtime, proof_now_ms),
         marker =
           build_marker(%{
             issue_id: issue_id,
             pool: pool,
             nonce: nonce,
             proof_bytes: proof_bytes,
             observation: observation,
             observation_bytes: observation_bytes,
             paths: paths,
             postimages: postimages,
             proof_payload: proof_payload,
             proof_now_ms: proof_now_ms
           }),
         :ok <- ensure_evidence_directory(issue_id),
         :ok <- durable_create(marker_path, Jason.encode!(marker)) do
      {:ok, marker}
    end
  end

  defp validate_resumable_marker(%{"status" => "applying"} = marker, runtime) do
    with :ok <- validate_current_pre_or_post(marker, runtime),
         {:ok, paths} <- paths_from_marker_preimages(marker, runtime),
         {:ok, observation, observation_bytes, proof_bytes, proof_payload} <- verify_saved_proof(marker, paths),
         :ok <- verify_local_claim(paths, proof_payload, runtime),
         {:ok, postimages} <- prepare_postimages(paths, proof_payload, runtime, marker["verificationNowMs"]),
         true <- postimages_match_marker?(postimages, marker),
         {:ok, _snapshot} <-
           ConfirmedRecoveryKubernetes.observe(
             Map.put(observation["expected"], "assignmentSHA256", proof_payload["assignmentSHA256"]),
             observation["kubernetes"]["cluster"]
           ),
         true <- digest(observation_bytes) == marker["observationSHA256"],
         true <- digest(proof_bytes) == marker["proofSHA256"] do
      :ok
    else
      _ -> {:error, :saved_hgs740_evidence_changed}
    end
  end

  defp validate_resumable_marker(%{"status" => "local_applied"} = marker, runtime) do
    with :ok <- verify_postimages(marker, runtime),
         {:ok, _observation} <- read_candidate(marker["issueId"], marker) do
      :ok
    else
      _ -> {:error, :saved_hgs740_postimage_changed}
    end
  end

  defp validate_resumable_marker(_marker, _runtime), do: {:error, :existing_hgs740_marker_conflict}

  defp paths_from_marker_preimages(marker, runtime) do
    with {:ok, journal_bytes} <- decode_marker_image(marker, "claimJournal"),
         {:ok, fence_bytes} <- decode_marker_image(marker, "fence"),
         {:ok, graph_bytes} <- decode_marker_image(marker, "responsibilityGraph"),
         {:ok, journal} <- Journal.decode_bytes(journal_bytes),
         {:ok, fence} <- FencePersistence.decode_bytes(fence_bytes),
         {:ok, graph} <- GraphPersistence.decode_bytes(graph_bytes) do
      {:ok,
       %{
         journal: %{path: runtime.journal_path, bytes: journal_bytes, state: journal},
         fence: %{path: runtime.execution_fence_path, bytes: fence_bytes, state: fence},
         graph: %{path: runtime.responsibility_graph_path, bytes: graph_bytes, state: graph}
       }}
    else
      _ -> {:error, :invalid_hgs740_preimage_image}
    end
  end

  defp decode_marker_image(marker, name) do
    with encoded when is_binary(encoded) <- marker["preimageImages"][name],
         {:ok, bytes} <- Base.url_decode64(encoded, padding: false),
         true <- digest(bytes) == marker_preimage_for(marker, name) do
      {:ok, bytes}
    else
      _ -> {:error, :invalid_hgs740_preimage_image}
    end
  end

  defp verify_saved_proof(marker, paths) do
    directory = marker_directory(marker["issueId"])

    with {:ok, observation_bytes} <- read_trusted_evidence(Path.join(directory, "candidate.json")),
         {:ok, proof_bytes} <- read_trusted_evidence(Path.join(directory, "confirmed-root-envelope.json")),
         {:ok, observation} when is_map(observation) <- decode_candidate_bytes(observation_bytes),
         {:ok, public_key} <- read_public_key(),
         {:ok, payload_hint} <- decode_proof_payload(proof_bytes),
         bindings <- proof_bindings(payload_hint, marker["pool"], marker["issueId"], marker["nonce"], paths, marker["verificationNowMs"]),
         {:ok, payload} <- ConfirmedRecoveryEvidence.verify(proof_bytes, public_key, bindings),
         true <- payload["observation"] == observation,
         true <- payload["assignmentSHA256"] == marker["assignmentSHA256"],
         true <- "sha256:" <> digest(observation_bytes) == marker["evidenceRef"] do
      {:ok, observation, observation_bytes, proof_bytes, payload}
    else
      _ -> {:error, :invalid_saved_hgs740_proof}
    end
  end

  defp postimages_match_marker?(postimages, marker) do
    Enum.all?(
      [{"claimJournal", "claimJournal"}, {"fence", "fence"}, {"responsibilityGraph", "responsibilityGraph"}],
      fn {computed_name, marker_name} ->
        image = postimages[computed_name]

        is_map(image) and digest(image.bytes) == marker["postimages"][marker_name]["sha256"] and
          Base.url_encode64(image.bytes, padding: false) == marker["postimages"][marker_name]["bytes"]
      end
    )
  end

  defp verify_signed_proof(issue_id, pool, nonce, paths) do
    directory = marker_directory(issue_id)
    observation_path = Path.join(directory, "candidate.json")
    proof_path = Path.join(directory, "confirmed-root-envelope.json")

    with :ok <- trusted_evidence_directory(directory),
         {:ok, observation_bytes} <- read_trusted_evidence(observation_path),
         {:ok, proof_bytes} <- read_trusted_evidence(proof_path),
         {:ok, observation} when is_map(observation) <- decode_candidate_bytes(observation_bytes),
         {:ok, public_key} <- read_public_key(),
         {:ok, payload_hint} <- decode_proof_payload(proof_bytes),
         now_ms <- System.system_time(:millisecond),
         bindings <- proof_bindings(payload_hint, pool, issue_id, nonce, paths, now_ms),
         {:ok, payload} <- ConfirmedRecoveryEvidence.verify(proof_bytes, public_key, bindings),
         true <- payload["observation"] == observation,
         true <- payload["observation"]["expected"] == observation["expected"] do
      {:ok, observation, observation_bytes, proof_bytes, payload, now_ms}
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp proof_bindings(payload, pool, issue_id, nonce, paths, now_ms) do
    %{
      pool: pool,
      issue_id: issue_id,
      generation: 2,
      reservation_id: payload["reservationId"],
      assignment_sha256: payload["assignmentSHA256"],
      nonce: nonce,
      fence_sha256: digest(paths.fence.bytes),
      claim_journal_sha256: digest(paths.journal.bytes),
      responsibility_graph_sha256: digest(paths.graph.bytes),
      now_ms: now_ms
    }
  end

  defp decode_proof_payload(proof_bytes) do
    with {:ok, envelope} when is_map(envelope) <- Jason.decode(proof_bytes),
         {:ok, payload_bytes} <- Base.url_decode64(envelope["payload"], padding: false),
         {:ok, payload} when is_map(payload) <- Jason.decode(payload_bytes) do
      {:ok, payload}
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp read_public_key do
    pem_path = Path.join(@identity_root, "public.pem")
    metadata_path = Path.join(@identity_root, "public.json")

    with {:ok, pem} <- read_root_file(pem_path, 16_384),
         {:ok, metadata} <- read_root_file(metadata_path, 4_096),
         {:ok, %{"providerFingerprint" => fingerprint}} <- Jason.decode(metadata),
         true <- fingerprint == "903b66d70e23219ee947bdbbdd738b29851302a24985edd4a69abc8a2875d8e6",
         {:ok, public_key} <- decode_public_key(pem),
         true <- public_key_fingerprint(public_key) == fingerprint do
      {:ok, public_key}
    else
      _ -> {:error, :untrusted_recovery_key}
    end
  end

  defp decode_public_key(pem) do
    expected_prefix = <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>>

    with [{:SubjectPublicKeyInfo, der, :not_encrypted}] <- :public_key.pem_decode(pem),
         <<^expected_prefix::binary, public_key::binary-size(32)>> <- der do
      {:ok, public_key}
    else
      _ -> {:error, :untrusted_recovery_key}
    end
  rescue
    _ -> {:error, :untrusted_recovery_key}
  end

  defp public_key_fingerprint(public_key) do
    der = <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>> <> public_key
    Base.encode64(der) |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
  end

  defp read_state_preimages(runtime) do
    with {:ok, journal_bytes} <- read_state_file(runtime.journal_path),
         {:ok, fence_bytes} <- read_state_file(runtime.execution_fence_path),
         {:ok, graph_bytes} <- read_state_file(runtime.responsibility_graph_path),
         {:ok, journal} <- Journal.decode_bytes(journal_bytes),
         {:ok, fence} <- FencePersistence.decode_bytes(fence_bytes),
         {:ok, graph} <- GraphPersistence.decode_bytes(graph_bytes),
         {:ok, ownership} <- state_ownership(runtime_state_paths(runtime)) do
      {:ok,
       %{
         journal: %{path: runtime.journal_path, bytes: journal_bytes, state: journal},
         fence: %{path: runtime.execution_fence_path, bytes: fence_bytes, state: fence},
         graph: %{path: runtime.responsibility_graph_path, bytes: graph_bytes, state: graph},
         ownership: ownership
       }}
    else
      _ -> {:error, :invalid_local_preimage}
    end
  end

  defp state_ownership(paths) do
    names = ["claimJournal", "fence", "responsibilityGraph"]

    with {:ok, files} <- collect_state_file_ownership(Enum.zip(names, paths)),
         owner = files["claimJournal"]["uid"],
         true <- Enum.all?(files, fn {_name, record} -> record["uid"] == owner end),
         {:ok, directories} <- state_directory_identity(paths, owner) do
      {:ok, Map.put(files, "directories", directories)}
    else
      _ -> {:error, :untrusted_state_owner}
    end
  end

  defp collect_state_file_ownership(entries) do
    Enum.reduce_while(entries, {:ok, %{}}, fn {name, path}, {:ok, ownership} ->
      case state_file_owner_record(path) do
        {:ok, record} -> {:cont, {:ok, Map.put(ownership, name, record)}}
        error -> {:halt, error}
      end
    end)
  end

  defp state_file_owner_record(path) do
    with {:ok, %File.Stat{type: :regular, uid: uid, gid: gid, mode: mode, links: 1}} <- File.lstat(path),
         true <- uid > 0 and gid >= 0 and band(mode, 0o077) == 0 do
      {:ok, %{"uid" => uid, "gid" => gid, "mode" => band(mode, 0o777)}}
    else
      _ -> {:error, :untrusted_state_owner}
    end
  end

  defp state_directory_identity(paths, owner) do
    paths
    |> Enum.flat_map(&(Path.dirname(&1) |> directory_ancestors([])))
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, %{}}, fn path, {:ok, identities} ->
      case File.lstat(path, time: :posix) do
        {:ok,
         %File.Stat{
           type: :directory,
           major_device: major,
           minor_device: minor,
           inode: inode,
           uid: uid,
           gid: gid,
           mode: mode
         }}
        when uid in [0, owner] and band(mode, 0o022) == 0 ->
          identity = %{
            "majorDevice" => major,
            "minorDevice" => minor,
            "inode" => inode,
            "uid" => uid,
            "gid" => gid,
            "mode" => band(mode, 0o777)
          }

          {:cont, {:ok, Map.put(identities, path, identity)}}

        _ ->
          {:halt, {:error, :untrusted_state_directory}}
      end
    end)
  end

  defp directory_ancestors(@runtime_state_root, acc), do: [@runtime_state_root | acc]
  defp directory_ancestors("/", _acc), do: []
  defp directory_ancestors(path, acc), do: directory_ancestors(Path.dirname(path), [path | acc])

  defp validate_state_directories(marker, runtime, expected_state \\ :frozen) do
    ownership = marker["stateOwnership"]
    expected = ownership["directories"]
    owner = ownership["claimJournal"]["uid"]

    with true <- is_map(expected),
         {:ok, actual} <- state_directory_identity(runtime_state_paths(runtime), owner),
         true <- state_directories_match?(actual, expected, expected_state) do
      :ok
    else
      _ -> {:error, :state_directory_identity_changed}
    end
  end

  defp state_directories_match?(actual, expected, :original), do: actual == expected

  defp state_directories_match?(actual, expected, :frozen) do
    Enum.all?(expected, fn {path, original} ->
      expected_record =
        if path in @transaction_writable_roots do
          %{original | "uid" => 0, "gid" => 0, "mode" => 0o700}
        else
          original
        end

      actual[path] == expected_record
    end)
  end

  defp state_directories_match?(_actual, _expected, _state), do: false

  defp freeze_state_directories(marker, runtime) do
    ownership = marker["stateOwnership"]
    directories = ownership["directories"]
    owner = ownership["claimJournal"]["uid"]

    with :ok <- validate_freezable_directories(directories),
         :ok <- change_transaction_directories(directories, :freeze),
         :ok <- validate_state_directories(marker, runtime, :frozen),
         :ok <- no_processes_for_uid(owner) do
      :ok
    else
      _ -> {:error, :transaction_state_directory_freeze_failed}
    end
  end

  defp restore_state_directories(marker, runtime) do
    directories = marker["stateOwnership"]["directories"]

    with :ok <- validate_state_directories(marker, runtime, :frozen),
         :ok <- change_transaction_directories(directories, :restore),
         :ok <- validate_state_directories(marker, runtime, :original) do
      :ok
    else
      _ -> {:error, :transaction_state_directory_restore_failed}
    end
  end

  defp validate_freezable_directories(directories) when is_map(directories) do
    if Enum.all?(@transaction_writable_roots, &is_map_key(directories, &1)),
      do: :ok,
      else: {:error, :transaction_state_directory_missing}
  end

  defp validate_freezable_directories(_directories), do: {:error, :transaction_state_directory_missing}

  defp change_transaction_directories(directories, action) do
    Enum.reduce_while(@transaction_writable_roots, :ok, fn path, :ok ->
      original = directories[path]

      case change_transaction_directory(path, original, action) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp change_transaction_directory(path, original, :freeze) do
    with {:ok, current} <- directory_identity(path),
         true <- directory_transition_allowed?(current, original, :freeze),
         :ok <- :file.change_owner(String.to_charlist(path), 0, 0),
         :ok <- File.chmod(path, 0o700),
         {:ok, frozen} <- directory_identity(path),
         true <- frozen == %{original | "uid" => 0, "gid" => 0, "mode" => 0o700} do
      :ok
    else
      _ -> {:error, :transaction_state_directory_changed}
    end
  end

  defp change_transaction_directory(path, original, :restore) do
    with {:ok, current} <- directory_identity(path),
         true <- directory_transition_allowed?(current, original, :restore),
         :ok <- :file.change_owner(String.to_charlist(path), original["uid"], original["gid"]),
         :ok <- File.chmod(path, original["mode"]),
         {:ok, restored} <- directory_identity(path),
         true <- restored == original do
      :ok
    else
      _ -> {:error, :transaction_state_directory_changed}
    end
  end

  defp same_directory_inode?(left, right) do
    left["majorDevice"] == right["majorDevice"] and left["minorDevice"] == right["minorDevice"] and
      left["inode"] == right["inode"]
  end

  defp directory_identity(path) do
    case File.lstat(path, time: :posix) do
      {:ok,
       %File.Stat{
         type: :directory,
         major_device: major,
         minor_device: minor,
         inode: inode,
         uid: uid,
         gid: gid,
         mode: mode
       }} ->
        {:ok,
         %{
           "majorDevice" => major,
           "minorDevice" => minor,
           "inode" => inode,
           "uid" => uid,
           "gid" => gid,
           "mode" => band(mode, 0o777)
         }}

      _ ->
        {:error, :untrusted_state_directory}
    end
  end

  defp restore_state_file_metadata(path, %{"uid" => uid, "gid" => gid, "mode" => mode}) do
    with true <- is_integer(uid) and uid > 0 and is_integer(gid) and gid >= 0,
         true <- is_integer(mode) and band(mode, 0o077) == 0,
         :ok <- :file.change_owner(String.to_charlist(path), uid, gid),
         :ok <- File.chmod(path, mode),
         {:ok, %File.Stat{type: :regular, uid: ^uid, gid: ^gid, mode: actual_mode, links: 1}} <- File.lstat(path),
         true <- band(actual_mode, 0o777) == mode do
      :ok
    else
      _ -> {:error, :state_file_ownership_restore_failed}
    end
  end

  defp restore_state_file_metadata(_path, _ownership), do: {:error, :invalid_state_owner}

  defp verify_local_claim(paths, payload, _runtime) do
    expected = payload["observation"]["expected"]
    claim = current_claim(paths.journal.state, expected)
    issue_id = payload["issueId"]
    key = Journal.reservation_key(issue_id, expected["managedProjectProfileId"], expected["repositoryRef"], 2)
    reservation = paths.journal.state.reservations[key]

    with true <- is_map(reservation),
         true <- claim == expected,
         %{dispatch: %{phase: "confirmed", allocation_id: nil}} <- reservation,
         true <- map_size(Map.get(reservation, :failed_worker_turns, %{})) == 0,
         true <- map_size(Map.get(reservation, :cleanup_receipts, %{})) == 0,
         :ok <- exact_local_predecessor(paths, payload["observation"]["predecessorRetirement"], expected),
         :ok <- exact_active_fence(paths.fence.state, issue_id, expected),
         :ok <- exact_active_runtime_lease(paths.graph.state, expected),
         :ok <- require_no_local_workers(payload["observation"]) do
      :ok
    else
      _ -> {:error, :confirmed_claim_precondition_changed}
    end
  end

  defp current_claim(journal, expected) do
    matches =
      journal.reservations
      |> Map.values()
      |> Enum.filter(fn reservation ->
        reservation.issue_id == expected["issueId"] and reservation.generation == expected["generation"]
      end)

    case matches do
      [reservation] ->
        %{
          "projectionId" => reservation.projection_id,
          "reservationId" => reservation.reservation_id,
          "workspaceId" => Map.get(reservation, :workspace_id),
          "companyId" => Map.get(reservation, :company_id),
          "issueId" => reservation.issue_id,
          "runnerId" => reservation.runner_id,
          "managedProjectProfileId" => reservation.managed_project_profile_id,
          "repositoryRef" => reservation.repository_ref,
          "scopeKeys" => Enum.sort(reservation.scope_keys),
          "generation" => reservation.generation,
          "sessionId" => reservation.session_id,
          "processId" => reservation.process_id,
          "responsibleDelegationId" => reservation.responsible_delegation_id,
          "executionFenceToken" => reservation.execution_fence_token,
          "runtimeLeaseId" => reservation.runtime_lease_id,
          "nonceHash" => digest(reservation.reservation_nonce)
        }

      _ ->
        nil
    end
  end

  defp exact_local_predecessor(
         paths,
         %{"execution" => expected_execution, "claim" => expected_claim, "receipt" => receipt},
         gen2_claim
       ) do
    with true <- current_claim(paths.journal.state, expected_claim) == expected_claim,
         {:ok, encoded_fence} <- FencePersistence.encode_bytes(paths.fence.state),
         {:ok, fence_document} <- Jason.decode(encoded_fence),
         history when is_list(history) <- fence_document["history"],
         true <- Enum.any?(history, &(&1 == expected_execution)),
         true <- expected_execution["retirement"] == receipt,
         true <- receipt["prior_responsible_id"] == expected_claim["responsibleDelegationId"],
         true <- receipt["successor_responsible_id"] == gen2_claim["responsibleDelegationId"],
         %{status: :revoked, runtime_lease: nil} <-
           Map.get(paths.graph.state.delegations, receipt["prior_responsible_id"]) do
      :ok
    else
      _ -> {:error, :predecessor_retirement_not_persisted}
    end
  end

  defp exact_local_predecessor(_paths, _predecessor, _current_claim),
    do: {:error, :predecessor_retirement_not_persisted}

  defp exact_active_fence(fence, issue_id, expected) do
    case Map.get(fence.executions, issue_id) do
      %{
        generation: 2,
        status: :active,
        ownership: :reconciled,
        cleanup: :pending,
        terminal: nil,
        cleanup_receipt: nil,
        retirement: nil,
        termination_unconfirmed: false,
        leases: leases
      }
      when is_map(leases) ->
        lease = Map.get(leases, expected["sessionId"])

        if is_map(lease) and lease.process_id == expected["processId"] and lease.status == :active and
             lease.termination_required == false do
          :ok
        else
          {:error, :execution_lease_mismatch}
        end

      _ ->
        {:error, :execution_fence_mismatch}
    end
  end

  defp exact_active_runtime_lease(graph, expected) do
    case get_in(graph, [:delegations, expected["responsibleDelegationId"]]) do
      %{runtime_lease: lease, status: :active} when is_map(lease) ->
        with true <- lease.issue_id == expected["issueId"],
             true <- lease.generation == 2,
             true <- lease.session_id == expected["sessionId"],
             true <- lease.process_id == expected["processId"],
             true <- lease.repository == expected["repositoryRef"] do
          :ok
        else
          _ -> {:error, :runtime_lease_mismatch}
        end

      _ ->
        {:error, :runtime_lease_mismatch}
    end
  end

  defp require_no_local_workers(observation) do
    with true <- observation["globalPause"] == true,
         true <- observation["runnerStopped"] == true,
         true <- observation["neverSpawned"] == true,
         true <- observation["supervisedWorkerAbsent"] == true,
         true <- observation["processCount"] == 0,
         true <- observation["workspaceAbsent"] == true,
         true <- observation["turnsAbsent"] == true,
         true <- observation["dispatchPhase"] == "confirmed" do
      :ok
    else
      _ -> {:error, :worker_quiescence_not_proven}
    end
  end

  defp prepare_postimages(paths, payload, _runtime, now_ms) do
    expected = payload["observation"]["expected"]
    reservation_id = payload["reservationId"]
    issue_id = payload["issueId"]
    key = Journal.reservation_key(issue_id, expected["managedProjectProfileId"], expected["repositoryRef"], 2)

    with {:ok, next_journal} <- Dispatch.begin_confirmed_recovery(paths.journal.state, key),
         {:ok, next_fence, :released} <-
           ExecutionFence.release(paths.fence.state, %{issue_id: issue_id, generation: 2}, expected["sessionId"], :spawn_failed),
         runtime_lease <- %{
           issue_id: issue_id,
           repository: expected["repositoryRef"],
           generation: 2,
           session_id: expected["sessionId"],
           process_id: expected["processId"]
         },
         {:ok, next_graph, :released} <-
           ResponsibilityGraph.release_runtime_lease(paths.graph.state, expected["responsibleDelegationId"], runtime_lease, now_ms),
         {:ok, journal_bytes} <- Journal.encode_bytes(next_journal),
         {:ok, fence_bytes} <- FencePersistence.encode_bytes(next_fence),
         {:ok, graph_bytes} <- GraphPersistence.encode_bytes(next_graph) do
      {:ok,
       %{
         "claimJournal" => %{bytes: journal_bytes, path: paths.journal.path},
         "fence" => %{bytes: fence_bytes, path: paths.fence.path},
         "responsibilityGraph" => %{bytes: graph_bytes, path: paths.graph.path},
         "reservationId" => reservation_id
       }}
    else
      _ -> {:error, :confirmed_claim_transition_rejected}
    end
  end

  defp build_marker(context) do
    %{
      issue_id: issue_id,
      pool: pool,
      nonce: nonce,
      proof_bytes: proof_bytes,
      observation: observation,
      observation_bytes: observation_bytes,
      paths: paths,
      postimages: postimages,
      proof_payload: proof_payload,
      proof_now_ms: now_ms
    } = context

    %{
      "contractVersion" => @marker_version,
      "issueId" => issue_id,
      "pool" => pool,
      "generation" => 2,
      "reservationId" => postimages["reservationId"],
      "expected" => observation["expected"],
      "assignmentSHA256" => proof_payload["assignmentSHA256"],
      "nonce" => nonce,
      "status" => "applying",
      "verificationNowMs" => now_ms,
      "proofSHA256" => digest(proof_bytes),
      "evidenceRef" => "sha256:" <> digest(observation_bytes),
      "observationSHA256" => digest(observation_bytes),
      "preimages" => %{
        "claimJournalSHA256" => digest(paths.journal.bytes),
        "fenceSHA256" => digest(paths.fence.bytes),
        "responsibilityGraphSHA256" => digest(paths.graph.bytes)
      },
      "preimageImages" => %{
        "claimJournal" => Base.url_encode64(paths.journal.bytes, padding: false),
        "fence" => Base.url_encode64(paths.fence.bytes, padding: false),
        "responsibilityGraph" => Base.url_encode64(paths.graph.bytes, padding: false)
      },
      "stateOwnership" => paths.ownership,
      "postimages" =>
        postimages
        |> Map.drop(["reservationId"])
        |> Map.new(fn
          {name, %{bytes: bytes}} -> {name, %{"sha256" => digest(bytes), "bytes" => Base.url_encode64(bytes, padding: false)}}
        end)
    }
  end

  defp apply_marker(marker, marker_path, runtime) do
    with true <- marker["status"] in ["applying", "local_applied"],
         :ok <- freeze_state_directories(marker, runtime),
         :ok <- validate_current_pre_or_post(marker, runtime),
         :ok <-
           ConfirmedRecoveryWAL.replay(@state_names, fn
             "claimJournal" ->
               apply_state_image(marker, "claimJournal", runtime.journal_path, runtime, &Journal.decode_bytes/1, &Journal.save/2)

             "fence" ->
               apply_state_image(marker, "fence", runtime.execution_fence_path, runtime, &FencePersistence.decode_bytes/1, &FencePersistence.save/2)

             "responsibilityGraph" ->
               apply_state_image(marker, "responsibilityGraph", runtime.responsibility_graph_path, runtime, &GraphPersistence.decode_bytes/1, &GraphPersistence.save/2)
           end),
         :ok <- verify_postimages(marker, runtime),
         {:ok, applied_marker} <- set_marker_applied(marker, marker_path) do
      {:ok, applied_marker}
    else
      _ -> {:error, :hgs740_transaction_incomplete}
    end
  end

  defp validate_current_pre_or_post(marker, runtime) do
    current = [
      {"claimJournal", runtime.journal_path, "claimJournalSHA256"},
      {"fence", runtime.execution_fence_path, "fenceSHA256"},
      {"responsibilityGraph", runtime.responsibility_graph_path, "responsibilityGraphSHA256"}
    ]

    if Enum.all?(current, &current_image_valid?(&1, marker)) and validate_state_directories(marker, runtime) == :ok do
      :ok
    else
      {:error, :transaction_preimage_changed}
    end
  end

  defp current_image_valid?({name, path, preimage_key}, marker) do
    case read_state_file(path, marker["stateOwnership"][name]["uid"]) do
      {:ok, bytes} ->
        current_sha = digest(bytes)
        preimage_sha = marker["preimages"][preimage_key]
        postimage_sha = marker["postimages"][name]["sha256"]
        current_sha == preimage_sha or current_sha == postimage_sha

      _ ->
        false
    end
  end

  defp apply_state_image(marker, name, path, runtime, decode, save) do
    ownership = marker["stateOwnership"][name]

    with {:ok, bytes} <- Base.url_decode64(marker["postimages"][name]["bytes"], padding: false),
         true <- digest(bytes) == marker["postimages"][name]["sha256"],
         {:ok, state} <- decode.(bytes),
         {:ok, current} <- read_state_file(path, ownership["uid"]) do
      apply_image(current, marker_preimage_for(marker, name), bytes, fn _postimage ->
        persist_state_image(current == bytes, path, state, ownership, marker, runtime, save)
      end)
    else
      _ -> {:error, :invalid_transaction_postimage}
    end
  end

  defp persist_state_image(already_applied, path, state, ownership, marker, runtime, save) do
    uid = ownership["uid"]

    with :ok <- require_mutation_quiescent(runtime, uid),
         :ok <- validate_state_directories(marker, runtime),
         :ok <- if(already_applied, do: :ok, else: save.(path, state)),
         :ok <- restore_state_file_metadata(path, ownership),
         :ok <- fsync_directory(Path.dirname(path)),
         :ok <- validate_state_directories(marker, runtime) do
      require_mutation_quiescent(runtime, uid)
    end
  end

  defp marker_preimage_for(marker, "claimJournal"), do: marker["preimages"]["claimJournalSHA256"]
  defp marker_preimage_for(marker, "fence"), do: marker["preimages"]["fenceSHA256"]
  defp marker_preimage_for(marker, "responsibilityGraph"), do: marker["preimages"]["responsibilityGraphSHA256"]

  defp verify_postimages(marker, runtime) do
    paths = %{
      "claimJournal" => runtime.journal_path,
      "fence" => runtime.execution_fence_path,
      "responsibilityGraph" => runtime.responsibility_graph_path
    }

    if validate_state_directories(marker, runtime) == :ok and Enum.all?(paths, &postimage_matches?(&1, marker)) do
      :ok
    else
      {:error, :hgs740_postimage_mismatch}
    end
  end

  defp postimage_matches?({name, path}, marker) do
    case read_state_file(path, marker["stateOwnership"][name]["uid"]) do
      {:ok, bytes} -> digest(bytes) == marker["postimages"][name]["sha256"]
      _ -> false
    end
  end

  defp set_marker_applied(%{"status" => "local_applied"} = marker, _path), do: {:ok, marker}

  defp set_marker_applied(marker, path) do
    applied_marker =
      marker
      |> Map.put("status", "local_applied")
      |> Map.put("completedAt", DateTime.utc_now() |> DateTime.to_iso8601())

    with :ok <- durable_replace(path, Jason.encode!(applied_marker)), do: {:ok, applied_marker}
  end

  defp publish_local_candidate(marker, _marker_path) do
    expected =
      local_receipt_payload(%{
        "pool" => marker["pool"],
        "issueId" => marker["issueId"],
        "generation" => 2,
        "reservationId" => marker["reservationId"],
        "assignmentSHA256" => marker["assignmentSHA256"],
        "assignmentDigest" => ConfirmedRecoveryEvidence.tuple_digest(marker["expected"]),
        "expected" => marker["expected"],
        "nonce" => marker["nonce"],
        "transactionId" => marker["nonce"],
        "evidenceRef" => marker["evidenceRef"],
        "observationSHA256" => marker["observationSHA256"],
        "proofSHA256" => marker["proofSHA256"],
        "preimages" => %{
          "claimJournalSHA256" => marker["preimages"]["claimJournalSHA256"],
          "fenceSHA256" => marker["preimages"]["fenceSHA256"],
          "responsibilityGraphSHA256" => marker["preimages"]["responsibilityGraphSHA256"]
        },
        "postimages" => %{
          "claimJournalSHA256" => marker["postimages"]["claimJournal"]["sha256"],
          "fenceSHA256" => marker["postimages"]["fence"]["sha256"],
          "responsibilityGraphSHA256" => marker["postimages"]["responsibilityGraph"]["sha256"]
        },
        "postconditions" => %{
          "dispatchPhase" => "recovery_pending",
          "executionLeaseStatus" => "released",
          "executionReleaseReason" => "spawn_failed",
          "responsibilityRuntimeLeaseStatus" => "released"
        },
        "completedAt" => marker["completedAt"]
      })

    path = Path.join(marker_directory(marker["issueId"]), "local-transition-candidate.json")
    bytes = ConfirmedRecoveryEvidence.canonical_json(expected)

    case read_trusted_evidence(path) do
      {:ok, ^bytes} -> :ok
      {:error, :enoent} -> durable_create(path, bytes)
      _ -> {:error, :local_transition_candidate_conflict}
    end
  end

  defp marker_postimages_valid(marker) when is_map(marker) do
    postimages = marker["postimages"]
    preimage_images = marker["preimageImages"]
    ownership = marker["stateOwnership"]

    with true <- marker["contractVersion"] == @marker_version,
         true <- marker["status"] in ["applying", "local_applied", "complete"],
         true <- is_map(postimages),
         true <- is_map(preimage_images),
         true <- valid_state_ownership?(ownership),
         true <-
           Enum.all?(@state_names, fn name ->
             entry = postimages[name]

             is_map(entry) and is_binary(entry["bytes"]) and
               digest(Base.url_decode64!(entry["bytes"], padding: false)) == entry["sha256"]
           end),
         true <-
           Enum.all?(@state_names, fn name ->
             image = preimage_images[name]
             is_binary(image) and digest(Base.url_decode64!(image, padding: false)) == marker_preimage_for(marker, name)
           end) do
      :ok
    else
      _ -> {:error, :invalid_hgs740_marker}
    end
  rescue
    _ -> {:error, :invalid_hgs740_marker}
  end

  defp marker_postimages_valid(_marker), do: {:error, :invalid_hgs740_marker}

  @doc false
  @spec valid_state_ownership?(map()) :: boolean()
  def valid_state_ownership?(ownership) when is_map(ownership) do
    Enum.sort(Map.keys(ownership)) == Enum.sort(@state_names ++ ["directories"]) and
      Enum.all?(@state_names, &valid_state_owner_record?(ownership[&1])) and
      valid_state_directory_records?(ownership["directories"], ownership["claimJournal"]["uid"])
  end

  def valid_state_ownership?(_ownership), do: false

  @doc false
  @spec directory_transition_allowed?(map(), map(), :freeze | :restore) :: boolean()
  def directory_transition_allowed?(current, original, :freeze) when is_map(current) and is_map(original) do
    same_directory_inode?(current, original) and
      current in [
        original,
        %{original | "uid" => 0, "gid" => 0},
        %{original | "mode" => 0o700}
      ]
  end

  def directory_transition_allowed?(current, original, :restore) when is_map(current) and is_map(original) do
    current == %{original | "uid" => 0, "gid" => 0, "mode" => 0o700} and
      same_directory_inode?(current, original)
  end

  def directory_transition_allowed?(_current, _original, _action), do: false

  defp valid_state_owner_record?(%{"uid" => uid, "gid" => gid, "mode" => mode} = record) do
    exact_keys?(record, ~w(uid gid mode)) and is_integer(uid) and uid > 0 and is_integer(gid) and
      gid >= 0 and is_integer(mode) and band(mode, 0o077) == 0
  end

  defp valid_state_owner_record?(_record), do: false

  defp valid_state_directory_records?(directories, owner) when is_map(directories) and map_size(directories) > 0 do
    Enum.all?(directories, fn {path, record} ->
      is_binary(path) and (path == @runtime_state_root or String.starts_with?(path, @runtime_state_root <> "/")) and
        exact_keys?(record, ~w(majorDevice minorDevice inode uid gid mode)) and
        Enum.all?(record, fn {_key, value} -> is_integer(value) and value >= 0 end) and
        record["uid"] in [0, owner] and
        band(record["mode"], 0o022) == 0
    end)
  end

  defp valid_state_directory_records?(_directories, _owner), do: false

  defp validate_marker_identity(marker, issue_id, pool, nonce) do
    with true <- marker["contractVersion"] == @marker_version,
         true <- marker["issueId"] == issue_id,
         true <- marker["pool"] == pool,
         true <- marker["nonce"] == nonce,
         true <- marker["generation"] == 2,
         true <- marker["status"] in ["applying", "local_applied", "complete"] do
      :ok
    else
      _ -> {:error, :hgs740_marker_identity_mismatch}
    end
  end

  defp verify_all_markers(_runtime) do
    case File.lstat(@evidence_root) do
      {:error, :enoent} ->
        :ok

      {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when band(mode, 0o777) == 0o700 ->
        with {:ok, issue_entries} <- File.ls(@evidence_root),
             true <- Enum.all?(issue_entries, &Regex.match?(~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, &1)),
             :ok <- verify_issue_markers(issue_entries) do
          :ok
        else
          _ -> {:error, :hgs740_startup_held_closed}
        end

      _ ->
        {:error, :hgs740_startup_held_closed}
    end
  end

  defp verify_issue_markers(issues) do
    Enum.reduce_while(issues, :ok, fn issue_id, :ok ->
      case verify_issue_marker(issue_id) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp verify_issue_marker(issue_id) do
    case read_trusted_evidence(marker_path(issue_id)) do
      {:error, :enoent} -> no_marker_evidence_policy(issue_id)
      {:ok, bytes} -> verify_startup_marker(issue_id, bytes)
      _ -> {:error, :hgs740_startup_held_closed}
    end
  end

  defp no_marker_evidence_policy(issue_id) do
    directory = marker_directory(issue_id)

    case File.lstat(directory) do
      {:error, :enoent} ->
        no_marker_startup_policy(issue_id, false)

      {:ok, %File.Stat{type: :directory}} ->
        with :ok <- trusted_evidence_directory(directory),
             {:ok, entries} <- File.ls(directory) do
          no_marker_startup_policy(issue_id, entries != [])
        else
          _ -> {:error, :hgs740_startup_held_closed}
        end

      _ ->
        {:error, :hgs740_startup_held_closed}
    end
  end

  defp verify_startup_marker(issue_id, bytes) do
    with {:ok, marker} when is_map(marker) <- Jason.decode(bytes),
         :ok <- marker_postimages_valid(marker),
         true <- marker["issueId"] == issue_id,
         :ok <- trusted_evidence_directory(marker_directory(issue_id)),
         :ok <- verify_signed_local_receipt(marker, issue_id),
         :ok <- verify_marker_status(marker) do
      :ok
    else
      _ -> {:error, :hgs740_startup_held_closed}
    end
  end

  defp verify_marker_status(%{"status" => "local_applied"}), do: {:error, :hgs740_recovery_not_complete}

  defp verify_marker_status(%{"status" => "complete"} = marker), do: verify_completed_marker(marker)
  defp verify_marker_status(_marker), do: {:error, :hgs740_startup_held_closed}

  defp verify_completed_marker(marker) do
    with {:ok, runtime} <- fixed_runtime_paths(marker["pool"]),
         {:ok, final_proof, payload} <- verify_provider_final_proof(marker, runtime, false),
         true <- digest(final_proof) == marker["providerFinalProofSHA256"],
         true <- payload["journalSHA256"] == marker["providerJournalSHA256"],
         :ok <- valid_completed_marker_postconditions(marker, payload),
         :ok <- validate_state_directories(marker, runtime, :original),
         :ok <- release_state_invariants(marker, runtime) do
      :ok
    else
      _ -> {:error, :hgs740_startup_held_closed}
    end
  end

  defp valid_completed_marker_postconditions(marker, payload) do
    with true <- marker["status"] == "complete",
         true <- marker["generation"] == 2 and marker["pool"] in @pools,
         true <- payload["localGenerationMax"] == 2 and payload["neverSpawned"] == true,
         true <- payload["receipt"]["nextGenerationFloor"] == 3,
         true <- payload["receipt"] == marker["providerReceipt"],
         true <- payload["journalSHA256"] == marker["providerJournalSHA256"],
         true <- marker["completionPostimages"] == expected_postimage_hashes(marker) do
      :ok
    else
      _ -> {:error, :hgs740_completion_marker_invalid}
    end
  end

  defp expected_postimage_hashes(marker) do
    %{
      "claimJournalSHA256" => marker["postimages"]["claimJournal"]["sha256"],
      "fenceSHA256" => marker["postimages"]["fence"]["sha256"],
      "responsibilityGraphSHA256" => marker["postimages"]["responsibilityGraph"]["sha256"]
    }
  end

  defp verify_signed_local_receipt(marker, issue_id) do
    path = Path.join(marker_directory(issue_id), "local-transition-receipt.json")
    candidate_path = Path.join(marker_directory(issue_id), "local-transition-candidate.json")

    with {:ok, bytes} <- read_trusted_evidence(path),
         {:ok, candidate_bytes} <- read_trusted_evidence(candidate_path),
         {:ok, envelope} when is_map(envelope) <- Jason.decode(bytes),
         true <- Enum.sort(Map.keys(envelope)) == ["payload", "signature"],
         {:ok, payload_bytes} <- Base.url_decode64(envelope["payload"], padding: false),
         {:ok, signature} <- Base.url_decode64(envelope["signature"], padding: false),
         {:ok, public_key} <- read_public_key(),
         true <- ConfirmedRecoveryEvidence.canonical_json(envelope) == bytes,
         true <- :crypto.verify(:eddsa, :none, @receipt_domain <> payload_bytes, signature, [public_key, :ed25519]),
         {:ok, payload} when is_map(payload) <- Jason.decode(payload_bytes),
         true <- ConfirmedRecoveryEvidence.canonical_json(payload) == payload_bytes,
         true <- payload_bytes == candidate_bytes,
         true <- Enum.sort(Map.keys(payload)) == Enum.sort(@local_receipt_fields),
         true <- payload["contractVersion"] == @receipt_version,
         true <- payload["issueId"] == issue_id and payload["pool"] == marker["pool"],
         true <- payload["generation"] == 2 and payload["reservationId"] == marker["reservationId"],
         true <- payload["nonce"] == marker["nonce"] and payload["transactionId"] == marker["nonce"],
         true <- local_receipt_bytes_valid?(payload_bytes, candidate_bytes, marker),
         true <- payload["expected"] == marker["expected"],
         true <- payload["assignmentDigest"] == ConfirmedRecoveryEvidence.tuple_digest(marker["expected"]),
         true <- payload["proofSHA256"] == marker["proofSHA256"],
         true <- payload["assignmentSHA256"] == marker["assignmentSHA256"],
         true <-
           payload["preimages"] == %{
             "claimJournalSHA256" => marker["preimages"]["claimJournalSHA256"],
             "fenceSHA256" => marker["preimages"]["fenceSHA256"],
             "responsibilityGraphSHA256" => marker["preimages"]["responsibilityGraphSHA256"]
           },
         true <-
           payload["postimages"] == %{
             "claimJournalSHA256" => marker["postimages"]["claimJournal"]["sha256"],
             "fenceSHA256" => marker["postimages"]["fence"]["sha256"],
             "responsibilityGraphSHA256" => marker["postimages"]["responsibilityGraph"]["sha256"]
           },
         true <-
           payload["postconditions"] == %{
             "dispatchPhase" => "recovery_pending",
             "executionLeaseStatus" => "released",
             "executionReleaseReason" => "spawn_failed",
             "responsibilityRuntimeLeaseStatus" => "released"
           },
         true <- payload["evidenceRef"] == marker["evidenceRef"] and payload["observationSHA256"] == marker["observationSHA256"] do
      :ok
    else
      _ -> {:error, :hgs740_local_receipt_invalid}
    end
  rescue
    _ -> {:error, :hgs740_local_receipt_invalid}
  end

  defp runtime_paths(pool) do
    with {:ok, paths} <- fixed_runtime_paths(pool),
         true <- Config.execution_fence_state_path() == paths.execution_fence_path do
      {:ok, paths}
    else
      _ -> {:error, :configured_state_path_mismatch}
    end
  end

  defp fixed_runtime_paths(pool) do
    journal_path = Path.join(["/srv/dahlia-runner-state", "run", "pools", pool, "work-package.json"])
    state_dir = Path.join(["/srv/dahlia-runner-state", "workspaces", "pools", pool, ".symphony"])
    fence_path = Path.join(state_dir, "execution-fence.json")
    graph_path = Path.join(state_dir, "responsibility-graph.json")

    with :ok <- require_pool(pool) do
      {:ok,
       %{
         pool_key: pool,
         journal_path: journal_path,
         execution_fence_path: fence_path,
         responsibility_graph_path: graph_path
       }}
    end
  end

  defp trusted_runtime_files(runtime, pool) do
    expected_journal = Path.join(["/srv/dahlia-runner-state", "run", "pools", pool, "work-package.json"])
    expected_state_dir = Path.join(["/srv/dahlia-runner-state", "workspaces", "pools", pool, ".symphony"])

    with true <- runtime.journal_path == expected_journal,
         true <-
           runtime.execution_fence_path == Path.join(expected_state_dir, "execution-fence.json"),
         true <-
           runtime.responsibility_graph_path == Path.join(expected_state_dir, "responsibility-graph.json"),
         :ok <- trusted_service_state_files(runtime_state_paths(runtime)) do
      :ok
    else
      _ -> {:error, :untrusted_pool_state_path}
    end
  end

  defp runtime_state_paths(runtime) do
    [runtime.journal_path, runtime.execution_fence_path, runtime.responsibility_graph_path]
  end

  defp trusted_service_state_files(paths) do
    with stats when length(stats) == 3 <- Enum.map(paths, &File.lstat/1),
         true <- Enum.all?(stats, &match?({:ok, %File.Stat{type: :regular, links: 1, mode: mode}} when band(mode, 0o077) == 0, &1)),
         [{:ok, %File.Stat{uid: owner}} | _] = stats,
         true <- owner > 0,
         true <- Enum.all?(stats, fn {:ok, stat} -> stat.uid == owner end),
         true <- Enum.all?(paths, &trusted_state_ancestors?(Path.dirname(&1), owner)) do
      :ok
    else
      _ -> {:error, :untrusted_pool_state_path}
    end
  end

  defp trusted_state_ancestors?(path, owner) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, uid: uid, mode: mode}}
      when uid in [0, owner] and band(mode, 0o022) == 0 ->
        parent = Path.dirname(path)
        parent == path or trusted_state_ancestors?(parent, owner)

      _ ->
        false
    end
  end

  defp read_state_file(path), do: read_state_file(path, nil)

  defp read_state_file(path, allowed_owner) do
    with {:ok, before} <- state_file_snapshot(path, allowed_owner),
         {:ok, bytes} <- descriptor_read_state_file(path, before),
         true <- byte_size(bytes) <= 16_777_216,
         {:ok, after_read} <- state_file_snapshot(path, allowed_owner),
         true <- before == after_read do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_state_file}
    end
  end

  defp state_file_snapshot(path, allowed_owner) do
    with {:ok, stat} <- File.lstat(path, time: :posix),
         true <- stat.type == :regular and stat.links == 1 and stat.size in 1..16_777_216,
         owner = allowed_owner || stat.uid,
         true <- stat.uid in [owner, 0],
         true <- band(stat.mode, 0o077) == 0,
         {:ok, ancestors} <- state_directory_snapshots(Path.dirname(path), owner, []) do
      {:ok, {ancestors, Map.take(stat, @state_file_metadata)}}
    else
      _ -> {:error, :untrusted_state_file_metadata}
    end
  end

  defp state_directory_snapshots(path, state_owner, acc) do
    with {:ok, stat} <- File.lstat(path, time: :posix),
         true <- stat.type == :directory and stat.uid in [0, state_owner],
         true <- band(stat.mode, 0o022) == 0 do
      next = [{path, Map.take(stat, @state_file_metadata)} | acc]
      parent = Path.dirname(path)
      if parent == path, do: {:ok, next}, else: state_directory_snapshots(parent, state_owner, next)
    else
      _ -> {:error, :untrusted_state_file_ancestor}
    end
  end

  defp descriptor_read_state_file(path, {_ancestors, expected_metadata}) do
    case File.open(path, [:read, :binary, :raw]) do
      {:ok, io} ->
        result =
          with :ok <- descriptor_matches_state_file(io, expected_metadata),
               {:ok, bytes} <- :file.read(io, 16_777_217),
               :ok <- descriptor_matches_state_file(io, expected_metadata) do
            {:ok, bytes}
          else
            _ -> {:error, :untrusted_state_file_descriptor}
          end

        case {result, File.close(io)} do
          {{:ok, bytes}, :ok} -> {:ok, bytes}
          _ -> {:error, :untrusted_state_file_descriptor}
        end

      _ ->
        {:error, :untrusted_state_file_descriptor}
    end
  end

  defp descriptor_matches_state_file(io, expected_metadata) do
    with {:ok, record} <- :file.read_file_info(io, time: :posix),
         true <- Map.take(File.Stat.from_record(record), @state_file_metadata) == expected_metadata do
      :ok
    else
      _ -> {:error, :untrusted_state_file_descriptor}
    end
  end

  defp read_root_file(path, max_bytes) do
    with :ok <- trusted_root_directory(Path.dirname(path)),
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1, size: size}} <- File.lstat(path),
         true <- band(mode, 0o022) == 0 and size <= max_bytes,
         {:ok, bytes} <- File.read(path) do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_root_file}
    end
  end

  defp read_trusted_evidence(path) do
    with :ok <- trusted_evidence_directory(Path.dirname(path)),
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1, size: size}} <- File.lstat(path),
         true <- band(mode, 0o777) == 0o600 and size <= 16_777_216,
         {:ok, bytes} <- File.read(path) do
      {:ok, bytes}
    else
      {:error, :enoent} -> {:error, :enoent}
      _ -> {:error, :untrusted_hgs740_evidence}
    end
  end

  defp trusted_evidence_directory(path) do
    if path == @evidence_root or String.starts_with?(path, @evidence_root <> "/") do
      with :ok <- trusted_root_directory(Path.dirname(@evidence_root)),
           :ok <- exact_private_evidence_directories(path) do
        :ok
      else
        _ -> {:error, :untrusted_hgs740_path}
      end
    else
      {:error, :untrusted_hgs740_path}
    end
  end

  defp exact_private_evidence_directories(path) do
    relative = Path.relative_to(path, Path.dirname(@evidence_root))

    Enum.reduce_while(Path.split(relative), Path.dirname(@evidence_root), fn part, parent ->
      current = Path.join(parent, part)

      case File.lstat(current) do
        {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when band(mode, 0o777) == 0o700 ->
          {:cont, current}

        _ ->
          {:halt, {:error, :untrusted_hgs740_directory}}
      end
    end)
    |> case do
      final when is_binary(final) and final == path -> :ok
      _ -> {:error, :untrusted_hgs740_directory}
    end
  end

  defp trusted_root_directory(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when band(mode, 0o022) == 0 ->
        parent = Path.dirname(path)
        if parent == path, do: :ok, else: trusted_root_directory(parent)

      _ ->
        {:error, :untrusted_root_directory}
    end
  end

  defp ensure_evidence_directory(issue_id) do
    directory = marker_directory(issue_id)
    trusted_evidence_directory(directory)
  end

  defp durable_create(path, bytes) do
    with :ok <- exclusive_write_synced(path, bytes), do: fsync_directory(Path.dirname(path))
  end

  defp durable_replace(path, bytes) do
    temporary = path <> ".tmp-" <> Integer.to_string(System.unique_integer([:positive]))

    result =
      with :ok <- exclusive_write_synced(temporary, bytes),
           :ok <- File.rename(temporary, path),
           do: fsync_directory(Path.dirname(path))

    if result != :ok, do: File.rm(temporary)
    result
  end

  defp exclusive_write_synced(path, bytes) do
    case :file.open(String.to_charlist(path), [:write, :binary, :raw, :exclusive, :sync]) do
      {:ok, file} ->
        try do
          with :ok <- :file.write(file, bytes),
               :ok <- :file.sync(file),
               do: File.chmod(path, 0o600)
        after
          :file.close(file)
        end

      {:error, reason} ->
        {:error, {:durable_write_failed, reason}}
    end
  end

  defp fsync_directory(path) do
    case :file.open(String.to_charlist(path), [:read, :raw]) do
      {:ok, directory} ->
        result = :file.sync(directory)
        :file.close(directory)
        result

      {:error, reason} ->
        {:error, {:directory_sync_failed, reason}}
    end
  end

  defp marker_path(issue_id), do: Path.join(marker_directory(issue_id), "transaction.json")

  defp trusted_workflow_file(path, pool) do
    expected_path =
      Path.join(["/srv/dahlia-runner-state", "dahlia", "config", "symphony", "workflows", pool <> ".md"])

    with true <- Path.type(path) == :absolute and Path.expand(path) == path,
         true <- path == expected_path,
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1}} <- File.lstat(path),
         true <- band(mode, 0o022) == 0,
         :ok <- trusted_root_directory(Path.dirname(path)) do
      :ok
    else
      _ -> {:error, :untrusted_workflow_file}
    end
  end

  defp require_root do
    case File.stat("/proc/self") do
      {:ok, %File.Stat{uid: 0}} -> :ok
      _ -> {:error, :root_privilege_required}
    end
  end

  defp require_paused_gate do
    transition_path = Path.join(Path.dirname(@pause_path), "global-mutable-pause.transition")

    with :ok <- trusted_root_directory(Path.dirname(@pause_path)),
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1}} <- File.lstat(@pause_path),
         true <- band(mode, 0o777) == 0o640,
         {:ok, "paused\n"} <- File.read(@pause_path),
         {:error, :enoent} <- File.lstat(transition_path) do
      :ok
    else
      _ -> {:error, :global_gate_must_be_configured_and_paused}
    end
  end

  defp require_mutation_quiescent(runtime, uid) do
    with :ok <- require_services_quiescent(),
         :ok <- trusted_runtime_directories(runtime, runtime.pool_key, uid),
         :ok <- user_manager_quiescent(uid),
         :ok <- no_processes_for_uid(uid) do
      :ok
    else
      _ -> {:error, :pool_state_owner_not_quiescent}
    end
  end

  defp trusted_runtime_directories(runtime, pool, owner) do
    paths = runtime_state_paths(runtime)
    expected_journal = Path.join([@runtime_state_root, "run", "pools", pool, "work-package.json"])
    expected_state_dir = Path.join([@runtime_state_root, "workspaces", "pools", pool, ".symphony"])

    with true <- runtime.journal_path == expected_journal,
         true <- runtime.execution_fence_path == Path.join(expected_state_dir, "execution-fence.json"),
         true <- runtime.responsibility_graph_path == Path.join(expected_state_dir, "responsibility-graph.json"),
         true <- Enum.all?(paths, &trusted_state_ancestors?(Path.dirname(&1), owner)) do
      :ok
    else
      _ -> {:error, :untrusted_pool_state_directory}
    end
  end

  defp require_services_quiescent do
    units = Enum.map(@pools, &"dahlia-symphony@#{&1}.service") ++ ["dahlia-claim-witness.service", "dahlia-claim-witness.socket"]

    with :ok <- trusted_systemctl(),
         true <- Enum.all?(units, &unit_quiescent?/1) do
      :ok
    else
      _ -> {:error, :managed_services_not_quiescent}
    end
  end

  defp user_manager_quiescent(uid) do
    with :ok <- trusted_systemctl(),
         {properties, 0} <-
           System.cmd("/usr/bin/systemctl", ["show", "--property=ActiveState,ControlGroup,MainPID", "user@#{uid}.service"], stderr_to_stdout: true),
         {:ok, values} <- parse_systemd_properties(properties),
         true <- values["ActiveState"] in ["inactive", "failed"],
         true <- values["ControlGroup"] == "",
         true <- values["MainPID"] in ["0", nil] do
      :ok
    else
      _ -> {:error, :user_manager_not_quiescent}
    end
  rescue
    _ -> {:error, :user_manager_not_quiescent}
  end

  defp no_processes_for_uid(uid) do
    with {:ok, entries} <- File.ls("/proc"),
         true <- Enum.all?(entries, &proc_entry_not_owned_by?(&1, uid)) do
      :ok
    else
      _ -> {:error, :state_owner_process_present}
    end
  end

  defp proc_entry_not_owned_by?(entry, uid) do
    if Regex.match?(~r/\A[0-9]+\z/, entry) do
      case File.lstat(Path.join("/proc", entry)) do
        {:ok, %File.Stat{uid: ^uid}} -> false
        {:ok, %File.Stat{}} -> true
        {:error, :enoent} -> true
        _ -> false
      end
    else
      true
    end
  end

  defp unit_quiescent?(unit) do
    with {enabled, status} <- System.cmd("/usr/bin/systemctl", ["is-enabled", unit], stderr_to_stdout: true),
         true <- status == 1 and String.trim(enabled) == "masked",
         {properties, 0} <-
           System.cmd(
             "/usr/bin/systemctl",
             ["show", "--property=ActiveState,ControlGroup,MainPID", unit],
             stderr_to_stdout: true
           ),
         {:ok, values} <- parse_systemd_properties(properties),
         true <- values["ActiveState"] in ["inactive", "failed"],
         true <- values["ControlGroup"] == "",
         true <- values["MainPID"] in ["0", nil] do
      true
    else
      _ -> false
    end
  rescue
    _ -> false
  end

  defp parse_systemd_properties(output) do
    Enum.reduce_while(String.split(output, "\n", trim: true), {:ok, %{}}, fn line, {:ok, values} ->
      case parse_systemd_property(line, values) do
        {:ok, next_values} -> {:cont, {:ok, next_values}}
        :invalid -> {:halt, :invalid}
      end
    end)
    |> case do
      {:ok, values} when map_size(values) == length(@systemd_properties) -> {:ok, values}
      _ -> {:error, :invalid_systemd_properties}
    end
  end

  defp parse_systemd_property(line, values) do
    case String.split(line, "=", parts: 2) do
      [key, value] when key in @systemd_properties and not is_map_key(values, key) ->
        {:ok, Map.put(values, key, value)}

      _ ->
        :invalid
    end
  end

  defp trusted_systemctl do
    case File.lstat("/usr/bin/systemctl") do
      {:ok, %File.Stat{type: :regular, uid: 0, mode: mode}} when band(mode, 0o022) == 0 -> :ok
      _ -> {:error, :untrusted_systemctl}
    end
  end

  defp require_pool(pool) when pool in @pools, do: :ok
  defp require_pool(_pool), do: {:error, :invalid_pool}

  defp require_issue_id(issue_id) do
    if Regex.match?(~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, issue_id),
      do: :ok,
      else: {:error, :invalid_issue_id}
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp digest?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
  defp text?(value), do: is_binary(value) and value != ""
  defp timestamp?(value) when is_binary(value), do: match?({:ok, _, _}, DateTime.from_iso8601(value))
  defp timestamp?(_value), do: false

  defp validate_ordered_json(%Jason.OrderedObject{values: pairs}) do
    keys = Enum.map(pairs, &elem(&1, 0))

    if Enum.all?(keys, &is_binary/1) and length(keys) == length(Enum.uniq(keys)) do
      validate_ordered_values(pairs)
    else
      {:error, :invalid_candidate_json}
    end
  end

  defp validate_ordered_json(values) when is_list(values) do
    Enum.reduce_while(values, :ok, fn value, :ok ->
      case validate_ordered_json(value) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp validate_ordered_json(_value), do: :ok

  defp validate_ordered_values(pairs) do
    Enum.reduce_while(pairs, :ok, fn {_key, value}, :ok ->
      case validate_ordered_json(value) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp ordered_json_to_term(%Jason.OrderedObject{values: pairs}) do
    Map.new(pairs, fn {key, value} -> {key, ordered_json_to_term(value)} end)
  end

  defp ordered_json_to_term(values) when is_list(values), do: Enum.map(values, &ordered_json_to_term/1)
  defp ordered_json_to_term(value), do: value
end
