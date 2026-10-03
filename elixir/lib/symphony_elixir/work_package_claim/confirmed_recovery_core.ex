defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryCore do
  @moduledoc """
  Root-only write-ahead transition for the paused HGS-740 confirmed claim.

  The signed proof is verified against exact persisted preimages before a marker is
  written. The marker contains the three validated postimages and is durable before
  the first state replacement, allowing exact crash replay without editing JSON by
  hand. Startup verification keeps admission closed while a transition is partial.
  """

  import Bitwise, only: [band: 2]

  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence

  alias SymphonyElixir.WorkPackageClaim.{
    ConfirmedRecoveryClaimTransition,
    ConfirmedRecoveryContext,
    ConfirmedRecoveryEvidence,
    ConfirmedRecoveryIssuer,
    ConfirmedRecoveryKubernetes,
    ConfirmedRecoveryLineage,
    ConfirmedRecoveryProviderRelease,
    ConfirmedRecoveryReconciliation,
    ConfirmedRecoveryRootHost,
    ConfirmedRecoveryStateMachine,
    ConfirmedRecoveryWAL,
    Journal
  }

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryUnsubmittedPredecessor

  @issue_id "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"
  @state_root "/srv/dahlia-runner-state"
  @local_receipt_fields ~w(assignmentDigest assignmentSHA256 completedAt contractVersion evidenceRef expected generation issueId nonce observationSHA256 pool postconditions postimages preimages proofSHA256 reservationId transactionId)
  @local_receipt_fields_v2 ~w(assignmentDigest assignmentSHA256 assignmentSnapshotState completedAt contractVersion evidenceRef expected generation issueId nonce observationSHA256 pool postconditions postimages preimages proofSHA256 reservationId transactionId)
  @state_file_metadata [:major_device, :minor_device, :inode, :uid, :gid, :mode, :links, :size, :mtime, :ctime]
  @marker_version "work-package-hgs740-local-transition.v1"
  @marker_version_v2 "work-package-hgs740-local-transition.v2"
  @marker_version_v3 "work-package-hgs740-local-transition.v3"
  @receipt_version "work-package-hgs740-local-transition-receipt.v1"
  @receipt_version_v2 "work-package-hgs740-local-transition-receipt.v2"
  @receipt_version_v3 "work-package-hgs740-local-transition-receipt.v3"
  @receipt_domain "hypergrid-work-package-recovery:hgs740-local-transition-receipt.v1\0"
  @receipt_domain_v2 "hypergrid-work-package-recovery:hgs740-local-transition-receipt.v2\0"
  @receipt_domain_v3 "hypergrid-work-package-recovery:hgs740-local-transition-receipt.v3\0"
  @pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)
  @state_names ~w(claimJournal fence responsibilityGraph)
  @type result :: {:ok, :applied | :already_applied} | {:error, term()}

  @doc "Applies or resumes the exact signed confirmed-claim transition as root."
  @spec apply(String.t(), String.t(), String.t(), String.t()) :: result()
  def apply(issue_id, pool, workflow_path, nonce)
      when is_binary(issue_id) and is_binary(pool) and is_binary(workflow_path) and is_binary(nonce) do
    with {:ok, context} <- ConfirmedRecoveryRootHost.authorize_apply(issue_id, pool, workflow_path, nonce),
         {:ok, result} <-
           ConfirmedRecoveryRootHost.with_pool_lock(context, fn -> apply_authorized_context(context) end) do
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

  @doc false
  @spec local_receipt_snapshot(ConfirmedRecoveryContext.t()) :: {:ok, map()} | {:error, term()}
  def local_receipt_snapshot(%ConfirmedRecoveryContext{} = context) do
    runtime = runtime_with_host(context)
    directory = marker_directory(context.issue_id, runtime)
    candidate_path = Path.join(directory, "local-transition-candidate.json")

    with :ok <- validate_context(context),
         :ok <- host0(runtime, :require_paused_gate),
         :ok <- host0(runtime, :require_services_quiescent),
         {:ok, marker_bytes} <- read_trusted_evidence(marker_path(context.issue_id, runtime), runtime),
         {:ok, marker} when is_map(marker) <- Jason.decode(marker_bytes),
         :ok <- validate_marker_identity(marker, context.issue_id, context.pool, context.nonce),
         true <- marker["status"] == "local_applied",
         :ok <- marker_postimages_valid(marker),
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         {:ok, preimages} <- paths_from_marker_preimages(marker, runtime),
         {:ok, _observation, observation_bytes, proof_bytes, payload} <- verify_saved_proof(marker, preimages),
         true <- digest(observation_bytes) == marker["observationSHA256"],
         true <- digest(proof_bytes) == marker["proofSHA256"],
         :ok <- verify_local_claim(preimages, payload, runtime),
         {:ok, computed} <- prepare_postimages(preimages, payload, runtime, marker["verificationNowMs"]),
         true <- postimages_match_marker?(computed, marker),
         :ok <- release_state_invariants(marker, runtime),
         {:ok, current} <- read_state_preimages(runtime),
         images = %{"claimJournal" => current.journal.bytes},
         images = Map.put(images, "fence", current.fence.bytes),
         images = Map.put(images, "responsibilityGraph", current.graph.bytes),
         true <- Enum.all?(images, fn {name, bytes} -> digest(bytes) == marker["postimages"][name]["sha256"] end),
         {:ok, candidate_bytes} <- read_trusted_evidence(candidate_path, runtime),
         true <- candidate_bytes == local_candidate_bytes(marker),
         {:ok, inputs} <- signed_input_directory(directory, runtime),
         {:ok, ^observation_bytes} <- read_trusted_evidence(Path.join(inputs, "candidate.json"), runtime),
         {:ok, ^proof_bytes} <- read_trusted_evidence(Path.join(inputs, "confirmed-root-envelope.json"), runtime),
         {:ok, ^candidate_bytes} <- read_trusted_evidence(candidate_path, runtime),
         {:ok, ^marker_bytes} <- read_trusted_evidence(marker_path(context.issue_id, runtime), runtime),
         :ok <- verify_postimages(marker, runtime),
         :ok <- host0(runtime, :require_paused_gate),
         :ok <- host0(runtime, :require_services_quiescent),
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]) do
      snapshot = %{marker_bytes: marker_bytes, candidate_bytes: candidate_bytes}
      snapshot = Map.merge(snapshot, %{committed_images: images, proof_bytes: proof_bytes})
      {:ok, Map.put(snapshot, :observation_bytes, observation_bytes)}
    else
      _ -> {:error, :hgs740_local_receipt_snapshot_held_closed}
    end
  rescue
    _ -> {:error, :hgs740_local_receipt_snapshot_held_closed}
  catch
    _, _ -> {:error, :hgs740_local_receipt_snapshot_held_closed}
  end

  def local_receipt_snapshot(_context), do: {:error, :hgs740_local_receipt_snapshot_held_closed}

  @doc false
  @spec release_only_snapshot(ConfirmedRecoveryContext.t()) :: {:ok, map()} | {:error, term()}
  def release_only_snapshot(%ConfirmedRecoveryContext{} = context) do
    runtime = runtime_with_host(context)
    path = Path.join(marker_directory(context.issue_id, runtime), "reconciliation/epoch-5/manifest.json")

    with {:ok, snapshot} <- local_receipt_snapshot(context),
         {:ok, observation} <- Jason.decode(snapshot.observation_bytes),
         %{"epoch" => "epoch-5", "observedAt" => observed_at} = metadata <- observation["reconciliation"],
         true <- observed_at == observation["observedAt"],
         {:ok, manifest} <- read_trusted_evidence(path, runtime),
         true <- manifest == ConfirmedRecoveryEvidence.canonical_json(metadata),
         {:ok, ^snapshot} <- local_receipt_snapshot(context),
         {:ok, ^manifest} <- read_trusted_evidence(path, runtime) do
      {:ok, Map.put(snapshot, :manifest_bytes, manifest)}
    else
      _ -> {:error, :hgs740_release_binding_held_closed}
    end
  rescue
    _ -> {:error, :hgs740_release_binding_held_closed}
  end

  @doc false
  @spec release_only_artifact(ConfirmedRecoveryContext.t(), String.t(), binary() | nil) :: term()
  def release_only_artifact(%ConfirmedRecoveryContext{} = context, name, bytes)
      when name in ["release-only-attempt.json", "release-only-attestation.json", "release-only-bundle.json", "local-transition-receipt.json"] and
             (is_nil(bytes) or (is_binary(bytes) and byte_size(bytes) <= 262_144)) do
    runtime = runtime_with_host(context)
    path = Path.join(marker_directory(context.issue_id, runtime), name)

    with :ok <- validate_context(context),
         :ok <- host0(runtime, :require_paused_gate),
         :ok <- host0(runtime, :require_services_quiescent) do
      if is_nil(bytes), do: read_trusted_evidence(path, runtime), else: create_release_artifact(path, bytes, runtime)
    end
  end

  def release_only_artifact(_context, _name, _bytes), do: {:error, :invalid_release_artifact}

  defp create_release_artifact(path, bytes, runtime) do
    with :ok <- trusted_evidence_directory(Path.dirname(path), runtime),
         {:error, :enoent} <- read_trusted_evidence(path, runtime),
         :ok <- durable_create(path, bytes, runtime),
         {:ok, ^bytes} <- read_trusted_evidence(path, runtime) do
      :ok
    else
      {:ok, _} -> {:error, :release_artifact_already_exists}
      error -> error
    end
  end

  @doc "Completes a locally applied recovery after exact provider release and fresh no-Job/no-Pod readback."
  @spec complete(String.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def complete(issue_id, pool, workflow_path)
      when is_binary(issue_id) and is_binary(pool) and is_binary(workflow_path) do
    with {:ok, context} <- ConfirmedRecoveryRootHost.authorize_completion(issue_id, pool, workflow_path),
         :ok <-
           ConfirmedRecoveryRootHost.with_pool_lock(context, fn -> complete_authorized_context(context) end) do
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
    with {:ok, context} <- ConfirmedRecoveryRootHost.authorize_startup(workflow_path, pool),
         :ok <- verify_startup_authorized_context(context) do
      :ok
    else
      {:error, _reason} = error -> error
    end
  rescue
    _ -> {:error, :hgs740_startup_held_closed}
  catch
    _, _ -> {:error, :hgs740_startup_held_closed}
  end

  def verify_startup(_workflow_path, _pool), do: {:error, :invalid_hgs740_startup_request}

  if Mix.env() == :test do
    @doc false
    def apply_with_test_context(%ConfirmedRecoveryContext{} = context), do: apply_authorized_context(context)

    @doc false
    def apply_with_test_context(_context), do: {:error, :confirmed_recovery_held_closed}

    @doc false
    def complete_with_test_context(%ConfirmedRecoveryContext{} = context), do: complete_authorized_context(context)

    @doc false
    def complete_with_test_context(_context), do: {:error, :hgs740_completion_held_closed}

    @doc false
    def verify_startup_with_test_context(%ConfirmedRecoveryContext{} = context),
      do: verify_startup_authorized_context(context)

    @doc false
    def verify_startup_with_test_context(_context), do: {:error, :hgs740_startup_held_closed}

    @doc false
    def validate_test_context(%ConfirmedRecoveryContext{} = context), do: validate_context(context)

    @doc false
    def validate_test_context(_context), do: {:error, :invalid_verified_recovery_context}

    @doc false
    def persist_initial_marker_with_test_context(marker_path, marker, runtime),
      do: persist_initial_marker(marker_path, marker, runtime)

    @doc false
    @spec replace_marker_with_test_context(Path.t(), binary(), map()) :: :ok | {:error, term()}
    def replace_marker_with_test_context(marker_path, bytes, runtime),
      do: durable_replace(marker_path, bytes, runtime)
  end

  defp apply_authorized_context(%ConfirmedRecoveryContext{} = context) do
    runtime = runtime_with_host(context)

    with :ok <- validate_context(context),
         {:ok, result} <- apply_locked(context.issue_id, context.pool, context.nonce, runtime) do
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

  defp complete_authorized_context(%ConfirmedRecoveryContext{} = context) do
    runtime = runtime_with_host(context)

    with :ok <- validate_context(context),
         {:ok, :complete} <- complete_locked(context.issue_id, context.pool, runtime) do
      :ok
    else
      {:error, _reason} = error -> error
    end
  rescue
    _ -> {:error, :hgs740_completion_held_closed}
  catch
    _, _ -> {:error, :hgs740_completion_held_closed}
  end

  defp verify_startup_authorized_context(%ConfirmedRecoveryContext{} = context) do
    runtime = runtime_with_host(context)

    with :ok <- validate_context(context),
         :ok <- verify_all_markers(runtime) do
      :ok
    else
      {:error, _reason} = error -> error
    end
  rescue
    _ -> {:error, :hgs740_startup_held_closed}
  catch
    _, _ -> {:error, :hgs740_startup_held_closed}
  end

  @doc false
  @spec marker_directory(String.t()) :: Path.t()
  def marker_directory(issue_id), do: ConfirmedRecoveryRootHost.marker_directory(issue_id)

  defp runtime_with_host(%ConfirmedRecoveryContext{runtime: runtime, host_ops: host_ops}),
    do: Map.put(runtime, :host_ops, host_ops)

  @spec validate_context(ConfirmedRecoveryContext.t()) :: :ok | {:error, :invalid_verified_recovery_context}
  defp validate_context(%ConfirmedRecoveryContext{
         issue_id: issue_id,
         pool: pool,
         nonce: nonce,
         workflow_path: workflow_path,
         runtime: runtime,
         host_ops: host_ops
       })
       when is_binary(issue_id) and is_binary(pool) and is_binary(nonce) and
              is_binary(workflow_path) and is_map(runtime) and is_map(host_ops) do
    with :ok <- require_pool(pool),
         :ok <- require_issue_id(issue_id),
         true <- runtime.pool_key == pool,
         {:ok, expected} <- host_ops(host_ops, :fixed_runtime_paths, [pool]),
         true <- runtime_paths_match?(runtime, expected) do
      :ok
    else
      _ -> {:error, :invalid_verified_recovery_context}
    end
  end

  defp validate_context(_context), do: {:error, :invalid_verified_recovery_context}

  defp runtime_paths_match?(runtime, expected) when is_map(expected) do
    Enum.all?([:pool_key, :journal_path, :execution_fence_path, :responsibility_graph_path], fn key ->
      Map.get(runtime, key) == Map.get(expected, key)
    end)
  end

  defp runtime_paths_match?(_runtime, _expected), do: false

  defp host(runtime, operation, args) do
    host_ops(runtime.host_ops, operation, args)
  end

  defp host_ops(operations, operation, args) do
    case Map.fetch(operations, operation) do
      {:ok, callback} when is_function(callback, length(args)) -> Kernel.apply(callback, args)
      _ -> {:error, :invalid_host_operations}
    end
  end

  defp host0(runtime, operation), do: host(runtime, operation, [])

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

  def no_marker_startup_policy(issue_id, evidence_directory_exists),
    do: ConfirmedRecoveryStateMachine.no_marker_startup_policy(issue_id, @issue_id, evidence_directory_exists)

  @doc false
  @spec local_receipt_payload(map()) :: map()
  def local_receipt_payload(fields) when is_map(fields) do
    version = Map.get(fields, "contractVersion", if(fields["assignmentSnapshotState"] == "absent", do: @receipt_version_v2, else: @receipt_version))
    Map.put(fields, "contractVersion", version)
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
    {receipt_version, receipt_fields, _receipt_domain} = receipt_contract(marker)

    with {:ok, payload} when is_map(payload) <- Jason.decode(payload_bytes),
         true <- ConfirmedRecoveryEvidence.canonical_json(payload) == payload_bytes,
         true <- Enum.sort(Map.keys(payload)) == Enum.sort(receipt_fields),
         true <- payload_bytes == candidate_bytes,
         true <- payload["contractVersion"] == receipt_version,
         true <- payload["assignmentSnapshotState"] == marker["assignmentSnapshotState"],
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

  defp receipt_contract(%{"contractVersion" => @marker_version_v3}),
    do: {@receipt_version_v3, @local_receipt_fields_v2, @receipt_domain_v3}

  defp receipt_contract(%{"contractVersion" => @marker_version_v2}),
    do: {@receipt_version_v2, @local_receipt_fields_v2, @receipt_domain_v2}

  defp receipt_contract(%{"contractVersion" => @marker_version}),
    do: {@receipt_version, @local_receipt_fields, @receipt_domain}

  defp receipt_contract(_marker), do: {@receipt_version, @local_receipt_fields, @receipt_domain}

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
    ConfirmedRecoveryProviderRelease.validate_receipt_binding(
      receipt,
      confirmed,
      recovery_id,
      expected,
      old_tuple_digest,
      fence_revision,
      proof_bytes
    )
  end

  def validate_hgs719_receipt_binding(_receipt, _confirmed, _recovery_id, _expected, _old_tuple_digest, _fence_revision, _proof_bytes),
    do: {:error, :provider_confirmation_receipt_mismatch}

  defp apply_locked(issue_id, pool, nonce, runtime) do
    marker_path = marker_path(issue_id, runtime)

    with :ok <- host(runtime, :require_service_stopped, [pool]),
         :ok <- host0(runtime, :require_services_quiescent),
         :ok <- host0(runtime, :require_paused_gate),
         {:ok, marker} <- load_or_create_marker(issue_id, pool, nonce, runtime, marker_path),
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         {:ok, applied_marker} <- apply_marker(marker, marker_path, runtime),
         :ok <- host0(runtime, :require_paused_gate),
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         :ok <- host(runtime, :require_service_stopped, [pool]),
         :ok <- publish_local_candidate(applied_marker, marker_path, runtime) do
      {:ok, if(marker["status"] == "local_applied", do: :already_applied, else: :applied)}
    end
  end

  defp complete_locked(issue_id, pool, runtime) do
    marker_path = marker_path(issue_id, runtime)

    with :ok <- host(runtime, :require_service_stopped, [pool]),
         :ok <- host0(runtime, :require_services_quiescent),
         :ok <- host0(runtime, :require_paused_gate),
         {:ok, marker_bytes} <- read_trusted_evidence(marker_path, runtime),
         {:ok, marker} when is_map(marker) <- Jason.decode(marker_bytes),
         :ok <- validate_marker_identity(marker, issue_id, pool, marker["nonce"]),
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         :ok <- freeze_state_directories(marker, runtime),
         result <- complete_status(marker, issue_id, pool, runtime),
         {:ok, status} <- result do
      {:ok, status}
    else
      _ -> {:error, :hgs740_completion_held_closed}
    end
  end

  defp complete_status(%{"status" => "local_applied"} = marker, issue_id, pool, runtime) do
    ConfirmedRecoveryStateMachine.complete(marker, issue_id, pool, completion_operations(issue_id, runtime))
  end

  defp complete_status(%{"status" => "complete"} = marker, issue_id, pool, runtime) do
    ConfirmedRecoveryStateMachine.complete(marker, issue_id, pool, completion_operations(issue_id, runtime))
  end

  defp complete_status(_marker, _issue_id, _pool, _runtime), do: {:error, :hgs740_completion_held_closed}

  defp completion_operations(issue_id, runtime) do
    %{
      verify_receipt: fn marker, receipt_issue_id ->
        verify_signed_local_receipt(marker, receipt_issue_id, runtime)
      end,
      verify_postimages: fn marker -> verify_postimages(marker, runtime) end,
      provider_final: fn marker, require_current? ->
        verify_provider_final_proof(marker, runtime, require_current?)
      end,
      candidate: fn issue_id, marker -> read_candidate(issue_id, marker, runtime) end,
      observe: fn marker, candidate ->
        observe_kubernetes(marker, candidate, runtime)
      end,
      final_release_invariants: fn marker, payload ->
        final_local_release_invariants(marker, runtime, payload)
      end,
      mutation_quiescent: fn marker ->
        require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"])
      end,
      service_stopped: &host(runtime, :require_service_stopped, [&1]),
      write_complete: fn marker, proof, payload, observation ->
        complete_marker(marker, marker_path(issue_id, runtime), proof, payload, observation, runtime)
      end,
      digest: &digest/1,
      valid_postconditions: &valid_completed_marker_postconditions/2,
      release_invariants: fn marker -> release_state_invariants(marker, runtime) end,
      directories_frozen: fn marker -> validate_state_directories(marker, runtime, :frozen) end,
      restore_directories: fn marker -> restore_state_directories(marker, runtime) end
    }
  end

  if Mix.env() == :test do
    defp observe_kubernetes(marker, candidate, runtime) do
      observe_fresh_kubernetes(marker, candidate, runtime)
    end
  else
    defp observe_kubernetes(marker, candidate, _runtime) do
      observe_with_marker_contract(marker, candidate["kubernetes"]["cluster"])
    end
  end

  defp observe_fresh_kubernetes(marker, observation, runtime) do
    claim = marker_claim(marker)
    cluster = observation["kubernetes"]["cluster"]

    case Map.get(runtime.host_ops, :observe_kubernetes_for_test) do
      callback when is_function(callback, 2) -> callback.(claim, cluster)
      _ -> observe_with_marker_contract(marker, cluster)
    end
  end

  defp observe_with_marker_contract(%{"contractVersion" => version} = marker, cluster) when version in [@marker_version_v2, @marker_version_v3] do
    ConfirmedRecoveryKubernetes.observe_without_assignment_snapshot(marker_claim(marker), cluster)
  end

  defp observe_with_marker_contract(%{"contractVersion" => @marker_version} = marker, cluster) do
    ConfirmedRecoveryKubernetes.observe(marker_claim(marker), cluster)
  end

  defp observe_with_marker_contract(_marker, _cluster),
    do: {:error, :kubernetes_observation_unavailable}

  defp marker_claim(marker) do
    claim = Map.put(marker["expected"], "assignmentSHA256", marker["assignmentSHA256"])

    if marker["contractVersion"] in [@marker_version_v2, @marker_version_v3],
      do: Map.put(claim, "assignmentSnapshotState", "absent"),
      else: claim
  end

  defp read_candidate(issue_id, marker, runtime) do
    directory = marker_directory(issue_id, runtime)

    with {:ok, directory} <- signed_input_directory(directory, runtime),
         {:ok, observation_bytes} <- read_trusted_evidence(Path.join(directory, "candidate.json"), runtime),
         {:ok, proof_bytes} <- read_trusted_evidence(Path.join(directory, "confirmed-root-envelope.json"), runtime),
         true <- digest(observation_bytes) == marker["observationSHA256"],
         true <- digest(proof_bytes) == marker["proofSHA256"],
         {:ok, observation} when is_map(observation) <- decode_candidate_bytes(observation_bytes),
         true <- "sha256:" <> digest(observation_bytes) == marker["evidenceRef"] do
      {:ok, observation}
    else
      _ -> {:error, :confirmed_recovery_candidate_changed}
    end
  end

  defp verify_provider_final_proof(marker, runtime, require_current_journal?) do
    path = Path.join(runtime.host_ops.paths.provider_receipt_root, marker["issueId"] <> ".json")

    with {:ok, bytes} <- read_root_file(path, 1_048_576, runtime),
         {:ok, envelope} when is_map(envelope) <- Jason.decode(bytes),
         true <- Enum.sort(Map.keys(envelope)) == ["payload", "signature"],
         {:ok, payload_bytes} <- Base.url_decode64(envelope["payload"], padding: false),
         {:ok, signature} <- Base.url_decode64(envelope["signature"], padding: false),
         {:ok, public_key} <- host0(runtime, :read_public_key),
         true <- Jason.encode!(envelope) == bytes,
         true <- :crypto.verify(:eddsa, :none, payload_bytes, signature, [public_key, :ed25519]),
         {:ok, payload} when is_map(payload) <- Jason.decode(payload_bytes),
         true <- ConfirmedRecoveryProviderRelease.canonical_payload(payload) == payload_bytes,
         :ok <- validate_provider_final_payload(payload, marker, runtime, require_current_journal?),
         :ok <- verify_hgs719_operation(marker, payload, bytes, public_key, runtime) do
      {:ok, bytes, payload}
    else
      _ -> {:error, :provider_final_proof_invalid}
    end
  rescue
    _ -> {:error, :provider_final_proof_invalid}
  end

  defp validate_provider_final_payload(payload, marker, runtime, require_current_journal?) do
    with {:ok, journal_sha256} <- provider_journal_sha256(marker, runtime, require_current_journal?),
         :ok <- ConfirmedRecoveryProviderRelease.validate_final_payload(payload, marker, journal_sha256) do
      :ok
    else
      _ -> {:error, :provider_final_proof_invalid}
    end
  end

  defp provider_journal_sha256(marker, _runtime, false),
    do: {:ok, marker["postimages"]["claimJournal"]["sha256"]}

  defp provider_journal_sha256(marker, runtime, true) do
    read_state_file(runtime.journal_path, marker["stateOwnership"]["claimJournal"]["uid"], runtime)
    |> case do
      {:ok, bytes} -> {:ok, digest(bytes)}
      _ -> {:error, :provider_journal_unavailable}
    end
  end

  defp verify_hgs719_operation(marker, payload, final_envelope_bytes, public_key, runtime) do
    state_root = runtime.host_ops.paths.state_root

    directory =
      Path.join([
        state_root,
        "evidence",
        "claim-recovery-hgs719",
        marker["pool"],
        "hgs719-#{marker["pool"]}-#{ConfirmedRecoveryEvidence.tuple_digest(marker["expected"])}"
      ])

    with :ok <- exact_private_operation_directory(directory, runtime),
         {:ok, files} <- read_hgs719_operation_files(directory, runtime),
         :ok <-
           ConfirmedRecoveryProviderRelease.validate_operation(
             files,
             marker,
             payload,
             final_envelope_bytes,
             public_key
           ) do
      :ok
    else
      _ -> {:error, :provider_final_operation_mismatch}
    end
  rescue
    _ -> {:error, :provider_final_operation_mismatch}
  end

  defp read_hgs719_operation_files(directory, runtime) do
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
      with {:ok, bytes} <- read_private_operation_file(Path.join(directory, name), runtime),
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

  defp exact_keys?(value, keys) when is_map(value), do: Enum.sort(Map.keys(value)) == Enum.sort(keys)
  defp exact_keys?(_value, _keys), do: false

  defp exact_private_operation_directory(path, runtime) do
    root = Path.join([runtime.host_ops.paths.state_root, "evidence", "claim-recovery-hgs719"])

    with true <- String.starts_with?(path, root <> "/"),
         :ok <- trusted_root_directory(Path.dirname(root), runtime),
         :ok <- exact_private_directories(path, Path.dirname(root), runtime) do
      :ok
    else
      _ -> {:error, :untrusted_provider_operation_directory}
    end
  end

  defp exact_private_directories(path, base, runtime) do
    relative = Path.relative_to(path, base)

    Enum.reduce_while(Path.split(relative), base, fn part, parent ->
      current = Path.join(parent, part)

      case host(runtime, :lstat, [current]) do
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

  defp read_private_operation_file(path, runtime) do
    with {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1, size: size}} <- host(runtime, :lstat, [path]),
         true <- band(mode, 0o777) == 0o600 and size in 1..262_144,
         {:ok, bytes} <- host(runtime, :read, [path]),
         true <- byte_size(bytes) == size do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_provider_operation_file}
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
         :ok <- marker_snapshot_contract_in_bytes(marker, paths.journal.bytes),
         :ok <- ConfirmedRecoveryLineage.released_fence_lease(paths.fence.state, issue_id, expected),
         :ok <-
           ConfirmedRecoveryLineage.released_graph_lease(
             paths.graph.state,
             expected,
             marker["verificationNowMs"]
           ) do
      :ok
    else
      _ -> {:error, :local_release_invariants_changed}
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
           |> Map.put("completionCommittedAt", DateTime.utc_now() |> DateTime.to_iso8601()),
         true <- completed["completionPostimages"]["claimJournalSHA256"] == marker["postimages"]["claimJournal"]["sha256"],
         true <- completed["completionPostimages"]["fenceSHA256"] == marker["postimages"]["fence"]["sha256"],
         true <- completed["completionPostimages"]["responsibilityGraphSHA256"] == marker["postimages"]["responsibilityGraph"]["sha256"],
         :ok <- require_mutation_quiescent(runtime, marker["stateOwnership"]["claimJournal"]["uid"]),
         :ok <- release_state_invariants(marker, runtime),
         :ok <-
           ConfirmedRecoveryWAL.commit_then_release(
             fn -> durable_replace(path, Jason.encode!(completed), runtime) end,
             fn -> restore_state_directories(marker, runtime) end
           ) do
      :ok
    else
      _ -> {:error, :hgs740_completion_held_closed}
    end
  end

  defp state_hashes(runtime, ownership) do
    with {:ok, journal} <- read_state_file(runtime.journal_path, ownership["claimJournal"]["uid"], runtime),
         {:ok, fence} <- read_state_file(runtime.execution_fence_path, ownership["fence"]["uid"], runtime),
         {:ok, graph} <- read_state_file(runtime.responsibility_graph_path, ownership["responsibilityGraph"]["uid"], runtime) do
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
    case read_trusted_evidence(marker_path, runtime) do
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
         {:ok, ownership} <- state_ownership(runtime_state_paths(runtime), runtime),
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
         :ok <- persist_initial_marker(marker_path, marker, runtime) do
      {:ok, marker}
    end
  end

  defp persist_initial_marker(marker_path, marker, runtime) when is_map(marker) and is_map(runtime) do
    with :ok <- trusted_evidence_directory(Path.dirname(marker_path), runtime),
         do: durable_create(marker_path, Jason.encode!(marker), runtime)
  end

  defp persist_initial_marker(_marker_path, _marker, _runtime), do: {:error, :untrusted_hgs740_path}

  defp validate_resumable_marker(%{"status" => "applying"} = marker, runtime) do
    with :ok <- validate_current_pre_or_post(marker, runtime),
         {:ok, paths} <- paths_from_marker_preimages(marker, runtime),
         {:ok, observation, observation_bytes, proof_bytes, proof_payload} <- verify_saved_proof(marker, paths),
         :ok <- verify_local_claim(paths, proof_payload, runtime),
         {:ok, postimages} <- prepare_postimages(paths, proof_payload, runtime, marker["verificationNowMs"]),
         true <- postimages_match_marker?(postimages, marker),
         {:ok, _snapshot} <- observe_fresh_kubernetes(marker, observation, runtime),
         true <- digest(observation_bytes) == marker["observationSHA256"],
         true <- digest(proof_bytes) == marker["proofSHA256"] do
      :ok
    else
      _ -> {:error, :saved_hgs740_evidence_changed}
    end
  end

  defp validate_resumable_marker(%{"status" => "local_applied"} = marker, runtime) do
    with :ok <- verify_postimages(marker, runtime),
         {:ok, _observation} <- read_candidate(marker["issueId"], marker, runtime) do
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
         graph: %{path: runtime.responsibility_graph_path, bytes: graph_bytes, state: graph},
         runtime: runtime
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
    directory = marker_directory(marker["issueId"], paths.runtime)

    with {:ok, directory} <- signed_input_directory(directory, paths.runtime),
         {:ok, observation_bytes} <- read_trusted_evidence(Path.join(directory, "candidate.json"), paths.runtime),
         {:ok, proof_bytes} <- read_trusted_evidence(Path.join(directory, "confirmed-root-envelope.json"), paths.runtime),
         {:ok, observation} when is_map(observation) <- decode_candidate_bytes(observation_bytes),
         {:ok, payload_hint} <- decode_proof_payload(proof_bytes),
         bindings <- proof_bindings(payload_hint, marker["pool"], marker["issueId"], marker["nonce"], paths, marker["verificationNowMs"]),
         {:ok, payload} <- host(paths.runtime, :verify_signed_evidence, [proof_bytes, bindings]),
         true <- payload["observation"] == observation,
         true <- marker_version(payload) == marker["contractVersion"],
         true <- payload["assignmentSnapshotState"] == marker["assignmentSnapshotState"],
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

  @doc false
  @spec verify_signed_proof(String.t(), String.t(), String.t(), map()) ::
          {:ok, map(), binary(), binary(), map(), integer()} | {:error, :invalid_confirmed_recovery_evidence}
  def verify_signed_proof(issue_id, pool, nonce, paths) do
    runtime = paths.runtime
    directory = marker_directory(issue_id, runtime)

    with :ok <- trusted_evidence_directory(directory, runtime),
         {:ok, directory} <- signed_input_directory(directory, runtime),
         {:ok, observation_bytes} <- read_trusted_evidence(Path.join(directory, "candidate.json"), runtime),
         {:ok, proof_bytes} <- read_trusted_evidence(Path.join(directory, "confirmed-root-envelope.json"), runtime),
         {:ok, observation} when is_map(observation) <- decode_candidate_bytes(observation_bytes),
         {:ok, payload_hint} <- decode_proof_payload(proof_bytes),
         now_ms <- host0(runtime, :now_ms),
         bindings <- proof_bindings(payload_hint, pool, issue_id, nonce, paths, now_ms),
         {:ok, payload} <- host(runtime, :verify_signed_evidence, [proof_bytes, bindings]),
         true <- payload["observation"] == observation,
         true <- payload["observation"]["expected"] == observation["expected"] do
      {:ok, observation, observation_bytes, proof_bytes, payload, now_ms}
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  end

  defp signed_input_directory(directory, runtime) do
    ConfirmedRecoveryReconciliation.input_directory(directory, &host(runtime, :lstat, [&1]))
  end

  @doc "Checks the actual apply preconditions and computes postimages without signing or writing state."
  @spec preflight_transition(ConfirmedRecoveryContext.t(), map()) :: :ok | {:error, term()}
  def preflight_transition(%ConfirmedRecoveryContext{} = context, bundle) do
    runtime = Map.put(context.runtime, :host_ops, context.host_ops)
    now_ms = host0(runtime, :now_ms)

    payload =
      ConfirmedRecoveryIssuer.build_payload(
        bundle,
        context.pool,
        context.issue_id,
        context.nonce,
        now_ms
      )

    with :ok <- trusted_runtime_files(runtime, context.pool),
         {:ok, paths} <- read_state_preimages(runtime),
         :ok <- verify_local_claim(paths, payload, runtime),
         {:ok, _postimages} <- prepare_postimages(paths, payload, runtime, now_ms) do
      :ok
    end
  rescue
    _ -> {:error, :confirmed_claim_precondition_changed}
  end

  @doc false
  @spec proof_bindings(map(), String.t(), String.t(), String.t(), map(), integer()) :: map()
  def proof_bindings(payload, pool, issue_id, nonce, paths, now_ms) do
    bindings = %{
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

    if payload["contractVersion"] in ["work-package-paused-confirmed-recovery.v2", "work-package-paused-confirmed-recovery.v3"],
      do: Map.put(bindings, :assignment_snapshot_state, "absent"),
      else: bindings
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

  defp read_state_preimages(runtime) do
    with {:ok, journal_bytes} <- read_state_file(runtime.journal_path, nil, runtime),
         {:ok, fence_bytes} <- read_state_file(runtime.execution_fence_path, nil, runtime),
         {:ok, graph_bytes} <- read_state_file(runtime.responsibility_graph_path, nil, runtime),
         {:ok, journal} <- Journal.decode_bytes(journal_bytes),
         {:ok, fence} <- FencePersistence.decode_bytes(fence_bytes),
         {:ok, graph} <- GraphPersistence.decode_bytes(graph_bytes),
         {:ok, ownership} <- state_ownership(runtime_state_paths(runtime), runtime) do
      {:ok,
       %{
         journal: %{path: runtime.journal_path, bytes: journal_bytes, state: journal},
         fence: %{path: runtime.execution_fence_path, bytes: fence_bytes, state: fence},
         graph: %{path: runtime.responsibility_graph_path, bytes: graph_bytes, state: graph},
         ownership: ownership,
         runtime: runtime
       }}
    else
      _ -> {:error, :invalid_local_preimage}
    end
  end

  defp state_ownership(paths, runtime) do
    names = ["claimJournal", "fence", "responsibilityGraph"]

    with {:ok, files} <- collect_state_file_ownership(Enum.zip(names, paths), runtime),
         owner = files["claimJournal"]["uid"],
         true <- Enum.all?(files, fn {_name, record} -> record["uid"] == owner end),
         {:ok, directories} <- state_directory_identity(paths, owner, runtime) do
      {:ok, Map.put(files, "directories", directories)}
    else
      _ -> {:error, :untrusted_state_owner}
    end
  end

  defp collect_state_file_ownership(entries, runtime) do
    Enum.reduce_while(entries, {:ok, %{}}, fn {name, path}, {:ok, ownership} ->
      case state_file_owner_record(path, runtime) do
        {:ok, record} -> {:cont, {:ok, Map.put(ownership, name, record)}}
        error -> {:halt, error}
      end
    end)
  end

  defp state_file_owner_record(path, runtime) do
    with {:ok, %File.Stat{type: :regular, uid: uid, gid: gid, mode: mode, links: 1}} <- host(runtime, :lstat, [path]),
         true <- uid > 0 and gid >= 0 and band(mode, 0o077) == 0 do
      {:ok, %{"uid" => uid, "gid" => gid, "mode" => band(mode, 0o777)}}
    else
      _ -> {:error, :untrusted_state_owner}
    end
  end

  defp state_directory_identity(paths, owner, runtime) do
    paths
    |> Enum.flat_map(&(Path.dirname(&1) |> directory_ancestors(runtime.host_ops.paths.state_root, [])))
    |> Enum.uniq()
    |> Enum.reduce_while({:ok, %{}}, fn path, {:ok, identities} ->
      case host(runtime, :lstat_posix, [path]) do
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

  defp directory_ancestors(path, path, acc), do: [path | acc]
  defp directory_ancestors("/", _root, _acc), do: []
  defp directory_ancestors(path, root, acc), do: directory_ancestors(Path.dirname(path), root, [path | acc])

  defp validate_state_directories(marker, runtime, expected_state \\ :frozen) do
    ownership = marker["stateOwnership"]
    expected = ownership["directories"]
    owner = ownership["claimJournal"]["uid"]

    with true <- is_map(expected),
         {:ok, actual} <- state_directory_identity(runtime_state_paths(runtime), owner, runtime),
         true <- state_directories_match?(actual, expected, expected_state, runtime) do
      :ok
    else
      _ -> {:error, :state_directory_identity_changed}
    end
  end

  defp state_directories_match?(actual, expected, :original, _runtime), do: actual == expected

  defp state_directories_match?(actual, expected, :frozen, runtime) do
    writable_roots = transaction_writable_roots(runtime)

    state_directory_keys_match?(actual, expected) and
      Enum.all?(expected, fn {path, original} ->
        expected_record =
          if path in writable_roots do
            %{original | "uid" => 0, "gid" => 0, "mode" => 0o700}
          else
            original
          end

        actual[path] == expected_record
      end)
  end

  defp state_directories_match?(actual, expected, :freeze_preflight, runtime) do
    writable_roots = transaction_writable_roots(runtime)

    state_directory_keys_match?(actual, expected) and
      Enum.all?(expected, fn {path, original} ->
        if path in writable_roots do
          directory_transition_allowed?(actual[path], original, :freeze)
        else
          actual[path] == original
        end
      end)
  end

  defp state_directory_keys_match?(actual, expected) do
    actual |> Map.keys() |> Enum.sort() == expected |> Map.keys() |> Enum.sort()
  end

  defp transaction_writable_roots(runtime) do
    root = runtime.host_ops.paths.state_root
    [Path.join(root, "run"), Path.join(root, "workspaces")]
  end

  defp freeze_state_directories(marker, runtime) do
    ownership = marker["stateOwnership"]
    directories = ownership["directories"]
    owner = ownership["claimJournal"]["uid"]

    with :ok <- validate_freezable_directories(directories, runtime),
         :ok <- validate_state_directories_for_freeze(marker, runtime),
         :ok <- change_transaction_directories(directories, :freeze, runtime),
         :ok <- validate_state_directories(marker, runtime, :frozen),
         :ok <- host(runtime, :no_processes_for_uid, [owner]) do
      :ok
    else
      _ -> {:error, :transaction_state_directory_freeze_failed}
    end
  end

  defp restore_state_directories(marker, runtime) do
    directories = marker["stateOwnership"]["directories"]

    with :ok <- validate_state_directories(marker, runtime, :frozen),
         :ok <- change_transaction_directories(directories, :restore, runtime),
         :ok <- validate_state_directories(marker, runtime, :original) do
      :ok
    else
      _ -> {:error, :transaction_state_directory_restore_failed}
    end
  end

  defp validate_freezable_directories(directories, runtime) when is_map(directories) do
    if Enum.all?(transaction_writable_roots(runtime), &is_map_key(directories, &1)),
      do: :ok,
      else: {:error, :transaction_state_directory_missing}
  end

  defp validate_freezable_directories(_directories, _runtime), do: {:error, :transaction_state_directory_missing}

  defp validate_state_directories_for_freeze(marker, runtime) do
    ownership = marker["stateOwnership"]
    expected = ownership["directories"]
    owner = ownership["claimJournal"]["uid"]

    with true <- is_map(expected),
         {:ok, actual} <- state_directory_identity(runtime_state_paths(runtime), owner, runtime),
         true <- state_directories_match?(actual, expected, :freeze_preflight, runtime) do
      :ok
    else
      _ -> {:error, :state_directory_identity_changed}
    end
  end

  defp change_transaction_directories(directories, action, runtime) do
    Enum.reduce_while(transaction_writable_roots(runtime), :ok, fn path, :ok ->
      original = directories[path]

      case change_transaction_directory(path, original, action, runtime) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp change_transaction_directory(path, original, :freeze, runtime) do
    with {:ok, current} <- directory_identity(path, runtime),
         true <- directory_transition_allowed?(current, original, :freeze),
         :ok <- host(runtime, :change_owner, [path, 0, 0]),
         :ok <- host(runtime, :chmod, [path, 0o700]),
         {:ok, frozen} <- directory_identity(path, runtime),
         true <- frozen == %{original | "uid" => 0, "gid" => 0, "mode" => 0o700} do
      :ok
    else
      _ -> {:error, :transaction_state_directory_changed}
    end
  end

  defp change_transaction_directory(path, original, :restore, runtime) do
    with {:ok, current} <- directory_identity(path, runtime),
         true <- directory_transition_allowed?(current, original, :restore),
         :ok <- host(runtime, :change_owner, [path, original["uid"], original["gid"]]),
         :ok <- host(runtime, :chmod, [path, original["mode"]]),
         {:ok, restored} <- directory_identity(path, runtime),
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

  defp directory_identity(path, runtime) do
    case host(runtime, :lstat_posix, [path]) do
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

  defp restore_state_file_metadata(path, %{"uid" => uid, "gid" => gid, "mode" => mode}, runtime) do
    with true <- is_integer(uid) and uid > 0 and is_integer(gid) and gid >= 0,
         true <- is_integer(mode) and band(mode, 0o077) == 0,
         :ok <- host(runtime, :change_owner, [path, uid, gid]),
         :ok <- host(runtime, :chmod, [path, mode]),
         {:ok, %File.Stat{type: :regular, uid: ^uid, gid: ^gid, mode: actual_mode, links: 1}} <-
           host(runtime, :lstat, [path]),
         true <- band(actual_mode, 0o777) == mode do
      :ok
    else
      _ -> {:error, :state_file_ownership_restore_failed}
    end
  end

  defp restore_state_file_metadata(_path, _ownership, _runtime), do: {:error, :invalid_state_owner}

  @doc false
  @spec verify_local_claim(map(), map(), map()) :: :ok | {:error, :confirmed_claim_precondition_changed}
  def verify_local_claim(paths, payload, _runtime) do
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
         :ok <- local_snapshot_contract(paths, reservation, payload, expected),
         :ok <- predecessor_contract(paths, payload, expected),
         :ok <- fence_contract(paths, issue_id, expected, reservation, payload),
         :ok <- runtime_lease_contract(paths.graph.state, expected, payload),
         :ok <- require_no_local_workers(payload["observation"]) do
      :ok
    else
      _ -> {:error, :confirmed_claim_precondition_changed}
    end
  end

  defp local_snapshot_contract(
         paths,
         reservation,
         %{
           "contractVersion" => "work-package-paused-confirmed-recovery.v1",
           "assignmentSHA256" => assignment_sha,
           "issueId" => issue_id
         },
         expected
       )
       when is_binary(assignment_sha) do
    key = Journal.reservation_key(issue_id, expected["managedProjectProfileId"], expected["repositoryRef"], 2)

    with :present <- Journal.assignment_snapshot_state(paths.journal.bytes, key),
         :ok <- assignment_snapshot_matches?(reservation, assignment_sha) do
      :ok
    else
      _ -> {:error, :confirmed_claim_precondition_changed}
    end
  rescue
    _ -> {:error, :confirmed_claim_precondition_changed}
  end

  defp local_snapshot_contract(
         paths,
         reservation,
         %{
           "contractVersion" => version,
           "assignmentSnapshotState" => "absent",
           "assignmentSHA256" => nil,
           "issueId" => issue_id
         },
         expected
       )
       when version in ["work-package-paused-confirmed-recovery.v2", "work-package-paused-confirmed-recovery.v3"] do
    key = Journal.reservation_key(issue_id, expected["managedProjectProfileId"], expected["repositoryRef"], 2)

    with true <- is_nil(Map.get(reservation, :assignment_snapshot)),
         :absent <- Journal.assignment_snapshot_state(paths.journal.bytes, key) do
      :ok
    else
      _ -> {:error, :confirmed_claim_precondition_changed}
    end
  end

  defp local_snapshot_contract(_paths, _reservation, _payload, _expected),
    do: {:error, :confirmed_claim_precondition_changed}

  defp assignment_snapshot_matches?(reservation, assignment_sha) do
    with snapshot when is_binary(snapshot) <- Map.get(reservation, :assignment_snapshot),
         {:ok, assignment} <- ManagedAssignmentBundle.from_snapshot(snapshot),
         lease = assignment.lease,
         true <- assignment.sha256 == assignment_sha,
         true <- lease.issue_id == reservation.issue_id,
         true <- lease.repository == reservation.repository_ref,
         true <- lease.generation == reservation.generation,
         true <- lease.session_id == reservation.session_id,
         true <- lease.process_id == reservation.process_id do
      :ok
    else
      _ -> {:error, :confirmed_claim_precondition_changed}
    end
  rescue
    _ -> {:error, :confirmed_claim_precondition_changed}
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

  defp predecessor_contract(paths, %{"contractVersion" => "work-package-paused-confirmed-recovery.v3"} = payload, expected),
    do: ConfirmedRecoveryUnsubmittedPredecessor.persisted(paths, payload["observation"]["predecessorRetirement"], expected)

  defp predecessor_contract(paths, payload, expected),
    do: exact_local_predecessor(paths, payload["observation"]["predecessorRetirement"], expected)

  defp runtime_lease_contract(graph, expected, %{"contractVersion" => "work-package-paused-confirmed-recovery.v3"}),
    do: ConfirmedRecoveryUnsubmittedPredecessor.blocked_lease(graph, expected)

  defp runtime_lease_contract(graph, expected, _payload), do: exact_active_runtime_lease(graph, expected)

  defp fence_contract(paths, issue_id, expected, reservation, %{"contractVersion" => "work-package-paused-confirmed-recovery.v3"}) do
    with {:ok, candidate} <- ConfirmedRecoveryUnsubmittedPredecessor.reconciled_fence_candidate(paths.fence.state, reservation),
         do: exact_active_fence(candidate, issue_id, expected)
  end

  defp fence_contract(paths, issue_id, expected, _reservation, _payload),
    do: exact_active_fence(paths.fence.state, issue_id, expected)

  @doc false
  @spec exact_local_predecessor(map(), map(), map()) :: :ok | {:error, :predecessor_retirement_not_persisted}
  def exact_local_predecessor(
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

  def exact_local_predecessor(_paths, _predecessor, _current_claim),
    do: {:error, :predecessor_retirement_not_persisted}

  @doc false
  @spec exact_active_fence(map(), String.t(), map()) ::
          :ok | {:error, :execution_fence_mismatch | :execution_lease_mismatch}
  def exact_active_fence(fence, issue_id, expected) do
    case Map.get(fence.executions, issue_id) do
      %{
        generation: 2,
        status: :active,
        ownership: :reconciled,
        cleanup: :pending,
        terminal: nil,
        cleanup_receipt: nil,
        termination_unconfirmed: false,
        leases: leases
      } = execution
      when is_map(leases) ->
        lease = Map.get(leases, expected["sessionId"])

        if is_nil(Map.get(execution, :retirement)) and is_map(lease) and lease.process_id == expected["processId"] and lease.status == :active and
             lease.termination_required == false do
          :ok
        else
          {:error, :execution_lease_mismatch}
        end

      _ ->
        {:error, :execution_fence_mismatch}
    end
  end

  @doc false
  @spec exact_active_runtime_lease(map(), map()) :: :ok | {:error, :runtime_lease_mismatch}
  def exact_active_runtime_lease(graph, expected) do
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

  @doc false
  @spec require_no_local_workers(map()) :: :ok | {:error, :worker_quiescence_not_proven}
  def require_no_local_workers(observation) do
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
    case ConfirmedRecoveryClaimTransition.prepare_postimages(
           paths.journal.state,
           paths.fence.state,
           paths.graph.state,
           payload,
           now_ms
         ) do
      {:ok, postimages} ->
        {:ok,
         %{
           "claimJournal" => %{bytes: postimages.claimJournal, path: paths.journal.path},
           "fence" => %{bytes: postimages.fence, path: paths.fence.path},
           "responsibilityGraph" => %{bytes: postimages.responsibilityGraph, path: paths.graph.path},
           "reservationId" => postimages.reservationId
         }}

      {:error, _reason} ->
        {:error, :confirmed_claim_transition_rejected}
    end
  end

  @doc false
  @spec build_marker(map()) :: map()
  def build_marker(context) do
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

    marker = %{
      "contractVersion" => marker_version(proof_payload),
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

    if proof_payload["contractVersion"] in ["work-package-paused-confirmed-recovery.v2", "work-package-paused-confirmed-recovery.v3"],
      do: Map.put(marker, "assignmentSnapshotState", "absent"),
      else: marker
  end

  defp apply_marker(marker, marker_path, runtime) do
    case marker_images(marker) do
      {:ok, images} ->
        ConfirmedRecoveryStateMachine.apply(marker, images, %{
          freeze: fn -> freeze_state_directories(marker, runtime) end,
          verify_pre_or_post: fn -> validate_current_pre_or_post(marker, runtime) end,
          read_current: fn name -> read_marker_image(name, marker, runtime) end,
          persist: fn name, bytes, already_applied? ->
            persist_marker_image(name, bytes, already_applied?, marker, runtime)
          end,
          verify_postimages: fn -> verify_postimages(marker, runtime) end,
          mark_local: fn current_marker -> set_marker_applied(current_marker, marker_path, runtime) end
        })

      _ ->
        {:error, :hgs740_transaction_incomplete}
    end
  end

  defp validate_current_pre_or_post(marker, runtime) do
    current = [
      {"claimJournal", runtime.journal_path, "claimJournalSHA256"},
      {"fence", runtime.execution_fence_path, "fenceSHA256"},
      {"responsibilityGraph", runtime.responsibility_graph_path, "responsibilityGraphSHA256"}
    ]

    if Enum.all?(current, &current_image_valid?(&1, marker, runtime)) and
         marker_snapshot_contract_for_live_journal?(marker, runtime) and
         validate_state_directories(marker, runtime) == :ok do
      :ok
    else
      {:error, :transaction_preimage_changed}
    end
  end

  defp current_image_valid?({name, path, preimage_key}, marker, runtime) do
    case read_state_file(path, marker["stateOwnership"][name]["uid"], runtime) do
      {:ok, bytes} ->
        current_sha = digest(bytes)
        preimage_sha = marker["preimages"][preimage_key]
        postimage_sha = marker["postimages"][name]["sha256"]
        current_sha == preimage_sha or current_sha == postimage_sha

      _ ->
        false
    end
  end

  defp marker_images(marker) do
    marker_images(@state_names, marker, [])
  end

  defp marker_images([], _marker, images), do: {:ok, Enum.reverse(images)}

  defp marker_images([name | rest], marker, images) do
    case Base.url_decode64(marker["postimages"][name]["bytes"], padding: false) do
      {:ok, bytes} ->
        if digest(bytes) == marker["postimages"][name]["sha256"] do
          image = %{
            name: name,
            preimage_sha256: marker_preimage_for(marker, name),
            postimage_bytes: bytes
          }

          marker_images(rest, marker, [image | images])
        else
          {:error, :invalid_transaction_postimage}
        end

      _ ->
        {:error, :invalid_transaction_postimage}
    end
  end

  defp read_marker_image(name, marker, runtime) do
    {path, _decode, _save} = marker_image_adapter(name, runtime)

    case read_state_file(path, marker["stateOwnership"][name]["uid"], runtime) do
      {:ok, bytes} -> bytes
      {:error, _reason} = error -> error
    end
  end

  defp persist_marker_image(name, postimage_bytes, already_applied?, marker, runtime) do
    {path, decode, save} = marker_image_adapter(name, runtime)
    ownership = marker["stateOwnership"][name]

    case decode.(postimage_bytes) do
      {:ok, state} -> persist_state_image(already_applied?, path, state, ownership, marker, runtime, save)
      _ -> {:error, :invalid_transaction_postimage}
    end
  end

  defp marker_image_adapter("claimJournal", runtime),
    do: {runtime.journal_path, &Journal.decode_bytes/1, :claim_journal}

  defp marker_image_adapter("fence", runtime),
    do: {runtime.execution_fence_path, &FencePersistence.decode_bytes/1, :fence}

  defp marker_image_adapter("responsibilityGraph", runtime),
    do: {runtime.responsibility_graph_path, &GraphPersistence.decode_bytes/1, :responsibility_graph}

  defp persist_state_image(already_applied, path, state, ownership, marker, runtime, save) do
    uid = ownership["uid"]

    with :ok <- require_mutation_quiescent(runtime, uid),
         :ok <- validate_state_directories(marker, runtime),
         :ok <- if(already_applied, do: :ok, else: host(runtime, :save_state, [save, path, state])),
         :ok <- restore_state_file_metadata(path, ownership, runtime),
         :ok <- fsync_directory(Path.dirname(path), runtime),
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

    if validate_state_directories(marker, runtime) == :ok and
         Enum.all?(paths, &postimage_matches?(&1, marker, runtime)) and
         marker_snapshot_contract_for_live_journal?(marker, runtime) do
      :ok
    else
      {:error, :hgs740_postimage_mismatch}
    end
  end

  defp postimage_matches?({name, path}, marker, runtime) do
    case read_state_file(path, marker["stateOwnership"][name]["uid"], runtime) do
      {:ok, bytes} -> digest(bytes) == marker["postimages"][name]["sha256"]
      _ -> false
    end
  end

  defp marker_snapshot_contract_for_live_journal?(marker, runtime) do
    case read_state_file(runtime.journal_path, marker["stateOwnership"]["claimJournal"]["uid"], runtime) do
      {:ok, bytes} -> marker_snapshot_contract_in_bytes(marker, bytes) == :ok
      _ -> false
    end
  end

  defp marker_snapshot_contract_in_bytes(%{"contractVersion" => version} = marker, bytes) when version in [@marker_version_v2, @marker_version_v3] do
    expected = marker["expected"]
    key = Journal.reservation_key(marker["issueId"], expected["managedProjectProfileId"], expected["repositoryRef"], 2)

    case Journal.assignment_snapshot_state(bytes, key) do
      :absent -> :ok
      _ -> {:error, :assignment_snapshot_present}
    end
  end

  defp marker_snapshot_contract_in_bytes(
         %{
           "contractVersion" => @marker_version,
           "assignmentSHA256" => assignment_sha
         } = marker,
         bytes
       )
       when is_binary(assignment_sha) do
    expected = marker["expected"]
    key = Journal.reservation_key(marker["issueId"], expected["managedProjectProfileId"], expected["repositoryRef"], 2)

    with :present <- Journal.assignment_snapshot_state(bytes, key),
         {:ok, journal} <- Journal.decode_bytes(bytes),
         reservation when is_map(reservation) <- journal.reservations[key],
         true <- current_claim(journal, expected) == expected,
         :ok <- assignment_snapshot_matches?(reservation, assignment_sha) do
      :ok
    else
      _ -> {:error, :assignment_snapshot_changed}
    end
  rescue
    _ -> {:error, :assignment_snapshot_changed}
  end

  defp marker_snapshot_contract_in_bytes(_marker, _bytes), do: {:error, :invalid_recovery_contract}

  defp set_marker_applied(%{"status" => "local_applied"} = marker, _path, _runtime), do: {:ok, marker}

  defp set_marker_applied(marker, path, runtime) do
    applied_marker =
      marker
      |> Map.put("status", "local_applied")
      |> Map.put("completedAt", DateTime.utc_now() |> DateTime.to_iso8601())

    with :ok <- durable_replace(path, Jason.encode!(applied_marker), runtime), do: {:ok, applied_marker}
  end

  defp local_candidate_bytes(marker) do
    receipt_fields = %{
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
    }

    receipt_fields =
      if marker["contractVersion"] in [@marker_version_v2, @marker_version_v3],
        do: Map.put(receipt_fields, "assignmentSnapshotState", "absent"),
        else: receipt_fields

    {receipt_version, _fields, _domain} = receipt_contract(marker)
    expected = local_receipt_payload(Map.put(receipt_fields, "contractVersion", receipt_version))

    ConfirmedRecoveryEvidence.canonical_json(expected)
  end

  defp publish_local_candidate(marker, _marker_path, runtime) do
    path = Path.join(marker_directory(marker["issueId"], runtime), "local-transition-candidate.json")
    bytes = local_candidate_bytes(marker)

    case read_trusted_evidence(path, runtime) do
      {:ok, ^bytes} -> :ok
      {:error, :enoent} -> durable_create(path, bytes, runtime)
      _ -> {:error, :local_transition_candidate_conflict}
    end
  end

  defp marker_postimages_valid(marker) when is_map(marker) do
    marker_version = marker["contractVersion"]

    valid? =
      valid_marker_snapshot_contract?(marker) and
        ConfirmedRecoveryStateMachine.valid_marker_images?(
          marker,
          marker_version,
          @state_names,
          &valid_state_ownership?/1,
          &marker_preimage_for(marker, &1),
          &digest/1
        )

    if valid?, do: :ok, else: {:error, :invalid_hgs740_marker}
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
        %{original | "mode" => 0o700},
        %{original | "uid" => 0, "gid" => 0, "mode" => 0o700}
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
      is_binary(path) and (path == @state_root or String.starts_with?(path, @state_root <> "/")) and
        exact_keys?(record, ~w(majorDevice minorDevice inode uid gid mode)) and
        Enum.all?(record, fn {_key, value} -> is_integer(value) and value >= 0 end) and
        record["uid"] in [0, owner] and
        band(record["mode"], 0o022) == 0
    end)
  end

  defp valid_state_directory_records?(_directories, _owner), do: false

  defp validate_marker_identity(marker, issue_id, pool, nonce) do
    with true <- valid_marker_snapshot_contract?(marker),
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

  defp marker_version(%{"contractVersion" => "work-package-paused-confirmed-recovery.v3"}),
    do: @marker_version_v3

  defp marker_version(%{"contractVersion" => "work-package-paused-confirmed-recovery.v2"}),
    do: @marker_version_v2

  defp marker_version(%{"contractVersion" => "work-package-paused-confirmed-recovery.v1"}),
    do: @marker_version

  defp marker_version(_payload), do: nil

  defp valid_marker_snapshot_contract?(
         %{
           "contractVersion" => @marker_version,
           "assignmentSHA256" => assignment_sha
         } = marker
       )
       when is_binary(assignment_sha) do
    Regex.match?(~r/\A[0-9a-f]{64}\z/, assignment_sha) and
      not Map.has_key?(marker, "assignmentSnapshotState")
  end

  defp valid_marker_snapshot_contract?(%{
         "contractVersion" => version,
         "assignmentSnapshotState" => "absent",
         "assignmentSHA256" => nil
       })
       when version in [@marker_version_v2, @marker_version_v3],
       do: true

  defp valid_marker_snapshot_contract?(_marker), do: false

  defp verify_all_markers(runtime) do
    root = runtime.host_ops.paths.evidence_root

    case host(runtime, :lstat, [root]) do
      {:error, :enoent} ->
        :ok

      {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when band(mode, 0o777) == 0o700 ->
        with {:ok, issue_entries} <- host(runtime, :ls, [root]),
             true <- Enum.all?(issue_entries, &Regex.match?(~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, &1)),
             :ok <- verify_issue_markers(issue_entries, runtime) do
          :ok
        else
          _ -> {:error, :hgs740_startup_held_closed}
        end

      _ ->
        {:error, :hgs740_startup_held_closed}
    end
  end

  defp verify_issue_markers(issues, runtime) do
    Enum.reduce_while(issues, :ok, fn issue_id, :ok ->
      case verify_issue_marker(issue_id, runtime) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp verify_issue_marker(issue_id, runtime) do
    case read_trusted_evidence(marker_path(issue_id, runtime), runtime) do
      {:error, :enoent} -> no_marker_evidence_policy(issue_id, runtime)
      {:ok, bytes} -> verify_startup_marker(issue_id, bytes, runtime)
      _ -> {:error, :hgs740_startup_held_closed}
    end
  end

  defp no_marker_evidence_policy(issue_id, runtime) do
    directory = marker_directory(issue_id, runtime)

    case host(runtime, :lstat, [directory]) do
      {:error, :enoent} ->
        no_marker_startup_policy(issue_id, false)

      {:ok, %File.Stat{type: :directory}} ->
        with :ok <- trusted_evidence_directory(directory, runtime),
             {:ok, entries} <- host(runtime, :ls, [directory]) do
          no_marker_startup_policy(issue_id, entries != [])
        else
          _ -> {:error, :hgs740_startup_held_closed}
        end

      _ ->
        {:error, :hgs740_startup_held_closed}
    end
  end

  defp verify_startup_marker(issue_id, bytes, runtime) do
    with {:ok, marker} when is_map(marker) <- Jason.decode(bytes),
         :ok <- marker_postimages_valid(marker),
         true <- marker["issueId"] == issue_id,
         :ok <- trusted_evidence_directory(marker_directory(issue_id, runtime), runtime),
         :ok <- verify_signed_local_receipt(marker, issue_id, runtime),
         :ok <- verify_marker_status(marker, runtime) do
      :ok
    else
      _ -> {:error, :hgs740_startup_held_closed}
    end
  end

  defp verify_marker_status(%{"status" => "local_applied"}, _runtime), do: {:error, :hgs740_recovery_not_complete}

  defp verify_marker_status(%{"status" => "complete"} = marker, runtime), do: verify_completed_marker(marker, runtime)
  defp verify_marker_status(_marker, _runtime), do: {:error, :hgs740_startup_held_closed}

  defp verify_completed_marker(marker, runtime) do
    ConfirmedRecoveryStateMachine.verify_completed(marker, %{
      runtime: fn pool ->
        with {:ok, paths} <- host(runtime, :fixed_runtime_paths, [pool]) do
          {:ok, Map.put(paths, :host_ops, runtime.host_ops)}
        end
      end,
      provider_final: fn current_marker, runtime, require_current? ->
        verify_provider_final_proof(current_marker, runtime, require_current?)
      end,
      digest: &digest/1,
      valid_postconditions: &valid_completed_marker_postconditions/2,
      directories_original: fn current_marker, runtime ->
        validate_state_directories(current_marker, runtime, :original)
      end,
      release_invariants: fn current_marker, runtime -> release_state_invariants(current_marker, runtime) end
    })
  end

  defp valid_completed_marker_postconditions(marker, payload) do
    if ConfirmedRecoveryStateMachine.valid_completed_postconditions?(marker, payload, @pools),
      do: :ok,
      else: {:error, :hgs740_completion_marker_invalid}
  end

  defp verify_signed_local_receipt(marker, issue_id, runtime) do
    directory = marker_directory(issue_id, runtime)
    path = Path.join(directory, "local-transition-receipt.json")
    candidate_path = Path.join(directory, "local-transition-candidate.json")
    {receipt_version, receipt_fields, receipt_domain} = receipt_contract(marker)

    with {:ok, bytes} <- read_trusted_evidence(path, runtime),
         {:ok, candidate_bytes} <- read_trusted_evidence(candidate_path, runtime),
         {:ok, envelope} when is_map(envelope) <- Jason.decode(bytes),
         true <- Enum.sort(Map.keys(envelope)) == ["payload", "signature"],
         {:ok, payload_bytes} <- Base.url_decode64(envelope["payload"], padding: false),
         {:ok, signature} <- Base.url_decode64(envelope["signature"], padding: false),
         {:ok, public_key} <- host0(runtime, :read_public_key),
         true <- ConfirmedRecoveryEvidence.canonical_json(envelope) == bytes,
         true <- is_binary(receipt_domain),
         true <- :crypto.verify(:eddsa, :none, receipt_domain <> payload_bytes, signature, [public_key, :ed25519]),
         {:ok, payload} when is_map(payload) <- Jason.decode(payload_bytes),
         true <- ConfirmedRecoveryEvidence.canonical_json(payload) == payload_bytes,
         true <- payload_bytes == candidate_bytes,
         true <- Enum.sort(Map.keys(payload)) == Enum.sort(receipt_fields),
         true <- payload["contractVersion"] == receipt_version,
         true <- payload["assignmentSnapshotState"] == marker["assignmentSnapshotState"],
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

  defp trusted_runtime_files(runtime, pool) do
    with {:ok, expected} <- host(runtime, :fixed_runtime_paths, [pool]),
         true <- runtime_paths_match?(runtime, expected),
         :ok <- trusted_service_state_files(runtime_state_paths(runtime), runtime) do
      :ok
    else
      _ -> {:error, :untrusted_pool_state_path}
    end
  end

  defp runtime_state_paths(runtime) do
    [runtime.journal_path, runtime.execution_fence_path, runtime.responsibility_graph_path]
  end

  defp trusted_service_state_files(paths, runtime) do
    with stats when length(stats) == 3 <- Enum.map(paths, &host(runtime, :lstat, [&1])),
         true <-
           Enum.all?(
             stats,
             &match?({:ok, %File.Stat{type: :regular, links: 1, mode: mode}} when band(mode, 0o077) == 0, &1)
           ),
         [{:ok, %File.Stat{uid: owner}} | _] = stats,
         true <- owner > 0,
         true <- Enum.all?(stats, fn {:ok, stat} -> stat.uid == owner end),
         true <- Enum.all?(paths, &trusted_state_ancestors?(Path.dirname(&1), owner, runtime)) do
      :ok
    else
      _ -> {:error, :untrusted_pool_state_path}
    end
  end

  defp trusted_state_ancestors?(path, owner, runtime) do
    case host(runtime, :lstat, [path]) do
      {:ok, %File.Stat{type: :directory, uid: uid, mode: mode}}
      when uid in [0, owner] and band(mode, 0o022) == 0 ->
        parent = Path.dirname(path)
        parent == path or trusted_state_ancestors?(parent, owner, runtime)

      _ ->
        false
    end
  end

  defp read_state_file(path, allowed_owner, runtime) do
    with {:ok, before} <- state_file_snapshot(path, allowed_owner, runtime),
         {:ok, bytes} <- descriptor_read_state_file(path, before, runtime),
         true <- byte_size(bytes) <= 16_777_216,
         {:ok, after_read} <- state_file_snapshot(path, allowed_owner, runtime),
         true <- before == after_read do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_state_file}
    end
  end

  defp state_file_snapshot(path, allowed_owner, runtime) do
    with {:ok, stat} <- host(runtime, :lstat_posix, [path]),
         true <- stat.type == :regular and stat.links == 1 and stat.size in 1..16_777_216,
         owner = allowed_owner || stat.uid,
         true <- stat.uid in [owner, 0],
         true <- band(stat.mode, 0o077) == 0,
         {:ok, ancestors} <- state_directory_snapshots(Path.dirname(path), owner, [], runtime) do
      {:ok, {ancestors, Map.take(stat, @state_file_metadata)}}
    else
      _ -> {:error, :untrusted_state_file_metadata}
    end
  end

  defp state_directory_snapshots(path, state_owner, acc, runtime) do
    with {:ok, stat} <- host(runtime, :lstat_posix, [path]),
         true <- stat.type == :directory and stat.uid in [0, state_owner],
         true <- band(stat.mode, 0o022) == 0 do
      next = [{path, Map.take(stat, @state_file_metadata)} | acc]
      parent = Path.dirname(path)

      if parent == path do
        {:ok, next}
      else
        state_directory_snapshots(parent, state_owner, next, runtime)
      end
    else
      _ -> {:error, :untrusted_state_file_ancestor}
    end
  end

  defp descriptor_read_state_file(path, {_ancestors, expected_metadata}, runtime) do
    case host(runtime, :open, [path, [:read, :binary, :raw]]) do
      {:ok, io} ->
        result =
          with :ok <- descriptor_matches_state_file(io, expected_metadata, runtime),
               {:ok, bytes} <- host(runtime, :raw_read, [io, 16_777_217]),
               :ok <- descriptor_matches_state_file(io, expected_metadata, runtime) do
            {:ok, bytes}
          else
            _ -> {:error, :untrusted_state_file_descriptor}
          end

        case {result, host(runtime, :close, [io])} do
          {{:ok, bytes}, :ok} -> {:ok, bytes}
          _ -> {:error, :untrusted_state_file_descriptor}
        end

      _ ->
        {:error, :untrusted_state_file_descriptor}
    end
  end

  defp descriptor_matches_state_file(io, expected_metadata, runtime) do
    with {:ok, record} <- host(runtime, :read_file_info, [io, [time: :posix]]),
         true <- Map.take(File.Stat.from_record(record), @state_file_metadata) == expected_metadata do
      :ok
    else
      _ -> {:error, :untrusted_state_file_descriptor}
    end
  end

  defp read_root_file(path, max_bytes, runtime) do
    with :ok <- trusted_root_directory(Path.dirname(path), runtime),
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1, size: size}} <- host(runtime, :lstat, [path]),
         true <- band(mode, 0o022) == 0 and size <= max_bytes,
         {:ok, bytes} <- host(runtime, :read, [path]) do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_root_file}
    end
  end

  defp read_trusted_evidence(path, runtime) do
    with :ok <- trusted_evidence_directory(Path.dirname(path), runtime),
         {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1, size: size}} <-
           host(runtime, :lstat, [path]),
         true <- band(mode, 0o777) == 0o600 and size <= 16_777_216,
         {:ok, bytes} <- host(runtime, :read, [path]) do
      {:ok, bytes}
    else
      {:error, :enoent} -> {:error, :enoent}
      _ -> {:error, :untrusted_hgs740_evidence}
    end
  end

  defp trusted_evidence_directory(path, runtime) do
    root = runtime.host_ops.paths.evidence_root

    if path == root or String.starts_with?(path, root <> "/") do
      with :ok <- trusted_root_directory(Path.dirname(root), runtime),
           :ok <- exact_private_evidence_directories(path, Path.dirname(root), runtime) do
        :ok
      else
        _ -> {:error, :untrusted_hgs740_path}
      end
    else
      {:error, :untrusted_hgs740_path}
    end
  end

  defp exact_private_evidence_directories(path, base, runtime) do
    relative = Path.relative_to(path, base)

    Enum.reduce_while(Path.split(relative), base, fn part, parent ->
      current = Path.join(parent, part)

      case host(runtime, :lstat, [current]) do
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

  defp trusted_root_directory(path, runtime) do
    case host(runtime, :lstat, [path]) do
      {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when band(mode, 0o022) == 0 ->
        parent = Path.dirname(path)
        if parent == path, do: :ok, else: trusted_root_directory(parent, runtime)

      _ ->
        {:error, :untrusted_root_directory}
    end
  end

  defp durable_create(path, bytes, runtime) do
    with :ok <- exclusive_write_synced(path, bytes, runtime),
         do: fsync_directory(Path.dirname(path), runtime)
  end

  defp durable_replace(path, bytes, runtime) do
    temporary = path <> ".tmp-" <> Integer.to_string(host0(runtime, :unique_integer))

    result =
      with :ok <- exclusive_write_synced(temporary, bytes, runtime),
           :ok <- host(runtime, :rename, [temporary, path]),
           do: fsync_directory(Path.dirname(path), runtime)

    if result != :ok, do: host(runtime, :remove, [temporary])
    result
  end

  defp exclusive_write_synced(path, bytes, runtime) do
    case host(runtime, :raw_open, [path, [:write, :binary, :raw, :exclusive, :sync]]) do
      {:ok, file} ->
        try do
          with :ok <- host(runtime, :raw_write, [file, bytes]),
               :ok <- host(runtime, :raw_sync, [file]),
               do: host(runtime, :chmod, [path, 0o600])
        after
          host(runtime, :raw_close, [file])
        end

      {:error, reason} ->
        {:error, {:durable_write_failed, reason}}
    end
  end

  defp fsync_directory(path, runtime) do
    host(runtime, :sync_directory, [path])
  end

  defp marker_directory(issue_id, runtime),
    do: Path.join([runtime.host_ops.paths.evidence_root, issue_id, "generation-2"])

  defp marker_path(issue_id, runtime), do: Path.join(marker_directory(issue_id, runtime), "transaction.json")

  defp require_mutation_quiescent(runtime, uid),
    do: host(runtime, :require_mutation_quiescent, [runtime, uid])

  defp require_pool(pool) when pool in @pools, do: :ok
  defp require_pool(_pool), do: {:error, :invalid_pool}

  defp require_issue_id(issue_id) do
    if Regex.match?(~r/\A[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/, issue_id),
      do: :ok,
      else: {:error, :invalid_issue_id}
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
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
