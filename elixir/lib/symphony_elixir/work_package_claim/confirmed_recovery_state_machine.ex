defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryStateMachine do
  @moduledoc false

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWAL

  @spec no_marker_startup_policy(String.t(), String.t(), boolean()) ::
          :ok | {:error, :hgs740_transaction_marker_missing}
  def no_marker_startup_policy(issue_id, protected_issue_id, evidence_directory_exists)
      when is_binary(issue_id) and is_binary(protected_issue_id) and is_boolean(evidence_directory_exists) do
    if issue_id == protected_issue_id or evidence_directory_exists do
      {:error, :hgs740_transaction_marker_missing}
    else
      :ok
    end
  end

  def no_marker_startup_policy(_issue_id, _protected_issue_id, _evidence_directory_exists),
    do: {:error, :hgs740_transaction_marker_missing}

  @spec valid_completed_postconditions?(map(), map(), [String.t()]) :: boolean()
  def valid_completed_postconditions?(marker, payload, pools)
      when is_map(marker) and is_map(payload) and is_list(pools) do
    receipt = payload["receipt"]

    valid_completed_identity?(marker, pools) and valid_release_receipt?(marker, payload, receipt) and
      valid_completion_hashes?(marker, payload)
  rescue
    _ -> false
  end

  def valid_completed_postconditions?(_marker, _payload, _pools), do: false

  @spec valid_marker_images?(
          map(),
          String.t(),
          [String.t()],
          (term() -> boolean()),
          (String.t() -> String.t()),
          (binary() -> String.t())
        ) :: boolean()
  def valid_marker_images?(marker, version, state_names, ownership_valid?, preimage_key, digest)
      when is_map(marker) and is_binary(version) and is_list(state_names) and
             is_function(ownership_valid?, 1) and is_function(preimage_key, 1) and is_function(digest, 1) do
    postimages = marker["postimages"]
    preimage_images = marker["preimageImages"]
    ownership = marker["stateOwnership"]

    valid_marker_header?(marker, version, postimages, preimage_images, ownership, ownership_valid?) and
      valid_postimage_entries?(postimages, state_names, digest) and
      valid_preimage_entries?(preimage_images, state_names, preimage_key, digest)
  rescue
    _ -> false
  end

  def valid_marker_images?(_marker, _version, _state_names, _ownership_valid?, _preimage_key, _digest), do: false

  @spec apply(map(), [map()], map()) :: {:ok, map()} | {:error, :hgs740_transaction_incomplete}
  def apply(marker, images, operations) when is_map(marker) and is_list(images) and is_map(operations) do
    with true <- marker["status"] in ["applying", "local_applied"],
         :ok <- operations.freeze.(),
         :ok <- operations.verify_pre_or_post.(),
         :ok <- ConfirmedRecoveryWAL.apply_images(images, operations.read_current, operations.persist),
         :ok <- operations.verify_postimages.(),
         {:ok, applied_marker} <- operations.mark_local.(marker) do
      {:ok, applied_marker}
    else
      _ -> {:error, :hgs740_transaction_incomplete}
    end
  end

  def apply(_marker, _images, _operations), do: {:error, :hgs740_transaction_incomplete}

  @spec complete(map(), String.t(), String.t(), map()) :: {:ok, :complete} | {:error, :hgs740_completion_held_closed}
  def complete(marker, issue_id, pool, operations)
      when is_map(marker) and is_binary(issue_id) and is_binary(pool) and is_map(operations) do
    case marker["status"] do
      "local_applied" -> complete_local_applied(marker, issue_id, pool, operations)
      "complete" -> complete_marker_replay(marker, issue_id, pool, operations)
      _ -> {:error, :hgs740_completion_held_closed}
    end
  end

  def complete(_marker, _issue_id, _pool, _operations), do: {:error, :hgs740_completion_held_closed}

  @spec verify_completed(map(), map()) :: :ok | {:error, :hgs740_startup_held_closed}
  def verify_completed(marker, operations) when is_map(marker) and is_map(operations) do
    with {:ok, runtime} <- operations.runtime.(marker["pool"]),
         {:ok, proof, payload} <- operations.provider_final.(marker, runtime, false),
         true <- operations.digest.(proof) == marker["providerFinalProofSHA256"],
         true <- payload["journalSHA256"] == marker["providerJournalSHA256"],
         :ok <- operations.valid_postconditions.(marker, payload),
         :ok <- operations.directories_original.(marker, runtime),
         :ok <- operations.release_invariants.(marker, runtime) do
      :ok
    else
      _ -> {:error, :hgs740_startup_held_closed}
    end
  end

  def verify_completed(_marker, _operations), do: {:error, :hgs740_startup_held_closed}

  defp complete_local_applied(marker, issue_id, pool, operations) do
    with :ok <- operations.verify_receipt.(marker, issue_id),
         :ok <- operations.verify_postimages.(marker),
         {:ok, proof, payload} <- operations.provider_final.(marker, true),
         {:ok, candidate} <- operations.candidate.(issue_id, marker),
         {:ok, observation} <- operations.observe.(marker, candidate),
         :ok <- operations.final_release_invariants.(marker, payload),
         :ok <- operations.mutation_quiescent.(marker),
         :ok <- operations.service_stopped.(pool),
         :ok <- operations.write_complete.(marker, proof, payload, observation) do
      {:ok, :complete}
    else
      _ -> {:error, :hgs740_completion_held_closed}
    end
  end

  defp complete_marker_replay(marker, issue_id, pool, operations) do
    with :ok <- operations.verify_receipt.(marker, issue_id),
         {:ok, proof, payload} <- operations.provider_final.(marker, false),
         true <- operations.digest.(proof) == marker["providerFinalProofSHA256"],
         true <- payload["journalSHA256"] == marker["providerJournalSHA256"],
         :ok <- operations.valid_postconditions.(marker, payload),
         :ok <- operations.release_invariants.(marker),
         {:ok, candidate} <- operations.candidate.(issue_id, marker),
         {:ok, _observation} <- operations.observe.(marker, candidate),
         :ok <- operations.mutation_quiescent.(marker),
         :ok <- operations.service_stopped.(pool),
         :ok <- operations.directories_frozen.(marker),
         :ok <- operations.restore_directories.(marker) do
      {:ok, :complete}
    else
      _ -> {:error, :hgs740_completion_held_closed}
    end
  end

  defp expected_postimage_hashes(marker) do
    %{
      "claimJournalSHA256" => marker["postimages"]["claimJournal"]["sha256"],
      "fenceSHA256" => marker["postimages"]["fence"]["sha256"],
      "responsibilityGraphSHA256" => marker["postimages"]["responsibilityGraph"]["sha256"]
    }
  end

  defp valid_completed_identity?(marker, pools) do
    marker["status"] == "complete" and marker["generation"] == 2 and marker["pool"] in pools
  end

  defp valid_release_receipt?(marker, payload, receipt) do
    payload["localGenerationMax"] == 2 and payload["neverSpawned"] == true and is_map(receipt) and
      receipt["nextGenerationFloor"] == 3 and receipt == marker["providerReceipt"]
  end

  defp valid_completion_hashes?(marker, payload) do
    payload["journalSHA256"] == marker["providerJournalSHA256"] and
      marker["completionPostimages"] == expected_postimage_hashes(marker)
  end

  defp valid_marker_header?(marker, version, postimages, preimage_images, ownership, ownership_valid?) do
    marker["contractVersion"] == version and marker["status"] in ["applying", "local_applied", "complete"] and
      is_map(postimages) and is_map(preimage_images) and ownership_valid?.(ownership)
  end

  defp valid_postimage_entries?(postimages, state_names, digest) do
    Enum.all?(state_names, fn name ->
      entry = postimages[name]

      is_map(entry) and is_binary(entry["bytes"]) and
        digest.(Base.url_decode64!(entry["bytes"], padding: false)) == entry["sha256"]
    end)
  end

  defp valid_preimage_entries?(preimage_images, state_names, preimage_key, digest) do
    Enum.all?(state_names, fn name ->
      image = preimage_images[name]

      is_binary(image) and digest.(Base.url_decode64!(image, padding: false)) == preimage_key.(name)
    end)
  end
end
