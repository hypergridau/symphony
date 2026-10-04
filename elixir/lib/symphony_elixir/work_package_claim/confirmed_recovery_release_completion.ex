defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletion do
  @moduledoc "Append-only completion evidence for an already committed release, used by canonical completion and startup."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionBundle, as: Bundle
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionHistory, as: History
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionProof, as: Proof
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseProtocol, as: Protocol

  @witness "release-completion-witness.json"
  @release_names ~w(release-only-bundle.json release-only-attempt.json release-only-attestation.json)
  @completion_fields ~w(providerFinalProofSHA256 providerJournalSHA256 completionObservation completionPostimages providerReceipt completionCommittedAt)

  @spec verify(map(), map(), boolean(), map()) :: :legacy | {:ok, binary(), map()} | {:error, atom()}
  def verify(marker, runtime, require_current?, ports) do
    root = runtime.host_ops.marker_directory.(marker["issueId"])
    reads = Map.new([@witness | @release_names], &{&1, ports.read.(Path.join(root, &1))})

    if legacy?(reads, marker),
      do: :legacy,
      else: verify_release(marker, runtime, require_current?, root, reads, ports)
  rescue
    _ -> {:error, :hgs740_release_completion_held_closed}
  catch
    _, _ -> {:error, :hgs740_release_completion_held_closed}
  end

  @spec require_fresh_commit(binary(), integer()) :: :ok | {:error, atom()}
  def require_fresh_commit(bytes, now) do
    case Jason.decode(bytes) do
      {:ok, %{"contractVersion" => "hgs740-release-completion-witness.v1", "providerReadback" => readback}} ->
        if Proof.fresh?(readback, now), do: :ok, else: {:error, :hgs740_completion_readback_stale}

      {:ok, %{"payload" => _, "signature" => _}} ->
        :ok

      _ ->
        {:error, :hgs740_completion_readback_invalid}
    end
  rescue
    _ -> {:error, :hgs740_completion_readback_invalid}
  end

  defp legacy?(reads, marker) do
    Enum.all?(Map.values(reads), &(&1 == {:error, :enoent})) and
      not String.starts_with?(get_in(marker, ["providerReceipt", "recoveryId"]) || "", "hgs740-release:")
  end

  defp verify_release(marker, runtime, current?, root, reads, ports) do
    with {:ok, bundle_bytes} <- reads["release-only-bundle.json"],
         {:ok, bundle} <- canonical(bundle_bytes),
         {:ok, retained} <- retained_witness(reads[@witness], marker, root, ports),
         {:ok, original_bytes} <- original_marker(retained, marker, root, ports),
         {:ok, snapshot} <- snapshot(original_bytes, root, runtime, ports),
         {:ok, receipt_bytes} <- ports.read.(Path.join(root, "local-transition-receipt.json")),
         true <- receipt_bytes == bundle["receipt"],
         {:ok, binding} <- Protocol.binding(snapshot, bundle["binding"]["sourceHeads"]),
         true <- binding == bundle["binding"],
         {:ok, readback} <- readback(retained, marker, bundle, runtime, current?),
         {:ok, payload} <- validate(readback, bundle, snapshot, reads, runtime),
         :ok <- journal_matches(marker, runtime, payload, current?, ports),
         witness = %{"contractVersion" => "hgs740-release-completion-witness.v1", "originalMarker" => Base.url_encode64(original_bytes, padding: false), "providerReadback" => readback},
         {:ok, bytes} <- retain_witness(retained, witness, root, ports) do
      {:ok, bytes, payload}
    else
      _ -> {:error, :hgs740_release_completion_held_closed}
    end
  end

  defp retained_witness({:error, :enoent}, %{"status" => "local_applied"}, _root, _ports), do: {:ok, nil}

  defp retained_witness({:ok, bytes}, _marker, _root, _ports) do
    with {:ok, witness} <- canonical(bytes),
         true <- exact?(witness, ~w(contractVersion originalMarker providerReadback)),
         true <- witness["contractVersion"] == "hgs740-release-completion-witness.v1" do
      {:ok, %{bytes: bytes, value: witness}}
    end
  end

  defp retained_witness(_, _, _, _), do: {:error, :invalid_completion_witness}

  defp original_marker(nil, marker, root, ports) do
    with {:ok, bytes} <- ports.read.(Path.join(root, "transaction.json")), {:ok, ^marker} <- Jason.decode(bytes), do: {:ok, bytes}
  end

  defp original_marker(retained, marker, _root, _ports) do
    encoded = retained.value["originalMarker"]

    with {:ok, bytes} <- Base.url_decode64(encoded, padding: false),
         true <- Base.url_encode64(bytes, padding: false) == encoded,
         {:ok, original} <- Jason.decode(bytes),
         true <- original["status"] == "local_applied",
         true <- if(marker["status"] == "complete", do: Map.put(Map.drop(marker, @completion_fields), "status", "local_applied") == original, else: marker == original) do
      {:ok, bytes}
    end
  end

  defp snapshot(bytes, root, runtime, ports) do
    with {:ok, marker} <- Jason.decode(bytes),
         {:ok, inputs} <- ports.inputs.(root),
         {:ok, candidate} <- ports.read.(Path.join(root, "local-transition-candidate.json")),
         {:ok, proof} <- ports.read.(Path.join(inputs, "confirmed-root-envelope.json")),
         {:ok, observation} <- ports.read.(Path.join(inputs, "candidate.json")),
         {:ok, manifest} <- ports.read.(Path.join(root, "reconciliation/epoch-5/manifest.json")),
         images <- Map.new(marker["postimages"], fn {name, row} -> {name, Base.url_decode64!(row["bytes"], padding: false)} end),
         true <- runtime.pool_key == marker["pool"] do
      value = %{marker_bytes: bytes, candidate_bytes: candidate, proof_bytes: proof}
      {:ok, Map.merge(value, %{observation_bytes: observation, manifest_bytes: manifest, committed_images: images})}
    end
  end

  defp readback(retained, _marker, _bundle, _runtime, false) when not is_nil(retained),
    do: {:ok, retained.value["providerReadback"]}

  defp readback(retained, marker, bundle, runtime, true) do
    with {:ok, fresh} <- runtime.host_ops.read_confirmed_release.(marker, bundle),
         true <- Proof.fresh?(fresh, runtime.host_ops.now_ms.()),
         true <- is_nil(retained) or Proof.fresh?(retained.value["providerReadback"], runtime.host_ops.now_ms.()),
         true <- is_nil(retained) or Map.drop(fresh, ["observedAt"]) == Map.drop(retained.value["providerReadback"], ["observedAt"]) do
      {:ok, if(is_nil(retained), do: fresh, else: retained.value["providerReadback"])}
    end
  end

  defp readback(_, _, _, _, _), do: {:error, :completion_witness_required}

  defp validate(readback, bundle, snapshot, reads, runtime) do
    with {:ok, public} <- runtime.host_ops.read_public_key.(),
         {:ok, {review, enrollment_bytes} = files} <- runtime.host_ops.read_release_history.(readback),
         {:ok, enrollment} <- History.verify(review, enrollment_bytes, readback, public),
         {:ok, confirmed} <- time(readback["confirmation"]["confirmedAt"]),
         {:ok, signed} <- Bundle.verify(bundle, bundle["binding"], snapshot.candidate_bytes, readback["localDecision"], readback["providerDecision"], enrollment["ownerPrincipal"], public, confirmed),
         {:ok, attempt_bytes} <- reads["release-only-attempt.json"],
         true <- attempt_bytes == Evidence.canonical_json(signed.authorization),
         {:ok, attestation_bytes} <- reads["release-only-attestation.json"],
         true <- attestation_bytes == bundle["attestation"],
         {:ok, payload} <- Proof.verify(readback, bundle, signed, enrollment["nativeFingerprint"]),
         {:ok, ^files} <- runtime.host_ops.read_release_history.(readback) do
      {:ok, payload}
    end
  end

  defp journal_matches(marker, runtime, payload, current?, ports) do
    with {:ok, hash} <- ports.journal.(marker, runtime, current?), true <- hash == payload["journalSHA256"], do: :ok
  end

  defp retain_witness(nil, witness, root, ports) do
    bytes = Evidence.canonical_json(witness)

    with true <- byte_size(bytes) <= 1_048_576, :ok <- ports.create.(Path.join(root, @witness), bytes), {:ok, ^bytes} <- ports.read.(Path.join(root, @witness)), do: {:ok, bytes}
  end

  defp retain_witness(retained, witness, _root, _ports) do
    if retained.value == witness, do: {:ok, retained.bytes}, else: {:error, :completion_witness_changed}
  end

  defp canonical(bytes) do
    with {:ok, value} <- Jason.decode(bytes), true <- bytes == Evidence.canonical_json(value), do: {:ok, value}
  end

  defp exact?(v, fields), do: is_map(v) and Enum.sort(Map.keys(v)) == Enum.sort(fields)

  defp time(v) when is_binary(v) do
    with {:ok, dt, 0} <- DateTime.from_iso8601(v), do: {:ok, DateTime.to_unix(dt, :millisecond)}
  end

  defp time(_), do: :error
end
