defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuer do
  @moduledoc """
  Builds and signs the canonical HGS-740 proof envelope from a reviewed root input bundle.

  This module owns serialization, domain separation, validation and verifier roundtrip. The
  caller supplies trusted root observations and signing/verification callbacks; it performs no
  host or provider I/O itself.
  """

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence

  @contract_v1 "work-package-paused-confirmed-recovery.v1"
  @contract_v2 "work-package-paused-confirmed-recovery.v2"
  @bundle_fields_v1 ~w(assignmentSHA256 observation providerHeld reservationId)
  @bundle_fields_v2 ~w(assignmentSHA256 assignmentSnapshotState observation providerHeld reservationId)

  @type result :: {:ok, map(), binary(), binary()} | {:error, :invalid_confirmed_recovery_evidence}

  @doc "Validates a canonical issuer bundle, signs the proof and verifies it before returning bytes."
  @spec issue(
          binary(),
          String.t(),
          String.t(),
          String.t(),
          map(),
          (binary() -> binary()),
          (binary(), map() -> {:ok, map()} | {:error, term()})
        ) :: result()
  def issue(bundle_bytes, pool, issue_id, nonce, bindings, sign, verify)
      when is_binary(bundle_bytes) and is_binary(pool) and is_binary(issue_id) and is_binary(nonce) and
             is_map(bindings) and is_function(sign, 1) and is_function(verify, 2) do
    with {:ok, bundle} <- decode_bundle(bundle_bytes),
         payload <- build_payload(bundle, pool, issue_id, nonce, bindings.now_ms),
         :ok <- Evidence.validate_payload(payload, bindings),
         payload_bytes <- Evidence.canonical_json(payload),
         message when is_binary(message) <- Evidence.signature_message(payload_bytes, payload["contractVersion"]),
         {:ok, signature} <- sign_bytes(sign, message),
         true <- byte_size(signature) == 64,
         envelope_bytes <-
           Evidence.canonical_json(%{
             "payload" => Base.url_encode64(payload_bytes, padding: false),
             "signature" => Base.url_encode64(signature, padding: false)
           }),
         {:ok, ^payload} <- verify.(envelope_bytes, bindings) do
      {:ok, payload, payload_bytes, envelope_bytes}
    else
      _ -> {:error, :invalid_confirmed_recovery_evidence}
    end
  rescue
    _ -> {:error, :invalid_confirmed_recovery_evidence}
  catch
    _kind, _reason -> {:error, :invalid_confirmed_recovery_evidence}
  end

  def issue(_bundle, _pool, _issue_id, _nonce, _bindings, _sign, _verify),
    do: {:error, :invalid_confirmed_recovery_evidence}

  @doc false
  @spec bindings(map(), String.t(), String.t(), String.t(), non_neg_integer()) :: map()
  def bindings(bundle, pool, issue_id, nonce, now_ms) when is_map(bundle) do
    observation = bundle["observation"]

    common = %{
      pool: pool,
      issue_id: issue_id,
      generation: 2,
      reservation_id: bundle["reservationId"],
      assignment_sha256: bundle["assignmentSHA256"],
      nonce: nonce,
      fence_sha256: observation["fenceSHA256"],
      claim_journal_sha256: observation["claimJournalSHA256"],
      responsibility_graph_sha256: observation["responsibilityGraphSHA256"],
      now_ms: now_ms
    }

    if bundle["assignmentSnapshotState"] == "absent",
      do: Map.put(common, :assignment_snapshot_state, "absent"),
      else: common
  end

  def bindings(_bundle, _pool, _issue_id, _nonce, now_ms), do: %{now_ms: now_ms}

  @doc false
  @spec build_payload(map(), String.t(), String.t(), String.t(), integer()) :: map()
  def build_payload(bundle, pool, issue_id, nonce, now_ms) when is_map(bundle) and is_integer(now_ms) do
    issued_at = DateTime.from_unix!(now_ms, :millisecond) |> DateTime.to_iso8601()
    expires_at = DateTime.from_unix!(now_ms + 30_000, :millisecond) |> DateTime.to_iso8601()

    payload = %{
      "contractVersion" => if(bundle["assignmentSnapshotState"] == "absent", do: @contract_v2, else: @contract_v1),
      "pool" => pool,
      "issueId" => issue_id,
      "generation" => 2,
      "reservationId" => bundle["reservationId"],
      "assignmentSHA256" => bundle["assignmentSHA256"],
      "issuedAt" => issued_at,
      "expiresAt" => expires_at,
      "nonce" => nonce,
      "observation" => bundle["observation"],
      "providerHeld" => bundle["providerHeld"]
    }

    if bundle["assignmentSnapshotState"] == "absent",
      do: Map.put(payload, "assignmentSnapshotState", "absent"),
      else: payload
  end

  def build_payload(_bundle, _pool, _issue_id, _nonce, _now_ms), do: %{}

  defp decode_bundle(bytes) do
    with {:ok, bundle} when is_map(bundle) <- Jason.decode(bytes),
         true <- Evidence.canonical_json(bundle) == bytes,
         true <- valid_bundle_shape?(bundle) do
      {:ok, bundle}
    else
      _ -> {:error, :invalid_bundle}
    end
  end

  defp valid_bundle_shape?(%{"assignmentSnapshotState" => "absent", "assignmentSHA256" => nil} = bundle),
    do: Enum.sort(Map.keys(bundle)) == Enum.sort(@bundle_fields_v2)

  defp valid_bundle_shape?(%{"assignmentSnapshotState" => _state}), do: false

  defp valid_bundle_shape?(bundle),
    do: Enum.sort(Map.keys(bundle)) == Enum.sort(@bundle_fields_v1) and is_binary(bundle["assignmentSHA256"])

  defp sign_bytes(sign, message) do
    case sign.(message) do
      {:ok, signature} when is_binary(signature) -> {:ok, signature}
      signature when is_binary(signature) -> {:ok, signature}
      _ -> {:error, :signing_failed}
    end
  end
end
