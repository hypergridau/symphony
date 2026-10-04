defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionBundle do
  @moduledoc "Verifies the three original domains and both historical human records at durable confirmation time."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseProtocol, as: Protocol

  @bundle_keys ~w(binding authorization attestation receipt)
  @envelope_keys ~w(payload signature)
  @attestation_keys ~w(contractVersion authorizationSHA256 binding observedAt expiresAt checks readbacks)
  @readback_keys ~w(hostReadbackSHA256 providerReadbackSHA256 jobsReadbackSHA256 podsReadbackSHA256 nativeCustodySHA256)
  @provider_keys ~w(approvalId workspaceId companyId approvalState targetAction decisionMode approverType approverRef decisionActorType decisionActorRef supersededByApprovalId decidedAt expiresAt decisionPayload)
  @decision_keys ~w(protocolVersion bindingSHA256 sourceHeads attemptId allowedActions nativeDecisionId)
  @hex64 ~r/\A[0-9a-f]{64}\z/
  @auth_domain "hypergrid-work-package-recovery:hgs740-release-authorization.v1\0"
  @att_domain "hypergrid-work-package-recovery:hgs740-release-attestation.v1\0"
  @receipt_domain "hypergrid-work-package-recovery:hgs740-local-transition-receipt.v3\0"

  @spec verify(map(), map(), binary(), map(), map(), String.t(), binary(), integer()) :: {:ok, map()} | {:error, atom()}
  def verify(bundle, binding, candidate_bytes, local, provider, owner, public32, confirmed_ms) do
    with true <- closed?(bundle, @bundle_keys),
         true <- bundle["binding"] == binding,
         true <- is_binary(public32) and byte_size(public32) == 32,
         true <- is_binary(candidate_bytes) and is_integer(confirmed_ms),
         true <- hash(candidate_bytes) == binding["localCandidateSHA256"],
         {:ok, expected_auth} <- Protocol.authorize(local, binding, owner, confirmed_ms),
         {:ok, _auth_bytes, auth} <- signed(bundle["authorization"], @auth_domain, public32),
         true <- auth == expected_auth,
         {:ok, _att_bytes, att} <- signed(bundle["attestation"], @att_domain, public32),
         {:ok, observed_ms} <- valid_attestation(att, binding, auth, bundle["authorization"], confirmed_ms),
         {:ok, receipt_bytes, receipt} <- signed(bundle["receipt"], @receipt_domain, public32),
         true <- receipt_bytes == candidate_bytes,
         {:ok, completed_ms} <- iso_ms(receipt["completedAt"]),
         true <- completed_ms <= observed_ms,
         true <- valid_provider?(provider, local, binding, auth, owner, confirmed_ms) do
      {:ok, %{authorization: auth, attestation: att, receipt: receipt}}
    else
      _ -> invalid()
    end
  rescue
    _ -> invalid()
  catch
    _, _ -> invalid()
  end

  defp signed(raw, domain, public32) when is_binary(raw) and byte_size(raw) <= 262_144 do
    with {:ok, envelope} <- Jason.decode(raw),
         true <- closed?(envelope, @envelope_keys),
         true <- Evidence.canonical_json(envelope) == raw,
         {:ok, payload_bytes} <- unb64(envelope["payload"]),
         {:ok, signature} <- unb64(envelope["signature"]),
         true <- byte_size(signature) == 64,
         {:ok, payload} <- Jason.decode(payload_bytes),
         true <- is_map(payload) and Evidence.canonical_json(payload) == payload_bytes,
         true <- :crypto.verify(:eddsa, :none, domain <> payload_bytes, signature, [public32, :ed25519]) do
      {:ok, payload_bytes, payload}
    else
      _ -> :error
    end
  end

  defp signed(_, _, _), do: :error

  defp valid_attestation(att, binding, auth, auth_raw, confirmed_ms) do
    checks = %{
      "globalPause" => true,
      "unitsMaskedAndQuiescent" => true,
      "nativeCustodyVerified" => true,
      "localPostimagesVerified" => true,
      "workerCount" => 0,
      "jobCount" => 0,
      "podCount" => 0,
      "credentialLeaseCount" => 0,
      "oauthSlotLeaseCount" => 0,
      "completeInventories" => true,
      "providerState" => "claimed_held"
    }

    with true <- closed?(att, @attestation_keys),
         true <- att["contractVersion"] == "hgs740-release-attestation.v1",
         true <- att["authorizationSHA256"] == hash(auth_raw),
         true <- att["binding"] == binding and att["checks"] === checks,
         true <- closed?(att["readbacks"], @readback_keys),
         true <- Enum.all?(Map.values(att["readbacks"]), &(is_binary(&1) and Regex.match?(@hex64, &1))),
         {:ok, issued} <- iso_ms(auth["issuedAt"]),
         {:ok, auth_expiry} <- iso_ms(auth["expiresAt"]),
         {:ok, observed} <- iso_ms(att["observedAt"]),
         {:ok, expiry} <- iso_ms(att["expiresAt"]),
         true <- observed >= issued,
         true <- expiry > observed and expiry - observed <= 60_000 and expiry <= auth_expiry,
         true <- observed - 5_000 <= confirmed_ms and confirmed_ms < expiry,
         true <- confirmed_ms - observed <= 60_000 do
      {:ok, observed}
    else
      _ -> :error
    end
  end

  defp valid_provider?(p, local, binding, auth, owner, confirmed_ms) do
    decision = p["decisionPayload"]

    with true <- closed?(p, @provider_keys),
         true <- is_binary(p["approvalId"]) and byte_size(p["approvalId"]) in 1..256,
         true <- p["approvalId"] != local["approvalId"],
         true <- p["workspaceId"] == binding["expected"]["workspaceId"],
         true <- p["companyId"] == binding["expected"]["companyId"],
         true <- p["approvalState"] == "approved" and p["targetAction"] == "hgs740_release_provider",
         true <- p["decisionMode"] == "human" and p["approverType"] == "user" and p["decisionActorType"] == "user",
         true <- p["approverRef"] == owner and p["decisionActorRef"] == owner and is_nil(p["supersededByApprovalId"]),
         true <- p["expiresAt"] == local["expiresAt"],
         {:ok, decided} <- iso_ms(p["decidedAt"]),
         {:ok, expiry} <- iso_ms(p["expiresAt"]),
         true <- decided <= confirmed_ms and confirmed_ms < expiry,
         true <- closed?(decision, @decision_keys),
         true <- decision["protocolVersion"] == "hgs740-release-only.v1",
         true <- decision["bindingSHA256"] == hash(Evidence.canonical_json(binding)),
         true <- decision["sourceHeads"] == binding["sourceHeads"] and decision["attemptId"] == auth["attemptId"],
         true <- decision["allowedActions"] == ["prepare_provider_release", "confirm_provider_release"],
         true <- decision["nativeDecisionId"] == local["approvalId"] do
      true
    else
      _ -> false
    end
  end

  defp unb64(s) when is_binary(s) do
    with {:ok, bytes} <- Base.url_decode64(s, padding: false),
         true <- Base.url_encode64(bytes, padding: false) == s do
      {:ok, bytes}
    end
  end

  defp unb64(_), do: :error

  defp iso_ms(s) when is_binary(s) do
    with {:ok, dt, 0} <- DateTime.from_iso8601(s), do: {:ok, DateTime.to_unix(dt, :millisecond)}
  end

  defp iso_ms(_), do: :error
  defp closed?(map, keys), do: is_map(map) and Enum.sort(Map.keys(map)) == Enum.sort(keys)
  defp hash(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp invalid, do: {:error, :hgs740_release_completion_invalid}
end
