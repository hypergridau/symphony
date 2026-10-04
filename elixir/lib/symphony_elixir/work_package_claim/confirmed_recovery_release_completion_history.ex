defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionHistory do
  @moduledoc "Historical enrollment is evidence at confirmedAt, never a new write admission."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence

  @review_fields ~w(contractVersion reviewedAt sourceHeads bindingSHA256 nativeAcceptedBuildReceiptSHA256 providerArtifacts)
  @enrollment_fields ~w(contractVersion acceptedProtocol acceptedAt expiresAt installedReviewSHA256 ownerUserId ownerPrincipal nativeFingerprint publicKey binding)
  @artifacts ~w(index.js bootstrap/provider-core-app.js work-package-projection/hgs740-release.contract.js work-package-projection/hgs740-release.service.js work-package-projection/hgs740-release.persistence.js work-package-projection/hgs742-approval.contract.js work-package-projection/hgs742-approval.service.js work-package-projection/hgs742-admission-files.js work-package-projection/hgs742-accepted-runtime.js routes/provider/register-provider-routes.js routes/provider/hgs740-release.routes.js routes/experience/claim-approvals.routes.js routes/experience/approvals.routes.js routes/experience/claim-review-page.routes.js routes/experience/claim-review-page.origin.js experience-adapters/security/experience-security-headers.js routes/experience/register-experience-routes.js)
  @spki <<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>>

  @spec verify(binary(), binary(), map(), binary()) :: {:ok, map()} | {:error, atom()}
  def verify(review_bytes, enrollment_bytes, readback, public_key) do
    with {:ok, review} <- Jason.decode(review_bytes),
         {:ok, enrollment} <- Jason.decode(enrollment_bytes),
         true <- exact?(review, @review_fields) and exact?(enrollment, @enrollment_fields),
         true <- review["contractVersion"] == "hgs740-installed-admission-review.v1",
         true <- enrollment["contractVersion"] == "hgs740-accepted-owner-enrollment.v1",
         true <- enrollment["acceptedProtocol"] == "hgs740-release-only.v1",
         true <- hash(review_bytes) == readback["installedReviewSHA256"],
         true <- hash(enrollment_bytes) == readback["acceptedEnrollmentSHA256"],
         true <- enrollment["installedReviewSHA256"] == hash(review_bytes),
         true <- enrollment["binding"] == readback["binding"],
         true <- review["sourceHeads"] == enrollment["binding"]["sourceHeads"],
         true <- review["bindingSHA256"] == hash(Evidence.canonical_json(enrollment["binding"])),
         true <- digest?(review["nativeAcceptedBuildReceiptSHA256"]),
         true <- exact?(review["providerArtifacts"], @artifacts),
         true <- Enum.all?(Map.values(review["providerArtifacts"]), &digest?/1),
         true <- public_key_matches?(enrollment, public_key),
         true <- text?(enrollment["ownerUserId"]) and email?(enrollment["ownerPrincipal"]),
         {:ok, reviewed} <- time(review["reviewedAt"]),
         {:ok, accepted} <- time(enrollment["acceptedAt"]),
         {:ok, expiry} <- time(enrollment["expiresAt"]),
         {:ok, confirmed} <- time(readback["confirmation"]["confirmedAt"]),
         true <- reviewed <= accepted and accepted <= confirmed and confirmed < expiry,
         true <- (expiry - accepted) in 1..86_400_000 do
      {:ok, enrollment}
    else
      _ -> {:error, :hgs740_release_history_invalid}
    end
  rescue
    _ -> {:error, :hgs740_release_history_invalid}
  end

  defp public_key_matches?(enrollment, key) do
    der = @spki <> key

    byte_size(key) == 32 and enrollment["nativeFingerprint"] == hash(Base.encode64(der)) and
      :public_key.pem_decode(enrollment["publicKey"]) == [{:SubjectPublicKeyInfo, der, :not_encrypted}]
  end

  defp email?(v), do: is_binary(v) and byte_size(v) <= 256 and Regex.match?(~r/\A[^\s@]+@[^\s@]+\.[^\s@]+\z/, v)
  defp text?(v), do: is_binary(v) and byte_size(v) in 1..256
  defp digest?(v), do: is_binary(v) and Regex.match?(~r/\A[0-9a-f]{64}\z/, v)
  defp exact?(v, fields), do: is_map(v) and Enum.sort(Map.keys(v)) == Enum.sort(fields)
  defp hash(v), do: :crypto.hash(:sha256, v) |> Base.encode16(case: :lower)

  defp time(v) when is_binary(v) do
    with {:ok, dt, 0} <- DateTime.from_iso8601(v), do: {:ok, DateTime.to_unix(dt, :millisecond)}
  end

  defp time(_), do: :error
end
