defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseCompletionHost do
  @moduledoc "Fixed protected lookup hints and historical files. These ports issue no release or signatures."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliationHost, as: Custody
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseRuntime, as: Runtime
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseTransport, as: Transport

  @intent "/srv/dahlia-runner-state/evidence/hgs740-approved-release-20261004-v1/release-dispatch-intent.json"
  @history "/srv/dahlia-runner-state/identity/claim-witness/accepted-owner-enrollments/v1"

  @spec readback(map(), map()) :: {:ok, map()} | {:error, atom()}
  def readback(marker, bundle) do
    with {:ok, bytes} <- Custody.read_private(@intent, 262_144),
         {:ok, hint} <- Jason.decode(bytes),
         true <- exact?(hint, ~w(attemptId binding dispatchedAt localApprovalId providerApprovalId)),
         true <- hint["binding"] == bundle["binding"] and marker["expected"] == bundle["binding"]["expected"],
         {:ok, envelope} <- Jason.decode(bundle["authorization"]),
         {:ok, auth_bytes} <- Base.url_decode64(envelope["payload"], padding: false),
         {:ok, auth} <- Jason.decode(auth_bytes),
         true <- hint["attemptId"] == auth["attemptId"] and hint["localApprovalId"] == auth["decisionId"],
         true <- is_binary(hint["providerApprovalId"]) and byte_size(hint["providerApprovalId"]) in 1..256,
         {:ok, identity} <- Runtime.confirmation(marker),
         {:ok, readback} <- Transport.confirmed_readback(bundle, hint["providerApprovalId"], identity),
         true <- readback["providerDecision"]["approvalId"] == hint["providerApprovalId"],
         {:ok, ^bytes} <- Custody.read_private(@intent, 262_144) do
      {:ok, readback}
    else
      _ -> {:error, :hgs740_confirmation_readback_held_closed}
    end
  rescue
    _ -> {:error, :hgs740_confirmation_readback_held_closed}
  end

  @spec history(map()) :: {:ok, {binary(), binary()}} | {:error, atom()}
  def history(readback) do
    digest = readback["acceptedEnrollmentSHA256"]

    with true <- is_binary(digest) and Regex.match?(~r/\A[0-9a-f]{64}\z/, digest),
         root = Path.join(@history, digest),
         {:ok, review} <- Custody.read_private(Path.join(root, "installed-source-review.json"), 65_536),
         {:ok, enrollment} <- Custody.read_private(Path.join(root, "accepted-enrollment.json"), 65_536) do
      {:ok, {review, enrollment}}
    else
      _ -> {:error, :hgs740_confirmation_history_held_closed}
    end
  rescue
    _ -> {:error, :hgs740_confirmation_history_held_closed}
  end

  defp exact?(v, fields), do: is_map(v) and Enum.sort(Map.keys(v)) == Enum.sort(fields)
end
