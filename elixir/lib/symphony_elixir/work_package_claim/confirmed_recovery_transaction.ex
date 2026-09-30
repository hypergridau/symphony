defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransaction do
  @moduledoc """
  Public root entrypoints for the HGS-740 confirmed recovery transaction.

  Transaction decisions live in `ConfirmedRecoveryCore`; Linux host authority and
  side effects are supplied by `ConfirmedRecoveryRootHost`.
  """

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryCore
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuance

  @type result :: {:ok, :applied | :already_applied} | {:error, term()}

  @doc "Applies or resumes the exact signed confirmed-claim transition as root."
  @spec apply(String.t(), String.t(), String.t(), String.t()) :: result()
  def apply(issue_id, pool, workflow_path, nonce),
    do: ConfirmedRecoveryCore.apply(issue_id, pool, workflow_path, nonce)

  @doc "Issues immutable HGS-740 evidence from a root-owned observation bundle while paused."
  @spec issue(String.t(), String.t(), String.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def issue(issue_id, pool, workflow_path, nonce, bundle_path),
    do: ConfirmedRecoveryIssuance.issue(issue_id, pool, workflow_path, nonce, bundle_path)

  @doc "Completes a locally applied recovery after exact provider release and fresh no-Job/no-Pod readback."
  @spec complete(String.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def complete(issue_id, pool, workflow_path),
    do: ConfirmedRecoveryCore.complete(issue_id, pool, workflow_path)

  @doc "Verifies all durable HGS-740 markers before a pool service starts."
  @spec verify_startup(String.t(), String.t()) :: :ok | {:error, term()}
  def verify_startup(workflow_path, pool),
    do: ConfirmedRecoveryCore.verify_startup(workflow_path, pool)

  @doc false
  @spec marker_directory(String.t()) :: Path.t()
  def marker_directory(issue_id), do: ConfirmedRecoveryCore.marker_directory(issue_id)

  @doc false
  @spec no_marker_startup_policy(String.t(), boolean()) :: :ok | {:error, :hgs740_transaction_marker_missing}
  def no_marker_startup_policy(issue_id, evidence_directory_exists),
    do: ConfirmedRecoveryCore.no_marker_startup_policy(issue_id, evidence_directory_exists)
end
