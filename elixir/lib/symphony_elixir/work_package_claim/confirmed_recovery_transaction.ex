defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransaction do
  @moduledoc """
  Public root entrypoints for the HGS-740 confirmed recovery transaction.

  Transaction decisions live in `ConfirmedRecoveryCore`; Linux host authority and
  side effects are supplied by `ConfirmedRecoveryRootHost`.
  """

  alias SymphonyElixir.WorkPackageClaim.{ConfirmedRecoveryCore, ConfirmedRecoveryRootHost}

  @type result :: {:ok, :applied | :already_applied} | {:error, term()}

  @doc "Applies or resumes the exact signed confirmed-claim transition as root."
  @spec apply(String.t(), String.t(), String.t(), String.t()) :: result()
  def apply(issue_id, pool, workflow_path, nonce),
    do: apply_request(issue_id, pool, workflow_path, nonce)

  defp apply_request(issue_id, pool, workflow_path, nonce)
       when is_binary(issue_id) and is_binary(pool) and is_binary(workflow_path) and is_binary(nonce) do
    with {:ok, context} <- ConfirmedRecoveryRootHost.authorize_apply(issue_id, pool, workflow_path, nonce),
         {:ok, {:ok, result}} <-
           ConfirmedRecoveryRootHost.with_pool_lock(context, fn -> ConfirmedRecoveryCore.apply(context) end) do
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

  defp apply_request(_issue_id, _pool, _workflow_path, _nonce),
    do: {:error, :invalid_confirmed_recovery_request}

  @doc "Completes a locally applied recovery after exact provider release and fresh no-Job/no-Pod readback."
  @spec complete(String.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def complete(issue_id, pool, workflow_path),
    do: complete_request(issue_id, pool, workflow_path)

  defp complete_request(issue_id, pool, workflow_path)
       when is_binary(issue_id) and is_binary(pool) and
              is_binary(workflow_path) do
    with {:ok, context} <- ConfirmedRecoveryRootHost.authorize_completion(issue_id, pool, workflow_path),
         {:ok, :ok} <-
           ConfirmedRecoveryRootHost.with_pool_lock(context, fn -> ConfirmedRecoveryCore.complete(context) end) do
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

  defp complete_request(_issue_id, _pool, _workflow_path),
    do: {:error, :invalid_hgs740_completion_request}

  @doc "Verifies all durable HGS-740 markers before a pool service starts."
  @spec verify_startup(String.t(), String.t()) :: :ok | {:error, term()}
  def verify_startup(workflow_path, pool),
    do: verify_startup_request(workflow_path, pool)

  defp verify_startup_request(workflow_path, pool) when is_binary(workflow_path) and is_binary(pool) do
    with {:ok, context} <- ConfirmedRecoveryRootHost.authorize_startup(workflow_path, pool),
         :ok <- ConfirmedRecoveryCore.verify_startup(context) do
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

  defp verify_startup_request(_workflow_path, _pool),
    do: {:error, :invalid_hgs740_startup_request}

  @doc false
  @spec marker_directory(String.t()) :: Path.t()
  def marker_directory(issue_id), do: ConfirmedRecoveryCore.marker_directory(issue_id)

  @doc false
  @spec no_marker_startup_policy(String.t(), boolean()) :: :ok | {:error, :hgs740_transaction_marker_missing}
  def no_marker_startup_policy(issue_id, evidence_directory_exists),
    do: ConfirmedRecoveryCore.no_marker_startup_policy(issue_id, evidence_directory_exists)
end
