defmodule SymphonyElixir.RKE2Job.Provider do
  @moduledoc """
  Source-only create/get/delete reconciliation for exact assignment Jobs.

  Callers must pass a bundle built from an upstream signature-verified manifest entry.
  `JobSpec` validates bundle integrity and placement before any client call; this provider
  does not verify manifest signatures or connect to Kubernetes itself.
  """

  alias SymphonyElixir.RKE2Job.JobSpec

  @type result :: {:ok, map()} | {:held, term()} | {:error, term()}

  @doc "Idempotently creates or verifies the Job for a trusted assignment bundle."
  @spec ensure(map(), keyword()) :: result()
  def ensure(assignment, opts) when is_map(assignment) and is_list(opts) do
    with {:ok, client, context} <- ports(opts),
         {:ok, config} <- config(opts),
         {:ok, expected} <- JobSpec.compile(assignment, config) do
      ensure_job(client, expected, context)
    end
  end

  def ensure(_assignment, _opts), do: {:error, :invalid_rke2_job_request}

  @doc "Deletes only a previously read, exact matching Job using its Kubernetes UID."
  @spec delete(map(), keyword()) :: :ok | {:held, term()} | {:error, term()}
  def delete(assignment, opts) when is_map(assignment) and is_list(opts) do
    with {:ok, client, context} <- ports(opts),
         {:ok, config} <- config(opts),
         {:ok, expected} <- JobSpec.compile(assignment, config) do
      delete_job(client, expected, context)
    end
  end

  def delete(_assignment, _opts), do: {:error, :invalid_rke2_job_request}

  defp ensure_job(client, expected, context) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])

    case client.create_job(namespace, expected, context) do
      {:ok, created} -> verify_owned(created, expected)
      {:error, :already_exists} -> get_existing(client, namespace, name, expected, context)
      {:error, reason} -> {:error, {:job_create_failed, reason}}
      _ -> {:held, :invalid_job_create_response}
    end
  end

  defp get_existing(client, namespace, name, expected, context) do
    case client.get_job(namespace, name, context) do
      {:ok, existing} -> verify_owned(existing, expected)
      {:error, :not_found} -> {:held, :job_create_race_unresolved}
      {:error, reason} -> {:held, {:job_read_failed, reason}}
      _ -> {:held, :invalid_job_read_response}
    end
  end

  defp verify_owned(job, expected) do
    if JobSpec.owned_job?(job, expected), do: {:ok, job}, else: {:held, :job_identity_or_spec_mismatch}
  end

  defp delete_job(client, expected, context) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])

    case client.get_job(namespace, name, context) do
      {:error, :not_found} -> {:held, :job_not_found_for_delete}
      {:error, reason} -> {:held, {:job_read_failed, reason}}
      {:ok, job} -> delete_verified(client, job, namespace, name, expected, context)
      _ -> {:held, :invalid_job_read_response}
    end
  end

  defp delete_verified(client, job, namespace, name, expected, context) do
    uid = get_in(job, ["metadata", "uid"])

    cond do
      not JobSpec.owned_job?(job, expected) ->
        {:held, :job_identity_or_spec_mismatch}

      not is_binary(uid) or String.trim(uid) == "" ->
        {:held, :job_uid_missing}

      true ->
        case client.delete_job(namespace, name, uid, context) do
          :ok -> :ok
          {:error, reason} -> {:held, {:job_delete_failed, reason}}
          _ -> {:held, :invalid_job_delete_response}
        end
    end
  end

  defp ports(opts) do
    with client when is_atom(client) <- Keyword.get(opts, :client),
         true <- Code.ensure_loaded?(client),
         true <- function_exported?(client, :create_job, 3),
         true <- function_exported?(client, :get_job, 3),
         true <- function_exported?(client, :delete_job, 4) do
      {:ok, client, Keyword.get(opts, :client_context)}
    else
      _ -> {:error, :rke2_job_client_missing}
    end
  end

  defp config(opts) do
    case Keyword.get(opts, :config) do
      %{namespace: _, image: _} = config -> {:ok, config}
      _ -> {:error, :rke2_job_trusted_config_missing}
    end
  end
end
