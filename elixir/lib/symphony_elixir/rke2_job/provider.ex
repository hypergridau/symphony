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
      delete_job(client, expected, context, nil, :missing_job_is_held)
    end
  end

  def delete(_assignment, _opts), do: {:error, :invalid_rke2_job_request}

  @doc "Deletes only the exact owned Job allocation recorded by its server UID."
  @spec delete_owned(map(), String.t(), keyword()) :: :ok | {:held, term()} | {:error, term()}
  def delete_owned(assignment, uid, opts)
      when is_map(assignment) and is_binary(uid) and byte_size(uid) > 0 and is_list(opts) do
    with {:ok, client, context} <- ports(opts),
         {:ok, config} <- config(opts),
         {:ok, expected} <- JobSpec.compile(assignment, config) do
      delete_job(client, expected, context, uid, :already_absent_is_clean)
    end
  end

  def delete_owned(_assignment, _uid, _opts), do: {:error, :invalid_rke2_job_request}

  defp ensure_job(client, expected, context) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])

    case client.create_job(namespace, expected, context) do
      {:ok, created} -> verify_owned(created, expected)
      {:error, :already_exists} -> get_existing(client, namespace, name, expected, context)
      {:error, reason} -> reconcile_uncertain_create(client, namespace, name, expected, context, reason)
      _ -> reconcile_uncertain_create(client, namespace, name, expected, context, :invalid_create_response)
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

  defp reconcile_uncertain_create(client, namespace, name, expected, context, reason) do
    case client.get_job(namespace, name, context) do
      {:ok, existing} -> verify_owned(existing, expected)
      {:error, :not_found} -> {:held, {:job_create_outcome_uncertain, reason}}
      {:error, read_reason} -> {:held, {:job_create_and_read_uncertain, reason, read_reason}}
      _ -> {:held, {:job_create_and_read_uncertain, reason, :invalid_read_response}}
    end
  end

  defp verify_owned(job, expected) do
    if JobSpec.owned_job?(job, expected), do: {:ok, job}, else: {:held, :job_identity_or_spec_mismatch}
  end

  defp delete_job(client, expected, context, expected_uid, missing_job) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])

    case client.get_job(namespace, name, context) do
      {:error, :not_found} ->
        case missing_job do
          :already_absent_is_clean -> :ok
          :missing_job_is_held -> {:held, :job_not_found_for_delete}
        end

      {:error, reason} ->
        {:held, {:job_read_failed, reason}}

      {:ok, job} ->
        delete_verified(client, job, namespace, name, expected, context, expected_uid)

      _ ->
        {:held, :invalid_job_read_response}
    end
  end

  defp delete_verified(client, job, namespace, name, expected, context, expected_uid) do
    case verified_delete_uid(job, expected, expected_uid) do
      {:ok, uid} -> delete_job_request(client, namespace, name, uid, expected, context)
      {:error, reason} -> {:held, reason}
    end
  end

  defp verified_delete_uid(job, expected, expected_uid) do
    uid = get_in(job, ["metadata", "uid"])

    cond do
      not JobSpec.owned_job_for_cleanup?(job, expected) -> {:error, :job_identity_or_spec_mismatch}
      not is_binary(uid) or String.trim(uid) == "" -> {:error, :job_uid_missing}
      not is_nil(expected_uid) and uid != expected_uid -> {:error, :job_allocation_identity_mismatch}
      true -> {:ok, uid}
    end
  end

  defp delete_job_request(client, namespace, name, uid, expected, context) do
    case client.delete_job(namespace, name, uid, context) do
      :ok -> :ok
      {:error, reason} -> reconcile_delete(client, namespace, name, uid, expected, context, reason)
      _ -> {:held, :invalid_job_delete_response}
    end
  end

  defp reconcile_delete(client, namespace, name, uid, expected, context, reason) do
    case client.get_job(namespace, name, context) do
      {:error, :not_found} ->
        :ok

      {:ok, job} ->
        cond do
          not JobSpec.owned_job_for_cleanup?(job, expected) ->
            {:held, :job_identity_or_spec_mismatch}

          get_in(job, ["metadata", "uid"]) != uid ->
            {:held, :job_allocation_identity_mismatch}

          true ->
            {:held, {:job_delete_outcome_uncertain, reason}}
        end

      {:error, read_reason} ->
        {:held, {:job_delete_and_read_uncertain, reason, read_reason}}

      _ ->
        {:held, {:job_delete_and_read_uncertain, reason, :invalid_read_response}}
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
