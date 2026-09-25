defmodule SymphonyElixir.RKE2Job.ManagedExecutorAdapter do
  @moduledoc """
  Allocation-only bridge from an assignment bundle to the RKE2 Job provider.

  This module deliberately does not implement the full ManagedExecutor adapter
  behavior. Credential leasing, checkout, execution, result publication, and
  signed cleanup remain separate lifecycle ports. The caller supplies trusted
  provider configuration, a fakeable Kubernetes client, and a client-context
  provider; this module does not load credentials or contact a cluster by itself.
  Jobs stay suspended until a later lifecycle slice defines and authorizes an
  explicit activation boundary.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{HTTPClient, JobSpec, Provider}

  @allocation_version 1

  @type allocation :: %{id: String.t(), status: :ready}
  @type result :: {:ok, allocation()} | {:held, term()} | {:error, term()}

  @doc "Creates or reconciles the exact deterministic Job for an allocation key."
  @spec allocate_or_reconcile(map(), String.t(), term()) :: result()
  def allocate_or_reconcile(assignment, idempotency_key, context) do
    with :ok <- validate_assignment(assignment),
         :ok <- validate_key(idempotency_key, assignment, :allocation),
         {:ok, ports} <- ports(context),
         {:ok, expected} <- JobSpec.compile(assignment, ports.config),
         {:ok, client_context} <- client_context(ports, assignment, :allocate, idempotency_key),
         {:ok, job} <- Provider.ensure(assignment, provider_opts(ports, client_context)),
         {:ok, id} <- allocation_id(expected, job) do
      {:ok, %{id: id, status: :ready}}
    else
      {:held, reason} -> {:held, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Deletes only the exact Job UID recorded in a prior allocation."
  @spec delete_owned(allocation(), map(), String.t(), term()) :: :ok | {:held, term()} | {:error, term()}
  def delete_owned(allocation, assignment, idempotency_key, context) do
    with :ok <- validate_assignment(assignment),
         :ok <- validate_key(idempotency_key, assignment, :delete),
         {:ok, ports} <- ports(context),
         {:ok, expected} <- JobSpec.compile(assignment, ports.config),
         {:ok, uid} <- allocation_uid(allocation, expected),
         {:ok, client_context} <- client_context(ports, assignment, :delete, idempotency_key) do
      Provider.delete_owned(assignment, uid, provider_opts(ports, client_context))
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp validate_assignment(assignment) when is_map(assignment),
    do: ManagedAssignmentBundle.validate_bundle(assignment)

  defp validate_assignment(_assignment), do: {:error, :invalid_rke2_job_assignment}

  defp validate_key(key, %{sha256: digest}, operation)
       when is_binary(key) and is_binary(digest) do
    if key == digest <> ":" <> Atom.to_string(operation),
      do: :ok,
      else: {:error, :invalid_rke2_job_idempotency_key}
  end

  defp validate_key(_key, _assignment, _operation), do: {:error, :invalid_rke2_job_idempotency_key}

  defp ports(context) when is_map(context) do
    client = Map.get(context, :client, HTTPClient)
    context_provider = Map.get(context, :client_context_provider)
    config = Map.get(context, :config)

    with true <- is_atom(client) and client_loaded?(client),
         true <- is_atom(context_provider) and context_provider_loaded?(context_provider),
         %{namespace: _, image: _} <- config do
      {:ok,
       %{
         client: client,
         client_context_provider: context_provider,
         client_context_provider_context: Map.get(context, :client_context_provider_context),
         config: config
       }}
    else
      _ -> {:error, :rke2_job_adapter_ports_invalid}
    end
  end

  defp ports(_context), do: {:error, :rke2_job_adapter_ports_invalid}

  defp client_loaded?(client) do
    Code.ensure_loaded?(client) and function_exported?(client, :create_job, 3) and
      function_exported?(client, :get_job, 3) and function_exported?(client, :delete_job, 4)
  end

  defp context_provider_loaded?(provider) do
    Code.ensure_loaded?(provider) and function_exported?(provider, :client_context, 4)
  end

  defp client_context(ports, assignment, operation, idempotency_key) do
    case ports.client_context_provider.client_context(
           assignment,
           operation,
           idempotency_key,
           ports.client_context_provider_context
         ) do
      {:ok, context} -> {:ok, context}
      {:error, _reason} -> {:error, :rke2_job_client_context_unavailable}
      _ -> {:error, :invalid_rke2_job_client_context_response}
    end
  rescue
    _error -> {:error, :rke2_job_client_context_unavailable}
  end

  defp provider_opts(ports, client_context) do
    [client: ports.client, client_context: client_context, config: ports.config]
  end

  defp allocation_id(expected, job) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])
    uid = get_in(job, ["metadata", "uid"])

    if is_binary(uid) and byte_size(uid) > 0 do
      encoded = Jason.encode!([@allocation_version, namespace, name, uid, expected["metadata"]["annotations"]["symphony.hypergrid.au/assignment-sha256"]])
      {:ok, "rke2job:v1:" <> Base.url_encode64(encoded, padding: false)}
    else
      {:held, :job_uid_missing}
    end
  end

  defp allocation_uid(%{id: "rke2job:v1:" <> encoded, status: :ready} = allocation, expected)
       when map_size(allocation) == 2 do
    with {:ok, payload} <- Base.url_decode64(encoded, padding: false),
         {:ok, [@allocation_version, namespace, name, uid, digest]} <- Jason.decode(payload),
         true <- namespace == get_in(expected, ["metadata", "namespace"]),
         true <- name == get_in(expected, ["metadata", "name"]),
         true <- digest == get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-sha256"]),
         true <- valid_uid?(uid) do
      {:ok, uid}
    else
      _ -> {:error, :job_allocation_identity_mismatch}
    end
  rescue
    _error -> {:error, :job_allocation_identity_mismatch}
  end

  defp allocation_uid(_allocation, _expected), do: {:error, :job_allocation_identity_mismatch}

  defp valid_uid?(uid) when is_binary(uid),
    do: byte_size(uid) in 1..256 and Regex.match?(~r/\A[A-Za-z0-9][A-Za-z0-9._:-]*\z/, uid)

  defp valid_uid?(_uid), do: false
end
