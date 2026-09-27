defmodule SymphonyElixir.RKE2Job.ManagedExecutorAdapter do
  @moduledoc """
  Source-only bridge from an assignment bundle to the RKE2 Job provider.

  This module deliberately does not implement the full ManagedExecutor adapter
  behavior. Credential leasing, checkout, execution, result publication, and
  signed cleanup remain separate lifecycle ports. The caller supplies trusted
  provider configuration, a fakeable Kubernetes client, and a client-context
  provider; this module does not load credentials or contact a cluster by itself.
  The trusted host also supplies its Dahlia registration context. Allocation is
  not ready until Dahlia acknowledges the exact server-assigned Job UID.
  Allocation leaves Jobs suspended. Activation requires a host-owned guard to
  revalidate admission and credential readiness before any Kubernetes call.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{HTTPClient, JobAllocationRegistration, JobSpec, Provider}
  alias SymphonyElixir.RKE2Job.{ResultJournal, ResultReader, SuspendedAbort}

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
         :ok <- registration_prerequisites(context, assignment),
         {:ok, client_context} <- client_context(ports, assignment, :allocate, idempotency_key),
         :ok <- auth_slot_guard(context, ports.config, :reserve, [ports.config[:auth_slot], assignment]),
         {:ok, job} <- Provider.ensure(assignment, provider_opts(ports, client_context)),
         {:ok, id} <- allocation_id(expected, job),
         {:ok, uid} <- allocation_uid(%{id: id, status: :ready}, expected),
         :ok <- register_allocation(context, assignment, id, uid),
         :ok <- auth_slot_guard(context, ports.config, :bind_uid, [ports.config[:auth_slot], assignment, %{id: id, status: :ready}]) do
      {:ok, %{id: id, status: :ready}}
    else
      {:held, reason} -> {:held, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Activates only the exact allocated UID after caller-owned admission and credential checks."
  @spec activate_owned(allocation(), map(), String.t(), term()) :: {:ok, map()} | {:held, term()} | {:error, term()}
  def activate_owned(allocation, assignment, idempotency_key, context) do
    with :ok <- validate_assignment(assignment),
         :ok <- validate_key(idempotency_key, assignment, :activate),
         {:ok, ports} <- ports(context),
         {:ok, expected} <- JobSpec.compile(assignment, ports.config),
         {:ok, uid} <- allocation_uid(allocation, expected),
         :ok <- authorize_activation(context, assignment, allocation, idempotency_key),
         :ok <- auth_slot_guard(context, ports.config, :authorize, [ports.config[:auth_slot], assignment, allocation]),
         {:ok, client_context} <- client_context(ports, assignment, :activate, idempotency_key) do
      Provider.activate_owned(assignment, uid, provider_opts(ports, client_context))
    else
      {:held, reason} -> {:held, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp authorize_activation(context, assignment, allocation, idempotency_key) when is_map(context) do
    guard = Map.get(context, :activation_guard)

    if is_atom(guard) and Code.ensure_loaded?(guard) and function_exported?(guard, :authorize, 4) do
      case guard.authorize(assignment, allocation, idempotency_key, Map.get(context, :activation_guard_context)) do
        :ok -> :ok
        {:held, reason} -> {:held, reason}
        {:error, reason} -> {:held, {:activation_authorization_failed, reason}}
        _ -> {:held, :invalid_activation_authorization_response}
      end
    else
      {:held, :activation_guard_missing}
    end
  rescue
    _error -> {:held, :activation_authorization_failed}
  end

  @doc "Deletes an un-slotted allocation's exact Job UID; slotted Jobs require terminal finalization."
  @spec delete_owned(allocation(), map(), String.t(), term()) :: :ok | {:held, term()} | {:error, term()}
  def delete_owned(allocation, assignment, idempotency_key, context) do
    with :ok <- validate_assignment(assignment),
         :ok <- validate_key(idempotency_key, assignment, :delete),
         {:ok, ports} <- ports(context),
         {:ok, expected} <- JobSpec.compile(assignment, ports.config),
         {:ok, uid} <- allocation_uid(allocation, expected),
         :ok <- direct_delete_allowed?(ports.config),
         {:ok, client_context} <- client_context(ports, assignment, :delete, idempotency_key),
         :ok <- Provider.delete_owned(assignment, uid, provider_opts(ports, client_context)),
         :ok <- auth_slot_guard(context, ports.config, :release, [ports.config[:auth_slot], assignment, allocation]) do
      :ok
    else
      {:held, reason} -> {:held, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp direct_delete_allowed?(%{auth_slot: slot}) when is_map(slot),
    do: {:held, :terminal_finalization_required}

  defp direct_delete_allowed?(_config), do: :ok

  @doc "Deprecated compatibility entrypoint; held because it cannot carry a durable prepare acknowledgment."
  @deprecated "Use prepare_abort_unstarted_owned/4, persist Dahlia's acknowledgment, then confirm_abort_unstarted_owned/6."
  @spec abort_unstarted_owned(allocation(), map(), String.t(), term()) ::
          {:held, :durable_abort_prepare_ack_required}
  def abort_unstarted_owned(_allocation, _assignment, _idempotency_key, _context),
    do: {:held, :durable_abort_prepare_ack_required}

  @doc "Reads and returns immutable-prepare input for a suspended slotted Job; performs no delete."
  @spec prepare_abort_unstarted_owned(allocation(), map(), String.t(), term()) ::
          {:ok, map()} | {:held, term()} | {:error, term()}
  def prepare_abort_unstarted_owned(allocation, assignment, idempotency_key, context) do
    with :ok <- validate_assignment(assignment),
         :ok <- validate_key(idempotency_key, assignment, :abort_unstarted),
         {:ok, ports} <- ports(context),
         {:ok, expected} <- JobSpec.compile(assignment, ports.config),
         {:ok, uid} <- allocation_uid(allocation, expected),
         :ok <- auth_slot_guard(context, ports.config, :verify_bound, [ports.config[:auth_slot], assignment, allocation]),
         {:ok, client_context} <- client_context(ports, assignment, :abort_prepare, idempotency_key) do
      SuspendedAbort.prepare_owned(assignment, allocation.id, uid, provider_opts(ports, client_context))
    else
      {:held, reason} -> {:held, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Deprecated confirmation entrypoint; held because it has no provider acknowledgment."
  @deprecated "Pass Dahlia's durable prepare acknowledgment to confirm_abort_unstarted_owned/6."
  @spec confirm_abort_unstarted_owned(allocation(), map(), String.t(), map(), term()) ::
          {:held, :durable_abort_prepare_ack_required}
  def confirm_abort_unstarted_owned(_allocation, _assignment, _idempotency_key, _observation, _context),
    do: {:held, :durable_abort_prepare_ack_required}

  @doc "Verifies a durable provider acknowledgment, rechecks the observation, and conditionally deletes the Job."
  @spec confirm_abort_unstarted_owned(allocation(), map(), String.t(), map(), map(), term()) ::
          :ok | {:held, term()} | {:error, term()}
  def confirm_abort_unstarted_owned(allocation, assignment, idempotency_key, observation, prepare_ack, context) do
    with :ok <- validate_assignment(assignment),
         :ok <- validate_key(idempotency_key, assignment, :abort_unstarted),
         {:ok, ports} <- ports(context),
         {:ok, expected} <- JobSpec.compile(assignment, ports.config),
         {:ok, uid} <- allocation_uid(allocation, expected),
         :ok <- auth_slot_guard(context, ports.config, :verify_bound, [ports.config[:auth_slot], assignment, allocation]),
         {:ok, client_context} <- client_context(ports, assignment, :abort_confirm, idempotency_key) do
      ack_opts =
        provider_opts(ports, client_context)
        |> Keyword.put(:prepare_ack_guard, Map.get(context, :prepare_ack_guard))
        |> Keyword.put(:prepare_ack_guard_context, Map.get(context, :prepare_ack_guard_context))
        |> Keyword.put(:confirmed_delete_journal, Map.get(context, :confirmed_delete_journal))

      SuspendedAbort.confirm_owned(assignment, allocation.id, uid, observation, prepare_ack, ack_opts)
    else
      {:held, reason} -> {:held, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc "Journals one terminal worker result before deleting its exact Job UID; retains the OAuth slot lease."
  @spec finalize_terminal_owned(allocation(), map(), String.t(), term()) ::
          {:ok, map()} | {:held, term()} | {:error, term()}
  def finalize_terminal_owned(allocation, assignment, idempotency_key, context) do
    with :ok <- validate_assignment(assignment),
         :ok <- validate_key(idempotency_key, assignment, :finalize),
         {:ok, ports} <- ports(context),
         {:ok, expected} <- JobSpec.compile(assignment, ports.config),
         {:ok, uid} <- allocation_uid(allocation, expected),
         {:ok, client_context} <- client_context(ports, assignment, :finalize, idempotency_key),
         {:ok, observation} <- terminal_checkpoint(assignment, uid, ports, client_context, context),
         :ok <- Provider.delete_owned(assignment, uid, provider_opts(ports, client_context)) do
      {:ok, observation}
    else
      {:held, reason} -> {:held, reason}
      {:error, reason} -> {:error, reason}
    end
  end

  defp terminal_checkpoint(assignment, uid, ports, client_context, context) do
    root = if is_map(context), do: Map.get(context, :result_journal_root), else: nil

    case ResultJournal.load(assignment, uid, root) do
      {:ok, observation} -> {:ok, observation}
      :missing -> read_and_journal_terminal(assignment, uid, ports, client_context, root)
      other -> other
    end
  end

  defp read_and_journal_terminal(assignment, uid, ports, client_context, root) do
    with {:ok, observation} <-
           ResultReader.read(assignment, uid,
             client: ports.client,
             client_context: client_context,
             config: ports.config
           ),
         {:ok, _path} <- ResultJournal.record(assignment, observation, root) do
      ResultJournal.load(assignment, uid, root)
    end
  end

  defp auth_slot_guard(_context, %{auth_slot: nil}, _action, _args), do: :ok
  defp auth_slot_guard(_context, config, _action, _args) when not is_map_key(config, :auth_slot), do: :ok

  defp auth_slot_guard(context, _config, action, args) when is_map(context) do
    guard = Map.get(context, :auth_slot_lease_guard)
    arity = length(args) + 1

    if is_atom(guard) and Code.ensure_loaded?(guard) and function_exported?(guard, action, arity) do
      case apply(guard, action, args ++ [Map.get(context, :auth_slot_lease_guard_context)]) do
        :ok -> :ok
        {:held, reason} -> {:held, reason}
        {:error, reason} -> {:held, {:auth_slot_lease_guard_failed, reason}}
        _ -> {:held, :invalid_auth_slot_lease_guard_response}
      end
    else
      {:held, :auth_slot_lease_guard_missing}
    end
  rescue
    _error -> {:held, :auth_slot_lease_guard_failed}
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

  defp register_allocation(context, assignment, id, uid) when is_map(context) do
    registry = Map.get(context, :allocation_registry, JobAllocationRegistration)
    binding = Map.get(context, :claim_binding)

    if valid_registry?(registry) and valid_registration_binding?(binding, assignment) do
      case registry.register(id, uid, binding.reservation_id, Map.get(context, :allocation_registry_context)) do
        :ok -> :ok
        {:held, reason} -> {:held, reason}
        _ -> {:held, :job_allocation_registration_unverified}
      end
    else
      {:held, :job_allocation_registration_unavailable}
    end
  rescue
    _error -> {:held, :job_allocation_registration_unavailable}
  end

  defp register_allocation(_context, _assignment, _id, _uid), do: {:held, :job_allocation_registration_unavailable}

  defp registration_prerequisites(context, assignment) when is_map(context) do
    registry = Map.get(context, :allocation_registry, JobAllocationRegistration)
    binding = Map.get(context, :claim_binding)

    if valid_registry?(registry) and function_exported?(registry, :ready?, 1) and
         valid_registration_binding?(binding, assignment) and
         registry.ready?(Map.get(context, :allocation_registry_context)) do
      :ok
    else
      {:held, :job_allocation_registration_unavailable}
    end
  rescue
    _error -> {:held, :job_allocation_registration_unavailable}
  end

  defp registration_prerequisites(_context, _assignment), do: {:held, :job_allocation_registration_unavailable}

  defp valid_registry?(registry),
    do: is_atom(registry) and Code.ensure_loaded?(registry) and function_exported?(registry, :register, 4)

  defp valid_registration_binding?(binding, assignment) when is_map(binding) do
    Map.get(binding, :issue_id) == assignment.lease.issue_id and
      Map.get(binding, :generation) == assignment.lease.generation and
      Map.get(binding, :repository_ref) == assignment.repository_ref and
      Map.get(binding, :runner_id) == assignment.seat and
      is_binary(Map.get(binding, :reservation_id))
  end

  defp valid_registration_binding?(_binding, _assignment), do: false

  defp ports(context) when is_map(context) do
    client = Map.get(context, :client, HTTPClient)
    context_provider = Map.get(context, :client_context_provider)
    config = Map.get(context, :config)

    with true <- is_atom(client) and client_loaded?(client),
         true <- is_atom(context_provider) and context_provider_loaded?(context_provider),
         %{namespace: _, image: _, repository_id: _} <- config do
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
      function_exported?(client, :get_job, 3) and function_exported?(client, :list_pods, 2) and
      function_exported?(client, :activate_job, 5) and
      function_exported?(client, :delete_job, 4)
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
