defmodule SymphonyElixir.RKE2Job.HostAllocationContext do
  @moduledoc """
  Builds one trusted suspended-Job allocation context on the persistent host.

  The configured image and repository selector come from host settings. The
  Kubernetes bearer is read for this operation only; the OAuth claim is leased
  from Dahlia before it enters the Job spec. No credential enters the signed
  assignment or the claim journal.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.PathSafety
  alias SymphonyElixir.RKE2Job.{AuthCacheVerifierObserver, DahliaAssignmentBinding, DahliaAuthSlotLeaseGuard}
  alias SymphonyElixir.RKE2Job.{HostClientContext, HTTPClient, JobSpec}
  alias SymphonyElixir.RKE2Job.{JobAllocationRegistration, ManagedExecutorAdapter, ResultJournal}
  alias SymphonyElixir.RKE2Job.SuspendedAbort

  @namespace "frigga"
  @settings ~w(
    SYMPHONY_RKE2_API_SERVER
    SYMPHONY_RKE2_CREDENTIAL_ROOT
    SYMPHONY_RKE2_WORKER_IMAGE
    SYMPHONY_RKE2_REPOSITORY_ID
    SYMPHONY_RKE2_AUTH_SLOT_ID
    SYMPHONY_RKE2_AUTH_CLAIM_NAME
    SYMPHONY_RKE2_RESULT_JOURNAL_ROOT
    SYMPHONY_RKE2_ABORT_JOURNAL_ROOT
    SYMPHONY_RKE2_WORKSPACE_ROOT
    SYMPHONY_DAHLIA_ASSIGNMENT_BIND_ORIGIN
  )
  @digest_image ~r|\A[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}\z|
  @dns_label ~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/
  @test_environment Mix.env() == :test

  @doc "Parses an all-or-nothing host declaration; no token or OAuth bytes are read."
  @spec configuration(map(), map() | nil, String.t(), String.t()) :: {:ok, map()} | :disabled | {:error, term()}
  def configuration(env, manifest, provider_url, runner_token) when is_map(env) do
    present = Enum.filter(@settings, &present?(Map.get(env, &1)))

    cond do
      present == [] -> :disabled
      length(present) != length(@settings) -> {:error, {:incomplete_rke2_host_context, @settings -- present}}
      true -> validate_configuration(env, manifest, provider_url, runner_token)
    end
  end

  def configuration(_env, _manifest, _provider_url, _runner_token), do: {:error, :invalid_rke2_host_context}

  @doc "Leases the exact OAuth claim and builds the existing adapter's host ports."
  @spec prepare(map(), map(), map()) :: {:ok, map()} | {:held, term()}
  def prepare(assignment, binding, config) when is_map(assignment) and is_map(binding) and is_map(config) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         true <- assignment.repository_ref == config.repository_ref,
         true <- assignment.lease.issue_id == binding.issue_id and assignment.lease.generation == binding.generation,
         true <- binding.repository_ref == config.repository_ref and is_binary(binding.runner_id),
         {:ok, bound_config} <- bind_assignment(assignment, binding, config),
         {:ok, kube_context} <- client_context(assignment, bound_config),
         guard_context = guard_context(bound_config, binding, kube_context),
         {:ok, slot} <- prepare_slot(assignment, bound_config, guard_context) do
      {:ok, build_context(bound_config, binding, slot, guard_context)}
    else
      _ -> {:held, :rke2_host_allocation_context_unavailable}
    end
  rescue
    _ -> {:held, :rke2_host_allocation_context_unavailable}
  end

  def prepare(_assignment, _binding, _config), do: {:held, :rke2_host_allocation_context_unavailable}

  @doc "Rebuilds a retained suspended Job context from exact readback without reserving another OAuth lease."
  @spec reattach(map(), map(), String.t(), map()) :: {:ok, map()} | {:held, term()}
  def reattach(assignment, binding, allocation_id, config),
    do: reattach_existing(assignment, binding, allocation_id, config, true)

  @doc "Reattaches only when the exact suspended Job has no execution status or owned Pods."
  @spec reattach_unstarted(map(), map(), String.t(), map()) :: {:ok, map()} | {:held, term()}
  def reattach_unstarted(assignment, binding, allocation_id, config) do
    with {:ok, context} <- reattach(assignment, binding, allocation_id, config),
         {:ok, kube_context} <- client_context(assignment, config),
         {:ok, expected} <- JobSpec.compile(assignment, context.config),
         {:ok, uid} <- terminal_allocation_uid(allocation_id, expected),
         {:ok, _observation} <-
           SuspendedAbort.prepare_owned(assignment, allocation_id, uid,
             client: observation_client(config),
             client_context: kube_context,
             config: context.config
           ) do
      {:ok, context}
    else
      _ -> {:held, :rke2_retained_unstarted_job_unverified}
    end
  rescue
    _ -> {:held, :rke2_retained_unstarted_job_unverified}
  catch
    _kind, _reason -> {:held, :rke2_retained_unstarted_job_unverified}
  end

  @doc "Rebuilds the context for an activated exact Job without issuing another OAuth lease."
  @spec reattach_started(map(), map(), String.t(), map()) :: {:ok, map()} | {:held, term()}
  def reattach_started(assignment, binding, allocation_id, config),
    do: reattach_existing(assignment, binding, allocation_id, config, false)

  defp reattach_existing(assignment, binding, allocation_id, config, suspended)
       when is_map(assignment) and is_map(binding) and is_binary(allocation_id) and is_map(config) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         true <- assignment.repository_ref == config.repository_ref,
         true <- assignment.lease.issue_id == binding.issue_id and assignment.lease.generation == binding.generation,
         true <- binding.repository_ref == config.repository_ref and is_binary(binding.runner_id),
         {:ok, bound_config} <- bind_assignment(assignment, binding, config),
         {:ok, kube_context} <- client_context(assignment, bound_config),
         {:ok, preflight_job} <- JobSpec.compile(assignment, job_config(bound_config, nil)),
         name = get_in(preflight_job, ["metadata", "name"]),
         {:ok, job} <- job_reader(config).(@namespace, name, kube_context),
         {:ok, slot} <- retained_slot(job, assignment, bound_config),
         {:ok, expected} <- JobSpec.compile(assignment, job_config(bound_config, slot)),
         true <- JobSpec.owned_job_for_cleanup?(job, expected),
         true <- get_in(job, ["spec", "suspend"]) == suspended,
         true <- allocation_id == encoded_allocation_id(expected, job),
         guard_context = guard_context(bound_config, binding, kube_context),
         :ok <- slot_guard(bound_config).verify_claim_uid(slot, guard_context),
         :ok <-
           slot_guard(bound_config).verify_bound(
             slot,
             assignment,
             %{id: allocation_id, status: :ready},
             guard_context
           ) do
      {:ok, build_context(bound_config, binding, slot, guard_context)}
    else
      _ -> {:held, :rke2_retained_allocation_unverified}
    end
  rescue
    _ -> {:held, :rke2_retained_allocation_unverified}
  end

  defp reattach_existing(_assignment, _binding, _allocation_id, _config, _suspended),
    do: {:held, :rke2_retained_allocation_unverified}

  @doc "Rebuilds a terminal context from the durable result and slot binding after Job deletion."
  @spec reattach_terminal(map(), map(), String.t(), map()) :: {:ok, map()} | {:held, term()}
  def reattach_terminal(assignment, binding, allocation_id, config)
      when is_map(assignment) and is_map(binding) and is_binary(allocation_id) and is_map(config) do
    with :ok <- ManagedAssignmentBundle.validate_bundle(assignment),
         true <- assignment.repository_ref == config.repository_ref,
         true <- assignment.lease.issue_id == binding.issue_id and assignment.lease.generation == binding.generation,
         true <- binding.repository_ref == config.repository_ref and is_binary(binding.runner_id),
         {:ok, bound_config} <- bind_assignment(assignment, binding, config),
         {:ok, preflight_job} <- JobSpec.compile(assignment, job_config(bound_config, nil)),
         {:ok, uid} <- terminal_allocation_uid(allocation_id, preflight_job),
         {:ok, _observation, slot} <- ResultJournal.load_with_slot(assignment, uid, config.result_journal_root),
         true <- is_map(slot) and slot.slot_id == config.slot_id and slot.claim_name == config.claim_name,
         true <- slot.binding_sha256 == bound_config.assignment_subject_digest,
         {:ok, kube_context} <- terminal_client_context(assignment, bound_config),
         guard_context = guard_context(bound_config, binding, kube_context),
         # The exact lease may already be released; Dahlia validates its retained receipt on replay.
         :ok <- slot_guard(bound_config).verify_claim_uid(slot, guard_context) do
      {:ok, build_context(bound_config, binding, slot, guard_context)}
    else
      _ -> {:held, :rke2_terminal_allocation_unverified}
    end
  rescue
    _ -> {:held, :rke2_terminal_allocation_unverified}
  end

  def reattach_terminal(_assignment, _binding, _allocation_id, _config),
    do: {:held, :rke2_terminal_allocation_unverified}

  defp terminal_allocation_uid("rke2job:v1:" <> encoded, expected) do
    with {:ok, bytes} <- Base.url_decode64(encoded, padding: false),
         {:ok, [1, @namespace, name, uid, digest]} <- Jason.decode(bytes),
         true <- name == get_in(expected, ["metadata", "name"]),
         true <- digest == get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-sha256"]),
         true <- encoded_allocation_id(expected, %{"metadata" => %{"uid" => uid}}) == "rke2job:v1:" <> encoded do
      {:ok, uid}
    else
      _ -> {:held, :rke2_terminal_allocation_unverified}
    end
  end

  defp terminal_allocation_uid(_allocation_id, _expected), do: {:held, :rke2_terminal_allocation_unverified}

  defp build_context(config, binding, slot, guard_context) do
    %{
      adapter: Map.get(config, :adapter, ManagedExecutorAdapter),
      client: HTTPClient,
      client_context_provider: HostClientContext,
      client_context_provider_context: %{api_server: config.api_server, credential_root: config.credential_root},
      config: job_config(config, slot),
      allocation_registry: JobAllocationRegistration,
      allocation_registry_context: %{base_url: config.provider_url, runner_token: config.runner_token},
      auth_slot_lease_guard: DahliaAuthSlotLeaseGuard,
      auth_slot_lease_guard_context: guard_context,
      result_journal_root: config.result_journal_root,
      claim_binding: binding
    }
    |> maybe_test_port(config, :test_pid)
  end

  defp job_config(config, slot) do
    %{namespace: @namespace, image: config.image, repository_id: config.repository_id}
    |> maybe_slot(slot, config)
    |> maybe_binding_digest(config)
  end

  defp maybe_slot(job_config, nil, _config), do: job_config

  defp maybe_slot(job_config, slot, config) do
    Map.merge(job_config, %{auth_slot: slot, auth_slot_catalog: %{config.slot_id => config.claim_name}})
  end

  defp maybe_binding_digest(job_config, config) do
    case Map.get(config, :assignment_subject_digest) do
      digest when is_binary(digest) -> Map.put(job_config, :assignment_binding_digest, digest)
      _ -> job_config
    end
  end

  defp retained_slot(job, assignment, config) do
    annotations = get_in(job, ["metadata", "annotations"])
    volumes = get_in(job, ["spec", "template", "spec", "volumes"])

    slot = %{
      slot_id: is_map(annotations) && annotations["symphony.hypergrid.au/codex-auth-slot"],
      lease_id: is_map(annotations) && annotations["symphony.hypergrid.au/codex-auth-lease"],
      claim_uid: is_map(annotations) && annotations["symphony.hypergrid.au/codex-auth-claim-uid"],
      claim_name: retained_claim_name(volumes),
      assignment_sha256: assignment.sha256,
      binding_sha256: is_map(annotations) && annotations["symphony.hypergrid.au/assignment-binding-sha256"],
      seat: assignment.seat
    }

    if slot.slot_id == config.slot_id and slot.claim_name == config.claim_name,
      do: {:ok, slot},
      else: {:held, :rke2_retained_allocation_unverified}
  end

  defp retained_claim_name(volumes) when is_list(volumes) do
    case Enum.filter(volumes, &(is_map(&1) and &1["name"] == "codex-auth-slot")) do
      [%{"persistentVolumeClaim" => %{"claimName" => name}}] -> name
      _ -> nil
    end
  end

  defp retained_claim_name(_volumes), do: nil

  defp encoded_allocation_id(expected, job) do
    namespace = get_in(expected, ["metadata", "namespace"])
    name = get_in(expected, ["metadata", "name"])
    uid = get_in(job, ["metadata", "uid"])
    digest = get_in(expected, ["metadata", "annotations", "symphony.hypergrid.au/assignment-sha256"])

    if is_binary(uid) and byte_size(uid) > 0,
      do: "rke2job:v1:" <> Base.url_encode64(Jason.encode!([1, namespace, name, uid, digest]), padding: false),
      else: nil
  end

  defp job_reader(config), do: Map.get(config, :job_read_fun, &HTTPClient.get_job/3)

  defp observation_client(config) do
    if @test_environment, do: Map.get(config, :observation_client, HTTPClient), else: HTTPClient
  end

  defp validate_configuration(env, %{repository_ref: repository_ref} = manifest, provider_url, runner_token)
       when is_binary(repository_ref) and is_binary(provider_url) and is_binary(runner_token) do
    api_server = env["SYMPHONY_RKE2_API_SERVER"]
    root = env["SYMPHONY_RKE2_CREDENTIAL_ROOT"]
    image = env["SYMPHONY_RKE2_WORKER_IMAGE"]
    repository_id = env["SYMPHONY_RKE2_REPOSITORY_ID"]
    slot_id = env["SYMPHONY_RKE2_AUTH_SLOT_ID"]
    claim_name = env["SYMPHONY_RKE2_AUTH_CLAIM_NAME"]
    result_journal_root = env["SYMPHONY_RKE2_RESULT_JOURNAL_ROOT"]
    abort_journal_root = env["SYMPHONY_RKE2_ABORT_JOURNAL_ROOT"]
    workspace_root = env["SYMPHONY_RKE2_WORKSPACE_ROOT"]
    assignment_bind_origin = env["SYMPHONY_DAHLIA_ASSIGNMENT_BIND_ORIGIN"]

    if valid_host_values?(api_server, root, image, repository_id, slot_id, claim_name, result_journal_root) and
         valid_abort_roots?(abort_journal_root, workspace_root) and
         https_origin?(assignment_bind_origin) and
         https_origin?(provider_url) and byte_size(repository_ref) > 0 and byte_size(runner_token) > 0 do
      {:ok,
       %{
         api_server: api_server,
         credential_root: root,
         image: image,
         repository_id: repository_id,
         slot_id: slot_id,
         claim_name: claim_name,
         result_journal_root: result_journal_root,
         abort_journal_root: abort_journal_root,
         workspace_root: workspace_root,
         repository_ref: repository_ref,
         provider_url: provider_url,
         assignment_bind_origin: assignment_bind_origin,
         managed_delegations: manifest,
         runner_token: runner_token
       }}
    else
      {:error, :invalid_rke2_host_context}
    end
  end

  defp validate_configuration(_env, _manifest, _provider_url, _runner_token),
    do: {:error, :invalid_rke2_host_context}

  defp valid_host_values?(api_server, root, image, repository_id, slot_id, claim_name, result_journal_root) do
    https_origin?(api_server) and absolute_root?(root) and
      digest_image?(image) and repository_id?(repository_id) and dns_label?(slot_id) and dns_label?(claim_name) and
      absolute_root?(result_journal_root)
  end

  defp absolute_root?(root) when is_binary(root),
    do: Path.type(root) == :absolute and Path.expand(root) == root

  defp absolute_root?(_root), do: false

  defp valid_abort_roots?(journal_root, workspace_root) do
    absolute_root?(journal_root) and absolute_root?(workspace_root) and
      case {PathSafety.canonicalize(journal_root), PathSafety.canonicalize(workspace_root)} do
        {{:ok, canonical_journal}, {:ok, canonical_workspace}} ->
          not inside_root?(canonical_journal, canonical_workspace)

        _ ->
          false
      end
  end

  defp inside_root?(path, root) do
    PathSafety.lexically_equal?(path, root) or
      String.starts_with?(path, String.trim_trailing(root, "/") <> "/")
  end

  defp digest_image?(image), do: byte_size(image) <= 512 and Regex.match?(@digest_image, image)

  defp repository_id?(value),
    do: byte_size(value) <= 20 and Regex.match?(~r/\A[1-9][0-9]*\z/, value)

  defp client_context(assignment, config) do
    provider = Map.get(config, :client_context_fun, &HostClientContext.client_context/4)
    provider.(assignment, :allocate, assignment.sha256 <> ":allocation", config)
  end

  defp terminal_client_context(assignment, config) do
    provider = Map.get(config, :client_context_fun, &HostClientContext.client_context/4)
    provider.(assignment, :finalize, assignment.sha256 <> ":finalize", config)
  end

  defp prepare_slot(assignment, config, guard_context) do
    slot_guard(config).prepare_slot(
      assignment,
      config.slot_id,
      %{config.slot_id => config.claim_name},
      guard_context
    )
  end

  defp guard_context(config, binding, kube_context) do
    %{
      base_url: config.provider_url,
      runner_token: config.runner_token,
      reservation_id: binding.reservation_id,
      assignment_subject_digest: config.assignment_subject_digest,
      pvc_namespace: @namespace,
      pvc_client_context: kube_context,
      result_journal_root: config.result_journal_root,
      cleanup_receipt_fun: cleanup_receipt_fun(config)
    }
    |> maybe_test_port(config, :pvc_read_fun)
    |> maybe_test_port(config, :post_fun)
  end

  defp bind_assignment(assignment, binding, config) do
    case DahliaAssignmentBinding.bind(assignment, binding, config) do
      {:ok, %{assignment_digest: digest}} -> {:ok, Map.put(config, :assignment_subject_digest, digest)}
      {:held, reason} -> {:held, reason}
    end
  end

  defp cleanup_receipt_fun(config) do
    fn slot, assignment, allocation ->
      provider = Map.get(config, :client_context_fun, &HostClientContext.client_context/4)

      case provider.(assignment, :finalize, assignment.sha256 <> ":finalize", config) do
        {:ok, client_context} ->
          AuthCacheVerifierObserver.observe(slot, assignment, allocation, %{
            config: %{
              image: config.image,
              catalog: %{config.slot_id => config.claim_name},
              journal_root: config.result_journal_root
            },
            client_context: client_context
          })

        _ ->
          {:held, :rke2_host_cleanup_context_unavailable}
      end
    end
  end

  defp maybe_test_port(context, config, name) do
    case Map.fetch(config, name) do
      {:ok, value} -> Map.put(context, name, value)
      :error -> context
    end
  end

  defp slot_guard(config), do: Map.get(config, :slot_guard, DahliaAuthSlotLeaseGuard)

  defp https_origin?(value) when is_binary(value) do
    uri = URI.parse(value)

    uri.scheme == "https" and is_binary(uri.host) and uri.host != "" and uri.userinfo == nil and
      uri.query == nil and uri.fragment == nil and uri.path in [nil, "", "/"]
  end

  defp https_origin?(_value), do: false

  defp dns_label?(value) when is_binary(value), do: byte_size(value) in 1..63 and Regex.match?(@dns_label, value)
  defp dns_label?(_value), do: false
  defp present?(value), do: is_binary(value) and String.trim(value) != ""
end
