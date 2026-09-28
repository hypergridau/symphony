defmodule SymphonyElixir.RKE2Job.HostAllocationContext do
  @moduledoc """
  Builds one trusted suspended-Job allocation context on the persistent host.

  The configured image and repository selector come from host settings. The
  Kubernetes bearer is read for this operation only; the OAuth claim is leased
  from Dahlia before it enters the Job spec. No credential enters the signed
  assignment or the claim journal.
  """

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.RKE2Job.{DahliaAuthSlotLeaseGuard, HostClientContext, HTTPClient}
  alias SymphonyElixir.RKE2Job.{JobAllocationRegistration, ManagedExecutorAdapter}

  @namespace "frigga"
  @settings ~w(SYMPHONY_RKE2_API_SERVER SYMPHONY_RKE2_CREDENTIAL_ROOT SYMPHONY_RKE2_WORKER_IMAGE SYMPHONY_RKE2_REPOSITORY_ID SYMPHONY_RKE2_AUTH_SLOT_ID SYMPHONY_RKE2_AUTH_CLAIM_NAME)
  @digest_image ~r|\A[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}\z|
  @dns_label ~r/\A[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\z/

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
         {:ok, kube_context} <- client_context(assignment, config),
         guard_context = guard_context(config, binding, kube_context),
         {:ok, slot} <- prepare_slot(assignment, config, guard_context) do
      {:ok,
       %{
         adapter: Map.get(config, :adapter, ManagedExecutorAdapter),
         client: HTTPClient,
         client_context_provider: HostClientContext,
         client_context_provider_context: %{api_server: config.api_server, credential_root: config.credential_root},
         config: %{
           namespace: @namespace,
           image: config.image,
           repository_id: config.repository_id,
           auth_slot: slot,
           auth_slot_catalog: %{config.slot_id => config.claim_name}
         },
         allocation_registry: JobAllocationRegistration,
         allocation_registry_context: %{base_url: config.provider_url, runner_token: config.runner_token},
         auth_slot_lease_guard: DahliaAuthSlotLeaseGuard,
         auth_slot_lease_guard_context: guard_context,
         claim_binding: binding
       }
       |> maybe_test_port(config, :test_pid)}
    else
      _ -> {:held, :rke2_host_allocation_context_unavailable}
    end
  rescue
    _ -> {:held, :rke2_host_allocation_context_unavailable}
  end

  def prepare(_assignment, _binding, _config), do: {:held, :rke2_host_allocation_context_unavailable}

  defp validate_configuration(env, %{repository_ref: repository_ref}, provider_url, runner_token)
       when is_binary(repository_ref) and is_binary(provider_url) and is_binary(runner_token) do
    api_server = env["SYMPHONY_RKE2_API_SERVER"]
    root = env["SYMPHONY_RKE2_CREDENTIAL_ROOT"]
    image = env["SYMPHONY_RKE2_WORKER_IMAGE"]
    repository_id = env["SYMPHONY_RKE2_REPOSITORY_ID"]
    slot_id = env["SYMPHONY_RKE2_AUTH_SLOT_ID"]
    claim_name = env["SYMPHONY_RKE2_AUTH_CLAIM_NAME"]

    if valid_host_values?(api_server, root, image, repository_id, slot_id, claim_name) and
         https_origin?(provider_url) and byte_size(repository_ref) > 0 and byte_size(runner_token) > 0 do
      {:ok,
       %{
         api_server: api_server,
         credential_root: root,
         image: image,
         repository_id: repository_id,
         slot_id: slot_id,
         claim_name: claim_name,
         repository_ref: repository_ref,
         provider_url: provider_url,
         runner_token: runner_token
       }}
    else
      {:error, :invalid_rke2_host_context}
    end
  end

  defp validate_configuration(_env, _manifest, _provider_url, _runner_token),
    do: {:error, :invalid_rke2_host_context}

  defp valid_host_values?(api_server, root, image, repository_id, slot_id, claim_name) do
    https_origin?(api_server) and Path.type(root) == :absolute and Path.expand(root) == root and
      digest_image?(image) and repository_id?(repository_id) and dns_label?(slot_id) and dns_label?(claim_name)
  end

  defp digest_image?(image), do: byte_size(image) <= 512 and Regex.match?(@digest_image, image)

  defp repository_id?(value),
    do: byte_size(value) <= 20 and Regex.match?(~r/\A[1-9][0-9]*\z/, value)

  defp client_context(assignment, config) do
    provider = Map.get(config, :client_context_fun, &HostClientContext.client_context/4)
    provider.(assignment, :allocate, assignment.sha256 <> ":allocation", config)
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
      pvc_namespace: @namespace,
      pvc_client_context: kube_context
    }
    |> maybe_test_port(config, :pvc_read_fun)
    |> maybe_test_port(config, :post_fun)
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
