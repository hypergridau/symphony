defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryKubernetes do
  @moduledoc """
  Root-only readback of the retained claim's RKE2 Jobs and Pods.

  This module is read-only. It uses the fixed host API and credential paths, the
  normal TLS-verifying Kubernetes client, and complete resource-version-stable
  namespace lists. Any uncertainty denies completion.
  """

  alias SymphonyElixir.RKE2Job.{HostClientContext, HTTPClient}
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence

  @api_server "https://10.0.14.10:6443"
  @credential_root "/etc/symphony/frigga-kubernetes"
  @namespace "frigga"
  @claim_issue_label "symphony.hypergrid.au/issue-id"
  @claim_generation_label "symphony.hypergrid.au/generation"
  @claim_issue_annotation "symphony.hypergrid.au/assignment-issue-id"
  @claim_generation_annotation "symphony.hypergrid.au/assignment-generation"

  @doc "Reads fresh complete Jobs and Pods and proves the exact issue generation absent."
  @spec observe(map(), map()) :: {:ok, map()} | {:error, :claim_resources_present | :kubernetes_observation_unavailable}
  def observe(claim, expected_cluster) when is_map(claim) and is_map(expected_cluster) do
    observe_with(
      claim,
      expected_cluster,
      &client_context/1,
      &HTTPClient.list_jobs_complete/2,
      &HTTPClient.list_pods_complete/2
    )
  end

  def observe(_claim, _expected_cluster), do: {:error, :kubernetes_observation_unavailable}

  if Mix.env() == :test do
    @doc false
    @spec observe_with_test_adapter(
            map(),
            map(),
            (map() -> term()),
            (String.t(), term() -> term()),
            (String.t(), term() -> term())
          ) ::
            {:ok, map()} | {:error, :kubernetes_observation_unavailable}
    def observe_with_test_adapter(claim, expected_cluster, context, jobs, pods),
      do: observe_with(claim, expected_cluster, context, jobs, pods)
  end

  defp observe_with(claim, expected_cluster, context_loader, list_jobs, list_pods) do
    with true <- claim["generation"] == 2,
         true <- expected_cluster["apiServer"] == @api_server,
         true <- is_binary(claim["issueId"]) and is_binary(claim["assignmentSHA256"]),
         true <- Regex.match?(~r/\A[0-9a-f]{64}\z/, claim["assignmentSHA256"]),
         {:ok, context, ca_sha256} <- context_loader.(claim),
         true <- ca_sha256 == expected_cluster["caSha256"],
         {:ok, jobs} <- list_jobs.(@namespace, context),
         :ok <- complete_resources_absent(jobs.items, claim, :job),
         {:ok, pods} <- list_pods.(@namespace, context),
         :ok <- complete_resources_absent(pods.items, claim, :pod),
         {:ok, final_jobs} <- list_jobs.(@namespace, context),
         :ok <- complete_resources_absent(final_jobs.items, claim, :job),
         true <- valid_resources(jobs.items, :job) and valid_resources(pods.items, :pod) and valid_resources(final_jobs.items, :job) do
      {:ok,
       %{
         "observedAt" => DateTime.utc_now() |> DateTime.to_iso8601(),
         "apiServer" => @api_server,
         "namespace" => @namespace,
         "jobs" => %{
           "firstResourceVersion" => jobs.resource_version,
           "confirmingResourceVersion" => final_jobs.resource_version,
           "sha256" => digest(Jason.encode!(jobs.items)),
           "claimAbsent" => true
         },
         "pods" => %{
           "resourceVersion" => pods.resource_version,
           "sha256" => digest(Jason.encode!(pods.items)),
           "claimAbsent" => true
         }
       }}
    else
      false -> {:error, :kubernetes_observation_unavailable}
      _ -> {:error, :kubernetes_observation_unavailable}
    end
  rescue
    _ -> {:error, :kubernetes_observation_unavailable}
  catch
    _, _ -> {:error, :kubernetes_observation_unavailable}
  end

  @doc false
  @spec complete_resources_absent([map()], map(), :job | :pod) :: :ok | {:error, :claim_resources_present}
  def complete_resources_absent(items, claim, kind) when is_list(items) and is_map(claim) and kind in [:job, :pod] do
    if not valid_resources(items, kind) or Enum.any?(items, &matching_claim_resource?(&1, claim, kind)),
      do: {:error, :claim_resources_present},
      else: :ok
  end

  def complete_resources_absent(_items, _claim, _kind), do: {:error, :claim_resources_present}

  defp client_context(claim) do
    digest = ConfirmedRecoveryEvidence.tuple_digest(Map.delete(claim, "assignmentSHA256"))
    assignment = %{sha256: digest, environment: %{target_environment: :rke2}}
    idempotency_key = digest <> ":observe"
    config = %{api_server: @api_server, credential_root: @credential_root}

    with :ok <- trusted_credential_root(),
         true <- is_binary(digest),
         {:ok, context} <- HostClientContext.client_context(assignment, :observe, idempotency_key, config),
         {:ok, ca_bytes} <- read_trusted_ca(Path.join(@credential_root, "kubernetes-ca.crt")) do
      {:ok, context, digest(ca_bytes)}
    else
      _ -> {:error, :kubernetes_observation_unavailable}
    end
  end

  defp trusted_credential_root do
    if trusted_root_directory?(@credential_root), do: :ok, else: {:error, :untrusted_kubernetes_credential_root}
  end

  defp trusted_root_directory?(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when Bitwise.band(mode, 0o022) == 0 ->
        parent = Path.dirname(path)
        parent == path or trusted_root_directory?(parent)

      _ ->
        false
    end
  end

  defp read_trusted_ca(path) do
    with {:ok, %File.Stat{type: :regular, uid: 0, mode: mode, links: 1, size: size}} <- File.lstat(path),
         true <- Bitwise.band(mode, 0o022) == 0 and size in 1..1_048_576,
         {:ok, bytes} <- File.read(path),
         true <- byte_size(bytes) == size do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_kubernetes_ca}
    end
  end

  defp valid_resources(items, kind) do
    Enum.all?(items, &valid_resource?(&1, kind))
  end

  defp valid_resource?(item, kind) do
    metadata = item["metadata"]
    valid_resource_metadata?(metadata) and valid_resource_kind?(item, kind)
  end

  defp valid_resource_metadata?(metadata) do
    is_map(metadata) and metadata["namespace"] == @namespace and nonempty?(metadata["uid"]) and
      nonempty?(metadata["name"]) and valid_map_or_nil?(metadata["labels"]) and
      valid_map_or_nil?(metadata["annotations"])
  end

  defp valid_resource_kind?(item, :pod), do: item["kind"] in [nil, "Pod"]
  defp valid_resource_kind?(item, :job), do: item["kind"] in [nil, "Job"]
  defp nonempty?(value), do: is_binary(value) and value != ""
  defp valid_map_or_nil?(nil), do: true
  defp valid_map_or_nil?(value), do: is_map(value)

  defp matching_claim_resource?(resource, claim, kind) do
    metadata = resource["metadata"] || %{}
    labels = metadata["labels"] || %{}
    annotations = metadata["annotations"] || %{}
    compact_issue = digest(claim["issueId"]) |> binary_part(0, 32)
    generation = Integer.to_string(claim["generation"])

    issue_match =
      labels[@claim_issue_label] == compact_issue or
        (kind == :job and annotations[@claim_issue_annotation] == claim["issueId"])

    generation_match =
      labels[@claim_generation_label] == generation or
        (kind == :job and annotations[@claim_generation_annotation] == generation)

    issue_match and generation_match
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
