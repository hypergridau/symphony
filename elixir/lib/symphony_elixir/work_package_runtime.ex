defmodule SymphonyElixir.WorkPackageRuntime do
  @moduledoc """
  Builds the production work-package adapter from host-injected settings.

  Provider credentials stay in the host environment. A managed runner is
  enabled only when the complete provider tuple is present; partial settings
  fail closed during supervisor startup.
  """

  alias SymphonyElixir.{Config, WorkPackageCleanup}
  alias SymphonyElixir.ManagedResponsibility.Manifest

  @provider_url "DAHLIA_WORK_PACKAGE_PROVIDER_URL"
  @runner_token "DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"
  @attestation_key "DAHLIA_WORK_PACKAGE_ATTESTATION_KEY"
  @runner_id "DAHLIA_RUNNER_ID"
  @profile_id "DAHLIA_MANAGED_PROJECT_PROFILE_ID"
  @journal_path "DAHLIA_WORK_PACKAGE_JOURNAL_PATH"
  @archive_root "DAHLIA_WORK_PACKAGE_ARCHIVE_ROOT"
  @recovery_directory "DAHLIA_WORK_PACKAGE_RECOVERY_DIRECTORY"
  @recovery_public_key "DAHLIA_WORK_PACKAGE_RECOVERY_PUBLIC_KEY"

  @required_env [@provider_url, @runner_token, @attestation_key, @runner_id, @profile_id]

  @doc "Returns the host-configured managed runtime or the explicit disabled state."
  @spec configuration(keyword()) :: {:ok, map()} | :disabled | {:error, term()}
  def configuration(opts \\ []) when is_list(opts) do
    env = Keyword.get(opts, :env, System.get_env())
    values = Map.new(@required_env, &{&1, Map.get(env, &1)})
    present = Enum.filter(@required_env, &present?(Map.get(values, &1)))

    cond do
      present == [] ->
        :disabled

      length(present) != length(@required_env) ->
        {:error, {:incomplete_work_package_runtime, @required_env -- present}}

      true ->
        build_runtime(values, env)
    end
  end

  @doc false
  @spec required_environment() :: [String.t()]
  def required_environment, do: @required_env

  @doc "Returns whether the host declared a repository pool that requires managed runtime claims."
  @spec managed_pool?() :: boolean()
  def managed_pool? do
    present?(System.get_env("SYMPHONY_POOL_KEY")) or
      present?(System.get_env("SYMPHONY_REPOSITORY_REF"))
  end

  defp build_runtime(values, env) do
    with {:ok, base_url} <- valid_base_url(values[@provider_url]),
         {:ok, journal_path} <- configured_path(env, @journal_path, default_journal_path()),
         {:ok, archive_root} <- configured_path(env, @archive_root, default_archive_root(journal_path)),
         {:ok, managed_delegations} <- Manifest.load(env, System.system_time(:millisecond)),
         {:ok, claim_recovery} <- recovery_configuration(env) do
      {:ok,
       %{
         base_url: base_url,
         runner_token: values[@runner_token],
         attestation_key: values[@attestation_key],
         runner_id: values[@runner_id],
         pool_key: Map.get(env, "SYMPHONY_POOL_KEY"),
         managed_project_profile_id: values[@profile_id],
         journal_path: journal_path,
         archive_root: archive_root,
         managed_delegations: managed_delegations,
         claim_recovery: claim_recovery,
         cleanup_prepare_fun: fn state, token, head, entry ->
           WorkPackageCleanup.prepare(state, token, head, entry, archive_root: archive_root)
         end,
         cleanup_evidence_fun: fn state, token, head ->
           WorkPackageCleanup.verify(state, token, head, archive_root: archive_root)
         end,
         secret_environment_names: [
           @runner_token,
           @attestation_key,
           "DAHLIA_WORK_PACKAGE_CLAIM_TOKEN",
           "DAHLIA_CLEANUP_ATTESTATION_KEY"
         ]
       }}
    end
  end

  defp recovery_configuration(env) do
    case {Map.get(env, @recovery_directory), Map.get(env, @recovery_public_key)} do
      {nil, nil} ->
        {:ok, nil}

      {directory, encoded_key} when is_binary(directory) and is_binary(encoded_key) ->
        with :absolute <- Path.type(directory),
             {:ok, %{type: :directory}} <- File.lstat(directory),
             {:ok, key} when byte_size(key) == 32 <- Base.url_decode64(encoded_key, padding: false) do
          {:ok, %{directory: directory, public_key: key}}
        else
          _ -> {:error, :invalid_claim_recovery_configuration}
        end

      _ ->
        {:error, :incomplete_claim_recovery_configuration}
    end
  end

  defp valid_base_url(value) when is_binary(value) do
    value = String.trim(value)
    uri = URI.parse(value)

    if uri.scheme in ["http", "https"] and is_binary(uri.host) and uri.host != "" do
      {:ok, String.trim_trailing(value, "/")}
    else
      {:error, :invalid_work_package_provider_url}
    end
  end

  defp valid_base_url(_value), do: {:error, :invalid_work_package_provider_url}

  defp configured_path(env, name, default) do
    case Map.get(env, name) do
      value when is_binary(value) ->
        if String.trim(value) != "", do: {:ok, Path.expand(value)}, else: {:error, {:invalid_work_package_path, name}}

      nil ->
        {:ok, default}

      _ ->
        {:error, {:invalid_work_package_path, name}}
    end
  end

  defp default_journal_path do
    try do
      Config.execution_fence_state_path() <> ".work-package"
    rescue
      _ -> Path.join(System.tmp_dir!(), "symphony-work-package.json")
    end
  end

  defp default_archive_root(journal_path), do: Path.join(Path.dirname(journal_path), "cleanup-archives")

  defp present?(value), do: is_binary(value) and String.trim(value) != ""
end
