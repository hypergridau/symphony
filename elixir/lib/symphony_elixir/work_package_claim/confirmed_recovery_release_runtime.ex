defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseRuntime do
  @moduledoc "Reads existing root-private provider identities. It provisions no credentials or enrollment."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryCore, as: Core
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliationHost, as: Custody
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost, as: Host

  @root "/srv/dahlia-runner-state/identity"

  @spec provider(struct(), map()) :: {:ok, map()} | {:error, atom()}
  def provider(context, enrollment) do
    provider(context, enrollment, &Core.release_only_snapshot/1, &Custody.read_private/2)
  end

  if Mix.env() == :test do
    @doc false
    @spec with_test_reads(map(), map(), function(), function()) :: {:ok, map()} | {:error, atom()}
    def with_test_reads(context, enrollment, snapshot, read), do: provider(context, enrollment, snapshot, read)
  end

  defp provider(context, enrollment, snapshot_read, private_read) do
    with true <- enrollment[:protocol_accepted] == true and enrollment[:trust_enrolled] == true,
         {:ok, _paths} <- Host.fixed_runtime_paths(context.pool),
         {:ok, snapshot} <- snapshot_read.(context),
         {:ok, marker} <- Jason.decode(snapshot.marker_bytes),
         {:ok, env} <- private_read.(@root <> "/managed-pools-hgs382/" <> context.pool <> ".env", 16_384),
         {:ok, values} <- identities(env),
         true <- values["DAHLIA_RUNNER_ID"] == marker["expected"]["runnerId"],
         runner when is_binary(runner) <- values["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
         {:ok, admin} <- private_read.(@root <> "/claim-recovery-hgs485/provider-admin.token", 4096) do
      {:ok, %{protocol_accepted: true, trust_enrolled: true, runner_token: runner, admin_token: String.trim(admin)}}
    else
      _ -> {:error, :hgs740_provider_identity_unavailable}
    end
  rescue
    _ -> {:error, :hgs740_provider_identity_unavailable}
  end

  defp identities(bytes) do
    Enum.reduce_while(String.split(bytes, "\n", trim: true), {:ok, %{}}, fn line, {:ok, values} ->
      case String.split(line, "=", parts: 2) do
        [name, value] when name in ["DAHLIA_RUNNER_ID", "DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"] ->
          identity(values, name, unquote_value(value))

        _ ->
          {:cont, {:ok, values}}
      end
    end)
  end

  defp identity(values, name, value) do
    if Map.has_key?(values, name) or not is_binary(value),
      do: {:halt, {:error, :invalid_provider_identity}},
      else: {:cont, {:ok, Map.put(values, name, value)}}
  end

  defp unquote_value(value) do
    case Regex.run(~r/\A(?:"([^"\s$`\\]+)"|'([^'\s]+)'|([^\s"'$`\\]+))\z/, value) do
      nil -> nil
      [_ | captures] -> Enum.find(captures, &(&1 != ""))
    end
  end
end
