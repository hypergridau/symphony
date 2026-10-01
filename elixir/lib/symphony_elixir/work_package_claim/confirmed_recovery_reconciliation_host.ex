defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliationHost do
  @moduledoc false

  import Bitwise, only: [band: 2]
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuer, as: Issuer
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliation, as: Epoch
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost, as: Host

  @inputs ~w(started.json reviewed-preflight.json provider-held-readback.json manifest.json issuer-input.json)

  @spec verify(map()) :: :ok | {:error, term()}
  def verify(%{"reconciliation" => _metadata} = observation) do
    with :ok <- Epoch.validate(observation),
         issue_id <- observation["expected"]["issueId"],
         :ok <- require_directory(issue_id, observation["reconciliation"]["epoch"]),
         do: Epoch.verify(observation, Host.marker_directory(issue_id), &read_private/2)
  end

  def verify(observation) when is_map(observation) do
    # Once reserved, this epoch cannot be silently bypassed by a legacy input.
    issue_id = get_in(observation, ["expected", "issueId"])

    if retained_claim?(observation) or (is_binary(issue_id) and File.exists?(Epoch.epoch_directory(Host.marker_directory(issue_id)))),
      do: {:error, :reconciliation_binding_missing},
      else: :ok
  end

  def verify(_observation), do: {:error, :invalid_reconciliation_epoch}

  defp retained_claim?(observation) do
    expected = observation["expected"] || %{}

    expected["issueId"] == "f77e349e-21d9-4bdf-bad3-ce08b302e7e8" and expected["generation"] == 2 and
      expected["reservationId"] == "workpkgreservation_e19008ccb2764fe79ca68bf500d20a1f" and
      expected["projectionId"] == "workpkg_4446a7d851764ecf9bf62bfbae26d1cc"
  end

  @spec verify_envelope(map(), binary()) :: :ok | {:error, term()}
  def verify_envelope(%{"reconciliation" => _metadata} = observation, envelope) do
    with :ok <- verify(observation),
         directory <- Host.marker_directory(observation["expected"]["issueId"]),
         saved <- Path.join(Epoch.epoch_directory(directory, observation["reconciliation"]["epoch"]), "issued-envelope.json") do
      verify_saved_envelope(saved, directory, envelope, &File.lstat/1, &read_private/2, &File.exists?/1)
    end
  end

  def verify_envelope(observation, _envelope), do: verify(observation)

  defp verify_saved_envelope(saved, directory, envelope, lstat, read, exists) do
    case lstat.(saved) do
      {:ok, _} ->
        if read.(saved, 1_048_576) == {:ok, envelope}, do: :ok, else: {:error, :epoch_issuance_conflict}

      {:error, :enoent} ->
        if Enum.any?(~w(candidate.json confirmed-root-envelope.json transaction.json), &exists.(Path.join(directory, &1))), do: {:error, :epoch_issuance_conflict}, else: :ok

      _ ->
        {:error, :epoch_issuance_conflict}
    end
  end

  @spec require_directory(String.t(), String.t()) :: :ok | {:error, term()}
  def require_directory(issue_id, epoch \\ "epoch-1") when epoch in ["epoch-1", "epoch-2", "epoch-3"] do
    base = Host.marker_directory(issue_id)
    directory = Epoch.epoch_directory(base, epoch)

    with :ok <- successor_custody(base, epoch),
         do: require_directory_with(directory, &trusted_directory/1, &File.lstat/1, &File.ls/1)
  end

  defp successor_custody(base, epoch),
    do: successor_custody_with(base, epoch, &trusted_directory/1, &File.lstat/1, &File.ls/1)

  defp successor_custody_with(base, "epoch-1", _trusted, lstat, _ls) do
    if Enum.all?(["epoch-2", "epoch-3"], &(lstat.(Epoch.epoch_directory(base, &1)) == {:error, :enoent})),
      do: :ok,
      else: {:error, :reconciliation_epoch_downgrade}
  end

  defp successor_custody_with(base, "epoch-2", trusted, lstat, ls) do
    case lstat.(Epoch.epoch_directory(base, "epoch-3")) do
      {:error, :enoent} -> unsigned_predecessor(base, "epoch-1", trusted, lstat, ls)
      _ -> {:error, :reconciliation_epoch_downgrade}
    end
  end

  defp successor_custody_with(base, "epoch-3", trusted, lstat, ls) do
    with :ok <- unsigned_predecessor(base, "epoch-1", trusted, lstat, ls),
         do: unsigned_predecessor(base, "epoch-2", trusted, lstat, ls)
  end

  defp unsigned_predecessor(base, epoch, trusted, lstat, ls) do
    predecessor = Epoch.epoch_directory(base, epoch)

    with :ok <- require_directory_with(predecessor, trusted, lstat, ls),
         {:ok, entries} <- ls.(predecessor),
         true <- Enum.sort(entries) == Enum.sort(@inputs) do
      :ok
    else
      _ -> {:error, :invalid_reconciliation_epoch}
    end
  end

  defp require_directory_with(directory, trusted, lstat, ls) do
    with :ok <- trusted.(directory),
         {:ok, %File.Stat{mode: mode}} <- lstat.(directory),
         true <- band(mode, 0o777) == 0o700,
         {:ok, entries} <- ls.(directory),
         true <- Enum.all?(@inputs, &(&1 in entries)),
         true <- Enum.all?(entries, &(&1 in ["issued-envelope.json" | @inputs])) do
      :ok
    else
      _ -> {:error, :invalid_reconciliation_epoch}
    end
  end

  @spec read_private(String.t(), pos_integer()) :: {:ok, binary()} | {:error, term()}
  def read_private(path, maximum) do
    read_private_with(path, maximum, &trusted_directory/1, &File.lstat/1, &File.read/1)
  end

  defp read_private_with(path, maximum, trusted, lstat, read) do
    with :ok <- trusted.(Path.dirname(path)),
         {:ok, %File.Stat{type: :regular, uid: 0, gid: 0, mode: mode, links: 1, size: size} = before} <- lstat.(path),
         true <- band(mode, 0o777) == 0o600 and size in 1..maximum,
         {:ok, bytes} <- read.(path),
         true <- byte_size(bytes) == size,
         {:ok, ^before} <- lstat.(path) do
      {:ok, bytes}
    else
      _ -> {:error, :untrusted_reconciliation_file}
    end
  end

  @spec persist(String.t(), binary(), binary(), function(), function()) :: :ok | {:error, term()}
  def persist(issue_id, candidate, envelope, write, sync) do
    directory = Host.marker_directory(issue_id)

    with {:ok, observation} <- Jason.decode(candidate),
         epoch <- Epoch.epoch_directory(directory, observation["reconciliation"]["epoch"]) do
      persist_epoch(directory, epoch, candidate, envelope, write, sync)
    else
      _ -> {:error, :issuer_output_conflict}
    end
  rescue
    _ -> {:error, :issuer_output_conflict}
  end

  defp persist_epoch(directory, epoch, candidate, envelope, write, sync) do
    # Retain the signed result before publishing either transaction input. A
    # crash can only republish these exact bytes, never sign another result.
    with {:error, :enoent} <- File.lstat(Path.join(directory, "transaction.json")),
         :ok <- retain_exact(Path.join(epoch, "issued-envelope.json"), envelope, write),
         :ok <- sync.(epoch),
         :ok <- retain_exact(Path.join(directory, "candidate.json"), candidate, write),
         :ok <- retain_exact(Path.join(directory, "confirmed-root-envelope.json"), envelope, write),
         :ok <- sync.(directory) do
      :ok
    else
      _ -> {:error, :issuer_output_conflict}
    end
  end

  @spec resume(map(), String.t()) :: :not_issued | :ok | {:error, term()}
  def resume(context, bundle_path) do
    directory = Host.marker_directory(context.issue_id)

    epoch =
      Enum.find(["epoch-1", "epoch-2", "epoch-3"], fn name ->
        bundle_path == Path.join(Epoch.epoch_directory(directory, name), "issuer-input.json")
      end)

    if epoch do
      with :ok <- require_directory(context.issue_id, epoch),
           do: resume_saved(context, Path.join(Epoch.epoch_directory(directory, epoch), "issued-envelope.json"), &File.lstat/1, &read_private/2)
    else
      :not_issued
    end
  end

  defp resume_saved(context, path, lstat, read) do
    case lstat.(path) do
      {:error, :enoent} ->
        :not_issued

      {:ok, _} ->
        with {:ok, envelope} <- read.(path, 1_048_576),
             {:ok, encoded} <- Jason.decode(envelope),
             {:ok, bytes} <- Base.url_decode64(encoded["payload"], padding: false),
             {:ok, payload} <- Jason.decode(bytes),
             bindings <- Issuer.bindings(payload, context.pool, context.issue_id, context.nonce, context.host_ops.now_ms.()),
             {:ok, ^payload} <- context.host_ops.verify_signed_evidence.(envelope, bindings),
             :ok <- persist_saved(context, payload, envelope) do
          :ok
        else
          _ -> {:error, :saved_reconciliation_issuance_invalid}
        end

      _ ->
        {:error, :saved_reconciliation_issuance_invalid}
    end
  end

  defp persist_saved(context, payload, envelope) do
    candidate = Evidence.canonical_json(payload["observation"])
    context.host_ops.persist_issuer_outputs.(context.issue_id, candidate, envelope)
  end

  defp retain_exact(path, bytes, write) do
    case File.lstat(path) do
      {:error, :enoent} ->
        write.(path, bytes)

      {:ok, _} ->
        if read_private(path, 1_048_576) == {:ok, bytes}, do: :ok, else: {:error, :issuer_output_conflict}

      _ ->
        {:error, :issuer_output_conflict}
    end
  end

  defp trusted_directory(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory, uid: 0, mode: mode}} when band(mode, 0o022) == 0 ->
        parent = Path.dirname(path)
        if parent == path, do: :ok, else: trusted_directory(parent)

      _ ->
        {:error, :untrusted_reconciliation_directory}
    end
  end

  if Mix.env() == :test do
    @doc false
    @spec successor_custody_for_test(String.t(), map()) :: :ok | {:error, term()}
    def successor_custody_for_test(epoch, operations),
      do: successor_custody_with("/fixed/generation-2", epoch, operations.trusted, operations.lstat, operations.ls)

    @doc false
    @spec require_directory_for_test(map()) :: :ok | {:error, term()}
    def require_directory_for_test(operations),
      do: require_directory_with("/fixed/epoch", operations.trusted, operations.lstat, operations.ls)

    @doc false
    @spec verify_saved_envelope_for_test(binary(), map()) :: :ok | {:error, term()}
    def verify_saved_envelope_for_test(envelope, operations),
      do: verify_saved_envelope("/fixed/epoch/saved", "/fixed/parent", envelope, operations.lstat, operations.read, operations.exists)

    @doc false
    @spec read_private_for_test(String.t(), pos_integer(), map()) :: {:ok, binary()} | {:error, term()}
    def read_private_for_test(path, maximum, operations),
      do: read_private_with(path, maximum, operations.trusted, operations.lstat, operations.read)

    @doc false
    @spec resume_saved_for_test(map(), String.t(), map()) :: :not_issued | :ok | {:error, term()}
    def resume_saved_for_test(context, path, operations), do: resume_saved(context, path, operations.lstat, operations.read)
  end
end
