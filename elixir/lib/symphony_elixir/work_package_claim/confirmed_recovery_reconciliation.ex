defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliation do
  @moduledoc "Fixed append-only epoch for the retained HGS-740 failed issuance."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedEpoch, as: FailedEpoch
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedSecondEpoch, as: FailedSecondEpoch

  @issue "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"
  @historical %{
    "issuer-input.json" => "8fbc20b0b4c4c0679facf881fd93d10f6e288346d13626074a777a2b11378f99",
    "provider-held-readback.json" => "1d8262b146c7261d926f9802ceb6cfc189a149aa6506600a68ce74df11cec0b5",
    "reviewed-preflight.json" => "4775184a720b41b4871f148b8a73ca4a34c8aba0edd8ce08c8a3e2eaf6687ceb"
  }
  @fields ~w(contractVersion epoch historicalSHA256 reviewedPreflightSHA256 providerReadbackSHA256 providerHeldSHA256 issuerInputSHA256 observedAt)
  @stable ~w(expected localGenerationMax fenceSHA256 claimJournalSHA256 responsibilityGraphSHA256 predecessorRetirement witnesses witnessLogSHA256 hostIdentity bootId dispatchPhase)

  @spec historical_hashes() :: map()
  def historical_hashes, do: @historical

  @spec epoch_directory(String.t(), String.t()) :: String.t()
  def epoch_directory(directory, epoch \\ "epoch-1") when epoch in ["epoch-1", "epoch-2", "epoch-3"],
    do: Path.join([directory, "reconciliation", epoch])

  @spec validate(map()) :: :ok | {:error, :invalid_reconciliation_epoch}
  def validate(observation), do: validate_with_history(observation, @historical)

  defp validate_with_history(%{"reconciliation" => metadata} = observation, historical) when is_map(metadata) do
    with true <- valid_metadata?(metadata),
         true <- metadata["historicalSHA256"] == historical,
         true <- metadata["observedAt"] == observation["observedAt"],
         true <- observation["expected"]["issueId"] == @issue,
         true <- observation["expected"]["generation"] == 2,
         true <- observation["expected"]["reservationId"] == "workpkgreservation_e19008ccb2764fe79ca68bf500d20a1f",
         true <- observation["expected"]["projectionId"] == "workpkg_4446a7d851764ecf9bf62bfbae26d1cc",
         true <- Enum.all?(~w(reviewedPreflightSHA256 providerReadbackSHA256 providerHeldSHA256 issuerInputSHA256), &digest?(metadata[&1])) do
      :ok
    else
      _ -> {:error, :invalid_reconciliation_epoch}
    end
  rescue
    _ -> {:error, :invalid_reconciliation_epoch}
  end

  defp validate_with_history(_observation, _historical), do: {:error, :invalid_reconciliation_epoch}

  defp valid_metadata?(%{"epoch" => "epoch-1"} = metadata),
    do:
      Enum.sort(Map.keys(metadata)) == Enum.sort(@fields) and
        metadata["contractVersion"] == "hgs740-reconciliation-observation.v1"

  defp valid_metadata?(%{"epoch" => "epoch-2"} = metadata),
    do:
      Enum.sort(Map.keys(metadata)) == Enum.sort(["predecessorEpoch" | @fields]) and
        metadata["contractVersion"] == "hgs740-reconciliation-observation.v2" and FailedEpoch.valid?(metadata)

  defp valid_metadata?(%{"epoch" => "epoch-3"} = metadata),
    do:
      Enum.sort(Map.keys(metadata)) == Enum.sort(["predecessorEpoch2", "ancestorEpoch1" | @fields]) and
        metadata["contractVersion"] == "hgs740-reconciliation-observation.v3" and FailedSecondEpoch.valid?(metadata)

  defp valid_metadata?(_metadata), do: false

  @doc "Reads only fixed paths through the root adapter; historical timestamps are never refreshed."
  @spec verify(map(), String.t(), (String.t(), pos_integer() -> {:ok, binary()} | {:error, term()})) ::
          :ok | {:error, :invalid_reconciliation_epoch}
  def verify(observation, directory, read), do: verify_with_history(observation, directory, read, @historical)

  defp verify_with_history(observation, directory, read, historical_hashes) when is_map(observation) and is_function(read, 2) do
    with :ok <- validate_with_history(observation, historical_hashes),
         epoch <- epoch_directory(directory, observation["reconciliation"]["epoch"]),
         :ok <- verify_predecessor(observation["reconciliation"], directory, read),
         {:ok, history} <- read_files(directory, historical_hashes, read),
         {:ok, historical} <- decode(history["issuer-input.json"]),
         true <- stable?(historical["observation"], observation),
         true <- historical["assignmentSnapshotState"] == "absent" and is_nil(historical["assignmentSHA256"]),
         true <- historical["predecessorClaimState"] == "unsubmitted",
         metadata <- observation["reconciliation"],
         {:ok, manifest} <- read.(Path.join(epoch, "manifest.json"), 262_144),
         true <- manifest == Evidence.canonical_json(metadata),
         {:ok, _files} <- read_files(epoch, epoch_hashes(metadata), read),
         {:ok, input_bytes} <- read.(Path.join(epoch, "issuer-input.json"), 1_048_576),
         {:ok, input} <- decode(input_bytes),
         true <- input["observation"]["reconciliation"] == metadata,
         plain_observation <- Map.delete(input["observation"], "reconciliation"),
         plain <- Map.put(input, "observation", plain_observation),
         true <- digest(Evidence.canonical_json(plain)) == metadata["issuerInputSHA256"],
         true <- stable?(plain_observation, observation) do
      :ok
    else
      _ -> {:error, :invalid_reconciliation_epoch}
    end
  rescue
    _ -> {:error, :invalid_reconciliation_epoch}
  end

  defp verify_with_history(_observation, _directory, _read, _historical), do: {:error, :invalid_reconciliation_epoch}

  defp verify_predecessor(%{"epoch" => "epoch-2"} = metadata, directory, read),
    do: FailedEpoch.verify(metadata, directory, read)

  defp verify_predecessor(%{"epoch" => "epoch-3"} = metadata, directory, read),
    do: FailedSecondEpoch.verify(metadata, directory, read)

  defp verify_predecessor(_metadata, _directory, _read), do: :ok

  if Mix.env() == :test do
    @doc false
    @spec verify_test_epoch(map(), String.t(), function(), map()) :: :ok | {:error, term()}
    def verify_test_epoch(observation, directory, read, hashes),
      do: verify_with_history(observation, directory, read, hashes)
  end

  defp epoch_hashes(metadata) do
    %{"reviewed-preflight.json" => metadata["reviewedPreflightSHA256"], "provider-held-readback.json" => metadata["providerReadbackSHA256"]}
  end

  defp read_files(directory, hashes, read) do
    Enum.reduce_while(hashes, {:ok, %{}}, fn {name, hash}, {:ok, files} ->
      with {:ok, bytes} <- read.(Path.join(directory, name), 1_048_576),
           true <- digest(bytes) == hash do
        {:cont, {:ok, Map.put(files, name, bytes)}}
      else
        _ ->
          {:halt, :error}
      end
    end)
  end

  defp decode(bytes) do
    with {:ok, value} when is_map(value) <- Jason.decode(bytes),
         true <- Evidence.canonical_json(value) == bytes do
      {:ok, value}
    else
      _ -> :error
    end
  end

  defp stable?(left, right) when is_map(left) and is_map(right),
    do: Enum.all?(@stable, &(Map.fetch(left, &1) == Map.fetch(right, &1)))

  defp stable?(_left, _right), do: false
  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp digest?(value), do: is_binary(value) and Regex.match?(~r/\A[0-9a-f]{64}\z/, value)
end
