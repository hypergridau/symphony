defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedSignedEpoch do
  @moduledoc "Pins signed epoch 3, which failed before a local transition or provider release."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedSecondEpoch, as: Second

  @hashes %{
    "issued-envelope.json" => "27d273cac9fce9ec42b0f6dbc84e8fb94e5ee6ca39b3e26f5c1f9c3384ba75e6",
    "issuer-input.json" => "485818f93b102469fe6a670c95d9808462f1700142c30700c21e8f18e314fd60",
    "manifest.json" => "357b001e7a03967536ce94a42a93a162162bb53cea1e3fa9f0d562053ec08883",
    "provider-held-readback.json" => "688b83fab2b6e12bba628542558f478ed79b983421abfcfd7cdda4111407c305",
    "reviewed-preflight.json" => "f546ed3ab434fcac7bf0e5694c12d84af977d7bc39e963dc766e1abbf4524ccf",
    "started.json" => "ad0dd2de3f86bc94d23b5be8870845d9256004673ebd821e82f01298fb474944"
  }
  @base %{
    "candidate.json" => "e27b511837e55faece08bf2758017fef59bb26bdaa907239fa0f79aebf99576c",
    "confirmed-root-envelope.json" => "27d273cac9fce9ec42b0f6dbc84e8fb94e5ee6ca39b3e26f5c1f9c3384ba75e6"
  }
  @seal "04421ed979cd8c8c0e8b013e0ff20f9c683bff317d462abc4f8216ca6405f228"
  @seal_path "/srv/dahlia-runner-state/evidence/hgs740-reconciliation-20261001/failed-signed-epoch-3-seal-v1.json"

  @spec predecessor_binding() :: map()
  def predecessor_binding,
    do: %{"epoch" => "epoch-3", "evidenceSHA256" => @hashes, "baseSignedOutputSHA256" => @base, "failureSealSHA256" => @seal}

  @spec valid?(map()) :: boolean()
  def valid?(metadata), do: metadata["signedPredecessorEpoch3"] == predecessor_binding() and Second.valid?(metadata)

  @spec verify(map(), String.t(), function()) :: :ok | {:error, :invalid_reconciliation_epoch}
  def verify(metadata, directory, read),
    do: verify_with(metadata, directory, read, predecessor_binding(), &Second.verify/3)

  defp verify_with(metadata, directory, read, binding, ancestor) do
    with true <- metadata["signedPredecessorEpoch3"] == binding and Second.valid?(metadata),
         :ok <- ancestor.(metadata, directory, read),
         {:ok, seal_bytes} <- read.(@seal_path, 262_144),
         true <- digest(seal_bytes) == binding["failureSealSHA256"],
         true <- matches_all?(read, Path.join([directory, "reconciliation", "epoch-3"]), binding["evidenceSHA256"]),
         true <- matches_all?(read, directory, binding["baseSignedOutputSHA256"]),
         {:ok, manifest_bytes} <- read.(Path.join([directory, "reconciliation", "epoch-3", "manifest.json"]), 262_144),
         {:ok, manifest} <- Jason.decode(manifest_bytes),
         true <- Second.valid?(manifest) do
      :ok
    else
      _ -> {:error, :invalid_reconciliation_epoch}
    end
  rescue
    _ -> {:error, :invalid_reconciliation_epoch}
  end

  if Mix.env() == :test do
    @doc false
    @spec verify_for_test(map(), String.t(), function(), map(), function()) :: :ok | {:error, term()}
    def verify_for_test(metadata, directory, read, binding, ancestor),
      do: verify_with(metadata, directory, read, binding, ancestor)
  end

  defp matches_all?(read, directory, hashes) do
    Enum.all?(hashes, fn {name, hash} ->
      case read.(Path.join(directory, name), 1_048_576) do
        {:ok, bytes} -> digest(bytes) == hash
        _ -> false
      end
    end)
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
