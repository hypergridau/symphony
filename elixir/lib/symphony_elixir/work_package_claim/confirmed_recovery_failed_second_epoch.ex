defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedSecondEpoch do
  @moduledoc "Pins the observed unsigned epoch-2 HTTP startup failure and its unchanged ancestor."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryFailedEpoch, as: First

  @hashes %{
    "issuer-input.json" => "d0072944993410bf0180177282216526b342bd4d66cbb2af5dd111178d126882",
    "manifest.json" => "f3a8d848f35d8375dafe54d81cc839424fafa4be44d7b0ab4898cef0ad410396",
    "provider-held-readback.json" => "79058d5557fd624154e717c1c1a7e91bb48aee44b34011488121e60ccf6576aa",
    "reviewed-preflight.json" => "ac3a3e7d9a950d7141631eeaa2025cf6539ea2880f05fa590626822787717b15",
    "started.json" => "021c3d4aac486585b746331f611b30e3a874f68ca471d1fb9f5c2c1d92aaa5be"
  }
  @seal "1f8ab266c9cc3ad2438d1d320aa152e94bb0e77453ccf59f4fecc2ef0e8b7718"
  @seal_path "/srv/dahlia-runner-state/evidence/hgs740-reconciliation-20261001/failed-epoch-2-seal-v1.json"

  @spec predecessor_binding() :: map()
  def predecessor_binding, do: %{"epoch" => "epoch-2", "evidenceSHA256" => @hashes, "failureSealSHA256" => @seal}

  @spec valid?(map()) :: boolean()
  def valid?(metadata),
    do: metadata["predecessorEpoch2"] == predecessor_binding() and metadata["ancestorEpoch1"] == First.predecessor_binding()

  @spec verify(map(), String.t(), function()) :: :ok | {:error, :invalid_reconciliation_epoch}
  def verify(metadata, directory, read),
    do: verify_with(metadata, directory, read, @hashes, @seal, &First.verify/3)

  defp verify_with(metadata, directory, read, hashes, seal, verify_ancestor) do
    binding = %{"epoch" => "epoch-2", "evidenceSHA256" => hashes, "failureSealSHA256" => seal}

    with true <- metadata["predecessorEpoch2"] == binding and metadata["ancestorEpoch1"] == First.predecessor_binding(),
         :ok <- verify_ancestor.(%{"predecessorEpoch" => metadata["ancestorEpoch1"]}, directory, read),
         {:ok, seal_bytes} <- read.(@seal_path, 262_144),
         true <- digest(seal_bytes) == seal,
         true <- Enum.all?(hashes, fn {name, hash} -> matches?(read, Path.join([directory, "reconciliation", "epoch-2", name]), hash) end),
         {:ok, manifest_bytes} <- read.(Path.join([directory, "reconciliation", "epoch-2", "manifest.json"]), 262_144),
         {:ok, manifest} <- Jason.decode(manifest_bytes),
         true <- manifest["predecessorEpoch"] == metadata["ancestorEpoch1"] do
      :ok
    else
      _ -> {:error, :invalid_reconciliation_epoch}
    end
  end

  if Mix.env() == :test do
    @doc false
    @spec verify_for_test(map(), String.t(), function(), map(), String.t(), function()) :: :ok | {:error, term()}
    def verify_for_test(metadata, directory, read, hashes, seal, ancestor),
      do: verify_with(metadata, directory, read, hashes, seal, ancestor)
  end

  defp matches?(read, path, hash) do
    case read.(path, 1_048_576) do
      {:ok, bytes} -> digest(bytes) == hash
      _ -> false
    end
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
end
