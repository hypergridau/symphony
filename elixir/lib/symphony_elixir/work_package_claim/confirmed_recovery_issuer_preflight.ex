defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryIssuerPreflight do
  @moduledoc "Read-only native Kubernetes qualification before reserving a reconciliation epoch."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryKubernetes, as: Kubernetes
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliation, as: Reconciliation
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReconciliationHost, as: Host

  @spec verify(String.t()) :: :ok | {:error, :kubernetes_observation_unavailable}
  def verify(directory) do
    verify_with(
      directory,
      Reconciliation.historical_hashes(),
      &Host.read_private/2,
      &Kubernetes.observe_without_assignment_snapshot/2
    )
  end

  defp verify_with(directory, hashes, read, observe) do
    with {:ok, input} <- read_history(directory, hashes, read),
         {:ok, bundle} <- Jason.decode(input),
         true <- Evidence.canonical_json(bundle) == input,
         %{"assignmentSHA256" => nil, "assignmentSnapshotState" => "absent", "observation" => observation} <- bundle,
         claim <- observation["expected"] |> Map.put("assignmentSHA256", nil) |> Map.put("assignmentSnapshotState", "absent"),
         {:ok, _readback} <- observe.(claim, observation["kubernetes"]["cluster"]) do
      :ok
    else
      _ -> {:error, :kubernetes_observation_unavailable}
    end
  rescue
    _ -> {:error, :kubernetes_observation_unavailable}
  catch
    _, _ -> {:error, :kubernetes_observation_unavailable}
  end

  defp read_history(directory, hashes, read) do
    Enum.reduce_while(hashes, {:ok, nil}, fn {name, hash}, {:ok, input} ->
      with {:ok, bytes} <- read.(Path.join(directory, name), 1_048_576),
           true <- :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower) == hash do
        {:cont, {:ok, if(name == "issuer-input.json", do: bytes, else: input)}}
      else
        _ -> {:halt, :invalid_history}
      end
    end)
  end

  if Mix.env() == :test do
    @doc false
    @spec verify_for_test(String.t(), map(), function(), function()) :: :ok | {:error, term()}
    def verify_for_test(directory, hashes, read, observe), do: verify_with(directory, hashes, read, observe)
  end
end
