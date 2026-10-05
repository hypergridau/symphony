defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWorkflow do
  @moduledoc """
  Binds fixed Linux recovery workflows to the installed canonical control export.

  The host supplies root-controlled, bounded reads. Every pool output must match
  the complete derivation receipt before any selected workflow is accepted.
  """

  @root "/srv/dahlia-runner-state"
  @dahlia @root <> "/dahlia"
  @output "config/symphony/recovery-workflows"
  @renderer "scripts/symphony/linux-workflow.mjs"
  @pools ~w(hypergrid-gitops hypergrid-infra midgard asgard orchestrator grid)
  @max_bytes 524_288

  @spec verify(String.t(), String.t(), (String.t(), pos_integer() -> {:ok, binary()} | {:error, term()})) ::
          :ok | {:error, :untrusted_workflow_file}
  def verify(path, pool, read) when is_binary(path) and is_function(read, 2) do
    with true <- pool in @pools,
         true <- path == @dahlia <> "/" <> @output <> "/" <> pool <> ".md",
         {:ok, controls} <- read_json(read, "linux-control-receipt.json"),
         {:ok, receipt} <- read_json(read, @output <> "/recovery-workflow-receipt.json"),
         :ok <- verify_receipt(receipt, controls, read) do
      :ok
    else
      _ -> {:error, :untrusted_workflow_file}
    end
  end

  defp read_json(read, relative) do
    with {:ok, bytes} <- read.(@dahlia <> "/" <> relative, @max_bytes) do
      Jason.decode(bytes)
    end
  end

  defp verify_receipt(receipt, controls, read) do
    with %{"schemaVersion" => 1, "sourceCommit" => commit, "files" => canonical} <- controls,
         true <- is_binary(commit) and Regex.match?(~r/\A[0-9a-f]{40}\z/, commit),
         true <- is_list(canonical) and length(canonical) in 1..137,
         %{
           "schemaVersion" => 1,
           "derivation" => "canonical-linux-workflow-v1",
           "sourceCommit" => ^commit,
           "runtimeRoot" => @root,
           "renderer" => renderer,
           "files" => files
         } <- receipt,
         true <- is_list(files) and length(files) == length(@pools),
         true <- Enum.map(files, &(is_map(&1) && Map.get(&1, "pool"))) == @pools,
         :ok <- verify_source(renderer, @renderer, canonical, read) do
      verify_outputs(files, canonical, read)
    else
      _ -> {:error, :invalid_derivation_receipt}
    end
  end

  defp verify_outputs(files, canonical, read) do
    Enum.reduce_while(files, :ok, fn file, :ok ->
      case verify_output(file, canonical, read) do
        :ok -> {:cont, :ok}
        error -> {:halt, error}
      end
    end)
  end

  defp verify_output(%{"pool" => pool, "source" => source} = file, canonical, read) do
    with true <- file["path"] == @output <> "/" <> pool <> ".md",
         true <- file["workspaceRoot"] == @root <> "/workspaces/pools/" <> pool,
         :ok <- verify_source(source, "config/symphony/workflows/" <> pool <> ".md", canonical, read),
         {:ok, _bytes} <- verify_bytes(file, read) do
      :ok
    else
      _ -> {:error, :invalid_derived_workflow}
    end
  end

  defp verify_output(_file, _canonical, _read), do: {:error, :invalid_derived_workflow}

  defp verify_source(source, expected, canonical, read) when is_map(source) do
    with true <- source["path"] == expected,
         [^source] <- Enum.filter(canonical, &(is_map(&1) and &1["path"] == expected)),
         {:ok, bytes} <- verify_bytes(source, read),
         true <- source["blob"] == digest(:sha, ["blob ", Integer.to_string(byte_size(bytes)), <<0>>, bytes]) do
      :ok
    else
      _ -> {:error, :invalid_canonical_workflow_input}
    end
  end

  defp verify_source(_source, _expected, _canonical, _read), do: {:error, :invalid_canonical_workflow_input}

  defp verify_bytes(%{"path" => relative, "bytes" => size, "sha256" => hash, "mode" => "100644"}, read)
       when is_binary(relative) and is_integer(size) and size in 1..@max_bytes and is_binary(hash) do
    with {:ok, bytes} <- read.(@dahlia <> "/" <> relative, @max_bytes),
         true <- byte_size(bytes) == size and digest(:sha256, bytes) == hash do
      {:ok, bytes}
    else
      _ -> {:error, :workflow_hash_mismatch}
    end
  end

  defp verify_bytes(_entry, _read), do: {:error, :invalid_workflow_entry}

  defp digest(algorithm, bytes), do: algorithm |> :crypto.hash(bytes) |> Base.encode16(case: :lower)
end
