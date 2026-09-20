defmodule SymphonyElixir.ManagedTokenBudget do
  @moduledoc "Durable managed usage; one host writer, explicit initialization, and no implicit repair or renewal."

  alias SymphonyElixir.ManagedTokenBudget.{Codec, Correction, Registration, RetainedEvidence}

  @doc """
  Reads cumulative evidence against an independently installed retained prefix.
  The caller must hold real coordination locks and establish source quiescence
  and ledger provenance. Integrity rechecks are not locks or execution authority.
  """
  @spec retained_evidence(Path.t(), map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def retained_evidence(path, identity, issue_id, floor) do
    with :ok <- RetainedEvidence.validate_floor(floor),
         {:ok, ledger} <- load(path, identity) do
      RetainedEvidence.read(ledger, issue_id, floor, &verified_bytes/1)
    end
  end

  @spec load(Path.t(), map()) :: {:ok, map()} | {:error, term()}
  def load(path, identity) do
    with :ok <- plain_path(path),
         :ok <- no_pending_write(path),
         :ok <- regular_file(path),
         {:ok, bytes} <- File.read(path),
         {:ok, state} <- Codec.decode(bytes, identity) do
      {:ok, Map.merge(state, %{path: path, identity: identity, file_hash: digest(bytes), file_size: byte_size(bytes)})}
    end
  end

  @spec initialize(Path.t(), map(), [map()]) :: {:ok, map()} | {:error, term()}
  def initialize(path, identity, baselines) do
    with :ok <- plain_path(path),
         :ok <- no_pending_write(path),
         {:error, :enoent} <- File.lstat(path),
         {:ok, bytes} <- Codec.initial_bytes(identity, baselines),
         :ok <- write(path, [:write, :exclusive, :binary, :raw], bytes) do
      load(path, identity)
    else
      {:ok, _} -> {:error, :budget_ledger_exists}
      {:error, _} = error -> error
    end
  end

  @spec verify(map()) :: :ok | {:error, term()}
  def verify(ledger) do
    with {:ok, _bytes} <- verified_bytes(ledger), do: :ok
  end

  @doc "Retains an accounting hold; only explicit operator reconciliation may retire it."
  @spec block(map()) :: :ok | {:error, term()}
  def block(%{path: path}) do
    with :ok <- plain_path(path) do
      write(path <> ".blocked", [:write, :exclusive, :binary, :raw], "managed_usage_requires_reconciliation\n")
    end
  end

  def block(_ledger), do: {:error, :invalid_budget_ledger}

  @spec observe(map(), String.t(), pos_integer(), String.t(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def observe(ledger, issue_id, generation, thread_id, cumulative) do
    with {:ok, bytes} <- verified_bytes(ledger),
         {:ok, next, row} <- Codec.prepare(ledger, issue_id, generation, thread_id, cumulative) do
      append_observation(next, row, bytes)
    end
  end

  @doc "Appends an explicit operator correction for a zero-baseline issue with no observed execution."
  @spec correct_unstarted_floor(map(), map()) :: {:ok, map()} | {:error, term()}
  def correct_unstarted_floor(ledger, attrs) do
    with {:ok, bytes} <- verified_bytes(ledger),
         {:ok, next, row} <- Correction.prepare(ledger, attrs, bytes) do
      append_observation(next, row, bytes)
    end
  end

  @doc "Records an operator-verified new issue without renewing usage or granting execution authority."
  @spec register_new_issue(map(), map()) :: {:ok, map()} | {:error, term()}
  def register_new_issue(ledger, attrs) do
    with {:ok, bytes} <- verified_bytes(ledger),
         {:ok, next, row} <- Registration.prepare(ledger, attrs, bytes) do
      append_observation(next, row, bytes)
    end
  end

  defp append_observation(next, nil, _bytes), do: {:ok, next}

  defp append_observation(next, row, bytes) do
    line = Codec.encode(row)
    expected = bytes <> line
    intent = next.path <> ".pending"

    with :ok <- write(intent, [:write, :exclusive, :binary, :raw], digest(expected)),
         :ok <- write(next.path, [:write, :append, :binary, :raw], line),
         {:ok, ^expected} <- File.read(next.path),
         :ok <- File.rm(intent) do
      {:ok, %{next | file_hash: digest(expected), file_size: byte_size(expected)}}
    else
      _ -> {:error, :budget_write_pending_reconciliation}
    end
  end

  defp verified_bytes(%{path: path, file_hash: hash, file_size: size}) do
    with :ok <- plain_path(path),
         :ok <- no_pending_write(path),
         :ok <- regular_file(path),
         {:ok, bytes} <- File.read(path),
         true <- byte_size(bytes) == size and digest(bytes) == hash do
      {:ok, bytes}
    else
      _ -> {:error, :budget_ledger_changed_or_unavailable}
    end
  end

  defp verified_bytes(_ledger), do: {:error, :invalid_budget_ledger}

  defp no_pending_write(path) do
    if Enum.all?([".pending", ".blocked"], &(File.lstat(path <> &1) == {:error, :enoent})),
      do: :ok,
      else: {:error, :budget_write_pending_reconciliation}
  end

  defp write(path, modes, bytes) do
    case File.open(path, modes) do
      {:ok, io} ->
        result = with :ok <- :file.write(io, bytes), do: :file.sync(io)
        close = File.close(io)
        write_result(result, close)

      {:error, reason} ->
        {:error, {:budget_write_failed, reason}}
    end
  end

  defp write_result(:ok, :ok), do: :ok
  defp write_result({:error, reason}, _close), do: {:error, {:budget_write_uncertain, reason}}
  defp write_result(:ok, {:error, reason}), do: {:error, {:budget_close_uncertain, reason}}

  defp regular_file(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :regular}} -> :ok
      _ -> {:error, :budget_ledger_missing_or_not_regular}
    end
  end

  defp plain_path(path) when is_binary(path) do
    if Path.type(path) == :absolute and Path.expand(path) == path and
         not String.starts_with?(path, ["//", "\\\\"]) and plain_ancestors?(Path.dirname(path)),
       do: :ok,
       else: {:error, :invalid_budget_path}
  end

  defp plain_path(_path), do: {:error, :invalid_budget_path}

  defp plain_ancestors?(path) do
    case File.lstat(path) do
      {:ok, %File.Stat{type: :directory}} ->
        parent = Path.dirname(path)
        parent == path or plain_ancestors?(parent)

      _ ->
        false
    end
  end

  defp digest(bytes), do: :crypto.hash(:sha256, bytes)
end
