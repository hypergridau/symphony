defmodule SymphonyElixir.ManagedTokenBudget.RetainedEvidence do
  @moduledoc "Read-only retained-prefix evidence; neither grant authority nor a replacement accounting ledger."

  alias SymphonyElixir.ManagedTokenBudget.Codec

  @keys [:minimum_total, :prefix_hash, :prefix_size]
  @hash ~r/\A[0-9a-f]{64}\z/

  @spec validate_floor(term()) :: :ok | {:error, term()}
  def validate_floor(floor) when is_map(floor) do
    if Enum.sort(Map.keys(floor)) == @keys and
         is_binary(floor.prefix_hash) and String.valid?(floor.prefix_hash) and Regex.match?(@hash, floor.prefix_hash) and
         is_integer(floor.prefix_size) and floor.prefix_size > 0 and
         is_integer(floor.minimum_total) and floor.minimum_total >= 0,
       do: :ok,
       else: {:error, :invalid_retained_budget_floor}
  end

  def validate_floor(_), do: {:error, :invalid_retained_budget_floor}

  @doc false
  @spec read(map(), String.t(), map(), (map() -> {:ok, binary()} | {:error, term()})) :: {:ok, map()} | {:error, term()}
  def read(ledger, issue_id, floor, reader) do
    with {:ok, bytes} <- reader.(ledger),
         {:ok, evidence} <- verify(bytes, ledger.identity, issue_id, floor),
         {:ok, ^bytes} <- reader.(ledger) do
      {:ok, evidence}
    else
      {:error, _} = error -> error
      _ -> {:error, :retained_budget_snapshot_changed}
    end
  end

  @spec verify(binary(), map(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def verify(bytes, identity, issue_id, floor) when is_binary(bytes) and is_binary(issue_id) do
    with :ok <- validate_floor(floor),
         true <- floor.prefix_size <= byte_size(bytes),
         prefix = binary_part(bytes, 0, floor.prefix_size),
         true <- hex(prefix) == floor.prefix_hash,
         {:ok, current} <- Codec.decode(bytes, identity),
         {:ok, retained} <- Codec.decode(prefix, identity),
         {:ok, total} <- Map.fetch(current.issue_totals, issue_id),
         {:ok, prior_total} <- Map.fetch(retained.issue_totals, issue_id),
         true <- prior_total >= floor.minimum_total and total >= prior_total do
      {:ok,
       %{
         ledger_identity: identity,
         verified_prefix_sha256: hex(bytes),
         verified_prefix_size: byte_size(bytes),
         cumulative_total: total,
         minimum_prefix_sha256: floor.prefix_hash,
         minimum_prefix_size: floor.prefix_size,
         minimum_total: floor.minimum_total
       }}
    else
      _ -> {:error, :retained_budget_evidence_mismatch}
    end
  end

  def verify(_, _, _, _), do: {:error, :retained_budget_arguments_invalid}

  defp hex(bytes), do: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
end
