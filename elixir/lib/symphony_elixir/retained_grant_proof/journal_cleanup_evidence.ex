defmodule SymphonyElixir.RetainedGrantProof.JournalCleanupEvidence do
  @moduledoc """
  Validates cleanup receipt data; it does not authenticate snapshot provenance.
  A trusted caller must first use SnapshotEvidence.match_claim/5 on independently
  pinned original journal bytes. This constructor cannot enforce that precondition.
  Termination acknowledgement is required. Repository cleanup is optional because
  termination may retain a held scope; absence does not establish cleanup or release.
  The result is inert journal data, not current authority, native quiescence,
  current workspace head, signature validity, or permission to act.
  The supplied journal digest is checked for syntax, not authenticated here.
  """
  alias SymphonyElixir.RetainedGrantProof.ReceiptEvidence
  @outer_kinds ["termination_confirmed", "repository_cleanup_verified"]

  @spec new(term()) :: {:ok, map()} | {:error, :journal_cleanup_evidence_invalid}
  def new({:ok, snapshot}) when is_map(snapshot) do
    with true <- exact_snapshot_shape?(snapshot),
         true <- is_map(snapshot.claim),
         true <- lower_hex64?(snapshot.journal_sha256),
         {:ok, receipts} <- validate_cleanup(snapshot.cleanup_receipts, snapshot.claim) do
      {:ok, %{claim: snapshot.claim, journal_sha256: snapshot.journal_sha256, cleanup_receipts: receipts}}
    else
      _ -> invalid()
    end
  end

  def new(_), do: invalid()

  defp exact_snapshot_shape?(snapshot) do
    map_size(snapshot) == 3 and
      Enum.sort(Map.keys(snapshot)) == [:claim, :cleanup_receipts, :journal_sha256]
  end

  defp validate_cleanup(receipts, claim) when is_map(receipts) do
    keys = Map.keys(receipts)

    if Map.has_key?(receipts, "termination_confirmed") and
         Enum.all?(keys, &(&1 in @outer_kinds)) do
      validate_receipts(Enum.sort(keys), receipts, claim)
    else
      invalid()
    end
  end

  defp validate_cleanup(_, _), do: invalid()

  defp validate_receipts(keys, receipts, claim) do
    Enum.reduce_while(keys, {:ok, %{}}, fn kind, {:ok, acc} ->
      validate_receipt(kind, receipts, claim, acc)
    end)
  end

  defp validate_receipt(kind, receipts, claim, acc) do
    case validate_one(kind, Map.fetch!(receipts, kind), claim) do
      {:ok, validated} -> {:cont, {:ok, Map.put(acc, kind, validated)}}
      _ -> {:halt, invalid()}
    end
  end

  defp validate_one(kind, receipt, claim) do
    with {:ok, %{receipt: original} = validated} <-
           ReceiptEvidence.validate_stored_readonly(receipt, claim),
         {:ok, ^kind} <- Map.fetch(original, :receipt_kind) do
      {:ok, validated}
    else
      _ -> invalid()
    end
  end

  defp lower_hex64?(value) when is_binary(value) and byte_size(value) == 64 do
    Enum.all?(:binary.bin_to_list(value), fn byte -> byte in ?0..?9 or byte in ?a..?f end)
  end

  defp lower_hex64?(_), do: false
  defp invalid, do: {:error, :journal_cleanup_evidence_invalid}
end
