defmodule SymphonyElixir.RetainedGrantProof.GrantEvidence do
  @moduledoc """
  Read-only evidence from the original root-pinned responsibility manifest.
  Only installed broker configuration selects the path, digest and context.
  Current revocation, claim, ledger, history and native authority remain separate
  checks. This module neither signs proofs nor admits execution.
  """

  alias SymphonyElixir.ManagedResponsibility
  alias SymphonyElixir.RetainedGrantProof.TrustedFile

  @context_keys [:managed_project_profile_id, :pool_key, :repository_ref, :runner_id]

  @spec load(Path.t(), String.t(), map(), String.t(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def load(path, expected_digest, context, issue_id, now_ms) do
    with {:ok, bytes} <- TrustedFile.load(path, expected_digest, 262_144) do
      decode(bytes, context, issue_id, now_ms)
    end
  end

  @doc false
  @spec decode(binary(), map(), String.t(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def decode(bytes, context, issue_id, now_ms)
      when is_binary(bytes) and is_map(context) and is_binary(issue_id) and is_integer(now_ms) and now_ms >= 0 do
    with true <- Enum.sort(Map.keys(context)) == @context_keys,
         {:ok, payload} <- Jason.decode(bytes),
         {:ok, manifest} <- ManagedResponsibility.decode(payload, context, now_ms),
         [entry] <- Enum.filter(manifest.entries, &(&1.issue_id == issue_id)),
         {:ok, maximum} <- grant_limit(entry.accountable.budget, entry.responsible.budget) do
      {:ok,
       %{
         original_grant_digest: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower),
         context: context,
         issue_id: entry.issue_id,
         identifier: entry.identifier,
         authority_ref: manifest.authority_ref,
         owner_id: entry.owner_id,
         scope: entry.responsible.scope,
         accountable_id: entry.accountable.id,
         responsible_id: entry.responsible.id,
         actor_id: entry.responsible.actor_id,
         delegations: %{accountable: entry.accountable, responsible: entry.responsible},
         max_tokens: maximum,
         expires_at_ms: min(entry.accountable.expires_at_ms, entry.responsible.expires_at_ms)
       }}
    else
      _ -> {:error, :retained_grant_evidence_invalid}
    end
  end

  def decode(_, _, _, _), do: {:error, :retained_grant_evidence_invalid}

  defp grant_limit(%{mode: :progress_scoped, max_tokens: nil}, %{mode: :progress_scoped, max_tokens: nil}),
    do: {:ok, nil}

  defp grant_limit(%{max_tokens: accountable} = parent, %{max_tokens: responsible} = child)
       when is_integer(accountable) and accountable > 0 and is_integer(responsible) and responsible > 0 do
    if Map.get(parent, :mode, :finite) == :finite and Map.get(child, :mode, :finite) == :finite,
      do: {:ok, min(accountable, responsible)},
      else: {:error, :retained_grant_evidence_invalid}
  end

  defp grant_limit(_, _), do: {:error, :retained_grant_evidence_invalid}
end
