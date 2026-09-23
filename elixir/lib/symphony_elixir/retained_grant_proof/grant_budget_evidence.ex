defmodule SymphonyElixir.RetainedGrantProof.GrantBudgetEvidence do
  @moduledoc """
  Internal read-only collection from installed grant and ledger bindings.
  The broker must obtain the binding from pinned root configuration, never a
  request. Current claim, revocation, history/archive and native checks remain
  mandatory separate steps. These facts are neither a signature nor admission.
  """

  alias SymphonyElixir.ManagedTokenBudget
  alias SymphonyElixir.RetainedGrantProof.GrantEvidence
  alias SymphonyElixir.RetainedGrantProof.TrustedFile

  @keys ~w(manifest_path manifest_sha256 ledger_path context issue_id owner_id accountable_id responsible_id scope floor ledger_checkpoint_sha256 ledger_checkpoint_size)a
  @identity_keys [:pool_key, :repository_ref, :managed_project_profile_id]
  @context_keys [:pool_key, :repository_ref, :managed_project_profile_id, :runner_id]
  @floor_keys [:prefix_hash, :prefix_size, :minimum_total]

  @spec load_config(Path.t(), String.t(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def load_config(path, digest, now_ms) do
    with {:ok, bytes} <- TrustedFile.load(path, digest, 262_144),
         {:ok, payload} <- Jason.decode(bytes),
         true <- is_map(payload) and Enum.sort(Map.keys(payload)) == ["binding", "schema_version"] and payload["schema_version"] === 1,
         {:ok, binding} <- atom_keys(payload["binding"], @keys),
         {:ok, context} <- atom_keys(binding.context, @context_keys),
         {:ok, floor} <- atom_keys(binding.floor, @floor_keys) do
      load(%{binding | context: context, floor: floor}, now_ms)
    else
      _ -> {:error, :retained_grant_budget_binding_invalid}
    end
  end

  @doc false
  @spec load(map(), non_neg_integer()) :: {:ok, map()} | {:error, term()}
  def load(binding, now_ms) when is_map(binding) and is_integer(now_ms) and now_ms >= 0 do
    with true <- Enum.sort(Map.keys(binding)) == Enum.sort(@keys),
         {:ok, grant} <- GrantEvidence.load(binding.manifest_path, binding.manifest_sha256, binding.context, binding.issue_id, now_ms),
         true <-
           grant.owner_id == binding.owner_id and grant.accountable_id == binding.accountable_id and
             grant.responsible_id == binding.responsible_id and Jason.decode!(Jason.encode!(grant.scope)) == binding.scope,
         identity = Map.take(grant.context, @identity_keys),
         {:ok, budget} <- ManagedTokenBudget.retained_evidence(binding.ledger_path, identity, binding.issue_id, binding.floor),
         true <-
           budget.verified_prefix_sha256 == binding.ledger_checkpoint_sha256 and
             budget.verified_prefix_size == binding.ledger_checkpoint_size,
         true <- (is_nil(grant.max_tokens) or budget.cumulative_total < grant.max_tokens) and grant.expires_at_ms > now_ms do
      {:ok, %{schema_version: 1, grant: grant, budget: budget, observed_at_ms: now_ms}}
    else
      _ -> {:error, :retained_grant_budget_evidence_invalid}
    end
  end

  def load(_, _), do: {:error, :retained_grant_budget_evidence_invalid}

  defp atom_keys(payload, keys) when is_map(payload) do
    if Enum.sort(Map.keys(payload)) == Enum.sort(Enum.map(keys, &Atom.to_string/1)),
      do: {:ok, Map.new(keys, &{&1, payload[Atom.to_string(&1)]})},
      else: {:error, :retained_grant_budget_binding_invalid}
  end

  defp atom_keys(_, _), do: {:error, :retained_grant_budget_binding_invalid}
end
