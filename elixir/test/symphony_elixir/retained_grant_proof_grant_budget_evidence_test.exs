defmodule SymphonyElixir.RetainedGrantProofGrantBudgetEvidenceTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedTokenBudget, as: Budget
  alias SymphonyElixir.RetainedGrantProof.GrantBudgetEvidence

  @fixture System.get_env("HGS600_BUDGET_FIXTURE")

  test "missing bindings and request overrides never provide evidence" do
    for invalid <- [nil, %{}, %{request_nonce: "caller"}, %{"schema_version" => 1}] do
      assert {:error, _} = GrantBudgetEvidence.load(invalid, 10_000)
    end

    assert {:error, _} = GrantBudgetEvidence.load_config("relative", nil, 10_000)
  end

  @tag skip: is_nil(@fixture)
  test "pinned configuration joins the actual original manifest and cumulative ledger without writes" do
    path = Path.join(@fixture, "config.json")
    bytes = File.read!(path)
    digest = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    assert {:ok, evidence} = GrantBudgetEvidence.load_config(path, digest, 10_000)
    assert evidence.schema_version == 1
    assert evidence.grant.max_tokens == 500_000
    assert evidence.grant.expires_at_ms == 70_000
    assert evidence.budget.cumulative_total == 125
    assert evidence.budget.minimum_total == 100
    assert evidence.budget.ledger_identity.pool_key == evidence.grant.context.pool_key
    assert evidence.budget.ledger_identity.repository_ref == evidence.grant.scope.repository
    assert File.read!(path) == bytes
    assert {:error, _} = GrantBudgetEvidence.load_config(path, String.duplicate("0", 64), 10_000)
    assert {:error, _} = GrantBudgetEvidence.load_config(path, digest, 70_000)
  end

  @tag skip: is_nil(@fixture)
  test "installed binding mismatches cannot weaken the original scope or ledger checkpoint" do
    %{"binding" => raw} = @fixture |> Path.join("config.json") |> File.read!() |> Jason.decode!()
    keys = ~w(manifest_path manifest_sha256 ledger_path context issue_id owner_id accountable_id responsible_id scope floor ledger_checkpoint_sha256 ledger_checkpoint_size)a
    binding = convert(raw, keys)

    binding = %{
      binding
      | context: convert(binding.context, ~w(pool_key repository_ref managed_project_profile_id runner_id)a),
        floor: convert(binding.floor, ~w(prefix_hash prefix_size minimum_total)a)
    }

    {:ok, _} = GrantBudgetEvidence.load(binding, 10_000)

    for {key, value} <- [
          {:manifest_sha256, String.duplicate("0", 64)},
          {:owner_id, "other"},
          {:accountable_id, "other"},
          {:responsible_id, "other"},
          {:scope, %{}},
          {:issue_id, "other"},
          {:ledger_checkpoint_sha256, String.duplicate("0", 64)},
          {:ledger_checkpoint_size, binding.ledger_checkpoint_size - 1},
          {:floor, %{binding.floor | minimum_total: 126}},
          {:context, %{binding.context | runner_id: "other"}}
        ] do
      assert {:error, _} = GrantBudgetEvidence.load(Map.put(binding, key, value), 10_000)
    end

    assert {:error, _} = GrantBudgetEvidence.load(Map.put(binding, :request_override, true), 10_000)
  end

  defp convert(map, keys), do: Map.new(keys, &{&1, map[Atom.to_string(&1)]})

  @tag skip: is_nil(@fixture)
  test "original allowance boundary denies exhaustion without changing accounting" do
    for total <- [499_999, 500_000, 500_001] do
      {binding, _ledger} = synthetic_binding(total)
      before = File.read!(binding.ledger_path)
      result = GrantBudgetEvidence.load(binding, 10_000)
      if total < 500_000, do: assert(match?({:ok, _}, result)), else: assert(match?({:error, _}, result))
      assert File.read!(binding.ledger_path) == before
    end
  end

  @tag skip: is_nil(@fixture)
  test "valid older checkpoint and accounting holds cannot satisfy installed current checkpoint" do
    {binding, ledger} = synthetic_binding(100)
    old = File.read!(binding.ledger_path)
    {:ok, current} = Budget.observe(ledger, binding.issue_id, 1, "synthetic-thread", 25)
    binding = %{binding | ledger_checkpoint_sha256: hex(current.file_hash), ledger_checkpoint_size: current.file_size}
    assert {:ok, _} = GrantBudgetEvidence.load(binding, 10_000)
    File.write!(binding.ledger_path, old)
    assert {:error, _} = GrantBudgetEvidence.load(binding, 10_000)
    assert File.read!(binding.ledger_path) == old

    for suffix <- [".pending", ".blocked"] do
      {held, _ledger} = synthetic_binding(100)
      before = File.read!(held.ledger_path)
      File.write!(held.ledger_path <> suffix, "synthetic-hold", [:exclusive])
      assert {:error, _} = GrantBudgetEvidence.load(held, 10_000)
      assert File.read!(held.ledger_path) == before
      assert File.read!(held.ledger_path <> suffix) == "synthetic-hold"
    end
  end

  defp synthetic_binding(total) do
    %{"binding" => raw} = @fixture |> Path.join("config.json") |> File.read!() |> Jason.decode!()
    binding = convert(raw, ~w(manifest_path manifest_sha256 ledger_path context issue_id owner_id accountable_id responsible_id scope floor ledger_checkpoint_sha256 ledger_checkpoint_size)a)
    binding = %{binding | context: convert(binding.context, ~w(pool_key repository_ref managed_project_profile_id runner_id)a)}
    dir = Path.join(System.tmp_dir!(), "grant-budget-#{Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)}")
    File.mkdir!(dir)
    path = Path.join(dir, "usage.jsonl")

    baseline = %{
      issue_id: binding.issue_id,
      known_minimum_tokens: total,
      continuation_floor: 1,
      evidence_ref: "test:synthetic",
      authority_ref: "test:synthetic"
    }

    {:ok, ledger} = Budget.initialize(path, Map.take(binding.context, ~w(pool_key repository_ref managed_project_profile_id)a), [baseline])
    floor = %{prefix_hash: hex(ledger.file_hash), prefix_size: ledger.file_size, minimum_total: total}

    installed = %{
      binding
      | ledger_path: path,
        floor: floor,
        ledger_checkpoint_sha256: hex(ledger.file_hash),
        ledger_checkpoint_size: ledger.file_size
    }

    {installed, ledger}
  end

  defp hex(bytes), do: Base.encode16(bytes, case: :lower)
end
