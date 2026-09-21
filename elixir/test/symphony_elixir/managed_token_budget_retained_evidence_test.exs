defmodule SymphonyElixir.ManagedTokenBudgetRetainedEvidenceTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedTokenBudget, as: Budget
  alias SymphonyElixir.ManagedTokenBudget.RetainedEvidence

  @identity %{pool_key: "test", repository_ref: "test/repo", managed_project_profile_id: "test-profile"}
  @issue "11111111-1111-4111-8111-111111111111"
  @other "22222222-2222-4222-8222-222222222222"

  setup do
    dir = Path.join(System.tmp_dir!(), "retained-budget-#{Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)}")
    File.mkdir!(dir)
    path = Path.join(dir, "usage.jsonl")

    baseline = %{
      issue_id: @issue,
      known_minimum_tokens: 100,
      continuation_floor: 1,
      evidence_ref: "test:fixture",
      authority_ref: "test:fixture"
    }

    {:ok, ledger} = Budget.initialize(path, @identity, [baseline])
    floor = %{prefix_hash: hex(ledger.file_hash), prefix_size: ledger.file_size, minimum_total: 100}
    %{path: path, ledger: ledger, floor: floor}
  end

  test "retained prefix and accumulated usage survive reload without writes", %{path: path, ledger: ledger, floor: floor} do
    {:ok, current} = Budget.observe(ledger, @issue, 1, "test-thread", 25)
    before = File.read!(path)

    for _ <- 1..2 do
      assert {:ok, evidence} = Budget.retained_evidence(path, @identity, @issue, floor)
      assert evidence.ledger_identity == @identity
      assert evidence.cumulative_total == 125
      assert evidence.verified_prefix_sha256 == hex(current.file_hash)
      assert evidence.verified_prefix_size == byte_size(before)
      assert evidence.minimum_prefix_sha256 == floor.prefix_hash
      assert evidence.minimum_prefix_size == floor.prefix_size
      assert evidence.minimum_total == 100
      assert File.read!(path) == before
      refute File.exists?(path <> ".pending")
      refute File.exists?(path <> ".blocked")
    end
  end

  test "malformed floors, partial rows, changed prefixes and missing identities deny", %{path: path, floor: floor} do
    before = File.read!(path)

    floors = [
      nil,
      %{},
      Map.put(floor, :authority, true),
      %{floor | prefix_size: 0},
      %{floor | prefix_size: floor.prefix_size + 1},
      %{floor | prefix_size: floor.prefix_size - 1},
      %{floor | prefix_hash: String.duplicate("0", 64)},
      %{floor | minimum_total: 101},
      %{floor | minimum_total: -1},
      %{floor | prefix_hash: <<255>>}
    ]

    for invalid <- floors do
      assert {:error, _} = Budget.retained_evidence(path, @identity, @issue, invalid)
      assert File.read!(path) == before
    end

    assert {:error, _} = Budget.retained_evidence(path, @identity, @other, floor)
    assert {:error, _} = Budget.retained_evidence(path, %{@identity | pool_key: "other"}, @issue, floor)
    File.write!(path, String.replace(before, "test:fixture", "test:changed"))
    changed = File.read!(path)
    assert {:error, _} = Budget.retained_evidence(path, @identity, @issue, floor)
    assert File.read!(path) == changed
  end

  test "pending and blocked accounting stays frozen", %{path: path, floor: floor} do
    before = File.read!(path)

    for suffix <- [".pending", ".blocked"] do
      File.write!(path <> suffix, "retained-test-hold", [:exclusive])
      assert {:error, _} = Budget.retained_evidence(path, @identity, @issue, floor)
      assert File.read!(path <> suffix) == "retained-test-hold"
      assert File.read!(path) == before
    end
  end

  test "a valid retained prefix cannot hide malformed full bytes", %{path: path, floor: floor} do
    bytes = File.read!(path) <> "{broken}\n"
    File.write!(path, bytes)
    assert {:error, _} = Budget.retained_evidence(path, @identity, @issue, floor)
    assert File.read!(path) == bytes
  end

  test "a hash-matching partial row is not a canonical retained ledger", %{path: path, floor: floor} do
    bytes = File.read!(path)
    prefix = binary_part(bytes, 0, byte_size(bytes) - 1)
    partial = %{floor | prefix_hash: hex(:crypto.hash(:sha256, prefix)), prefix_size: byte_size(prefix)}
    assert {:error, _} = Budget.retained_evidence(path, @identity, @issue, partial)
    assert File.read!(path) == bytes
  end

  test "replacement between reads rejects evidence without repairing the replacement", %{path: path, ledger: ledger, floor: floor} do
    replacement = String.replace(File.read!(path), "test:fixture", "test:replacement")
    Process.put(:retained_snapshot_reads, 0)

    reader = fn original ->
      count = Process.get(:retained_snapshot_reads)
      Process.put(:retained_snapshot_reads, count + 1)
      if count == 1, do: File.write!(path, replacement)
      with :ok <- Budget.verify(original), do: File.read(path)
    end

    assert {:error, _} = RetainedEvidence.read(ledger, @issue, floor, reader)
    assert Process.get(:retained_snapshot_reads) == 2
    assert File.read!(path) == replacement
  end

  defp hex(bytes), do: Base.encode16(bytes, case: :lower)
end
