Code.require_file("../support/managed_responsibility_fixture.exs", __DIR__)

defmodule SymphonyElixir.RetainedGrantProofGrantEvidenceTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.ManagedResponsibilityFixture, as: Fixture
  alias SymphonyElixir.RetainedGrantProof.GrantEvidence

  @now 10_000
  @root_fixture System.get_env("HGS600_GRANT_FIXTURE")

  @tag skip: is_nil(@root_fixture)
  test "a root-pinned manifest uses the full loader and denies digest, expiry and runner mismatch" do
    path = Path.join(@root_fixture, "manifest.json")
    bytes = File.read!(path)
    digest = Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    assert {:ok, evidence} = GrantEvidence.load(path, digest, Fixture.context(), Fixture.issue(1).id, @now)
    assert evidence.original_grant_digest == digest
    assert evidence.max_tokens == 500_000
    assert {:error, _} = GrantEvidence.load(path, String.duplicate("0", 64), Fixture.context(), Fixture.issue(1).id, @now)
    assert {:error, _} = GrantEvidence.load(path, digest, Fixture.context(), Fixture.issue(1).id, 70_000)
    assert {:error, _} = GrantEvidence.load(path, digest, %{Fixture.context() | runner_id: "wrong"}, Fixture.issue(1).id, @now)
    assert File.read!(path) == bytes
  end

  test "existing decoder derives the original scope, paired allowance and expiry without granting execution" do
    payload = Fixture.payload(@now)
    bytes = Jason.encode!(payload)
    assert {:ok, evidence} = GrantEvidence.decode(bytes, Fixture.context(), Fixture.issue(1).id, @now)
    assert evidence.original_grant_digest == Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
    assert evidence.context == Fixture.context()
    assert evidence.issue_id == Fixture.issue(1).id
    assert evidence.owner_id == "owner-test"
    assert evidence.scope.issue_id == evidence.issue_id
    assert evidence.scope.repository == Fixture.context().repository_ref
    assert evidence.accountable_id == "accountable-1"
    assert evidence.responsible_id == "responsible-1"
    assert evidence.actor_id == "runner-test"
    assert evidence.max_tokens == 500_000
    assert evidence.expires_at_ms == 70_000
    refute Map.has_key?(evidence, :signature)
    refute Map.has_key?(evidence, :ready)

    bounded =
      payload
      |> put_in(["entries", Access.at(0), "responsible", "budget", "max_tokens"], 400_000)
      |> put_in(["entries", Access.at(0), "responsible", "expires_at_ms"], 60_000)

    assert {:ok, bounded_evidence} = GrantEvidence.decode(Jason.encode!(bounded), Fixture.context(), Fixture.issue(1).id, @now)
    assert bounded_evidence.max_tokens == 400_000
    assert bounded_evidence.expires_at_ms == 60_000
  end

  test "explicit progress grant has no task token maximum but keeps exact pair and expiry" do
    progress =
      Fixture.payload(@now)
      |> update_in(["entries", Access.at(0)], fn entry ->
        Enum.reduce(["accountable", "responsible"], entry, fn role, current ->
          current
          |> put_in([role, "budget", "mode"], "progress_scoped")
          |> put_in([role, "budget", "max_tokens"], nil)
        end)
      end)

    assert {:ok, evidence} =
             GrantEvidence.decode(Jason.encode!(progress), Fixture.context(), Fixture.issue(1).id, @now)

    assert evidence.max_tokens == nil
    assert evidence.expires_at_ms == 70_000

    mismatched =
      progress
      |> put_in(["entries", Access.at(0), "responsible", "budget", "mode"], "finite")
      |> put_in(["entries", Access.at(0), "responsible", "budget", "max_tokens"], 500_000)

    assert {:error, _} = GrantEvidence.decode(Jason.encode!(mismatched), Fixture.context(), Fixture.issue(1).id, @now)
    assert {:error, _} = GrantEvidence.decode(Jason.encode!(progress), Fixture.context(), Fixture.issue(1).id, 70_000)
  end

  test "routing, pair, authority, expiry and schema failures cannot produce grant evidence" do
    payload = Fixture.payload(@now)
    first = hd(payload["entries"])

    invalid = [
      Map.put(payload, "schema_version", 2),
      Map.put(payload, "authority_ref", ""),
      Map.put(payload, "entries", []),
      Map.put(payload, "entries", [first, first]),
      put_in(payload, ["entries", Access.at(0), "owner_id"], "other-owner"),
      put_in(payload, ["entries", Access.at(0), "responsible", "actor_id"], "other-runner"),
      put_in(payload, ["entries", Access.at(0), "responsible", "parent_delegation_id"], "other-parent"),
      put_in(payload, ["entries", Access.at(0), "responsible", "scope", "repository"], "other/repo"),
      put_in(payload, ["entries", Access.at(0), "responsible", "budget", "max_children"], 1),
      put_in(payload, ["entries", Access.at(0), "accountable", "expires_at_ms"], @now),
      put_in(payload, ["entries", Access.at(0), "responsible", "expires_at_ms"], @now)
    ]

    for candidate <- invalid do
      assert {:error, _} = GrantEvidence.decode(Jason.encode!(candidate), Fixture.context(), Fixture.issue(1).id, @now)
    end

    bytes = Jason.encode!(payload)

    for key <- Map.keys(Fixture.context()) do
      assert {:error, _} = GrantEvidence.decode(bytes, Map.put(Fixture.context(), key, "wrong"), Fixture.issue(1).id, @now)
    end

    assert {:error, _} = GrantEvidence.decode(bytes, Map.put(Fixture.context(), :authority, true), Fixture.issue(1).id, @now)
    assert {:error, _} = GrantEvidence.decode(bytes, Fixture.context(), "missing-issue", @now)
    assert {:error, _} = GrantEvidence.decode("{broken}", Fixture.context(), Fixture.issue(1).id, @now)
    assert {:error, _} = GrantEvidence.load("relative", nil, Fixture.context(), Fixture.issue(1).id, @now)
  end
end
