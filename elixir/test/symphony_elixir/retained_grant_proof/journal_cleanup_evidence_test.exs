defmodule SymphonyElixir.RetainedGrantProof.JournalCleanupEvidenceTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.RetainedGrantProof.JournalCleanupEvidence
  @digest String.duplicate("a", 64)
  @atom_keys ~w(projection_id reservation_id receipt_id receipt_kind execution_capacity_state scope_state reservation_state generation evidence_ref accepted_head replayed)a
  @wire_keys ~w(projectionId reservationId receiptId receiptKind executionCapacityState scopeState reservationState generation evidenceRef acceptedHead replayed)

  defp claim do
    %{
      issue_id: "issue",
      managed_project_profile_id: "profile",
      repository_ref: "repo",
      projection_id: "projection",
      reservation_id: "reservation",
      reservation_nonce: "nonce",
      scope_keys: ["cleanup", "repository"],
      runner_id: "runner",
      generation: 7,
      session_id: "session",
      process_id: "process",
      responsible_delegation_id: "delegation",
      execution_fence_token: "fence",
      runtime_lease_id: "lease"
    }
  end

  defp receipt(c, kind, {scope, reservation}, head, style \\ :atom) do
    semantic =
      Map.delete(c, :projection_id)
      |> Map.merge(%{
        contract_version: "work-package-cleanup-receipt.v1",
        receipt_kind: kind,
        terminal_outcome: "failed",
        observed_at: "2026-09-18T18:00:00Z",
        evidence_ref: "evidence",
        accepted_head: head
      })

    id =
      "cleanup-" <>
        (semantic
         |> canonical_object()
         |> then(&:crypto.hash(:sha256, &1))
         |> Base.encode16(case: :lower))

    ack = %{
      projection_id: c.projection_id,
      reservation_id: c.reservation_id,
      receipt_id: id,
      receipt_kind: kind,
      execution_capacity_state: "released",
      scope_state: scope,
      reservation_state: reservation,
      generation: c.generation,
      evidence_ref: "evidence",
      accepted_head: head,
      replayed: false
    }

    ack =
      if style == :wire,
        do: Map.new(Enum.zip(@atom_keys, @wire_keys), fn {atom, key} -> {key, Map.fetch!(ack, atom)} end),
        else: ack

    Map.merge(semantic, %{receipt_id: id, acknowledgement: ack})
  end

  defp canonical_object(map) do
    body =
      map
      |> Enum.sort_by(fn {key, _} -> Atom.to_string(key) end)
      |> Enum.map_join(",", fn {key, value} ->
        Jason.encode!(Atom.to_string(key)) <> ":" <> Jason.encode!(value)
      end)

    "{" <> body <> "}"
  end

  defp snapshot(c, receipts), do: {:ok, %{claim: c, journal_sha256: @digest, cleanup_receipts: receipts}}
  defp termination(c), do: receipt(c, "termination_confirmed", {"held", "claimed"}, "head-a")

  test "acknowledged termination preserves held and released states" do
    for states <- [{"held", "claimed"}, {"released", "released"}] do
      r = receipt(claim(), "termination_confirmed", states, "head-a")
      assert {:ok, evidence} = JournalCleanupEvidence.new(snapshot(claim(), %{"termination_confirmed" => r}))
      ack = evidence.cleanup_receipts["termination_confirmed"].acknowledgement
      assert {ack.scope_state, ack.reservation_state} == states
      assert ack.execution_capacity_state == "released"
    end
  end

  test "both receipts normalize wire ACK and retain different original head facts" do
    c = claim()
    term = termination(c)
    repo = receipt(c, "repository_cleanup_verified", {"released", "released"}, "head-b", :wire)

    assert {:ok, evidence} =
             JournalCleanupEvidence.new(
               snapshot(c, %{
                 "termination_confirmed" => term,
                 "repository_cleanup_verified" => repo
               })
             )

    assert evidence.claim == c
    assert evidence.journal_sha256 == @digest
    assert evidence.cleanup_receipts["termination_confirmed"].receipt == term
    assert evidence.cleanup_receipts["repository_cleanup_verified"].receipt.accepted_head == "head-b"
    assert evidence.cleanup_receipts["repository_cleanup_verified"].acknowledgement.scope_state == "released"
  end

  test "a malformed optional repository receipt cannot be silently ignored" do
    c = claim()
    term = termination(c)
    repo = receipt(c, "repository_cleanup_verified", {"released", "released"}, "head-a")

    for bad <- [nil, [], Map.put(repo, :signature, ""), Map.delete(repo, :acknowledgement), put_in(repo, [:acknowledgement, :projection_id], "other")] do
      assert {:error, :journal_cleanup_evidence_invalid} =
               JournalCleanupEvidence.new(
                 snapshot(c, %{
                   "termination_confirmed" => term,
                   "repository_cleanup_verified" => bad
                 })
               )
    end
  end

  test "outer keys must select the correct receipt kind and contain termination" do
    c = claim()
    term = termination(c)
    repo = receipt(c, "repository_cleanup_verified", {"released", "released"}, "head")

    for receipts <- [
          %{},
          nil,
          [],
          %{"repository_cleanup_verified" => repo},
          %{"termination_confirmed" => term, "unknown" => repo},
          %{termination_confirmed: term},
          %{"termination_confirmed" => term, :termination_confirmed => term},
          %{"termination_confirmed" => repo},
          %{"termination_confirmed" => term, "repository_cleanup_verified" => term}
        ] do
      assert {:error, :journal_cleanup_evidence_invalid} = JournalCleanupEvidence.new(snapshot(c, receipts))
    end
  end

  test "actual receipt validator rejects cross-claim and acknowledgement changes" do
    c = claim()
    term = termination(c)
    forged = termination(%{c | reservation_nonce: "other"})

    for bad <- [forged, put_in(term, [:acknowledgement, :projection_id], "other"), Map.put(term, :signature, "")] do
      assert {:error, :journal_cleanup_evidence_invalid} =
               JournalCleanupEvidence.new(snapshot(c, %{"termination_confirmed" => bad}))
    end
  end

  test "strict result and digest shapes do not grant provenance or tolerate aliases" do
    c = claim()
    {:ok, base} = snapshot(c, %{"termination_confirmed" => termination(c)})

    for digest <- [nil, 123, <<255>>, String.duplicate("A", 64), String.duplicate("a", 63)] do
      assert {:error, :journal_cleanup_evidence_invalid} = JournalCleanupEvidence.new({:ok, %{base | journal_sha256: digest}})
    end

    for value <- [
          nil,
          [],
          %{},
          {:error, :missing},
          {:ok, nil},
          {:ok, Map.put(base, :unexpected, true)},
          {:ok, Map.delete(base, :claim) |> Map.put("claim", c)},
          {:ok, %{base | claim: nil}},
          {:ok, %{base | claim: %{c | reservation_nonce: ""}}}
        ] do
      assert {:error, :journal_cleanup_evidence_invalid} = JournalCleanupEvidence.new(value)
    end
  end
end
