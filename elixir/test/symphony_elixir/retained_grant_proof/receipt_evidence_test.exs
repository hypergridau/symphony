defmodule SymphonyElixir.RetainedGrantProof.ReceiptEvidenceTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.RetainedGrantProof.ReceiptEvidence

  @wire_keys ~w(projectionId reservationId receiptId receiptKind executionCapacityState scopeState reservationState generation evidenceRef acceptedHead replayed)
  @atom_keys ~w(projection_id reservation_id receipt_id receipt_kind execution_capacity_state scope_state reservation_state generation evidence_ref accepted_head replayed)a

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

  defp receipt(
         kind \\ "termination_confirmed",
         outcome \\ "completed",
         states \\ {"held", "claimed"}
       ) do
    c = claim()

    semantic =
      Map.delete(c, :projection_id)
      |> Map.merge(%{
        contract_version: "work-package-cleanup-receipt.v1",
        receipt_kind: kind,
        terminal_outcome: outcome,
        observed_at: "2026-09-18T18:00:00Z",
        evidence_ref: "evidence",
        accepted_head: "head"
      })

    id = independent_id(semantic)
    {scope, reservation} = states

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
      accepted_head: "head",
      replayed: false
    }

    Map.merge(semantic, %{receipt_id: id, acknowledgement: ack})
  end

  defp independent_id(semantic) do
    fields = semantic |> Enum.sort_by(fn {key, _} -> Atom.to_string(key) end)

    pairs =
      Enum.map(fields, fn {key, value} ->
        [Jason.encode!(Atom.to_string(key)), ?:, Jason.encode!(value)]
      end)

    bytes = IO.iodata_to_binary([?{, Enum.intersperse(pairs, ?,), ?}])
    "cleanup-" <> Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
  end

  test "valid terminal receipts retain held cleanup and accept both canonical acknowledgement encodings" do
    for outcome <- ["completed", "failed", "blocked"],
        {kind, states} <- [
          {"termination_confirmed", {"held", "claimed"}},
          {"termination_confirmed", {"released", "released"}},
          {"repository_cleanup_verified", {"released", "released"}}
        ] do
      r = receipt(kind, outcome, states)

      assert {:ok, %{receipt: ^r, acknowledgement: ack}} =
               ReceiptEvidence.validate_stored_readonly(r, claim())

      assert ack.scope_state == elem(states, 0)

      wire =
        Map.new(Enum.zip(@atom_keys, @wire_keys), fn {atom, key} ->
          {key, Map.fetch!(ack, atom)}
        end)

      assert {:ok, %{acknowledgement: ^ack}} =
               ReceiptEvidence.validate_stored_readonly(%{r | acknowledgement: wire}, claim())
    end

    r = receipt()

    assert {:ok, %{acknowledgement: %{replayed: true}}} =
             ReceiptEvidence.validate_stored_readonly(
               %{r | acknowledgement: %{r.acknowledgement | replayed: true}},
               claim()
             )
  end

  test "semantic integrity refuses altered or malformed receipt values without signing authority" do
    r = receipt()

    changed =
      Map.delete(r, :acknowledgement)
      |> Map.delete(:receipt_id)
      |> Map.put(:reservation_nonce, "forged")

    forged =
      changed
      |> Map.put(:receipt_id, independent_id(changed))
      |> Map.put(:acknowledgement, r.acknowledgement)

    cases = [
      forged,
      Map.delete(r, :acknowledgement),
      Map.delete(r, :accepted_head),
      Map.put(r, :receipt_id, "cleanup-tampered"),
      Map.put(r, :generation, 0),
      Map.put(r, :evidence_ref, <<255>>),
      Map.put(r, :accepted_head, self()),
      Map.put(r, :signature, nil),
      Map.put(r, :attested_at, nil),
      Map.put(r, :scope_keys, ["repository", "cleanup"]),
      Map.put(r, :scope_keys, ["cleanup", "cleanup"]),
      Map.put(r, :unexpected, true),
      Map.delete(r, :accepted_head) |> Map.put("accepted_head", "head"),
      receipt("termination_confirmed", "completed", {"released", "claimed"}),
      receipt("repository_cleanup_verified", "completed", {"held", "claimed"})
    ]

    for bad <- cases do
      assert {:error, :retained_cleanup_receipt_invalid} =
               ReceiptEvidence.validate_stored_readonly(bad, claim())
    end
  end

  test "unknown and duplicate acknowledgement aliases cannot disappear during normalization" do
    r = receipt()
    ack = r.acknowledgement
    duplicate = Map.delete(ack, :replayed) |> Map.put("scopeState", "held")
    assert map_size(duplicate) == 11

    cases = [
      duplicate,
      Map.put(ack, "scopeState", "held"),
      Map.delete(ack, :replayed) |> Map.put(nil, false),
      Map.delete(ack, :replayed) |> Map.put("unknown", false),
      Map.put(ack, :evidence_ref, <<255>>),
      Map.put(ack, :evidence_ref, fn -> :ok end),
      Map.put(ack, :generation, 0),
      Map.put(ack, :replayed, "true"),
      Map.put(ack, :execution_capacity_state, "held"),
      Map.put(ack, :projection_id, "wrong"),
      Map.put(ack, :accepted_head, "wrong"),
      Map.delete(ack, :scope_state)
    ]

    for bad <- cases do
      assert {:error, :retained_cleanup_receipt_invalid} =
               ReceiptEvidence.validate_stored_readonly(%{r | acknowledgement: bad}, claim())
    end
  end

  test "independent claim must have the exact typed authority tuple" do
    c = claim()

    cases = [
      nil,
      [],
      Map.delete(c, :runtime_lease_id),
      Map.put(c, :unexpected, true),
      Map.put(c, :issue_id, <<255>>),
      Map.put(c, :issue_id, fn -> :ok end),
      Map.put(c, :process_id, self()),
      Map.put(c, :generation, 0),
      Map.put(c, :generation, true),
      Map.put(c, :scope_keys, []),
      Map.put(c, :scope_keys, [" "]),
      Map.put(c, :scope_keys, ["cleanup", "cleanup"]),
      Map.put(c, :scope_keys, ["repository", "cleanup"]),
      Map.put(c, :reservation_nonce, "wrong")
    ]

    for bad <- cases do
      assert {:error, :retained_cleanup_receipt_invalid} =
               ReceiptEvidence.validate_stored_readonly(receipt(), bad)
    end

    assert {:error, :retained_cleanup_receipt_invalid} =
             ReceiptEvidence.validate_stored_readonly(nil, c)
  end
end
