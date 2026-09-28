defmodule SymphonyElixir.RKE2JobAuthCacheVerifierAttemptJournalTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RKE2Job.AuthCacheVerifierAttemptJournal

  @digest String.duplicate("a", 64)
  @image "ghcr.io/hypergridau/symphony-worker@sha256:" <> String.duplicate("b", 64)
  @assignment %{sha256: @digest, seat: "builder"}
  @slot %{
    slot_id: "oauth-slot-1",
    claim_name: "codex-oauth-slot-1",
    claim_uid: "pvc-uid-1",
    lease_id: "11111111-1111-4111-8111-111111111111",
    assignment_sha256: @digest,
    seat: "builder"
  }
  @job_uid "assignment-job-uid"

  setup do
    root = Path.join(System.tmp_dir!(), "symphony-verifier-attempt-" <> Ecto.UUID.generate())
    File.mkdir!(root)
    File.chmod!(root, 0o700)
    on_exit(fn -> File.rm_rf!(root) end)
    %{root: root}
  end

  test "persists a fresh attempt and replays that exact identity after restart", %{root: root} do
    assert {:ok, first} = AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)
    assert first["schemaVersion"] == 1
    assert first["jobUid"] == @job_uid
    assert first["claimUid"] == @slot.claim_uid
    assert first["seat"] == "builder"
    assert first["image"] == @image
    assert String.match?(first["attemptId"], ~r/\A[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\z/)
    assert {:ok, ^first} = AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)

    [path] = Path.wildcard(Path.join(root, "*.auth-verifier-attempt.json"))
    assert File.stat!(path).mode |> Bitwise.band(0o077) |> Kernel.==(0)
  end

  test "holds a changed image or claim under an existing attempt", %{root: root} do
    assert {:ok, _} = AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)

    assert {:held, :auth_cache_verifier_attempt_conflict} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, String.replace(@image, "b", "c"), root)

    assert {:held, :auth_cache_verifier_attempt_conflict} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, %{@slot | claim_uid: "replacement-pvc"}, @image, root)

    assert {:error, :invalid_auth_cache_verifier_attempt} =
             AuthCacheVerifierAttemptJournal.ensure(%{@assignment | seat: "other"}, @job_uid, @slot, @image, root)
  end

  test "holds malformed, linked or public records and rejects unsafe roots", %{root: root} do
    path = Path.join(root, @digest <> "-" <> @job_uid <> ".auth-verifier-attempt.json")
    File.write!(path, "{")
    File.chmod!(path, 0o600)

    assert {:held, :auth_cache_verifier_attempt_invalid} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)

    File.chmod!(path, 0o644)

    assert {:held, :auth_cache_verifier_attempt_read_unavailable} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)

    File.chmod!(path, 0o600)
    File.ln!(path, Path.join(root, "alias.json"))

    assert {:held, :auth_cache_verifier_attempt_read_unavailable} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)

    File.rm!(Path.join(root, "alias.json"))
    File.rm!(path)
    File.ln_s!("/tmp/elsewhere", path)

    assert {:held, :auth_cache_verifier_attempt_read_unavailable} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)

    File.chmod!(root, 0o755)

    assert {:error, :invalid_auth_cache_verifier_attempt_root} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)
  end

  test "rejects malformed identities and an oversized saved record", %{root: root} do
    invalid = [
      {%{@assignment | sha256: "bad"}, @job_uid, @slot, @image},
      {@assignment, "../other", @slot, @image},
      {@assignment, @job_uid, %{@slot | lease_id: "not-a-lease"}, @image},
      {@assignment, @job_uid, %{@slot | slot_id: "Bad/slot"}, @image},
      {@assignment, @job_uid, %{@slot | claim_uid: "../claim"}, @image},
      {@assignment, @job_uid, %{@slot | claim_uid: nil}, @image},
      {@assignment, @job_uid, %{@slot | claim_name: nil}, @image},
      {@assignment, @job_uid, @slot, "ghcr.io/hypergridau/symphony-worker:latest"}
    ]

    for {assignment, job_uid, slot, image} <- invalid do
      assert {:error, :invalid_auth_cache_verifier_attempt} =
               AuthCacheVerifierAttemptJournal.ensure(assignment, job_uid, slot, image, root)
    end

    assert {:error, :invalid_auth_cache_verifier_attempt_root} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, "relative/root")

    assert {:error, :invalid_auth_cache_verifier_attempt_root} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, nil)

    regular_root = Path.join(root, "regular-file")
    File.write!(regular_root, "ordinary")

    assert {:error, :invalid_auth_cache_verifier_attempt_root} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, regular_root)

    assert {:error, :invalid_auth_cache_verifier_attempt_root} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, Path.join(root, "missing-directory"))

    assert {:error, :invalid_auth_cache_verifier_attempt} =
             AuthCacheVerifierAttemptJournal.ensure(nil, @job_uid, @slot, @image, root)

    path = Path.join(root, @digest <> "-" <> @job_uid <> ".auth-verifier-attempt.json")
    File.write!(path, String.duplicate("x", 4_097))
    File.chmod!(path, 0o600)

    assert {:held, :auth_cache_verifier_attempt_read_unavailable} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)
  end

  test "rejects forged attempt IDs and extra saved fields", %{root: root} do
    assert {:ok, saved} = AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)
    [path] = Path.wildcard(Path.join(root, "*.auth-verifier-attempt.json"))

    for altered <- [
          Map.put(saved, "attemptId", "not-a-uuid"),
          Map.put(saved, "unexpected", true),
          Map.delete(saved, "attemptId")
        ] do
      File.write!(path, Jason.encode!(altered))
      File.chmod!(path, 0o600)

      assert match?({:held, _}, AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root))
    end
  end

  test "rejects a symlinked root and converges on one attempt after concurrent calls", %{root: root} do
    link = Path.join(System.tmp_dir!(), "symphony-verifier-link-" <> Ecto.UUID.generate())
    File.ln_s!(root, link)
    on_exit(fn -> File.rm!(link) end)

    assert {:error, :invalid_auth_cache_verifier_attempt_root} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, link)

    parent_link = Path.join(System.tmp_dir!(), "symphony-verifier-parent-" <> Ecto.UUID.generate())
    File.ln_s!(Path.dirname(root), parent_link)
    on_exit(fn -> File.rm!(parent_link) end)

    assert {:error, :invalid_auth_cache_verifier_attempt_root} =
             AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, Path.join(parent_link, Path.basename(root)))

    results =
      1..8
      |> Task.async_stream(fn _ -> AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root) end,
        max_concurrency: 8,
        timeout: 5_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    accepted = for {:ok, intent} <- results, do: intent
    assert accepted != []
    assert Enum.uniq(accepted) |> length() |> Kernel.==(1)
    assert {:ok, hd(accepted)} == AuthCacheVerifierAttemptJournal.ensure(@assignment, @job_uid, @slot, @image, root)
  end
end
