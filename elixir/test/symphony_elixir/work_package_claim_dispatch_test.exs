defmodule SymphonyElixir.WorkPackageClaimDispatchTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.WorkPackageClaim.{Dispatch, Journal}

  @now ~U[2026-09-09 00:00:00Z]
  @input %{runner_id: "runner", managed_project_profile_id: "profile", repository_ref: "repo", managed_delegations: %{authority: "operator"}}

  setup do
    reservation = %{
      issue_id: "issue",
      managed_project_profile_id: "profile",
      repository_ref: "repo",
      projection_id: "projection",
      reservation_id: "reservation",
      reservation_nonce: "private-test-nonce",
      runner_id: "runner",
      generation: 1,
      session_id: "worker:issue:1",
      process_id: "worker:issue:1",
      responsible_delegation_id: "responsible",
      execution_fence_token: "issue:1",
      runtime_lease_id: "worker:issue:1",
      scope_keys: ["repo:repo"]
    }

    key = Journal.reservation_key("issue", "profile", "repo", 1)
    {:ok, journal} = Journal.put(Journal.new(), key, reservation)
    %{journal: journal, key: key}
  end

  test "disk round trip preserves submission and immutable authority", %{journal: journal, key: key} do
    {:ok, journal} = Dispatch.submit(journal, key, @input, @now)
    path = Path.join(System.tmp_dir!(), "claim-dispatch-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(path) end)
    assert :ok = Journal.save(path, journal)
    assert {:ok, ^journal} = Journal.load(path)
    assert journal.reservations[key].dispatch.phase == "submitted"
    assert {:error, :claim_authority_changed} = Dispatch.submit(journal, key, %{@input | runner_id: "other"}, @now)
    assert {:error, :claim_authority_changed} = Dispatch.submit(journal, key, %{@input | managed_delegations: %{}}, @now)
  end

  test "six durable attempts have bounded backoff", %{journal: journal, key: key} do
    {journal, delays} =
      Enum.reduce(1..6, {journal, []}, fn count, {previous, delays} ->
        dispatch = previous.reservations[key][:dispatch]
        now = if dispatch, do: DateTime.from_unix!(dispatch.retry_at_ms, :millisecond), else: @now
        {:ok, next} = Dispatch.submit(previous, key, @input, now)
        dispatch = next.reservations[key].dispatch
        assert dispatch.attempts == count
        {next, delays ++ [dispatch.retry_at_ms - DateTime.to_unix(now, :millisecond)]}
      end)

    assert delays == [5_000, 10_000, 20_000, 40_000, 60_000, 60_000]
    assert {:error, :claim_recovery_exhausted} = Dispatch.submit(journal, key, @input, @now)
    refute Dispatch.ready?(journal.reservations[key], DateTime.to_unix(@now, :millisecond) + 999_999)
  end

  test "pending claim waits until due without creating a new identity", %{journal: journal, key: key} do
    {:ok, journal} = Dispatch.submit(journal, key, @input, @now)
    pending = journal.reservations[key]
    refute Dispatch.ready?(pending, pending.dispatch.retry_at_ms - 1)
    assert {:error, :claim_recovery_backoff} = Dispatch.submit(journal, key, @input, DateTime.from_unix!(pending.dispatch.retry_at_ms - 1, :millisecond))
    assert {:ok, retried} = Dispatch.submit(journal, key, @input, DateTime.from_unix!(pending.dispatch.retry_at_ms, :millisecond))
    assert retried.reservations[key].dispatch.attempts == 2
    assert Dispatch.ready?(pending, pending.dispatch.retry_at_ms)
    assert {:ok, ^pending} = Dispatch.find(journal, "issue", "profile", "repo", 1)
    assert {:error, _} = Dispatch.find(journal, "issue", "profile", "repo", 2)
  end

  test "legacy rows cannot prove that spawn was never attempted", %{journal: journal} do
    assert {:error, :claim_recovery_journal_missing} = Dispatch.find(journal, "issue", "profile", "repo", 1)
  end

  test "confirmed authority permits exactly one durable spawn attempt", %{journal: journal, key: key} do
    {:ok, journal} = Dispatch.submit(journal, key, @input, @now)
    assert {:error, _} = Dispatch.begin_spawn(journal, key, @input)
    {:ok, journal} = Dispatch.confirm(journal, key)
    {:ok, journal} = Dispatch.begin_spawn(journal, key, @input)
    assert journal.reservations[key].dispatch.phase == "spawn_started"
    assert {:error, _} = Dispatch.begin_spawn(journal, key, @input)
    assert {:error, :claim_spawn_already_attempted} = Dispatch.submit(journal, key, @input, @now)
  end

  test "deterministic rejection is retained and cannot enter another retry", %{journal: journal, key: key} do
    {:ok, journal} = Dispatch.submit(journal, key, @input, @now)
    {:ok, journal} = Dispatch.block(journal, key)
    assert {:error, :claim_reconciliation_required} = Dispatch.submit(journal, key, @input, @now)
    assert {:error, :claim_reconciliation_required} = Dispatch.find(journal, "issue", "profile", "repo", 1)
  end

  test "a confirmed claim can be durably fenced for recovery before spawn", %{journal: journal, key: key} do
    {:ok, submitted} = Dispatch.submit(journal, key, @input, @now)
    {:ok, confirmed} = Dispatch.confirm(submitted, key)
    {:ok, pending} = Dispatch.begin_recovery(confirmed, key, @input)
    assert pending.reservations[key].dispatch.phase == "recovery_pending"
    assert {:error, :invalid_claim_dispatch_transition} = Dispatch.begin_spawn(pending, key, @input)
    assert {:error, :claim_reconciliation_required} = Dispatch.submit(pending, key, @input, @now)
    assert {:error, :claim_reconciliation_required} = Dispatch.find(pending, "issue", "profile", "repo", 1)

    assert {:error, :claim_authority_changed} =
             Dispatch.begin_recovery(confirmed, key, %{@input | runner_id: "changed"})

    path = Path.join(System.tmp_dir!(), "claim-recovery-pending-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(path) end)
    assert :ok = Journal.save(path, pending)
    assert {:ok, ^pending} = Journal.load(path)
  end

  test "a lost provider acknowledgement can be fenced without a second claim", %{journal: journal, key: key} do
    {:ok, submitted} = Dispatch.submit(journal, key, @input, @now)
    {:ok, pending} = Dispatch.begin_recovery(submitted, key, @input)
    assert pending.reservations[key].dispatch.phase == "recovery_pending"
    assert {:error, :claim_reconciliation_required} = Dispatch.submit(pending, key, @input, @now)
    assert {:error, :invalid_claim_dispatch_transition} = Dispatch.begin_spawn(pending, key, @input)
    assert {:error, :invalid_claim_dispatch_transition} = Dispatch.begin_recovery(pending, key, @input)
  end
end
