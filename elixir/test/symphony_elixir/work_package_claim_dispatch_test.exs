defmodule SymphonyElixir.WorkPackageClaimDispatchTest do
  use ExUnit.Case, async: true
  alias SymphonyElixir.WorkPackageClaim.{Dispatch, Journal}
  alias SymphonyElixir.WorkPackageClaim.Handoff

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

  test "a suspended allocation survives pause and restart but cannot enter the local spawn path", %{journal: journal, key: key} do
    {:ok, submitted} = Dispatch.submit(journal, key, @input, @now)
    {:ok, confirmed} = Dispatch.confirm(submitted, key)
    allocation_id = "rke2job:v1:exact-allocation"
    {:ok, allocated} = Dispatch.record_suspended_allocation(confirmed, key, @input, allocation_id)
    assert {:ok, replayed} = Dispatch.record_suspended_allocation(allocated, key, @input, allocation_id)
    assert replayed.reservations[key].dispatch.allocation_id == allocation_id

    reservation = allocated.reservations[key]
    assert reservation.dispatch.phase == "allocation_suspended"
    assert reservation.dispatch.allocation_id == allocation_id
    refute Dispatch.ready?(reservation, DateTime.to_unix(@now, :millisecond) + 999_999)
    assert {:error, :suspended_allocation_controller_required} = Dispatch.begin_spawn(allocated, key, @input)

    assert {:error, :suspended_allocation_controller_required} =
             Dispatch.submit(allocated, key, @input, DateTime.add(@now, 60, :second))

    {:ok, paused} = Dispatch.begin_recovery(allocated, key, @input)
    assert paused.reservations[key].dispatch == reservation.dispatch
    assert {:ok, persisted} = round_trip(paused)
    assert {:ok, ^reservation} = Dispatch.find(persisted, "issue", "profile", "repo", 1)

    assert {:error, :suspended_allocation_controller_required} =
             Dispatch.retry_status(persisted.reservations[key], DateTime.to_unix(@now, :millisecond) + 999_999)
  end

  test "a suspended allocation cannot be rebound to another allocation", %{journal: journal, key: key} do
    {:ok, submitted} = Dispatch.submit(journal, key, @input, @now)
    {:ok, confirmed} = Dispatch.confirm(submitted, key)
    {:ok, allocated} = Dispatch.record_suspended_allocation(confirmed, key, @input, "rke2job:v1:first")

    assert {:error, :suspended_allocation_identity_changed} =
             Dispatch.record_suspended_allocation(allocated, key, @input, "rke2job:v1:second")
  end

  test "a managed activation intent preserves only the exact allocation and authority", %{journal: journal, key: key} do
    {:ok, submitted} = Dispatch.submit(journal, key, @input, @now)
    {:ok, confirmed} = Dispatch.confirm(submitted, key)
    allocation_id = "rke2job:v1:exact-allocation"
    {:ok, suspended} = Dispatch.record_suspended_allocation(confirmed, key, @input, allocation_id)

    assert {:error, :suspended_allocation_identity_changed} =
             Dispatch.begin_suspended_spawn(suspended, key, @input, "rke2job:v1:other-allocation")

    assert {:error, :claim_authority_changed} =
             Dispatch.begin_suspended_spawn(suspended, key, %{@input | runner_id: "other"}, allocation_id)

    assert {:ok, started} = Dispatch.begin_suspended_spawn(suspended, key, @input, allocation_id)
    assert started.reservations[key].dispatch.phase == "spawn_started"
    assert started.reservations[key].dispatch.allocation_id == allocation_id
    assert {:ok, ^started} = Dispatch.begin_suspended_spawn(started, key, @input, allocation_id)

    assert {:ok, pending} = Dispatch.begin_pre_witness_recovery(started, key, @input)
    assert pending.reservations[key].dispatch.phase == "recovery_pending"
    assert pending.reservations[key].dispatch.allocation_id == allocation_id
    assert {:ok, persisted} = round_trip(pending)
    assert persisted.reservations[key].dispatch.allocation_id == allocation_id
  end

  test "handoff activation follows durable intent and resumes the same allocation after restart", %{journal: journal, key: key} do
    {:ok, submitted} = Dispatch.submit(journal, key, @input, @now)
    {:ok, confirmed} = Dispatch.confirm(submitted, key)
    allocation_id = "rke2job:v1:exact-allocation"
    {:ok, suspended} = Dispatch.record_suspended_allocation(confirmed, key, @input, allocation_id)

    ports = %{
      begin_intent: fn ^allocation_id ->
        {:ok, intent} = Dispatch.begin_suspended_spawn(suspended, key, @input, allocation_id)
        Process.put(:handoff_intent, intent)
        :ok
      end,
      reconcile_intent: fn ^allocation_id ->
        intent = Process.get(:handoff_intent)
        assert is_map(intent)
        path = Path.join(System.tmp_dir!(), "claim-handoff-#{System.unique_integer([:positive])}.json")
        on_exit(fn -> File.rm(path) end)
        assert :ok = Journal.save(path, intent)
        assert {:ok, restarted} = Journal.load(path)
        assert {:ok, replayed} = Dispatch.replay_spawn(restarted, key, @input)
        assert replayed.dispatch.phase == "spawn_started"
        assert replayed.dispatch.allocation_id == allocation_id
        :ok
      end,
      activate: fn ^allocation_id ->
        assert is_map(Process.get(:handoff_intent))
        {:held, :activation_ack_lost}
      end
    }

    assert {:held, :activation_ack_lost} = Handoff.resume(suspended.reservations[key].dispatch, ports)

    assert {:held, :activation_ack_lost} =
             Handoff.resume(%{phase: "spawn_started", allocation_id: allocation_id}, ports)

    Process.delete(:handoff_intent)
  end

  test "a suspended phase without an allocation identity is invalid" do
    assert {:error, :invalid_dispatch_journal} =
             Dispatch.decode(%{
               "phase" => "allocation_suspended",
               "attempts" => 1,
               "retry_at_ms" => 1,
               "authority_digest" => String.duplicate("a", 64),
               "allocation_id" => nil
             })
  end

  test "legacy dispatch rows decode with no allocation handoff" do
    assert {:ok, %{phase: "confirmed", allocation_id: nil}} =
             Dispatch.decode(%{
               "phase" => "confirmed",
               "attempts" => 1,
               "retry_at_ms" => 1,
               "authority_digest" => String.duplicate("a", 64)
             })
  end

  test "legacy in-memory spawn markers remain valid journal rows", %{journal: journal, key: key} do
    reservation =
      Map.put(journal.reservations[key], :dispatch, %{
        phase: "spawn_started",
        attempts: 1,
        retry_at_ms: 0,
        authority_digest: String.duplicate("a", 64)
      })

    legacy = %{journal | reservations: Map.put(journal.reservations, key, reservation)}
    path = Path.join(System.tmp_dir!(), "claim-legacy-spawn-marker-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(path) end)

    assert :ok = Journal.save(path, legacy)
    assert {:ok, loaded} = Journal.load(path)
    assert loaded.reservations[key].dispatch.phase == "spawn_started"
    assert loaded.reservations[key].dispatch.allocation_id == nil
  end

  defp round_trip(journal) do
    path = Path.join(System.tmp_dir!(), "claim-suspended-allocation-#{System.unique_integer([:positive])}.json")
    on_exit(fn -> File.rm(path) end)
    with :ok <- Journal.save(path, journal), do: Journal.load(path)
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
