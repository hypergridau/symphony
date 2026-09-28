defmodule SymphonyElixir.WorkPackageClaimHandoffTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.Handoff

  test "records root intent before activating the exact suspended allocation" do
    allocation_id = "rke2job:v1:exact-job"
    test_pid = self()

    ports = %{
      begin_intent: fn ^allocation_id ->
        send(test_pid, :intent_recorded)
        :ok
      end,
      reconcile_intent: fn _ -> flunk("fresh allocation must not use restart reconciliation") end,
      activate: fn ^allocation_id ->
        assert_received :intent_recorded
        send(test_pid, :activated)
        {:ok, :ready}
      end
    }

    assert {:ok, :ready} = Handoff.resume(%{phase: "allocation_suspended", allocation_id: allocation_id}, ports)
    assert_received :activated
  end

  test "restart reconciles the durable intent and resumes only the same allocation" do
    allocation_id = "rke2job:v1:exact-job"
    test_pid = self()

    ports = %{
      begin_intent: fn _ -> flunk("restart must not create a second root intent") end,
      reconcile_intent: fn ^allocation_id ->
        send(test_pid, :intent_reconciled)
        :ok
      end,
      activate: fn ^allocation_id ->
        assert_received :intent_reconciled
        {:ok, :already_active}
      end
    }

    assert {:ok, :already_active} = Handoff.resume(%{phase: "spawn_started", allocation_id: allocation_id}, ports)
  end

  test "intent denial prevents activation" do
    test_pid = self()

    ports = %{
      begin_intent: fn _ -> {:held, :global_pause} end,
      reconcile_intent: fn _ -> :ok end,
      activate: fn _ ->
        send(test_pid, :activated)
        {:ok, :ready}
      end
    }

    assert {:held, :global_pause} =
             Handoff.resume(%{phase: "allocation_suspended", allocation_id: "rke2job:v1:job"}, ports)

    refute_received :activated
  end

  test "missing lifecycle port fails closed before invoking any port" do
    test_pid = self()

    ports = %{
      begin_intent: fn _ ->
        send(test_pid, :intent)
        :ok
      end,
      activate: fn _ ->
        send(test_pid, :activated)
        {:ok, :ready}
      end
    }

    assert {:held, {:suspended_allocation_handoff_port_missing, :reconcile_intent}} =
             Handoff.resume(%{phase: "allocation_suspended", allocation_id: "rke2job:v1:job"}, ports)

    refute_received :intent
    refute_received :activated
  end

  test "invalid phase and dispatch identities fail closed before invoking ports" do
    test_pid = self()

    ports = %{
      begin_intent: fn _ ->
        send(test_pid, :called)
        :ok
      end,
      reconcile_intent: fn _ ->
        send(test_pid, :called)
        :ok
      end,
      activate: fn _ ->
        send(test_pid, :called)
        {:ok, :ready}
      end
    }

    assert {:held, :suspended_allocation_handoff_invalid} = Handoff.resume(%{}, ports)

    assert {:held, :suspended_allocation_handoff_phase_invalid} =
             Handoff.resume(%{phase: "confirmed", allocation_id: "rke2job:v1:job"}, ports)

    refute_received :called
  end

  test "bad or raising intent ports never reach activation" do
    Enum.each(
      [
        {:error, :denied},
        :malformed,
        :raises
      ],
      fn response ->
        test_pid = self()

        begin_intent = fn _allocation_id ->
          case response do
            :raises -> raise "synthetic intent port failure"
            other -> other
          end
        end

        ports = %{
          begin_intent: begin_intent,
          reconcile_intent: fn _ -> :ok end,
          activate: fn _ ->
            send(test_pid, :activated)
            {:ok, :ready}
          end
        }

        expected =
          case response do
            {:error, reason} -> {:error, reason}
            :malformed -> {:held, :suspended_allocation_intent_unverified}
            :raises -> {:held, :suspended_allocation_intent_unavailable}
          end

        assert expected == Handoff.resume(%{phase: "allocation_suspended", allocation_id: "rke2job:v1:job"}, ports)
        refute_received :activated
      end
    )
  end

  test "bad or raising activation ports remain held" do
    Enum.each(
      [
        {:error, :activation_denied},
        :malformed,
        :raises
      ],
      fn response ->
        activate = fn _allocation_id ->
          case response do
            :raises -> raise "synthetic activation port failure"
            other -> other
          end
        end

        ports = %{
          begin_intent: fn _ -> :ok end,
          reconcile_intent: fn _ -> :ok end,
          activate: activate
        }

        expected =
          case response do
            {:error, reason} -> {:error, reason}
            :malformed -> {:held, :suspended_allocation_activation_unverified}
            :raises -> {:held, :suspended_allocation_activation_unavailable}
          end

        assert expected == Handoff.resume(%{phase: "allocation_suspended", allocation_id: "rke2job:v1:job"}, ports)
      end
    )
  end
end
