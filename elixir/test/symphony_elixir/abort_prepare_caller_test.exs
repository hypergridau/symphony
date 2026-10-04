Code.require_file("../support/rke2_job_fake_client.exs", __DIR__)

defmodule SymphonyElixir.AbortPrepareTestClientContext do
  def client_context(_assignment, _operation, _key, client), do: {:ok, client}
end

defmodule SymphonyElixir.AbortPrepareTestRegistry do
  def ready?(_context), do: true
  def register(_allocation_id, _uid, _reservation_id, _context), do: :ok
end

defmodule SymphonyElixir.AbortPrepareTestSlotGuard do
  def reserve(_slot, _assignment, _context), do: :ok
  def bind_uid(_slot, _assignment, _allocation, _context), do: :ok
  def verify_bound(_slot, _assignment, _allocation, _context), do: :ok
end

defmodule SymphonyElixir.AbortPreparePermissiveGuard do
  def verify(_assignment, _allocation, _observation, _acknowledgement, _context), do: :ok
end

defmodule SymphonyElixir.AbortPrepareTestRootInputPublisher do
  def publish(request) do
    case Process.get(:abort_root_input_publish_fun) do
      fun when is_function(fun, 1) -> fun.(request)
      _ -> :ok
    end
  end
end

defmodule SymphonyElixir.AbortPrepareCallerTest do
  use ExUnit.Case, async: false

  alias SymphonyElixir.ManagedAssignmentBundle
  alias SymphonyElixir.ManagedExecutor.{AbortResultPublisher, Record}
  alias SymphonyElixir.RKE2Job.{AbortPrepareCaller, AbortPrepareJournal, ManagedExecutorAdapter}
  alias SymphonyElixir.RKE2JobFakeClient
  alias SymphonyElixir.WorkPackageClaim.HostWitness

  setup do
    if match?({:win32, _}, :os.type()), do: Process.put(:abort_prepare_journal_windows_test_only, true)

    root = Path.join(System.tmp_dir!(), "symphony-abort-prepare-#{System.unique_integer([:positive])}")
    :ok = File.mkdir(root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(root, 0o700)
    on_exit(fn -> File.rm_rf(root) end)

    {:ok, client} =
      Agent.start_link(fn ->
        %{
          jobs: %{},
          pods: %{},
          creates: 0,
          deletes: [],
          create_error: nil,
          create_commit?: true,
          get_error: nil,
          delete_error: nil,
          delete_commit?: false,
          pod_list_resource_version: "list-rv-9"
        }
      end)

    result_root = Path.join(File.cwd!(), ".tmp-abort-prepare-results-#{System.unique_integer([:positive])}")
    :ok = File.mkdir(result_root)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(result_root, 0o700)
    if match?({:win32, _}, :os.type()), do: Process.put(:abort_result_journal_windows_test_only, true)
    on_exit(fn -> File.rm_rf(result_root) end)

    %{root: root, result_root: result_root, client: client}
  end

  test "production policy rejects injectable transport and witness seams", context do
    previous =
      Enum.map([:abort_prepare_journal_root, :abort_prepare_workspace_root, :abort_result_journal_root], fn key ->
        {key, Application.get_env(:symphony_elixir, key)}
      end)

    workspace = Path.join(System.tmp_dir!(), "trusted-worker-workspaces")
    Application.put_env(:symphony_elixir, :abort_prepare_journal_root, context.root)
    Application.put_env(:symphony_elixir, :abort_prepare_workspace_root, workspace)
    Application.put_env(:symphony_elixir, :abort_result_journal_root, context.result_root)

    on_exit(fn ->
      Enum.each(previous, fn
        {key, nil} -> Application.delete_env(:symphony_elixir, key)
        {key, value} -> Application.put_env(:symphony_elixir, key, value)
      end)
    end)

    base = %{journal_root: context.root, workspace_root: workspace, witness_input: %{}, adapter_context: %{abort_result_journal_root: context.result_root}}
    secure = Map.put(base, :host_witness, HostWitness)
    refute AbortPrepareCaller.production_context_allowed?(Map.put(secure, :post_fun, fn _, _ -> :ok end))
    injected_witness = %{secure | witness_input: %{host_witness_fun: fn _ -> :ok end}}
    override_module = Map.put(secure, :host_witness, SymphonyElixir.AbortPrepareTestClientContext)
    refute AbortPrepareCaller.production_context_allowed?(injected_witness)
    refute AbortPrepareCaller.production_context_allowed?(override_module)
    assert AbortPrepareCaller.production_context_allowed?(secure)

    for root <- [nil, context.root, workspace, context.result_root <> "-other"] do
      refute AbortPrepareCaller.production_context_allowed?(put_in(secure, [:adapter_context, :abort_result_journal_root], root))
    end
  end

  test "invalid entrypoints, incomplete claims and a missing journal fail closed", context do
    assert {:error, :invalid_abort_prepare_journal_claim} = AbortPrepareJournal.identity_key(nil)

    assert {:error, :invalid_abort_prepare_journal_claim} =
             AbortPrepareJournal.identity_key(%{"unsupported" => self()})

    assignment = assignment()
    assert {:error, :invalid_abort_prepare_caller_input} = AbortPrepareCaller.prepare(nil, assignment, "bad", %{})
    assert {:held, :abort_prepare_ack_guard_missing} = AbortPrepareCaller.confirm(nil, assignment, "bad", nil, %{})

    adapter = adapter_context(context, assignment)
    allocation = %{id: "allocation-one", status: :ready}
    witness_input = witness_input(fn _request -> {:error, :unexpected_witness_call} end)
    caller = caller_context(context, adapter, witness_input, fn _, _ -> :unexpected_provider_call end)

    assert {:held, :abort_prepare_journal_missing} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), %{}, caller)

    invalid_claim = put_in(caller, [:reservation, :reservation_nonce], "")

    assert {:error, :host_witness_claim_incomplete} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), invalid_claim)
  end

  test "invalid provider configuration is rejected before host evidence or transport", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    posts = Agent.start_link(fn -> 0 end) |> elem(1)
    witness_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        Agent.update(witness_calls, &(&1 + 1))
        {:ok, receipt(false, String.duplicate("9", 64))}
      end)

    caller =
      context
      |> caller_context(adapter, witness_input, fn _, _ ->
        Agent.update(posts, &(&1 + 1))
        {:error, :unexpected_transport}
      end)
      |> put_in([:provider_context, :base_url], "http://dahlia.example")

    assert {:error, :invalid_abort_prepare_caller_input} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert Agent.get(posts, & &1) == 0
    assert Agent.get(witness_calls, & &1) == 0
  end

  test "an incomplete Kubernetes pod snapshot holds before root intent, provider POST or journal creation", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    :ok = Agent.update(context.client, &Map.put(&1, :list_pods_snapshot_error, :snapshot_unavailable))
    witness_calls = Agent.start_link(fn -> 0 end) |> elem(1)
    provider_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        Agent.update(witness_calls, &(&1 + 1))
        {:ok, receipt(false, String.duplicate("8", 64))}
      end)

    caller =
      caller_context(context, adapter, witness_input, fn _url, options ->
        Agent.update(provider_calls, &(&1 + 1))
        request = Jason.decode!(Keyword.fetch!(options, :body))

        {:ok,
         %Req.Response{
           status: 200,
           body: %{
             "data" => %{
               "prepareId" => request["prepareId"],
               "projectionId" => "projection-one",
               "reservationId" => "reservation-one",
               "preparedAt" => "2026-09-27T12:00:00.000Z",
               "replayed" => false
             }
           }
         }}
      end)

    invalid_adapter = %{caller | adapter_context: Map.put(adapter, :client, nil)}

    assert {:error, :rke2_job_adapter_ports_invalid} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), invalid_adapter)

    assert Agent.get(witness_calls, & &1) == 0
    assert Agent.get(provider_calls, & &1) == 0
    assert File.ls!(context.root) == []

    :ok = Agent.update(context.client, &Map.put(&1, :list_pods_snapshot_error, :snapshot_unavailable))

    assert {:held, :suspended_abort_pod_read_unavailable} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert Agent.get(witness_calls, & &1) == 0
    assert Agent.get(provider_calls, & &1) == 0
    assert File.ls!(context.root) == []

    :ok = Agent.update(context.client, &Map.delete(&1, :list_pods_snapshot_error))
    assert {:ok, _prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    assert Agent.get(witness_calls, & &1) == 1
    assert Agent.get(provider_calls, & &1) == 1
  end

  test "session and delegation mismatches hold before witness, journal, provider or Kubernetes mutation", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    witness_calls = Agent.start_link(fn -> 0 end) |> elem(1)
    provider_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        Agent.update(witness_calls, &(&1 + 1))
        {:ok, receipt(false, String.duplicate("8", 64))}
      end)

    post_fun = fn _, _ ->
      Agent.update(provider_calls, &(&1 + 1))
      {:error, :unexpected_provider_call}
    end

    original = caller_context(context, adapter, witness_input, post_fun)
    before = Agent.get(context.client, & &1)

    mismatched_contexts = [
      Map.put(original, :reservation, %{reservation() | session_id: "different-session", runtime_lease_id: "different-session"}),
      Map.put(original, :reservation, %{reservation() | responsible_delegation_id: "different-delegation"}),
      put_in(original, [:adapter_context, :claim_binding, :runner_id], "different-runner"),
      put_in(original, [:adapter_context, :claim_binding, :workspace_id], "different-workspace"),
      put_in(original, [:adapter_context, :claim_binding, :managed_project_profile_id], "different-profile"),
      put_in(original, [:adapter_context, :claim_binding, :scope_keys], ["repo:other"]),
      put_in(original, [:adapter_context, :claim_binding, :nonce_sha256], String.duplicate("0", 64))
    ]

    for caller <- mismatched_contexts do
      assert {:held, :abort_prepare_claim_binding_mismatch} =
               AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    end

    assert Agent.get(witness_calls, & &1) == 0
    assert Agent.get(provider_calls, & &1) == 0
    assert Agent.get(context.client, & &1) == before
    assert File.ls!(context.root) == []
  end

  test "root intent precedes provider POST and a lost response replays exact saved bytes", context do
    assignment = assignment()
    adapter_context = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(
               assignment,
               allocation_key(assignment),
               adapter_context
             )

    events = Agent.start_link(fn -> [] end) |> elem(1)
    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn request ->
        Agent.update(events, &[{:root, request} | &1])
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})
        {:ok, receipt(attempt > 1, String.duplicate("a", 64))}
      end)

    post_state = Agent.start_link(fn -> %{bodies: [], fail_first?: true} end) |> elem(1)

    post_fun = fn _url, options ->
      bytes = Keyword.fetch!(options, :body)
      Agent.update(events, &[{:provider, bytes} | &1])

      Agent.get_and_update(post_state, fn state ->
        next = %{state | bodies: state.bodies ++ [bytes]}

        if state.fail_first? do
          {{:error, :timeout}, %{next | fail_first?: false}}
        else
          request = Jason.decode!(bytes)

          response = %Req.Response{
            status: 200,
            body: %{
              "data" => %{
                "prepareId" => request["prepareId"],
                "projectionId" => "projection-one",
                "reservationId" => "reservation-one",
                "preparedAt" => "2026-09-27T12:00:00.000Z",
                "replayed" => true
              }
            }
          }

          {{:ok, response}, next}
        end
      end)
    end

    caller_context = caller_context(context, adapter_context, witness_input, post_fun)

    assert {:held, :abort_prepare_provider_outcome_uncertain} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller_context)

    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller_context)
    state = Agent.get(post_state, & &1)
    assert length(state.bodies) == 2
    assert Enum.at(state.bodies, 0) == Enum.at(state.bodies, 1)

    ordered_events = Agent.get(events, &Enum.reverse/1)
    assert Enum.map(ordered_events, &elem(&1, 0)) == [:root, :provider, :root, :provider]
    assert elem(Enum.at(ordered_events, 1), 1) == elem(Enum.at(ordered_events, 3), 1)
    assert elem(Enum.at(ordered_events, 0), 1)["abortPrepare"] == elem(Enum.at(ordered_events, 2), 1)["abortPrepare"]

    request = Jason.decode!(hd(state.bodies))
    assert request["contractVersion"] == "work-package-pre-execution-abort-prepare.v1"
    assert request["assignmentDigest"] == assignment.sha256
    assert request["allocationId"] == allocation.id
    assert request["slotClaimPodsAbsent"]
    request_hash = :crypto.hash(:sha256, hd(state.bodies)) |> Base.encode16(case: :lower)
    assert prepared.prepare_ack["prepareRequestSHA256"] == request_hash

    # A completed retry checks the root receipt again and recovers its durable
    # provider acknowledgment without another POST.
    event_count = length(ordered_events)
    assert {:ok, recovered} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller_context)
    assert recovered.prepare_ack == prepared.prepare_ack
    assert length(Agent.get(post_state, & &1.bodies)) == 2
    assert length(Agent.get(events, & &1)) == event_count + 1
    {:ok, original_claim} = HostWitness.request(witness_input, "claim_bound", reservation())
    original_key = Map.put(original_claim["claim"], "pool", "midgard")
    {:ok, original_identity} = AbortPrepareJournal.identity_key(original_key)
    {:ok, other_identity} = AbortPrepareJournal.identity_key(Map.put(original_key, "pool", "asgard"))
    refute original_identity == other_identity
  end

  test "journal guard rejects changed provider ack and confirms only its persisted observation", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})
        {:ok, receipt(attempt > 1, String.duplicate("b", 64))}
      end)

    post_fun = fn _url, options ->
      request = Jason.decode!(Keyword.fetch!(options, :body))

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "data" => %{
             "prepareId" => request["prepareId"],
             "projectionId" => "projection-one",
             "reservationId" => "reservation-one",
             "preparedAt" => "2026-09-27T12:00:00.000Z",
             "replayed" => false
           }
         }
       }}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)
    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    retain_result(caller, allocation, assignment)
    changed = put_in(prepared.prepare_ack["prepareId"], "11111111-2222-4333-8444-555555555555")

    changed_prepared = %{
      prepared
      | prepare_ack: changed,
        prepare_ack_guard: SymphonyElixir.AbortPreparePermissiveGuard,
        prepare_ack_guard_context: %{journal_root: context.root, claim: %{"pool" => "asgard"}}
    }

    assert {:held, :prepare_ack_observation_mismatch} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), changed_prepared, caller)

    assert Agent.get(context.client, & &1.deletes) == []

    bad_timestamp = put_in(prepared.prepare_ack["preparedAt"], "not-a-timestamp")

    assert {:held, :prepare_ack_observation_mismatch} =
             AbortPrepareCaller.confirm(
               allocation,
               assignment,
               key(assignment),
               %{prepared | prepare_ack: bad_timestamp},
               caller
             )

    assert Agent.get(context.client, & &1.deletes) == []

    missing_ack = %{prepared | prepare_ack: nil}

    assert {:held, :prepare_ack_observation_mismatch} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), missing_ack, caller)

    assert Agent.get(context.client, & &1.deletes) == []

    {:ok, claim} = HostWitness.request(witness_input, "claim_bound", reservation())
    claim = Map.put(claim["claim"], "pool", "midgard")
    {:ok, record} = AbortPrepareJournal.load(context.root, claim)
    changed_ack = Map.put(prepared.prepare_ack, "replayed", not prepared.prepare_ack["replayed"])

    assert {:held, :abort_prepare_ack_conflict} =
             AbortPrepareJournal.record_ack(context.root, claim, record, changed_ack)

    changed_receipt = %{"version" => 1, "sequence" => 1, "hash" => String.duplicate("f", 64), "replayed" => false}

    assert {:held, :abort_prepare_intent_conflict} =
             AbortPrepareJournal.record_intent(context.root, claim, record, changed_receipt)

    {:ok, claim_key} = AbortPrepareJournal.identity_key(prepared.prepare_ack_guard_context.claim)
    ack_path = Path.join(context.root, claim_key <> ".ack.json")
    {:ok, saved_ack_record} = File.read(ack_path)
    :ok = File.write(ack_path, "{}")

    assert {:held, :prepare_ack_observation_mismatch} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    assert Agent.get(context.client, & &1.deletes) == []
    :ok = File.write(ack_path, saved_ack_record)

    if match?({:unix, _}, :os.type()) do
      :ok = File.chmod(ack_path, 0o644)

      assert {:held, :prepare_ack_observation_mismatch} =
               AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

      assert Agent.get(context.client, & &1.deletes) == []
      :ok = File.chmod(ack_path, 0o600)
    end

    assert :ok = AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)
    assert length(Agent.get(context.client, & &1.deletes)) == 1

    {:ok, claim_key} = AbortPrepareJournal.identity_key(prepared.prepare_ack_guard_context.claim)
    checkpoint_path = Path.join(context.root, claim_key <> ".confirmed-delete.json")
    {:ok, checkpoint_bytes} = File.read(checkpoint_path)
    checkpoint = Jason.decode!(checkpoint_bytes)
    assert checkpoint["prepare_id"] == prepared.prepare_ack["prepareId"]
    assert checkpoint["request_sha256"] == prepared.prepare_ack["prepareRequestSHA256"]
    assert checkpoint["job_uid"] == hd(Agent.get(context.client, & &1.deletes))
    assert checkpoint["post_delete_pod_snapshot"]["complete"]
    assert :ok = AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)
    assert length(Agent.get(context.client, & &1.deletes)) == 1

    corrupted = Map.put(checkpoint, "job_uid", "different-job-uid")
    :ok = File.write(checkpoint_path, Jason.encode!(corrupted))

    assert {:held, :abort_prepare_confirmed_delete_checkpoint_invalid} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    assert length(Agent.get(context.client, & &1.deletes)) == 1
  end

  test "root-intent rejection keeps the request and blocks POST until a later receipt", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_attempts = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn request ->
        if request["operation"] == "abort_prepare_intent" do
          attempt = Agent.get_and_update(root_attempts, &{&1 + 1, &1 + 1})

          case attempt do
            1 -> {:error, :witness_unavailable}
            2 -> raise "synthetic root witness crash"
            3 -> :invalid_witness_result
            _ -> {:ok, receipt(false, String.duplicate("c", 64))}
          end
        else
          {:ok, %{"ok" => true, "claim" => request["claim"]}}
        end
      end)

    posts = Agent.start_link(fn -> [] end) |> elem(1)

    post_fun = fn _url, options ->
      bytes = Keyword.fetch!(options, :body)
      Agent.update(posts, &[bytes | &1])
      request = Jason.decode!(bytes)

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "data" => %{
             "prepareId" => request["prepareId"],
             "projectionId" => "projection-one",
             "reservationId" => "reservation-one",
             "preparedAt" => "2026-09-27T12:00:00.000Z",
             "replayed" => false
           }
         }
       }}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)

    assert {:held, :abort_prepare_root_intent_unverified} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert Agent.get(posts, & &1) == []
    assert {:ok, saved} = AbortPrepareJournal.load(context.root, claim_for_test())

    assert {:held, :abort_prepare_root_intent_unverified} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert {:held, :abort_prepare_root_intent_unverified} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert Agent.get(posts, & &1) == []
    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    assert Agent.get(root_attempts, & &1) == 4
    assert length(Agent.get(posts, & &1)) == 1
    assert Jason.decode!(saved.request_bytes)["prepareId"] == prepared.prepare_ack["prepareId"]
  end

  test "malformed provider acknowledgement remains uncheckpointed and retries the same request", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})
        {:ok, receipt(attempt > 1, String.duplicate("d", 64))}
      end)

    posts = Agent.start_link(fn -> [] end) |> elem(1)

    post_fun = fn _url, options ->
      bytes = Keyword.fetch!(options, :body)
      Agent.update(posts, &[bytes | &1])
      request = Jason.decode!(bytes)

      prepare_id =
        if length(Agent.get(posts, & &1)) == 1,
          do: "00000000-0000-4000-8000-000000000000",
          else: request["prepareId"]

      data = %{
        "prepareId" => prepare_id,
        "projectionId" => "projection-one",
        "reservationId" => "reservation-one",
        "preparedAt" => "2026-09-27T12:00:00.000Z",
        "replayed" => false
      }

      {:ok, %Req.Response{status: 200, body: Jason.encode!(data)}}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)

    assert {:held, :abort_prepare_provider_outcome_uncertain} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert {:ok, record} = AbortPrepareJournal.load(context.root, claim_for_test())
    {:ok, identity} = AbortPrepareJournal.identity_key(record.claim)
    refute File.exists?(Path.join(context.root, identity <> ".ack.json"))

    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    bodies = Agent.get(posts, &Enum.reverse/1)
    assert length(bodies) == 2
    assert Enum.at(bodies, 0) == Enum.at(bodies, 1)
    assert prepared.prepare_ack["prepareId"] == Jason.decode!(hd(bodies))["prepareId"]
  end

  test "provider denial does not checkpoint an acknowledgment and retries the identical request", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})
        {:ok, receipt(attempt > 1, String.duplicate("5", 64))}
      end)

    posts = Agent.start_link(fn -> [] end) |> elem(1)

    post_fun = fn _url, options ->
      bytes = Keyword.fetch!(options, :body)
      Agent.update(posts, &[bytes | &1])

      if length(Agent.get(posts, & &1)) == 1 do
        {:ok, %Req.Response{status: 503, body: %{"error" => "temporarily unavailable"}}}
      else
        request = Jason.decode!(bytes)

        data = %{
          "prepareId" => request["prepareId"],
          "projectionId" => "projection-one",
          "reservationId" => "reservation-one",
          "preparedAt" => "2026-09-27T12:00:00.000Z",
          "replayed" => true
        }

        {:ok, %Req.Response{status: 200, body: Jason.encode!(%{"data" => data})}}
      end
    end

    caller = caller_context(context, adapter, witness_input, post_fun)

    assert {:held, :abort_prepare_provider_outcome_uncertain} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    [retry_body, first_body] = Agent.get(posts, & &1)
    assert first_body == retry_body
    assert prepared.prepare_ack["replayed"]
  end

  test "malformed successful provider payloads remain uncertain and preserve exact request bytes", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})
        {:ok, receipt(attempt > 1, String.duplicate("4", 64))}
      end)

    posts = Agent.start_link(fn -> [] end) |> elem(1)

    post_fun = fn _url, options ->
      bytes = Keyword.fetch!(options, :body)
      Agent.update(posts, &[bytes | &1])
      attempt = length(Agent.get(posts, & &1))

      case attempt do
        1 ->
          {:ok, %Req.Response{status: 200, body: []}}

        2 ->
          {:ok, %Req.Response{status: 200, body: "{"}}

        _ ->
          request = Jason.decode!(bytes)

          data = %{
            "prepareId" => request["prepareId"],
            "projectionId" => "projection-one",
            "reservationId" => "reservation-one",
            "preparedAt" => "2026-09-27T12:00:00.000Z",
            "replayed" => true
          }

          {:ok, %Req.Response{status: 200, body: Jason.encode!(data)}}
      end
    end

    caller = caller_context(context, adapter, witness_input, post_fun)

    for _attempt <- 1..2 do
      assert {:held, :abort_prepare_provider_outcome_uncertain} =
               AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    end

    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    [final_bytes, second_bytes, first_bytes] = Agent.get(posts, & &1)
    assert first_bytes == second_bytes
    assert second_bytes == final_bytes
    assert prepared.prepare_ack["replayed"]
  end

  test "transport exceptions, throws and invalid responses keep the exact request for retry", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})
        {:ok, receipt(attempt > 1, String.duplicate("7", 64))}
      end)

    attempts = Agent.start_link(fn -> [] end) |> elem(1)

    post_fun = fn _url, options ->
      bytes = Keyword.fetch!(options, :body)
      Agent.update(attempts, &[bytes | &1])

      case length(Agent.get(attempts, & &1)) do
        1 ->
          raise "synthetic transport crash"

        2 ->
          throw(:synthetic_transport_exit)

        3 ->
          :invalid_transport_result

        4 ->
          {:ok, %Req.Response{status: 200, body: %{"data" => %{}}}}

        _ ->
          request = Jason.decode!(bytes)

          {:ok,
           %Req.Response{
             status: 200,
             body: %{
               "prepareId" => request["prepareId"],
               "projectionId" => "projection-one",
               "reservationId" => "reservation-one",
               "preparedAt" => "2026-09-27T12:00:00.000Z",
               "replayed" => true
             }
           }}
      end
    end

    caller = caller_context(context, adapter, witness_input, post_fun)
    broken_transport = %{caller | post_fun: :unavailable_transport}

    assert {:held, :abort_prepare_provider_outcome_uncertain} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), broken_transport)

    {:ok, saved} = AbortPrepareJournal.load(context.root, claim_for_test())

    for _attempt <- 1..4 do
      assert {:held, :abort_prepare_provider_outcome_uncertain} =
               AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    end

    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    bodies = Agent.get(attempts, &Enum.reverse/1)
    assert length(bodies) == 5
    assert Enum.uniq(bodies) == [hd(bodies)]
    assert hd(bodies) == saved.request_bytes
    assert prepared.prepare_ack["replayed"]
  end

  test "corrupt or insecure acknowledgements hold and a missing acknowledgement safely replays", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})
        {:ok, receipt(attempt > 1, String.duplicate("6", 64))}
      end)

    posts = Agent.start_link(fn -> [] end) |> elem(1)

    post_fun = fn _url, options ->
      bytes = Keyword.fetch!(options, :body)
      Agent.update(posts, &[bytes | &1])
      request = Jason.decode!(bytes)

      acknowledgement = %{
        "prepareId" => request["prepareId"],
        "projectionId" => "projection-one",
        "reservationId" => "reservation-one",
        "preparedAt" => "2026-09-27T12:00:00.000Z",
        "replayed" => length(Agent.get(posts, & &1)) > 1
      }

      {:ok, %Req.Response{status: 200, body: acknowledgement}}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)
    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    {:ok, identity} = AbortPrepareJournal.identity_key(prepared.prepare_ack_guard_context.claim)
    ack_path = Path.join(context.root, identity <> ".ack.json")
    {:ok, saved_ack} = File.read(ack_path)

    {:ok, envelope} = Jason.decode(saved_ack)
    :ok = File.write(ack_path, Jason.encode!(Map.put(envelope, "acknowledgement", [])))

    assert {:held, :abort_prepare_ack_journal_invalid} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    :ok = File.write(ack_path, "{")

    assert {:held, :abort_prepare_ack_journal_invalid} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    :ok = File.write(ack_path, String.duplicate("x", 8_193))

    assert {:held, :abort_prepare_ack_journal_invalid} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    :ok = File.write(ack_path, saved_ack)

    if match?({:unix, _}, :os.type()) do
      :ok = File.chmod(ack_path, 0o644)

      assert {:held, :abort_prepare_ack_journal_invalid} =
               AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

      :ok = File.chmod(ack_path, 0o600)
    end

    original_ack = prepared.prepare_ack

    assert {:ok, %{prepare_ack: ^original_ack}} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert length(Agent.get(posts, & &1)) == 1

    :ok = File.rm(ack_path)

    assert {:ok, replayed} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    [replay_bytes, initial_bytes] = Agent.get(posts, &Enum.reverse/1)
    assert replay_bytes == initial_bytes
    assert replayed.prepare_ack["replayed"]
  end

  test "partial request journal and a conflicting immutable request both fail closed", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    witness_input = witness_input(fn _request -> {:ok, receipt(false, String.duplicate("e", 64))} end)
    posts = Agent.start_link(fn -> 0 end) |> elem(1)

    post_fun = fn _url, options ->
      Agent.update(posts, &(&1 + 1))
      request = Jason.decode!(Keyword.fetch!(options, :body))

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "data" => %{
             "prepareId" => request["prepareId"],
             "projectionId" => "projection-one",
             "reservationId" => "reservation-one",
             "preparedAt" => "2026-09-27T12:00:00.000Z",
             "replayed" => false
           }
         }
       }}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)
    assert {:ok, _prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    claim = claim_for_test()
    assert {:ok, record} = AbortPrepareJournal.load(context.root, claim)
    assert {:ok, ^record} = AbortPrepareJournal.record(context.root, claim, record)

    conflicting = %{record | observation: Map.put(record.observation, "localConflict", true)}
    assert {:held, :abort_prepare_journal_conflict} = AbortPrepareJournal.record(context.root, claim, conflicting)

    oversized = %{record | observation: Map.put(record.observation, "oversized", String.duplicate("x", 20_000))}
    assert {:held, :abort_prepare_journal_record_too_large} = AbortPrepareJournal.record(context.root, claim, oversized)

    without_intent = Path.join(System.tmp_dir!(), "symphony-abort-prepare-no-intent-#{System.unique_integer([:positive])}")
    :ok = File.mkdir(without_intent)
    if match?({:unix, _}, :os.type()), do: :ok = File.chmod(without_intent, 0o700)

    assert {:held, :abort_prepare_root_intent_missing} =
             AbortPrepareJournal.record_ack(without_intent, claim, record, %{})

    {:ok, _removed} = File.rm_rf(without_intent)

    assert {:held, :invalid_abort_prepare_provider_acknowledgement} =
             AbortPrepareJournal.record_ack(context.root, claim, record, %{})

    if match?({:unix, _}, :os.type()) do
      link = context.root <> "-symlink"
      :ok = File.ln_s(context.root, link)
      assert {:error, :invalid_abort_prepare_journal_root} = AbortPrepareJournal.load(link, claim)
      :ok = File.rm(link)

      insecure_root = context.root <> "-insecure"
      :ok = File.mkdir(insecure_root)
      :ok = File.chmod(insecure_root, 0o755)
      assert {:error, :invalid_abort_prepare_journal_root} = AbortPrepareJournal.load(insecure_root, claim)
      {:ok, _removed} = File.rm_rf(insecure_root)
    end

    {:ok, identity} = AbortPrepareJournal.identity_key(claim)
    request_path = Path.join(context.root, identity <> ".request.json")
    :ok = File.write(request_path, "{partial")

    assert {:held, :abort_prepare_journal_invalid} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    :ok = File.write(request_path, String.duplicate("x", 16_385))

    assert {:held, :abort_prepare_journal_invalid} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    :ok = File.rm(request_path)
    :ok = File.mkdir(request_path)

    assert {:held, :abort_prepare_journal_read_unavailable} =
             AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert Agent.get(posts, & &1) == 1
  end

  test "confirmation holds before mutation when root returns a different receipt", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})

        case attempt do
          1 -> {:ok, receipt(false, String.duplicate("f", 64))}
          2 -> {:ok, receipt(true, String.duplicate("0", 64))}
          _ -> raise "root witness transport interrupted"
        end
      end)

    post_fun = fn _url, options ->
      request = Jason.decode!(Keyword.fetch!(options, :body))

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "data" => %{
             "prepareId" => request["prepareId"],
             "projectionId" => "projection-one",
             "reservationId" => "reservation-one",
             "preparedAt" => "2026-09-27T12:00:00.000Z",
             "replayed" => false
           }
         }
       }}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)
    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert {:held, :abort_prepare_root_intent_unverified} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    assert Agent.get(context.client, & &1.deletes) == []

    assert {:held, :abort_prepare_root_intent_unverified} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "root publisher receives only exact selectors after confirmed delete", context do
    on_exit(fn -> Process.delete(:abort_root_input_publish_fun) end)

    assignment = assignment()
    {:ok, expected_result_reference} = AbortResultPublisher.reference_for_assignment(assignment)
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    witness_input = witness_input(fn _request -> {:ok, receipt(true, String.duplicate("8", 64))} end)

    post_fun = fn _url, options ->
      request = Jason.decode!(Keyword.fetch!(options, :body))

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "data" => %{
             "prepareId" => request["prepareId"],
             "projectionId" => "projection-one",
             "reservationId" => "reservation-one",
             "preparedAt" => "2026-09-27T12:00:00.000Z",
             "replayed" => false
           }
         }
       }}
    end

    Process.put(:abort_root_input_publish_fun, fn request ->
      assert Enum.sort(Map.keys(request)) ==
               Enum.sort(~w(schemaVersion operation claimSHA256 assignmentDigest allocationId resultReference))

      assert request["schemaVersion"] == 1
      assert request["operation"] == "publish_pre_execution_abort_inputs"
      assert request["assignmentDigest"] == assignment.sha256
      assert request["allocationId"] == allocation.id
      assert request["resultReference"] == expected_result_reference
      refute Map.has_key?(request, "checkpoints")
      refute Map.has_key?(request, "claim")
      refute Map.has_key?(request, "proofContext")
      :ok
    end)

    caller = caller_context(context, adapter, witness_input, post_fun)
    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    retain_result(caller, allocation, assignment)
    assert :ok = AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)
    assert length(Agent.get(context.client, & &1.deletes)) == 1

    changed = Map.put(caller, :root_abort_result_reference, "managed-abort-result:v1:other")

    assert {:held, :root_abort_result_reference_mismatch} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, changed)

    assert length(Agent.get(context.client, & &1.deletes)) == 1
  end

  test "a reference alone cannot authorize deletion without the retained typed result", context do
    {assignment, allocation, caller, prepared} = prepared_fixture(context)
    {:ok, reference} = AbortResultPublisher.reference_for_assignment(assignment)
    caller = Map.put(caller, :root_abort_result_reference, reference)
    Process.put(:abort_root_input_publish_fun, fn _ -> flunk("publisher called before result durability") end)

    assert {:held, :abort_result_journal_missing} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    assert File.ls!(context.result_root) == []
    assert Agent.get(context.client, & &1.deletes) == []

    for result <- [nil, %{}, Map.put(caller.pre_execution_result, :abort_reason, :activation_unavailable)] do
      assert {:held, :pre_execution_result_invalid} =
               AbortPrepareCaller.confirm(
                 allocation,
                 assignment,
                 key(assignment),
                 prepared,
                 Map.put(caller, :pre_execution_result, result)
               )
    end

    assert Agent.get(context.client, & &1.deletes) == []
    assert File.ls!(context.result_root) == []
    assert {:ok, record} = AbortPrepareJournal.load(context.root, claim_for_test())
    assert record.prepare_id == prepared.prepare_ack["prepareId"]
  end

  test "missing roots and conflicting selectors hold before deletion", context do
    {assignment, allocation, caller, prepared} = prepared_fixture(context)
    retain_result(caller, allocation, assignment)

    for root <- [nil, "relative", context.result_root <> "-missing", context.root, caller.workspace_root] do
      changed = put_in(caller, [:adapter_context, :abort_result_journal_root], root)
      assert {:held, _} = AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, changed)
    end

    changed = Map.put(caller, :root_abort_result_reference, "managed-abort-result:v1:other")

    assert {:held, :root_abort_result_reference_mismatch} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, changed)

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "corrupt, conflicting and changed result evidence is preserved with zero deletes", context do
    {assignment, allocation, caller, prepared} = prepared_fixture(context)
    retain_result(caller, allocation, assignment)
    path = result_path(caller, assignment)
    original = File.read!(path)
    payload = Jason.decode!(original)
    wire = payload["result_base64"] |> Base.decode64!() |> Jason.decode!()
    changed_wire = wire |> Map.put("projectionId", "other-projection") |> Jason.encode!()

    conflicts = [
      "{partial",
      String.replace_prefix(original, "{", "{\"schema_version\":1,"),
      Jason.encode!(Map.put(payload, "generation", payload["generation"] + 1)),
      Jason.encode!(Map.put(payload, "allocation_id", "other-allocation")),
      Jason.encode!(Map.put(payload, "assignment_digest", String.duplicate("f", 64))),
      Jason.encode!(Map.put(payload, "issue_uuid", "11111111-2222-4333-8444-555555555598")),
      Jason.encode!(Map.put(payload, "reference", "managed-abort-result:v1:other")),
      Jason.encode!(%{payload | "result_base64" => Base.encode64(changed_wire), "sha256" => sha256(changed_wire)})
    ]

    for bytes <- conflicts do
      :ok = File.write(path, bytes)

      assert {:held, :abort_result_journal_invalid} =
               AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

      assert File.read!(path) == bytes
      assert Agent.get(context.client, & &1.deletes) == []
    end

    :ok = File.write(path, original)
    changed = Map.put(caller, :pre_execution_result, blocked_result(assignment, :credential_lease_expired))

    assert {:held, :abort_result_journal_invalid} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, changed)

    assert File.read!(path) == original
    assert Agent.get(context.client, & &1.deletes) == []
  end

  @tag skip: match?({:win32, _}, :os.type())
  test "private result custody is required before deletion", context do
    {assignment, allocation, caller, prepared} = prepared_fixture(context)
    retain_result(caller, allocation, assignment)
    path = result_path(caller, assignment)

    :ok = File.chmod(path, 0o644)

    assert {:held, :abort_result_journal_read_unavailable} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    :ok = File.chmod(path, 0o600)

    :ok = File.chmod(context.result_root, 0o755)

    assert {:held, :invalid_abort_result_journal_root} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    :ok = File.chmod(context.result_root, 0o700)

    link = context.result_root <> "-link"
    :ok = File.ln_s(context.result_root, link)
    on_exit(fn -> File.rm(link) end)
    changed = put_in(caller, [:adapter_context, :abort_result_journal_root], link)

    assert {:held, :invalid_abort_result_journal_root} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, changed)

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "restart retains result and prepare identity and replays failed post-delete publication once", context do
    {assignment, allocation, caller, prepared} = prepared_fixture(context)
    retain_result(caller, allocation, assignment)
    path = result_path(caller, assignment)
    original = File.read!(path)
    original_prepare = AbortPrepareJournal.load(context.root, claim_for_test())
    parent = self()

    failed_publisher = fn request ->
      send(parent, {:publication, request})
      assert length(Agent.get(context.client, & &1.deletes)) == 1
      {:held, :root_abort_input_publication_unavailable}
    end

    assert {^prepared, {:held, :root_abort_input_publication_unavailable}} =
             confirm_after_restart(allocation, assignment, caller, failed_publisher)

    assert_receive {:publication, request}
    assert {:ok, record} = original_prepare
    uid = get_in(record.observation, ["job", "uid"])
    checkpoint = AbortPrepareJournal.load_confirmed_delete(context.root, record.claim, record, uid)
    assert {:ok, _} = checkpoint
    assert File.read!(path) == original
    assert AbortPrepareJournal.load(context.root, claim_for_test()) == original_prepare

    denied_publisher = fn _ -> flunk("publication with unavailable result evidence") end
    :ok = File.write(path, "{partial")

    assert {^prepared, {:held, :abort_result_journal_invalid}} =
             confirm_after_restart(allocation, assignment, caller, denied_publisher)

    assert File.read!(path) == "{partial"
    :ok = File.write(path, original)
    saved = path <> ".saved"
    :ok = File.rename(path, saved)

    assert {^prepared, {:held, :abort_result_journal_missing}} =
             confirm_after_restart(allocation, assignment, caller, denied_publisher)

    :ok = File.rename(saved, path)
    assert length(Agent.get(context.client, & &1.deletes)) == 1
    assert AbortPrepareJournal.load_confirmed_delete(context.root, record.claim, record, uid) == checkpoint

    publisher = fn replay ->
      send(parent, {:publication, replay})
      :ok
    end

    assert {^prepared, :ok} = confirm_after_restart(allocation, assignment, caller, publisher)
    assert_receive {:publication, ^request}
    assert length(Agent.get(context.client, & &1.deletes)) == 1
    assert AbortPrepareJournal.load_confirmed_delete(context.root, record.claim, record, uid) == checkpoint
    assert File.read!(path) == original
  end

  test "confirmation holds before mutation when the root receipt disappears", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})
        if attempt == 1, do: {:ok, receipt(false, String.duplicate("7", 64))}, else: {:error, :root_history_missing}
      end)

    post_fun = fn _url, options ->
      request = Jason.decode!(Keyword.fetch!(options, :body))

      data = %{
        "prepareId" => request["prepareId"],
        "projectionId" => "projection-one",
        "reservationId" => "reservation-one",
        "preparedAt" => "2026-09-27T12:00:00.000Z",
        "replayed" => false
      }

      {:ok, %Req.Response{status: 200, body: %{"data" => data}}}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)
    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)

    assert {:held, :abort_prepare_root_intent_unverified} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    assert Agent.get(context.client, & &1.deletes) == []
  end

  test "a committed delete with a lost response has no replay checkpoint", context do
    Process.put(:abort_root_input_publish_fun, fn _ -> flunk("publication after uncertain delete") end)
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    witness_input = witness_input(fn _request -> {:ok, receipt(true, String.duplicate("8", 64))} end)

    post_fun = fn _url, options ->
      request = Jason.decode!(Keyword.fetch!(options, :body))

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "data" => %{
             "prepareId" => request["prepareId"],
             "projectionId" => "projection-one",
             "reservationId" => "reservation-one",
             "preparedAt" => "2026-09-27T12:00:00.000Z",
             "replayed" => false
           }
         }
       }}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)
    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    retain_result(caller, allocation, assignment)
    Agent.update(context.client, &Map.put(&1, :raise_after_suspended_delete, true))

    assert {:held, :suspended_abort_delete_uncertain} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    {:ok, claim_key} = AbortPrepareJournal.identity_key(prepared.prepare_ack_guard_context.claim)
    checkpoint_path = Path.join(context.root, claim_key <> ".confirmed-delete.json")
    refute File.exists?(checkpoint_path)
    assert length(Agent.get(context.client, & &1.deletes)) == 1

    Agent.update(context.client, &Map.put(&1, :raise_after_suspended_delete, false))

    assert {:held, :suspended_abort_job_already_absent} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    refute File.exists?(checkpoint_path)
    assert length(Agent.get(context.client, & &1.deletes)) == 1
  end

  test "a post-delete Pod reappearance prevents the confirmed-delete checkpoint", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    witness_input = witness_input(fn _request -> {:ok, receipt(true, String.duplicate("9", 64))} end)

    post_fun = fn _url, options ->
      request = Jason.decode!(Keyword.fetch!(options, :body))

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "data" => %{
             "prepareId" => request["prepareId"],
             "projectionId" => "projection-one",
             "reservationId" => "reservation-one",
             "preparedAt" => "2026-09-27T12:00:00.000Z",
             "replayed" => false
           }
         }
       }}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)
    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    retain_result(caller, allocation, assignment)

    late_pod = %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{
        "namespace" => prepared.observation["compiledIdentity"]["namespace"],
        "name" => "other-worker",
        "uid" => "late-pod-uid",
        "resourceVersion" => "late-pod-rv",
        "labels" => %{},
        "ownerReferences" => []
      },
      "spec" => %{
        "volumes" => [
          %{"name" => "shared", "persistentVolumeClaim" => %{"claimName" => prepared.observation["slotBinding"]["claimName"]}}
        ]
      }
    }

    Agent.update(context.client, &Map.put(&1, :pod_injected_on_suspended_delete, {"late-pod", late_pod}))

    assert {:held, :suspended_abort_pod_absence_unverified} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)

    {:ok, claim_key} = AbortPrepareJournal.identity_key(prepared.prepare_ack_guard_context.claim)
    refute File.exists?(Path.join(context.root, claim_key <> ".confirmed-delete.json"))
    assert length(Agent.get(context.client, & &1.deletes)) == 1
  end

  test "confirmation rejects a changed assignment lease claim before root or Kubernetes access", context do
    assignment = assignment()
    adapter = adapter_context(context, assignment)

    assert {:ok, allocation} =
             ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)

    root_calls = Agent.start_link(fn -> 0 end) |> elem(1)

    witness_input =
      witness_input(fn _request ->
        attempt = Agent.get_and_update(root_calls, &{&1 + 1, &1 + 1})
        {:ok, receipt(attempt > 1, String.duplicate("6", 64))}
      end)

    post_fun = fn _url, options ->
      request = Jason.decode!(Keyword.fetch!(options, :body))

      {:ok,
       %Req.Response{
         status: 200,
         body: %{
           "data" => %{
             "prepareId" => request["prepareId"],
             "projectionId" => "projection-one",
             "reservationId" => "reservation-one",
             "preparedAt" => "2026-09-27T12:00:00.000Z",
             "replayed" => false
           }
         }
       }}
    end

    caller = caller_context(context, adapter, witness_input, post_fun)
    assert {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    changed = %{caller | reservation: %{reservation() | process_id: "different-process"}}

    assert {:held, :abort_prepare_claim_binding_mismatch} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, changed)

    assert Agent.get(root_calls, & &1) == 1
    assert Agent.get(context.client, & &1.deletes) == []

    changed_binding = put_in(caller, [:adapter_context, :claim_binding, :repository_ref], "other/repo")

    assert {:held, :abort_prepare_claim_binding_mismatch} =
             AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, changed_binding)

    assert Agent.get(root_calls, & &1) == 1
    assert Agent.get(context.client, & &1.deletes) == []

    claim_binding_mismatches = [
      put_in(caller, [:adapter_context, :claim_binding, :workspace_id], "other-workspace"),
      put_in(caller, [:adapter_context, :claim_binding, :managed_project_profile_id], "other-profile"),
      put_in(caller, [:adapter_context, :claim_binding, :scope_keys], ["repo:other"]),
      put_in(caller, [:adapter_context, :claim_binding, :nonce_sha256], String.duplicate("0", 64))
    ]

    for changed_binding <- claim_binding_mismatches do
      assert {:held, :abort_prepare_claim_binding_mismatch} =
               AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, changed_binding)
    end

    assert Agent.get(root_calls, & &1) == 1
    assert Agent.get(context.client, & &1.deletes) == []
  end

  defp caller_context(context, adapter, witness_input, post_fun) do
    %{
      adapter_context: Map.put(adapter, :abort_result_journal_root, context.result_root),
      pre_execution_result: blocked_result(assignment()),
      witness_input: witness_input,
      reservation: reservation(),
      provider_context: %{base_url: "https://dahlia.example", runner_token: "synthetic-token"},
      journal_root: context.root,
      workspace_root: Path.join(System.tmp_dir!(), "symphony-worker-workspaces"),
      post_fun: post_fun,
      root_abort_input_publisher: SymphonyElixir.AbortPrepareTestRootInputPublisher
    }
  end

  defp blocked_result(assignment, reason \\ :credential_lease_denied) do
    %{
      assignment_digest: assignment.sha256,
      abort_reason: reason,
      outcome: :blocked,
      summary: Record.pre_execution_summary(reason),
      evidence_ref: "managed-executor:#{assignment.sha256}:#{reason}"
    }
  end

  defp retain_result(caller, allocation, assignment) do
    assert {:ok, _reference} =
             AbortResultPublisher.publish_or_reconcile_abort_result(
               allocation,
               assignment,
               caller.pre_execution_result,
               assignment.sha256 <> ":abort-result",
               caller.adapter_context
             )
  end

  defp result_path(caller, assignment) do
    {:ok, reference} = AbortResultPublisher.reference_for_assignment(assignment)
    Path.join(caller.adapter_context.abort_result_journal_root, sha256(reference) <> ".abort-result.json")
  end

  defp prepared_fixture(context) do
    assignment = assignment()
    adapter = adapter_context(context, assignment)
    {:ok, allocation} = ManagedExecutorAdapter.allocate_or_reconcile(assignment, allocation_key(assignment), adapter)
    witness = witness_input(fn _ -> {:ok, receipt(true, String.duplicate("8", 64))} end)

    post = fn _url, options ->
      request = Jason.decode!(Keyword.fetch!(options, :body))

      data = %{
        "prepareId" => request["prepareId"],
        "projectionId" => "projection-one",
        "reservationId" => "reservation-one",
        "preparedAt" => "2026-09-27T12:00:00.000Z",
        "replayed" => false
      }

      {:ok, %Req.Response{status: 200, body: %{"data" => data}}}
    end

    caller = caller_context(context, adapter, witness, post)
    {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
    {assignment, allocation, caller, prepared}
  end

  defp confirm_after_restart(allocation, assignment, caller, publisher) do
    Task.async(fn ->
      if match?({:win32, _}, :os.type()) do
        Process.put(:abort_prepare_journal_windows_test_only, true)
        Process.put(:abort_result_journal_windows_test_only, true)
      end

      Process.put(:abort_root_input_publish_fun, publisher)
      {:ok, prepared} = AbortPrepareCaller.prepare(allocation, assignment, key(assignment), caller)
      {prepared, AbortPrepareCaller.confirm(allocation, assignment, key(assignment), prepared, caller)}
    end)
    |> Task.await()
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp claim_for_test do
    input = witness_input(fn _request -> {:error, :unused} end)
    {:ok, %{"claim" => claim}} = HostWitness.request(input, "claim_bound", reservation())
    Map.put(claim, "pool", input.pool_key)
  end

  defp receipt(replayed, hash) do
    %{
      "ok" => true,
      "receipt" => %{
        "version" => 1,
        "sequence" => 1,
        "hash" => hash,
        "replayed" => replayed
      }
    }
  end

  defp witness_input(witness) do
    %{
      pool_key: "midgard",
      issue_id: "11111111-2222-4333-8444-555555555599",
      runner_id: "runner-17",
      managed_project_profile_id: "profile-one",
      repository_ref: "hypergridau/symphony",
      host_witness_fun: witness
    }
  end

  defp reservation do
    %{
      projection_id: "projection-one",
      reservation_id: "reservation-one",
      workspace_id: "workspace-one",
      company_id: "company-one",
      issue_id: "11111111-2222-4333-8444-555555555599",
      runner_id: "runner-17",
      managed_project_profile_id: "profile-one",
      repository_ref: "hypergridau/symphony",
      scope_keys: ["repo:hypergridau/symphony"],
      generation: 4,
      session_id: "session-one",
      process_id: "process-one",
      responsible_delegation_id: "delegation-one",
      execution_fence_token: "11111111-2222-4333-8444-555555555599:4",
      runtime_lease_id: "session-one",
      reservation_nonce: "secret-nonce"
    }
  end

  defp adapter_context(context, assignment) do
    slot = %{
      slot_id: "luna-slot-1",
      claim_name: "frigga-codex-luna-slot-1",
      claim_uid: "pvc-uid-one",
      lease_id: "11111111-2222-4333-8444-555555555501",
      assignment_sha256: assignment.sha256,
      seat: assignment.seat
    }

    %{
      client: RKE2JobFakeClient,
      client_context_provider: SymphonyElixir.AbortPrepareTestClientContext,
      client_context_provider_context: context.client,
      allocation_registry: SymphonyElixir.AbortPrepareTestRegistry,
      allocation_registry_context: nil,
      claim_binding: %{
        projection_id: "projection-one",
        reservation_id: "reservation-one",
        workspace_id: "workspace-one",
        company_id: "company-one",
        issue_id: "11111111-2222-4333-8444-555555555599",
        runner_id: "runner-17",
        generation: 4,
        repository_ref: "hypergridau/symphony",
        managed_project_profile_id: "profile-one",
        session_id: "session-one",
        process_id: "process-one",
        responsible_delegation_id: "delegation-one",
        execution_fence_token: "11111111-2222-4333-8444-555555555599:4",
        runtime_lease_id: "session-one",
        scope_keys: ["repo:hypergridau/symphony"],
        nonce_sha256: :crypto.hash(:sha256, "secret-nonce") |> Base.encode16(case: :lower)
      },
      auth_slot_lease_guard: SymphonyElixir.AbortPrepareTestSlotGuard,
      auth_slot_lease_guard_context: nil,
      config: %{
        namespace: "symphony-beta",
        image: "registry.example/symphony-worker@sha256:" <> String.duplicate("a", 64),
        repository_id: "123456789",
        auth_slot: slot,
        auth_slot_catalog: %{slot.slot_id => slot.claim_name}
      }
    }
  end

  defp assignment do
    {:ok, bundle} =
      ManagedAssignmentBundle.build(%{
        objective: %{id: "objective-1", identity: "objective-1", content: "Run a bounded source worker"},
        repository_ref: "hypergridau/symphony",
        base_ref: "refs/remotes/origin/main",
        branch: "codex/hgs733-host-abort-prepare",
        seat: "runner-17",
        lease: %{
          issue_id: "11111111-2222-4333-8444-555555555599",
          repository: "hypergridau/symphony",
          generation: 4,
          session_id: "session-one",
          process_id: "process-one"
        },
        intent_ancestry: ["objective-root", "delegation-one"],
        acceptance: %{deliverable: "durable abort prepare", evidence: "focused caller tests"},
        context_secret_refs: ["DAHLIA_WORK_PACKAGE_RUNNER_TOKEN"],
        platform: "linux-x86_64",
        environment_classification: "repository",
        environment_constraints: ["repository", "no-production-workload"],
        placement: :internal_beta,
        target_environment: :rke2
      })

    bundle
  end

  defp key(assignment), do: assignment.sha256 <> ":abort_unstarted"
  defp allocation_key(assignment), do: assignment.sha256 <> ":allocation"
end
