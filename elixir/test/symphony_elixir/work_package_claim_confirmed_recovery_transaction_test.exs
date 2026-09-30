defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransactionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryContext
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryCore, as: Transaction
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryLineage
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransaction, as: Facade
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWAL
  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.WorkPackageClaim.Journal

  test "WAL replay completes a crash after any partial prefix of state writes" do
    preimages = ["journal-before", "fence-before", "graph-before"]
    postimages = ["journal-after", "fence-after", "graph-after"]
    hashes = Enum.map(preimages, &sha256/1)

    for crash_after <- 0..2 do
      partially_applied =
        Enum.with_index(Enum.zip(preimages, postimages))
        |> Enum.map(fn {{before, after_image}, index} ->
          if index < crash_after, do: after_image, else: before
        end)

      replayed =
        Enum.zip([partially_applied, hashes, postimages])
        |> Enum.map(fn {current, preimage_hash, postimage} ->
          case Transaction.classify_image(current, preimage_hash, postimage) do
            :write -> postimage
            :already_applied -> current
          end
        end)

      assert replayed == postimages

      assert Enum.all?(Enum.zip(replayed, postimages), fn {actual, expected} ->
               Transaction.classify_image(actual, sha256("stale"), expected) == :already_applied
             end)
    end
  end

  test "WAL replay rejects a contradictory or missing preimage" do
    postimage = "exact-postimage"

    assert {:error, :transaction_target_conflict} =
             Transaction.classify_image("changed-by-another-writer", sha256("expected-preimage"), postimage)

    assert {:error, :transaction_target_conflict} = Transaction.classify_image(nil, sha256("before"), postimage)
  end

  test "production image replay stops before persisting any image after a conflict" do
    parent = self()

    images = [
      %{name: :journal, preimage_sha256: sha256("expected"), postimage_bytes: "journal-post"},
      %{name: :fence, preimage_sha256: sha256("fence-before"), postimage_bytes: "fence-post"}
    ]

    assert {:error, :transaction_target_conflict} =
             ConfirmedRecoveryWAL.apply_images(
               images,
               fn
                 :journal ->
                   "contradictory"

                 :fence ->
                   send(parent, :later_image_must_not_be_read)
                   "fence-before"
               end,
               fn name, _bytes, _already_applied? -> send(parent, {:persist_must_not_run, name}) end
             )

    refute_received :later_image_must_not_be_read
    refute_received {:persist_must_not_run, _name}
  end

  test "production WAL sequencer stops after the first failed state write" do
    parent = self()

    assert {:error, :synthetic_write_failure} =
             ConfirmedRecoveryWAL.replay([:journal, :fence, :graph], fn
               :journal ->
                 send(parent, :journal_written)
                 :ok

               :fence ->
                 send(parent, :fence_attempted)
                 {:error, :synthetic_write_failure}

               :graph ->
                 send(parent, :graph_must_not_be_attempted)
                 :ok
             end)

    assert_received :journal_written
    assert_received :fence_attempted
    refute_received :graph_must_not_be_attempted
    assert {:error, :invalid_replay_request} = ConfirmedRecoveryWAL.replay(:not_a_list, fn _ -> :ok end)
    assert {:error, :invalid_replay_result} = ConfirmedRecoveryWAL.replay([:bad], fn _ -> :skip end)
  end

  test "completion persists the terminal marker before releasing directory custody" do
    parent = self()

    assert :ok =
             ConfirmedRecoveryWAL.commit_then_release(
               fn ->
                 send(parent, :terminal_marker_persisted)
                 :ok
               end,
               fn ->
                 assert_received :terminal_marker_persisted
                 send(parent, :directory_custody_released)
                 :ok
               end
             )

    assert_received :directory_custody_released

    assert {:error, :synthetic_marker_failure} =
             ConfirmedRecoveryWAL.commit_then_release(
               fn -> {:error, :synthetic_marker_failure} end,
               fn -> send(parent, :custody_must_remain) end
             )

    refute_received :custody_must_remain

    assert {:error, :invalid_finalization_result} =
             ConfirmedRecoveryWAL.commit_then_release(fn -> :skipped end, fn -> :ok end)
  end

  test "completed lineage accepts gen2 release retained in fence history after gen3 admission" do
    issue_id = "f77e349e-21d9-4bdf-bad3-ce08b302e7e8"
    expected = %{"issueId" => issue_id, "sessionId" => "session-2", "processId" => "process-2", "responsibleDelegationId" => "delegation"}

    gen2 = %{
      generation: 2,
      status: :active,
      ownership: :reconciled,
      cleanup: :pending,
      terminal: nil,
      cleanup_receipt: nil,
      retirement: nil,
      termination_unconfirmed: false,
      leases: %{
        "session-2" => %{
          process_id: "process-2",
          status: :released,
          release_reason: :spawn_failed,
          termination_required: false
        }
      }
    }

    fence = %{executions: %{issue_id => %{generation: 3}}, history: [Map.put(gen2, :issue_id, issue_id)]}
    assert :ok = ConfirmedRecoveryLineage.released_fence_lease(fence, issue_id, expected)

    graph = %{
      delegations: %{
        "delegation" => %{runtime_lease: %{issue_id: issue_id, generation: 3}}
      },
      events: [
        %{type: :runtime_lease_released, delegation_id: "delegation", at_ms: 100},
        %{type: :runtime_lease_bound, delegation_id: "delegation", at_ms: 101}
      ]
    }

    assert :ok = ConfirmedRecoveryLineage.released_graph_lease(graph, expected, 100)
    refute ConfirmedRecoveryLineage.released_graph_lease(graph, expected, 99) == :ok
    refute ConfirmedRecoveryLineage.released_graph_lease(put_in(graph, [:events, Access.at(1), :at_ms], 100), expected, 100) == :ok
    refute ConfirmedRecoveryLineage.released_fence_lease(%{fence | history: []}, issue_id, expected) == :ok
  end

  test "WAL replay resumes each partial state-write prefix and skips exact postimages" do
    preimages = %{"journal" => "journal-before", "fence" => "fence-before", "graph" => "graph-before"}
    postimages = %{"journal" => "journal-after", "fence" => "fence-after", "graph" => "graph-after"}
    preimage_hashes = Map.new(preimages, fn {name, bytes} -> {name, sha256(bytes)} end)

    for crash_after <- 0..3 do
      {:ok, state} = Agent.start_link(fn -> {preimages, 0} end)

      first_attempt =
        run_replay(state, preimage_hashes, postimages, fn writes -> writes < crash_after end)

      if crash_after < 3 do
        assert {:error, :synthetic_crash} = first_attempt
      else
        assert :ok = first_attempt
      end

      assert :ok = run_replay(state, preimage_hashes, postimages, fn _writes -> true end)
      {replayed, writes_after_replay} = Agent.get(state, & &1)
      assert replayed == postimages

      assert :ok = run_replay(state, preimage_hashes, postimages, fn _writes -> true end)
      assert {^replayed, ^writes_after_replay} = Agent.get(state, & &1)
      Agent.stop(state)
    end
  end

  test "WAL replay repairs ownership after a crash between postimage save and metadata restore" do
    {:ok, state} = Agent.start_link(fn -> %{bytes: "before", uid: 1001, gid: 1001, mode: 0o600} end)
    preimage_sha = sha256("before")

    assert {:error, :synthetic_crash_after_save} =
             Transaction.apply_image("before", preimage_sha, "after", fn bytes ->
               Agent.update(state, fn image -> %{image | bytes: bytes, uid: 0, gid: 0, mode: 0o600} end)
               {:error, :synthetic_crash_after_save}
             end)

    transient = Agent.get(state, & &1)
    assert transient.bytes == "after" and transient.uid == 0

    assert :ok =
             Transaction.apply_image(transient.bytes, preimage_sha, "after", fn bytes ->
               Agent.update(state, fn image -> %{image | bytes: bytes, uid: 1001, gid: 1001, mode: 0o600} end)
               :ok
             end)

    assert %{bytes: "after", uid: 1001, gid: 1001, mode: 0o600} = Agent.get(state, & &1)
    Agent.stop(state)
  end

  test "startup may pass an untouched tree but denies evidence without its write-ahead marker" do
    assert :ok = Transaction.pre_marker_startup_policy(false)
    assert {:error, :hgs740_transaction_marker_missing} = Transaction.pre_marker_startup_policy(true)

    assert :ok = Transaction.no_marker_startup_policy("unrelated-issue", false)

    assert {:error, :hgs740_transaction_marker_missing} =
             Transaction.no_marker_startup_policy("unrelated-issue", true)

    assert {:error, :hgs740_transaction_marker_missing} =
             Transaction.no_marker_startup_policy("f77e349e-21d9-4bdf-bad3-ce08b302e7e8", false)
  end

  test "root entry points reject malformed arguments before any filesystem action" do
    assert {:error, :invalid_confirmed_recovery_request} = Facade.apply(nil, "midgard", "/workflow.md", "nonce")
    assert {:error, :invalid_hgs740_completion_request} = Facade.complete("issue", :midgard, "/workflow.md")
    assert {:error, :invalid_hgs740_startup_request} = Facade.verify_startup("/workflow.md", nil)
    assert Transaction.marker_directory("issue") == "/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery/issue/generation-2"
  end

  test "production Core independently authorizes every entry point before host callbacks" do
    issue_id = "24e34a86-b214-41bc-8a35-9e1d31bfb8e4"
    pool = "unknown-pool"
    workflow_path = "/srv/dahlia-runner-state/dahlia/config/symphony/workflows/midgard.md"
    nonce = "test-nonce"

    # The synthetic context used by policy tests is never accepted by a production
    # entry point. Every call reaches the fixed RootHost authorization sequence;
    # whether this process is root changes which first gate denies the request.
    assert {:error, reason} = Transaction.apply(issue_id, pool, workflow_path, nonce)
    assert reason in [:root_privilege_required, :invalid_pool]
    assert {:error, ^reason} = Transaction.complete(issue_id, pool, workflow_path)
    assert {:error, ^reason} = Transaction.verify_startup(workflow_path, pool)
    assert {:error, ^reason} = Facade.apply(issue_id, pool, workflow_path, nonce)
  end

  test "systemctl always targets the system manager and ignores inherited bus overrides" do
    assert {"/usr/bin/systemctl", ["--system", "show", "dahlia-symphony@midgard.service"], options} =
             ConfirmedRecoveryRootHost.systemctl_invocation_for_test(["show", "dahlia-symphony@midgard.service"])

    assert Keyword.fetch!(options, :stderr_to_stdout)

    assert Keyword.fetch!(options, :env) == [
             {"DBUS_SYSTEM_BUS_ADDRESS", nil},
             {"DBUS_SESSION_BUS_ADDRESS", nil},
             {"SYSTEMD_BUS_ADDRESS", nil},
             {"XDG_RUNTIME_DIR", nil}
           ]
  end

  test "Core startup admits an untouched evidence root and short-circuits an unsafe root" do
    {:ok, runtime} = ConfirmedRecoveryRootHost.fixed_runtime_paths("midgard")
    evidence_root = "/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery"

    untouched =
      core_context(%{
        lstat: fn ^evidence_root -> {:error, :enoent} end
      })

    assert :ok = Transaction.validate_test_context(untouched)
    assert :ok = Transaction.verify_startup_with_test_context(untouched)

    parent = self()

    writable_root =
      core_context(%{
        lstat: fn ^evidence_root ->
          send(parent, :evidence_root_checked)
          {:ok, %File.Stat{type: :directory, uid: 0, mode: 0o750}}
        end,
        ls: fn _path ->
          send(parent, :directory_must_not_be_listed)
          {:ok, []}
        end
      })

    assert {:error, :hgs740_startup_held_closed} = Transaction.verify_startup_with_test_context(writable_root)
    assert_received :evidence_root_checked
    refute_received :directory_must_not_be_listed
    assert runtime.journal_path == "/srv/dahlia-runner-state/run/pools/midgard/work-package.json"
  end

  test "Core apply stops at the service gate before evidence, signer, or state callbacks" do
    parent = self()

    context =
      core_context(%{
        require_service_stopped: fn pool ->
          send(parent, {:service_gate, pool})
          {:error, :pool_service_not_proven_stopped}
        end,
        require_services_quiescent: fn ->
          send(parent, :later_gate_must_not_run)
          :ok
        end,
        read: fn _path ->
          send(parent, :evidence_must_not_be_read)
          {:ok, ""}
        end,
        verify_signed_evidence: fn _bytes, _bindings ->
          send(parent, :signature_must_not_be_verified)
          {:error, :invalid}
        end
      })

    assert :ok = Transaction.validate_test_context(context)

    assert {:error, :pool_service_not_proven_stopped} =
             Transaction.apply_with_test_context(context)

    assert_received {:service_gate, "midgard"}
    refute_received :later_gate_must_not_run
    refute_received :evidence_must_not_be_read
    refute_received :signature_must_not_be_verified
  end

  test "Core apply short-circuits each admission gate before reading marker evidence" do
    {:ok, calls} = Agent.start_link(fn -> [] end)

    cases = [
      {:service_stopped, :pool_service_not_proven_stopped, [:service_stopped]},
      {:services_quiescent, :managed_services_not_quiescent, [:service_stopped, :services_quiescent]},
      {:paused_gate, :global_gate_not_paused, [:service_stopped, :services_quiescent, :paused_gate]}
    ]

    for {failed_gate, expected_error, expected_calls} <- cases do
      callback = fn gate ->
        Agent.update(calls, &[gate | &1])
        if gate == failed_gate, do: {:error, expected_error}, else: :ok
      end

      context =
        core_context(%{
          require_service_stopped: fn _pool -> callback.(:service_stopped) end,
          require_services_quiescent: fn -> callback.(:services_quiescent) end,
          require_paused_gate: fn -> callback.(:paused_gate) end,
          lstat: fn _path ->
            Agent.update(calls, &[:marker_read | &1])
            {:error, :enoent}
          end
        })

      assert {:error, ^expected_error} = Transaction.apply_with_test_context(context)
      assert Enum.reverse(Agent.get(calls, & &1)) == expected_calls
      Agent.update(calls, fn _ -> [] end)
    end

    Agent.stop(calls)
  end

  test "Core applies a real ephemeral signed gen2 transition from persisted preimages" do
    fixture = positive_apply_fixture()
    context = fixture.context

    assert :ok = Transaction.validate_test_context(context)
    assert :ok = Transaction.exact_local_predecessor(fixture.predecessor_paths, fixture.predecessor, fixture.expected)
    decoded_execution = fixture.predecessor_paths.fence.state.executions[fixture.expected["issueId"]]
    refute Map.has_key?(decoded_execution, :retirement)
    assert :ok = Transaction.exact_active_fence(fixture.predecessor_paths.fence.state, fixture.expected["issueId"], fixture.expected)

    contradictory_fence =
      put_in(fixture.predecessor_paths.fence.state, [:executions, fixture.expected["issueId"], :retirement], %{})

    refute Transaction.exact_active_fence(contradictory_fence, fixture.expected["issueId"], fixture.expected) == :ok

    assert :ok = Transaction.exact_active_runtime_lease(fixture.predecessor_paths.graph.state, fixture.expected)

    assert {:ok, :applied} = Transaction.apply_with_test_context(context)

    marker = Map.fetch!(Agent.get(fixture.vfs, & &1.files), fixture.marker_path)
    assert {:ok, decoded_marker} = Jason.decode(marker)
    assert decoded_marker["status"] == "local_applied"
    assert decoded_marker["generation"] == 2
    assert decoded_marker["reservationId"] == fixture.reservation_id
    assert decoded_marker["preimages"]["claimJournalSHA256"] == sha256(fixture.journal_bytes)
    assert decoded_marker["preimages"]["fenceSHA256"] == sha256(fixture.fence_bytes)
    assert decoded_marker["preimages"]["responsibilityGraphSHA256"] == sha256(fixture.graph_bytes)
    assert decoded_marker["postimages"]["claimJournal"]["sha256"] != decoded_marker["preimages"]["claimJournalSHA256"]
    assert Agent.get(fixture.vfs, &{&1.mutation_gate_calls, &1.state_write_calls}) == {10, 3}

    final_files = Agent.get(fixture.vfs, & &1.files)

    for {name, path} <- [
          {"claimJournal", fixture.state_paths.journal},
          {"fence", fixture.state_paths.fence},
          {"responsibilityGraph", fixture.state_paths.graph}
        ] do
      assert sha256(final_files[path]) == decoded_marker["postimages"][name]["sha256"]
    end

    assert {:ok, candidate} = Jason.decode(Map.fetch!(final_files, fixture.local_candidate_path))
    assert candidate["contractVersion"] == "work-package-hgs740-local-transition-receipt.v1"
    assert candidate["postconditions"]["dispatchPhase"] == "recovery_pending"

    assert {:error, _reason} =
             ConfirmedRecoveryRootHost.verify_signed_evidence(fixture.proof_bytes, fixture.bindings)

    Agent.stop(fixture.vfs)
  end

  test "Core completion preserves its held-closed result and stops after the service gate" do
    parent = self()

    context =
      core_context(%{
        require_service_stopped: fn _pool ->
          send(parent, :service_gate)
          {:error, :service_changed}
        end,
        require_services_quiescent: fn ->
          send(parent, :later_gate_must_not_run)
          :ok
        end,
        lstat: fn _path ->
          send(parent, :marker_must_not_be_read)
          {:error, :enoent}
        end
      })

    assert :ok = Transaction.validate_test_context(context)
    assert {:error, :hgs740_completion_held_closed} = Transaction.complete_with_test_context(context)
    assert_received :service_gate
    refute_received :later_gate_must_not_run
    refute_received :marker_must_not_be_read
  end

  test "Core completion collapses quiescence denial before inspecting the marker" do
    parent = self()

    context =
      core_context(%{
        require_service_stopped: fn _pool ->
          send(parent, :service_gate)
          :ok
        end,
        require_services_quiescent: fn ->
          send(parent, :quiescence_gate)
          {:error, :managed_services_not_quiescent}
        end,
        require_paused_gate: fn ->
          send(parent, :later_gate_must_not_run)
          :ok
        end,
        lstat: fn _path ->
          send(parent, :marker_must_not_be_read)
          {:error, :enoent}
        end
      })

    assert {:error, :hgs740_completion_held_closed} = Transaction.complete_with_test_context(context)
    assert_received :service_gate
    assert_received :quiescence_gate
    refute_received :later_gate_must_not_run
    refute_received :marker_must_not_be_read
  end

  test "candidate decoder accepts only exact insertion-order Python JSON bytes" do
    canonical = ~S({"z":1,"a":{"b":2}})
    assert {:ok, %{"z" => 1, "a" => %{"b" => 2}}} = Transaction.decode_candidate_bytes(canonical)
    assert {:error, :invalid_candidate_json} = Transaction.decode_candidate_bytes(~S({ "z":1,"a":{"b":2}}))
    assert {:error, :invalid_candidate_json} = Transaction.decode_candidate_bytes(~S({"z":1,"z":2}))
  end

  test "provider HTTP response parsing allows transport whitespace but rejects duplicate fields" do
    assert {:ok, %{"data" => %{"state" => "prepared"}}} =
             Transaction.decode_provider_response(~S({ "data": { "state": "prepared" } }) <> "\n")

    assert {:error, :invalid_provider_response_json} =
             Transaction.decode_provider_response(~S({"data":{"state":"prepared","state":"released"}}))
  end

  test "Core binds the exact proof to persisted preimages, issue, pool, nonce, and verification time" do
    paths = %{
      journal: %{bytes: "journal-before"},
      fence: %{bytes: "fence-before"},
      graph: %{bytes: "graph-before"}
    }

    payload = %{"reservationId" => "reservation-2", "assignmentSHA256" => String.duplicate("a", 64)}

    bindings =
      Transaction.proof_bindings(
        payload,
        "midgard",
        "24e34a86-b214-41bc-8a35-9e1d31bfb8e4",
        "nonce-2",
        paths,
        1_790_762_400_000
      )

    assert bindings == %{
             pool: "midgard",
             issue_id: "24e34a86-b214-41bc-8a35-9e1d31bfb8e4",
             generation: 2,
             reservation_id: "reservation-2",
             assignment_sha256: String.duplicate("a", 64),
             nonce: "nonce-2",
             fence_sha256: sha256("fence-before"),
             claim_journal_sha256: sha256("journal-before"),
             responsibility_graph_sha256: sha256("graph-before"),
             now_ms: 1_790_762_400_000
           }

    changed = put_in(paths, [:graph, :bytes], "changed-graph")

    refute Transaction.proof_bindings(payload, "midgard", "24e34a86-b214-41bc-8a35-9e1d31bfb8e4", "nonce-2", changed, 1_790_762_400_000) ==
             bindings
  end

  test "Core loads bound evidence and delegates signature verification before accepting the observation" do
    issue_id = "24e34a86-b214-41bc-8a35-9e1d31bfb8e4"
    nonce = "nonce-2"
    observation = %{"expected" => %{"issueId" => issue_id, "generation" => 2}, "globalPause" => true}

    payload = %{
      "reservationId" => "reservation-2",
      "assignmentSHA256" => String.duplicate("a", 64),
      "observation" => observation
    }

    fixture = core_proof_fixture(payload)
    parent = self()

    paths =
      put_in(fixture.paths.runtime.host_ops.verify_signed_evidence, fn bytes, bindings ->
        send(parent, {:signature_check, bytes, bindings})
        {:ok, payload}
      end).paths

    assert {:ok, ^observation, observation_bytes, proof_bytes, ^payload, 1_790_762_400_000} =
             Transaction.verify_signed_proof(issue_id, "midgard", nonce, paths)

    assert observation_bytes == fixture.observation_bytes
    assert proof_bytes == fixture.proof_bytes
    assert_received {:signature_check, ^proof_bytes, bindings}

    assert bindings == %{
             pool: "midgard",
             issue_id: issue_id,
             generation: 2,
             reservation_id: "reservation-2",
             assignment_sha256: String.duplicate("a", 64),
             nonce: nonce,
             fence_sha256: sha256("fence-before"),
             claim_journal_sha256: sha256("journal-before"),
             responsibility_graph_sha256: sha256("graph-before"),
             now_ms: 1_790_762_400_000
           }
  end

  test "Core rejects a signature callback denial and never accepts evidence on callback error" do
    issue_id = "24e34a86-b214-41bc-8a35-9e1d31bfb8e4"
    observation = %{"expected" => %{"issueId" => issue_id, "generation" => 2}, "globalPause" => true}
    payload = %{"reservationId" => "reservation-2", "assignmentSHA256" => String.duplicate("a", 64), "observation" => observation}
    fixture = core_proof_fixture(payload)
    parent = self()

    paths =
      put_in(fixture.paths.runtime.host_ops.verify_signed_evidence, fn _bytes, _bindings ->
        send(parent, :signature_denied)
        {:error, :invalid_confirmed_recovery_evidence}
      end).paths

    assert {:error, :invalid_confirmed_recovery_evidence} =
             Transaction.verify_signed_proof(issue_id, "midgard", "nonce-2", paths)

    assert_received :signature_denied
  end

  test "Core creates the initial marker only under private evidence directories and syncs before returning" do
    issue_id = "24e34a86-b214-41bc-8a35-9e1d31bfb8e4"
    context = core_context(%{})
    host_ops = context.host_ops
    runtime = Map.put(context.runtime, :host_ops, host_ops)
    evidence_root = host_ops.paths.evidence_root
    marker_path = Path.join([evidence_root, issue_id, "generation-2", "transaction.json"])
    marker_directory = Path.dirname(marker_path)
    parent = self()

    host_ops =
      Map.merge(host_ops, %{
        lstat: fn path ->
          cond do
            path == evidence_root or String.starts_with?(path, evidence_root <> "/") ->
              {:ok, %File.Stat{type: :directory, uid: 0, gid: 0, mode: 0o700}}

            path in ["/", "/srv", "/srv/dahlia-runner-state", "/srv/dahlia-runner-state/evidence"] ->
              {:ok, %File.Stat{type: :directory, uid: 0, gid: 0, mode: 0o755}}

            true ->
              {:error, :enoent}
          end
        end,
        raw_open: fn path, modes ->
          send(parent, {:marker_io, {:open, path, modes}})
          {:ok, if(path == marker_path, do: :marker_file, else: :marker_directory)}
        end,
        raw_write: fn file, bytes ->
          send(parent, {:marker_io, {:write, file, bytes}})
          :ok
        end,
        raw_sync: fn file ->
          send(parent, {:marker_io, {:sync, file}})
          :ok
        end,
        chmod: fn path, mode ->
          send(parent, {:marker_io, {:chmod, path, mode}})
          :ok
        end,
        raw_close: fn file ->
          send(parent, {:marker_io, {:close, file}})
          :ok
        end
      })

    runtime = Map.put(runtime, :host_ops, host_ops)
    marker = %{"issueId" => issue_id, "status" => "applying"}
    encoded_marker = Jason.encode!(marker)

    assert :ok = Transaction.persist_initial_marker_with_test_context(marker_path, marker, runtime)
    assert_received {:marker_io, {:open, ^marker_path, [:write, :binary, :raw, :exclusive, :sync]}}
    assert_received {:marker_io, {:write, :marker_file, ^encoded_marker}}
    assert_received {:marker_io, {:sync, :marker_file}}
    assert_received {:marker_io, {:chmod, ^marker_path, 0o600}}
    assert_received {:marker_io, {:close, :marker_file}}
    assert_received {:marker_io, {:open, ^marker_directory, [:read, :raw]}}
    assert_received {:marker_io, {:sync, :marker_directory}}
    assert_received {:marker_io, {:close, :marker_directory}}
    refute_received {:marker_io, _}
  end

  test "Core refuses initial marker creation before any write when evidence custody is untrusted" do
    issue_id = "24e34a86-b214-41bc-8a35-9e1d31bfb8e4"
    context = core_context(%{})
    runtime = Map.put(context.runtime, :host_ops, Map.put(context.host_ops, :lstat, fn _path -> {:error, :eperm} end))
    marker_path = Path.join([context.host_ops.paths.evidence_root, issue_id, "generation-2", "transaction.json"])

    assert {:error, :untrusted_hgs740_path} =
             Transaction.persist_initial_marker_with_test_context(marker_path, %{"issueId" => issue_id}, runtime)

    refute_received {:marker_io, _}
  end

  test "Core permits only the exact active execution and runtime leases" do
    expected = %{
      "issueId" => "24e34a86-b214-41bc-8a35-9e1d31bfb8e4",
      "sessionId" => "worker-session",
      "processId" => "worker-process",
      "repositoryRef" => "hypergrid.au/symphony",
      "responsibleDelegationId" => "responsible-gen2"
    }

    fence = %{
      executions: %{
        expected["issueId"] => %{
          generation: 2,
          status: :active,
          ownership: :reconciled,
          cleanup: :pending,
          terminal: nil,
          cleanup_receipt: nil,
          retirement: nil,
          termination_unconfirmed: false,
          leases: %{
            expected["sessionId"] => %{
              process_id: expected["processId"],
              status: :active,
              termination_required: false
            }
          }
        }
      }
    }

    graph = %{
      delegations: %{
        expected["responsibleDelegationId"] => %{
          status: :active,
          runtime_lease: %{
            issue_id: expected["issueId"],
            generation: 2,
            session_id: expected["sessionId"],
            process_id: expected["processId"],
            repository: expected["repositoryRef"]
          }
        }
      }
    }

    assert :ok = Transaction.exact_active_fence(fence, expected["issueId"], expected)
    assert :ok = Transaction.exact_active_runtime_lease(graph, expected)

    mismatched_generation = put_in(fence, [:executions, expected["issueId"], :generation], 3)

    refute Transaction.exact_active_fence(mismatched_generation, expected["issueId"], expected) == :ok

    termination_required =
      put_in(fence, [:executions, expected["issueId"], :leases, expected["sessionId"], :termination_required], true)

    refute Transaction.exact_active_fence(termination_required, expected["issueId"], expected) == :ok

    wrong_process =
      put_in(graph, [:delegations, expected["responsibleDelegationId"], :runtime_lease, :process_id], "another-process")

    refute Transaction.exact_active_runtime_lease(wrong_process, expected) == :ok
  end

  test "Core accepts only a complete no-worker observation and denies every missing proof bit" do
    observation = %{
      "globalPause" => true,
      "runnerStopped" => true,
      "neverSpawned" => true,
      "supervisedWorkerAbsent" => true,
      "processCount" => 0,
      "workspaceAbsent" => true,
      "turnsAbsent" => true,
      "dispatchPhase" => "confirmed"
    }

    assert :ok = Transaction.require_no_local_workers(observation)

    for {key, value} <- [
          {"globalPause", false},
          {"runnerStopped", false},
          {"neverSpawned", false},
          {"supervisedWorkerAbsent", false},
          {"processCount", 1},
          {"workspaceAbsent", false},
          {"turnsAbsent", false},
          {"dispatchPhase", "recovery_pending"}
        ] do
      assert {:error, :worker_quiescence_not_proven} = Transaction.require_no_local_workers(Map.put(observation, key, value))
    end
  end

  test "Core rejects a predecessor that is absent or fails the exact retirement shape" do
    assert {:error, :predecessor_retirement_not_persisted} =
             Transaction.exact_local_predecessor(%{}, %{}, %{})

    assert {:error, :predecessor_retirement_not_persisted} =
             Transaction.exact_local_predecessor(
               %{journal: %{state: %{reservations: %{}}}, fence: %{state: %{}}, graph: %{state: %{delegations: %{}}}},
               %{"execution" => %{}, "claim" => %{"issueId" => "issue", "generation" => 1}, "receipt" => %{}},
               %{}
             )
  end

  test "recorded state ownership metadata is exact and private" do
    valid = %{
      "claimJournal" => %{"uid" => 1001, "gid" => 1001, "mode" => 0o600},
      "fence" => %{"uid" => 1001, "gid" => 1001, "mode" => 0o600},
      "responsibilityGraph" => %{"uid" => 1001, "gid" => 1001, "mode" => 0o600},
      "directories" => %{
        "/srv/dahlia-runner-state/workspaces/pools/midgard/.symphony" => %{
          "majorDevice" => 8,
          "minorDevice" => 1,
          "inode" => 1234,
          "uid" => 1001,
          "gid" => 1001,
          "mode" => 0o700
        }
      }
    }

    assert Transaction.valid_state_ownership?(valid)
    refute Transaction.valid_state_ownership?(put_in(valid, ["fence", "mode"], 0o644))
    refute Transaction.valid_state_ownership?(Map.delete(valid, "fence"))
    refute Transaction.valid_state_ownership?(put_in(valid, ["responsibilityGraph", "uid"], 0))
    refute Transaction.valid_state_ownership?(put_in(valid, ["directories", "/srv/dahlia-runner-state/workspaces/pools/midgard/.symphony", "inode"], -1))
  end

  test "directory custody replay accepts only the recorded inode and exact transition states" do
    original = %{
      "majorDevice" => 8,
      "minorDevice" => 1,
      "inode" => 1234,
      "uid" => 1001,
      "gid" => 1001,
      "mode" => 0o700
    }

    frozen = %{original | "uid" => 0, "gid" => 0}

    assert Transaction.directory_transition_allowed?(original, original, :freeze)
    assert Transaction.directory_transition_allowed?(frozen, original, :freeze)
    assert Transaction.directory_transition_allowed?(frozen, original, :restore)
    refute Transaction.directory_transition_allowed?(%{original | "inode" => 9999}, original, :freeze)
    refute Transaction.directory_transition_allowed?(%{original | "mode" => 0o755}, original, :freeze)
    refute Transaction.directory_transition_allowed?(original, original, :restore)
  end

  test "local transition receipt requires exact candidate bytes and a timezone timestamp" do
    timestamp = "2026-09-30T12:00:00Z"

    payload =
      Map.new([
        {"assignmentDigest", "a"},
        {"assignmentSHA256", "b"},
        {"completedAt", timestamp},
        {"contractVersion", "work-package-hgs740-local-transition-receipt.v1"},
        {"evidenceRef", "c"},
        {"expected", %{}},
        {"generation", 2},
        {"issueId", "issue"},
        {"nonce", "nonce"},
        {"observationSHA256", "d"},
        {"pool", "midgard"},
        {"postconditions", %{}},
        {"postimages", %{}},
        {"preimages", %{}},
        {"proofSHA256", "e"},
        {"reservationId", "reservation"},
        {"transactionId", "nonce"}
      ])

    bytes = Evidence.canonical_json(payload)
    marker = %{"completedAt" => timestamp}

    assert Transaction.local_receipt_bytes_valid?(bytes, bytes, marker)
    refute Transaction.local_receipt_bytes_valid?(bytes, bytes <> " ", marker)

    invalid_timestamp =
      payload
      |> Map.put("completedAt", "2026-09-30T12:00:00")
      |> Evidence.canonical_json()

    refute Transaction.local_receipt_bytes_valid?(invalid_timestamp, invalid_timestamp, %{"completedAt" => "2026-09-30T12:00:00"})

    extra_field =
      payload
      |> Map.put("unexpected", "value")
      |> Evidence.canonical_json()

    refute Transaction.local_receipt_bytes_valid?(extra_field, extra_field, marker)
  end

  test "local transition receipt canonical bytes match the cross-language fixture" do
    fixture =
      ~S({"assignmentDigest":"015dc6bf0c98bab65f34341b2decb7600e7bec094adcd7a392576334eb5572de","assignmentSHA256":"7777777777777777777777777777777777777777777777777777777777777777","completedAt":"2026-09-30T12:00:00Z","contractVersion":"work-package-hgs740-local-transition-receipt.v1","evidenceRef":"sha256:dbe4d79e5afbf0dc82f3b75439c9becd8fa3668de36fa42eb50242977455b576","expected":{"companyId":"company","executionFenceToken":"11111111-2222-3333-4444-555555555555:2","generation":2,"issueId":"11111111-2222-3333-4444-555555555555","managedProjectProfileId":"profile","nonceHash":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","processId":"process","projectionId":"projection","repositoryRef":"hypergridau/symphony","reservationId":"reservation","responsibleDelegationId":"delegation","runnerId":"runner","runtimeLeaseId":"session","scopeKeys":["repo:hypergridau/symphony"],"sessionId":"session","workspaceId":"workspace"},"generation":2,"issueId":"11111111-2222-3333-4444-555555555555","nonce":"11111111-2222-3333-4444-555555555555","observationSHA256":"dbe4d79e5afbf0dc82f3b75439c9becd8fa3668de36fa42eb50242977455b576","pool":"midgard","postconditions":{"dispatchPhase":"recovery_pending","executionLeaseStatus":"released","executionReleaseReason":"spawn_failed","responsibilityRuntimeLeaseStatus":"released"},"postimages":{"claimJournalSHA256":"6666666666666666666666666666666666666666666666666666666666666666","fenceSHA256":"5555555555555555555555555555555555555555555555555555555555555555","responsibilityGraphSHA256":"4444444444444444444444444444444444444444444444444444444444444444"},"preimages":{"claimJournalSHA256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","fenceSHA256":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee","responsibilityGraphSHA256":"4444444444444444444444444444444444444444444444444444444444444444"},"proofSHA256":"a4ec297781c69857e4def5fe9fd17cecd0707d74f335ab988874ca0092123710","reservationId":"reservation","transactionId":"11111111-2222-3333-4444-555555555555"})

    assert {:ok, payload} = Jason.decode(fixture)

    payload =
      payload
      |> put_in(["postimages", "claimJournalSHA256"], String.duplicate("6", 64))
      |> put_in(["postimages", "fenceSHA256"], String.duplicate("5", 64))
      |> put_in(["postimages", "responsibilityGraphSHA256"], String.duplicate("4", 64))
      |> put_in(["preimages", "claimJournalSHA256"], String.duplicate("f", 64))
      |> put_in(["preimages", "fenceSHA256"], String.duplicate("e", 64))
      |> put_in(["preimages", "responsibilityGraphSHA256"], String.duplicate("4", 64))

    fixture = Evidence.canonical_json(payload)

    assert byte_size(fixture) == 2_050
    assert sha256(fixture) == "39d8ac6225eb4ff8e8df3f262f6c11f64b120fb4489fdd634cae1b3c97ee53a6"
  end

  test "final provider receipt is bound to the exact retained HGS719 operation proof" do
    proof = ~S({"recoveryId":"hgs719-midgard-digest","projectionId":"projection","fenceRevision":"17","oldTupleDigest":"tuple","runnerId":"runner"})

    receipt = %{
      "recoveryId" => "hgs719-midgard-digest",
      "projectionId" => "projection",
      "fenceRevision" => "17",
      "oldTupleDigest" => "tuple",
      "oldNonceHash" => String.duplicate("a", 64),
      "nextGenerationFloor" => 3,
      "confirmedAt" => "2026-09-30T12:00:00Z",
      "proofDigest" => sha256(proof),
      "projectionState" => "queued",
      "reservationState" => "released",
      "executionCapacityState" => "released",
      "scopeState" => "released"
    }

    expected = %{"projectionId" => "projection"}

    assert :ok =
             Transaction.validate_hgs719_receipt_binding(
               receipt,
               receipt,
               "hgs719-midgard-digest",
               expected,
               "tuple",
               "17",
               proof
             )

    refute :ok ==
             Transaction.validate_hgs719_receipt_binding(
               Map.put(receipt, "recoveryId", "hgs719-midgard-other"),
               receipt,
               "hgs719-midgard-digest",
               expected,
               "tuple",
               "17",
               proof
             )

    refute :ok ==
             Transaction.validate_hgs719_receipt_binding(
               Map.put(receipt, "fenceRevision", "18"),
               receipt,
               "hgs719-midgard-digest",
               expected,
               "tuple",
               "17",
               proof
             )

    refute :ok ==
             Transaction.validate_hgs719_receipt_binding(
               Map.put(receipt, "proofDigest", sha256("different proof")),
               receipt,
               "hgs719-midgard-digest",
               expected,
               "tuple",
               "17",
               proof
             )
  end

  defp core_context(overrides) do
    pool = "midgard"
    {:ok, runtime} = ConfirmedRecoveryRootHost.fixed_runtime_paths(pool)

    host_ops = Map.merge(ConfirmedRecoveryRootHost.operations(), overrides)

    %ConfirmedRecoveryContext{
      issue_id: "24e34a86-b214-41bc-8a35-9e1d31bfb8e4",
      pool: pool,
      nonce: "test-nonce",
      workflow_path: "/srv/dahlia-runner-state/dahlia/config/symphony/workflows/midgard.md",
      runtime: runtime,
      host_ops: host_ops
    }
  end

  defp positive_apply_fixture do
    issue_id = "24e34a86-b214-41bc-8a35-9e1d31bfb8e4"
    nonce = "11111111-2222-4333-8444-555555555501"
    now_ms = 1_790_762_400_000

    {expected, _old_claim, journal_bytes, fence_bytes, graph_bytes, predecessor} =
      confirmed_preimages(issue_id, now_ms)

    expected_hashes = %{
      "fenceSHA256" => sha256(fence_bytes),
      "claimJournalSHA256" => sha256(journal_bytes),
      "responsibilityGraphSHA256" => sha256(graph_bytes)
    }

    observation =
      Map.merge(
        %{
          "expected" => expected,
          "predecessorRetirement" => predecessor,
          "globalPause" => true,
          "runnerStopped" => true,
          "neverSpawned" => true,
          "supervisedWorkerAbsent" => true,
          "processCount" => 0,
          "workspaceAbsent" => true,
          "turnsAbsent" => true,
          "dispatchPhase" => "confirmed"
        },
        expected_hashes
      )

    payload = %{
      "pool" => "midgard",
      "issueId" => issue_id,
      "generation" => 2,
      "reservationId" => expected["reservationId"],
      "assignmentSHA256" => String.duplicate("a", 64),
      "nonce" => nonce,
      "observation" => observation
    }

    payload_bytes = Evidence.canonical_json(payload)
    {public_key, private_key} = :crypto.generate_key(:eddsa, :ed25519)
    signature = :crypto.sign(:eddsa, :none, Evidence.signature_message(payload_bytes), [private_key, :ed25519])

    proof_bytes =
      Evidence.canonical_json(%{
        "payload" => Base.url_encode64(payload_bytes, padding: false),
        "signature" => Base.url_encode64(signature, padding: false)
      })

    observation_bytes = Evidence.canonical_json(observation)
    {:ok, runtime} = ConfirmedRecoveryRootHost.fixed_runtime_paths("midgard")
    root_operations = ConfirmedRecoveryRootHost.operations()
    paths = [runtime.journal_path, runtime.execution_fence_path, runtime.responsibility_graph_path]

    files = %{
      runtime.journal_path => journal_bytes,
      runtime.execution_fence_path => fence_bytes,
      runtime.responsibility_graph_path => graph_bytes
    }

    evidence_root = root_operations.paths.evidence_root
    evidence_directory = Path.join([evidence_root, issue_id, "generation-2"])
    candidate_path = Path.join(evidence_directory, "candidate.json")
    proof_path = Path.join(evidence_directory, "confirmed-root-envelope.json")
    marker_path = Path.join(evidence_directory, "transaction.json")

    files =
      files
      |> Map.put(candidate_path, observation_bytes)
      |> Map.put(proof_path, proof_bytes)

    {:ok, journal_state} = Journal.decode_bytes(journal_bytes)
    {:ok, fence_state} = FencePersistence.decode_bytes(fence_bytes)
    {:ok, graph_state} = GraphPersistence.decode_bytes(graph_bytes)

    file_meta = Map.new(paths, &{&1, %{uid: 1001, gid: 1001, mode: 0o600}})

    {:ok, vfs} =
      Agent.start_link(fn ->
        %{files: files, file_meta: file_meta, dir_meta: %{}, mutation_gate_calls: 0, state_write_calls: 0}
      end)

    local_candidate_path = Path.join(evidence_directory, "local-transition-candidate.json")

    verify_signed_evidence = fn bytes, bindings ->
      with {:ok, envelope} when is_map(envelope) <- Jason.decode(bytes),
           {:ok, signed_payload} <- Base.url_decode64(envelope["payload"], padding: false),
           {:ok, signature} <- Base.url_decode64(envelope["signature"], padding: false),
           true <- :crypto.verify(:eddsa, :none, Evidence.signature_message(signed_payload), signature, [public_key, :ed25519]),
           true <- signed_payload == payload_bytes,
           true <- bindings.pool == payload["pool"] and bindings.issue_id == payload["issueId"],
           true <- bindings.generation == payload["generation"] and bindings.nonce == payload["nonce"],
           true <- bindings.reservation_id == payload["reservationId"],
           true <- bindings.assignment_sha256 == payload["assignmentSHA256"],
           true <- bindings.fence_sha256 == expected_hashes["fenceSHA256"],
           true <- bindings.claim_journal_sha256 == expected_hashes["claimJournalSHA256"],
           true <- bindings.responsibility_graph_sha256 == expected_hashes["responsibilityGraphSHA256"],
           {:ok, decoded_payload} <- Jason.decode(signed_payload) do
        {:ok, decoded_payload}
      else
        _ -> {:error, :invalid_confirmed_recovery_evidence}
      end
    end

    host_ops =
      root_operations
      |> Map.merge(%{
        require_service_stopped: fn _pool -> :ok end,
        require_services_quiescent: fn -> :ok end,
        require_paused_gate: fn -> :ok end,
        require_mutation_quiescent: fn _runtime, _uid ->
          Agent.update(vfs, &update_in(&1.mutation_gate_calls, fn calls -> calls + 1 end))
          :ok
        end,
        fixed_runtime_paths: fn pool -> ConfirmedRecoveryRootHost.fixed_runtime_paths(pool) end,
        lstat: fn path -> vfs_lstat(vfs, path, paths, evidence_root, issue_id) end,
        lstat_posix: fn path -> vfs_lstat_posix(vfs, path, paths, evidence_root, issue_id) end,
        open: fn path, _modes -> if Map.has_key?(Agent.get(vfs, & &1.files), path), do: {:ok, path}, else: {:error, :enoent} end,
        raw_read: fn path, _limit -> Map.fetch(Agent.get(vfs, & &1.files), path) end,
        read_file_info: fn path, _options ->
          case vfs_lstat_posix(vfs, path, paths, evidence_root, issue_id) do
            {:ok, stat} -> {:ok, File.Stat.to_record(stat)}
            error -> error
          end
        end,
        close: fn _file -> :ok end,
        read: fn path ->
          case Map.fetch(Agent.get(vfs, & &1.files), path) do
            {:ok, bytes} -> {:ok, bytes}
            :error -> {:error, :enoent}
          end
        end,
        now_ms: fn -> now_ms end,
        verify_signed_evidence: verify_signed_evidence,
        raw_open: fn path, _modes -> {:ok, {:raw_file, path}} end,
        raw_write: fn {:raw_file, path}, bytes ->
          Agent.update(vfs, &put_in(&1.files[path], bytes))
          :ok
        end,
        raw_sync: fn _file -> :ok end,
        raw_close: fn _file -> :ok end,
        change_owner: fn path, uid, gid -> vfs_change_owner(vfs, path, uid, gid) end,
        chmod: fn path, mode -> vfs_chmod(vfs, path, mode) end,
        no_processes_for_uid: fn _uid -> :ok end,
        save_state: fn kind, path, state ->
          vfs_save_state(vfs, kind, path, state)
        end,
        rename: fn source, destination ->
          Agent.get_and_update(vfs, fn state ->
            case Map.pop(state.files, source) do
              {nil, _files} ->
                {{:error, :enoent}, state}

              {bytes, files} ->
                meta = Map.get(state.file_meta, source, %{uid: 0, gid: 0, mode: 0o600})
                next = %{state | files: Map.put(files, destination, bytes), file_meta: state.file_meta |> Map.delete(source) |> Map.put(destination, meta)}
                {:ok, next}
            end
          end)
        end,
        remove: fn path ->
          Agent.update(vfs, fn state -> %{state | files: Map.delete(state.files, path), file_meta: Map.delete(state.file_meta, path)} end)
          :ok
        end
      })

    context = %ConfirmedRecoveryContext{
      issue_id: issue_id,
      pool: "midgard",
      nonce: nonce,
      workflow_path: "/srv/dahlia-runner-state/dahlia/config/symphony/workflows/midgard.md",
      runtime: runtime,
      host_ops: host_ops
    }

    %{
      context: context,
      vfs: vfs,
      state_paths: %{journal: runtime.journal_path, fence: runtime.execution_fence_path, graph: runtime.responsibility_graph_path},
      local_candidate_path: local_candidate_path,
      expected: expected,
      predecessor: predecessor,
      predecessor_paths: %{
        journal: %{state: journal_state},
        fence: %{state: fence_state},
        graph: %{state: graph_state}
      },
      marker_path: marker_path,
      proof_bytes: proof_bytes,
      bindings: %{
        pool: "midgard",
        issue_id: issue_id,
        generation: 2,
        reservation_id: expected["reservationId"],
        assignment_sha256: payload["assignmentSHA256"],
        nonce: nonce,
        fence_sha256: expected_hashes["fenceSHA256"],
        claim_journal_sha256: expected_hashes["claimJournalSHA256"],
        responsibility_graph_sha256: expected_hashes["responsibilityGraphSHA256"],
        now_ms: now_ms
      },
      journal_bytes: journal_bytes,
      fence_bytes: fence_bytes,
      graph_bytes: graph_bytes,
      reservation_id: expected["reservationId"]
    }
  end

  defp confirmed_preimages(issue_id, now_ms) do
    nonce1 = "generation-one-private-nonce"
    nonce2 = "generation-two-private-nonce"

    old_claim = %{
      "projectionId" => "projection-gen1",
      "reservationId" => "reservation-gen1",
      "workspaceId" => "workspace-1",
      "companyId" => "company-1",
      "issueId" => issue_id,
      "runnerId" => "runner-1",
      "managedProjectProfileId" => "profile-1",
      "repositoryRef" => "hypergrid.au/symphony",
      "scopeKeys" => ["issue:" <> issue_id, "repo:symphony"],
      "generation" => 1,
      "sessionId" => "worker:" <> issue_id <> ":1",
      "processId" => "worker:" <> issue_id <> ":1",
      "responsibleDelegationId" => "responsible-gen1",
      "executionFenceToken" => issue_id <> ":1",
      "runtimeLeaseId" => "worker:" <> issue_id <> ":1",
      "nonceHash" => sha256(nonce1)
    }

    expected = %{
      "projectionId" => "projection-gen2",
      "reservationId" => "reservation-gen2",
      "workspaceId" => "workspace-1",
      "companyId" => "company-1",
      "issueId" => issue_id,
      "runnerId" => "runner-1",
      "managedProjectProfileId" => "profile-1",
      "repositoryRef" => "hypergrid.au/symphony",
      "scopeKeys" => ["issue:" <> issue_id, "repo:symphony"],
      "generation" => 2,
      "sessionId" => "worker:" <> issue_id <> ":2",
      "processId" => "worker:" <> issue_id <> ":2",
      "responsibleDelegationId" => "responsible-gen2",
      "executionFenceToken" => issue_id <> ":2",
      "runtimeLeaseId" => "worker:" <> issue_id <> ":2",
      "nonceHash" => sha256(nonce2)
    }

    journal =
      Journal.new()
      |> put_reservation(issue_id, old_claim, nonce1, "confirmed")
      |> put_reservation(issue_id, expected, nonce2, "confirmed")

    {:ok, journal_bytes} = Journal.encode_bytes(journal)
    {fence, _old_execution} = confirmed_fence(issue_id, now_ms)
    {:ok, fence_bytes} = FencePersistence.encode_bytes(fence)
    fence_document = Jason.decode!(fence_bytes)
    old_execution_json = Enum.find(fence_document["history"], &(&1["generation"] == 1))
    {:ok, graph_bytes} = confirmed_graph(issue_id, expected, now_ms)

    predecessor = %{
      "execution" => old_execution_json,
      "claim" => old_claim,
      "receipt" => old_execution_json["retirement"]
    }

    {expected, old_claim, journal_bytes, fence_bytes, graph_bytes, predecessor}
  end

  defp put_reservation(journal, issue_id, claim, nonce, phase) do
    reservation = %{
      issue_id: issue_id,
      managed_project_profile_id: claim["managedProjectProfileId"],
      repository_ref: claim["repositoryRef"],
      projection_id: claim["projectionId"],
      reservation_id: claim["reservationId"],
      reservation_nonce: nonce,
      scope_keys: claim["scopeKeys"],
      runner_id: claim["runnerId"],
      workspace_id: claim["workspaceId"],
      company_id: claim["companyId"],
      generation: claim["generation"],
      session_id: claim["sessionId"],
      process_id: claim["processId"],
      responsible_delegation_id: claim["responsibleDelegationId"],
      execution_fence_token: claim["executionFenceToken"],
      runtime_lease_id: claim["runtimeLeaseId"],
      dispatch: %{
        phase: phase,
        attempts: 1,
        retry_at_ms: 0,
        authority_digest: String.duplicate("a", 64),
        allocation_id: nil
      }
    }

    key = Journal.reservation_key(issue_id, claim["managedProjectProfileId"], claim["repositoryRef"], claim["generation"])
    {:ok, next} = Journal.put(journal, key, reservation)
    next
  end

  defp confirmed_fence(issue_id, now_ms) do
    repo = "hypergrid.au/symphony"
    gen1_session = "worker:" <> issue_id <> ":1"

    {:ok, state, token1} =
      ExecutionFence.admit(ExecutionFence.new(), %{issue_id: issue_id, repository: repo, branch: "codex/hgs736", worktree: "C:/absent"}, 100)

    {:ok, state, :registered} =
      ExecutionFence.register(
        state,
        token1,
        :worker,
        %{
          issue_id: issue_id,
          repository: repo,
          session_id: gen1_session,
          process_id: gen1_session,
          branch: "codex/hgs736",
          worktree: "C:/absent",
          linear_state: "In Progress",
          pr_state: "OPEN",
          head: "unobserved",
          last_heartbeat_at: 0
        },
        100
      )

    {:ok, state} = ExecutionFence.release_unsubmitted_claim(state, token1, gen1_session)
    receipt = predecessor_receipt(issue_id, now_ms)

    evidence =
      receipt
      |> Map.new(fn {key, value} -> {String.to_atom(key), value} end)
      |> Map.delete(:retired_at_ms)
      |> Map.update!(:active_process, &String.to_atom/1)
      |> Map.update!(:local_claim, &String.to_atom/1)
      |> Map.update!(:provider_claim, &String.to_atom/1)
      |> Map.update!(:workspace, &String.to_atom/1)

    {:ok, state, :retired} = ExecutionFence.retire_unsubmitted(state, token1, evidence, receipt["retired_at_ms"])

    {:ok, state, token2} =
      ExecutionFence.admit(state, %{issue_id: issue_id, repository: repo, branch: "codex/hgs740", worktree: "C:/worktrees/hgs740"}, 101)

    gen2_session = "worker:" <> issue_id <> ":2"

    {:ok, state, :registered} =
      ExecutionFence.register(
        state,
        token2,
        :worker,
        %{
          issue_id: issue_id,
          repository: repo,
          session_id: gen2_session,
          process_id: gen2_session,
          branch: "codex/hgs740",
          worktree: "C:/worktrees/hgs740",
          linear_state: "In Progress",
          pr_state: "OPEN",
          head: "unobserved",
          last_heartbeat_at: 0
        },
        101
      )

    state = put_in(state, [:executions, issue_id, :retirement], nil)

    {:ok, old_json} =
      FencePersistence.encode_bytes(state)
      |> then(fn {:ok, bytes} -> {:ok, Jason.decode!(bytes)["history"] |> Enum.find(&(&1["generation"] == 1))} end)

    {state, old_json}
  end

  defp predecessor_receipt(issue_id, now_ms) do
    receipt = %{
      "active_process" => "absent",
      "evidence_ref" => "pending",
      "generation" => 1,
      "issue_id" => issue_id,
      "linear_state" => "In Progress",
      "local_claim" => "absent",
      "provider_claim" => "absent",
      "provider_projection_id" => "projection-gen1",
      "retired_at_ms" => now_ms - 100_000,
      "workspace" => "absent",
      "type" => "unsubmitted_successor",
      "repository_ref" => "hypergrid.au/symphony",
      "managed_project_profile_id" => "profile-1",
      "prior_accountable_id" => "accountable-gen1",
      "prior_responsible_id" => "responsible-gen1",
      "prior_accountable_digest" => String.duplicate("5", 64),
      "prior_responsible_digest" => String.duplicate("6", 64),
      "successor_accountable_id" => "accountable-gen2",
      "successor_responsible_id" => "responsible-gen2",
      "successor_accountable_digest" => String.duplicate("7", 64),
      "successor_responsible_digest" => String.duplicate("8", 64),
      "manifest_sha256" => String.duplicate("9", 64),
      "signer_key_sha256" => String.duplicate("a", 64),
      "observation_sha256" => String.duplicate("b", 64)
    }

    Map.put(receipt, "evidence_ref", Evidence.retirement_evidence_ref(receipt))
  end

  defp confirmed_graph(issue_id, expected, now_ms) do
    scope = %{
      company_id: "company-1",
      objective_id: "objective",
      initiative_id: "initiative",
      project_id: "symphony",
      work_package_id: "hgs740",
      issue_id: issue_id,
      repository: "hypergrid.au/symphony",
      paths: [],
      modules: [],
      environments: ["local"],
      actions: [:read, :observe, :delegate, :reconcile, :edit, :commit, :push, :state_mutation, :cleanup, :review, :report]
    }

    graph = ResponsibilityGraph.new()
    {:ok, graph, _} = ResponsibilityGraph.delegate(graph, graph_delegation("accountable-gen1", nil, :accountable, scope), 100)
    {:ok, graph, _} = ResponsibilityGraph.delegate(graph, graph_delegation("responsible-gen1", "accountable-gen1", :responsible, scope), 101)
    {:ok, graph, _} = ResponsibilityGraph.revoke(graph, "accountable-gen1", :superseded, 102)
    {:ok, graph, _} = ResponsibilityGraph.delegate(graph, graph_delegation("accountable-gen2", nil, :accountable, scope), 103)
    {:ok, graph, _} = ResponsibilityGraph.delegate(graph, graph_delegation("responsible-gen2", "accountable-gen2", :responsible, scope), 104)

    lease = %{
      issue_id: issue_id,
      repository: expected["repositoryRef"],
      generation: 2,
      session_id: expected["sessionId"],
      process_id: expected["processId"]
    }

    {:ok, graph} = ResponsibilityGraph.bind_runtime_lease(graph, expected["responsibleDelegationId"], lease, now_ms - 1)
    GraphPersistence.encode_bytes(graph)
  end

  defp graph_delegation(id, parent_id, role, scope) do
    actions = [:read, :observe, :delegate, :reconcile, :edit, :commit, :push, :state_mutation, :cleanup, :review, :report]

    %{
      id: id,
      parent_delegation_id: parent_id,
      role: role,
      actor_id: "actor-" <> id,
      scope: scope,
      authority: %{class: :routine_engineering, capabilities: actions, environments: ["local"]},
      budget: %{model: "gpt-6-luna", effort: :high, max_tokens: 1000, max_children: 2},
      expires_at_ms: 2_000_000_000_000,
      expected_deliverable: "source",
      expected_evidence: "tests",
      return_to_parent: %{owner_id: id, contract: "evidence"}
    }
  end

  defp vfs_lstat(vfs, path, state_paths, evidence_root, issue_id) do
    state = Agent.get(vfs, & &1)

    case Map.fetch(state.files, path) do
      {:ok, bytes} ->
        defaults = if path in state_paths, do: %{uid: 1001, gid: 1001, mode: 0o600}, else: %{uid: 0, gid: 0, mode: 0o600}
        metadata = Map.get(state.file_meta, path, defaults)
        {:ok, vfs_file_stat(path, bytes, metadata.uid, metadata.gid, metadata.mode)}

      :error ->
        vfs_directory_stat(vfs, path, evidence_root, issue_id)
    end
  end

  defp vfs_lstat_posix(vfs, path, state_paths, evidence_root, issue_id) do
    state = Agent.get(vfs, & &1)

    case Map.fetch(state.files, path) do
      {:ok, bytes} ->
        defaults = if path in state_paths, do: %{uid: 1001, gid: 1001, mode: 0o600}, else: %{uid: 0, gid: 0, mode: 0o600}
        metadata = Map.get(state.file_meta, path, defaults)
        {:ok, vfs_file_stat(path, bytes, metadata.uid, metadata.gid, metadata.mode)}

      :error ->
        vfs_directory_stat(vfs, path, evidence_root, issue_id)
    end
  end

  defp vfs_directory_stat(vfs, path, evidence_root, issue_id) do
    evidence_directory = Path.join([evidence_root, issue_id, "generation-2"])
    state_root = "/srv/dahlia-runner-state"

    defaults =
      cond do
        path in ["/", "/srv", state_root, Path.join(state_root, "evidence")] -> %{uid: 0, gid: 0, mode: 0o755}
        path in [evidence_root, Path.dirname(evidence_directory), evidence_directory] -> %{uid: 0, gid: 0, mode: 0o700}
        String.starts_with?(path, Path.join(state_root, "run") <> "/") or path == Path.join(state_root, "run") -> %{uid: 1001, gid: 1001, mode: 0o750}
        String.starts_with?(path, Path.join(state_root, "workspaces") <> "/") or path == Path.join(state_root, "workspaces") -> %{uid: 1001, gid: 1001, mode: 0o750}
        true -> nil
      end

    case defaults do
      nil ->
        {:error, :enoent}

      metadata ->
        actual = Map.get(Agent.get(vfs, & &1.dir_meta), path, metadata)
        {:ok, vfs_dir_stat(path, actual.uid, actual.gid, actual.mode)}
    end
  end

  defp vfs_change_owner(vfs, path, uid, gid) do
    Agent.update(vfs, fn state ->
      if Map.has_key?(state.files, path) do
        defaults = %{uid: 0, gid: 0, mode: 0o600}
        meta = Map.get(state.file_meta, path, defaults) |> Map.merge(%{uid: uid, gid: gid})
        %{state | file_meta: Map.put(state.file_meta, path, meta)}
      else
        defaults = %{uid: 1001, gid: 1001, mode: 0o750}
        meta = Map.get(state.dir_meta, path, defaults) |> Map.merge(%{uid: uid, gid: gid})
        %{state | dir_meta: Map.put(state.dir_meta, path, meta)}
      end
    end)

    :ok
  end

  defp vfs_chmod(vfs, path, mode) do
    Agent.update(vfs, fn state ->
      if Map.has_key?(state.files, path) do
        meta = Map.get(state.file_meta, path, %{uid: 0, gid: 0, mode: 0o600}) |> Map.put(:mode, mode)
        %{state | file_meta: Map.put(state.file_meta, path, meta)}
      else
        meta = Map.get(state.dir_meta, path, %{uid: 1001, gid: 1001, mode: 0o750}) |> Map.put(:mode, mode)
        %{state | dir_meta: Map.put(state.dir_meta, path, meta)}
      end
    end)

    :ok
  end

  defp vfs_save_state(vfs, kind, path, state) do
    encoded =
      case kind do
        :claim_journal -> Journal.encode_bytes(state)
        :fence -> FencePersistence.encode_bytes(state)
        :responsibility_graph -> GraphPersistence.encode_bytes(state)
      end

    case encoded do
      {:ok, bytes} ->
        Agent.update(vfs, fn current ->
          %{current | files: Map.put(current.files, path, bytes), state_write_calls: current.state_write_calls + 1}
        end)

        :ok

      error ->
        error
    end
  end

  defp vfs_file_stat(path, bytes, uid, gid, mode) do
    %File.Stat{
      type: :regular,
      size: byte_size(bytes),
      access: :read_write,
      atime: 1_790_762_400,
      mtime: 1_790_762_400,
      ctime: 1_790_762_400,
      mode: mode,
      links: 1,
      major_device: 8,
      minor_device: 1,
      inode: :erlang.phash2(path) + 1,
      uid: uid,
      gid: gid
    }
  end

  defp vfs_dir_stat(path, uid, gid, mode) do
    %File.Stat{
      type: :directory,
      size: 0,
      access: :read_write,
      atime: 1_790_762_400,
      mtime: 1_790_762_400,
      ctime: 1_790_762_400,
      mode: mode,
      links: 1,
      major_device: 8,
      minor_device: 1,
      inode: :erlang.phash2(path) + 1,
      uid: uid,
      gid: gid
    }
  end

  defp core_proof_fixture(payload) do
    issue_id = payload["observation"]["expected"]["issueId"]
    observation_bytes = Evidence.canonical_json(payload["observation"])
    payload_bytes = Evidence.canonical_json(payload)

    proof_bytes =
      Evidence.canonical_json(%{
        "payload" => Base.url_encode64(payload_bytes, padding: false),
        "signature" => Base.url_encode64("synthetic-signature", padding: false)
      })

    {:ok, runtime} = ConfirmedRecoveryRootHost.fixed_runtime_paths("midgard")
    evidence_root = "/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery"
    directory = Path.join([evidence_root, issue_id, "generation-2"])
    observation_path = Path.join(directory, "candidate.json")
    proof_path = Path.join(directory, "confirmed-root-envelope.json")
    files = %{observation_path => observation_bytes, proof_path => proof_bytes}
    parent = self()

    host_ops =
      ConfirmedRecoveryRootHost.operations()
      |> Map.merge(%{
        lstat: fn path ->
          cond do
            Map.has_key?(files, path) ->
              {:ok, %File.Stat{type: :regular, uid: 0, gid: 0, mode: 0o600, links: 1, size: byte_size(files[path])}}

            path == evidence_root or String.starts_with?(path, evidence_root <> "/") ->
              {:ok, %File.Stat{type: :directory, uid: 0, gid: 0, mode: 0o700}}

            String.starts_with?(evidence_root, path <> "/") or path == "/" ->
              {:ok, %File.Stat{type: :directory, uid: 0, gid: 0, mode: 0o755}}

            true ->
              send(parent, {:unexpected_lstat, path})
              {:error, :enoent}
          end
        end,
        read: fn path ->
          Map.fetch(files, path)
          |> case do
            {:ok, bytes} -> {:ok, bytes}
            :error -> {:error, :enoent}
          end
        end,
        now_ms: fn -> 1_790_762_400_000 end,
        verify_signed_evidence: fn _bytes, _bindings -> {:error, :synthetic_verifier_not_installed} end
      })

    runtime = Map.put(runtime, :host_ops, host_ops)

    %{
      observation_bytes: observation_bytes,
      proof_bytes: proof_bytes,
      paths: %{
        runtime: runtime,
        journal: %{bytes: "journal-before"},
        fence: %{bytes: "fence-before"},
        graph: %{bytes: "graph-before"}
      }
    }
  end

  defp sha256(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)

  defp run_replay(state, preimage_hashes, postimages, should_write?) do
    images =
      Enum.map(postimages, fn {name, postimage} ->
        %{name: name, preimage_sha256: preimage_hashes[name], postimage_bytes: postimage}
      end)

    ConfirmedRecoveryWAL.apply_images(
      images,
      fn name ->
        {current_images, _writes} = Agent.get(state, & &1)
        Map.get(current_images, name)
      end,
      fn name, bytes, already_applied? ->
        persist_replay_image(state, name, bytes, already_applied?, should_write?)
      end
    )
  end

  defp persist_replay_image(_state, _name, _bytes, true, _should_write?), do: :ok

  defp persist_replay_image(state, name, bytes, false, should_write?) do
    {_images, writes} = Agent.get(state, & &1)

    if should_write?.(writes) do
      Agent.update(state, fn {images, count} -> {Map.put(images, name, bytes), count + 1} end)
      :ok
    else
      {:error, :synthetic_crash}
    end
  end
end
