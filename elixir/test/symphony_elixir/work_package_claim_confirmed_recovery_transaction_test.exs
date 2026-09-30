defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransactionTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryContext
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryCore, as: Transaction
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryLineage
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryRootHost
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryTransaction, as: Facade
  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryWAL

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

  test "Core rejects an unverified context before asking for canonical runtime paths" do
    parent = self()

    context =
      core_context(
        %{
          fixed_runtime_paths: fn _pool ->
            send(parent, :runtime_paths_must_not_be_read)
            {:error, :should_not_run}
          end
        },
        verified?: false
      )

    assert {:error, :invalid_verified_recovery_context} = Transaction.verify_startup(context)
    refute_received :runtime_paths_must_not_be_read
  end

  test "Core startup admits an untouched evidence root and short-circuits an unsafe root" do
    {:ok, runtime} = ConfirmedRecoveryRootHost.fixed_runtime_paths("midgard")
    evidence_root = "/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery"

    untouched =
      core_context(%{
        lstat: fn ^evidence_root -> {:error, :enoent} end
      })

    assert :ok = Transaction.validate_context(untouched)
    assert :ok = Transaction.verify_startup(untouched)

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

    assert {:error, :hgs740_startup_held_closed} = Transaction.verify_startup(writable_root)
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

    assert :ok = Transaction.validate_context(context)

    assert {:error, :pool_service_not_proven_stopped} =
             Transaction.apply(context)

    assert_received {:service_gate, "midgard"}
    refute_received :later_gate_must_not_run
    refute_received :evidence_must_not_be_read
    refute_received :signature_must_not_be_verified
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

    assert :ok = Transaction.validate_context(context)
    assert {:error, :hgs740_completion_held_closed} = Transaction.complete(context)
    assert_received :service_gate
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

    assert :ok = Transaction.persist_initial_marker(marker_path, marker, runtime)
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
             Transaction.persist_initial_marker(marker_path, %{"issueId" => issue_id}, runtime)

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

  defp core_context(overrides, opts \\ []) do
    pool = "midgard"
    {:ok, runtime} = ConfirmedRecoveryRootHost.fixed_runtime_paths(pool)

    host_ops = Map.merge(ConfirmedRecoveryRootHost.operations(), overrides)

    %ConfirmedRecoveryContext{
      issue_id: "24e34a86-b214-41bc-8a35-9e1d31bfb8e4",
      pool: pool,
      nonce: "test-nonce",
      workflow_path: "/srv/dahlia-runner-state/dahlia/config/symphony/workflows/midgard.md",
      runtime: runtime,
      host_ops: host_ops,
      verified?: Keyword.get(opts, :verified?, true)
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
