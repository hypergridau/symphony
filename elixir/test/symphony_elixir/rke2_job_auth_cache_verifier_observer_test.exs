defmodule SymphonyElixir.RKE2JobAuthCacheVerifierObserverTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.RKE2Job.{AuthCacheVerifierJobSpec, AuthCacheVerifierObserver, DahliaAuthSlotLeaseGuard}

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
  @allocation %{
    id:
      "rke2job:v1:" <>
        Base.url_encode64(Jason.encode!([1, "frigga", "assignment-job", "original-job-uid", @digest]), padding: false),
    status: :ready
  }

  defmodule FakeClient do
    @moduledoc false

    def create_job(_namespace, expected, %{agent: agent}) do
      Agent.get_and_update(agent, fn state ->
        name = expected["metadata"]["name"]

        job =
          expected
          |> put_in(["metadata", "uid"], "verifier-job-uid")
          |> put_in(["metadata", "resourceVersion"], "job-rv-1")
          |> Map.put("status", %{"conditions" => [%{"type" => "Complete", "status" => "True"}]})

        pod = verifier_pod(expected)
        next = %{state | jobs: Map.put(state.jobs, name, job), pods: [pod | state.pods], creates: state.creates + 1}

        case state.create_outcome do
          :ok -> {{:ok, job}, next}
          :timeout -> {{:error, :timeout}, next}
          :lost -> {{:error, :timeout}, %{next | jobs: Map.delete(next.jobs, name), pods: state.pods}}
        end
      end)
    end

    def get_job(_namespace, name, %{agent: agent}) do
      Agent.get(agent, fn state ->
        case Map.fetch(state.jobs, name) do
          {:ok, job} -> {:ok, job}
          :error -> {:error, :not_found}
        end
      end)
    end

    def delete_job(_namespace, name, uid, %{agent: agent}) do
      Agent.get_and_update(agent, &delete_state(&1, name, uid))
    end

    defp delete_state(state, name, uid) do
      case Map.get(state.jobs, name) do
        %{"metadata" => %{"uid" => ^uid}} ->
          pods = if Map.get(state, :retain_pods_on_delete), do: state.pods, else: []
          pvc_uid = Map.get(state, :after_delete_pvc_uid, state.pvc_uid)
          next = %{state | jobs: Map.delete(state.jobs, name), pods: pods, pvc_uid: pvc_uid, deletes: state.deletes + 1}
          {:ok, next}

        _ ->
          {{:error, :uid_mismatch}, state}
      end
    end

    def get_pvc(_namespace, name, %{agent: agent}) do
      Agent.get(agent, fn state ->
        {:ok,
         %{
           "apiVersion" => "v1",
           "kind" => "PersistentVolumeClaim",
           "metadata" => %{"namespace" => "frigga", "name" => name, "uid" => state.pvc_uid},
           "status" => %{"phase" => "Bound"}
         }}
      end)
    end

    def list_pods_snapshot(_namespace, %{agent: agent}) do
      Agent.get(agent, fn state -> {:ok, %{items: state.pods, resource_version: "list-rv-1"}} end)
    end

    defp verifier_pod(expected) do
      name = expected["metadata"]["name"]

      %{
        "apiVersion" => "v1",
        "kind" => "Pod",
        "metadata" => %{
          "name" => name <> "-abcde",
          "namespace" => "frigga",
          "uid" => "verifier-pod-uid",
          "resourceVersion" => "pod-rv-1",
          "labels" => expected["spec"]["template"]["metadata"]["labels"],
          "ownerReferences" => [
            %{"apiVersion" => "batch/v1", "kind" => "Job", "name" => name, "uid" => "verifier-job-uid", "controller" => true}
          ]
        },
        "spec" => expected["spec"]["template"]["spec"],
        "status" => %{
          "phase" => "Succeeded",
          "containerStatuses" => [
            %{
              "name" => "oauth-cache-verifier",
              "ready" => false,
              "state" => %{
                "terminated" => %{
                  "exitCode" => 0,
                  "message" =>
                    Jason.encode!(%{
                      "schemaVersion" => 1,
                      "authCacheStatus" => "codex_login_status_authenticated",
                      "authCacheBytes" => 12_345
                    })
                }
              }
            }
          ]
        }
      }
    end
  end

  setup do
    root = Path.join(System.tmp_dir!(), "symphony-verifier-observer-" <> Ecto.UUID.generate())
    File.mkdir!(root)
    File.chmod!(root, 0o700)
    on_exit(fn -> File.rm_rf!(root) end)

    {:ok, agent} =
      Agent.start_link(fn ->
        %{jobs: %{}, pods: [], pvc_uid: "pvc-uid-1", creates: 0, deletes: 0, create_outcome: :ok}
      end)

    context = %{
      client: FakeClient,
      client_context: %{agent: agent},
      config: %{image: @image, catalog: %{"oauth-slot-1" => "codex-oauth-slot-1"}, journal_root: root}
    }

    %{agent: agent, context: context, root: root}
  end

  test "runs one verifier, checkpoints before delete, and replays without a second Job",
       %{agent: agent, context: context, root: root} do
    assert {:ok, first} = AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)
    assert map_size(first) == 13
    assert first["jobUid"] == "original-job-uid"
    assert first["authCacheStatus"] == "codex_login_status_authenticated"
    assert first["authCacheBytes"] == 12_345
    assert [_] = Path.wildcard(Path.join(root, "*.auth-verifier-result.json"))
    assert %{creates: 1, deletes: 1, jobs: %{}} = Agent.get(agent, & &1)

    assert {:ok, replay} = AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)
    assert replay["receiptId"] != first["receiptId"]
    assert %{creates: 1, deletes: 1} = Agent.get(agent, & &1)
  end

  test "reconciles a timed-out create from its exact existing Job", %{agent: agent, context: context} do
    Agent.update(agent, &%{&1 | create_outcome: :timeout})

    assert {:held, :auth_cache_verifier_create_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert {:ok, _receipt} = AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)
    assert %{creates: 1, deletes: 1} = Agent.get(agent, & &1)
  end

  test "holds an absent Job after uncertain create instead of creating another", %{agent: agent, context: context} do
    Agent.update(agent, &%{&1 | create_outcome: :lost})

    assert {:held, :auth_cache_verifier_create_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert {:held, :auth_cache_verifier_create_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert %{creates: 1, deletes: 0} = Agent.get(agent, & &1)
  end

  test "holds changed PVC identity and a competing claim consumer", %{agent: agent, context: context} do
    Agent.update(agent, &%{&1 | pvc_uid: "replacement-pvc"})

    assert {:held, :auth_cache_verifier_create_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert %{creates: 0} = Agent.get(agent, & &1)

    consumer = %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{"name" => "other", "namespace" => "frigga", "uid" => "other-pod-uid", "labels" => %{}},
      "spec" => %{"volumes" => [%{"name" => "auth", "persistentVolumeClaim" => %{"claimName" => @slot.claim_name}}]}
    }

    Agent.update(agent, &%{&1 | pvc_uid: @slot.claim_uid, pods: [consumer]})

    assert {:held, :auth_cache_verifier_create_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert %{creates: 0} = Agent.get(agent, & &1)
  end

  test "holds a competing consumer while the verifier Job exists", %{agent: agent, context: context} do
    Agent.update(agent, &%{&1 | create_outcome: :timeout})

    assert {:held, :auth_cache_verifier_create_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    consumer = %{
      "apiVersion" => "v1",
      "kind" => "Pod",
      "metadata" => %{"name" => "other", "namespace" => "frigga", "uid" => "other-pod-uid", "labels" => %{}},
      "spec" => %{"volumes" => [%{"name" => "auth", "persistentVolumeClaim" => %{"claimName" => @slot.claim_name}}]}
    }

    Agent.update(agent, &%{&1 | pods: [consumer | &1.pods]})

    assert {:held, :auth_cache_verifier_result_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert %{creates: 1, deletes: 0} = Agent.get(agent, & &1)

    Agent.update(agent, &%{&1 | pods: tl(&1.pods)})
    assert {:ok, _} = AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)
  end

  test "rejects a changed same-name Job after an uncertain create", %{agent: agent, context: context} do
    Agent.update(agent, &%{&1 | create_outcome: :timeout})

    assert {:held, :auth_cache_verifier_create_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    Agent.update(agent, fn state ->
      {name, job} = Enum.at(state.jobs, 0)
      %{state | jobs: Map.put(state.jobs, name, put_in(job, ["spec", "template", "spec", "hostNetwork"], true))}
    end)

    assert {:held, :auth_cache_verifier_result_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert %{creates: 1, deletes: 0} = Agent.get(agent, & &1)
  end

  test "a saved checkpoint cannot delete a replacement verifier Job", %{agent: agent, context: context, root: root} do
    assert {:ok, _} = AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)
    [attempt_path] = Path.wildcard(Path.join(root, "*.auth-verifier-attempt.json"))
    attempt = attempt_path |> File.read!() |> Jason.decode!()

    {:ok, expected} =
      AuthCacheVerifierJobSpec.compile(@assignment, @slot, %{
        namespace: "frigga",
        image: @image,
        catalog: %{"oauth-slot-1" => "codex-oauth-slot-1"},
        attempt_id: attempt["attemptId"]
      })

    replacement =
      expected
      |> put_in(["metadata", "uid"], "replacement-job-uid")
      |> put_in(["metadata", "resourceVersion"], "replacement-rv")

    Agent.update(agent, &%{&1 | jobs: %{expected["metadata"]["name"] => replacement}})

    assert {:held, :auth_cache_verifier_job_identity_changed} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert %{creates: 1, deletes: 1} = Agent.get(agent, & &1)
  end

  test "a PVC change after verifier deletion holds release but replays the checkpoint", %{agent: agent, context: context} do
    Agent.update(agent, &Map.put(&1, :after_delete_pvc_uid, "replacement-pvc"))

    assert {:held, :auth_cache_verifier_pvc_changed} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert %{creates: 1, deletes: 1} = Agent.get(agent, & &1)

    Agent.update(agent, &%{&1 | pvc_uid: @slot.claim_uid})
    assert {:ok, receipt} = AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)
    assert receipt["claimUid"] == @slot.claim_uid
    assert %{creates: 1, deletes: 1} = Agent.get(agent, & &1)
  end

  test "a lingering verifier Pod holds the slot until fresh absence readback", %{agent: agent, context: context} do
    Agent.update(agent, &Map.put(&1, :retain_pods_on_delete, true))

    assert {:held, :auth_cache_verifier_claim_consumer_present} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert %{creates: 1, deletes: 1, pods: [_]} = Agent.get(agent, & &1)

    Agent.update(agent, &%{&1 | pods: []})
    assert {:ok, _receipt} = AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)
    assert %{creates: 1, deletes: 1} = Agent.get(agent, & &1)
  end

  test "an existing original Job prevents verifier creation", %{agent: agent, context: context} do
    Agent.update(agent, &%{&1 | jobs: %{"assignment-job" => %{"metadata" => %{"uid" => "original-job-uid"}}}})

    assert {:held, :auth_cache_verifier_create_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, context)

    assert %{creates: 0, deletes: 0} = Agent.get(agent, & &1)
  end

  test "invalid host context or allocation cannot create a verifier", %{agent: agent, context: context} do
    assert {:held, :auth_cache_verifier_observation_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, @allocation, Map.delete(context, :client_context))

    assert {:held, :auth_cache_verifier_observation_unavailable} =
             AuthCacheVerifierObserver.observe(@slot, @assignment, %{id: "wrong", status: :ready}, context)

    assert %{creates: 0, deletes: 0} = Agent.get(agent, & &1)
  end

  test "release guard invokes the host observer and replays its saved receipt", %{agent: agent, context: context, root: root} do
    post_fun = fn _url, opts ->
      send(self(), {:provider_release, Keyword.fetch!(opts, :json)})
      {:ok, %Req.Response{status: 200, body: %{"data" => %{"released" => true}}}}
    end

    guard_context = %{
      base_url: "https://provider.invalid",
      runner_token: "synthetic-host-token",
      reservation_id: "reservation-1",
      result_journal_root: root,
      auth_cache_verifier_context: context,
      post_fun: post_fun
    }

    assert :ok = DahliaAuthSlotLeaseGuard.release(@slot, @assignment, @allocation, guard_context)
    assert_receive {:provider_release, first}
    assert first.allocationId == @allocation.id
    assert first.receipt["authCacheStatus"] == "codex_login_status_authenticated"
    assert %{creates: 1, deletes: 1} = Agent.get(agent, & &1)

    assert :ok = DahliaAuthSlotLeaseGuard.release(@slot, @assignment, @allocation, guard_context)
    assert_receive {:provider_release, ^first}
    assert %{creates: 1, deletes: 1} = Agent.get(agent, & &1)
  end
end
