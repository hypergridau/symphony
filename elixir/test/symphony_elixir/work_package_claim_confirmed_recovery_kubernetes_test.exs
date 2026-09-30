defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryKubernetesTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryKubernetes

  @claim %{
    "issueId" => "11111111-2222-3333-4444-555555555555",
    "generation" => 2,
    "assignmentSHA256" => String.duplicate("a", 64)
  }

  test "absence accepts a complete list with unrelated resources" do
    unrelated = %{
      "metadata" => %{
        "namespace" => "frigga",
        "name" => "symphony-other",
        "uid" => "job-other",
        "labels" => %{
          "symphony.hypergrid.au/issue-id" => "ffffffffffffffffffffffffffffffff",
          "symphony.hypergrid.au/generation" => "2"
        }
      }
    }

    assert :ok = ConfirmedRecoveryKubernetes.complete_resources_absent([unrelated], @claim, :job)
  end

  test "any retained Job or Pod for the exact issue and generation blocks completion" do
    issue_label =
      :crypto.hash(:sha256, @claim["issueId"])
      |> Base.encode16(case: :lower)
      |> binary_part(0, 32)

    job = %{
      "metadata" => %{
        "namespace" => "frigga",
        "name" => "symphony-retained",
        "uid" => "job-retained",
        "labels" => %{
          "symphony.hypergrid.au/issue-id" => issue_label,
          "symphony.hypergrid.au/generation" => "2",
          "symphony.hypergrid.au/assignment-sha256" => String.duplicate("b", 64)
        }
      }
    }

    pod = %{
      "metadata" => %{
        "namespace" => "frigga",
        "name" => "worker-retained",
        "uid" => "pod-retained",
        "labels" => %{
          "symphony.hypergrid.au/issue-id" => issue_label,
          "symphony.hypergrid.au/generation" => "2"
        }
      }
    }

    assert {:error, :claim_resources_present} =
             ConfirmedRecoveryKubernetes.complete_resources_absent([job], @claim, :job)

    assert {:error, :claim_resources_present} =
             ConfirmedRecoveryKubernetes.complete_resources_absent([pod], @claim, :pod)
  end

  test "malformed entries and absent data fail closed" do
    assert {:error, :claim_resources_present} =
             ConfirmedRecoveryKubernetes.complete_resources_absent([%{}], @claim, :job)

    assert {:error, :claim_resources_present} = ConfirmedRecoveryKubernetes.complete_resources_absent(nil, @claim, :pod)
  end

  test "every resource must have valid namespace identity and metadata maps" do
    base = %{
      "kind" => "Job",
      "metadata" => %{
        "namespace" => "frigga",
        "name" => "unrelated",
        "uid" => "uid-1",
        "labels" => %{},
        "annotations" => %{}
      }
    }

    assert :ok = ConfirmedRecoveryKubernetes.complete_resources_absent([base], @claim, :job)

    assert {:error, :claim_resources_present} =
             ConfirmedRecoveryKubernetes.complete_resources_absent([put_in(base, ["metadata", "namespace"], "default")], @claim, :job)

    assert {:error, :claim_resources_present} =
             ConfirmedRecoveryKubernetes.complete_resources_absent([put_in(base, ["metadata", "labels"], [])], @claim, :pod)

    assert {:error, :claim_resources_present} =
             ConfirmedRecoveryKubernetes.complete_resources_absent([put_in(base, ["kind"], "Pod")], @claim, :job)

    assert {:error, :claim_resources_present} =
             ConfirmedRecoveryKubernetes.complete_resources_absent([base], @claim, :pod)

    assert :ok =
             ConfirmedRecoveryKubernetes.complete_resources_absent([Map.put(base, "kind", "Pod")], @claim, :pod)
  end

  test "the exact issue annotation and generation also identify an allocated Job" do
    job = %{
      "kind" => "Job",
      "metadata" => %{
        "namespace" => "frigga",
        "name" => "symphony-claim",
        "uid" => "uid-2",
        "labels" => %{},
        "annotations" => %{
          "symphony.hypergrid.au/assignment-issue-id" => @claim["issueId"],
          "symphony.hypergrid.au/assignment-generation" => "2"
        }
      }
    }

    assert {:error, :claim_resources_present} =
             ConfirmedRecoveryKubernetes.complete_resources_absent([job], @claim, :job)
  end

  test "live observation denies before contacting Kubernetes when scope is wrong" do
    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryKubernetes.observe(Map.put(@claim, "generation", 1), %{"apiServer" => "https://10.0.14.10:6443"})

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryKubernetes.observe(@claim, %{"apiServer" => "https://wrong.example"})

    assert {:error, :kubernetes_observation_unavailable} = ConfirmedRecoveryKubernetes.observe(nil, %{})
  end

  test "fixed-scope readback requires Jobs, Pods, and a confirming Jobs list in order" do
    cluster = %{"apiServer" => "https://10.0.14.10:6443", "caSha256" => String.duplicate("c", 64)}
    {:ok, calls} = Agent.start_link(fn -> [] end)

    context = fn claim ->
      Agent.update(calls, &[{:context, claim} | &1])
      {:ok, :synthetic_context, cluster["caSha256"]}
    end

    jobs = fn namespace, :synthetic_context ->
      number = Agent.get(calls, &Enum.count(&1, fn {kind, _} -> kind == :jobs end)) + 1
      Agent.update(calls, &[{:jobs, namespace} | &1])
      {:ok, %{items: [], resource_version: Integer.to_string(number)}}
    end

    pods = fn namespace, :synthetic_context ->
      Agent.update(calls, &[{:pods, namespace} | &1])
      {:ok, %{items: [], resource_version: "7"}}
    end

    assert {:ok, observation} =
             ConfirmedRecoveryKubernetes.observe_with_test_adapter(@claim, cluster, context, jobs, pods)

    assert observation["apiServer"] == cluster["apiServer"]
    assert observation["namespace"] == "frigga"

    assert observation["jobs"] == %{
             "firstResourceVersion" => "1",
             "confirmingResourceVersion" => "2",
             "sha256" => :crypto.hash(:sha256, "[]") |> Base.encode16(case: :lower),
             "itemCount" => 0,
             "claimAbsent" => true
           }

    assert observation["pods"]["resourceVersion"] == "7"
    assert observation["pods"]["itemCount"] == 0
    assert observation["pods"]["claimAbsent"]

    assert Enum.reverse(Agent.get(calls, & &1)) == [
             {:context, @claim},
             {:jobs, "frigga"},
             {:pods, "frigga"},
             {:jobs, "frigga"}
           ]

    Agent.stop(calls)
  end

  test "readback stops at a retained Job, retained Pod, or wrong CA before terminal evidence" do
    cluster = %{"apiServer" => "https://10.0.14.10:6443", "caSha256" => String.duplicate("c", 64)}
    parent = self()
    context = fn _claim -> {:ok, :synthetic_context, cluster["caSha256"]} end
    issue_label = :crypto.hash(:sha256, @claim["issueId"]) |> Base.encode16(case: :lower) |> binary_part(0, 32)

    retained = %{
      "kind" => "Job",
      "metadata" => %{
        "namespace" => "frigga",
        "name" => "retained",
        "uid" => "retained-uid",
        "labels" => %{
          "symphony.hypergrid.au/issue-id" => issue_label,
          "symphony.hypergrid.au/generation" => "2"
        }
      }
    }

    retained_jobs = fn _namespace, _context -> {:ok, %{items: [retained], resource_version: "1"}} end

    no_pods = fn _namespace, _context ->
      send(parent, :pods_read)
      {:ok, %{items: [], resource_version: "1"}}
    end

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryKubernetes.observe_with_test_adapter(@claim, cluster, context, retained_jobs, no_pods)

    refute_received :pods_read

    empty_jobs = fn _namespace, _context ->
      send(parent, :jobs_read)
      {:ok, %{items: [], resource_version: "1"}}
    end

    retained_pods = fn _namespace, _context -> {:ok, %{items: [Map.put(retained, "kind", "Pod")], resource_version: "1"}} end

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryKubernetes.observe_with_test_adapter(@claim, cluster, context, empty_jobs, retained_pods)

    assert_received :jobs_read

    wrong_ca = fn _claim -> {:ok, :synthetic_context, String.duplicate("d", 64)} end

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryKubernetes.observe_with_test_adapter(@claim, cluster, wrong_ca, empty_jobs, no_pods)

    refute_received :jobs_read
    refute_received :pods_read
  end

  test "unexpected credential or list callback failures deny the observation" do
    cluster = %{"apiServer" => "https://10.0.14.10:6443", "caSha256" => String.duplicate("c", 64)}
    list = fn _namespace, _context -> {:ok, %{items: [], resource_version: "1"}} end

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryKubernetes.observe_with_test_adapter(
               @claim,
               cluster,
               fn _ -> raise "credential read failed" end,
               list,
               list
             )

    assert {:error, :kubernetes_observation_unavailable} =
             ConfirmedRecoveryKubernetes.observe_with_test_adapter(
               @claim,
               cluster,
               fn _ -> throw(:credential_read_failed) end,
               list,
               list
             )
  end
end
