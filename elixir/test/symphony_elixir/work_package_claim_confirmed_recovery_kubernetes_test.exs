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
end
