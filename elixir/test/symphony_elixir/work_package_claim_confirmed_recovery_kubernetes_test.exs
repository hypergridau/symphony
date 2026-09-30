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

    assert {:error, :claim_resources_present} = ConfirmedRecoveryKubernetes.complete_resources_absent([job], @claim, :job)
    assert {:error, :claim_resources_present} = ConfirmedRecoveryKubernetes.complete_resources_absent([pod], @claim, :pod)
  end

  test "malformed entries and absent data fail closed" do
    assert {:error, :claim_resources_present} = ConfirmedRecoveryKubernetes.complete_resources_absent([%{}], @claim, :job)
    assert {:error, :claim_resources_present} = ConfirmedRecoveryKubernetes.complete_resources_absent(nil, @claim, :pod)
  end
end
