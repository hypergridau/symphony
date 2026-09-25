defmodule SymphonyElixir.ManagedResponsibilityFixture do
  @moduledoc false
  alias SymphonyElixir.ManagedTokenBudget.Runtime, as: BudgetRuntime

  @spec initialize_budget(map()) :: map()
  def initialize_budget(state) do
    manifest = state.work_package_runtime.managed_delegations
    root = Path.dirname(SymphonyElixir.Workflow.workflow_file_path())

    runtime =
      state.work_package_runtime
      |> Map.put_new(:journal_path, Path.join(root, "claims.json"))
      |> Map.put_new(:managed_project_profile_id, manifest.managed_project_profile_id)

    state = %{state | work_package_runtime: runtime}
    {:ok, path, identity} = BudgetRuntime.location(runtime)

    baselines =
      Enum.map(manifest.entries, fn entry ->
        %{issue_id: entry.issue_id, known_minimum_tokens: 0, continuation_floor: 1, evidence_ref: "test:new-fixture-no-prior-worker", authority_ref: "test:explicit-bootstrap"}
      end)

    {:ok, _ledger} = SymphonyElixir.ManagedTokenBudget.initialize(path, identity, baselines)
    {:ok, state} = BudgetRuntime.load(state)
    state
  end

  @spec context(String.t()) :: map()
  def context(repository \\ "openai/symphony") do
    %{pool_key: "test-pool", repository_ref: repository, managed_project_profile_id: "profile-test", runner_id: "runner-test"}
  end

  @spec issue(pos_integer()) :: SymphonyElixir.Tracker.Issue.t()
  def issue(number) do
    %SymphonyElixir.Tracker.Issue{
      id: id(number),
      identifier: "HGS-#{number}",
      assignee_id: "owner-test",
      title: "Useful fixture task",
      state: "Todo",
      labels: ["symphony-ready"],
      dispatchable: true,
      branch_name: "codex/test-#{number}"
    }
  end

  @spec payload(integer(), String.t()) :: map()
  def payload(now, repository \\ "openai/symphony") do
    %{
      "schema_version" => 1,
      "pool_key" => "test-pool",
      "repository_ref" => repository,
      "managed_project_profile_id" => "profile-test",
      "authority_ref" => "test:explicit-coo-authority",
      "entries" => Enum.map([1, 2], &entry(&1, now, repository))
    }
  end

  defp id(number), do: "11111111-1111-4111-8111-" <> String.pad_leading(Integer.to_string(number), 12, "0")

  defp entry(number, now, repository) do
    issue = issue(number)
    actions = ~w(read edit commit push state_mutation cleanup report)

    scope = %{
      "company_id" => "company-test",
      "objective_id" => "objective-test",
      "initiative_id" => "initiative-test",
      "project_id" => "project-test",
      "work_package_id" => "package-#{number}",
      "issue_id" => issue.id,
      "repository" => repository,
      "paths" => ["."],
      "modules" => [],
      "environments" => ["repository"],
      "actions" => actions
    }

    common = %{
      "scope" => scope,
      "expires_at_ms" => now + 60_000,
      "expected_deliverable" => "Bounded source change",
      "expected_evidence" => "Independent source review and tests",
      "return_to_parent" => %{"owner_id" => "owner-test", "contract" => "Return exact reviewed outcome"}
    }

    accountable =
      Map.merge(common, %{
        "id" => "accountable-#{number}",
        "role" => "accountable",
        "actor_id" => "owner-test",
        "parent_delegation_id" => nil,
        "authority" => %{"class" => "routine_engineering", "capabilities" => actions ++ ~w(delegate observe reconcile), "environments" => ["repository"]},
        "budget" => %{"model" => "gpt-6-luna", "effort" => "max", "max_tokens" => 500_000, "max_children" => 1}
      })

    responsible =
      Map.merge(common, %{
        "id" => "responsible-#{number}",
        "role" => "responsible",
        "actor_id" => "runner-test",
        "parent_delegation_id" => accountable["id"],
        "authority" => %{"class" => "routine_engineering", "capabilities" => actions, "environments" => ["repository"]},
        "budget" => %{"model" => "gpt-6-luna", "effort" => "max", "max_tokens" => 500_000, "max_children" => 0}
      })

    %{"issue_id" => issue.id, "identifier" => issue.identifier, "owner_id" => issue.assignee_id, "accountable" => accountable, "responsible" => responsible}
  end
end
