defmodule SymphonyElixir.ModelRouterTest do
  use ExUnit.Case, async: true

  alias SymphonyElixir.Codex.ModelRouter
  alias SymphonyElixir.Tracker.Issue

  test "defaults to luna high and escalates through luna xhigh, luna max, then sol xhigh" do
    issue = %Issue{labels: ["symphony-ready"]}

    assert %{tier: "luna-high", model: "gpt-5.6-luna", effort: "high", escalated: false} =
             ModelRouter.resolve(issue, nil)

    assert %{tier: "luna-xhigh", model: "gpt-5.6-luna", effort: "xhigh", escalated: true} =
             ModelRouter.resolve(issue, 1)

    assert %{tier: "luna-max", model: "gpt-5.6-luna", effort: "max", escalated: true} =
             ModelRouter.resolve(issue, 2)

    assert %{tier: "sol-xhigh", model: "gpt-5.6-sol", effort: "xhigh", escalated: true} =
             ModelRouter.resolve(issue, 3)

    assert %{tier: "sol-xhigh"} = ModelRouter.resolve(issue, 20)
  end

  test "uses luna for bounded labels before escalating" do
    issue = %Issue{labels: ["Documentation"]}

    assert %{tier: "luna-high", effort: "high"} = ModelRouter.resolve(issue, 0)
    assert %{tier: "luna-xhigh"} = ModelRouter.resolve(issue, 1)
  end

  test "retired spark label falls back to luna" do
    issue = %Issue{labels: ["model:spark"]}

    assert %{tier: "luna-high", reason: "retired model:spark label fell back to luna-high"} =
             ModelRouter.resolve(issue, 0)
  end

  test "legacy terra labels stay inside the Luna ladder" do
    issue = %Issue{labels: ["model:terra"]}

    assert %{tier: "luna-xhigh", model: "gpt-5.6-luna", effort: "xhigh"} = ModelRouter.resolve(issue, 0)
  end

  test "high-complexity labels start at Luna xhigh and never use Terra" do
    issue = %Issue{labels: ["security"]}

    assert %{tier: "luna-xhigh", model: "gpt-5.6-luna", effort: "xhigh"} = ModelRouter.resolve(issue, 0)
    assert %{tier: "luna-max"} = ModelRouter.resolve(issue, 1)
    assert %{tier: "sol-xhigh"} = ModelRouter.resolve(issue, 2)
  end

  test "supported explicit model label wins over inferred complexity" do
    issue = %Issue{labels: ["security", "model:luna"]}

    assert %{tier: "luna-high", reason: "explicit model:luna label"} = ModelRouter.resolve(issue, 0)
  end

  test "opt-in GPT-6 Luna stays on its own ladder for every retry" do
    issue = %Issue{labels: ["symphony-ready", "model:gpt-6-luna"]}

    assert %{tier: "gpt6-luna-high", model: "gpt-6-luna", effort: "high", escalated: false} =
             ModelRouter.resolve(issue, nil)

    assert %{tier: "gpt6-luna-xhigh", model: "gpt-6-luna", effort: "xhigh", escalated: true} =
             ModelRouter.resolve(issue, 1)

    assert %{tier: "gpt6-luna-max", model: "gpt-6-luna", effort: "max", escalated: true} =
             ModelRouter.resolve(issue, 2)

    for attempt <- 3..20 do
      assert %{tier: "gpt6-luna-max", model: "gpt-6-luna", effort: "max"} =
               ModelRouter.resolve(issue, attempt)
    end
  end
end
