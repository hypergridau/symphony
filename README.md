# Symphony

Symphony turns project work into isolated, autonomous implementation runs, allowing teams to manage
work instead of supervising coding agents.

The Elixir reference runtime can optionally publish a small, sanitized runner-posture observation to
a private control plane. This call-home path is disabled until its runner identity, managed project
profile, pool, and bearer token are all supplied; it never sends prompts, secrets, logs, worktree
paths, or the raw orchestrator snapshot.

Managed Linux pools can also enable the generation-bound work-package runtime. The host must
provide the complete provider URL, runner token, attestation key, runner ID, and managed project
profile ID tuple; partial configuration fails startup, while a declared `SYMPHONY_POOL_KEY` or
`SYMPHONY_REPOSITORY_REF` without it fails startup as well. The Elixir runtime archives dirty and unmerged work, retains open PR
candidates, and releases provider scope only after independently verified supervisor termination
and workspace cleanup. See [elixir/README.md](elixir/README.md) for the host environment contract.
Recovery archives retain junctions and symbolic links as verified metadata, without following their
targets or requiring link-creation privileges on the archive host.

Before managed source execution, the local runtime prepares the authorized task branch and verifies
the actual checkout against the execution generation. Conflicting retained work is preserved for
recovery. Worker prompts receive the prepared identity; branch preparation belongs to the runtime.

An enforced managed pool consumes an Ed25519-signed, digest-pinned,
operator-issued delegation manifest; an empty manifest is signed too.
It creates responsibility only for the exact freshly eligible issue, through the existing graph
owner, and keeps successor admission behind predecessor cleanup and provider claims. A ready label
does not grant execution authority. See [responsibility delegation](docs/responsibility-delegation.md).

Model selection is also subject to that authority: manifest-managed workers default to GPT-6
Luna high, escalating to xhigh and max only after failed attempts. The optional
`model:gpt-6-luna` label is redundant for managed workers; legacy or conflicting `model:*`
labels fail admission. A matching operator-issued model and effort grant is still required.
Historical claims and grants are not rewritten; stale GPT-5.6 grants fail closed.

On the Linux runner, the root-owned global pause transition fences new Task
spawns by waiting for epoch-matched, synchronous state snapshots from every
active repository pool. The marker denies new admission callbacks while the
barrier is pending or interrupted. A callback already past its final gate may
start a child before the setter acknowledges the pause; none may start after
that acknowledgment. The pause does not terminate workers already running.

A generation released before claim submission does not reserve the repository indefinitely.
Another eligible issue may proceed only after the journal, exact delegation and absent local
workspace prove that the released generation has no remaining mutable authority.

Managed issue token totals survive scheduler restarts and claim release. Explicit historical
baselines and per-thread cumulative observations live beside the private claim journal. Missing or
uncertain accounting prevents further admission; recovery does not silently reset an allowance.
An explicit progress-scoped Luna grant can omit a per-task token count while retaining that
accounting and all independent scope, expiry, and progress controls; finite grants remain bounded.
The sole host operator can explicitly register a genuinely new canonical issue in the existing
ledger after verifying no prior execution. Registration preserves old usage and requires separate
current responsibility and provider authority before execution; the scheduler never creates it.

Managed claims retain their generation and repository capacity when an acknowledgement is lost.
The runner journals submission and the first spawn attempt separately, replays only the exact
current authority, and stops after bounded recovery attempts. A legacy or mismatched claim requires
explicit reconciliation; it cannot silently become a new worker generation.
For retained pre-spawn incidents, the host may supply a signed provider recovery receipt. The
runtime verifies the old journal, complete unstarted history and released scope before normal
admission advances the generation; it preserves the original evidence and never invents cleanup.

[![Symphony demo video preview](.github/media/symphony-demo-poster.jpg)](https://player.vimeo.com/video/1186371009?h=5626e4b899)

_In this [demo video](https://player.vimeo.com/video/1186371009?h=5626e4b899), Symphony monitors a Linear board for work and spawns agents to handle the tasks. The agents complete the tasks and provide proof of work: CI status, PR review feedback, complexity analysis, and walkthrough videos. When accepted, the agents land the PR safely. Engineers do not need to supervise Codex; they can manage the work at a higher level._

> [!WARNING]
> Symphony is a low-key engineering preview for testing in trusted environments.

## Running Symphony

### Requirements

Symphony works best in codebases that have adopted
[harness engineering](https://openai.com/index/harness-engineering/). Symphony is the next step --
moving from managing coding agents to managing work that needs to get done.

### Option 1. Make your own

Tell your favorite coding agent to build Symphony in a programming language of your choice:

> Implement Symphony according to the following spec:
> https://github.com/openai/symphony/blob/main/SPEC.md

### Option 2. Use our experimental reference implementation

Check out [elixir/README.md](elixir/README.md) for instructions on how to set up your environment
and run the Elixir-based Symphony implementation. You can also ask your favorite coding agent to
help with the setup:

> Set up Symphony for my repository based on
> https://github.com/openai/symphony/blob/main/elixir/README.md

---

## License

This project is licensed under the [Apache License 2.0](LICENSE).
