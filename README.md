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

The source-only disposable RKE2 adapter now registers the server-assigned Job UID with Dahlia
before reporting an allocation ready. The trusted host supplies the validated provider claim,
provider origin and runner token; a denied or uncertain registration leaves the Job suspended.
This handoff is not a deployed disposable worker or credential route.
The deterministic Job compiler can optionally mount a host-selected, writable Codex OAuth
session-slot claim at `CODEX_HOME` while keeping the repository workspace ephemeral. This is
source support only: no exclusive slot lease, authenticated session, production worker image,
or live Job is established by the compiler.
The managed adapter requires a host-owned slot lease guard at reserve, UID bind, activation,
and post-cleanup release when that optional mount is selected. No production guard is wired yet.
The source-only managed executor can release a pre-checkout allocation after a confirmed
credential denial, expiry, or invalid response, but only through Dahlia's signed abort
proof and exact provider release acknowledgement. Uncertain credential outcomes stay held.

An enforced managed pool consumes an Ed25519-signed, digest-pinned,
operator-issued delegation manifest; an empty manifest is signed too.
It creates responsibility only for the exact freshly eligible issue, through the existing graph
owner, and keeps successor admission behind predecessor cleanup and provider claims. A ready label
does not grant execution authority. See [responsibility delegation](docs/responsibility-delegation.md).
Each signed work-package ID must match the provider's canonical projection, and correction of an
active never-submitted grant requires a distinct signed successor plus the paused, evidence-bound
operator retirement path; prior grant and fence records remain auditable.

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

### Paused confirmed-claim recovery

HGS-740 handles a confirmed generation-2 provider claim when no RKE2 Job was allocated. The
source-only root commands are:

```text
symphony --issue-hgs740-confirmed-recovery --workflow <trusted-WORKFLOW.md> --nonce <proof-nonce-uuid> --bundle <generation-2>/issuer-input.json <issue-uuid> <pool-key>
symphony --apply-hgs740-confirmed-recovery --workflow <trusted-WORKFLOW.md> --nonce <proof-nonce-uuid> <issue-uuid> <pool-key>
symphony --complete-hgs740-recovery --workflow <trusted-WORKFLOW.md> <issue-uuid> <pool-key>
symphony --verify-hgs740-startup --workflow <trusted-WORKFLOW.md> <pool-key>
```

Issuance accepts a canonical root-owned, mode-`0600` JSON input bundle with exactly `assignmentSHA256`,
`reservationId`, `observation`, and `providerHeld`. It requires the paused gate and all managed
services stopped and masked, checks the state-owner processes, refreshes complete Frigga Job/Pod
absence, validates the existing HGS-740 schema and pinned signer, then writes the canonical
`candidate.json` and detached root-signed `confirmed-root-envelope.json` with exclusive creation
and mode `0600`. Existing artifacts are never replaced; an interrupted partial write is preserved
for investigation. The bundle's provider readback must come from a trusted root-owned collector
using the provider's dual-auth held-claim readback; it must contain no auth material. Symphony
validates its schema and freshness but does not fetch that provider readback itself, so this source
path is not end-to-end live qualification.

The absent-snapshot v2 bundle also includes `assignmentSnapshotState: "absent"` and a null
assignment digest. A retirement-only v3 bundle adds `predecessorClaimState: "unsubmitted"`:
no generation-1 provider claim or witness is fabricated. Its separately signed proof
binds the exact persisted retirement and native source/grant digests, with a sole gen2
witness and complete six-pool chains. Recovery releases the exact restart-blocked gen2
lease while preserving blocked authority. The legacy contracts retain their stricter
predecessor requirements; source tests and a read-only state probe are separate from live
release qualification.

The installed runner calls startup verification from a root `ExecStartPre` with an empty
environment. Before an apply marker exists, startup is allowed only when that issue has no HGS-740
evidence directory; once candidate evidence exists without its durable transaction marker, startup
fails closed until root applies or investigates it. `applying` markers always deny startup.
`local_applied` markers deny startup until provider release is complete. The complete command
accepts only the pinned HGS-485 final release receipt and fresh full Frigga Job/Pod snapshots;
it also checks the released journal/fence/graph invariants. A completed marker rechecks its signed
receipts and claim lineage at startup. Later valid generations may create new Jobs and leases.

Evidence is stored root-only under
`/srv/dahlia-runner-state/evidence/hgs740-confirmed-recovery/<issue-uuid>/generation-2/`:
`candidate.json`, `confirmed-root-envelope.json`, `transaction.json`,
`local-transition-candidate.json`, and `local-transition-receipt.json`. The transaction marker is
written and synced before any of the three domain persistence APIs update local state. After that
marker, root takes custody of the runner-writable `run/` and `workspaces/` trees by making those
two directory inodes root-owned mode `0700`; a retry accepts only their recorded original or
custody metadata and replays exact saved postimages. Completion restores the original directory
metadata before publishing the complete marker. A crash leaves startup denied and root replayable.
Contradictory bytes, missing evidence, uncertain Kubernetes pagination, an absent signer, or an
unpaused gate keep the transition held.
The completion command does not unpause admission. This source change does not install the
ExecStartPre hook, run on a host, or qualify a live recovery.

The apply and complete commands require all six pool units and both witness units stopped and
masked, the state owner's user manager inactive, and no process running under the state owner's
UID. State files retain their original UID, GID, and mode from the durable marker; after each
domain save or replay the command restores and verifies that metadata before it advances the
marker. Completed startup checks live generation-2 recovery, released fence and graph leases, and
the recorded directory identities while allowing later generation-3 journal entries and workers.
Provider-held evidence must include complete, empty broker-credential and OAuth-slot lease
inventories from the same database snapshot as the held reservation readback.

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

Pre-execution abort input publication sends exact selectors to the fixed root
service after confirmed deletion. The result reference is derived from the signed
assignment; publication requires the matching retained journal and root
acknowledgement. See [the runtime contract](elixir/README.md).

Paused HGS-740 recovery supports one append-only reconciliation observation
epoch for the exact retained generation-2 failed issuance. Historical inputs
remain pinned and unchanged; fresh evidence uses the existing signed recovery
and provider release path. See [the runtime contract](elixir/README.md).
