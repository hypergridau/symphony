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
The trusted pre-execution abort caller requires exact durable typed blocked-result
bytes before deleting an unstarted allocation, then preserves its confirmed-delete
checkpoint for root publication replay. This source safety guard does not establish
autonomous failure routing or live lifecycle acceptance.

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

The root adapter now exposes a read-only `read_local_receipt_snapshot/4` handoff.
Synthetic qualification now covers the Hypergrid pool's generation-two v3
retirement-only state in epoch 5, including a durable-sync failure, restart after
expiry, unchanged signed inputs, blocked grants, repeatable snapshots and denial
when an epoch input disappears. Dahlia's paired fixture checks the actual native
marker, candidate and state bytes; representative binding metadata is test-only.
Under the existing paused/quiescent launcher lock it verifies the recorded proof
at its saved verification time, reconstructs the deterministic transition, checks
the current recovered lineage, and returns exact marker, candidate, proof,
observation and committed postimage bytes. It does not issue a receipt or fresh
release observation, sign, publish files, release the provider or relax startup.
This supplies native-verified bytes for the separate Dahlia release-only contract;
owner authorization, fresh attestation and receipt publication remain separate gates.

Canonical `ConfirmedRecoveryCore.complete/3` also recognizes an already committed
HGS-740 release-only bundle. Under the existing paused, quiescent pool lock it
reads the fixed provider confirmation endpoint, checks the original signatures
and human decisions at the durable confirmation time, and appends a root-private
completion witness before committing the terminal marker. Startup verifies that
retained witness without network access or renewed authority. Partial release
evidence holds closed; the legacy HGS-719 verifier remains strict. This source
repair does not itself install or complete the live recovery, unpause admission,
create an epoch, sign a receipt, or start a worker.

The packaged recovery CLI starts the YAML parser required for its workflow,
while preserving its independent root maintenance path and absent orchestration.

The paired source-only release coordinator binds that native snapshot and the
protected original epoch-5 manifest to an action-scoped Dahlia human approval
ledger projection. Native host ports now reuse existing locking, root custody,
bounded Kubernetes evidence, protected provider identities and fixed HTTPS ledger
and held-state transport. Trusted ports reserve one attempt before collecting a
distinct release attestation, recheck custody and the decision around signing,
then exclusively publish the unchanged v3 receipt and retained bundle. Partial
attempts hold without recollection; complete bundles replay read-only. Authorization
and attestation freshness are checked after slow snapshot and ledger rereads,
immediately before signing and each subsequent publication. The original
sealed observation and native state stay unchanged. Production `release_only/4`
is unconditionally closed pending separate protocol acceptance, owner/attestor
enrollment and installed collector admission. A gated trusted composition is
available for isolated qualification, while normal production commands remain closed.
This source change performs no live
collection, signing, provider release, deployment, unpause or epoch allocation.

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

The host now checks the bound OAuth slot before recording spawn intent. Only
Dahlia's exact typed `codex_auth_slot_denied` response can retain an `abort_pending`
claim. The canonical blocked result, unused worker lease release, root disposal
proof and existing prepare/confirm checkpoints remain durable across restart.
Cleanup retains the claim until provider release is independently verified.
These are source and synthetic-test contracts; installed/live qualification is
separate.

Paused HGS-740 recovery supports a fixed append-only reconciliation observation
epoch for the exact retained generation-2 failed issuance. Historical inputs
remain pinned and unchanged; fresh evidence uses the existing signed recovery
and provider release path. See [the runtime contract](elixir/README.md).
The recovery workflow verifier accepts the same bounded 130-file control export
as Dahlia's reconciliation exporter.
Root recovery acquires the existing UID 1001 launcher lock while retaining
root signing authority. Preflight checks this lock before observations are
collected, and recovery verifies the held inode throughout the operation.
The observed unsigned epoch-1 lock failure permits one explicit epoch-2
successor bound to its unchanged five files and retained failure seal. Reserving
that successor prevents fallback to epoch 1.
The sealed unsigned epoch-2 HTTP startup failure permits one fixed epoch-3
successor with a V3 manifest binding both failed histories and seals. Reserving
it prohibits earlier fallback. Native issuer preflight now proves fresh
Jobs/Pods/Jobs absence under the existing root recovery lock before collection.
The cold recovery executable starts Req's HTTP dependencies only after validating
the fixed Kubernetes identity, credential context and CA. It does not start
Symphony or its workers; failed HTTP startup or readback keeps recovery held.
The signed epoch-3 service-guard failure permits one fixed epoch-4 successor.
It pins the complete signed history and retains the original transaction inputs
in place. Fresh inputs are published only inside epoch 4, and every reader
selects that directory or fails closed once it exists. Root recovery requires
the same masked, stopped unit before collection, signing and apply; generic
successor-retirement service rules remain unchanged. Preflight computes the
local transition without signing or writing state before reserving an epoch.
Private evidence reads tolerate only the access-time update caused by reading
the file; all identity, custody and mutation checks remain fixed. Failed signed
observations remain retained and do not acquire fresh runtime authority.
Owner authorization permits only one fixed epoch-5 successor for this same
generation-2 claim. It pins all four historical epochs and failure seals,
verifies the fourth signature at its recorded issuance, and cannot downgrade
once reserved. Epoch 5 does not authorize another execution generation or epoch.

HGS-740 WAL durability uses separate file-sync and protected Linux directory-sync
operations. Directory opens require `O_DIRECTORY` and `O_NOFOLLOW`; failures
retain the applying transaction and keep provider release closed. Recovery uses
the same saved signed evidence, nonce and authorization time; it does not collect
or sign a new reconciliation epoch or waive provider first-release freshness.
