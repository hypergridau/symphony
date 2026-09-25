# Symphony Elixir

This directory contains the current Elixir/OTP implementation of Symphony, based on
[`SPEC.md`](../SPEC.md) at the repository root.

> [!WARNING]
> Symphony Elixir is prototype software intended for evaluation only and is presented as-is.
> We recommend implementing your own hardened version based on `SPEC.md`.

## Screenshot

![Symphony Elixir screenshot](../.github/media/elixir-screenshot.png)

## How it works

1. Polls the configured tracker for candidate work (included adapters: Linear, GitHub Issues, Jira
   Cloud, Asana, and GitLab)
2. Creates a workspace per issue
3. Launches Codex in [App Server mode](https://developers.openai.com/codex/app-server/) inside the
   workspace
4. Sends a workflow prompt to Codex
5. Keeps Codex working on the issue until the work is done

During app-server sessions, the selected tracker adapter may advertise provider-native tools. The
Linear serves `linear_graphql`, GitHub Issues serves `github_api`, Jira Cloud serves
`jira_rest`, Asana serves `asana_api`, and GitLab serves `gitlab_api`. Symphony executes those
tools with configured host-side auth and removes declared tracker-token environment variables from
the Codex child, so the agent does not need a second tracker login.

Manifest-managed issues route the initial attempt to GPT-6 Luna high by default. Later failed
attempts can escalate only to GPT-6 Luna xhigh and max; this route never falls back to another
model. `model:gpt-6-luna` is accepted but unnecessary, while legacy or conflicting `model:*`
labels fail managed admission. A matching host-controlled delegation grant for the exact model
and maximum effort remains necessary alongside the normal issue, repository, and execution
controls. Unmanaged legacy routing remains separate.

If a claimed issue moves to a terminal state (`Done`, `Closed`, `Cancelled`, or `Duplicate`),
Symphony stops the active agent for that issue and cleans up matching workspaces.

If Codex reports that operator input, approval, or MCP elicitation is required, Symphony keeps the
issue claimed and exposes it as blocked in the runtime state, JSON API, and dashboard. Blocked
entries are in memory only; restarting the orchestrator clears that blocked map, so any still-active
tracker issue can become a dispatch candidate again after restart.

## How to use it

1. Make sure your codebase is set up to work well with agents: see
   [Harness engineering](https://openai.com/index/harness-engineering/).
2. Get a new personal token in Linear via Settings â†’ Security & access â†’ Personal API keys, and
   set it as the `LINEAR_API_KEY` environment variable.
3. Copy this directory's `WORKFLOW.md` to your repo.
4. Optionally copy the `commit`, `push`, `pull`, `land`, and `linear` skills to your repo.
   - The `linear` skill expects Symphony's `linear_graphql` app-server tool for raw Linear GraphQL
     operations such as comment editing or upload flows.
5. Customize the copied `WORKFLOW.md` file for your project.
   - To get your project's slug, right-click the project and copy its URL. The slug is part of the
     URL.
   - When creating a workflow based on this repo, note that it depends on non-standard Linear
     issue statuses: "Rework", "Human Review", and "Merging". You can customize them in
     Team Settings â†’ Workflow in Linear.
6. Follow the instructions below to install the required runtime dependencies and start the service.

## Prerequisites

We recommend using [mise](https://mise.jdx.dev/) to manage Elixir/Erlang versions.

```bash
mise install
mise exec -- elixir --version
```

## Run

```bash
git clone https://github.com/openai/symphony
cd symphony/elixir
mise trust
mise install
mise exec -- mix setup
mise exec -- mix build
mise exec -- ./bin/symphony ./WORKFLOW.md
```

### Global mutable-admission pause

Repository-pool operators can prevent new workers from being admitted by setting
`SYMPHONY_GLOBAL_PAUSE_FILE` to a host-local state file. A configured gate is
fail-closed: only the exact file contents `running` permit new workers; missing,
unreadable, or invalid contents remain paused. The orchestrator checks the gate
while polling, before retry dispatch, before execution-fence admission, and
immediately before spawning a worker. Existing workers are not terminated by the
gate.

On the Linux runner, the trusted root setter places a root-owned
`global-mutable-pause.transition` marker beside the gate while it persists
`paused` and waits for every active pool's fresh `/api/v1/state` reply. A
configured pool treats any malformed marker as paused; a valid marker reports
its exact epoch in `pause_gate.transition_epoch`. The state endpoint is
uncached and obtains the snapshot synchronously from the same orchestrator
GenServer that makes the final Task spawn decision. The root setter must not
report a completed pause until it has epoch-matched replies from every active
pool or proof the service is stopped. A dispatch callback that passed its
final gate before marker creation can still start a child while the barrier
waits for its snapshot; the setter returns only after that callback finishes.
Already-running workers continue; this protocol fences new Task spawns after
pause completion, not ongoing work. A failed or interrupted
barrier leaves the marker in place and admission closed for supported recovery.

Startup terminal-workspace cleanup is fence-aware: a terminal issue with no
recorded execution generation in the current pool, or a generation already
marked cleaned, can use the existing path-safe cleanup path. Any recorded
non-cleaned generation remains deferred for explicit fence reconciliation.

### File-change progress and token limits

The no-progress guard recognizes app-server `item/completed` notifications whose nested
`fileChange` item completed successfully in the current thread and turn. Every change must
contain a nonempty diff, a supported add/update/delete kind, and an absolute path lexically
inside the running workspace. Approval requests, failed or declined changes, and unrelated
items do not count as durable progress.

A completed item advances both progress baselines once. Repeated or conflicting uses of
the same thread/turn/item identity do not advance them again. Each running attempt retains
at most 1,024 identities without eviction; further file changes fail closed at that limit.
This observation state does not grant execution authority or replace workspace containment.
Existing session/turn outcomes, cumulative token limits, no-progress limits, time limits,
and claim fencing remain in force. Restarted attempts must use the existing fenced recovery
path; an old session's notifications cannot qualify a new session's progress.

Managed checkout preflight also recognizes committed source advancement. The first verified
HEAD establishes an attempt-local baseline without progress credit. A later strict descendant
earns durable progress only when its endpoint tree differs. Empty commits advance the
observation cursor without credit; unchanged HEADs do neither. Rewritten history, failed Git
observations and rejected checkpoints hold the attempt through its final hooks.

The existing worker process performs bounded Git reads before its next authorized tool.
During a managed checkout turn it also performs the same wrapped preflight on a private
one-second timer while Codex is silent. Timer ticks consume the current pending inactivity
deadline; external stream messages continue to reset the full sliding timeout. A tick is
rearmed only after a successful preflight, and the latest timer is synchronously canceled
and drained when the turn exits. Timer messages carry a per-turn reference and do not
affect unmanaged callers or later turns.
The orchestrator receives a synchronous checkpoint bound to the actual caller, current
generation, session, repository, workspace and branch. It updates only the durable baseline
to currently reported cumulative usage. Later usage remains chargeable; neither historical
usage nor the meaningful-progress baseline is adjusted. Restart begins a new observation
baseline through the existing fenced recovery path. A checkpoint is source progress, not
reviewed completion, and cannot authorize publication, terminal cleanup or a successor.

## Burrito releases

Symphony ships self-contained executables built with
[Burrito](https://github.com/burrito-elixir/burrito). They embed Erlang/OTP, Elixir, and Symphony,
but still expect `codex`, `git`, and the selected tracker credentials on the target machine.

Supported release targets:

- `macos_arm64`
- `macos_x86_64`
- `linux_arm64`
- `linux_x86_64`

`v*` tags publish all four targets with checksums. A manual workflow run builds the same
artifacts without creating a release.

After downloading the executable for your platform from a release:

```bash
chmod +x ./symphony-v0.0.1-macos_arm64
./symphony-v0.0.1-macos_arm64 ./WORKFLOW.md
```

## Configuration

Pass a custom workflow file path to `./bin/symphony` when starting the service:

```bash
./bin/symphony /path/to/custom/WORKFLOW.md
```

If no path is passed, Symphony defaults to `./WORKFLOW.md`.

Optional flags:

- `--logs-root` tells Symphony to write logs under a different directory (default: `./log`)
- `--port` also starts the Phoenix observability service (default: disabled)
- `--activate-responsibility-graph` performs the one-time local transition from manual to
  machine-enforced responsibility admission after the runtime starts

The activation switch is intended for the current COO or explicitly delegated runtime owner
while the global mutable-admission gate is paused. It requires a declared managed pool, the
complete `DAHLIA_WORK_PACKAGE_*` runtime tuple, and an exact `paused` global pause file. The
default remains manual, and an already enforced graph returns success without changing its
persisted state. The launcher never enables this switch implicitly; the authorized local start
command is:

```bash
./bin/symphony \
  --i-understand-that-this-will-be-running-without-the-usual-guardrails \
  --activate-responsibility-graph \
  /path/to/WORKFLOW.md
```

The command only changes the existing responsibility graph snapshot. It does not clear the
pause gate, create delegations, admit an issue, or start a worker. Restarting with the same
switch reloads the persisted `enforced` graph and is idempotent; remove the switch to retain the
manual default for a fresh graph.

### Private runner call-home

The runtime can report a fixed, sanitized Symphony posture projection to a private Dahlia provider
endpoint. The reporter is disabled when any required setting is absent or invalid. Configure these
values in the runner host environment, not in a repository-owned `WORKFLOW.md`:

```text
DAHLIA_RUNNER_CALL_HOME_URL=https://provider.example/runner/v1/symphony/observations
DAHLIA_RUNNER_CALL_HOME_TOKEN=<host-injected secret>
DAHLIA_RUNNER_ID=<scoped runner identity>
DAHLIA_MANAGED_PROJECT_PROFILE_ID=<managed profile identity>
DAHLIA_SYMPHONY_POOL_KEY=<registered pool key>
```

`DAHLIA_RESPONSIBLE_DELEGATION_ID` and `DAHLIA_EXECUTION_FENCE_ID` are required for active-run
observations. `DAHLIA_RUNNER_OBSERVATION_INTERVAL_MS` defaults to 5 seconds and is capped at 60
seconds; `DAHLIA_RUNNER_OBSERVATION_STATE_PATH` can override the local sequence state file. Each
observation contains only the versioned contract fields and the reporter persists its sequence
before sending, so the provider can reject replayed or out-of-order observations.

### Work-package runtime claim

`SymphonyElixir.WorkPackageClaim` is a bounded operator adapter for the provider's v1 runner
reservation contract. It requires a validated current execution-fence generation and its active
responsible runtime lease, plus an active responsible delegation for the exact issue and repository.
The reservation nonce and authority tuple are written to a private atomic journal before claim; a
lost response or restart therefore reuses the same reservation and generation. When the
orchestrator is started with its admitted Linux supervisor and work-package runtime callbacks,
it claims this reservation before launching the mutable worker and posts termination and verified
cleanup receipts from the same fence. Runner tokens and attestation keys are supplied by the host
and are never logged or placed in workflow files.

Cleanup receipt authority retains the validated reservation generation through journal writes
and acknowledgement replay. Later generations use their own journal entry even when a legacy
entry is present; termination and repository-cleanup receipts never fall back to an older claim.

Managed review handoff remains discoverable from the persisted execution fence after the worker
stops or the orchestrator restarts. The ordinary poll confirms the exact execution session and
retains the repository claim while review is pending. A blocked worker that moves to a non-active
review state keeps its blocked entry and cleanup authority until a terminal tracker transition;
the review handoff cannot discard the only generation-bound path to the provider receipts. A
terminal tracker transition also retains that claim until signed termination and repository
cleanup acknowledgements for the exact generation are present in the durable journal.
Restart reconciliation confirms termination at or after the supervisor evidence timestamp, so an
observation captured after the restart's initial clock snapshot remains valid. Process termination
alone emits no terminal receipt. A fresh terminal tracker state and a merged GitHub PR matching the clean workspace's
current commit are required before successful fencing and cleanup. The qualified local Linux path
checks the actual branch, repository origin, merge commit and absence of an open PR on that branch;
the tracker-derived branch and the worker's initial checkout head are not acceptance evidence.
Missing or conflicting evidence keeps the workspace and capacity held. Portable archive/restore
verification and signed provider acknowledgements remain separate required cleanup steps. If a
cleaned fence no longer has readable independent archive evidence, the repository-receipt replay
is quarantined for the remainder of that process instead of retrying on every poll. Repair remains
fail-closed and a controlled restart provides one fresh replay attempt.

The journal records `submitted`, `confirmed`, and `spawn_started` separately. The last marker is
synced before attempting a worker task, so a restart can replay a submitted claim only when its
current generation and unstarted lease still match. Uncertain claims keep their repository and
pool capacity without appearing as running workers. Recovery makes at most six claim requests,
with delays of 5, 10, 20, 40, and at most 60 seconds; requests have a five-second connection deadline
and a 30-second response deadline to accommodate the provider's current native prerequisite check.
Each retry signs a fresh timestamp with the same nonce and authority tuple.

Managed local source preparation consumes the checkout created by `hooks.after_create`.
The hook must leave a full clone on clean `main`, with `HEAD` equal to the captured `origin/main`
and an exact `https://github.com/<owner>/<repository>.git` origin matching the execution record.
The clone must include all remote branch refs; shallow/single-branch clones are rejected.
Bootstrap dependencies without modifying checkout contents. The runtime creates the exact fenced
task branch only if neither its local nor captured remote ref exists. It performs no additional
network access and does not reset, force-switch or repair a retained checkout. The workspace root
must already exist. Linked Git worktrees, path aliases and remote worker hosts are unsupported.

An exclusively created `.git/symphony-execution.json` binds repository, canonical workspace, branch,
issue, generation, session and initial commit. Same-generation continuation may retain dirty work
and descendant commits. Missing, partial, mismatched or noncanonical markers require recovery.
The marker file is synced and read back; directory-entry power-loss durability is not claimed.
On Windows, managed identity comparisons follow case-insensitive filesystem path rules, and local
hooks use Git for Windows' bundled `sh.exe` when a standalone POSIX shell is unavailable.
Before-run hooks, Codex startup, turns and guarded tool boundaries compare actual checkout identity
as well as current execution authority. Runtime observations report the verified branch and head.
The runtime appends the exact prepared identity to each worker turn; generic workflow branch-creation
instructions do not override that identity. A new branch or generation requires runtime admission.

Git probes use explicit arguments, a 15-second deadline and 64 KiB output limit. Timeout or overflow
fails the attempt; a closed port alone is not evidence that its OS child terminated. Unconfirmed
termination requires host process reconciliation before signed cleanup or reuse. These checkout
checks preserve identity and recovery state; they are not an OS isolation boundary.

A deterministic provider rejection, exhausted budget, missing/corrupt journal, changed authority,
or recorded spawn attempt requires reconciliation and preserves the fence. Legacy journal entries
without a spawn marker cannot prove that no worker started. Old provider claims whose local
generation has advanced require a supported forward-only recovery; never restore an old fence,
reset provider state, or fabricate a terminal head/cleanup receipt.

A spawned attempt that failed before useful work uses
`ExecutionFence.FailedAttempt.record/4` after every required process termination
has already been confirmed. This records a local `Failed attempt` outcome with
an exact accepted head and a content-addressed failure evidence reference; it
does not change the Linear issue state. The operation preserves lease evidence
and is replayable after persistence. Unknown ownership, active leases, stale
generations, missing worker termination or conflicting outcomes reject the
transition. Complete the existing portable cleanup and retain both provider
receipt acknowledgements before using a supported failed-attempt recovery to
request fresh authority for the same useful issue. A cleaned failed attempt can
reconcile its restart-blocked accountable delegation only after its responsible
runtime lease is released; current authorization and normal admission still apply.
When a current manifest names a distinct pair, failed-attempt recovery accepts it
only after verified terminal cleanup and retirement of the previous, lease-free pair.
Both new IDs must be absent, and the owner, runner and scope must match the old
authority. Recovery first applies normal expiry reconciliation to its graph candidate,
so elapsed active or restart-blocked grants do not require a separate persisted expiry
step. Normal admission creates the new pair; prior grant fields and history remain
intact apart from the legitimate expiry status and event. Rejected candidates are not
persisted, and the claim journal and fenced generation history remain intact. Existing same-ID recovery
and signed pre-spawn abandonment keep their original checks.

An owner can explicitly revoke exhausted failed-attempt authority without changing
its expiry or budget. After both cleanup acknowledgements and lease release,
`WorkPackageClaim.Recovery.authority_revocation_ref/4` computes a reference bound
to the exact failed generation, original grants and verified cleanup. It grants no
authority and mutates nothing. Use public `ResponsibilityGraph.revoke/4` on the
accountable root with that reference as its reason; its responsible child must be
revoked by that same cascade. A separately issued, validated manifest can name
distinct new IDs and an optional entry-level `prior_authority_revocation_ref`
containing that exact SHA-256 reference. Recovery still checks the native owner,
runner, scope, absent new IDs, failed cleanup and normal admission. Active
unexpired authority, a partial revocation or a changed grant fingerprint remains
inadmissible. Old grants and debits stay intact; any replacement token ceiling is
cumulative and must include the failed attempt's usage.

Restart reconciliation requires an unexpired delegation, a nondecreasing heartbeat,
an active unexpired parent, and the exact persisted runtime lease. Public graph
reconciliation expires both active and restart-blocked authority at the original
expiry boundary while retaining its clocks, budget, history and runtime identity.
Expiry does not prove worker termination, release execution fences or provider
reservations, clean a workspace, renew a budget, or authorize another attempt.

Releasing an exact runtime lease from an active delegation preserves its heartbeat
and expiry, including when cleanup occurs after expiry. The release event records
the actual time; it does not renew authority. Already expired or revoked delegation
statuses remain ineligible for this release operation.

An expired, restart-blocked local generation proven never submitted can instead
use `WorkPackageClaim.Unsubmitted.retire_expired/6`. Decode its retained pinned
authorization with `ResponsibilityGraph.Persistence.decode_delegation_input/1`;
do not backdate or admit expired authorization. The trusted host operator must stop
the scoped claim writer, retain raw authorization and before-state hashes, and supply
generation-bound provider-claim/process absence with a retained evidence reference.
The candidate independently checks immutable grants, exact leases, a readable claim
journal with no current or later reservation, and an absent canonical local workspace
with plain ancestors. Observed, supervised or uncertain workers remain ineligible.
Persist both candidates under the existing exclusive writer/CAS contract, preserving
an immutable intent and partial-failure evidence. Repeating the same retirement is
idempotent. A split graph/fence write remains fail closed until coherent recovery.

Retirement expires the pair and releases only the never-submitted local lease. Its
receipt retains both grant fingerprints and exact generation, profile, repository,
workspace and worker identity. It neither invents a Git checkpoint nor satisfies
terminal Git cleanup, releases a provider reservation, changes usage, or authorizes
the old issue. A distinct current-manifest successor can pass the repository gate
only after the retained receipt, unchanged grants, journal absence and workspace
absence are reverified. Normal admission and same-issue generation fencing still apply.

When the canonical Linear issue is now terminal and an independently observed
provider projection has no claim, `WorkPackageClaim.Unsubmitted.retire_terminal/6`
can additionally close the never-submitted local generation. A trusted operator
must retain the exact Linear status/UUID, provider projection and claim absence,
process census, claim journal, immutable grant and fence snapshots, and absent
canonical workspace under one evidence reference. The API rechecks the local
journal, workspace, grant lifecycle and untouched lease before recording a
distinct `retired` fence with that reference. This has no accepted Git head or
synthetic cleanup receipt; `cleanup: cleaned` records that there was no local
checkout to remove. It does not terminate a worker, release provider capacity,
close a provider projection, or clear a separate failed cleanup hold. Persist
the graph and fence candidates together under the existing exclusive writer/CAS
recovery contract; a partial write is not admission authority. If an expired
grant pair was already retired under a prior evidence reference, retain that
original graph receipt and append the distinct terminal-local fence evidence.

To enable this managed runtime on a Linux runner, the host must provide the complete tuple below;
the service rejects a partial tuple during supervisor startup and leaves the adapter disabled when
all five values are absent. A host that declares `SYMPHONY_POOL_KEY` or
`SYMPHONY_REPOSITORY_REF` is treated as a managed pool and must provide the tuple:

```text
DAHLIA_WORK_PACKAGE_PROVIDER_URL=https://provider.example
DAHLIA_WORK_PACKAGE_RUNNER_TOKEN=<host-injected secret>
DAHLIA_WORK_PACKAGE_ATTESTATION_KEY=<host-injected secret>
DAHLIA_RUNNER_ID=<scoped runner identity>
DAHLIA_MANAGED_PROJECT_PROFILE_ID=<managed profile identity>

# Launcher identity (required for managed-pool readiness)
SYMPHONY_ACCEPTED_SOURCE_HEAD=<launcher-attested 40-character Git object ID>
```

For an explicitly confirmed pre-spawn recovery, configure both
`DAHLIA_WORK_PACKAGE_RECOVERY_DIRECTORY` (an existing absolute directory) and
`DAHLIA_WORK_PACKAGE_RECOVERY_PUBLIC_KEY` (the unpadded base64url 32-byte Ed25519 public key).
The host writes one immutable `<issue UUID>.json` envelope containing base64url `payload` and
`signature`; the signed payload uses `work-package-pre-spawn-recovery.v1`, the exact provider
claim and confirmation receipt, local generation maximum, original journal SHA-256 and
`neverSpawned: true`. The independent root signer must verify the provider response and retained
history while the runtime is durably stopped across restart. Keep the private key outside the
runtime and workers; publish receipts atomically into a root-owned directory.

Receipt adoption is read-only. The current generation must equal the retained local maximum,
all issue generations must have released, unobserved leases, all issue workspaces must be absent,
and the original journal bytes and provider tuple must match. A spawn marker or worker observation
blocks adoption. Existing manifest, owner and budget checks still apply; accountable restart
reconciliation uses the responsibility graph. Normal admission archives the previous generation
and increments it once. An envelope already passed by a newer generation does not override that
generation's claim/recovery checks. A reservation lookup that explicitly reports not-ready releases
only the current never-submitted local lease; old journal entries remain history. After restart,
a valid journal without that generation and an untouched, unsupervised worker lease allow the normal
recovery path to release exact responsibility before the fence and admit a higher generation.
Missing journals for active leases, submitted claims, observed workers and conflicting identity stay
held. The receipt cannot roll local authority back, and a missing fence never authorizes reuse of history.

An already released never-submitted generation can also yield to a different eligible issue.
Both the execution and responsibility admission gates require the same proof: a readable journal
with no same-or-newer claim, unchanged operator grants, and an absent local workspace whose
ancestors are plain directories. Recovery must leave the existing fence and graph unchanged;
missing journals, links, remote workspaces or partly released authority keep the repository held.
Retained old generations and grants remain history; they are not marked complete or cleaned.

`DAHLIA_WORK_PACKAGE_JOURNAL_PATH` and `DAHLIA_WORK_PACKAGE_ARCHIVE_ROOT` optionally select the
private reservation journal and archive root. The archive root must be outside active workspaces.

Managed token accounting uses `<effective-journal-path>.token-usage.jsonl`. Before starting a
managed pool, the host operator must explicitly call `SymphonyElixir.ManagedTokenBudget.initialize/3`
with that absolute plain path, the manifest's `pool_key`, `repository_ref` and
`managed_project_profile_id`, and at most 256 reviewed historical issue baselines. Each baseline contains
`issue_id` (stable UUID), `known_minimum_tokens`, positive `continuation_floor`, `evidence_ref` and
`authority_ref`. The known minimum covers only generations below that floor. A previously active
issue requires retained usage evidence; absence of a ledger is never evidence of zero consumption.
An empty baseline list initializes an idle pool without authorizing any issue. Initialization is
exclusive; there is no automatic bootstrap, rollover, reset, or allowance-renewal operation.

For a genuinely new canonical issue after initialization, stop the pool and independently verify
its native UUID and absence of prior execution, claims, responsibility, fences and workspaces.
Missing accounting alone is not evidence of zero use. The sole host writer may explicitly call
`ManagedTokenBudget.register_new_issue/2` with the freshly loaded ledger and exactly: lowercase
`issue_id`, `known_minimum_tokens: 0`, `continuation_floor: 1`, `evidence_ref`, `authority_ref`,
`ledger_prefix_sha256` (lowercase SHA256 of the current bytes) and `ledger_prefix_size_bytes`.
The append binds the complete prefix, retains every prior byte and usage total, seals bootstrap,
and preserves the 256-issue historical portfolio limit. Existing UUIDs, including case aliases, cannot be registered again.
Exact logical retries after reload or later usage append nothing; conflicting retries and duplicate
physical rows fail. Pending or blocked writes prevent registration. Deploy this decoder before
appending registration records; older runtimes reject them. Registration is bookkeeping, not an
execution grant, allowance renewal or automatic scheduler action. Current manifest, responsibility,
provider claim, generation, capacity and token limits still control admission.

`ManagedTokenBudget.retained_evidence/4` reads cumulative evidence without changing accounting.
Supply the absolute ledger path, exact ledger identity, stable issue UUID and independently
installed floor containing exactly `prefix_hash` (lowercase SHA256), positive `prefix_size`
and nonnegative `minimum_total`. Both the full snapshot and retained prefix must replay as
canonical ledgers with the same identity and registered issue. The retained total must meet
the floor, and current usage must not decrease. Pending/blocked accounting, changed snapshots,
partial prefix rows and mismatched evidence fail closed. The result reports current and minimum
prefix hashes/sizes and cumulative usage; it contains no grant or execution authority.
The caller must establish ledger provenance, hold actual coordination locks and keep the
source quiescent. Repeated integrity reads are not locks. A caller-selected weaker floor
does not authorize continuation: a privileged issuer must bind independently installed
configuration before any proof is trusted. This API does not implement that issuer, sign
proofs, renew allowances, consume dispositions or make retained workspaces reusable.

`RetainedGrantProof.TrustedFile.load/3` is an internal file-integrity prerequisite for
a privileged broker launcher. Only root-installed configuration may select its path,
expected SHA256 and limit (at most 262144 bytes). It requires canonical Linux paths,
root-owned non-writable ancestors, a root-owned regular single-link file, bounded
reads, an exact digest and unchanged file/ancestor metadata around the read. It
does not authorize a request or implement a signer. A privileged launcher must
use a separately pinned root-owned code copy; never load worker-owned SDK source
as root. Actual process/key separation and broker installation remain separate
qualification requirements.

`RetainedGrantProof.GrantEvidence.load/5` composes that pinned file loader with
the existing full `ManagedResponsibility.decode/3` validator. Installed bindings
provide the exact pool/repository/profile/runner context and stable issue UUID.
The result derives original manifest digest, authority reference, owner, scope,
pair identities and the minimum paired allowance/expiry from the validated bytes.
Its internal byte decoder is evidence preparation, not issuer authority. Current
revocation, claim/history/archive, cumulative ledger, native owner and profile
checks are still required before a privileged issuer or provider trusts a proof.

`RetainedGrantProof.GrantBudgetEvidence.load_config/3` joins the original grant
and cumulative ledger from digest-pinned root-owned configuration. Version 1
accepts only `schema_version` and `binding`; the binding contains exactly
`manifest_path`, `manifest_sha256`, `ledger_path`, `context`, `issue_id`,
`owner_id`, `accountable_id`, `responsible_id`, `scope`, `floor`,
`ledger_checkpoint_sha256` and `ledger_checkpoint_size`. It checks the exact
validated responsibility pair, owner, scope, routing context, retained floor
and full current ledger checkpoint, and denies exhausted or expired grants.
The launcher must supply the trusted clock and installed paths/digests; these
are never request overrides. A checkpoint is evidence, not another accounting
ledger or an allowance renewal. Actual locks, quiescence, revocation and
claim/history/archive/native checks remain mandatory before signing. The
collector returns inert facts and does not implement signing or admission.
Its prepared root configuration tests use `HGS600_BUDGET_FIXTURE` and explicitly
skip without that fixture; synthetic tests do not qualify an installed issuer.

For exact checkpoint validation, `ExecutionFence.Persistence.decode_bytes/1` and
`ResponsibilityGraph.Persistence.decode_bytes/1` reuse their existing snapshot
validators on authenticated immutable bytes. They do not look up paths or recover
another file. Successful decoding establishes snapshot structure, not freshness,
provenance, current authority or permission to act on a stale copy.

`RetainedGrantProof.SnapshotEvidence.decode/3` verifies independently installed
`fence_sha256` and `graph_sha256` checkpoints before invoking those exact-byte
validators. Each snapshot is bounded to 262144 bytes. It returns the validated
fence, responsibility graph and checkpoint hashes, with no path lookup or
recovery fallback. The launcher must read current original snapshots under the
actual quiescent locks; a stale authenticated copy is not live revocation or
claim evidence. Cross-snapshot issue/generation/grant/history/archive matching
and current native authority remain required before signing.

`SnapshotEvidence.match_retained/4` checks the decoded states against the full
original delegation pair and an installed retained-generation binding. It
requires exact immutable delegation fields, active unexpired authority, the
original terminal record, branch/worktree and responsible worker lease, and
matching issue/repository/generation. `ExecutionFence.retained_process_quiescence/3`
uses the existing termination and active-lease predicates without modifying the
cleanup/reuse predicate. It establishes only persisted process/lease consistency;
the privileged launcher must independently observe live OS quiescence. Pending
cleanup remains pending, and the ordinary exact-head cleanup guard still applies.
No consistency result releases a reservation or authorizes a successor.

`SnapshotEvidence.match_claim/5` validates bounded digest-pinned original claim
journal bytes with the existing `WorkPackageClaim.Journal.decode_bytes/1`.
Installed bindings select the exact original reservation, nonce, projection,
scope keys, runner, responsibility, fence token and runtime lease. They must
match the grant routing context and retained worker tuple at its generation.
The canonical generation-scoped journal key must exist, and duplicate entries
for that same issue/profile/repository/generation deny. Optional cleanup
receipts are returned as journal evidence only; their provider acknowledgement,
signature and immutable history require independent verification. No caller
may provide the checkpoint hash or expected claim as authority.

`RetainedGrantProof.NonceRequest.decode/1` bounds the broker request to exactly
43 canonical unpadded base64url bytes encoding one 32-byte nonce. It rejects
padding, aliases, extra frames, whitespace and authority-bearing maps. The
trusted transport must enforce bounded reads/timeouts and reject appended
frames. Paths, keys, facts, hashes, issue context and clocks remain installed
launcher inputs; nonce decoding neither proves freshness nor consumes replay.

`RetainedGrantProof.Envelope` is an internal Ed25519 primitive for the separately
pinned root launcher, not an SDK signing service. The wire purpose is
`hypergrid.retained-grant-evidence.v1`, version 1. Its separately installed issuer
fingerprint is lowercase hex SHA256 of the raw 32-byte public key, distinct from
the recovery SPKI fingerprint. The signer derives its public key from the installed
32-byte seed and checks that fingerprint before signing; it accepts no public-key
override. The signed bytes are the UTF-8 purpose plus NUL, raw public key, raw
32-byte nonce, unsigned big-endian 64-bit observed/expiry milliseconds, and SHA256
of the exact collected fact bytes. Facts are bounded to 262144 bytes, clocks to
JavaScript safe integers, and proof lifetime to at most 60000 milliseconds.
The exact wire fields are `version`, `purpose`, `issuer_fingerprint`, `nonce`,
`observed_at_ms`, `expires_at_ms`, `facts`, `signature`; binary fields use canonical
unpadded base64url. Verification requires independent installed public key and
fingerprint, the expected nonce and trusted clock. It authenticates bytes only:
grant expiry, current authority, native graph, replay consumption and disposition
remain separate checks. No key belongs in SDK/controller/worker environment;
actual root-owned service/process/key isolation and provider installation remain
unqualified. The module reads no files or environment and allocates no keys/grants.

`RetainedGrantProof.ChallengeEnvelope.verify/5` separately authenticates the
fixed `hypergrid.retained-grant-challenge.v1` purpose with an independently
installed raw public key and fingerprint, the expected 32-byte nonce and a
trusted clock. It uses the same eight-field envelope and signed-byte layout,
but cannot verify an evidence-purpose or recovery-purpose signature as a
challenge. It has no signing API and reads no files, environment or clock.
Returned facts remain opaque bounded bytes; typed provider snapshot validation
and original grant/path bindings are separate prerequisites. Re-verifying the
same valid unexpired challenge is read-only and succeeds without a replay store.

`ChallengeEnvelope.verify_with_digest/5` preserves `verify/5` and adds a lowercase
hex SHA256 digest of the exact authenticated signing message to its successful
result. It authenticates once before hashing, using the independently installed
public key, expected nonce, verified times and exact decoded fact bytes.
It does not hash a reserialized JSON envelope or normalize opaque facts.
The digest binds bytes; it does not establish current native authority.

`RetainedGrantProof.ReceiptEvidence.validate_stored_readonly/2` validates a
stored cleanup receipt against an independently validated exact claim map.
It checks strict field types, the existing semantic receipt identity, and the
provider acknowledgement, refusing unknown keys and duplicate wire/atom aliases
before normalization. A termination acknowledgement may release execution while
retaining the repository scope; repository-cleanup acknowledgement requires both
scope and reservation released. Failed and blocked terminal outcomes remain valid.
This pure check authenticates no signer and creates no grant, release or admission.
Its focused tests use an independently encoded receipt identity and cover valid
atom/camel acknowledgements plus malformed claims, receipt values and aliases.

`RetainedGrantProof.JournalCleanupEvidence.new/1` composes the independently
matched original claim journal with `ReceiptEvidence`. It requires acknowledged
termination, validates every optional repository cleanup receipt, and rejects
unknown outer kinds or a key/receipt-kind mismatch. Original acknowledgement
states and accepted heads remain inert facts; the journal digest is checked for
syntax only. No signature, current authority, scope release or admission is granted.
The constructor cannot authenticate snapshot provenance. Its trusted caller must
first use `SnapshotEvidence.match_claim/5` with independently pinned original
journal bytes. Optional repository cleanup permits observing a termination that
retains a held scope; its absence proves neither cleanup nor scope release.

The challenge must not choose local manifest, ledger, snapshot or key paths.
A later broker must collect those facts under actual local quiescent
coordination and bind the exact authenticated challenge signing-message digest
to its proof. The provider must reacquire its canonical observation/admission
locks and check current native ownership, revocation, claim, routing, receipt
acknowledgements and original grant/floor authority after proof verification.
Local observation intervals do not make guest filesystem and provider database
locks atomic. Fresh local revalidation before HGS-593 consumption remains
required. The combined facts schema, collectors, broker installation and
independent signing-key confinement remain incomplete; challenge verification
alone creates no grant, reservation, scope release, worker or disposition.

The focused challenge tests are
`test/symphony_elixir/retained_grant_proof/challenge_envelope_test.exs`.
They cover fresh opaque facts and repeated verification, distinct purposes,
key pinning, nonce substitution, malformed shapes, canonical encoding,
signature/fact corruption, input bounds and safe-integer freshness. Passing
these tests is source evidence, not full quality, installation or admission
acceptance.

`TrustedFile.load_seed/2` is an internal root-launcher prerequisite, never a
remote file interface. It requires exactly 32 bytes, root ownership, a single
regular-file link and exact mode 0400 or 0600 at both metadata snapshots. Both
public and seed reads also compare the opened descriptor's metadata before and
after the bounded read with the pinned pathname snapshot. The launcher must
never return seed bytes or accept a request-selected path/digest. Linux POSIX
ACL group masks are zero for those seed modes, excluding effective named-user
reads; a confidential seed positive still requires real root-runtime qualification.

A privileged launcher must use separately authenticated root-owned executables,
libraries and module search paths. The currently observed worker-owned mise
Elixir/Erlang installation is unsuitable for root execution. Copying it into a
root directory does not establish provenance. Historical TrustedFile root tests
through that runtime are preserved synthetic observations, not trusted-runtime
isolation evidence; current source tests explicitly execute as the worker UID.

The trusted-file OS tests require real prepared fixtures via
`HGS600_TRUSTED_FILE_FIXTURE` and `HGS600_ACL_FIXTURE`; full manifest-loader tests
use `HGS600_GRANT_FIXTURE`. Without those fixtures, the corresponding tests are
explicitly skipped. CI skips must not be reported as OS or privileged-broker
qualification. Linux POSIX ACL masks are reflected in group mode bits; tested
named-user effective writes fail the mode checks, including directory ancestors.
Other filesystem/ACL semantics and Windows deployment are not qualified here.

The historical portfolio capacity is shared by bootstrap, replay and registration.
It does not increase worker concurrency, per-issue token grants or the separate
20-entry managed delegation manifest limit. At 256 issues, registration still
fails closed; never reset the ledger to make room. Older runtimes with the
20-issue accounting bound cannot read a ledger after its twenty-first issue.

Generation floors are per issue, not per pool: an issue with no prior execution starts at 1.
To correct a mistaken floor for an unstarted issue, stop its managed service and retain the ledger,
fence, journal and independent no-worker/no-claim evidence. The sole host writer may explicitly call
`ManagedTokenBudget.correct_unstarted_floor/2` on a freshly loaded ledger. Supply `issue_id`, a unique
`correction_id`, the matching `previous_floor` greater than 1, `new_floor: 1`, the exact lowercase
`ledger_before_sha256`, `evidence_ref` and `authority_ref`. The issue must have zero historical minimum,
zero cumulative usage and no observed thread or highwater, including zero-token observations.
The operation appends one correction, preserves original bootstrap bytes and the original baseline,
and changes only the replayed effective floor. Exact retries are idempotent; conflicting or repeated
corrections fail. The scheduler never invokes it. This uses the existing single-host-writer append
contract; it is not concurrent-writer locking, credential authorization or an allowance increase.

The scheduler loads known issue totals before startup maintenance. A finite managed grant has an
effective cumulative ceiling of the smaller of positive `codex.max_total_tokens` and its positive
`budget.max_tokens`. An explicitly authorized Luna `progress_scoped` grant instead requires
`budget.max_tokens: null` and has no per-task token-count stop. Cumulative usage is still recorded
monotonically across retries and restarts. Running work must still match its active graph grant and
exact bound lease; missing or changed authority latches accounting and stops the worker. The
configured value remains positive for managed workflows and continues to constrain finite grants.
Expiry, scope, model, effort, provider/cash/capacity limits, and no-progress/time-stall checks remain
independent controls. Unmanaged zero retains the optional cap's disabled behavior.
Each execution starts a new actual Codex thread. Duplicate or lower cumulative observations add
zero, later turns keep the same thread highwater, and later generations accumulate additional usage.
Overshoots are retained. Managed claim release never deletes totals. If a stopped worker has queued
usage, the scheduler drains only its exact execution token/session before releasing ownership.

The single host writer verifies the full file before observations, writes a flushed `.pending`
intent, appends and flushes usage, verifies resulting bytes, then retires the intent. Any uncertainty
latches admission and attempts to retain a `.blocked` marker; both markers prevent cold startup.
Preserve and explicitly reconcile these artifacts together with claim/fence and original protocol
evidence. A storage outage may itself prevent writing the hold marker, so retained execution claims
still require recovery rather than automatic respawn. File flushes and process-restart replay are
covered; filesystem power-loss durability and unreported external spend are not claimed.

Archives now use version 2: regular file bytes and empty directories are copied, while junctions
and symbolic links remain evidence-bound path/target metadata. Verification never recreates or
follows those links. Version 1 archives retain their original verification and evidence references.
See [workspace recovery archives](docs/workspace-recovery-archives.md) for recovery and retry rules.
The `/api/v1/state` response reports the effective `SYMPHONY_POOL_KEY`,
`SYMPHONY_REPOSITORY_REF`, workflow workspace root, `SYMPHONY_GLOBAL_PAUSE_FILE`, and the accepted
source head under `runtime_identity`. The launcher must provide `SYMPHONY_CURRENT_SOURCE_HEAD` as
the matching 40-character observed revision from its executable attestation; an absent or malformed
observation leaves the source unverified and makes managed-pool readiness fail, while a mismatch
marks the identity stale.
The same response reports `execution_authority.fence` (`hgs294`) and `execution_authority.delegation`
(`hgs300`) together with their posture derived from the durable fence and responsibility graph.
No provider or attestation credential is included in this projection.
Before Codex protocol initialization, the port-owning worker waits up to five seconds for its
exact systemd scope to become active with a nonempty cgroup. Each systemctl probe has an enforced
deadline using GNU `timeout`; a missing timeout executable fails closed. Early process exit,
pause, missing containment, or excess startup output also fail closed. Output remains ordered
in the port mailbox; failure diagnostics contain bounded counts and hashes, never raw output.
The orchestrator independently rechecks the live identity and durably records it before the
worker sends protocol requests. A failed startup with unknown termination retains its fence
and requires supported reconciliation; it does not authorize another dispatch attempt.

The local Linux path uses the systemd user supervisor and records the claim, process-tree proof,
and archive evidence before releasing provider capacity. Remote SSH workers remain held when this
proof or the independent archive verifier is unavailable. These provider credentials are scrubbed
from the Codex child environment.

### Explicit managed responsibility

For an enforced managed Linux pool, the COO/runtime owner must install a non-secret, root-owned manifest
and set `DAHLIA_MANAGED_DELEGATION_PATH`, `DAHLIA_MANAGED_DELEGATION_SHA256`,
`DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519`, and
`DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519` in the trusted
service environment. The file must be regular, at most 256 KiB, not group/world writable, with no
symlink ancestors. Its SHA-256 must match the pinned lowercase digest. The exact
raw bytes must also verify under the domain-separated Ed25519 signature and
host-pinned public key. Keep the signing private key off the runner guest and
separate from the claim-witness key. Partial or invalid
configuration fails startup. A declared managed pool also rejects both settings being absent.
An empty manifest explicitly authorizes no work; only undeclared legacy operation may omit it.
The runtime never grants authority from an issue's prose or labels.

The manifest is bounded operator configuration, not another queue. It binds one pool, repository
and managed profile to at most 20 exact native issues, each with an accountable owner and a bounded
responsible child. The ordinary fresh-issue admission path checks owner, identity, expiry,
repository ownership, previous execution cleanup and the selected model/effort/token ceiling.
The graph remains the only delegation ledger, and provider reservation/claim remains mandatory.
No workspace or worker is created by loading the manifest.

Nonempty managed grants require signed manifest v2 assignment context. It binds objective ID and
content, the fixed `refs/remotes/origin/main` base, and an allowlisted `linux-x86_64` target with
`repository` classification and explicit constraints. Admission compares the signed objective
snapshot with the freshly fetched issue title and optional description. Empty signed v1 manifests
remain loadable; nonempty v1 grants fail closed. The signature domain is selected by schema version
(`hypergrid.symphony.managed-delegation.v1` or `.v2`, each followed by a zero byte before the exact
manifest bytes). The manifest signing script requires Python 3 to select that version safely.

The current Dahlia/Grid grant issuer and its rollout still need to emit and sign v2 nonempty entries.
Keep managed admission paused until the issuer change is reviewed and the signed manifest is installed.

Nonempty v2 assignment contexts must include signed placement fields matching HGS-728 policy:
`internal_beta` targets `rke2`, and `hosted_production` targets `lke`. Older nonempty v2 grants without
those fields fail closed and must be reissued by the cross-repository issuer. The source-only managed
executor also defines an assignment-bound JIT credential lease port with acquire, renew and revoke
callbacks; it does not implement a broker or issue credentials. Admission stays paused until the
issuer and runtime integrations are separately reviewed and qualified.
This source change does not qualify production host admission or workload execution.

The HGS-729 RKE2 provider is a separate source-only port. Callers must pass an assignment
bundle built from an upstream signature-verified manifest entry. The provider validates
the canonical bundle and `internal_beta`/`rke2` placement; it does not verify the manifest
signature itself. It compiles the bundle into a deterministic Job using a digest-pinned
image and trusted namespace supplied by the caller. The Job has fixed execution parameters,
an ephemeral workspace, restricted container security, bounded resources, and a deadline.
The fakeable client contract supports create/get/delete reconciliation; a same-name Job
with a different assignment or spec is held, and deletion requires an exact identity and
server UID. No live Kubernetes client, cluster configuration, credentials, Pod, or spawn
path is provided or qualified by this source slice. See the
[managed responsibility contract](../docs/responsibility-delegation.md#managed-assignment-bundle).

Pause the existing global gate before replacing the manifest or its digest, then restart the
supervised pool and verify its readiness before resuming. This is explicit runtime-owner
configuration, not a worker-editable authorization file. A restart never silently revives a
previously bound, blocked, revoked or terminal delegation. See the
[manifest contract](../docs/responsibility-delegation.md#managed-authorization-manifest).

Real root-owned file, managed startup and HTTP readiness qualification runs with
`SYMPHONY_TEST_ROOT_MANIFEST_FILES=1 mix test` in a qualified Linux fixture as root. Ordinary
unprivileged test runs skip those seven privileged cases; the pure input, admission, claim,
restart and failure tests still run. This switch affects tests only and does not relax runtime
file ownership or digest checks.

The `WORKFLOW.md` file uses YAML front matter for configuration, plus a Markdown body used as the
Codex session prompt.

Minimal example:

```md
---
tracker:
  kind: linear
  provider:
    project_slug: "..."
workspace:
  root: ~/code/workspaces
hooks:
  after_create: |
    git clone git@github.com:your-org/your-repo.git .
agent:
  max_concurrent_agents: 10
  max_turns: 20
codex:
  command: codex app-server
---

You are working on an issue from the configured tracker {{ issue.identifier }}.

Title: {{ issue.title }} Body: {{ issue.description }}
```

Notes:

- If a value is missing, defaults are used.
- `tracker.kind` selects an adapter. Adapter-owned endpoint, scope, and auth settings belong under
  `tracker.provider`; the current Linear adapter still accepts the older flat `endpoint`,
  `api_key`, `project_slug`, and `assignee` aliases for compatibility.
- `tracker.required_labels` is optional. When set, an issue must have every
  configured label to dispatch or continue running. Label matching ignores
  case and surrounding whitespace. A blank configured label matches no issue.
- Safer Codex defaults are used when policy fields are omitted:
  - `codex.approval_policy` defaults to `{"reject":{"sandbox_approval":true,"rules":true,"mcp_elicitations":true}}`
  - `codex.thread_sandbox` defaults to `workspace-write`
  - `codex.turn_sandbox_policy` defaults to a `workspaceWrite` policy rooted at the current issue workspace
- `codex.turn_timeout_ms` is the maximum silence interval while a turn is streaming. Each
  app-server update resets it; it is not a total turn runtime cap.
- Supported `codex.approval_policy` values depend on the targeted Codex app-server version. In the current local Codex schema, string values include `untrusted`, `on-failure`, `on-request`, and `never`, and object-form `reject` is also supported.
- Supported `codex.thread_sandbox` values: `read-only`, `workspace-write`, `danger-full-access`.
- When `codex.turn_sandbox_policy` is set explicitly, Symphony passes the map through to Codex
  unchanged. Compatibility then depends on the targeted Codex app-server version rather than local
  Symphony validation.
- Workflows that run package managers or other commands that resolve external hosts should set
  `networkAccess: true` in `codex.turn_sandbox_policy`; otherwise DNS/network access may be denied
  by the Codex turn sandbox.
- `agent.max_turns` caps how many back-to-back Codex turns Symphony will run in a single agent
  invocation when a turn completes normally but the issue is still in an active state. Default: `20`.
- If the Markdown body is blank, Symphony uses a default prompt template that includes the issue
  identifier, title, and body.
- Use `hooks.after_create` to bootstrap a fresh workspace. For a Git-backed repo, you can run
  `git clone ... .` there, along with any other setup commands you need.
- If a hook needs `mise exec` inside a freshly cloned workspace, trust the repo config and fetch
  the project dependencies in `hooks.after_create` before invoking `mise` later from other hooks.
- For the Linear adapter, `tracker.provider.api_key` reads from `LINEAR_API_KEY` when unset or
  when value is `$LINEAR_API_KEY`. The legacy flat `tracker.api_key` alias behaves the same way.
- Do not put a literal tracker token in a repo-owned `WORKFLOW.md` if Codex can read that
  workspace. Use `$VAR`/host-side secret references so Symphony can keep the token out of the
  child environment.
- For path values, `~` is expanded to the home directory.
- For env-backed path values, use `$VAR`. `workspace.root` resolves `$VAR` before path handling,
  while `codex.command` stays a shell command string and any `$VAR` expansion there happens in the
  launched shell.

```yaml
tracker:
  provider:
    api_key: $LINEAR_API_KEY
workspace:
  root: $SYMPHONY_WORKSPACE_ROOT
hooks:
  after_create: |
    git clone --depth 1 "$SOURCE_REPO_URL" .
codex:
  command: "$CODEX_BIN --config 'model=\"gpt-5.5\"' app-server"
```

- If `WORKFLOW.md` is missing or has invalid YAML at startup, Symphony does not boot.
- If a later reload fails, Symphony keeps running with the last known good workflow and logs the
  reload error until the file is fixed.
- `server.port` or CLI `--port` enables the optional Phoenix LiveView dashboard and JSON API at
  `/`, `/api/v1/state`, `/api/v1/<issue_identifier>`, and `/api/v1/refresh`.

### Linear adapter profile

- Config: use `tracker.kind: linear` with `tracker.provider.endpoint` (default
  `https://api.linear.app/graphql`), `api_key` (defaults to `LINEAR_API_KEY` and accepts
  `$VAR`), required `project_slug`, and optional `assignee` (a Linear user ID or `me`,
  defaulting to `LINEAR_ASSIGNEE`).
  The legacy flat `tracker.endpoint`, `api_key`, `project_slug`, and `assignee` aliases remain
  supported. `required_labels`, `active_states`, and `terminal_states` stay under `tracker`.
- Scope and paging: candidate reads filter the configured project slug and requested state names,
  following Linear pages of 50. ID refreshes are also project-scoped and batch up to 50 IDs. Empty
  state/ID lists return `{:ok, []}` without a Linear request.
- Rate limiting: host-local Symphony processes share a provider rate-limit state file and lock.
  HTTP `Retry-After` headers and Linear GraphQL rate-limit bodies both persist a bounded cooldown;
  a one-hour provider window therefore fails closed locally instead of being retried against the
  exhausted API bucket.
  Managed Linux requires the util-linux `flock` executable for this lock. Hosts without that
  kernel primitive, including Windows hosts, fail closed rather than using an unverified stale-lock
  fallback.
- Identity and normalization: `issue.id` is the Linear issue ID and `issue.native_ref` is currently
  `nil`. Records missing a nonblank ID, identifier, title, or state are dropped from candidate
  pages and fail ID refreshes. State keeps Linear's spelling; integer priorities are preserved and
  other priority values become `nil`; RFC 3339 timestamps are parsed and unusable timestamps become
  `nil`. Labels are trimmed, lowercased, deduplicated, and blanks are dropped; blockers come from
  inverse `blocks` relations.
- Dispatchability: the adapter marks an issue dispatchable only when optional assignee routing
  matches and a `Todo` issue has no non-terminal blocker. The generic scheduler then applies
  active/terminal states, required labels, claims, retries, and concurrency.
- Tool: the Linear adapter advertises `linear_graphql`, accepting either a raw query string or an
  object with nonblank `query` and optional object `variables`. Symphony executes it host-side
  with the session-bound endpoint/token and strips declared token environment variables from the
  Codex child. `project_slug` scopes scheduler reads, not raw tool calls; the tool can access
  whatever the configured Linear token can access.
- Responsibility and errors: `linear_graphql` adds no idempotency key, retry, scope guard, or
  rate-limit policy, so workflows own idempotent mutations and handling provider errors. Read/config
  failures use `{:error, :missing_linear_api_token}`, `{:error, :missing_linear_project_slug}`,
  `{:error, :invalid_linear_endpoint}`, `{:error, :invalid_linear_assignee}`,
  `{:error, :missing_linear_viewer_identity}`, `{:error, {:linear_api_status, status}}`,
  `{:error, {:linear_api_request, reason}}`, `{:error, {:linear_graphql_errors, errors}}`,
  `{:error, :linear_unknown_payload}`, or `{:error, :linear_missing_end_cursor}`. Tool results
  are maps with `"success"`, JSON-string `"output"`, and text `"contentItems"`; invalid
  arguments, missing auth, and transport failures return `"success" => false` with
  `{"error": {"message": ...}}`, while top-level GraphQL errors preserve the response body with
  `"success" => false`.
  For portable reporting, map missing/invalid token, project, endpoint, assignee, or viewer errors
  to `tracker_config` or `tracker_auth`, request failures to `tracker_transport`, non-200 responses to
  `tracker_response` (`429` is `tracker_rate_limited`), GraphQL/unknown payload failures to
  `tracker_payload`, and missing cursors to `tracker_pagination`; logs and tool responses carry the
  human-readable provider detail.

### GitHub Issues adapter

- Config: use `tracker.kind: github` with required `tracker.provider.repo` in `owner/repo` form,
  optional `token` (defaults to `GITHUB_TOKEN` and accepts `$VAR`), and optional `api_url`
  (default `https://api.github.com`, HTTPS only). Set explicit `active_states` and
  `terminal_states`; active entries may be `open` and terminal entries may be `closed`.
- Reads and identity: polling is scoped to the configured repository; `issue.id` is the
  repository issue number, `issue.identifier` is `GH-<number>`, hidden or deleted `404` issues are
  omitted on refresh, and pull requests returned by the Issues API are not dispatchable.
- Tool and auth: `github_api` accepts a relative REST `path` plus optional `params` and JSON
  `body`; Symphony executes it host-side with the session-bound token, removes configured tracker
  credentials and provider authentication aliases from the Codex child, and leaves raw tool access
  limited by that token's GitHub permissions.

### Jira Cloud adapter

- Config: use `tracker.kind: jira` with provider `base_url`, `email`, `api_token`, and required
  `project_key`; the first three default to `JIRA_BASE_URL`, `JIRA_EMAIL`, and `JIRA_API_TOKEN`
  and accept `$VAR`. Set explicit Jira-native `active_states` and `terminal_states`.
- Issues and reads: candidate reads and ID refreshes stay scoped to the configured project and
  requested statuses; `issue.id` is Jira's immutable ID and `issue.identifier` is the issue key.
- Blockers: inward `Blocks` links populate `blocked_by`; issues in Jira's `new` status category
  wait until blockers reach configured terminal states, while in-progress categories keep running.
- Tool: `jira_rest` sends relative `/rest/api/3/` requests host-side with configured Basic auth,
  strips token environment variables from Codex, and can reach whatever the Jira credential can.

### Asana adapter

- Config: use `tracker.kind: asana` with required `tracker.provider.project_gid`, optional
  `endpoint` (default `https://app.asana.com/api/1.0`), and `api_key` (defaults to `ASANA_PAT` and
  accepts `$VAR`); `active_states` and `terminal_states` are project section names.
- Scope: Symphony polls tasks in the configured project, treats their section as state, and omits
  deleted or out-of-project tasks during ID refreshes.
- Tool: `asana_api` sends relative Asana REST requests host-side with the configured auth; Symphony
  strips `ASANA_PAT` and configured token variables from the Codex child, while raw tool calls are
  not limited to the configured project.

### GitLab adapter

- Configure `tracker.kind: gitlab` with `tracker.provider.project_path`, optional `api_url`, and
  `api_key` (default `GITLAB_PAT`); use `opened` and `closed` tracker states.
- Symphony reads project issues by IID and exposes route-safe `GL-<iid>` identifiers.
- `gitlab_api` forwards raw GitLab REST requests with host-side auth and keeps configured tracker
  credentials and provider authentication aliases out of the Codex child.

## Web dashboard

The observability UI now runs on a minimal Phoenix stack:

- LiveView for the dashboard at `/`
- JSON API for operational debugging under `/api/v1/*`
- Bandit as the HTTP server
- Phoenix dependency static assets for the LiveView client bootstrap
- Tracker issue identifiers link to the tracker-provided URL when it uses `http` or `https`

## Project Layout

- `lib/`: application code and Mix tasks
- `test/`: ExUnit coverage for runtime behavior
- `WORKFLOW.md`: in-repo workflow contract used by local runs
- `../.codex/`: repository-local Codex skills and setup helpers

## Testing

```bash
make all
```

The quality gate keeps a pinned warning baseline from main commit
`b2ca242e99c5f8c4fc3e474b298c57fc9004fd7d` in
`config/quality-baseline.json`. `mix lint` still enforces public specs and rejects
new or increased Credo warning identities; `make dialyzer` rejects new or
increased Dialyzer warnings. Line-number shifts do not count as new warnings,
but duplicate counts do. Fixing old warnings reduces the current count without
changing the recorded baseline. The coverage floor is 85.95%, the measured
Linux baseline; a lower result fails `make all`. The baseline is not permission
to introduce new lint or type debt, and changing it requires a separately
reviewed quality-policy change.

Run the real external end-to-end test only when you want Symphony to create disposable Linear
resources and launch a real `codex app-server` session:

```bash
cd elixir
export LINEAR_API_KEY=...
make e2e
```

Optional environment variables:

- `SYMPHONY_LIVE_LINEAR_TEAM_KEY` defaults to `SYME2E`
- `SYMPHONY_LIVE_SSH_WORKER_HOSTS` uses those SSH hosts when set, as a comma-separated list

`make e2e` runs two live scenarios:
- one with a local worker
- one with SSH workers

If `SYMPHONY_LIVE_SSH_WORKER_HOSTS` is unset, the SSH scenario uses `docker compose` to start two
disposable SSH workers on `localhost:<port>`. The live test generates a temporary SSH keypair,
mounts the host `~/.codex/auth.json` into each worker, verifies that Symphony can talk to them
over real SSH, then runs the same orchestration flow against those worker addresses. This keeps
the transport representative without depending on long-lived external machines.

Set `SYMPHONY_LIVE_SSH_WORKER_HOSTS` if you want `make e2e` to target real SSH hosts instead.

The live test creates a temporary Linear project and issue, writes a temporary `WORKFLOW.md`, runs
a real agent turn, verifies the workspace side effect, requires Codex to comment on and close the
Linear issue, then marks the project completed so the run remains visible in Linear.

Run the opt-in GitHub Issues live test with a disposable/scratch repository:

```bash
cd elixir
export SYMPHONY_LIVE_GITHUB_REPO=owner/scratch-repo
export GITHUB_TOKEN=...
SYMPHONY_RUN_GITHUB_LIVE_E2E=1 mix test test/symphony_elixir/github_live_e2e_test.exs
```

Run the opt-in Jira Cloud live test against a disposable project whose credential can browse,
create, comment on, transition, and delete issues:

```bash
cd elixir
export JIRA_BASE_URL=https://your-site.atlassian.net
export JIRA_EMAIL=...
export JIRA_API_TOKEN=...
export SYMPHONY_LIVE_JIRA_PROJECT_KEY=TEST
SYMPHONY_RUN_JIRA_LIVE_E2E=1 mix test test/symphony_elixir/jira_live_e2e_test.exs
```

Run the opt-in Asana live E2E against disposable Asana resources:

```bash
cd elixir
export ASANA_PAT=...
export SYMPHONY_LIVE_ASANA_WORKSPACE_GID=...
# Required only when the workspace is an organization:
# export SYMPHONY_LIVE_ASANA_TEAM_GID=...
SYMPHONY_RUN_ASANA_LIVE_E2E=1 mix test test/symphony_elixir/asana_live_e2e_test.exs
```

Run the opt-in GitLab live E2E against a disposable project:

```bash
cd elixir
export GITLAB_PAT=...
export SYMPHONY_LIVE_GITLAB_PROJECT_ID=...
SYMPHONY_RUN_GITLAB_LIVE_E2E=1 mix test test/symphony_elixir/gitlab_live_e2e_test.exs
```

## FAQ

### Why Elixir?

Elixir is built on Erlang/BEAM/OTP, which is great for supervising long-running processes. It has an
active ecosystem of tools and libraries. It also supports hot code reloading without stopping
actively running subagents, which is very useful during development.

### What's the easiest way to set this up for my own codebase?

Launch `codex` in your repo, give it the URL to the Symphony repo, and ask it to set things up for
you.

## License

This project is licensed under the [Apache License 2.0](../LICENSE).

The existing observability state and per-issue APIs include `checkout_progress`
after the first accepted managed checkout observation. Its current HEAD and
sequence can advance without credit. The nullable `committed` witness records
the last accepted nonempty descendant commit, its sequence, exact managed
generation/session, current-known token baseline and acceptance time. Later
empty commits or delayed usage cannot relabel that credited OID. This is a
projection of the live accepted checkpoint, not additional accounting credit or
terminal acceptance; an unobserved checkout has no progress field.
