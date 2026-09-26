# Responsibility and delegation contract

`SymphonyElixir.ResponsibilityGraph` is the organisation-level authority layer
above the execution generation and session fence described in
`docs/execution-fencing.md`.

Each delegation names one role (`accountable`, `responsible`, `reviewer`,
`consulted`, or `observer`), a complete company/objective/initiative/project/
work-package/issue scope, bounded repository/path/environment/action authority,
a model/effort/token/child budget, an expected deliverable and evidence return
contract, and an expiry. There may be only one active accountable delegation
for an exact scope. Active responsible delegations may run in parallel only
when their scopes are deterministically disjoint.

Child delegations inherit and narrow their parent scope, capabilities,
environments, model, effort, token budget, and child budget. A parent
responsible delegation loses mutation authority over an overlapping child
scope while that child is active. Reviewers are read-only; a finding is a
proposal that a manager accepts through a new remediation delegation.

The `runtime_lease` field is a reference to an HGS-294 issue-execution lease,
not another process registry. Mutable authorization uses
`ResponsibilityGraph.authorize_with_execution_fence/4`, which requires both
the graph capability and the current generation's active worker lease. Revoking
or terminalizing a delegation returns the affected lease references and
`revoke_and_fence/6` fences those generations through HGS-294 before the caller
performs cleanup.

The graph starts in `manual` mode while the bootstrap is still coordinated by
the bounded prompt convention. After the graph has been reviewed and proven,
the orchestrator can persist `enforced` mode through
`Orchestrator.activate_responsibility_graph/1`. In enforced mode, normal worker
admission must find exactly one active responsible delegation for the issue and
bind its HGS-294 runtime lease before the worker starts. The worker's side-effect
guard then authorizes through both contracts; an active worker exit releases the
binding so a bounded retry can receive a fresh generation.

The running escript exposes that transition through the local
`--activate-responsibility-graph` boot switch. The switch is opt-in and is accepted
only when `SYMPHONY_POOL_KEY` or `SYMPHONY_REPOSITORY_REF` declares a managed pool,
the complete work-package runtime tuple is present, and
`SYMPHONY_GLOBAL_PAUSE_FILE` contains the exact operator state `paused`. It invokes
the existing orchestrator transition after startup, so the graph snapshot is
validated and persisted by its normal owner. A repeated boot against an already
enforced snapshot is idempotent. This is a local control reserved to the current
COO or an explicitly delegated runtime owner; there is no activation HTTP endpoint
and the launcher does not enable the switch automatically.

The orchestrator persists the graph as a versioned, sanitized JSON snapshot at
`responsibility-graph.json` beside the execution-fence snapshot. On restart,
delegations already bound to a runtime lease are blocked until that lease is
explicitly reconciled. Unbound responsible delegations remain active because
no worker has owned them yet; first admission binds their fresh HGS-294
generation. The orchestrator exposes only a read-only graph projection for
operations surfaces; delegation mutations remain explicit API calls with
typed validation.

## Managed authorization manifest

An enforced managed Linux pool must load explicit COO/runtime-owner authorization through
`DAHLIA_MANAGED_DELEGATION_PATH`, `DAHLIA_MANAGED_DELEGATION_SHA256`,
`DAHLIA_MANAGED_DELEGATION_SIGNATURE_ED25519`, and
`DAHLIA_MANAGED_DELEGATION_PUBLIC_KEY_ED25519`. The latter two are lowercase
hex for a detached 64-byte Ed25519 signature and its 32-byte trusted public key.
The signer signs the exact raw file bytes prefixed by the schema-matched domain
`hypergrid.symphony.managed-delegation.v1` or
`hypergrid.symphony.managed-delegation.v2`, followed by one zero byte. The trusted service
environment pins the exact bytes of a root-owned regular file and the verifying
key; links, writable-by-others files, oversized input, partial configuration,
digest or signature mismatches fail closed. Keep the signing private key off the
runner guest and separate from the HGS-719 claim-witness key. The file contains
authorization inputs, never runtime leases, statuses, clock history or graph events.

The JSON object has `schema_version`, `pool_key`, `repository_ref`,
`managed_project_profile_id`, a nonempty `authority_ref`, and at most 20 `entries`.
An empty signed v1 or v2 list explicitly authorizes no work and remains loadable
while managed admission is paused. Nonempty v1 grants lack the assignment snapshot
and fail closed at admission. A declared managed pool cannot omit manifest
settings to select legacy admission; only an undeclared legacy runtime may omit them.
Each entry contains `issue_id` (UUID), `identifier`, `owner_id`, and `accountable`/`responsible`
delegation inputs using the existing graph field names. The accountable actor is the current
native issue owner; the responsible actor must match the configured `DAHLIA_RUNNER_ID`. Scope identifies
company, objective, initiative, project, work package, issue and repository without wildcards.
Paths are explicit repository-relative paths, with `.` allowed for the fresh issue repository;
environment is `repository`. Both grants use `routine_engineering` authority, future expiry,
bounded model/effort/token budgets and a deliverable/evidence return contract. The responsible
child cannot delegate children. This is authority metadata, not a claim of OS sandbox isolation.
Each nonempty v2 entry additionally signs an `assignment_context` with an
`objective` `{id, content}`, `base_ref`, and `environment` `{platform,
classification, constraints}`. The objective ID must equal the delegation
scope's objective ID. Content is exactly the freshly fetched native issue title,
optionally followed by a blank line and its nonblank description; admission
rejects drift. `base_ref` is fixed to `refs/remotes/origin/main`. This slice
allows target platform `linux-x86_64` and requires classification `repository`
plus at least one nonempty constraint. The assignment bundle context comes from
this decoded signed entry, never ambient runtime or prompt fields.

The exact v2 assignment context also requires signed `placement` and
`target_environment` fields. `internal_beta` is valid only with `rke2`, and
`hosted_production` is valid only with `lke`, matching HGS-728 placement policy.
The normalized values are included in assignment bundle schema v2 and its
canonical digest. Existing nonempty v2 grants that omit these fields or name an
invalid pair fail closed and must be reissued by the signed-manifest issuer;
empty signed v1 manifests remain compatible. This source contract does not
provision either target or qualify its runtime.

To issue or rotate a grant, keep an Ed25519 PEM private key on a trusted root
signing host outside the runner guest, owned by root with mode `0600`. Freeze the
manifest's exact bytes, copy that non-secret file to the signing host, and run
[`scripts/sign-managed-delegation.sh`](../scripts/sign-managed-delegation.sh)
with the private-key and manifest paths on Linux with Bash, GNU coreutils,
Python 3, OpenSSL Ed25519 support, and `/dev/shm` available. It selects the
signature domain from `schema_version` and rejects unsupported versions. The tool prints the file digest,
detached signature, and raw public key as the three service settings above;
it does not print private key material. Install the same manifest bytes as a
root-owned regular file and update all three settings together in the trusted
service environment while the admission gate is paused and the pool stopped.
Read back the installed file digest and runtime identity's signer-key fingerprint
after restart. Retain the prior manifest and settings for rollback. The signing
host must keep the private key recoverable; a public key or signature alone
cannot issue the next grant. Sign an empty manifest under the same protocol.

Loading validates each intended pair without making it active. A fresh native candidate must
match its exact UUID, identifier and assignee before the orchestrator constructs a graph
candidate. Another active or blocked responsible owner in the same repository prevents it.
Every other execution in that repository must have a terminal, quiescent, validated cleanup
record. Only the normal admission owner persists the selected pair with its bound runtime lease;
provider reservation and claim still precede spawn. Exact active replay is idempotent; a partial,
changed, expired, blocked, revoked or terminal pair cannot be silently recreated.
The provider reservation's projection must match the authorized work-package ID on first claim
and journal replay; issue identity alone cannot substitute another work package.

The existing fence and graph are separate persisted files. A write failure leaves the admission
failed closed; a bound grant is blocked on restart until explicit existing lease reconciliation.
This manifest path does not claim distributed atomicity or erase unresolved crash evidence.
Pause before an operator changes authorization configuration, update its pinned digest and
restart the pool. Do not repair missing authority by editing live graph JSON or changing to
manual enforcement. Completed graph responsibility alone is not cleanup acceptance.

## Managed assignment bundle

Managed worker spawn binds the objective ID, identity and content, repository, explicit base ref,
prepared branch, responsible seat, current execution lease, delegation ancestry, acceptance
contract, secret environment variable names, target platform constraints, and signed placement / target
environment into a canonical SHA-256 assignment bundle. Secret values are never included. The worker
validates the bundle before workspace creation.

The runtime carries the decoded, signature-verified manifest entry, and admission
compares its objective snapshot with the canonical issue before graph writes.
Managed spawn reuses that signed entry to build the assignment bundle. This is a
source slice only: the cross-repository manifest issuer must emit and sign v2
nonempty entries before such grants can run. Keep the managed gate paused until
that issuer and its rollout are reviewed. No runtime admission, production host,
or workload execution is qualified by this change.

## Managed executor lifecycle contract

`SymphonyElixir.ManagedExecutor` defines a source-only lifecycle over explicit
adapter and durable-journal behaviors. Its allocation, checkout, execution,
result-publication and cleanup calls all carry stable keys derived from the
validated assignment digest. The executor now requires the result of
`WorkPackageClaim.claim/2` before any
allocation, checks its reservation, attestation and provider response against
the assignment, and records the stable non-secret claim identity in journal v6.
Resume requires that same identity; a fresh attestation timestamp and signature
for the same retained reservation do not change it. Unbound v5 journals cannot start or resume
allocation; valid v5 terminal evidence remains readable. This is a guarded
source API, not a production Orchestrator connection or an independent proof.
The provider and root witness remain responsible for validating company,
workspace, profile and reservation scope; a caller-created map is not claim
authority. A separate fresh journal is not a one-time-use fence, so production
must retain the provider claim and managed journal across crashes.
Checkout intent is projected only from the signed assignment's repository,
base ref and branch; an adapter receipt that differs is reported as a blocked
pre-execution result and enters journaled cleanup debt. The
result has a stable idempotency key and no accepted head. Uncertain result
publication retries with that key before cleanup starts. Cleanup retry remains
bound to the exact allocation and assignment; it never repeats checkout or
execution. A successful adapter cleanup call returns a blocked outcome while
the journal stays nonterminal in `abort_cleanup_pending`. HGS-350's
`work-package-cleanup-receipt.v1` requires an accepted head from a terminal
execution and cannot attest cleanup before checkout. No second signed receipt
contract is implemented in Symphony; Dahlia's distinct pre-execution abort
receipt and independent verifier proof require a trusted signer and host cleanup
observations before release. `WorkPackageClaim.HostWitness.record_abort/3` can
submit their proof, receipt, assignment, allocation and blocked-result references
with the exact retained claim tuple. The root witness records provenance only;
the managed executor does not submit that tuple to the witness or verifier. An abort
therefore remains held. Checkout heads must be canonical 40- or 64-character hexadecimal Git
object identifiers. The adapter return shapes are deliberately narrow so
workspace paths, command strings, credential values and raw process output are
not lifecycle fields.

Before execution, the typed adapter acquires a JIT credential lease bound to the
assignment digest and allocation, renews it using a stable key, and refuses to
execute when the lease is denied or expired. Lease responses contain only the
assignment digest, allocation ID and expiry; the executor derives the journaled
lease handle from the assignment digest and acquire request key. Adapter-supplied
lease references and credential material are never journaled. Malformed or
uncertain responses are quarantined by the stable acquire/renew request key;
replay retries revoke by request before abort. Cleanup revokes the accepted lease
before terminal evidence; abort cleanup revokes it when one was acquired.
Lifecycle journal schema v4 stores the executor-derived handle and request-key
cleanup debt, and rejects v1-v3 records rather than interpreting their state
without the expanded lease lifecycle. These are port semantics only: no broker, credential issuance, or
host integration is implemented.

The journal uses versioned compare-and-swap and must be durable and atomic in any
future implementation. Allocation and checkout interruptions can retry through
their idempotent reconciliation ports. A journaled `execution_started` state
never starts execution a second time: it asks the adapter for read-only outcome
reconciliation and remains held when the result is unknown. Result publication
and cleanup use reconcile-or-ensure ports with stable keys. The terminal phase
requires HGS-350 contract-versioned signed cleanup evidence bound to the exact
issue generation, session/process lease, repository, terminal outcome and
accepted head. It also confirms workspace removal, credential revocation and
reviewer-lease release; replay re-verifies that evidence.

The module is not connected to `Orchestrator` and has no production adapter or
journal implementation. Its deterministic fake adapter suite performs no
network or shell calls and supplies no credential values. It does not admit a
worker, qualify a disposable runner, or establish the signed HGS-350 receipt
implementation.

The HGS-729 source-only RKE2 Job provider accepts only an assignment bundle built from an
upstream signature-verified manifest entry, validates its canonical digest, and requires
schema-v2 `internal_beta`/`rke2` placement. The compiler itself does not verify the manifest
signature; callers preserve that provenance. It derives the Kubernetes Job name from issue ID,
generation, and assignment digest; uses a trusted namespace and a digest-pinned image;
and carries the validated non-secret assignment as structured JSON to a fixed worker
entrypoint. The Job has only bounded ephemeral `emptyDir` workspace/temp volumes,
fixed CPU/memory limits and deadline, a non-root read-only-root filesystem container,
default seccomp, dropped capabilities, and no service-account token. Its create/get/delete
port is fakeable. A separate activation call requires the recorded allocation UID, reads
the exact suspended Job, and sends an atomic JSON Patch that tests UID, resource version,
and suspended state before resuming it. It reads the active Job back; an active exact-UID
replay is idempotent. Activation requires a host-owned authorization guard to
recheck fresh admission and credential readiness before Kubernetes credentials
are requested; no production guard is installed. `SymphonyElixir.RKE2Job.HTTPClient`
is a separate HTTPS adapter: each
request requires a host-supplied HTTPS API origin, exact namespace, bearer token, and readable
regular CA certificate file; redirects and retries are disabled, and connection/response
timeouts are bounded. It is not wired into runtime configuration or Orchestrator. Provider
create ambiguity is reconciled by an exact GET before success is reported. Existing Jobs are accepted only when
their exact assignment identity and spec match; mismatches and unknown deletion targets
are held, and delete uses the fetched Kubernetes UID as a precondition. No credential source,
activation caller in Orchestrator, spawn, or Pod integration is included, and no RKE2 cluster or worker execution
is qualified.
