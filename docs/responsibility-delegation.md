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
The signer signs the exact raw file bytes prefixed by
`hypergrid.symphony.managed-delegation.v1` and one zero byte. The trusted service
environment pins the exact bytes of a root-owned regular file and the verifying
key; links, writable-by-others files, oversized input, partial configuration,
digest or signature mismatches fail closed. Keep the signing private key off the
runner guest and separate from the HGS-719 claim-witness key. The file contains
authorization inputs, never runtime leases, statuses, clock history or graph events.

The JSON object has `schema_version:1`, `pool_key`, `repository_ref`,
`managed_project_profile_id`, a nonempty `authority_ref`, and at most 20 `entries`.
An empty list explicitly authorizes no work and must also be signed. A declared
managed pool cannot omit manifest settings to select legacy admission; only an
undeclared legacy runtime may omit them.
Each entry contains `issue_id` (UUID), `identifier`, `owner_id`, and `accountable`/`responsible`
delegation inputs using the existing graph field names. The accountable actor is the current
native issue owner; the responsible actor must match the configured `DAHLIA_RUNNER_ID`. Scope identifies
company, objective, initiative, project, work package, issue and repository without wildcards.
Paths are explicit repository-relative paths, with `.` allowed for the fresh issue repository;
environment is `repository`. Both grants use `routine_engineering` authority, future expiry,
bounded model/effort/token budgets and a deliverable/evidence return contract. The responsible
child cannot delegate children. This is authority metadata, not a claim of OS sandbox isolation.

To issue or rotate a grant, keep an Ed25519 PEM private key on a trusted root
signing host outside the runner guest, owned by root with mode `0600`. Freeze the
manifest's exact bytes, copy that non-secret file to the signing host, and run
[`scripts/sign-managed-delegation.sh`](../scripts/sign-managed-delegation.sh)
with the private-key and manifest paths on Linux with Bash, GNU coreutils,
OpenSSL Ed25519 support, and `/dev/shm` available. The tool prints the file digest,
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
