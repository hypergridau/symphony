# Issue execution fencing

`SymphonyElixir.ExecutionFence` is the repository-local contract for safe
issue/worktree ownership across worker, reviewer, merge, and cleanup events.
It is deliberately pure: callers persist each returned snapshot atomically
and perform Git, process, tracker, or filesystem actions only after the
corresponding decision succeeds.

## Contract

- Admission creates one monotonically increasing generation for an issue. A
  mutable generation is scoped to one repository, branch, and worktree.
- A generation must explicitly register its worker and any reviewers. Each
  registration includes an opaque session/process identity, role, branch,
  worktree, tracker/PR state, head, and heartbeat.
- Every mutation guard, heartbeat, release, fence, and cleanup request carries
  the exact issue/generation token. Older generations return
  `:stale_generation` before a side effect can be considered.
- Terminal observation changes the generation to `:terminal` and records the
  terminal state, accepted head, merge identity, and observation time. It does
  not release leases implicitly; delayed workers and reviewers therefore
  remain visible until released or deterministically expired.
- Cleanup is admitted only for a terminal generation whose exact accepted head
  matches, whose ownership is reconciled, and whose leases are all released or
  expired. Repeating cleanup for that generation is `:already_cleaned`.
- A post-terminal head mismatch emits one generation-bound, idempotent
  `post_terminal_head_divergence` triage record in the durable fence snapshot.
  Repeated cleanup attempts cannot overwrite the original evidence or silently
  delete the preserved delta.
- A same-repository admission is blocked while another generation is active,
  unquiescent, or has unknown/contradictory ownership. Once every lease for an
  active generation is released, that generation is quiescent and the next
  admission receives a new generation, fencing the released worker token.
  Terminal tracker state dominates stale non-terminal session observations.

`reconcile_sessions/4` consumes an explicit, sanitized session snapshot. It
returns deterministic `:reconciled` or `:blocked` evidence and expires leases
whose last heartbeat exceeds the caller-supplied TTL. Unknown sessions,
scope/head/process contradictions, future heartbeats, and stale terminal
observations remain fail-closed.

The current orchestrator can adopt this contract at its scheduling and
workspace boundaries without giving the contract authority to delete a
worktree, mutate a branch, terminate a process, or change Linear/GitHub state.

The worker seam accepts a synchronous `execution_fence_guard` callback. The
orchestrator owns the serializable snapshot, admits and registers a logical worker
lease before spawning, and the worker calls the guard before
`Workspace.create_for_issue/2`. The worker reports the exact checked-out Git head
at workspace start and shutdown; matching, generation-bound runtime messages
persist that head on the lease and carry it into terminal cleanup. Terminal
observation fences the generation before the existing stop path. A fenced task may
therefore not begin workspace creation or later mutable work.

Cleanup re-reads the exact Git head from the recorded local or remote workspace
after all leases quiesce. A changed, unreadable, or unrecorded head preserves the
workspace and leaves the execution pending for reconciliation; `PR merged` or an
unknown head is never treated as proof of quiescence.

Managed checkout identity follows the host filesystem's lexical rules: Windows
comparisons normalize separators and case while canonicalization still rejects
symlink aliases. Local workspace hooks use `sh` when available and fall back to
Git for Windows' bundled `sh.exe`, preserving the same command and timeout
contract on hosts without a standalone POSIX shell.

The serialized snapshot is persisted through
`ExecutionFence.Persistence`. Save uses a temporary file and a recoverable
previous-snapshot rename on platforms where replacement cannot be atomic; startup
rehydration validates the complete bidirectional lease registry and can recover a
valid previous snapshot if the primary is missing. Non-cleaned generations are
marked unknown and fail closed after restart.

If the global mutable-admission pause arrives after a managed provider claim is
confirmed but before the worker task starts, the orchestrator records the issue
in its process-local blocked/claimed maps and retains the confirmed claim journal,
execution fence, and responsibility graph unchanged. No worker task starts. This
is a fail-closed hold, not a durable blocked marker: restart recovery or resumption
from this exact paused-claim state is not yet qualified. Do not infer that the
provider claim was released or safe to replay from the in-memory block alone.
