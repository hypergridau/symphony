# Disposable worker task proof

The disposable worker keeps a small proof in the termination receipt so the
host can journal what the worker actually observed before deleting the Job.
This proof describes one ordinary Codex assignment. The HGS-736 no-checkout
qualification remains a separate path and does not run Codex or source
validation.

## Codex evidence

The receipt labels `gpt-6-luna` and `high` as the model and reasoning requested
on the fixed Codex argv. It does not claim that Codex used an effective model;
the current JSONL parser does not establish that value. The remaining flags
report whether a completed turn was observed, whether an error was observed,
whether Codex reported a model reroute, and whether the event stream was
malformed or exceeded the per-line bound.

Publication requires a zero Codex exit, an observed completed turn, and false
error, reroute and invalid-stream flags. The parser retains only these finite
classifications. It drops event IDs, response text, error messages, usage,
commands and raw event payloads. The exact existing Code Mode-disabled notice
keeps its prior classification as a known warning; any other completed error
item counts as an error.

## Source validation

Before requesting a publication lease, the worker checks the exact bounded
contents snapshot that the publisher will commit. It feeds those captured
bytes to Git on standard input, so a later workspace-path change cannot alter
what the check attests:

```text
git diff --no-index --check -- /dev/null -
```

The pinned Linux worker accepts only exit status `1` with empty combined output,
which is Git's clean “files differ” result for a new file. A whitespace finding,
unexpected output, another exit status, oversized input or output, a timeout or
a command failure blocks publication. The receipt stores the check kind, number
of files checked and a pass flag; it does not store filenames or diagnostics.

## Receipt compatibility

New worker results use strict schema version 2. Completed results require a
valid Codex proof and a passing validation count equal to the published file
count. Failed results may retain partial boolean evidence and a failed or
unreached validation state. Preflight results carry no worker proof.

The host continues to accept exact schema-v1 receipts for jobs created by an
older image that terminate after an image rollout. Schema-v1 acceptance is
compatibility for those already-running jobs; new worker code always emits
schema version 2. The result reader and result journal both validate the exact
receipt keys and bounded proof values before the host stores the observation.
