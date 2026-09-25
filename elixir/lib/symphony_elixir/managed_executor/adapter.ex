defmodule SymphonyElixir.ManagedExecutor.Adapter do
  @moduledoc """
  Typed ports used by the managed executor lifecycle.

  Implementations must reconcile by the supplied idempotency key. They receive
  the signed assignment contract and non-secret references only; host credentials
  belong to the execution adapter and must never be returned here.

  Credential lease references are non-authorizing, non-secret identifiers using
  only ASCII letters, digits, colon, underscore, or hyphen. Acquisition and
  renewal reconcile by key. A malformed response with a safe reference is
  durably quarantined by the journal and revoked before abort. An unsafe or
  absent reference is quarantined by the stable acquire/renew request key and
  revoked through the request-reconciliation port. Revocation by reference or
  request key is idempotent and verifies ownership before revoking. Execution adapters
  must recheck lease expiry at the side-effect boundary, since journal persistence
  can take long enough for a locally checked expiry to pass.
  """

  @type assignment :: map()
  @type allocation :: %{id: String.t(), status: :ready}
  @type credential_lease :: %{
          lease_ref: String.t(),
          assignment_digest: String.t(),
          allocation_id: String.t(),
          expires_at_ms: non_neg_integer()
        }
  @type checkout_intent :: %{repository_ref: String.t(), base_ref: String.t(), branch: String.t()}
  @type checkout_receipt :: %{
          assignment_digest: String.t(),
          repository_ref: String.t(),
          base_ref: String.t(),
          branch: String.t(),
          head: String.t()
        }
  @type execution_result :: %{
          assignment_digest: String.t(),
          outcome: :completed | :failed | :blocked,
          summary: String.t(),
          evidence_ref: String.t(),
          accepted_head: String.t()
        }
  @type cleanup_evidence :: %{
          contract_version: String.t(),
          receipt_kind: String.t(),
          assignment_digest: String.t(),
          allocation_id: String.t(),
          issue_id: String.t(),
          generation: pos_integer(),
          session_id: String.t(),
          process_id: String.t(),
          repository_ref: String.t(),
          accepted_head: String.t(),
          terminal_outcome: :completed | :failed | :blocked,
          workspace_removed: true,
          credentials_revoked: true,
          reviewer_leases_released: true,
          evidence_ref: String.t(),
          checksum: String.t(),
          signer_id: String.t(),
          signature: String.t()
        }
  @type abort_reason :: :checkout_preparation_failed | :checkout_intent_mismatch
  @type pre_execution_result :: %{
          assignment_digest: String.t(),
          abort_reason: abort_reason(),
          outcome: :blocked,
          summary: String.t(),
          evidence_ref: String.t()
        }

  @callback allocate_or_reconcile(assignment(), String.t(), term()) :: {:ok, allocation()} | {:error, term()}
  @callback prepare_checkout(allocation(), assignment(), checkout_intent(), String.t(), term()) ::
              {:ok, checkout_receipt()} | {:error, term()}
  @callback acquire_credential_lease(allocation(), assignment(), String.t(), term()) ::
              {:ok, credential_lease()} | {:error, :denied | term()}
  @callback renew_credential_lease(allocation(), assignment(), credential_lease(), String.t(), term()) ::
              {:ok, credential_lease()} | {:error, :denied | term()}
  @callback revoke_credential_lease(allocation(), assignment(), String.t(), String.t(), term()) ::
              :ok | {:error, term()}
  @callback revoke_credential_lease_request(allocation(), assignment(), String.t(), String.t(), term()) ::
              :ok | {:error, term()}
  @callback execute(allocation(), assignment(), checkout_receipt(), credential_lease(), String.t(), term()) ::
              {:ok, execution_result()} | {:error, term()}
  @callback reconcile_execution(
              allocation(),
              assignment(),
              checkout_receipt(),
              credential_lease(),
              String.t(),
              term()
            ) ::
              {:ok, execution_result() | nil} | {:error, term()}
  @callback publish_or_reconcile_result(allocation(), assignment(), execution_result(), String.t(), term()) ::
              {:ok, String.t()} | {:error, term()}
  @callback ensure_terminal_cleanup(allocation(), assignment(), execution_result(), String.t(), term()) ::
              {:ok, cleanup_evidence()} | {:error, term()}
  @callback ensure_abort_cleanup(allocation(), assignment(), abort_reason(), String.t(), term()) ::
              :ok | {:error, term()}
  @callback publish_or_reconcile_abort_result(allocation(), assignment(), pre_execution_result(), String.t(), term()) ::
              {:ok, String.t()} | {:error, term()}
  @callback verify_terminal_cleanup(cleanup_evidence(), allocation(), assignment(), execution_result(), term()) ::
              :ok | {:error, term()}
end
