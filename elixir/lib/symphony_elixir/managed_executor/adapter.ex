defmodule SymphonyElixir.ManagedExecutor.Adapter do
  @moduledoc """
  Typed ports used by the managed executor lifecycle.

  Implementations must reconcile by the supplied idempotency key. They receive
  the signed assignment contract and non-secret references only; host credentials
  belong to the execution adapter and must never be returned here.

  The executor derives each lease handle from the signed assignment digest and
  acquire request key. Adapter responses never contain a lease reference or
  credential material. Acquire and renew reconcile by the supplied request key;
  revocation by the executor-derived handle or by an uncertain request key is
  idempotent, verifies ownership, and reports success only after cleanup.
  Checkout and execution adapters must recheck lease expiry at each side-effect boundary,
  since journal persistence
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
  @type credential_lease_response :: %{
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
  @type abort_release_ack :: %{
          projection_id: String.t(),
          reservation_id: String.t(),
          receipt_id: String.t(),
          receipt_kind: String.t(),
          assignment_digest: String.t(),
          allocation_id: String.t(),
          abort_result_ref: String.t(),
          generation: pos_integer(),
          execution_capacity_state: String.t(),
          scope_state: String.t(),
          reservation_state: String.t(),
          evidence_ref: String.t(),
          replayed: boolean()
        }

  @callback allocate_or_reconcile(assignment(), String.t(), term()) :: {:ok, allocation()} | {:error, term()}
  # Calls with the same idempotency key must reconcile the same checkout rather
  # than create another workspace, including replay after a lost acknowledgement.
  # :no_checkout is permitted only after the adapter has proved that no checkout
  # was accepted and no workspace remains. Ambiguous effects must return another
  # error so the executor retains the claim for reconciliation.
  @callback prepare_checkout(allocation(), assignment(), checkout_intent(), credential_lease(), String.t(), term()) ::
              {:ok, checkout_receipt()} | {:error, :no_checkout | term()}
  @callback acquire_credential_lease(allocation(), assignment(), String.t(), term()) ::
              {:ok, credential_lease_response()} | {:error, :denied | term()}
  @callback renew_credential_lease(allocation(), assignment(), credential_lease(), String.t(), term()) ::
              {:ok, credential_lease_response()} | {:error, :denied | term()}
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
  # The implementation must reconcile the exact provider release by the stable
  # key after a lost acknowledgement, and return only a verified release result.
  @callback ensure_abort_cleanup(allocation(), assignment(), map(), abort_reason(), String.t(), String.t(), term()) ::
              {:ok, abort_release_ack()} | {:error, term()}
  @callback publish_or_reconcile_abort_result(allocation(), assignment(), pre_execution_result(), String.t(), term()) ::
              {:ok, String.t()} | {:error, term()}
  @callback verify_terminal_cleanup(cleanup_evidence(), allocation(), assignment(), execution_result(), term()) ::
              :ok | {:error, term()}
end
