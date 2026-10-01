defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryClaimTransition do
  @moduledoc """
  Pure journal, execution-fence, and responsibility-graph transition for HGS-740.

  The root transaction adapter supplies states only after validating their signed
  preimages and host quiescence. This module computes the exact serialized
  postimages without filesystem or process access.
  """

  alias SymphonyElixir.ExecutionFence
  alias SymphonyElixir.ExecutionFence.Persistence, as: FencePersistence
  alias SymphonyElixir.ResponsibilityGraph
  alias SymphonyElixir.ResponsibilityGraph.Persistence, as: GraphPersistence
  alias SymphonyElixir.WorkPackageClaim.{Dispatch, Journal}

  @doc "Computes exact postimages for the confirmed generation-two recovery transition."
  @spec prepare_postimages(map(), map(), map(), map(), non_neg_integer()) ::
          {:ok, %{claimJournal: binary(), fence: binary(), responsibilityGraph: binary(), reservationId: String.t()}}
          | {:error, :confirmed_claim_transition_rejected}
  def prepare_postimages(journal, fence, graph, payload, now_ms)
      when is_map(journal) and is_map(fence) and is_map(graph) and is_map(payload) and
             is_integer(now_ms) and now_ms >= 0 do
    expected = get_in(payload, ["observation", "expected"])
    reservation_id = payload["reservationId"]
    issue_id = payload["issueId"]

    with true <- is_map(expected),
         true <- is_binary(reservation_id),
         true <- is_binary(issue_id),
         key <- Journal.reservation_key(issue_id, expected["managedProjectProfileId"], expected["repositoryRef"], 2),
         {:ok, fence} <- reconcile_recovery_fence(fence, journal.reservations[key], payload),
         {:ok, next_journal} <- Dispatch.begin_confirmed_recovery(journal, key),
         {:ok, next_fence, :released} <-
           ExecutionFence.release(fence, %{issue_id: issue_id, generation: 2}, expected["sessionId"], :spawn_failed),
         runtime_lease <- %{
           issue_id: issue_id,
           repository: expected["repositoryRef"],
           generation: 2,
           session_id: expected["sessionId"],
           process_id: expected["processId"]
         },
         {:ok, next_graph, :released} <-
           release_recovery_graph(graph, expected["responsibleDelegationId"], runtime_lease, payload, now_ms),
         {:ok, journal_bytes} <- Journal.encode_bytes(next_journal),
         {:ok, fence_bytes} <- FencePersistence.encode_bytes(next_fence),
         {:ok, graph_bytes} <- GraphPersistence.encode_bytes(next_graph) do
      {:ok,
       %{
         claimJournal: journal_bytes,
         fence: fence_bytes,
         responsibilityGraph: graph_bytes,
         reservationId: reservation_id
       }}
    else
      _ -> {:error, :confirmed_claim_transition_rejected}
    end
  end

  def prepare_postimages(_journal, _fence, _graph, _payload, _now_ms),
    do: {:error, :confirmed_claim_transition_rejected}

  defp reconcile_recovery_fence(fence, reservation, %{"contractVersion" => "work-package-paused-confirmed-recovery.v3"}),
    do: ExecutionFence.reconcile_unstarted_claim(fence, reservation)

  defp reconcile_recovery_fence(fence, _reservation, _payload), do: {:ok, fence}

  defp release_recovery_graph(graph, id, lease, %{"contractVersion" => "work-package-paused-confirmed-recovery.v3"}, now_ms),
    do: ResponsibilityGraph.release_restart_blocked_runtime_lease(graph, id, lease, now_ms)

  defp release_recovery_graph(graph, id, lease, _payload, now_ms),
    do: ResponsibilityGraph.release_runtime_lease(graph, id, lease, now_ms)
end
