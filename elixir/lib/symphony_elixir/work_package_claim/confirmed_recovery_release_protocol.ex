defmodule SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryReleaseProtocol do
  @moduledoc "Source-only release coordinator. Production admission remains closed in RootHost."

  alias SymphonyElixir.WorkPackageClaim.ConfirmedRecoveryEvidence, as: Evidence

  @version "hgs740-release-only.v1"
  @auth_domain "hypergrid-work-package-recovery:hgs740-release-authorization.v1\0"
  @attest_domain "hypergrid-work-package-recovery:hgs740-release-attestation.v1\0"
  @receipt_domain "hypergrid-work-package-recovery:hgs740-local-transition-receipt.v3\0"
  @actions ~w(collect_release_attestation issue_local_transition_receipt)
  @record_fields ~w(approvalId workspaceId companyId approvalState targetAction decisionMode approverType approverRef decisionActorType decisionActorRef supersededByApprovalId decidedAt expiresAt decisionPayload)
  @payload_fields ~w(protocolVersion bindingSHA256 sourceHeads attemptId allowedActions nativeDecisionId)
  @checks %{
    "globalPause" => true,
    "unitsMaskedAndQuiescent" => true,
    "nativeCustodyVerified" => true,
    "localPostimagesVerified" => true,
    "workerCount" => 0,
    "jobCount" => 0,
    "podCount" => 0,
    "credentialLeaseCount" => 0,
    "oauthSlotLeaseCount" => 0,
    "completeInventories" => true,
    "providerState" => "claimed_held"
  }

  @doc "Runs only with trusted enrollment and host ports; reserves before its single collection."
  @spec run(String.t(), map(), map()) :: {:ok, atom(), map()} | {:error, atom()}
  def run(decision_id, enrollment, ports) do
    if admitted?(enrollment),
      do: normalize(ports.with_lock.(fn -> locked(decision_id, enrollment, ports) end)),
      else: {:error, :hgs740_release_protocol_not_admitted}
  rescue
    _ -> {:error, :hgs740_release_protocol_held_closed}
  catch
    _, _ -> {:error, :hgs740_release_protocol_held_closed}
  end

  defp normalize({:ok, status, bundle} = result) when status in [:created, :retained] and is_map(bundle), do: result
  defp normalize({:error, reason} = result) when is_atom(reason), do: result
  defp normalize(_), do: {:error, :hgs740_release_protocol_held_closed}

  defp admitted?(e) do
    e.protocol_accepted == true and e.trust_enrolled == true and
      is_binary(e.owner_principal) and e.owner_principal != "" and
      fingerprint(e.native_public_key) == e.native_fingerprint and source_heads?(e.source_heads)
  end

  defp locked(id, enrollment, ports) do
    with {:ok, snapshot} <- ports.snapshot.(),
         {:ok, binding} <- binding(snapshot, enrollment.source_heads),
         {:ok, decision} <- ports.approval.(id),
         true <- decision["approvalId"] == id,
         {:ok, retained} <- retained(ports, binding, decision, enrollment) do
      resume(retained, snapshot, binding, decision, enrollment, ports)
    end
  end

  defp resume(:absent, snapshot, binding, decision, enrollment, ports) do
    with {:ok, auth} <- authorize(decision, binding, enrollment.owner_principal, ports.now.()),
         do: create(snapshot, binding, decision, auth, enrollment, ports)
  end

  defp resume(bundle, _snapshot, _binding, _decision, _enrollment, _ports), do: {:ok, :retained, bundle}

  @doc false
  @spec binding(map(), map()) :: {:ok, map()} | {:error, atom()}
  def binding(snapshot, heads) do
    with {:ok, marker} <- Jason.decode(snapshot.marker_bytes),
         {:ok, candidate} <- Jason.decode(snapshot.candidate_bytes),
         {:ok, observation} <- Jason.decode(snapshot.observation_bytes),
         {:ok, manifest} <- Jason.decode(snapshot.manifest_bytes),
         true <- source_heads?(heads),
         true <- marker["contractVersion"] == "work-package-hgs740-local-transition.v3",
         true <- marker["status"] == "local_applied" and marker["generation"] == 2,
         true <- marker["pool"] == "hypergrid-gitops" and marker["assignmentSnapshotState"] == "absent",
         true <- manifest == observation["reconciliation"] and manifest["epoch"] == "epoch-5",
         true <- manifest["observedAt"] == observation["observedAt"],
         true <- snapshot.manifest_bytes == Evidence.canonical_json(manifest),
         true <- hash(snapshot.proof_bytes) == marker["proofSHA256"],
         true <- hash(snapshot.observation_bytes) == marker["observationSHA256"],
         true <- candidate["expected"] == marker["expected"] and candidate["nonce"] == marker["nonce"],
         true <- images_match?(snapshot.committed_images, marker) do
      {:ok,
       %{
         "expected" => marker["expected"],
         "generation" => 2,
         "nonce" => marker["nonce"],
         "epoch" => "epoch-5",
         "originalObservedAt" => observation["observedAt"],
         "pool" => marker["pool"],
         "markerSHA256" => hash(snapshot.marker_bytes),
         "localCandidateSHA256" => hash(snapshot.candidate_bytes),
         "proofSHA256" => hash(snapshot.proof_bytes),
         "epochManifestSHA256" => hash(snapshot.manifest_bytes),
         "postimages" => candidate["postimages"],
         "sourceHeads" => heads
       }}
    else
      _ -> {:error, :hgs740_release_binding_held_closed}
    end
  rescue
    _ -> {:error, :hgs740_release_binding_held_closed}
  end

  @doc false
  @spec authorize(map(), map(), String.t(), integer()) :: {:ok, map()} | {:error, atom()}
  def authorize(record, binding, owner, now) do
    with true <- exact?(record, @record_fields),
         true <- human?(record, owner),
         true <- record["workspaceId"] == binding["expected"]["workspaceId"],
         true <- record["companyId"] == binding["expected"]["companyId"],
         payload <- record["decisionPayload"],
         true <- decision_binding?(payload, binding),
         {:ok, issued} <- time(record["decidedAt"]),
         {:ok, expires} <- time(record["expiresAt"]),
         true <- issued <= now and now < expires,
         true <- is_binary(record["approvalId"]) and record["approvalId"] != "" do
      {:ok,
       %{
         "contractVersion" => "hgs740-release-authorization.v1",
         "decisionId" => record["approvalId"],
         "decisionSHA256" => hash(Evidence.canonical_json(record)),
         "ownerPrincipal" => owner,
         "attemptId" => payload["attemptId"],
         "bindingSHA256" => hash(Evidence.canonical_json(binding)),
         "allowedActions" => @actions,
         "issuedAt" => record["decidedAt"],
         "expiresAt" => record["expiresAt"]
       }}
    else
      _ -> {:error, :hgs740_owner_decision_not_admitted}
    end
  rescue
    _ -> {:error, :hgs740_owner_decision_not_admitted}
  end

  defp human?(r, owner) do
    r["approvalState"] == "approved" and r["targetAction"] == "hgs740_release_local" and
      r["decisionMode"] == "human" and r["approverType"] == "user" and r["approverRef"] == owner and
      r["decisionActorType"] == "user" and r["decisionActorRef"] == owner and is_nil(r["supersededByApprovalId"])
  end

  defp decision_binding?(p, b) do
    exact?(p, @payload_fields) and p["protocolVersion"] == @version and p["allowedActions"] == @actions and
      p["bindingSHA256"] == hash(Evidence.canonical_json(b)) and p["sourceHeads"] == b["sourceHeads"] and
      is_nil(p["nativeDecisionId"]) and uuid?(p["attemptId"])
  end

  defp retained(ports, binding, decision, enrollment) do
    case ports.read.("release-only-bundle.json") do
      {:error, :enoent} ->
        case ports.read.("release-only-attempt.json") do
          {:error, :enoent} -> {:ok, :absent}
          _ -> {:error, :hgs740_partial_attempt_held_closed}
        end

      {:ok, bytes} ->
        with {:ok, bundle} <- Jason.decode(bytes),
             true <- exact?(bundle, ~w(binding authorization attestation receipt)),
             true <- bytes == Evidence.canonical_json(bundle),
             true <- bundle["binding"] == binding,
             {:ok, auth} <- verify(bundle["authorization"], @auth_domain, enrollment.native_public_key),
             {:ok, issued} <- time(auth["issuedAt"]),
             {:ok, ^auth} <- authorize(decision, binding, enrollment.owner_principal, issued),
             true <- auth["decisionSHA256"] == hash(Evidence.canonical_json(decision)),
             {:ok, attestation} <- verify(bundle["attestation"], @attest_domain, enrollment.native_public_key),
             true <- attestation["authorizationSHA256"] == hash(bundle["authorization"]),
             true <- attestation["binding"] == binding,
             {:ok, receipt} <- verify(bundle["receipt"], @receipt_domain, enrollment.native_public_key),
             true <- hash(Evidence.canonical_json(receipt)) == binding["localCandidateSHA256"] do
          {:ok, bundle}
        else
          _ -> {:error, :hgs740_retained_bundle_invalid}
        end

      _ ->
        {:error, :hgs740_retained_bundle_invalid}
    end
  end

  defp create(snapshot, binding, decision, auth, enrollment, ports) do
    with :ok <- ports.create.("release-only-attempt.json", Evidence.canonical_json(auth)),
         {:ok, ^auth} <- authorize(decision, binding, enrollment.owner_principal, ports.now.()),
         {:ok, auth_raw} <- envelope(auth, @auth_domain, enrollment, ports),
         started <- ports.now.(),
         {:ok, readbacks} <- ports.collect.(binding),
         {:ok, ^snapshot} <- ports.snapshot.(),
         {:ok, ^decision} <- ports.approval.(decision["approvalId"]),
         now <- ports.now.(),
         {:ok, observed} <- observations(readbacks, binding, started, now),
         :ok <- receipt_history(snapshot, observed),
         {:ok, ^auth} <- authorize(decision, binding, enrollment.owner_principal, now),
         {:ok, attestation} <- attest(readbacks, binding, auth_raw, auth, observed, now),
         :ok <- current_evidence(decision, binding, auth, attestation, enrollment, ports),
         {:ok, attest_raw} <- envelope(attestation, @attest_domain, enrollment, ports),
         :ok <- current_evidence(decision, binding, auth, attestation, enrollment, ports),
         :ok <- ports.create.("release-only-attestation.json", attest_raw),
         {:ok, ^snapshot} <- ports.snapshot.(),
         {:ok, ^decision} <- ports.approval.(decision["approvalId"]),
         :ok <- current_evidence(decision, binding, auth, attestation, enrollment, ports),
         {:ok, receipt_raw} <- envelope_bytes(snapshot.candidate_bytes, @receipt_domain, enrollment, ports),
         {:ok, ^snapshot} <- ports.snapshot.(),
         {:ok, ^decision} <- ports.approval.(decision["approvalId"]),
         :ok <- current_evidence(decision, binding, auth, attestation, enrollment, ports),
         :ok <- ports.create.("local-transition-receipt.json", receipt_raw),
         bundle = %{"binding" => binding, "authorization" => auth_raw, "attestation" => attest_raw, "receipt" => receipt_raw},
         {:ok, ^snapshot} <- ports.snapshot.(),
         {:ok, ^decision} <- ports.approval.(decision["approvalId"]),
         :ok <- current_evidence(decision, binding, auth, attestation, enrollment, ports),
         :ok <- ports.create.("release-only-bundle.json", Evidence.canonical_json(bundle)) do
      {:ok, :created, bundle}
    end
  end

  defp current_evidence(decision, binding, auth, attestation, enrollment, ports) do
    now = ports.now.()

    with {:ok, ^auth} <- authorize(decision, binding, enrollment.owner_principal, now) do
      fresh(attestation, now)
    end
  end

  defp observations(r, binding, start, now) do
    with true <- exact?(r, ~w(host provider jobs pods custody)),
         true <- r["host"]["sourceIdentity"] == "native-host" and r["host"]["globalPause"] == true,
         true <- r["host"]["unitsMaskedAndQuiescent"] == true and r["host"]["workerCount"] === 0,
         true <- provider?(r["provider"], binding),
         true <- inventory?(r["jobs"], "kubernetes:jobs") and inventory?(r["pods"], "kubernetes:pods"),
         true <- custody?(r["custody"], binding),
         times <- Enum.map(Map.values(r), &time(&1["observedAt"])),
         true <- Enum.all?(times, fn {status, t} -> status == :ok and start - 5_000 <= t and t <= now + 5_000 end),
         observed <- times |> Enum.map(&elem(&1, 1)) |> Enum.min(),
         true <- now - observed <= 60_000 do
      {:ok, observed}
    else
      _ -> {:error, :hgs740_release_observation_invalid}
    end
  end

  defp receipt_history(snapshot, observed) do
    with {:ok, candidate} <- Jason.decode(snapshot.candidate_bytes),
         {:ok, completed} <- time(candidate["completedAt"]),
         true <- completed <= observed do
      :ok
    else
      _ -> {:error, :hgs740_release_receipt_history_invalid}
    end
  end

  defp provider?(p, b),
    do:
      p["sourceIdentity"] == "provider-core:postgres" and p["expected"] == b["expected"] and
        p["state"] == "claimed_held" and p["complete"] == true and p["credentialLeases"] == [] and p["oauthSlotLeases"] == []

  defp inventory?(r, source), do: r["sourceIdentity"] == source and r["complete"] == true and r["items"] == [] and text?(r["resourceVersion"])

  defp custody?(c, b),
    do:
      c["sourceIdentity"] == "native-custody" and c["verified"] == true and
        c["markerSHA256"] == b["markerSHA256"] and c["postimages"] == b["postimages"] and c["sourceHeads"] == b["sourceHeads"]

  defp attest(readbacks, binding, auth_raw, auth, observed, now) do
    {:ok, expiry} = time(auth["expiresAt"])
    expires = min(observed + 60_000, expiry)
    names = %{"host" => "hostReadbackSHA256", "provider" => "providerReadbackSHA256", "jobs" => "jobsReadbackSHA256", "pods" => "podsReadbackSHA256", "custody" => "nativeCustodySHA256"}
    hashes = Map.new(readbacks, fn {name, value} -> {names[name], hash(Evidence.canonical_json(value))} end)

    value = %{
      "contractVersion" => "hgs740-release-attestation.v1",
      "authorizationSHA256" => hash(auth_raw),
      "binding" => binding,
      "observedAt" => iso(observed),
      "expiresAt" => iso(expires),
      "checks" => @checks,
      "readbacks" => hashes
    }

    with :ok <- fresh(value, now), do: {:ok, value}
  end

  defp fresh(a, now) do
    {:ok, observed} = time(a["observedAt"])
    {:ok, expiry} = time(a["expiresAt"])
    valid = observed - 5_000 <= now and now < expiry and (expiry - observed) in 1..60_000
    if valid, do: :ok, else: {:error, :hgs740_release_attestation_expired}
  end

  defp envelope(value, domain, enrollment, ports), do: envelope_bytes(Evidence.canonical_json(value), domain, enrollment, ports)

  defp envelope_bytes(bytes, domain, enrollment, ports) do
    with {:ok, signature} when byte_size(signature) == 64 <- ports.sign.(domain <> bytes),
         true <- :crypto.verify(:eddsa, :none, domain <> bytes, signature, [enrollment.native_public_key, :ed25519]) do
      envelope = %{"payload" => Base.url_encode64(bytes, padding: false)}
      envelope = Map.put(envelope, "signature", Base.url_encode64(signature, padding: false))
      {:ok, Evidence.canonical_json(envelope)}
    else
      _ -> {:error, :hgs740_release_signature_invalid}
    end
  end

  defp verify(raw, domain, public) do
    with true <- is_binary(raw) and byte_size(raw) <= 262_144,
         {:ok, e} <- Jason.decode(raw),
         true <- exact?(e, ~w(payload signature)),
         true <- raw == Evidence.canonical_json(e),
         {:ok, bytes} <- Base.url_decode64(e["payload"], padding: false),
         {:ok, signature} <- Base.url_decode64(e["signature"], padding: false),
         true <- byte_size(signature) == 64 and Base.url_encode64(signature, padding: false) == e["signature"],
         true <- Base.url_encode64(bytes, padding: false) == e["payload"],
         true <- :crypto.verify(:eddsa, :none, domain <> bytes, signature, [public, :ed25519]),
         {:ok, payload} <- Jason.decode(bytes),
         true <- bytes == Evidence.canonical_json(payload) do
      {:ok, payload}
    else
      _ -> {:error, :invalid_release_signature}
    end
  end

  defp images_match?(images, marker) do
    exact?(images, ~w(claimJournal fence responsibilityGraph)) and
      Enum.all?(images, fn {name, bytes} ->
        row = marker["postimages"][name]
        row["sha256"] == hash(bytes) and row["bytes"] == Base.url_encode64(bytes, padding: false)
      end)
  end

  defp source_heads?(h), do: exact?(h, ~w(dahlia symphony)) and Enum.all?(Map.values(h), &Regex.match?(~r/\A[0-9a-f]{40}\z/, &1))
  defp exact?(v, fields), do: is_map(v) and Enum.sort(Map.keys(v)) == Enum.sort(fields)
  defp uuid?(v), do: is_binary(v) and Regex.match?(~r/\A[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}\z/, v)
  defp text?(v), do: is_binary(v) and v != ""

  defp time(v) when is_binary(v) do
    with {:ok, dt, 0} <- DateTime.from_iso8601(v), do: {:ok, DateTime.to_unix(dt, :millisecond)}
  end

  defp time(_), do: :error
  defp iso(ms), do: ms |> DateTime.from_unix!(:millisecond) |> DateTime.to_iso8601()
  defp hash(bytes), do: :crypto.hash(:sha256, bytes) |> Base.encode16(case: :lower)
  defp fingerprint(key), do: hash(Base.encode64(<<0x30, 0x2A, 0x30, 0x05, 0x06, 0x03, 0x2B, 0x65, 0x70, 0x03, 0x21, 0x00>> <> key))
end
