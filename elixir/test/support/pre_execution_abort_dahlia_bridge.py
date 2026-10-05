"""Pair Symphony's exact abort caller bytes with Dahlia's pure verifier fixture."""

import base64
import datetime
import hashlib
import importlib.util
import json
import pathlib
import sys


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    dahlia = pathlib.Path(sys.argv[1]).resolve()
    payload = json.loads(pathlib.Path(sys.argv[2]).read_text(encoding="utf-8"))
    paired = load("paired_predisposal_fixture", dahlia / "test_pre_execution_abort_predisposal.py")
    request, _fixture_claim, checkpoints, histories, provider, credentials, host, _result, _now = (
        paired.disposal_fixture())

    claim = payload["claim"]
    request = payload["request"]
    request_bytes = base64.b64decode(payload["prepareRequestBytes"], validate=True)
    observation = payload["observation"]
    acknowledgement = payload["acknowledgement"]
    raw_result = base64.b64decode(payload["resultBytes"], validate=True)

    if payload.get("tamper") == "unsupported_reason":
        value = json.loads(raw_result.decode("utf-8"))
        value["abortReason"] = "unsupported_abort_reason"
        raw_result = json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")

    request_body = json.loads(request_bytes.decode("utf-8"))
    request_sha256 = hashlib.sha256(request_bytes).hexdigest()
    prepare_id = request_body["prepareId"]
    now = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)
    observed_at = (now - datetime.timedelta(seconds=30)).isoformat(timespec="seconds").replace("+00:00", "Z")

    witness = paired.GATE.PREFLIGHT.witness
    bare_claim = {field: claim[field] for field in witness.CLAIM_FIELDS}
    source = {
        "sourceHead": "a" * 40,
        "executableSHA256": "b" * 64,
        "wrapperSHA256": "c" * 64,
        "attestationSHA256": "d" * 64,
    }
    records = []
    for operation in ("claim_intent", "claim_bound", "abort_prepare_intent"):
        event = {"version": 3 if operation == "abort_prepare_intent" else 1,
                 "pool": claim["pool"], "operation": operation, "claim": bare_claim}
        if operation == "abort_prepare_intent":
            event["abortPrepare"] = {"prepareId": prepare_id,
                                     "prepareRequestSHA256": request_sha256}
        record, _ = witness.prepare_record(
            records, event, "boot-paired-test", observed_at, source)
        records.append(record)
    histories = {pool: [] for pool in witness.POOLS}
    histories[claim["pool"]] = records

    checkpoints["request"] = {
        "schema_version": 1,
        "claim": claim,
        "assignment_digest": request["assignmentDigest"],
        "allocation_id": request["allocationId"],
        "prepare_id": prepare_id,
        "request_sha256": request_sha256,
        "request_bytes": request_bytes.decode("utf-8"),
        "observation": observation,
    }
    checkpoints["intent"] = {
        "schema_version": 1,
        "prepare_id": prepare_id,
        "request_sha256": request_sha256,
        "root_receipt": {"version": 1, "sequence": records[-1]["sequence"],
                         "hash": records[-1]["hash"]},
    }
    checkpoints["ack"] = {
        "schema_version": 1,
        "prepare_id": prepare_id,
        "request_sha256": request_sha256,
        "acknowledgement": acknowledgement,
    }

    provider.update({
        "observedAt": observed_at,
        "prepareId": prepare_id,
        "projectionId": claim["projectionId"],
        "reservationId": claim["reservationId"],
        "generation": claim["generation"],
        "runnerId": claim["runnerId"],
        "issueId": claim["issueId"],
        "assignmentDigest": request["assignmentDigest"],
        "allocationId": request["allocationId"],
        "slotLeaseId": observation["slotBinding"]["leaseId"],
        "namespace": observation["compiledIdentity"]["namespace"],
        "jobName": observation["compiledIdentity"]["name"],
        "jobUid": observation["job"]["uid"],
        "jobResourceVersion": observation["job"]["resourceVersion"],
        "prepareRequestSHA256": request_sha256,
        "preparedAt": acknowledgement["preparedAt"],
    })
    credentials["observedAt"] = observed_at
    for readback in credentials["readbacks"]:
        readback["observedAt"] = observed_at
    lease = credentials["readbacks"][0]["lease"]
    lease["subject"].update({
        "assignmentDigest": request["assignmentDigest"],
        "issueUuid": claim["issueId"],
        "generation": claim["generation"],
        "runnerId": claim["runnerId"],
        "repositoryRef": claim["repositoryRef"],
    })
    host.update({
        "observedAt": observed_at,
        "issueId": claim["issueId"],
        "generation": claim["generation"],
        "sessionId": claim["sessionId"],
        "processId": claim["processId"],
        "workspaceId": claim["workspaceId"],
    })
    result = {
        "observedAt": observed_at,
        "reference": request["resultReference"],
        "rawBytes": raw_result,
        "sha256": hashlib.sha256(raw_result).hexdigest(),
    }

    accepted = paired.GATE.verify_disposal(
        request, claim,
        {key: checkpoints[key] for key in ("request", "intent", "ack")},
        histories, provider, credentials, host, result, now)
    print(json.dumps(accepted, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except Exception as error:  # The caller treats any verifier exception as a fail-closed hold.
        print(json.dumps({"denied": str(error)}, sort_keys=True, separators=(",", ":")))
        raise SystemExit(2)
