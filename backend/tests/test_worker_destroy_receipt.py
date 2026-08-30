"""Exact cloud-effect authority tests for Worker destruction."""

from __future__ import annotations

import copy
import hashlib
import json
from datetime import datetime

import pytest

import backend.services.worker_proxy as worker_proxy_module
from backend.config import settings
from backend.models.worker import Worker
from backend.services.worker_drain_proof import (
    WORKER_NODE_DRAIN_PROOF_PROTOCOL,
    worker_node_drain_proof_signature,
)
from backend.services.worker_proxy import (
    WORKER_DESTROY_TERMINATION_ACTION,
    WORKER_DESTROY_TERMINATION_RECEIPT_VERSION,
    WorkerProxy,
    build_worker_destroy_termination_receipt,
    capture_worker_destroy_lifecycle_claim,
    worker_destroy_client_token_digest,
    worker_destroy_provision_spec_digest,
    worker_destroy_termination_receipt_matches,
)


AUTH_TOKEN = "worker-destroy-auth-token"
CLIENT_TOKEN = "ccm-stable-create-token"
CLOUD_SCOPE = {
    "provider": "aws",
    "partition": "aws",
    "account_id": "123456789012",
    "region": "us-east-1",
}
PROVISION_SPEC = {
    "version": 1,
    "name": "receipt-worker",
    "has_fixed_overrides": True,
    "overrides": {
        "instance_type": "t3.large",
        "ccm_port": 8000,
    },
    "cloud_scope": CLOUD_SCOPE,
    "client_token_digest": hashlib.sha256(
        CLIENT_TOKEN.encode("utf-8")
    ).hexdigest(),
}


def _worker() -> Worker:
    return Worker(
        id=1,
        name="receipt-worker",
        status="destroying",
        bootstrap_step=None,
        created_at=datetime(2026, 8, 14, 12, 0, 0),
        destroy_lifecycle_nonce="a" * 32,
        cloud_instance_id="i-0123456789abcdef0",
        private_ip="10.0.0.42",
        ccm_port=8000,
        auth_token=AUTH_TOKEN,
        provision_spec=copy.deepcopy(PROVISION_SPEC),
    )


def _signed_proof(claim) -> dict:
    payload = {
        "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
        "nonce": "b" * 32,
        "node_role": "worker",
        "drain_claim": claim.node_drain_claim,
        "runtime_sealed": True,
        "safe_to_destroy": True,
        "blockers": [],
        "blocker_count": 0,
        "task_count": 0,
    }
    return {
        **payload,
        "signature": worker_node_drain_proof_signature(
            payload,
            auth_token=AUTH_TOKEN,
        ),
    }


def _install_receipt(worker: Worker) -> tuple[dict, str]:
    claim = capture_worker_destroy_lifecycle_claim(worker)
    client_token_digest = worker_destroy_client_token_digest(CLIENT_TOKEN)
    receipt = build_worker_destroy_termination_receipt(
        claim,
        _signed_proof(claim),
        cloud_scope=CLOUD_SCOPE,
        provision_spec_digest=worker_destroy_provision_spec_digest(
            worker.provision_spec
        ),
        client_token_digest=client_token_digest,
        authorized_at=datetime(2026, 8, 14, 12, 1, 2, 345678),
    )
    worker.destroy_termination_receipt = receipt
    return receipt, client_token_digest


def _matches(worker: Worker, client_token_digest: str) -> bool:
    return worker_destroy_termination_receipt_matches(
        worker,
        cloud_scope=CLOUD_SCOPE,
        client_token_digest=client_token_digest,
    )


def _rehash(receipt: dict) -> None:
    unsigned = copy.deepcopy(receipt)
    unsigned.pop("receipt_digest", None)
    encoded = json.dumps(
        unsigned,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    receipt["receipt_digest"] = hashlib.sha256(encoded).hexdigest()


def test_signed_destroy_receipt_builds_and_matches_exact_worker() -> None:
    worker = _worker()
    receipt, client_token_digest = _install_receipt(worker)

    assert receipt["version"] == WORKER_DESTROY_TERMINATION_RECEIPT_VERSION
    assert receipt["action"] == WORKER_DESTROY_TERMINATION_ACTION
    assert receipt["cloud_scope"] == CLOUD_SCOPE
    assert receipt["private_ip"] == worker.private_ip
    assert "signature" not in receipt["proof"]
    assert receipt["proof_signature"] == _signed_proof(
        capture_worker_destroy_lifecycle_claim(worker)
    )["signature"]
    assert _matches(worker, client_token_digest)


@pytest.mark.parametrize(
    ("status", "bootstrap_step", "expected"),
    [
        ("destroying", None, True),
        ("ready", "destroy", True),
        ("error", "destroy", True),
        ("ready", None, False),
        ("error", None, False),
        ("stopped", "destroy", False),
        ("terminated", "destroy", False),
    ],
)
def test_destroy_receipt_status_contract(
    status: str,
    bootstrap_step: str | None,
    expected: bool,
) -> None:
    worker = _worker()
    _, client_token_digest = _install_receipt(worker)
    worker.status = status
    worker.bootstrap_step = bootstrap_step

    assert _matches(worker, client_token_digest) is expected


def test_builder_rejects_missing_wrong_or_tampered_proof_signature() -> None:
    worker = _worker()
    claim = capture_worker_destroy_lifecycle_claim(worker)
    kwargs = {
        "cloud_scope": CLOUD_SCOPE,
        "provision_spec_digest": worker_destroy_provision_spec_digest(
            worker.provision_spec
        ),
        "client_token_digest": worker_destroy_client_token_digest(CLIENT_TOKEN),
    }

    missing = _signed_proof(claim)
    missing.pop("signature")
    with pytest.raises(ValueError, match="proof envelope"):
        build_worker_destroy_termination_receipt(claim, missing, **kwargs)

    wrong = _signed_proof(claim)
    wrong["signature"] = "0" * 64
    with pytest.raises(ValueError, match="signed snapshot"):
        build_worker_destroy_termination_receipt(claim, wrong, **kwargs)

    tampered = _signed_proof(claim)
    tampered["task_count"] = 1
    with pytest.raises(ValueError, match="signed snapshot"):
        build_worker_destroy_termination_receipt(claim, tampered, **kwargs)


def test_matcher_reverifies_proof_hmac_after_receipt_digest_recomputed() -> None:
    worker = _worker()
    receipt, client_token_digest = _install_receipt(worker)
    receipt["proof"]["task_count"] = 1
    _rehash(receipt)

    assert not _matches(worker, client_token_digest)

    worker = _worker()
    receipt, client_token_digest = _install_receipt(worker)
    receipt["proof_signature"] = "0" * 64
    _rehash(receipt)

    assert not _matches(worker, client_token_digest)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("version", True),
        ("worker_id", True),
    ],
)
def test_receipt_bool_cannot_impersonate_integer(
    field: str,
    value: object,
) -> None:
    worker = _worker()
    receipt, client_token_digest = _install_receipt(worker)
    receipt[field] = value
    _rehash(receipt)

    assert not _matches(worker, client_token_digest)


def test_uppercase_or_malformed_destroy_nonce_is_rejected() -> None:
    worker = _worker()
    worker.destroy_lifecycle_nonce = "A" * 32
    with pytest.raises(ValueError, match="nonce"):
        capture_worker_destroy_lifecycle_claim(worker)

    worker = _worker()
    receipt, client_token_digest = _install_receipt(worker)
    worker.destroy_lifecycle_nonce = "A" * 32
    receipt["destroy_lifecycle_nonce"] = "A" * 32
    _rehash(receipt)
    assert not _matches(worker, client_token_digest)


@pytest.mark.parametrize("field", ["cloud_instance_id", "private_ip"])
@pytest.mark.parametrize("value", [None, "", "   "])
def test_builder_rejects_missing_cloud_endpoint_identity(
    field: str,
    value: object,
) -> None:
    worker = _worker()
    setattr(worker, field, value)
    claim = capture_worker_destroy_lifecycle_claim(worker)
    proof = _signed_proof(claim)

    with pytest.raises(ValueError, match="lifecycle claim"):
        build_worker_destroy_termination_receipt(
            claim,
            proof,
            cloud_scope=CLOUD_SCOPE,
            provision_spec_digest=worker_destroy_provision_spec_digest(
                worker.provision_spec
            ),
            client_token_digest=worker_destroy_client_token_digest(CLIENT_TOKEN),
        )


def test_action_and_cloud_scope_are_exact_effect_boundaries() -> None:
    worker = _worker()
    receipt, client_token_digest = _install_receipt(worker)
    receipt["action"] = "stop_instance"
    _rehash(receipt)
    assert not _matches(worker, client_token_digest)

    worker = _worker()
    _, client_token_digest = _install_receipt(worker)
    other_scope = dict(CLOUD_SCOPE, account_id="999999999999")
    assert not worker_destroy_termination_receipt_matches(
        worker,
        cloud_scope=other_scope,
        client_token_digest=client_token_digest,
    )

    claim = capture_worker_destroy_lifecycle_claim(worker)
    malformed_scope = dict(CLOUD_SCOPE, extra="not-canonical")
    with pytest.raises(ValueError, match="cloud scope"):
        build_worker_destroy_termination_receipt(
            claim,
            _signed_proof(claim),
            cloud_scope=malformed_scope,
            provision_spec_digest=worker_destroy_provision_spec_digest(
                worker.provision_spec
            ),
            client_token_digest=client_token_digest,
        )


def test_provision_spec_and_client_token_drift_revoke_receipt() -> None:
    worker = _worker()
    _, client_token_digest = _install_receipt(worker)
    worker.provision_spec["overrides"]["instance_type"] = "m7i.large"
    assert not _matches(worker, client_token_digest)

    worker = _worker()
    _, client_token_digest = _install_receipt(worker)
    other_token_digest = worker_destroy_client_token_digest("ccm-other-token")
    assert other_token_digest != client_token_digest
    assert not _matches(worker, other_token_digest)

    worker = _worker()
    worker.provision_spec["cloud_scope"] = dict(
        CLOUD_SCOPE,
        account_id="999999999999",
    )
    claim = capture_worker_destroy_lifecycle_claim(worker)
    client_token_digest = worker_destroy_client_token_digest(CLIENT_TOKEN)
    worker.destroy_termination_receipt = build_worker_destroy_termination_receipt(
        claim,
        _signed_proof(claim),
        cloud_scope=CLOUD_SCOPE,
        provision_spec_digest=worker_destroy_provision_spec_digest(
            worker.provision_spec
        ),
        client_token_digest=client_token_digest,
    )
    assert not _matches(worker, client_token_digest)


@pytest.mark.asyncio
async def test_remote_drain_proof_preserves_verified_signature(
    monkeypatch,
) -> None:
    worker = _worker()
    claim = capture_worker_destroy_lifecycle_claim(worker)
    proxy = WorkerProxy(db_factory=None, relay=None)

    async def _claimed(_claim):
        assert _claim is claim
        return worker

    monkeypatch.setattr(proxy, "_require_destroy_lifecycle_claim", _claimed)
    monkeypatch.setattr(settings, "auth_token", "manager-control-token")

    class Response:
        def __init__(self, body: dict):
            self.body = body

        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict:
            return copy.deepcopy(self.body)

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_args):
            return None

        async def post(self, _url, *, headers, json):
            assert headers["Authorization"] == f"Bearer {AUTH_TOKEN}"
            payload = {
                "protocol_version": WORKER_NODE_DRAIN_PROOF_PROTOCOL,
                "nonce": json["nonce"],
                "node_role": "worker",
                "drain_claim": claim.node_drain_claim,
                "runtime_sealed": True,
                "safe_to_destroy": True,
                "blockers": [],
                "blocker_count": 0,
                "task_count": 0,
            }
            return Response({
                **payload,
                "signature": worker_node_drain_proof_signature(
                    payload,
                    auth_token=AUTH_TOKEN,
                ),
            })

    monkeypatch.setattr(
        worker_proxy_module.httpx,
        "AsyncClient",
        lambda **_kwargs: Client(),
    )

    signed_proof = await proxy.require_claimed_destroy_drain_proof(claim)

    assert "signature" in signed_proof
    receipt = build_worker_destroy_termination_receipt(
        claim,
        signed_proof,
        cloud_scope=CLOUD_SCOPE,
        provision_spec_digest=worker_destroy_provision_spec_digest(
            worker.provision_spec
        ),
        client_token_digest=worker_destroy_client_token_digest(CLIENT_TOKEN),
    )
    assert receipt["proof_signature"] == signed_proof["signature"]
