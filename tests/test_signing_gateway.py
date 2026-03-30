"""
tests/test_signing_gateway.py — Tests for the signing gateway and MPC nodes.

Tests the signing logic in isolation without requiring running services.
Covers:
  - Threshold signature collection
  - Partial signature determinism
  - Insufficient partials error
"""

import hashlib
import json

import pytest


def compute_partial_signature(node_id: str, payload: dict) -> str:
    """Reproduce the MPC node's deterministic partial signature logic."""
    sorted_payload = json.dumps(payload, sort_keys=True).encode()
    return hashlib.sha256(node_id.encode() + sorted_payload).hexdigest()


def collect_signatures(
    node_ids: list[str],
    payload: dict,
    threshold: int,
) -> dict:
    """Simulate the signing gateway's signature collection logic."""
    partials = []
    for node_id in node_ids:
        sig = compute_partial_signature(node_id, payload)
        partials.append({"node_id": node_id, "partial": sig})

    if len(partials) < threshold:
        raise ValueError(
            f"Insufficient partials: got {len(partials)}, need {threshold}"
        )

    combined = hashlib.sha256(
        "".join(p["partial"] for p in sorted(partials, key=lambda p: p["node_id"])).encode()
    ).hexdigest()
    return {"signature": f"0x{combined}", "partials": len(partials)}


class TestPartialSignature:

    def test_deterministic_output(self):
        payload = {"transaction_id": "FX-001", "amount": "1000"}
        sig1 = compute_partial_signature("node-1", payload)
        sig2 = compute_partial_signature("node-1", payload)
        assert sig1 == sig2

    def test_different_nodes_produce_different_partials(self):
        payload = {"transaction_id": "FX-001", "amount": "1000"}
        sig1 = compute_partial_signature("node-1", payload)
        sig2 = compute_partial_signature("node-2", payload)
        assert sig1 != sig2

    def test_different_payloads_produce_different_partials(self):
        sig1 = compute_partial_signature("node-1", {"tx": "A"})
        sig2 = compute_partial_signature("node-1", {"tx": "B"})
        assert sig1 != sig2

    def test_partial_is_hex_string(self):
        sig = compute_partial_signature("node-1", {"data": "test"})
        assert len(sig) == 64
        int(sig, 16)


class TestThresholdCollection:

    def test_three_of_three_succeeds(self):
        result = collect_signatures(
            ["node-1", "node-2", "node-3"],
            {"transaction_id": "FX-002"},
            threshold=2,
        )
        assert result["signature"].startswith("0x")
        assert result["partials"] == 3

    def test_two_of_three_threshold(self):
        result = collect_signatures(
            ["node-1", "node-2"],
            {"transaction_id": "FX-003"},
            threshold=2,
        )
        assert result["partials"] == 2

    def test_insufficient_partials_raises(self):
        with pytest.raises(ValueError, match="Insufficient"):
            collect_signatures(
                ["node-1"],
                {"transaction_id": "FX-004"},
                threshold=2,
            )

    def test_signature_format(self):
        result = collect_signatures(
            ["node-1", "node-2", "node-3"],
            {"settlement_ref": "FXS-TEST"},
            threshold=2,
        )
        sig = result["signature"]
        assert sig.startswith("0x")
        assert len(sig) == 66

    def test_ordering_does_not_affect_result(self):
        payload = {"id": "FX-005"}
        r1 = collect_signatures(["node-1", "node-2", "node-3"], payload, 2)
        r2 = collect_signatures(["node-3", "node-1", "node-2"], payload, 2)
        assert r1["signature"] == r2["signature"]
