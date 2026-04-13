"""
shared/blockchain_sim.py — Simulated blockchain recording for
demo and sandbox environments.

Generates deterministic, realistic-looking blockchain artifacts
(tx hashes, block numbers, block hashes) without requiring an
actual blockchain RPC connection.

Every completed transaction is "recorded" on a simulated private
L2 chain, producing a receipt with:
  - tx_hash      : 0x-prefixed 64-char hex hash
  - block_number : monotonically increasing integer
  - block_hash   : 0x-prefixed 64-char hex hash
  - confirmations: always 1 (instant finality on private chain)
  - gas_used     : deterministic from transaction data
  - network      : "stablecoin-l2"

Hybrid architecture note:
  The blockchain layer and the traditional fiat rail are complementary,
  not alternatives. Every settlement records the token movement on-chain
  unconditionally. The fiat clearing leg (FedWire, SWIFT, TARGET2, etc.)
  is recorded separately via record_fiat_rail(). Both receipts are stored
  on the settlement record — the blockchain receipt for the token leg,
  the fiat receipt for the underlying cash settlement.
"""

import hashlib
import threading

_lock = threading.Lock()
_block_counter = 19_500_000


def _hex_hash(seed: str) -> str:
    """Generate a 0x-prefixed 64-char hex hash from a seed."""
    return "0x" + hashlib.sha256(seed.encode()).hexdigest()


def record_on_chain(
    transaction_id: str,
    operation: str,
    extra_seed: str = "",
) -> dict:
    """Simulate recording a transaction on a private L2 chain.

    Args:
        transaction_id: Unique reference for the transaction.
        operation: Type of operation (e.g. "token_issuance").
        extra_seed: Additional data for hash uniqueness.

    Returns:
        Receipt dict with tx_hash, block_number, block_hash,
        confirmations, gas_used, and network.
    """
    global _block_counter

    with _lock:
        _block_counter += 1
        block_num = _block_counter

    seed = f"{transaction_id}:{operation}:{extra_seed}"
    tx_hash = _hex_hash(seed)
    block_hash = _hex_hash(f"block:{block_num}:{seed}")

    gas_seed = int(
        hashlib.md5(seed.encode()).hexdigest()[:4], 16
    )
    gas_used = 21_000 + (gas_seed % 80_000)

    return {
        "tx_hash": tx_hash,
        "block_number": block_num,
        "block_hash": block_hash,
        "confirmations": 1,
        "gas_used": gas_used,
        "network": "stablecoin-l2",
    }


# Rail-specific reference formats (simulated)
_FIAT_RAIL_FORMATS = {
    "fedwire":  lambda ref: f"FW{ref[:12].upper()}",
    "swift":    lambda ref: f"SWIFT-{ref[:8].upper()}-{ref[8:16].upper()}",
    "target2":  lambda ref: f"T2-{ref[:16].upper()}",
    "internal": lambda ref: f"INT-{ref[:16].upper()}",
    "blockchain": None,  # no separate fiat leg
}


def record_fiat_rail(transaction_id: str, rail: str) -> dict:
    """Simulate the fiat clearing leg for a given settlement rail.

    In a real institution, this would be the FedWire IMAD/OMAD,
    SWIFT UETR, or TARGET2 transaction reference returned by the
    actual payment rail. Here we generate a deterministic reference.

    Returns None for 'blockchain' rail (no fiat leg).
    """
    formatter = _FIAT_RAIL_FORMATS.get(rail)
    if formatter is None:
        return None

    seed = f"{transaction_id}:{rail}:fiat"
    ref_seed = hashlib.sha256(seed.encode()).hexdigest()
    ref = formatter(ref_seed)

    return {
        "rail": rail,
        "reference": ref,
        "status": "settled",
        "settled_at": None,  # populated by caller with real timestamp
    }
