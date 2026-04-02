"""
shared/state_machine.py — Settlement state machine transition validation.

Defines valid state transitions for RTGS and FX settlements and provides
a validation function that raises HTTP 409 on invalid transitions.
"""

from fastapi import HTTPException, status


RTGS_VALID_TRANSITIONS: dict[str, set[str]] = {
    "pending":     {"approved", "cancelled"},
    "approved":    {"signed", "cancelled"},
    "signed":      {"processing", "cancelled"},
    "processing":  {"broadcasted", "failed"},
    "broadcasted": {"confirmed", "failed"},
    "confirmed":   {"settled"},
    "settled":     set(),
    "failed":      {"pending"},
    "cancelled":   set(),
}

FX_VALID_TRANSITIONS: dict[str, set[str]] = {
    "queued":     {"processing", "cancelled"},
    "processing": {"settled", "failed"},
    "settled":    set(),
    "failed":     {"queued"},
    "cancelled":  set(),
}


def validate_transition(
    current: str,
    target: str,
    transition_map: dict[str, set[str]],
) -> None:
    """Validate a state transition; raise 409 if invalid."""
    current_lower = current.lower()
    target_lower = target.lower()
    allowed = transition_map.get(current_lower, set())
    if target_lower not in allowed:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                f"Invalid transition: {current} -> {target}. "
                f"Allowed from '{current}': {sorted(allowed) or 'none'}"
            ),
        )
