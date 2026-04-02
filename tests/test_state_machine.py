"""
tests/test_state_machine.py — Settlement state machine tests.

Covers: valid transitions, invalid transitions (409),
terminal states, and FAILED->PENDING retry.
"""

import pytest
from fastapi import HTTPException

from shared.state_machine import (
    RTGS_VALID_TRANSITIONS,
    FX_VALID_TRANSITIONS,
    validate_transition,
)


class TestRTGSTransitions:
    def test_pending_to_approved(self):
        validate_transition("pending", "approved", RTGS_VALID_TRANSITIONS)

    def test_approved_to_signed(self):
        validate_transition("approved", "signed", RTGS_VALID_TRANSITIONS)

    def test_signed_to_processing(self):
        validate_transition("signed", "processing", RTGS_VALID_TRANSITIONS)

    def test_processing_to_broadcasted(self):
        validate_transition("processing", "broadcasted", RTGS_VALID_TRANSITIONS)

    def test_broadcasted_to_confirmed(self):
        validate_transition("broadcasted", "confirmed", RTGS_VALID_TRANSITIONS)

    def test_confirmed_to_settled(self):
        validate_transition("confirmed", "settled", RTGS_VALID_TRANSITIONS)

    def test_failed_to_pending_retry(self):
        validate_transition("failed", "pending", RTGS_VALID_TRANSITIONS)

    def test_any_to_cancelled(self):
        for state in ("pending", "approved", "signed"):
            validate_transition(state, "cancelled", RTGS_VALID_TRANSITIONS)

    def test_invalid_pending_to_settled(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_transition("pending", "settled", RTGS_VALID_TRANSITIONS)
        assert exc_info.value.status_code == 409

    def test_invalid_settled_to_anything(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_transition("settled", "pending", RTGS_VALID_TRANSITIONS)
        assert exc_info.value.status_code == 409

    def test_terminal_cancelled(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_transition("cancelled", "pending", RTGS_VALID_TRANSITIONS)
        assert exc_info.value.status_code == 409

    def test_case_insensitive(self):
        validate_transition("PENDING", "APPROVED", RTGS_VALID_TRANSITIONS)


class TestFXTransitions:
    def test_queued_to_processing(self):
        validate_transition("queued", "processing", FX_VALID_TRANSITIONS)

    def test_processing_to_settled(self):
        validate_transition("processing", "settled", FX_VALID_TRANSITIONS)

    def test_processing_to_failed(self):
        validate_transition("processing", "failed", FX_VALID_TRANSITIONS)

    def test_failed_to_queued_retry(self):
        validate_transition("failed", "queued", FX_VALID_TRANSITIONS)

    def test_invalid_queued_to_settled(self):
        with pytest.raises(HTTPException) as exc_info:
            validate_transition("queued", "settled", FX_VALID_TRANSITIONS)
        assert exc_info.value.status_code == 409
