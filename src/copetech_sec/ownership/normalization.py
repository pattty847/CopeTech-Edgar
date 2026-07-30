"""Canonical ownership transaction semantics."""

from __future__ import annotations


TRANSACTION_CODE_MAP = {
    "P": "Open market or private purchase",
    "S": "Open market or private sale",
    "V": "Voluntary early report",
    "A": "Grant, award or other acquisition",
    "D": "Disposition to the issuer",
    "F": "Payment of exercise price or tax by delivering/withholding securities",
    "I": "Discretionary transaction",
    "M": "Exercise or conversion of derivative security exempt under Rule 16b-3",
    "C": "Conversion of derivative security",
    "E": "Expiration of short derivative position",
    "H": "Expiration (or cancellation) of long derivative position",
    "O": "Exercise of out-of-the-money derivative security",
    "X": "Exercise of in-the-money or at-the-money derivative security",
    "G": "Bona fide gift",
    "J": "Other acquisition or disposition",
    "K": "Transaction in equity swap or similar instrument",
    "L": "Small acquisition under Rule 16a-6",
    "U": "Disposition pursuant to a tender of shares",
    "W": "Acquisition or disposition by will or the laws of descent and distribution",
    "Z": "Deposit into or withdrawal from voting trust",
}
ACQUISITION_CODE_HINTS = frozenset({"P", "A", "L"})
DISPOSITION_CODE_HINTS = frozenset({"S", "D", "F", "U", "E", "H"})


def resolve_direction(
    acquired_disposed_code: str | None,
    transaction_code: str | None,
) -> tuple[bool, bool]:
    """Return acquisition/disposition flags, preferring the filing's A/D value."""

    direction = (acquired_disposed_code or "").strip().upper()
    if direction == "A":
        return True, False
    if direction == "D":
        return False, True
    hint = (transaction_code or "").strip().upper()
    if hint in ACQUISITION_CODE_HINTS:
        return True, False
    if hint in DISPOSITION_CODE_HINTS:
        return False, True
    return False, False
