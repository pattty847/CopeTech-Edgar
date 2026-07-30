"""Ownership XML acquisition, parsing, normalization, and signal boundaries."""

from .normalization import (
    ACQUISITION_CODE_HINTS,
    DISPOSITION_CODE_HINTS,
    TRANSACTION_CODE_MAP,
    resolve_direction,
)
from .parser import OwnershipXmlParser
from .service import OwnershipFilingService, ownership_payload_fingerprint
from .signals import OwnershipSignalClassifier

__all__ = [
    "ACQUISITION_CODE_HINTS",
    "DISPOSITION_CODE_HINTS",
    "OwnershipFilingService",
    "OwnershipSignalClassifier",
    "OwnershipXmlParser",
    "TRANSACTION_CODE_MAP",
    "ownership_payload_fingerprint",
    "resolve_direction",
]
