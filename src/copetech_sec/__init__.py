"""Reusable SEC EDGAR backend extracted from Sentinel."""

from .sec_api import SECDataFetcher
from .errors import (
    CopeTechEdgarError,
    SecAccessDeniedError,
    SecMalformedResponseError,
    SecNotFoundError,
    SecRateLimitError,
    SecRequestError,
    SecResponseTooLargeError,
    SecTransportError,
)
from .identifiers import Accession, Cik, Ticker

__all__ = [
    "Accession",
    "Cik",
    "CopeTechEdgarError",
    "SECDataFetcher",
    "SecAccessDeniedError",
    "SecMalformedResponseError",
    "SecNotFoundError",
    "SecRateLimitError",
    "SecRequestError",
    "SecResponseTooLargeError",
    "SecTransportError",
    "Ticker",
]
