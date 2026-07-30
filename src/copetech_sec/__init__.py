"""Reusable SEC EDGAR backend extracted from Sentinel."""

from .client import EdgarClient
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
    "EdgarClient",
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
