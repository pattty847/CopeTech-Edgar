"""Public error taxonomy for SEC acquisition and resource boundaries."""

from __future__ import annotations


class CopeTechEdgarError(Exception):
    """Base class for failures callers may handle intentionally."""


class SecRequestError(CopeTechEdgarError):
    """A request to an SEC-owned endpoint failed."""

    def __init__(
        self,
        message: str,
        *,
        url: str,
        status_code: int | None = None,
        retryable: bool = False,
    ):
        super().__init__(message)
        self.url = url
        self.status_code = status_code
        self.retryable = retryable


class SecNotFoundError(SecRequestError):
    """The requested SEC resource does not exist."""


class SecAccessDeniedError(SecRequestError):
    """SEC rejected the request or caller identity."""


class SecRateLimitError(SecRequestError):
    """SEC continued throttling after the configured retry budget."""


class SecTransportError(SecRequestError):
    """Connection or timeout failure after the configured retry budget."""


class SecMalformedResponseError(SecRequestError):
    """A successful response did not satisfy the requested representation."""


class SecResponseTooLargeError(SecRequestError):
    """A response exceeded the configured bounded-read ceiling."""
