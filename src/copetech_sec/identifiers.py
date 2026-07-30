"""Validated identifiers used at SEC and filesystem trust boundaries."""

from __future__ import annotations

import re


_ACCESSION_PATTERN = re.compile(r"^\d{10}-\d{2}-\d{6}$")
_COMPACT_ACCESSION_PATTERN = re.compile(r"^\d{18}$")
_TICKER_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,31}$")


class Cik(str):
    """Canonical ten-digit SEC Central Index Key."""

    def __new__(cls, value: object) -> "Cik":
        raw = str(value).strip()
        if not raw.isdigit() or not 1 <= len(raw) <= 10 or int(raw) == 0:
            raise ValueError("CIK must contain 1-10 digits and cannot be zero.")
        return super().__new__(cls, raw.zfill(10))

    @property
    def archive_path(self) -> str:
        return self.lstrip("0")


class Accession(str):
    """Canonical dashed SEC accession number."""

    def __new__(cls, value: object) -> "Accession":
        raw = str(value).strip()
        if _COMPACT_ACCESSION_PATTERN.fullmatch(raw):
            raw = f"{raw[:10]}-{raw[10:12]}-{raw[12:]}"
        if not _ACCESSION_PATTERN.fullmatch(raw):
            raise ValueError(
                "Accession number must match ##########-##-###### "
                "or contain the equivalent 18 digits."
            )
        return super().__new__(cls, raw)

    @property
    def compact(self) -> str:
        return self.replace("-", "")

    @property
    def submitter_cik(self) -> Cik:
        return Cik(self[:10])


class Ticker(str):
    """Normalized ticker accepted by SEC company-ticker lookup APIs."""

    def __new__(cls, value: object) -> "Ticker":
        raw = str(value).strip().upper()
        if not _TICKER_PATTERN.fullmatch(raw):
            raise ValueError(
                "Ticker must be 1-32 characters using letters, digits, '.' or '-'."
            )
        return super().__new__(cls, raw)
