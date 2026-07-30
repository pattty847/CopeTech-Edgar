"""SEC submissions acquisition and complete filing-history normalization."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import re
from typing import Any, Iterable

from .cache_manager import SecCacheManager
from .errors import SecNotFoundError
from .http_client import SecHttpClient
from .identifiers import Accession, Cik


_HISTORY_FILE_PATTERN = re.compile(r"^CIK\d{10}-submissions-\d{3,}\.json$")


@dataclass(frozen=True)
class FilingSearchResult:
    items: list[dict[str, Any]]
    retrieved_at: str
    source_files: tuple[str, ...]
    truncated: bool
    next_cursor: str | None
    warnings: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {
            "items": self.items,
            "metadata": {
                "retrievedAt": self.retrieved_at,
                "source": "sec-submissions",
                "sourceFiles": list(self.source_files),
                "truncated": self.truncated,
                "nextCursor": self.next_cursor,
                "warnings": list(self.warnings),
            },
        }


class SubmissionsResource:
    """Queries recent and historical submissions files as one filing timeline."""

    HISTORY_ENDPOINT = "https://data.sec.gov/submissions/{name}"

    def __init__(self, http_client: SecHttpClient, cache_manager: SecCacheManager):
        self.http_client = http_client
        self.cache_manager = cache_manager

    async def query_filings(
        self,
        submissions: dict[str, Any],
        *,
        cik: str,
        forms: Iterable[str],
        days_back: int,
        use_cache: bool,
        limit: int | None = None,
        cursor: str | None = None,
    ) -> FilingSearchResult:
        if days_back < 0:
            raise ValueError("days_back cannot be negative.")
        if limit is not None and limit <= 0:
            raise ValueError("limit must be greater than zero.")
        offset = self._parse_cursor(cursor)
        normalized_cik = Cik(cik)
        requested_forms = frozenset(form for form in forms if form)
        if not requested_forms:
            raise ValueError("At least one form type is required.")

        cutoff = datetime.now(timezone.utc).date() - timedelta(days=days_back)
        primary_name = f"CIK{normalized_cik}.json"
        filings_node = submissions.get("filings")
        if not isinstance(filings_node, dict):
            raise ValueError("Submissions payload is missing the filings object.")
        recent = filings_node.get("recent")
        if not isinstance(recent, dict):
            raise ValueError("Submissions payload is missing filings.recent.")

        source_files = [primary_name]
        warnings: list[str] = []
        records = self._records_from_block(
            recent,
            cik=normalized_cik,
            source_file=primary_name,
            warnings=warnings,
        )

        history_descriptors = filings_node.get("files") or []
        if not isinstance(history_descriptors, list):
            raise ValueError("submissions.filings.files must be a list.")
        for descriptor in history_descriptors:
            if not isinstance(descriptor, dict):
                warnings.append("invalid_historical_file_descriptor")
                continue
            name = str(descriptor.get("name") or "")
            filing_to = str(descriptor.get("filingTo") or "")
            if filing_to and filing_to < cutoff.isoformat():
                continue
            if not _HISTORY_FILE_PATTERN.fullmatch(name):
                warnings.append(f"invalid_historical_file_name:{name or '<missing>'}")
                continue
            payload = await self._load_history_file(
                normalized_cik,
                name,
                use_cache=use_cache,
            )
            if payload is None:
                warnings.append(f"historical_file_not_found:{name}")
                continue
            source_files.append(name)
            records.extend(
                self._records_from_block(
                    payload,
                    cik=normalized_cik,
                    source_file=name,
                    warnings=warnings,
                )
            )

        filtered = [
            record
            for record in records
            if record["form"] in requested_forms
            and record["filing_date"] >= cutoff.isoformat()
        ]
        deduped = {
            record["accession_no"]: record
            for record in filtered
        }
        ordered = sorted(
            deduped.values(),
            key=lambda record: (record["filing_date"], record["accession_no"]),
            reverse=True,
        )
        page_end = len(ordered) if limit is None else offset + limit
        page = ordered[offset:page_end]
        next_cursor = str(page_end) if page_end < len(ordered) else None
        return FilingSearchResult(
            items=page,
            retrieved_at=datetime.now(timezone.utc).isoformat(),
            source_files=tuple(source_files),
            truncated=next_cursor is not None,
            next_cursor=next_cursor,
            warnings=tuple(sorted(set(warnings))),
        )

    async def _load_history_file(
        self,
        cik: Cik,
        name: str,
        *,
        use_cache: bool,
    ) -> dict[str, Any] | None:
        cache_key = f"CIK{cik}"
        if use_cache:
            cached = await self.cache_manager.load_data(
                cache_key,
                "submissions_history",
                history_file=name,
            )
            if isinstance(cached, dict):
                return cached
        try:
            payload = await self.http_client.make_request(
                self.HISTORY_ENDPOINT.format(name=name),
                is_json=True,
            )
        except SecNotFoundError:
            return None
        if not isinstance(payload, dict):
            raise TypeError(
                f"Historical submissions file {name} must be a dict, "
                f"got {type(payload).__name__}."
            )
        await self.cache_manager.save_data(
            cache_key,
            "submissions_history",
            payload,
            history_file=name,
        )
        return payload

    @staticmethod
    def _parse_cursor(cursor: str | None) -> int:
        if cursor is None:
            return 0
        if not cursor.isdigit():
            raise ValueError("cursor must be a non-negative integer offset.")
        return int(cursor)

    @staticmethod
    def _records_from_block(
        block: dict[str, Any],
        *,
        cik: Cik,
        source_file: str,
        warnings: list[str],
    ) -> list[dict[str, Any]]:
        required = ("form", "filingDate", "accessionNumber", "reportDate")
        arrays: dict[str, list[Any]] = {}
        for field in required:
            value = block.get(field)
            if not isinstance(value, list):
                warnings.append(f"missing_or_invalid_filing_array:{source_file}:{field}")
                return []
            arrays[field] = value
        row_count = min(len(arrays[field]) for field in required)
        if len({len(arrays[field]) for field in required}) != 1:
            warnings.append(f"uneven_filing_arrays:{source_file}")

        optional_fields = {
            "primaryDocument": block.get("primaryDocument") or [],
            "primaryDocDescription": block.get("primaryDocDescription") or [],
            "items": block.get("items") or [],
        }
        records: list[dict[str, Any]] = []
        for index in range(row_count):
            try:
                accession = Accession(arrays["accessionNumber"][index])
            except ValueError:
                warnings.append(f"invalid_accession:{source_file}:{index}")
                continue
            filing_date = str(arrays["filingDate"][index] or "")
            try:
                date.fromisoformat(filing_date)
            except ValueError:
                warnings.append(f"invalid_filing_date:{source_file}:{index}")
                continue
            records.append(
                {
                    "accession_no": str(accession),
                    "filing_date": filing_date,
                    "form": str(arrays["form"][index] or ""),
                    "report_date": str(arrays["reportDate"][index] or ""),
                    "url": (
                        "https://www.sec.gov/Archives/edgar/data/"
                        f"{cik.archive_path}/{accession.compact}/"
                    ),
                    "primary_document": SubmissionsResource._optional_at(
                        optional_fields["primaryDocument"],
                        index,
                    ),
                    "primary_document_description": SubmissionsResource._optional_at(
                        optional_fields["primaryDocDescription"],
                        index,
                    ),
                    "items": SubmissionsResource._optional_at(
                        optional_fields["items"],
                        index,
                    ) or "",
                    "source_file": source_file,
                }
            )
        return records

    @staticmethod
    def _optional_at(values: Any, index: int) -> Any:
        return values[index] if isinstance(values, list) and index < len(values) else None
