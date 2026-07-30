"""Typed, provider-neutral tool contracts for agent runtimes."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Mapping

from ..client import EdgarClient


JsonObject = dict[str, Any]
ToolHandler = Callable[[Mapping[str, Any]], Awaitable[Any]]


@dataclass(frozen=True, slots=True)
class AgentToolDefinition:
    """One read-only tool definition with a JSON-Schema-shaped input contract."""

    tool_id: str
    description: str
    input_schema: JsonObject

    def to_dict(self) -> JsonObject:
        return {
            "id": self.tool_id,
            "description": self.description,
            "inputSchema": self.input_schema,
            "sideEffect": "read_only",
        }


@dataclass(frozen=True, slots=True)
class AgentToolResult:
    """An invocation result that distinguishes absence from acquisition failure.

    Library exceptions intentionally propagate rather than being converted to empty data.
    ``not_found`` therefore means a successful SEC query with no matching public record.
    """

    tool_id: str
    status: str
    data: Any

    def to_dict(self) -> JsonObject:
        return {
            "toolId": self.tool_id,
            "status": self.status,
            "data": self.data,
        }


def _object_schema(
    properties: JsonObject,
    *,
    required: tuple[str, ...] = (),
) -> JsonObject:
    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


_TICKER = {
    "type": "string",
    "description": "Public ticker symbol, such as AAPL or GOOGL.",
}
_CIK = {
    "type": "string",
    "description": "SEC Central Index Key, with or without leading zeroes.",
}


class EdgarAgentTools:
    """Expose explicit CopeTech-Edgar reads to any structured-tool runtime."""

    DEFINITIONS = (
        AgentToolDefinition(
            "edgar.company.get",
            "Resolve an issuer and return all SEC-listed share classes and former names.",
            _object_schema({"ticker": _TICKER}, required=("ticker",)),
        ),
        AgentToolDefinition(
            "edgar.filings.query",
            "List point-in-time SEC filing metadata for one issuer and form set.",
            _object_schema(
                {
                    "ticker": _TICKER,
                    "forms": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                    },
                    "days_back": {"type": "integer", "minimum": 0, "default": 365},
                    "limit": {"type": "integer", "minimum": 1},
                },
                required=("ticker", "forms"),
            ),
        ),
        AgentToolDefinition(
            "edgar.ownership.entries",
            "Parse transaction and holding rows from SEC Forms 3, 4, and 5.",
            _object_schema(
                {
                    "ticker": _TICKER,
                    "forms": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": ["3", "3/A", "4", "4/A", "5", "5/A"],
                        },
                    },
                    "days_back": {"type": "integer", "minimum": 0, "default": 730},
                    "filing_limit": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 50,
                    },
                },
                required=("ticker",),
            ),
        ),
        AgentToolDefinition(
            "edgar.financials.series",
            "Return an auditable point-in-time revenue or EPS series with filing provenance.",
            _object_schema(
                {
                    "ticker": _TICKER,
                    "metric": {
                        "type": "string",
                        "enum": ["revenue", "basic_eps", "diluted_eps"],
                    },
                    "frequency": {
                        "type": "string",
                        "enum": ["quarterly", "annual", "ttm"],
                    },
                    "as_of": {
                        "type": "string",
                        "description": "Optional ISO date limiting what was knowable.",
                    },
                },
                required=("ticker", "metric"),
            ),
        ),
        AgentToolDefinition(
            "edgar.financials.concept",
            "Fetch one issuer and one standard XBRL concept from the bounded SEC endpoint.",
            _object_schema(
                {
                    "ticker": _TICKER,
                    "taxonomy": {"type": "string", "default": "us-gaap"},
                    "concept": {"type": "string"},
                },
                required=("ticker", "concept"),
            ),
        ),
        AgentToolDefinition(
            "edgar.financials.frame",
            "Compare one standard XBRL concept across issuers in an SEC calendar frame.",
            _object_schema(
                {
                    "taxonomy": {"type": "string", "default": "us-gaap"},
                    "concept": {"type": "string"},
                    "unit": {"type": "string"},
                    "period": {
                        "type": "string",
                        "description": "CY####, CY####Q#, or CY####Q#I.",
                    },
                },
                required=("concept", "unit", "period"),
            ),
        ),
        AgentToolDefinition(
            "edgar.institutions.latest_holdings",
            "Return the latest parsed Form 13F holdings for an institutional manager.",
            _object_schema(
                {
                    "cik": _CIK,
                    "row_limit": {"type": "integer", "minimum": 1},
                },
                required=("cik",),
            ),
        ),
    )

    def __init__(self, client: EdgarClient):
        self.client = client
        self._handlers: dict[str, ToolHandler] = {
            "edgar.company.get": self._company_get,
            "edgar.filings.query": self._filings_query,
            "edgar.ownership.entries": self._ownership_entries,
            "edgar.financials.series": self._financial_series,
            "edgar.financials.concept": self._financial_concept,
            "edgar.financials.frame": self._financial_frame,
            "edgar.institutions.latest_holdings": self._latest_holdings,
        }

    def manifest(self) -> list[JsonObject]:
        return [definition.to_dict() for definition in self.DEFINITIONS]

    async def invoke(
        self,
        tool_id: str,
        arguments: Mapping[str, Any],
    ) -> AgentToolResult:
        handler = self._handlers.get(tool_id)
        if handler is None:
            raise KeyError(f"Unknown CopeTech-Edgar tool id: {tool_id}")
        data = await handler(arguments)
        return AgentToolResult(
            tool_id=tool_id,
            status="not_found" if data is None else "ok",
            data=data,
        )

    async def _company_get(self, arguments: Mapping[str, Any]) -> Any:
        return await self.client.companies.get(str(arguments["ticker"]))

    async def _filings_query(self, arguments: Mapping[str, Any]) -> Any:
        return await self.client.filings.query(
            str(arguments["ticker"]),
            list(arguments["forms"]),
            days_back=int(arguments.get("days_back", 365)),
            limit=(
                int(arguments["limit"])
                if arguments.get("limit") is not None
                else None
            ),
        )

    async def _ownership_entries(self, arguments: Mapping[str, Any]) -> Any:
        kwargs: JsonObject = {
            "days_back": int(arguments.get("days_back", 730)),
            "filing_limit": int(arguments.get("filing_limit", 50)),
        }
        if arguments.get("forms") is not None:
            kwargs["forms"] = list(arguments["forms"])
        return await self.client.ownership.entries(
            str(arguments["ticker"]),
            **kwargs,
        )

    async def _financial_series(self, arguments: Mapping[str, Any]) -> Any:
        return await self.client.financials.series(
            str(arguments["ticker"]),
            metric=str(arguments["metric"]),
            frequency=str(arguments.get("frequency", "quarterly")),
            as_of=(
                str(arguments["as_of"])
                if arguments.get("as_of") is not None
                else None
            ),
        )

    async def _financial_concept(self, arguments: Mapping[str, Any]) -> Any:
        return await self.client.financials.concept(
            str(arguments["ticker"]),
            taxonomy=str(arguments.get("taxonomy", "us-gaap")),
            concept=str(arguments["concept"]),
        )

    async def _financial_frame(self, arguments: Mapping[str, Any]) -> Any:
        return await self.client.financials.frame(
            taxonomy=str(arguments.get("taxonomy", "us-gaap")),
            concept=str(arguments["concept"]),
            unit=str(arguments["unit"]),
            period=str(arguments["period"]),
        )

    async def _latest_holdings(self, arguments: Mapping[str, Any]) -> Any:
        return await self.client.institutions.latest_holdings(
            str(arguments["cik"]),
            row_limit=(
                int(arguments["row_limit"])
                if arguments.get("row_limit") is not None
                else None
            ),
        )
