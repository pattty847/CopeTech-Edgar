#!/usr/bin/env python3
"""Tests for SecHttpClient retry, rate-limit, user-agent, and parsing behavior.

Avoids real network IO by injecting a fake aiohttp session via `_get_session`.
"""

from __future__ import annotations

import asyncio
import time
import unittest
from typing import Any

import aiohttp
from multidict import CIMultiDict, CIMultiDictProxy
from yarl import URL

from copetech_sec.http_client import SecHttpClient


class FakeResponse:
    def __init__(self, status: int, body: str = "", headers: dict[str, str] | None = None):
        self.status = status
        self._body = body
        self.headers = headers or {}

    async def text(self) -> str:
        return self._body

    def raise_for_status(self) -> None:
        if self.status >= 400 and self.status != 429:
            request_info = aiohttp.RequestInfo(
                url=URL("http://example.com"),
                method="GET",
                headers=CIMultiDictProxy(CIMultiDict()),
                real_url=URL("http://example.com"),
            )
            raise aiohttp.ClientResponseError(
                request_info=request_info,
                history=(),
                status=self.status,
                message=str(self.status),
            )


class FakeGetContext:
    def __init__(self, response: FakeResponse):
        self.response = response

    async def __aenter__(self) -> FakeResponse:
        return self.response

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class FakeSession:
    def __init__(self, responses: list[FakeResponse]):
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self.closed = False

    def get(self, url: str, headers: dict[str, str] | None = None, timeout: int | None = None) -> FakeGetContext:
        self.calls.append({"url": url, "headers": dict(headers or {}), "timeout": timeout})
        if not self._responses:
            return FakeGetContext(FakeResponse(500, ""))
        return FakeGetContext(self._responses.pop(0))

    async def close(self) -> None:
        self.closed = True


def _install_fake_session(client: SecHttpClient, session: FakeSession) -> None:
    async def _fake() -> FakeSession:
        return session

    client._get_session = _fake  # type: ignore[assignment]
    client._get_archive_session = _fake  # type: ignore[assignment]


def _patch_sleep(monkey_targets: list[Any]) -> list[float]:
    """Replace asyncio.sleep on the http_client module with a recorder so retry tests are fast."""
    sleeps: list[float] = []

    async def _fast_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    for module in monkey_targets:
        module.asyncio.sleep = _fast_sleep  # type: ignore[attr-defined]
    return sleeps


class UserAgentTests(unittest.TestCase):
    def test_normalizes_name_email_to_compliant_format(self):
        client = SecHttpClient(user_agent="Jane Doe jane@example.com")
        self.assertIn("Jane Doe/1.0", client.user_agent)
        self.assertIn("(jane@example.com)", client.user_agent)

    def test_preserves_already_formatted_user_agent(self):
        formatted = "Acme/2.3 (admin@acme.com)"
        client = SecHttpClient(user_agent=formatted)
        self.assertEqual(client.user_agent, formatted)

    def test_default_header_includes_user_agent(self):
        client = SecHttpClient(user_agent="X X x@y.z")
        self.assertIn("User-Agent", client.default_headers)
        self.assertTrue(client.default_headers["User-Agent"])

    def test_close_is_safe_without_initialized_session(self):
        client = SecHttpClient(user_agent="X X x@y.z")
        asyncio.run(client.close())


class MakeRequestTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        from copetech_sec import http_client as http_client_module

        self._module = http_client_module
        self._original_sleep = http_client_module.asyncio.sleep
        self._sleeps = _patch_sleep([http_client_module])

    def tearDown(self):
        self._module.asyncio.sleep = self._original_sleep

    async def _make_client(self, responses: list[FakeResponse]) -> tuple[SecHttpClient, FakeSession]:
        client = SecHttpClient(user_agent="Test test@example.com", rate_limit_sleep=0.0)
        session = FakeSession(responses)
        _install_fake_session(client, session)
        return client, session

    async def test_returns_dict_when_response_is_json(self):
        client, _ = await self._make_client([FakeResponse(200, '{"ok": true, "n": 3}')])
        result = await client.make_request("https://data.sec.gov/x", is_json=True)
        self.assertEqual(result, {"ok": True, "n": 3})

    async def test_returns_text_when_response_does_not_look_like_json(self):
        client, _ = await self._make_client([FakeResponse(200, "<html>not json</html>")])
        result = await client.make_request("https://data.sec.gov/x", is_json=True)
        self.assertEqual(result, "<html>not json</html>")

    async def test_returns_text_when_is_json_false(self):
        client, _ = await self._make_client([FakeResponse(200, "raw body")])
        result = await client.make_request("https://data.sec.gov/x", is_json=False)
        self.assertEqual(result, "raw body")

    async def test_429_retries_then_succeeds(self):
        client, session = await self._make_client(
            [
                FakeResponse(429, "", headers={"Retry-After": "1"}),
                FakeResponse(429, "", headers={"Retry-After": "1"}),
                FakeResponse(200, '{"ok": true}'),
            ]
        )
        result = await client.make_request("https://data.sec.gov/x", max_retries=5, is_json=True)
        self.assertEqual(result, {"ok": True})
        self.assertEqual(len(session.calls), 3)

    async def test_404_returns_none_without_retrying(self):
        client, session = await self._make_client(
            [
                FakeResponse(404, ""),
                FakeResponse(200, '{"ok": true}'),  # should never be consumed
            ]
        )
        result = await client.make_request("https://data.sec.gov/missing", max_retries=4)
        self.assertIsNone(result)
        self.assertEqual(len(session.calls), 1)

    async def test_403_returns_none_without_retrying(self):
        client, session = await self._make_client([FakeResponse(403, "")])
        result = await client.make_request("https://data.sec.gov/forbidden", max_retries=3)
        self.assertIsNone(result)
        self.assertEqual(len(session.calls), 1)

    async def test_500_retries_up_to_max_then_returns_none(self):
        client, session = await self._make_client(
            [FakeResponse(500, ""), FakeResponse(500, ""), FakeResponse(500, "")]
        )
        result = await client.make_request("https://data.sec.gov/x", max_retries=3)
        self.assertIsNone(result)
        self.assertEqual(len(session.calls), 3)

    async def test_user_agent_propagates_to_request_headers(self):
        client, session = await self._make_client([FakeResponse(200, "{}")])
        await client.make_request("https://data.sec.gov/x")
        self.assertEqual(len(session.calls), 1)
        self.assertIn("User-Agent", session.calls[0]["headers"])
        self.assertIn("@example.com", session.calls[0]["headers"]["User-Agent"])

    async def test_custom_headers_replace_defaults(self):
        client, session = await self._make_client([FakeResponse(200, "{}")])
        custom = {"User-Agent": "Custom UA", "Host": "www.sec.gov"}
        await client.make_request("https://www.sec.gov/x", headers=custom)
        self.assertEqual(session.calls[0]["headers"]["User-Agent"], "Custom UA")
        self.assertEqual(session.calls[0]["headers"]["Host"], "www.sec.gov")


class RateLimitSleepTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_interval_is_honored_between_calls(self):
        from copetech_sec import http_client as http_client_module

        recorded: list[float] = []
        original_sleep = http_client_module.asyncio.sleep

        async def recording_sleep(seconds: float) -> None:
            recorded.append(seconds)

        http_client_module.asyncio.sleep = recording_sleep
        try:
            client = SecHttpClient(user_agent="X X x@y.z", rate_limit_sleep=0.5)
            session = FakeSession([FakeResponse(200, "{}"), FakeResponse(200, "{}")])
            _install_fake_session(client, session)

            await client.make_request("https://data.sec.gov/a")
            client.last_request_time = time.time()
            await client.make_request("https://data.sec.gov/b")
        finally:
            http_client_module.asyncio.sleep = original_sleep

        self.assertTrue(any(s >= 0.4 for s in recorded), f"expected a >=0.4s rate-limit sleep, got {recorded}")


if __name__ == "__main__":
    unittest.main()
