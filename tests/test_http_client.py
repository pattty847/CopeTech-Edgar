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
from copetech_sec.errors import (
    SecAccessDeniedError,
    SecMalformedResponseError,
    SecNotFoundError,
    SecRequestError,
    SecResponseTooLargeError,
)


class FakeContent:
    """Stand-in for `aiohttp.ClientResponse.content`, which the client reads through so
    response bodies stay byte-bounded instead of being inflated wholesale by `.text()`."""

    def __init__(self, body: bytes, *, max_chunk_size: int | None = None):
        self._body = body
        self._offset = 0
        self._max_chunk_size = max_chunk_size

    async def read(self, limit: int = -1) -> bytes:
        if self._offset >= len(self._body):
            return b""
        if limit is None or limit < 0:
            limit = len(self._body) - self._offset
        if self._max_chunk_size is not None:
            limit = min(limit, self._max_chunk_size)
        end = min(len(self._body), self._offset + limit)
        chunk = self._body[self._offset:end]
        self._offset = end
        return chunk


class FakeResponse:
    def __init__(
        self,
        status: int,
        body: str = "",
        headers: dict[str, str] | None = None,
        *,
        max_chunk_size: int | None = None,
    ):
        self.status = status
        self._body = body
        self.headers = headers or {}
        self.content = FakeContent(body.encode("utf-8"), max_chunk_size=max_chunk_size)

    def get_encoding(self) -> str:
        return "utf-8"

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

    async def test_drains_a_json_response_that_arrives_in_multiple_chunks(self):
        client, session = await self._make_client(
            [FakeResponse(200, '{"companyfacts": ["complete"]}', max_chunk_size=7)]
        )
        result = await client.make_request("https://data.sec.gov/companyfacts", is_json=True)
        self.assertEqual(result, {"companyfacts": ["complete"]})
        self.assertEqual(len(session.calls), 1)

    async def test_rejects_non_json_when_json_was_requested(self):
        client, _ = await self._make_client([FakeResponse(200, "<html>not json</html>")])
        with self.assertRaises(SecMalformedResponseError):
            await client.make_request("https://data.sec.gov/x", is_json=True)

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

    async def test_404_raises_not_found_without_retrying(self):
        client, session = await self._make_client(
            [
                FakeResponse(404, ""),
                FakeResponse(200, '{"ok": true}'),  # should never be consumed
            ]
        )
        with self.assertRaises(SecNotFoundError):
            await client.make_request("https://data.sec.gov/missing", max_retries=4)
        self.assertEqual(len(session.calls), 1)

    async def test_403_raises_access_denied_without_retrying(self):
        client, session = await self._make_client([FakeResponse(403, "")])
        with self.assertRaises(SecAccessDeniedError):
            await client.make_request("https://data.sec.gov/forbidden", max_retries=3)
        self.assertEqual(len(session.calls), 1)

    async def test_500_retries_up_to_max_then_raises(self):
        client, session = await self._make_client(
            [FakeResponse(500, ""), FakeResponse(500, ""), FakeResponse(500, "")]
        )
        with self.assertRaises(SecRequestError) as caught:
            await client.make_request("https://data.sec.gov/x", max_retries=3)
        self.assertTrue(caught.exception.retryable)
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


class ConcurrentRateLimitTests(unittest.IsolatedAsyncioTestCase):
    """Regression: the limiter must hold under concurrency, not just sequentially.

    The original implementation compared `time.time()` against `last_request_time` and
    slept the difference. Under `asyncio.gather` every coroutine read the same stale
    timestamp, computed the same sleep, slept in parallel, and then dispatched
    simultaneously — so 50 concurrent callers issued 50 requests inside ~100ms against a
    configured 10 req/s budget. That is a SEC fair-access violation, and it is reachable
    from public API (`fetch_multiple_tickers`, `download_all_form_documents`, or any two
    concurrent consumers of one shared fetcher).
    """

    async def _dispatch_times(self, callers: int, interval: float) -> list[float]:
        dispatched: list[float] = []

        class RecordingContext:
            async def __aenter__(self) -> FakeResponse:
                dispatched.append(time.monotonic())
                return FakeResponse(200, "{}")

            async def __aexit__(self, *_exc: Any) -> bool:
                return False

        class RecordingSession:
            closed = False

            def get(self, *_args: Any, **_kwargs: Any) -> RecordingContext:
                return RecordingContext()

            async def close(self) -> None:
                self.closed = True

        client = SecHttpClient(user_agent="Audit audit@example.com", rate_limit_sleep=interval)
        _install_fake_session(client, RecordingSession())  # type: ignore[arg-type]
        await asyncio.gather(
            *(client.make_request(f"https://data.sec.gov/{i}.json") for i in range(callers))
        )
        return sorted(dispatched)

    async def test_concurrent_callers_are_spaced_by_the_request_interval(self):
        interval = 0.02
        callers = 25
        dispatched = await self._dispatch_times(callers, interval)

        self.assertEqual(len(dispatched), callers)
        span = dispatched[-1] - dispatched[0]
        # N slots spaced `interval` apart must span at least (N-1)*interval. A generous
        # 60% floor absorbs event-loop scheduling jitter without admitting a burst.
        self.assertGreater(
            span,
            (callers - 1) * interval * 0.6,
            f"{callers} concurrent requests dispatched within {span:.4f}s; "
            f"expected >= ~{(callers - 1) * interval:.4f}s of spacing",
        )

    async def test_no_window_exceeds_the_configured_request_budget(self):
        interval = 0.02  # 50 req/s
        dispatched = await self._dispatch_times(25, interval)

        # Peak requests observed inside any rolling one-interval window.
        peak = max(
            sum(1 for other in dispatched if start <= other < start + interval)
            for start in dispatched
        )
        self.assertLessEqual(
            peak, 2, f"burst of {peak} requests inside a single {interval}s slot window"
        )

    async def test_archive_and_data_paths_share_one_budget(self):
        # SEC's limit applies to the caller, not per hostname, so www.sec.gov/Archives and
        # data.sec.gov traffic must draw on the same reservation cursor.
        dispatched: list[float] = []

        class RecordingContext:
            async def __aenter__(self) -> FakeResponse:
                dispatched.append(time.monotonic())
                return FakeResponse(200, "{}")

            async def __aexit__(self, *_exc: Any) -> bool:
                return False

        class RecordingSession:
            closed = False

            def get(self, *_args: Any, **_kwargs: Any) -> RecordingContext:
                return RecordingContext()

            async def close(self) -> None:
                self.closed = True

        interval = 0.05
        client = SecHttpClient(user_agent="Audit audit@example.com", rate_limit_sleep=interval)
        _install_fake_session(client, RecordingSession())  # type: ignore[arg-type]

        await asyncio.gather(
            client.make_request("https://data.sec.gov/submissions/CIK0000320193.json"),
            client.make_archive_request("https://www.sec.gov/Archives/edgar/data/320193/x.xml"),
            client.make_request("https://data.sec.gov/api/xbrl/companyfacts/CIK0000320193.json"),
            client.make_archive_request("https://www.sec.gov/Archives/edgar/data/320193/y.xml"),
        )

        dispatched.sort()
        self.assertEqual(len(dispatched), 4)
        self.assertGreater(
            dispatched[-1] - dispatched[0],
            3 * interval * 0.6,
            "mixed archive/data requests bypassed the shared rate-limit budget",
        )


class BoundedResponseTests(unittest.IsolatedAsyncioTestCase):
    """Remote bodies are read through a byte ceiling; an unbounded read over a gzip
    stream is a decompression-bomb primitive."""

    async def test_oversized_content_length_is_refused(self):
        from copetech_sec import http_client as http_client_module

        client = SecHttpClient(user_agent="Audit audit@example.com", rate_limit_sleep=0)
        oversized = str(http_client_module.MAX_RESPONSE_BYTES + 1)
        session = FakeSession([FakeResponse(200, "{}", headers={"Content-Length": oversized})])
        _install_fake_session(client, session)

        with self.assertRaises(SecResponseTooLargeError):
            await client.make_request("https://data.sec.gov/huge.json")

    async def test_body_exceeding_cap_is_refused(self):
        from copetech_sec import http_client as http_client_module

        original_cap = http_client_module.MAX_RESPONSE_BYTES
        http_client_module.MAX_RESPONSE_BYTES = 16
        try:
            client = SecHttpClient(user_agent="Audit audit@example.com", rate_limit_sleep=0)
            session = FakeSession([FakeResponse(200, "x" * 64)])
            _install_fake_session(client, session)
            with self.assertRaises(SecResponseTooLargeError):
                await client.make_request("https://data.sec.gov/big.json", is_json=False)
        finally:
            http_client_module.MAX_RESPONSE_BYTES = original_cap

    async def test_body_within_cap_is_returned(self):
        client = SecHttpClient(user_agent="Audit audit@example.com", rate_limit_sleep=0)
        session = FakeSession([FakeResponse(200, '{"ok": true}', max_chunk_size=3)])
        _install_fake_session(client, session)
        self.assertEqual(await client.make_request("https://data.sec.gov/small.json"), {"ok": True})


class RetryAfterParsingTests(unittest.TestCase):
    """`Retry-After` may be delta-seconds or an HTTP-date (RFC 9110). The date form used
    to raise ValueError out of the retry loop and be swallowed as an unexpected error."""

    def test_parses_delta_seconds(self):
        from copetech_sec.http_client import _retry_after_seconds

        self.assertEqual(_retry_after_seconds("5"), 5.0)
        self.assertEqual(_retry_after_seconds(" 2.5 "), 2.5)

    def test_parses_http_date_without_raising(self):
        from copetech_sec.http_client import _retry_after_seconds

        value = _retry_after_seconds("Wed, 21 Oct 2099 07:28:00 GMT")
        self.assertIsNotNone(value)
        self.assertGreater(value, 0)

    def test_returns_none_for_missing_or_unparseable(self):
        from copetech_sec.http_client import _retry_after_seconds

        self.assertIsNone(_retry_after_seconds(None))
        self.assertIsNone(_retry_after_seconds(""))
        self.assertIsNone(_retry_after_seconds("not-a-date"))

    def test_negative_delta_is_clamped_to_zero(self):
        from copetech_sec.http_client import _retry_after_seconds

        self.assertEqual(_retry_after_seconds("-10"), 0.0)


class ThrottleStatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_503_is_retried_like_429(self):
        from copetech_sec import http_client as http_client_module

        sleeps = _patch_sleep([http_client_module])
        original_sleep = asyncio.sleep
        try:
            client = SecHttpClient(user_agent="Audit audit@example.com", rate_limit_sleep=0)
            session = FakeSession([FakeResponse(503, "", headers={"Retry-After": "1"}),
                                   FakeResponse(200, '{"ok": 1}')])
            _install_fake_session(client, session)
            result = await client.make_request("https://data.sec.gov/x.json")
        finally:
            http_client_module.asyncio.sleep = original_sleep

        self.assertEqual(result, {"ok": 1})
        self.assertEqual(len(session.calls), 2, "503 should be retried, not treated as fatal")
        self.assertTrue(any(s >= 1.0 for s in sleeps), f"expected Retry-After backoff, got {sleeps}")

    async def test_unconfigured_user_agent_does_not_fabricate_a_contact_address(self):
        # SEC fair access asks callers to declare identity; inventing
        # "email@example.com" misrepresents it. The placeholder must be honest.
        client = SecHttpClient(user_agent=None)
        ua = client.default_headers["User-Agent"]
        self.assertNotIn("@", ua)
        self.assertIn("copetech-edgar", ua)


if __name__ == "__main__":
    unittest.main()
