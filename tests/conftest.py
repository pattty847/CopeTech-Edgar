"""Test setup. Configures env vars consumed by `ServiceSettings.from_env()` at app import."""

from __future__ import annotations

import os

os.environ.setdefault("SEC_API_USER_AGENT", "Test Runner test@example.com")
os.environ.setdefault("DEMO_ACCESS_KEYS", "test-demo-key,second-key")
os.environ.setdefault("BACKEND_API_SECRET", "test-backend-secret")
os.environ.setdefault("RATE_LIMIT_PER_DAY", "60")
os.environ.setdefault("CORS_ALLOW_ORIGINS", "http://localhost:5173")
os.environ.pop("DYNAMODB_RATE_LIMITS_TABLE", None)
os.environ.pop("DYNAMODB_DEMO_JOBS_TABLE", None)
os.environ.pop("DYNAMODB_SEC_CACHE_INDEX_TABLE", None)
