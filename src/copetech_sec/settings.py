from dataclasses import dataclass
import os
import secrets


@dataclass(frozen=True)
class ServiceSettings:
    aws_region: str
    s3_bucket: str | None
    rate_limits_table: str | None
    demo_jobs_table: str | None
    sec_cache_index_table: str | None
    rate_limits_pk: str
    demo_jobs_pk: str
    sec_cache_index_pk: str
    cache_dir: str
    sec_user_agent: str | None
    backend_api_secret: str | None
    demo_access_keys: tuple[str, ...]
    cors_allow_origins: tuple[str, ...]
    rate_limit_per_day: int
    market_cache_ttl_seconds: int
    sec_request_sleep: float
    log_level: str
    port: int

    @classmethod
    def from_env(cls) -> "ServiceSettings":
        return cls(
            aws_region=os.environ.get("AWS_REGION", "us-east-1"),
            s3_bucket=os.environ.get("S3_BUCKET"),
            rate_limits_table=os.environ.get("DYNAMODB_RATE_LIMITS_TABLE"),
            demo_jobs_table=os.environ.get("DYNAMODB_DEMO_JOBS_TABLE"),
            sec_cache_index_table=os.environ.get("DYNAMODB_SEC_CACHE_INDEX_TABLE"),
            rate_limits_pk=os.environ.get("DYNAMODB_RATE_LIMITS_PK", "id"),
            demo_jobs_pk=os.environ.get("DYNAMODB_DEMO_JOBS_PK", "id"),
            sec_cache_index_pk=os.environ.get("DYNAMODB_SEC_CACHE_INDEX_PK", "id"),
            cache_dir=os.environ.get("SEC_CACHE_DIR", "data/edgar"),
            sec_user_agent=os.environ.get("SEC_API_USER_AGENT"),
            backend_api_secret=cls._optional_secret(os.environ.get("BACKEND_API_SECRET")),
            demo_access_keys=cls._csv_values(os.environ.get("DEMO_ACCESS_KEYS")),
            cors_allow_origins=cls._csv_values(os.environ.get("CORS_ALLOW_ORIGINS"))
            or ("http://localhost:5173", "http://127.0.0.1:5173"),
            rate_limit_per_day=int(os.environ.get("RATE_LIMIT_PER_DAY", "60")),
            market_cache_ttl_seconds=int(os.environ.get("MARKET_CACHE_TTL_SECONDS", "21600")),
            sec_request_sleep=float(os.environ.get("SEC_REQUEST_SLEEP", "0.1")),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            port=int(os.environ.get("PORT", "8000")),
        )

    @staticmethod
    def _optional_secret(value: str | None) -> str | None:
        if value is None:
            return None
        stripped = value.strip()
        if not stripped:
            return None
        return stripped

    def secret_matches(self, candidate: str | None) -> bool:
        if self.backend_api_secret is None:
            return True
        if candidate is None:
            return False
        return secrets.compare_digest(candidate, self.backend_api_secret)

    @staticmethod
    def _csv_values(value: str | None) -> tuple[str, ...]:
        if value is None:
            return ()
        return tuple(item.strip() for item in value.split(",") if item.strip())

    def demo_key_allowed(self, candidate: str | None) -> bool:
        if not self.demo_access_keys or candidate is None:
            return False
        return any(secrets.compare_digest(candidate, key) for key in self.demo_access_keys)
