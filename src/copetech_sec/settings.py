from dataclasses import dataclass
import os


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
    rate_limit_per_day: int
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
            rate_limit_per_day=int(os.environ.get("RATE_LIMIT_PER_DAY", "60")),
            sec_request_sleep=float(os.environ.get("SEC_REQUEST_SLEEP", "0.1")),
            log_level=os.environ.get("LOG_LEVEL", "INFO"),
            port=int(os.environ.get("PORT", "8000")),
        )
