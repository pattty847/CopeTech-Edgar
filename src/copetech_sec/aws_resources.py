from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import logging
import time
from typing import Any

import boto3
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError, NoCredentialsError

from .settings import ServiceSettings


class AwsResourceManager:
    def __init__(self, settings: ServiceSettings):
        self.settings = settings
        self._dynamodb = None
        self._s3 = None
        self._memory_counts: dict[str, int] = {}

    @property
    def dynamodb(self):
        if self._dynamodb is None:
            self._dynamodb = boto3.resource(
                "dynamodb",
                region_name=self.settings.aws_region,
                config=Config(connect_timeout=2, read_timeout=4, retries={"max_attempts": 2}),
            )
        return self._dynamodb

    @property
    def s3(self):
        if self._s3 is None:
            self._s3 = boto3.client(
                "s3",
                region_name=self.settings.aws_region,
                config=Config(connect_timeout=2, read_timeout=4, retries={"max_attempts": 2}),
            )
        return self._s3

    def public_config(self) -> dict[str, Any]:
        return {
            "aws_region": self.settings.aws_region,
            "s3_bucket_configured": bool(self.settings.s3_bucket),
            "rate_limits_table_configured": bool(self.settings.rate_limits_table),
            "demo_jobs_table_configured": bool(self.settings.demo_jobs_table),
            "sec_cache_index_table_configured": bool(self.settings.sec_cache_index_table),
            "rate_limit_per_day": self.settings.rate_limit_per_day,
        }

    def check_rate_limit(self, client_id: str) -> dict[str, Any]:
        day = datetime.now(timezone.utc).strftime("%Y%m%d")
        key = f"{client_id}#{day}"

        if self.settings.rate_limits_table:
            try:
                table = self.dynamodb.Table(self.settings.rate_limits_table)
                response = table.update_item(
                    Key={self.settings.rate_limits_pk: key},
                    UpdateExpression=(
                        "SET first_seen = if_not_exists(first_seen, :now), "
                        "last_seen = :now ADD request_count :one"
                    ),
                    ExpressionAttributeValues={":now": int(time.time()), ":one": 1},
                    ReturnValues="UPDATED_NEW",
                )
                count = int(response.get("Attributes", {}).get("request_count", 1))
                return {
                    "allowed": count <= self.settings.rate_limit_per_day,
                    "count": count,
                    "limit": self.settings.rate_limit_per_day,
                    "backend": "dynamodb",
                }
            except (BotoCoreError, ClientError, NoCredentialsError) as exc:
                logging.warning("DynamoDB rate limit check failed; falling back to memory: %s", exc)

        self._memory_counts[key] = self._memory_counts.get(key, 0) + 1
        return {
            "allowed": self._memory_counts[key] <= self.settings.rate_limit_per_day,
            "count": self._memory_counts[key],
            "limit": self.settings.rate_limit_per_day,
            "backend": "memory",
        }

    def record_sec_cache_lookup(self, ticker: str, payload_kind: str, hit: bool, metadata: dict[str, Any]) -> None:
        if not self.settings.sec_cache_index_table:
            return

        try:
            table = self.dynamodb.Table(self.settings.sec_cache_index_table)
            item_id = hashlib.sha256(f"{ticker.upper()}#{payload_kind}".encode("utf-8")).hexdigest()
            table.put_item(
                Item={
                    self.settings.sec_cache_index_pk: item_id,
                    "ticker": ticker.upper(),
                    "payload_kind": payload_kind,
                    "hit": hit,
                    "updated_at": int(time.time()),
                    "metadata": metadata,
                }
            )
        except (BotoCoreError, ClientError, NoCredentialsError) as exc:
            logging.warning("DynamoDB SEC cache index write failed: %s", exc)
