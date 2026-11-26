"""PyArrow's S3 retry strategy for PyIceberg's file reads and writes against R2 (#2989).

PyIceberg reads and writes Iceberg data and metadata files through PyArrow's `S3FileSystem`, not through the
boto3 client of `r2.storage_client()` (#2985). Its default strategy sends 3 requests in about 2 s and then fails on
R2's throttle answers (HTTP 429, 503 SlowDown / ServiceUnavailable). `R2S3RetryStrategy` keeps AWS's standard
strategy (exponential backoff with jitter, at most 20 s between attempts; throttle and 5xx answers are retried, a
refused request was not applied) with more attempts: measured against a local stub that answers 429 to every HEAD,
3 attempts gave up after 1.9 s, 10 after 89 s and 20 after 277 s, so one request waits up to about 5 minutes, as a
boto3 call does. PyIceberg builds it by name from the catalog property `s3.retry-strategy-impl` (`r2.catalog()`).
"""

from __future__ import annotations

from pyarrow.fs import AwsStandardS3RetryStrategy

MAX_ATTEMPTS = 20


class R2S3RetryStrategy(AwsStandardS3RetryStrategy):
    """AWS standard retries with MAX_ATTEMPTS attempts; PyIceberg instantiates it without arguments."""

    def __init__(self) -> None:
        super().__init__(max_attempts=MAX_ATTEMPTS)
