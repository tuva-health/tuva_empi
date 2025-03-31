import logging
from typing import Iterator, cast
from urllib.parse import urlparse

import boto3  # type: ignore[import-untyped]

logger = logging.getLogger(__name__)


class ObjectDoesNotExist(Exception):
    """S3 object does not exist."""


class S3Client:
    def __init__(self) -> None:
        self.s3 = boto3.client("s3")

    def get_object_chunks(self, s3_uri: str) -> Iterator[bytes]:
        try:
            logger.info(f"Retrieving object chunks: {s3_uri}")

            s3_uri_parsed = urlparse(s3_uri, allow_fragments=False)
            s3_bucket = s3_uri_parsed.netloc
            s3_key = s3_uri_parsed.path.lstrip("/")
            response = self.s3.get_object(Bucket=s3_bucket, Key=s3_key)

            return cast(Iterator[bytes], response["Body"].iter_chunks())

        except self.s3.exceptions.NoSuchKey as e:
            msg = "S3 object does not exist"
            logger.error(msg, s3_uri)
            raise ObjectDoesNotExist(msg) from e

    def get_object_lines(self, s3_uri: str) -> Iterator[bytes]:
        try:
            logger.info(f"Retrieving object lines: {s3_uri}")

            s3_uri_parsed = urlparse(s3_uri, allow_fragments=False)
            s3_bucket = s3_uri_parsed.netloc
            s3_key = s3_uri_parsed.path.lstrip("/")
            response = self.s3.get_object(Bucket=s3_bucket, Key=s3_key)

            return cast(Iterator[bytes], response["Body"].iter_lines())

        except self.s3.exceptions.NoSuchKey as e:
            msg = f"S3 object does not exist: {s3_uri}"
            logger.error(msg)
            raise ObjectDoesNotExist(msg) from e
