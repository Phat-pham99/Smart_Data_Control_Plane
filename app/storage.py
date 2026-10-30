from __future__ import annotations

import hashlib
from functools import lru_cache

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from app.config import Settings, get_settings


class IntegrityError(ValueError):
    pass


class ObjectStorage:
    def __init__(self, settings: Settings):
        self.settings = settings
        options = {
            "region_name": settings.s3_region,
            "aws_access_key_id": settings.aws_access_key_id,
            "aws_secret_access_key": settings.aws_secret_access_key,
            "config": Config(
                signature_version="s3v4",
                s3={"addressing_style": "path"},
                connect_timeout=settings.dependency_timeout_seconds,
                read_timeout=10,
                retries={"max_attempts": 1},
            ),
        }
        self.client = boto3.client("s3", endpoint_url=settings.s3_endpoint_url, **options)
        self.public_client = boto3.client(
            "s3",
            endpoint_url=settings.s3_public_endpoint_url or settings.s3_endpoint_url,
            **options,
        )

    def bootstrap(self):
        for bucket in (
            self.settings.s3_staging_bucket,
            self.settings.s3_bucket,
            self.settings.s3_archive_bucket,
        ):
            try:
                self.client.head_bucket(Bucket=bucket)
            except ClientError as error:
                if error.response["Error"]["Code"] not in ("404", "NoSuchBucket"):
                    raise
                options = {"Bucket": bucket}
                if self.settings.s3_region != "us-east-1":
                    options["CreateBucketConfiguration"] = {
                        "LocationConstraint": self.settings.s3_region
                    }
                self.client.create_bucket(**options)

    def upload_form(self, bucket, key, size, content_type):
        return self.public_client.generate_presigned_post(
            Bucket=bucket,
            Key=key,
            Fields={"Content-Type": content_type},
            Conditions=[{"Content-Type": content_type}, ["content-length-range", size, size]],
            ExpiresIn=self.settings.signed_url_seconds,
        )

    def verify(self, bucket, key, expected_size, expected_sha256):
        response = self.client.get_object(Bucket=bucket, Key=key)
        digest = hashlib.sha256()
        size = 0
        try:
            if response["ContentLength"] != expected_size:
                raise IntegrityError("SIZE_MISMATCH")
            while chunk := response["Body"].read(1024 * 1024):
                size += len(chunk)
                if size > expected_size or size > self.settings.max_upload_bytes:
                    raise IntegrityError("SIZE_MISMATCH")
                digest.update(chunk)
        finally:
            response["Body"].close()
        if size != expected_size or digest.hexdigest() != expected_sha256:
            raise IntegrityError("CHECKSUM_MISMATCH")
        return response["ETag"]

    def promote(self, obj):
        source_etag = self.verify(
            obj.staging_bucket, obj.staging_key, obj.size_bytes, obj.checksum_sha256
        )
        self.client.copy_object(
            Bucket=obj.bucket,
            Key=obj.key,
            CopySource={"Bucket": obj.staging_bucket, "Key": obj.staging_key},
            CopySourceIfMatch=source_etag,
            ContentType=obj.content_type,
            MetadataDirective="REPLACE",
            Metadata={"sha256": obj.checksum_sha256},
        )
        self.verify(obj.bucket, obj.key, obj.size_bytes, obj.checksum_sha256)

    def download_url(self, obj):
        response = self.client.head_object(Bucket=obj.bucket, Key=obj.key)
        if response["ContentLength"] != obj.size_bytes:
            raise IntegrityError("SIZE_MISMATCH")
        return self.public_client.generate_presigned_url(
            "get_object",
            Params={"Bucket": obj.bucket, "Key": obj.key},
            ExpiresIn=self.settings.signed_url_seconds,
        )


@lru_cache
def get_storage():
    return ObjectStorage(get_settings())


if __name__ == "__main__":
    get_storage().bootstrap()
