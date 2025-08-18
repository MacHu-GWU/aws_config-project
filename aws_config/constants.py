# -*- coding: utf-8 -*-

from enum_mate.api import BetterStrEnum


class S3BucketVersionStatus(BetterStrEnum):
    """
    Enumerate the status of S3 bucket versioning.

    See: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/put_bucket_versioning.html

    - NotEnabled: bucket doesn't turn on versioning.
    - Enabled: bucket turns on versioning.
    - Suspended: bucket turns on versioning but is suspended. We don't store
        config files in a bucket with 'suspended' status.
    """

    NOT_ENABLED = "NotEnabled"
    ENABLED = "Enabled"
    SUSPENDED = "Suspended"

    def is_not_enabled(self) -> bool:
        return self.value == S3BucketVersionStatus.NOT_ENABLED.value

    def is_enabled(self) -> bool:
        return self.value == S3BucketVersionStatus.ENABLED.value

    def is_suspended(self) -> bool:
        return self.value == S3BucketVersionStatus.SUSPENDED.value


ZFILL = 6
"""
Can support up to 999999 versions.
"""

LATEST_VERSION = "LATEST"


class AwsTagKeyEnum(BetterStrEnum):
    """
    Enumerate the keys of AWS tags used in the project.
    """

    CONFIG_VERSION = "config_version"
    CONFIG_SHA256 = "config_sha256"
