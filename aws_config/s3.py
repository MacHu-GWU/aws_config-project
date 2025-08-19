# -*- coding: utf-8 -*-

"""
Configuration storage and management using AWS S3 without versioning.

Provides a unified interface for storing application configurations in S3 buckets
with versioning disabled. This module requires S3 bucket versioning to be disabled
and implements custom file-based versioning for configuration management.

**Key Features:**

- Requires S3 bucket versioning to be disabled
- Implements custom versioning using file naming conventions
- Stores multiple files per parameter with sequential version numbers
- Manual version tracking with integer sequences (1, 2, 3, ...)
- Latest file always points to current version

.. warning::
    This module only works with versioning-disabled S3 buckets. Versioned
    buckets are not supported.
"""

try:
    import typing_extensions as T
except ImportError:  # pragma: no cover
    import typing as T

import dataclasses
from datetime import datetime, timezone, timedelta
from functools import cached_property

import botocore.exceptions
from boto_session_manager import BotoSesManager
from s3pathlib import S3Path

from . import exc
from .constants import ZFILL, S3MetadataKeyEnum, LATEST_VERSION
from .utils import sha256_of_text

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_s3.client import S3Client


def read_text(
    s3path: S3Path,
    **kwargs,
) -> str:
    """
    Read text content from S3 object with proper error handling.

    :after_param s3path: S3 path to read from
    :after_param kwargs: Additional arguments passed to S3Path.read_text()

    :returns: Text content of the S3 object

    :raises S3ObjectNotExist: If the S3 object does not exist
    """
    try:
        return s3path.read_text(**kwargs)
    except botocore.exceptions.ClientError as e:
        if "NoSuchKey" in str(e):
            raise exc.S3ObjectNotExist(f"S3 object {s3path.uri} not exist.")
        raise  # pragma: no cover


@dataclasses.dataclass(frozen=True)
class S3Parameter:
    """
    S3 parameter storage for versioning-disabled buckets.

    This class manages configuration storage in S3 buckets with versioning disabled.
    It provides a custom versioning system using file naming conventions to track
    configuration versions manually.

    **Why use S3 for configuration storage?**

    - Centralized configuration management across environments
    - Built-in durability and availability guarantees
    - Integration with AWS IAM for access control
    - Audit trail through CloudTrail
    - Cost-effective storage for configuration data
    - Custom versioning without S3 versioning overhead

    **How it works:**

    **Storage Structure:**

    - Multiple files per parameter using naming convention
    - Latest file: ``{s3dir_config}/{parameter_name}/{parameter_name}-000000-latest.json``
    - Versioned files: ``{s3dir_config}/{parameter_name}/{parameter_name}-999999-1.json``
    - Version numbers are sequential integers (1, 2, 3, ...)
    - File naming ensures latest appears first in listings

    **Version Management:**

    - Uses integer sequence for versions (1, 2, 3, ...)
    - Latest file contains pointer to current version
    - Historical versions preserved as separate files
    - Manual cleanup of old versions when needed

    **File Structure Example:**
    ```
    s3://bucket/config/myapp/myapp-000000-latest.json  (latest pointer)
    s3://bucket/config/myapp/myapp-999997-3.json       (version 3)
    s3://bucket/config/myapp/myapp-999998-2.json       (version 2)
    s3://bucket/config/myapp/myapp-999999-1.json       (version 1)
    ```

    :after_param s3dir_config: S3 directory where the parameter is stored (no filename)
    :after_param parameter_name: Parameter name used as the base filename

    .. note::
        This class only works with versioning-disabled S3 buckets. The bucket
        must have versioning disabled before using this class.
    """

    s3dir_config: S3Path = dataclasses.field()
    parameter_name: str = dataclasses.field()

    @cached_property
    def s3dir_param(self) -> S3Path:
        """S3 directory containing all files for this parameter."""
        return self.s3dir_config.joinpath(self.parameter_name).to_dir()

    def get_s3path(
        self,
        version: int | None = None,
    ) -> S3Path:
        """
        Get the S3 path for a specific version of the parameter.

        For versioning-disabled buckets, this creates different filenames
        based on the version number. The filename encoding ensures that
        the latest version appears first in S3 object listings.

        :after_param version: Version number (1, 2, 3, ...), None for latest

        :return: S3Path for the versioned file

        **File naming pattern:**

        - Latest: ``{parameter_name}-000000-latest.json``
        - Version 1: ``{parameter_name}-999999-1.json``
        - Version 2: ``{parameter_name}-999998-2.json``
        """
        if version is None:
            return self.s3dir_param.joinpath(
                f"{self.parameter_name}-{str(0).zfill(ZFILL)}-{LATEST_VERSION}.json",
            )
        else:
            return self.s3dir_param.joinpath(
                f"{self.parameter_name}-{(10 ** ZFILL) - version}-{version}.json",
            )

    def write(
        self,
        s3_client: "S3Client",
        value: str,
        version: int | None = None,
        write_text_kwargs: dict[str, T.Any] | None = None,
    ) -> S3Path:
        """
        Write configuration data to S3 with custom versioning.

        :after_param s3_client: S3Client for S3 operations
        :after_param value: Configuration data as JSON string
        :after_param version: Version number for metadata and filename
        :after_param write_text_kwargs: Additional arguments for S3 write operation

        :returns: S3Path of the written object
        """
        s3path = self.get_s3path(version)
        # For non-versioned buckets, track version in metadata
        if version is None:
            config_version = LATEST_VERSION
        else:
            config_version = str(version)
        metadata = {
            S3MetadataKeyEnum.CONFIG_VERSION.value: config_version,
            S3MetadataKeyEnum.CONFIG_SHA256.value: sha256_of_text(value),
        }
        if write_text_kwargs is None:
            write_text_kwargs = {}
        s3path_new = s3path.write_text(
            bsm=s3_client,
            data=value,
            metadata=metadata,
            **write_text_kwargs,
        )
        return s3path_new

    def read(
        self,
        s3_client: "S3Client",
        version: int | None = None,
        read_text_kwargs: dict[str, T.Any] | None = None,
    ) -> str:
        """
        Read configuration data from S3.

        Reads configuration data from the versioning-disabled S3 bucket using
        custom file naming conventions. Can read either the latest version
        or a specific version number.

        :after_param s3_client: S3Client for S3 operations
        :after_param version: Version number (1, 2, 3, ...) or None for latest
        :after_param read_text_kwargs: Additional arguments for S3 read operation

        :returns: Configuration data as JSON string
        """
        s3path = self.get_s3path(version)
        if read_text_kwargs is None:
            read_text_kwargs = {}
        return read_text(
            s3path=s3path,
            bsm=s3_client,
            **read_text_kwargs,
        )

    def delete(
        self,
        s3_client: "S3Client",
        version: int | None = None,
    ) -> S3Path:
        """
        Delete a specific version of the configuration file from S3.

        :after_param s3_client: S3Client for S3 operations
        :after_param version: Version number (1, 2, 3, ...) or None for latest

        :returns: S3Path of the deleted object
        """
        s3path = self.get_s3path(version)
        s3path.delete(bsm=s3_client)
        return s3path

    def delete_all(
        self,
        s3_client: "S3Client",
    ):
        """
        Delete all configuration files for this parameter from S3.

        Removes the entire parameter directory and all versioned files.
        This operation cannot be undone.

        :after_param s3_client: S3Client for S3 operations
        """
        self.s3dir_param.delete(bsm=s3_client)

    def delete_last(
        self,
        s3_client: "S3Client",
        keep_last_n: int = 10,
        purge_older_than_secs: int = 90 * 24 * 60 * 60,
    ):
        """
        Delete old configuration files based on retention policy.

        Cleans up old versioned files while preserving recent ones. Files are
        deleted only if they exceed both the count limit and age limit.

        :after_param s3_client: S3Client for S3 operations
        :after_param keep_last_n: Minimum number of files to keep (default: 10)
        :after_param purge_older_than_secs: Delete files older than this (default: 90 days)
        """
        s3path_list = list()
        for s3path in self.s3dir_param.iter_objects(bsm=s3_client):
            if s3path.basename.startswith(
                f"{self.parameter_name}-"
            ) and s3path.basename.endswith(".json"):
                s3path_list.append(s3path)
        now = datetime.now(tz=timezone.utc)
        expire = now - timedelta(seconds=purge_older_than_secs)
        for s3path in s3path_list[keep_last_n + 1 :]:
            if s3path.last_modified_at < expire:
                s3path.delete(bsm=s3_client)
