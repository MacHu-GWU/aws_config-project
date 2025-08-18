# -*- coding: utf-8 -*-

"""
Configuration storage and management using AWS S3.

Provides a unified interface for storing application configurations in S3 buckets
with automatic versioning support. Adapts behavior based on S3 bucket versioning
settings - uses native S3 versioning when available, or implements custom file-based
versioning for non-versioned buckets.

Main functions: :func:`deploy_config`, :func:`read_config`, :func:`delete_config`
"""

try:
    import typing_extensions as T
except ImportError:  # pragma: no cover
    import typing as T
import json
import dataclasses

from func_args.api import OPT
import botocore.exceptions
from boto_session_manager import BotoSesManager
from s3pathlib import S3Path

from .vendor.jsonutils import json_loads

from . import exc
from .constants import S3BucketVersionStatus, ZFILL, AwsTagKeyEnum
from .logger import logger
from .utils import sha256_of_config_data


def get_bucket_version_status(
    bsm: "BotoSesManager",
    bucket: str,
) -> S3BucketVersionStatus:
    """
    Get the versioning status of an S3 bucket.

    Queries S3 to determine if bucket versioning is enabled, disabled, or suspended.
    This information is crucial for determining how configuration files should be
    stored and managed.

    :param bsm: BotoSesManager object for AWS operations
    :param bucket: S3 bucket name to check
    :return: Bucket versioning status enum

    .. seealso::
        :class:`S3BucketVersionStatus` for possible status values
    """
    res = bsm.s3_client.get_bucket_versioning(Bucket=bucket)
    status = res.get("Status", S3BucketVersionStatus.NOT_ENABLED.value)
    S3BucketVersionStatus.ensure_is_valid_value(status)
    return S3BucketVersionStatus.get_by_value(status)


def _ensure_bucket_versioning_is_not_suspended(
    bucket: str,
    status: S3BucketVersionStatus,
):  # pragma: no cover
    """
    Ensure the S3 bucket versioning is not suspended.

    **Why suspended buckets are not supported:**

    When S3 bucket versioning is suspended, the behavior becomes unpredictable:
    - New objects are stored with null version IDs
    - Existing versioned objects remain unchanged
    - Delete operations create delete markers
    - Mixed state makes version management complex and error-prone

    **The problem with suspended state:**

    This creates an inconsistent environment where some objects have versions
    and others don't, making it impossible to reliably implement the version
    tracking logic that this module depends on.

    **Solution:**

    Either fully enable versioning or completely disable it. Both states are
    well-defined and supported by this module.

    :param bucket: S3 bucket name
    :param status: Current bucket versioning status
    :raises S3BucketVersionSuspendedError: If bucket versioning is suspended
    """
    if status.is_suspended():
        raise exc.S3BucketVersionSuspendedError(
            f"bucket {bucket!r} versioning is suspended. "
            f"I don't know how to handle this situation."
        )


@dataclasses.dataclass
class S3Object:
    """
    Represents an S3 object with metadata and properties.

    This class wraps the raw S3 API response and provides convenient access
    to object metadata, checksums, encryption details, and versioning information.
    It serves as a return value from deployment operations to give users insight
    into the created S3 objects.

    **Use cases:**
    - Verify deployment results
    - Access object metadata and checksums
    - Check encryption and versioning details
    - Debug S3 operations

    **Created by:**
    All deployment methods return an S3Object instance representing the
    uploaded configuration file.

    :param response: Raw S3 API response dictionary
    :param bucket: S3 bucket name where object is stored
    :param key: S3 object key (path within bucket)
    """

    response: dict[str, T.Any] = dataclasses.field()
    bucket: str = dataclasses.field()
    key: str = dataclasses.field()

    @classmethod
    def from_s3path(cls, s3path: "S3Path") -> "T.Self":
        return cls(
            response=s3path.response,
            bucket=s3path.bucket,
            key=s3path.key,
        )

    @property
    def expiration(self) -> str | None:
        return self.response.get("Expiration")

    @property
    def etag(self) -> str | None:
        return self.response.get("ETag")

    @property
    def checksum_crc32(self) -> str | None:
        return self.response.get("ChecksumCRC32")

    @property
    def checksum_crc32c(self) -> str | None:
        return self.response.get("ChecksumCRC32C")

    @property
    def checksum_sha1(self) -> str | None:
        return self.response.get("ChecksumSHA1")

    @property
    def checksum_sha256(self) -> str | None:
        return self.response.get("ChecksumSHA256")

    @property
    def server_side_encryption(self) -> str | None:
        return self.response.get("ServerSideEncryption")

    @property
    def version_id(self) -> str | None:
        return self.response.get("VersionId")

    @property
    def sse_customer_algorithm(self) -> str | None:
        return self.response.get("SSECustomerAlgorithm")

    @property
    def sse_customer_key_md5(self) -> str | None:
        return self.response.get("SSECustomerKeyMD5")

    @property
    def see_kms_key_id(self) -> str | None:
        return self.response.get("SSEKMSKeyId")

    @property
    def sse_kms_encryption_context(self) -> str | None:
        return self.response.get("SSEKMSEncryptionContext")

    @property
    def bucket_key_enabled(self) -> bool | None:
        return self.response.get("BucketKeyEnabled")

    @property
    def request_charged(self) -> str | None:
        return self.response.get("RequestCharged")


@dataclasses.dataclass
class S3Parameter:
    """
    This class represents a S3 parameter stored in a S3 bucket. It handles configuration
    management with different behaviors based on whether S3 bucket versioning is enabled.

    **Why use S3 for configuration storage?**

    - Centralized configuration management across environments
    - Built-in durability and availability guarantees
    - Integration with AWS IAM for access control
    - Audit trail through CloudTrail
    - Cost-effective storage for configuration data

    **How it works:**

    The class adapts its behavior based on the S3 bucket's versioning configuration:

    **For Versioned Buckets (versioning enabled):**

    - Uses S3's native versioning capabilities
    - Stores only one file: ``${s3folder_config}/${parameter_name}.json``
    - Each deployment creates a new version of the same object
    - Version IDs are managed by S3 (e.g., "3/L4kqtJlcpXroDTDmpUMLUo")
    - Deletion creates delete markers rather than removing data
    - Supports hard deletion of all versions when needed

    **For Non-Versioned Buckets (versioning disabled):**

    - Implements custom versioning using file naming convention
    - Creates two files per deployment:

      - Latest: ``${s3folder_config}/${parameter_name}/${parameter_name}-latest.json``
      - Versioned: ``${s3folder_config}/${parameter_name}/${parameter_name}-000001.json``

    - Version numbers are sequential integers (1, 2, 3, ...)
    - Latest file contains metadata pointing to current version
    - Historical versions are preserved as separate objects

    **Version Management:**

    - Non-versioned: Uses integer sequence (1, 2, 3, ...)
    - Versioned: Uses S3 version IDs (UUIDs generated by S3)
    - Both support reading latest version and retrieving version history

    :param s3dir_config: S3 directory where the parameter is stored (no filename)
    :param parameter_name: Parameter name used as the base filename
    :param version_status: S3 bucket versioning status enum
    :param version_enabled: Whether S3 bucket versioning is enabled
    :param s3path_latest: S3 path to the file representing the latest version

    .. seealso::

        - :meth:`~S3Parameter.deploy_latest_when_version_not_enabled`
        - :meth:`~S3Parameter.deploy_latest_when_version_is_enabled`
        - :func:`~get_bucket_version_status`
    """

    s3dir_config: S3Path = dataclasses.field()
    parameter_name: str = dataclasses.field()
    version_status: S3BucketVersionStatus = dataclasses.field()
    version_enabled: bool = dataclasses.field()
    s3path_latest: S3Path = dataclasses.field()

    @classmethod
    def new(
        cls,
        bsm: BotoSesManager,
        s3folder_config: str,
        parameter_name: str,
    ) -> "T.Self":
        s3dir_config = S3Path(s3folder_config).to_dir()
        s3_bucket_version_status = get_bucket_version_status(
            bsm=bsm,
            bucket=s3dir_config.bucket,
        )
        _ensure_bucket_versioning_is_not_suspended(
            bucket=s3dir_config.bucket,
            status=s3_bucket_version_status,
        )
        if s3_bucket_version_status.is_enabled():
            s3path_latest = s3dir_config.joinpath(f"{parameter_name}.json")
        else:
            s3path_latest = s3dir_config.joinpath(
                parameter_name,
                f"{parameter_name}-latest.json",
            )
        return cls(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
            version_status=s3_bucket_version_status,
            version_enabled=s3_bucket_version_status.is_enabled(),
            s3path_latest=s3path_latest,
        )

    def read_latest(
        self,
        bsm: "BotoSesManager",
    ) -> tuple[dict, str]:
        """
        Read the latest config data and config version from S3.

        For versioning disabled bucket, the version is 1, 2, 3, ...
        For versioning enabled bucket, the version is the version id of the S3 object.
        """
        try:
            config_data = json_loads(self.s3path_latest.read_text(bsm=bsm))
        except botocore.exceptions.ClientError as e:
            if "NoSuchKey" in str(e):
                raise exc.S3ObjectNotExist(
                    f"S3 object {self.s3path_latest.uri} not exist."
                )
            else:  # pragma: no cover
                raise e
        if self.version_enabled:
            config_version = self.s3path_latest.version_id
        else:
            config_version = self.s3path_latest.metadata[
                AwsTagKeyEnum.CONFIG_VERSION.value
            ]
        return config_data, config_version

    def get_latest_config_version_when_version_not_enabled(
        self,
        bsm: "BotoSesManager",
    ) -> int | None:
        """
        Get the latest config version for non-versioned S3 buckets.

        **How it works:**

        1. First checks if the latest file exists and reads version from metadata
        2. If latest file doesn't exist, scans all versioned files in the directory
        3. Parses version numbers from filenames (e.g., myapp-000001.json -> 1)
        4. Returns the highest version number found

        **Why this method is needed:**

        Non-versioned buckets need custom version tracking since S3 doesn't provide
        native versioning. This method handles edge cases like:
        - Latest file deleted but versioned files remain
        - Corrupted metadata in latest file
        - Manual cleanup scenarios

        :param bsm: BotoSesManager for S3 operations
        :return: Latest version number (integer) or None if no versions found

        .. note::
            This method only works for non-versioned buckets. For versioned buckets,
            use :meth:`get_latest_config_version_when_version_is_enabled` instead.
        """
        if self.s3path_latest.exists(bsm=bsm):
            return int(self.s3path_latest.metadata[AwsTagKeyEnum.CONFIG_VERSION.value])
        else:
            versions: list[int] = list()
            for s3path in self.s3path_latest.parent.iter_objects(bsm=bsm):
                try:
                    versions.append(int(s3path.fname.split("-")[-1]))
                except Exception:  # pragma: no cover
                    pass
            if len(versions):
                return max(versions)
            else:
                return None

    def get_latest_config_version_when_version_is_enabled(
        self,
        bsm: "BotoSesManager",
    ) -> str | None:  # pragma: no cover
        """
        Get the latest config version for versioned S3 buckets.

        **How it works:**

        1. Lists the most recent 2 object versions using S3's versioning API
        2. Checks if the latest version is a delete marker
        3. If delete marker exists, returns the previous version ID
        4. Otherwise returns the current version ID

        **Why this method is needed:**

        Versioned buckets use S3's native versioning with UUID-based version IDs.
        This method handles the complexity of delete markers, which are created
        when objects are "deleted" in versioned buckets but the data still exists.

        **Version ID format:**
        S3 version IDs are opaque strings like "3/L4kqtJlcpXroDTDmpUMLUo"

        :param bsm: BotoSesManager for S3 operations
        :return: S3 version ID (string) or None if no versions found

        .. note::
            This method only works for versioned buckets. For non-versioned buckets,
            use :meth:`get_latest_config_version_when_version_not_enabled` instead.
        """
        s3path_list = self.s3path_latest.list_object_versions(limit=2, bsm=bsm).all()
        if len(s3path_list) == 0:
            return None
        else:
            if s3path_list[0].is_delete_marker():
                return s3path_list[1].version_id
            else:
                return s3path_list[0].version_id

    def deploy_latest_when_version_not_enabled(
        self,
        bsm: "BotoSesManager",
        config_data: dict[str, T.Any],
        config_version: str,
        tags: dict[str, str] | None = OPT,
    ) -> S3Object:
        """
        Deploy configuration to a non-versioned S3 bucket.

        **How it works:**

        1. Creates a versioned file: ``{parameter_name}-{version}.json``
        2. Uploads config data with version metadata and SHA256 checksum
        3. Copies the versioned file to create/update the latest file
        4. Both files contain identical data but serve different purposes

        **Why two files are needed:**

        - **Versioned file**: Permanent historical record, never overwritten
        - **Latest file**: Always points to current version, used for reading

        **File structure created:**

        - ``myapp/myapp-000001.json`` (versioned, permanent)
        - ``myapp/myapp-latest.json`` (latest pointer, overwritten)

        **Metadata stored:**

        - ``config_version``: Version number for tracking
        - ``config_sha256``: Content checksum for integrity

        :param bsm: BotoSesManager for S3 operations
        :param config_data: Configuration data to store
        :param config_version: Version string (will be zero-padded)
        :param tags: Optional S3 object tags
        :return: S3Object representing the deployed versioned file

        .. note::
            This method only works for non-versioned buckets. For versioned buckets,
            use :meth:`deploy_latest_when_version_is_enabled` instead.
        """
        basename = f"{self.parameter_name}-{config_version.zfill(ZFILL)}.json"
        s3path_versioned = self.s3path_latest.change(new_basename=basename)
        content = json.dumps(config_data, indent=4)
        config_sha256 = sha256_of_config_data(config_data)
        s3path_res = s3path_versioned.write_text(
            content,
            content_type="application/json",
            metadata={
                AwsTagKeyEnum.CONFIG_VERSION.value: config_version,
                AwsTagKeyEnum.CONFIG_SHA256.value: config_sha256,
            },
            tags=tags,
            bsm=bsm,
        )
        s3object = S3Object.from_s3path(s3path_res)
        s3path_versioned.copy_to(self.s3path_latest, overwrite=True, bsm=bsm)
        return s3object

    def deploy_latest_when_version_is_enabled(
        self,
        bsm: "BotoSesManager",
        config_data: dict[str, T.Any],
        tags: dict[str, str] | None = OPT,
    ) -> "S3Object":
        """
        Deploy configuration to a versioned S3 bucket.

        **How it works:**

        1. Uploads config data to the same S3 object path
        2. S3 automatically creates a new version with unique version ID
        3. Only stores SHA256 checksum in metadata (no version number needed)
        4. S3 manages all version history natively

        **Why versioned buckets are simpler:**

        - No manual version tracking required
        - S3 handles version IDs automatically
        - No need for separate "latest" and "versioned" files
        - Built-in atomic updates

        **File structure:**

        - Single file: ``myapp.json``
        - Multiple versions: Same path, different version IDs
        - S3 automatically serves latest version by default

        **Metadata stored:**

        - ``config_sha256``: Content checksum for integrity
        - No version metadata needed (S3 provides version ID)

        **Version ID examples:**
        S3 generates UUIDs like "3/L4kqtJlcpXroDTDmpUMLUo"

        :param bsm: BotoSesManager for S3 operations
        :param config_data: Configuration data to store
        :param tags: Optional S3 object tags
        :return: S3Object representing the deployed configuration

        .. note::
            This method only works for versioned buckets. For non-versioned buckets,
            use :meth:`deploy_latest_when_version_not_enabled` instead.
        """
        content = json.dumps(config_data, indent=4)
        config_sha256 = sha256_of_config_data(config_data)
        s3path_res = self.s3path_latest.write_text(
            content,
            content_type="application/json",
            metadata={
                AwsTagKeyEnum.CONFIG_SHA256.value: config_sha256,
            },
            tags=tags,
            bsm=bsm,
        )
        s3object = S3Object.from_s3path(s3path_res)
        return s3object


def _show_deploy_info(s3path: "S3Path"):
    logger.info(f"🚀️ deploy config file/files at {s3path.uri} ...")
    logger.info(f"preview at: {s3path.console_url}")


def read_config(
    bsm: "BotoSesManager",
    s3folder_config: str,
    parameter_name: str,
) -> tuple[dict, str]:
    """
    Read config data and config version from S3.

    :return: config data and version
    """
    s3parameter = S3Parameter.new(
        bsm=bsm,
        s3folder_config=s3folder_config,
        parameter_name=parameter_name,
    )
    return s3parameter.read_latest(bsm=bsm)


@logger.start_and_end(
    msg="deploy config file to S3",
)
def deploy_config(
    bsm: "BotoSesManager",
    s3folder_config: str,
    parameter_name: str,
    config_data: dict,
    tags: dict[str, str] | None = OPT,
) -> S3Object | None:
    """
    Deploy configuration to AWS S3 with automatic versioning behavior.

    **Behavior varies by bucket versioning:**

    **Versioned buckets:**
    - Creates/updates single file: ``{s3folder_config}/{parameter_name}.json``
    - S3 automatically manages versions with unique version IDs
    - No duplicate files, cleaner structure

    **Non-versioned buckets:**
    - Creates two files per deployment:
      - Latest: ``{s3folder_config}/{parameter_name}/{parameter_name}-latest.json``
      - Versioned: ``{s3folder_config}/{parameter_name}/{parameter_name}-000001.json``
    - Manual version tracking with sequential numbering

    **Smart deployment:**
    - Compares config data before deployment
    - Skips deployment if data unchanged (returns None)
    - Prevents unnecessary S3 operations and costs

    :param bsm: BotoSesManager object for AWS operations
    :param s3folder_config: S3 directory where parameter is stored (no filename)
    :param parameter_name: Parameter name used as base filename
    :param config_data: Configuration data to deploy
    :param tags: Optional S3 object tags

    :return: S3Object representing deployed config, or None if no deployment needed
    """
    s3parameter = S3Parameter.new(
        bsm=bsm,
        s3folder_config=s3folder_config,
        parameter_name=parameter_name,
    )
    s3path_latest = s3parameter.s3path_latest
    _show_deploy_info(s3path=s3path_latest)

    already_exists = s3path_latest.exists(bsm=bsm)
    if already_exists:
        existing_config_data, _ = s3parameter.read_latest(bsm=bsm)
        if existing_config_data == config_data:
            logger.info("config data is the same as existing one, do nothing.")
            return None

    if s3parameter.version_enabled is False:
        latest_version = s3parameter.get_latest_config_version_when_version_not_enabled(
            bsm=bsm,
        )
        if latest_version is None:
            new_version = 1
        else:
            new_version = latest_version + 1
        s3object = s3parameter.deploy_latest_when_version_not_enabled(
            bsm=bsm,
            config_data=config_data,
            config_version=str(new_version),
            tags=tags,
        )
    else:
        s3object = s3parameter.deploy_latest_when_version_is_enabled(
            bsm=bsm,
            config_data=config_data,
            tags=tags,
        )
    logger.info("done!")
    return s3object


def _show_delete_info(s3path: "S3Path"):
    logger.info(f"🗑️ delete config file/files at: {s3path.uri} ...")
    logger.info(f"preview at: {s3path.console_url}")


@logger.start_and_end(
    msg="delete config file from S3",
)
def delete_config(
    bsm: "BotoSesManager",
    s3folder_config: str,
    parameter_name: str,
    include_history: bool = False,
) -> bool:
    """
    Delete configuration from AWS S3 with different behaviors for versioned/non-versioned buckets.

    **Behavior varies by bucket versioning:**

    **Non-versioned buckets:**

    - ``include_history=False``: Deletes only the latest file
      - ``{s3folder_config}/{parameter_name}/{parameter_name}-latest.json``
      - Historical versions remain: ``{parameter_name}-000001.json``, etc.

    - ``include_history=True``: Deletes entire parameter directory
      - ``{s3folder_config}/{parameter_name}/`` (all files)
      - Removes all versions permanently

    **Versioned buckets:**

    - ``include_history=False``: Creates delete marker
      - ``{s3folder_config}/{parameter_name}.json`` appears deleted
      - All versions still exist, just hidden by delete marker
      - Can be recovered by removing delete marker

    - ``include_history=True``: Permanent deletion
      - ``{s3folder_config}/{parameter_name}.json`` deleted completely
      - All historical versions permanently removed
      - Cannot be recovered

    **Why different deletion strategies:**

    - Non-versioned: File-based cleanup, granular control
    - Versioned: Leverages S3's native versioning and delete markers
    - Soft delete (delete marker) vs hard delete (permanent removal)

    :param bsm: BotoSesManager object for AWS operations
    :param s3folder_config: S3 directory where parameter is stored (no filename)
    :param parameter_name: Parameter name used as base filename
    :param include_history: Whether to delete all historical versions permanently

    :return: True if deletion was performed

    .. seealso::
        - `S3 delete_object API <https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.delete_object>`_
        - :class:`S3BucketVersionStatus` for version behavior details
    """
    s3parameter = S3Parameter.new(
        bsm=bsm,
        s3folder_config=s3folder_config,
        parameter_name=parameter_name,
    )
    s3path_latest = s3parameter.s3path_latest
    if s3parameter.version_enabled is False:
        if include_history:
            _show_delete_info(s3path_latest.parent)
            s3path_latest.parent.delete(bsm=bsm)
        else:
            _show_delete_info(s3path_latest)
            s3path_latest.delete(bsm=bsm)
    else:
        _show_delete_info(s3path_latest)
        s3path_latest.delete(bsm=bsm, is_hard_delete=include_history)
    logger.info("done!")
    return True
