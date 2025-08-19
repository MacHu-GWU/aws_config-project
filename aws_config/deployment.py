# -*- coding: utf-8 -*-

import typing as T
import json
import dataclasses

from func_args.api import OPT
from s3pathlib import S3Path
from simple_aws_ssm_parameter_store.api import (
    ParameterType,
    ParameterTier,
    put_parameter_if_changed,
    delete_parameter,
)

from .constants import AwsTagKeyEnum
from .utils import sha256_of_config_data
from .s3 import (
    S3Parameter,
)

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_ssm.client import SSMClient
    from mypy_boto3_s3.client import S3Client


@dataclasses.dataclass
class Deployment:
    """
    Represents a single configuration deployment operation to AWS.

    This class encapsulates the data and metadata for deploying configuration
    to either AWS SSM Parameter Store or S3. It serves as a deployment unit
    that can be executed independently and tracks its own state.

    **Key responsibilities:**

    - Package configuration data for deployment
    - Execute deployment to SSM Parameter Store or S3
    - Handle deletion operations
    - Track deployment results and status

    **Deployment targets:**

    - **SSM Parameter Store**: Fast runtime access, encrypted storage
    - **S3**: Historical backup, versioning, cost-effective storage

    **Usage pattern:**

    1. Created by :meth:`BaseConfig.prepare_deploy` for each environment
    2. Execute deployment via :meth:`deploy_to_ssm_parameter` or :meth:`deploy_to_s3`
    3. Track results through deployment and deletion attributes

    :param parameter_name: AWS resource name for this deployment
    :param parameter_data: Configuration data to deploy (dict format)
    :param project_name: Project identifier for resource tagging
    :param env_name: Environment name (or 'all' for consolidated config)
    """

    parameter_name: str = dataclasses.field()
    parameter_data: dict[str, T.Any] = dataclasses.field()
    project_name: str = dataclasses.field()
    env_name: str = dataclasses.field()
    s3dir_config: S3Path = dataclasses.field()

    @property
    def parameter_value(self) -> str:
        return json.dumps(self.parameter_data)

    @property
    def parameter_name_for_arn(self) -> str:
        """
        Return the parameter name for ARN. The parameter name could have
        a leading "/", in this case, we should strip it out.
        """
        if self.parameter_name.startswith("/"):  # pragma: no cover
            return self.parameter_name[1:]
        else:
            return self.parameter_name

    def deploy_to_ssm_parameter(
        self,
        ssm_client: "SSMClient",
        description: str | None = OPT,
        type: ParameterType | None = OPT,
        tier: ParameterTier | None = OPT,
        key_id: str | None = OPT,
        overwrite: bool = False,
        allowed_pattern: str | None = OPT,
        tags: dict[str, str] | None = OPT,
        policies: str | None = OPT,
        data_type: str | None = OPT,
    ):
        """
        Deploy configuration data to AWS SSM Parameter Store.

        Stores configuration as encrypted JSON in SSM Parameter Store for
        fast runtime access by applications. Automatically adds metadata
        tags for tracking and organization.

        :param ssm_client: SSMClient for AWS operations
        :param parameter_with_encryption: Whether to encrypt parameter value
        :param tags: Additional AWS resource tags
        :param verbose: Whether to log deployment progress
        :return: AWS Parameter object representing the deployed configuration
        """
        if tags is None:
            tags = {}
        config_sha256 = sha256_of_config_data(self.parameter_data)
        new_tags = {
            AwsTagKeyEnum.project_name.value: self.project_name,
            AwsTagKeyEnum.env_name.value: self.env_name,
            AwsTagKeyEnum.config_sha256.value: config_sha256,
        }
        tags.update(new_tags)
        before_param, after_param = put_parameter_if_changed(
            ssm_client=ssm_client,
            name=self.parameter_name,
            value=self.parameter_value,
            description=description,
            type=type,
            tier=tier,
            key_id=key_id,
            overwrite=overwrite,
            allowed_pattern=allowed_pattern,
            tags=tags,
            policies=policies,
            data_type=data_type,
        )
        return before_param, after_param

    def deploy_to_s3(
        self,
        s3_client: "S3Client",
        version: int,
        tags: dict[str, str] | None = None,
    ) -> tuple[S3Path, S3Path]:
        """
        Deploy configuration data to AWS S3 for backup and versioning.

        Stores configuration as JSON files in S3 with automatic versioning
        support. Provides cost-effective historical storage and audit trail.

        :param bsm: BotoSesManager for AWS operations
        :param s3folder_config: S3 folder URI where config will be stored
        :param tags: Additional AWS resource tags
        :param verbose: Whether to log deployment progress
        :return: S3Object representing the deployed configuration file
        """
        if tags is None:
            tags = {}
        new_tags = {
            AwsTagKeyEnum.project_name: self.project_name,
            AwsTagKeyEnum.env_name: self.env_name,
        }
        tags.update(new_tags)
        s3_parameter = S3Parameter(
            s3dir_config=self.s3dir_config,
            parameter_name=self.parameter_name,
        )
        s3path_latest = s3_parameter.write(
            bsm=s3_client,
            value=self.parameter_value,
            version=None,
            write_text_kwargs={"tags": tags},
        )
        s3path_versioned = s3_parameter.write(
            bsm=s3_client,
            value=self.parameter_value,
            version=version,
            write_text_kwargs={"tags": tags},
        )
        return s3path_latest, s3path_versioned

    def delete_from_ssm_parameter(
        self,
        ssm_parameter: "SSMClient",
    ) -> bool:
        """
        Delete configuration from AWS SSM Parameter Store.

        Permanently removes the parameter from SSM Parameter Store.
        This operation cannot be undone.

        :param bsm: BotoSesManager for AWS operations

        :return: Boolean indicating if deletion occurred
        """
        return delete_parameter(
            ssm_client=ssm_parameter,
            name=self.parameter_name,
        )

    def delete_from_s3(
        self,
        s3_client: "S3Client",
        version: int | None = None,
    ):
        """
        Delete configuration from AWS S3.

        Removes configuration files from S3. Behavior depends on the
        include_history flag and bucket versioning settings.

        :param bsm: BotoSesManager for AWS operations

        :return: Boolean indicating if deletion occurred
        """
        s3_parameter = S3Parameter(
            s3dir_config=self.s3dir_config,
            parameter_name=self.parameter_name,
        )
        s3path = s3_parameter.get_s3path(version=version)
        s3path.delete(bsm=s3_client)
