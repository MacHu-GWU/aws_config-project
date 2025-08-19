# -*- coding: utf-8 -*-

"""
Simple Command Pattern wrapper for AWS configuration deployment operations.
"""

import typing as T
import json
import dataclasses

from func_args.api import OPT
from s3pathlib import S3Path
from simple_aws_ssm_parameter_store.api import (
    ParameterType,
    ParameterTier,
    Parameter,
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
    Simple wrapper for AWS configuration deployment operations.

    Uses Command Pattern to encapsulate deployment operations to SSM Parameter Store
    and S3. Simplifies AWS operations by providing unified interface for deployment
    and cleanup across different AWS services.

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

    # @property
    # def parameter_name_for_arn(self) -> str:
    #     """
    #     Return the parameter name for ARN. The parameter name could have
    #     a leading "/", in this case, we should strip it out.
    #     """
    #     if self.parameter_name.startswith("/"):  # pragma: no cover
    #         return self.parameter_name[1:]
    #     else:
    #         return self.parameter_name

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
    ) -> tuple[Parameter | None, Parameter | None]:
        """
        Deploy configuration to SSM Parameter Store.

        :param ssm_client: SSMClient for AWS operations
        :param tags: Additional AWS resource tags
        :return: Tuple of (before_param, after_param)
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
        Deploy configuration to S3 with versioning.

        :param s3_client: S3Client for AWS operations
        :param version: Version number for the deployment
        :param tags: Additional AWS resource tags
        :return: Tuple of (latest_s3path, versioned_s3path)
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
            s3_client=s3_client,
            value=self.parameter_value,
            version=None,
            write_text_kwargs={"tags": tags},
        )
        s3path_versioned = s3_parameter.write(
            s3_client=s3_client,
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
        Delete configuration from SSM Parameter Store.

        :param ssm_parameter: SSMClient for AWS operations
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
        Delete configuration from S3.

        :param s3_client: S3Client for AWS operations
        :param version: Version to delete, None for latest
        """
        s3_parameter = S3Parameter(
            s3dir_config=self.s3dir_config,
            parameter_name=self.parameter_name,
        )
        s3path = s3_parameter.get_s3path(version=version)
        s3path.delete(bsm=s3_client)
