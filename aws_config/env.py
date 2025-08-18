# -*- coding: utf-8 -*-

"""
"""

# standard library
try:
    import typing_extensions as T
except:  # pragma: no cover
    import typing as T
import os
import json
import string
import dataclasses
from pathlib import Path
from functools import cached_property

# third party library (include vendor)
from pydantic import BaseModel, Field, ConfigDict, field_validator
from which_runtime.api import Runtime
from which_env.api import (
    validate_env_name,
    CommonEnvNameEnum,
    BaseEnvNameEnum,
    detect_current_env,
)
from which_bsm.api import BaseBotoSesEnum
from .vendor.jsonutils import json_loads
from .vendor.strutils import slugify

# modules from this submodule
from .constants import AwsTagKeyEnum, EnvVarNameEnum
from .app import AppMixin
from .name import NameMixin
from .deploy import DeployMixin

# type hint
if T.TYPE_CHECKING:  # pragma: no cover
    from s3pathlib import S3Path
    from boto_session_manager import BotoSesManager


def validate_project_name(project_name: str):
    """
    Validate project name follows AWS-compatible naming conventions.

    Ensures project names work across AWS services and infrastructure tools.
    Rules enforce compatibility with S3 buckets, SSM parameters, and IAM resources.

    :param project_name: Project identifier to validate
    :raises ValueError: If name violates naming rules
    """
    if project_name[0] not in string.ascii_lowercase:
        raise ValueError("first letter of project_name has to be a-z!")
    if project_name[-1] not in (string.ascii_lowercase + string.digits):
        raise ValueError("last letter of project_name has to be a-z, 0-9!")
    if len(set(project_name).difference(string.ascii_lowercase + string.digits + "_-")):
        raise ValueError("project_name can only has a-z, 0-9, - or _!")


def normalize_parameter_name(param_name: str) -> str:
    """
    Normalize parameter names to comply with AWS SSM Parameter Store restrictions.

    AWS SSM Parameter Store prohibits parameter names that start with "aws" or "ssm"
    reserved prefixes. This function automatically adds a "p-" prefix to avoid conflicts.

    **Why this is needed:**
    - Prevents runtime errors when deploying parameters
    - Ensures consistent naming across all AWS accounts
    - Handles edge cases where project names might conflict with AWS reserved words

    :param param_name: Original parameter name
    :return: AWS-compliant parameter name

    Example:
        - "my-project" -> "my-project" (unchanged)
        - "aws-project" -> "p-aws-project" (prefixed)
        - "ssm-config" -> "p-ssm-config" (prefixed)

    Reference:
        https://docs.aws.amazon.com/cli/latest/reference/ssm/put-parameter.html#options
    """
    if param_name.startswith("aws") or param_name.startswith("ssm"):
        return f"p-{param_name}"
    else:
        return param_name


class BaseEnv(
    BaseModel,
    AppMixin,
    NameMixin,
    DeployMixin,
):
    """ """

    model_config = ConfigDict(
        frozen=True,
        extra="ignore",
    )

    project_name: str = Field(default=None)
    env_name: str = Field(default=None)
    aws_account_id: str | None = Field(default=None)
    aws_region: str | None = Field(default=None)
    s3uri_data: str | None = Field(default=None)
    s3uri_artifacts: str | None = dataclasses.field(default=None)

    @field_validator("project_name", mode="after")
    @classmethod
    def _check_project_name(cls, value: str) -> str:
        validate_project_name(value)
        return value

    @field_validator("env_name", mode="after")
    @classmethod
    def _check_env_name(cls, value: str) -> str:
        validate_env_name(value)
        return value

    @classmethod
    def from_dict(cls, data: dict[str, T.Any]) -> "T.Self":
        return cls(**data)

    def to_dict(self) -> dict[str, T.Any]:
        return self.model_dump()

    @cached_property
    def project_name_slug(self) -> str:
        """
        Project name with hyphen delimiters for URLs and display.

        Example: "my_project" becomes "my-project"
        """
        return slugify(self.project_name, delim="-")

    @cached_property
    def project_name_snake(self) -> str:
        """
        Project name with underscore delimiters for code and systems.

        Example: "my-project" becomes "my_project"
        """
        return slugify(self.project_name, delim="_")

    @cached_property
    def prefix_name_slug(self) -> str:
        """
        Combined project and environment name with hyphen delimiters.

        Used for resource naming and display purposes.
        Example: "my-project-dev"
        """
        return f"{self.project_name_slug}-{self.env_name}"

    @cached_property
    def prefix_name_snake(self) -> str:
        """
        Combined project and environment name with mixed delimiters.

        Used for system identifiers and internal naming.
        Example: "my_project-dev"
        """
        return f"{self.project_name_snake}-{self.env_name}"

    @cached_property
    def parameter_name(self) -> str:
        """
        AWS SSM Parameter Store name for this environment's configuration.

        Follows the pattern: "${project_name}-${env_name}" with automatic
        normalization to avoid AWS naming restrictions (e.g., "aws" prefix).

        Example: "my_project-dev" -> "my_project-dev"
        Example: "aws_project-prod" -> "p-aws_project-prod"
        """
        return normalize_parameter_name(self.prefix_name_snake)

    @property
    def s3dir_data(self) -> S3Path:
        """
        :class:`s3pathlib.S3Path` object of ``s3uri_data``
        """
        return S3Path.from_s3_uri(self.s3uri_data).to_dir()

    @property
    def s3dir_env_data(self) -> S3Path:
        """
        Environment specific s3 folder to store project data.
        """
        return self.s3dir_data.joinpath("envs", self.env_name).to_dir()

    @property
    def s3dir_artifacts(self) -> S3Path:  # pragma: no cover
        """
        Shared artifacts s3 dir for all environments.
        """
        return S3Path.from_s3_uri(self.s3uri_artifacts).to_dir()

    @property
    def s3dir_env_artifacts(self) -> S3Path:  # pragma: no cover
        """
        Env specific artifacts s3 dir.

        example: ``${s3dir_artifacts}/envs/${env_name}/
        """
        return self.s3dir_artifacts.joinpath("envs", self.env_name).to_dir()

    @property
    def s3dir_tmp_artifacts(self) -> S3Path:
        """
        Example: ``${s3dir_artifacts}/tmp/``
        """
        return self.s3dir_artifacts.joinpath("tmp").to_dir()

    @property
    def s3dir_config_artifacts(self) -> S3Path:
        """
        Example: ``${s3dir_artifacts}/config/``
        """
        return self.s3dir_artifacts.joinpath("config").to_dir()

    @property
    def env_vars(self: "BaseEnv") -> dict[str, str]:
        """
        Common environment variable for all computational resources in this environment.
        It is primarily for "self awareness" (detect who I am, which environment I am in).
        """
        return {
            EnvVarNameEnum.PARAMETER_NAME.value: self.parameter_name,
            EnvVarNameEnum.PROJECT_NAME.value: self.project_name,
            EnvVarNameEnum.ENV_NAME.value: self.env_name,
        }

    @property
    def devops_aws_tags(self: "BaseEnv") -> dict[str, str]:
        """
        Common AWS resources tags for all resources in devops environment.
        """
        return {
            AwsTagKeyEnum.tech_project_name.value: self.project_name,
            AwsTagKeyEnum.tech_env_name.value: CommonEnvNameEnum.devops.value,
        }

    @property
    def workload_aws_tags(self: "BaseEnv") -> dict[str, str]:
        """
        Common AWS resources tags for all resources in workload environment.
        """
        return {
            AwsTagKeyEnum.tech_project_name.value: self.project_name,
            AwsTagKeyEnum.tech_env_name.value: self.env_name,
        }

    @property
    def cloudformation_stack_name(self: "BaseEnv") -> str:
        """
        Cloudformation stack name.
        """
        return self.prefix_name_slug


T_BASE_ENV = T.TypeVar("T_BASE_ENV", bound=BaseEnv)
