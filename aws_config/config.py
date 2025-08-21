# -*- coding: utf-8 -*-

"""
Configuration management with inheritance, validation, and AWS deployment.

Provides BaseConfig class for hierarchical configuration management with
shared value inheritance, environment-specific parameter generation, and
integrated deployment to AWS SSM Parameter Store and S3.
"""

try:
    import typing_extensions as T
except ImportError:  # pragma: no cover
    import typing as T
import os
import copy
import json
import dataclasses
from pathlib import Path
from functools import cached_property

from func_args.api import OPT
from s3pathlib import S3Path
from simple_aws_ssm_parameter_store.api import (
    ParameterType,
    ParameterTier,
    Parameter,
    get_parameter,
    put_parameter_if_changed,
    delete_parameter,
)
from which_env.api import validate_env_name, BaseEnvNameEnum
from configcraft.api import DEFAULTS, apply_inheritance, deep_merge
from .vendor.strutils import slugify
from .vendor.jsonutils import json_loads

from .constants import ALL, DATA, SECRET_DATA, AwsTagKeyEnum
from .utils import sha256_of_text
from .s3 import S3Parameter
from .env import validate_project_name, normalize_parameter_name, T_BASE_ENV

if T.TYPE_CHECKING:  # pragma: no cover
    from mypy_boto3_ssm.client import SSMClient
    from mypy_boto3_s3.client import S3Client

T_BASE_ENV_NAME_ENUM = T.TypeVar(
    "T_BASE_ENV_NAME_ENUM",
    bound=BaseEnvNameEnum,
)


@dataclasses.dataclass(frozen=True)
class DeploymentResult:
    """
    Result of a :meth:`BaseConfig.deploy_env_parameter` operation.
    to AWS SSM Parameter Store and S3.

    :param before_param: Parameter before deployment (if exists)
    :param after_param: Parameter after deployment (if exists)
    :param s3path_latest: S3 path for the latest version (if created)
    :param s3path_versioned: S3 path for the versioned file (if created)
    """

    before_param: Parameter | None
    after_param: Parameter | None
    s3dir_config: S3Path
    s3path_latest: S3Path | None
    s3path_versioned: S3Path | None

    def __post_init__(self):
        if (self.before_param is None) and (
            self.after_param is None
        ):  # pragma: no cover
            raise ValueError(
                "Either before_param (when it's an update) "
                "or after_param (when it's a create) must be set."
            )
        v = sum([(self.s3path_latest is None), (self.s3path_versioned is None)])
        if v == 1:  # pragma: no cover
            raise ValueError(
                "s3path_latest and s3path_versioned must both be set "
                "(when it's a create or update) or not set (when there is not update."
            )

    @property
    def is_ssm_deployed(self) -> bool:
        """
        Indicate if SSM parameter deployment operation happened.
        """
        return self.after_param is not None

    @property
    def is_s3_deployed(self) -> bool:
        """
        Indicate if S3 parameter deployment operation happened.
        """
        return self.s3path_latest is not None

    @property
    def parameter_name(self) -> str:
        """
        Get the SSM parameter name after deployment.
        """
        if self.after_param is None:
            return self.before_param.name
        else:
            return self.after_param.name

    @property
    def version(self) -> int:
        """
        Get the version of the SSM parameter after deployment.
        """
        if self.after_param is None:
            return self.before_param.version
        else:
            return self.after_param.version


@dataclasses.dataclass(frozen=True)
class DeleteResult:
    """
    Result of a :meth:`BaseConfig.delete_env_parameter` operation.
    to AWS SSM Parameter Store and S3.

    :param parameter_name: Name of the deleted SSM parameter
    :param s3dir_config: S3 directory where the configuration was stored
    """

    parameter_name: str
    s3dir_config: S3Path | None


@dataclasses.dataclass
class BaseConfig(
    T.Generic[T_BASE_ENV, T_BASE_ENV_NAME_ENUM],
):
    """
    Hierarchical configuration management with inheritance, validation, and AWS deployment.

    BaseConfig is the core of the ``aws_config`` library, providing a complete configuration
    lifecycle from local development to production deployment. It handles shared value
    inheritance, environment-specific parameter generation, validation, and integrated
    deployment to AWS SSM Parameter Store and S3.

    **Design Philosophy**

    The configuration system uses a hierarchical structure with shared value inheritance
    to minimize duplication while maintaining environment-specific customization. Two
    separate data structures handle sensitive and non-sensitive configuration:

    - ``data``: Non-sensitive configuration (project settings, S3 URIs, etc.)
    - ``secret_data``: Sensitive configuration (passwords, API keys, account IDs)

    This separation allows for different security handling, deployment strategies, and
    access controls while maintaining a unified configuration interface.

    **Configuration Structure**

    Configuration uses a DEFAULTS section for shared values with environment-specific
    overrides. Shared values use special prefixes to control inheritance:

    - ``*`` prefix: Available to all environments
    - ``env.`` prefix: Available only to the specified environment

    Example configuration structure::

        # Non-sensitive data
        data = {
            "DEFAULTS": {
                "*.project_name": "my_app",         # Available to all environments
                "*.aws_region": "us-east-1",        # Shared AWS region
                "dev.debug_enabled": True,          # Only available to dev environment
            },
            "dev": {
                "s3uri_data": "s3://myapp-dev/data/",
                "s3uri_artifacts": "s3://myapp-dev/artifacts/",
                "username": "alice",
            },
            "prod": {
                "s3uri_data": "s3://myapp-prod/data/",
                "s3uri_artifacts": "s3://myapp-prod/artifacts/",
                "username": "bob",
            },
        }

        # Sensitive data
        secret_data = {
            "dev": {
                "aws_account_id": "111111111111",
                "password": "alicepassword",
            },
            "prod": {
                "aws_account_id": "111111111111",
                "password": "bobpassword",
            },
        }

    **Configuration Lifecycle**

    The configuration follows a complete lifecycle from development to production:

    1. **Development**: Start with two local JSON files

       - ``config.json``: Non-sensitive configuration
       - ``secret-config.json``: Sensitive configuration (gitignored)

       Load using :meth:`load_from_file`::

           data, secret_data = BaseConfig.load_from_file(
               "config.json",
               "secret-config.json"
           )
           config = MyConfig(
               data=data,
               secret_data=secret_data,
               EnvClass=MyEnv,
               EnvNameEnumClass=MyEnvEnum,
               version="1.0.0"
           )

    2. **Deployment**: Admin deploys configuration to AWS infrastructure

       Deploy to SSM Parameter Store (primary) and S3 (backup) using :meth:`deploy_env_parameter`::

           # Deploy specific environment
           result = config.deploy_env_parameter(
               ssm_client=ssm_client,
               s3_client=s3_client,
               s3dir_config=S3Path("s3://my-config-bucket/configs/"),
               env_name="prod",
               type=ParameterType.SECURE_STRING
           )

           # Deploy consolidated multi-environment configuration
           result = config.deploy_env_parameter(
               ssm_client=ssm_client,
               s3_client=s3_client,
               s3dir_config=S3Path("s3://my-config-bucket/configs/"),
               # env_name defaults to ALL
           )

       This creates:

       - SSM Parameter: ``my_app-prod`` (environment-specific) or ``my_app`` (consolidated)
       - S3 Files: ``latest.json`` and ``v1.json`` (versioned backup)

    3. **Runtime Access**: Applications load configuration from SSM

       Use :meth:`load_parameter` in application code::

           # Production application loads its specific environment
           data, secret_data = BaseConfig.load_parameter(
               ssm_client=ssm_client,
               parameter_name="my_app-prod",
               with_decryption=True
           )
           config = MyConfig(data=data, secret_data=secret_data, ...)
           env = config.get_env("prod")

           # Admin tools load consolidated configuration
           data, secret_data = BaseConfig.load_parameter(
               ssm_client=ssm_client,
               parameter_name="my_app",
               with_decryption=True
           )
           config = MyConfig(data=data, secret_data=secret_data, ...)
           dev_env = config.get_env("dev")
           prod_env = config.get_env("prod")

    4. **Disaster Recovery**: Restore from S3 backup when SSM is unavailable

       Use :meth:`load_from_s3` for disaster recovery::

           # Load from S3 backup when SSM Parameter Store is unavailable
           data, secret_data = BaseConfig.load_from_s3(
               s3_client=s3_client,
               s3dir_config=S3Path("s3://my-config-bucket/configs/"),
               parameter_name="my_app-prod"  # or None for auto-discovery
           )
           config = MyConfig(data=data, secret_data=secret_data, ...)

    **Parameter Naming Conventions**

    SSM Parameter names follow consistent patterns:

    - Environment-specific: ``{project_name}-{env_name}`` (e.g., ``my_app-prod``)
    - Consolidated: ``{project_name}`` (e.g., ``my_app``)

    S3 backup structure:

    - See :meth:`aws_config.s3.S3Parameter.get_s3path`

    **Type Safety and Validation**

    BaseConfig uses generic types to ensure type safety:

    - ``T_BASE_ENV``: Environment dataclass type (e.g., ``MyEnv``)
    - ``T_BASE_ENV_NAME_ENUM``: Environment name enum type (e.g., ``MyEnvEnum``)

    Environment instances are automatically validated using Pydantic/dataclass
    validation when calling :meth:`get_env`.

    **Deployment Strategy**

    The library supports both environment-specific and consolidated deployment patterns:

    - **Environment-specific**: Each environment gets its own SSM parameter
      (``my_app-dev``, ``my_app-prod``) containing only that environment's configuration
    - **Consolidated**: Single SSM parameter (``my_app``) contains all environments
      for admin tools and cross-environment operations

    **Security Features**

    - Separation of sensitive and non-sensitive data
    - Automatic encryption using SSM SecureString parameters
    - S3 backup with optional encryption and access controls
    - Configurable AWS resource tagging for compliance and cost tracking

    **Error Handling**

    - Validates project and environment names against standards
    - Validates configuration structure and data types
    - Provides detailed error messages for configuration issues
    - Graceful handling of missing parameters and network issues

    :param data: Non-sensitive configuration data with DEFAULTS and environment sections
    :param secret_data: Sensitive configuration data organized by environment
    :param EnvClass: Environment dataclass type for validation and type safety
    :param EnvNameEnumClass: Environment name enum class for validation
    :param version: Configuration version for tracking and deployment

    Example:
        Complete configuration setup::

            from enum import Enum
            from dataclasses import dataclass
            from pydantic import Field

            class EnvNameEnum(BaseEnvNameEnum):
                dev = "dev"
                prod = "prod"

            @dataclass
            class MyEnv(BaseEnv):
                username: str = Field()
                password: str = Field()
                s3uri_data: str = Field()

            class MyConfig(BaseConfig[MyEnv, EnvNameEnum]):
                pass

            # Load from local files during development
            data, secret_data = MyConfig.load_from_file(
                "config.json", "secret-config.json"
            )

            config = MyConfig(
                data=data,
                secret_data=secret_data,
                EnvClass=MyEnv,
                EnvNameEnumClass=EnvNameEnum,
                version="1.0.0"
            )

            # Get typed environment configuration
            prod_env = config.get_env("prod")
            print(f"Production S3 URI: {prod_env.s3uri_data}")
            print(f"Username: {prod_env.username}")

    .. note::
        This is the core class of the aws_config library. All configuration
        management, validation, inheritance, and deployment functionality
        is built around this class.

    .. seealso::
        - :class:`~aws_config.env.BaseEnv` for environment dataclass base
        - :meth:`deploy_env_parameter` for AWS deployment
        - :meth:`load_parameter` for runtime configuration loading
        - :meth:`load_from_s3` for disaster recovery scenarios
    """

    data: dict = dataclasses.field()
    secret_data: dict = dataclasses.field()
    EnvClass: T.Type["T_BASE_ENV"] = dataclasses.field()
    EnvNameEnumClass: T.Type["T_BASE_ENV_NAME_ENUM"] = dataclasses.field()
    version: str = dataclasses.field()

    _applied_data: dict = dataclasses.field(init=False)
    _applied_secret_data: dict = dataclasses.field(init=False)
    _merged_data: dict = dataclasses.field(init=False)

    def _validate(self):
        """
        Validate configuration structure and naming conventions.

        Ensures project and environment names follow standards and that
        the configuration structure is properly formatted.
        """
        validate_project_name(self.project_name)
        for env_name in self.data:
            if env_name != DEFAULTS:
                validate_env_name(env_name)

    def _apply_shared(self):
        """
        Process shared values and merge configuration data.

        Applies the shared value inheritance pattern and merges non-sensitive
        and sensitive configuration data into a unified structure.
        """
        self._applied_data = copy.deepcopy(self.data)
        self._applied_secret_data = copy.deepcopy(self.secret_data)
        # Apply shared value pattern ("*" and "env." prefixes)
        apply_inheritance(self._applied_data)
        apply_inheritance(self._applied_secret_data)
        # Merge non-sensitive and sensitive data
        self._merged_data = deep_merge(self._applied_data, self._applied_secret_data)

    def __user_post_init__(self):
        """
        Override this method in subclasses for custom initialization logic.

        Called after configuration processing and before the object is ready.
        """

    def __post_init__(self):
        """
        Internal post-initialization handler.

        Do not override this method. Use __user_post_init__ for custom logic.
        Handles validation, shared value processing, and user initialization.
        """
        self._validate()
        self._apply_shared()
        self.__user_post_init__()

    @cached_property
    def project_name(self) -> str:
        return self.data[DEFAULTS]["*.project_name"]

    @cached_property
    def project_name_slug(self) -> str:
        return slugify(self.project_name, delim="-")

    @cached_property
    def project_name_snake(self) -> str:
        return slugify(self.project_name, delim="_")

    @cached_property
    def parameter_name(self) -> str:
        """
        AWS SSM Parameter Store name for consolidated multi-environment configuration.

        Used for storing the complete configuration containing all environments.
        This is typically accessed by admin tools and deployment scripts.

        Pattern: "${project_name}" (no environment suffix)
        Example: "my_project"

        .. note::

            If you want to use "/path/to/parameter-name" style, you can override
            this property in your environment class to return a different value.
        """
        return normalize_parameter_name(self.project_name_snake)

    def get_env(
        self,
        env_name: T.Union[str, "T_BASE_ENV_NAME_ENUM"],
    ) -> "T_BASE_ENV":
        """
        Get environment-specific configuration as a typed dataclass instance.

        Retrieves and deserializes configuration data for the specified environment,
        applying all shared value inheritance and merging sensitive/non-sensitive data.

        :after_param env_name: Environment name (string) or enum value
        :return: Environment configuration instance with all values resolved
        :raises TypeError: If configuration data doesn't match environment schema
        """
        env_name = self.EnvNameEnumClass.ensure_str(env_name)
        data = copy.deepcopy(self._merged_data[env_name])
        data["env_name"] = env_name
        return self.EnvClass.from_dict(data)

    # --------------------------------------------------------------------------
    # Deployment
    # --------------------------------------------------------------------------
    def _get_all_parameter_data(
        self,
    ) -> tuple[str, dict[str, T.Any]]:
        parameter_name = self.parameter_name
        parameter_data = {
            DATA: self.data,
            SECRET_DATA: self.secret_data,
        }
        return parameter_name, parameter_data

    def _get_env_parameter_data(
        self,
        env_name: T.Union[str, "T_BASE_ENV_NAME_ENUM"],
    ) -> tuple[str, dict[str, T.Any]]:
        if env_name == ALL:
            return self._get_all_parameter_data()
        env_name = self.EnvNameEnumClass.ensure_str(env_name)
        env = self.get_env(env_name)
        parameter_name = env.parameter_name
        parameter_data = {
            DATA: {
                DEFAULTS: {
                    k: v
                    for k, v in self.data.get(DEFAULTS, {}).items()
                    if k.startswith("*") or k.startswith(f"{env_name}.")
                },
                env_name: self.data[env_name],
            },
            SECRET_DATA: {
                DEFAULTS: {
                    k: v
                    for k, v in self.secret_data.get(DEFAULTS, {}).items()
                    if k.startswith("*") or k.startswith(f"{env_name}.")
                },
                env_name: self.secret_data[env_name],
            },
        }
        return parameter_name, parameter_data

    def deploy_env_parameter(
        self,
        ssm_client: "SSMClient",
        s3_client: "S3Client",
        s3dir_config: S3Path,
        env_name: T.Union[str, "T_BASE_ENV_NAME_ENUM"] = ALL,
        description: str | None = OPT,
        type: ParameterType | None = OPT,
        tier: ParameterTier | None = OPT,
        key_id: str | None = OPT,
        allowed_pattern: str | None = OPT,
        tags: dict[str, str] | None = None,
        policies: str | None = OPT,
        data_type: str | None = OPT,
    ) -> DeploymentResult:
        """
        Deploy environment-specific configuration to AWS SSM Parameter Store and S3.

        Deploys configuration for a specific environment or ALL environments to both
        SSM Parameter Store (for runtime access) and S3 (for backup and versioning).
        Only creates S3 files if the SSM parameter actually changes.

        :after_param ssm_client: SSM client for parameter store operations
        :after_param s3_client: S3 client for backup storage
        :after_param s3dir_config: S3 directory for configuration backup
        :after_param env_name: Environment name or ALL for consolidated config
        :after_param tags: Additional AWS resource tags
        :return: DeploymentResult with operation details

        .. note::
            For deploying multiple environments with different AWS clients,
            call this method separately for each environment.
        """
        parameter_name, parameter_data = self._get_env_parameter_data(env_name)
        parameter_value = json.dumps(parameter_data, ensure_ascii=False)
        config_sha256 = sha256_of_text(parameter_value)
        if tags is None:
            tags = {}
        new_tags = {
            AwsTagKeyEnum.project_name.value: self.project_name,
            AwsTagKeyEnum.env_name.value: env_name,
            AwsTagKeyEnum.config_sha256.value: config_sha256,
        }
        tags.update(new_tags)
        tags.update(new_tags)
        before_param, after_param = put_parameter_if_changed(
            ssm_client=ssm_client,
            name=parameter_name,
            value=parameter_value,
            description=description,
            type=type,
            tier=tier,
            key_id=key_id,
            allowed_pattern=allowed_pattern,
            tags=tags,
            policies=policies,
            data_type=data_type,
        )
        # parameter changed
        if after_param is not None:
            s3_parameter = S3Parameter(
                s3dir_config=s3dir_config,
                parameter_name=parameter_name,
            )
            s3path_latest = s3_parameter.write(
                s3_client=s3_client,
                value=parameter_value,
                version=None,
                write_text_kwargs={"tags": tags},
            )
            s3path_versioned = s3_parameter.write(
                s3_client=s3_client,
                value=parameter_value,
                version=after_param.version,
                write_text_kwargs={"tags": tags},
            )
            return DeploymentResult(
                before_param=before_param,
                after_param=after_param,
                s3dir_config=s3dir_config,
                s3path_latest=s3path_latest,
                s3path_versioned=s3path_versioned,
            )
        # parameter not changed
        else:
            return DeploymentResult(
                before_param=before_param,
                after_param=after_param,
                s3dir_config=s3dir_config,
                s3path_latest=None,
                s3path_versioned=None,
            )

    def delete_env_parameter(
        self,
        ssm_client: "SSMClient",
        env_name: T.Union[str, "T_BASE_ENV_NAME_ENUM"] = ALL,
        s3_client: "S3Client" = None,
        include_s3: bool = False,
        s3dir_config: S3Path | None = None,
    ):
        """
        Delete environment configuration from AWS SSM Parameter Store.

        Removes the SSM parameter for the specified environment. Optionally
        deletes S3 backup files if explicitly requested. S3 deletion is
        disabled by default to preserve backup history.

        :after_param ssm_client: SSM client for parameter store operations
        :after_param env_name: Environment name or ALL for consolidated config
        :after_param s3_client: S3 client (required if include_s3=True)
        :after_param include_s3: Whether to also delete S3 backup files
        :after_param s3dir_config: S3 directory (required if include_s3=True)

        .. note::
            SSM parameter deletion removes all versions. S3 serves as backup
            and is preserved by default unless explicitly deleted.
        """
        parameter_name, _ = self._get_env_parameter_data(env_name)
        delete_parameter(
            ssm_client=ssm_client,
            name=parameter_name,
        )
        if include_s3:
            if (s3_client is None) or (s3dir_config is None):
                raise ValueError(
                    "s3_client and s3dir_config must be provided if include_s3 is True.",
                )
            s3_parameter = S3Parameter(
                s3dir_config=s3dir_config,
                parameter_name=parameter_name,
            )
            s3_parameter.delete_all(s3_client=s3_client)
        return DeleteResult(
            parameter_name=parameter_name,
            s3dir_config=s3dir_config,
        )

    @staticmethod
    def load_from_file(
        path_config_json: str | os.PathLike,
        path_secret_config_json: str | os.PathLike,
    ) -> tuple[dict[str, T.Any], dict[str, T.Any]]:
        """
        Load configuration data from local JSON files.

        :param path_config_json: Path to the main configuration JSON file
        :param path_secret_config_json: Path to the sensitive configuration JSON file

        :return: Tuple containing non-sensitive and sensitive configuration data
        """
        data = json_loads(Path(path_config_json).read_text(encoding="utf-8"))
        secret_data = json_loads(
            Path(path_secret_config_json).read_text(encoding="utf-8")
        )
        return data, secret_data

    @staticmethod
    def load_from_s3(
        s3_client: "S3Client",
        s3dir_config: S3Path,
        parameter_name: str | None = None,
        read_text_kwargs: dict[str, T.Any] | None = None,
    ) -> tuple[dict[str, T.Any], dict[str, T.Any]]:
        """
        Load configuration data from local JSON files.

        :param s3_client: S3 client for S3 operations
        :param s3dir_config: S3 directory path where configuration files are stored
        :param parameter_name: Optional specific parameter name to load
        :param read_text_kwargs: Additional keyword arguments for S3 read_text

        :return: Tuple containing non-sensitive and sensitive configuration data
        """
        s3_parameter = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        text = s3_parameter.read(
            s3_client=s3_client,
            version=None,
            read_text_kwargs=read_text_kwargs,
        )
        parameter_data = json_loads(text)
        data = parameter_data[DATA]
        secret_data = parameter_data[SECRET_DATA]
        return data, secret_data

    @staticmethod
    def load_parameter(
        ssm_client: "SSMClient",
        parameter_name: str,
        with_decryption: bool = False,
    ) -> tuple[dict[str, T.Any], dict[str, T.Any]]:
        """
        Load configuration data from an SSM Parameter Store parameter.

        :after_param ssm_client: SSM client for parameter store operations
        :after_param parameter_name: Name of the SSM parameter to load
        :after_param with_decryption: Whether to decrypt secure string parameters

        :return: Tuple containing non-sensitive and sensitive configuration data
        """
        parameter = get_parameter(
            ssm_client=ssm_client,
            name=parameter_name,
            with_decryption=with_decryption,
        )
        if parameter is None:
            raise ValueError(f"SSM Parameter {parameter_name!r} doesn't exist!")
        parameter_data = json_loads(parameter.value)
        data = parameter_data[DATA]
        secret_data = parameter_data[SECRET_DATA]
        return data, secret_data


T_BASE_CONFIG = T.TypeVar("T_BASE_CONFIG", bound=BaseConfig)
