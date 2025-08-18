# -*- coding: utf-8 -*-

try:
    import typing_extensions as T
except ImportError:  # pragma: no cover
    import typing as T
import dataclasses
from functools import cached_property

import copy
from which_env.api import validate_env_name, BaseEnvNameEnum
from configcraft.api import SHARED, apply_inheritance, deep_merge
from .vendor.strutils import slugify

from .env import validate_project_name, normalize_parameter_name, BaseEnv, T_BASE_ENV


T_BASE_ENV_NAME_ENUM = T.TypeVar(
    "T_BASE_ENV_NAME_ENUM",
    bound=BaseEnvNameEnum,
)


@dataclasses.dataclass
class BaseConfig(
    T.Generic[T_BASE_ENV, T_BASE_ENV_NAME_ENUM],
):
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
            if env_name != SHARED:
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
        return self.data["_shared"]["*.project_name"]

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

        :param env_name: Environment name (string) or enum value
        :return: Environment configuration instance with all values resolved
        :raises TypeError: If configuration data doesn't match environment schema
        """
        env_name = self.EnvNameEnumClass.ensure_str(env_name)
        data = copy.deepcopy(self._merged_data[env_name])
        data["env_name"] = env_name
        return self.EnvClass.from_dict(data)
