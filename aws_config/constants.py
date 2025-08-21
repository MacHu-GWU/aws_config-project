# -*- coding: utf-8 -*-

from enum_mate.api import BetterStrEnum
from which_env.api import USER_ENV_NAME, ENV_NAME
from which_runtime.api import USER_RUNTIME_NAME

ZFILL = 6
"""
Can support up to 999999 versions.
"""

LATEST_VERSION = "LATEST"


class S3MetadataKeyEnum(BetterStrEnum):
    """
    Enumerate the keys of AWS tags used in the project.
    """

    CONFIG_VERSION = "config_version"
    CONFIG_SHA256 = "config_sha256"


class AwsTagKeyEnum(BetterStrEnum):
    """
    Common AWS resource tag name enumeration.

    :after_param tech_project_name: project name for tech
    :after_param tech_env_name: sbx, prd, etc ...
    :after_param tech_version: software semantic version, 0.1.2
    :after_param tech_description: short description
    :after_param tech_human_creator: usually a name or email
    :after_param tech_machine_creator: usually a machine identifier
    :after_param auto_active_time: cron expression that to keep the resource active
    :after_param auto_delete_at: datetime that to delete the resource
    :after_param bus_ou: business organization unit
    :after_param bus_team: the team in your business organization
    :after_param bus_project_name: project name for business
    :after_param bus_owner: the owner of the resource, usually an email
    :after_param bus_user: who is using the resource, usually an email or a team name
    :after_param sec_confidentiality: confidential level, public, secret, etc ...
    :after_param sec_compliance: HIPAA, PCI, etc ...
    """

    tech_project_name = "tech:project_name"
    tech_env_name = "tech:env_name"
    tech_version = "tech:version"
    tech_description = "tech:description"
    tech_human_creator = "tech:human_creator"
    tech_machine_creator = "tech:machine_creator"
    auto_active_time = "auto:active_time"
    auto_delete_at = "auto:delete_at"
    bus_ou = "bus:ou"
    bus_team = "bus:team"
    bus_project_name = "bus:project_name"
    bus_owner = "bus:owner"
    bus_user = "bus:user"
    sec_confidentiality = "sec:confidentiality"
    sec_compliance = "sec:compliance"

    project_name = "config:project_name"
    env_name = "config:env_name"
    config_sha256 = "config:config_sha256"


ALL = "all"
"""
Special environment name to indicate all environment configs.
"""

DATA = "data"
"""
dictionary key for config data
"""

SECRET_DATA = "secret_data"
"""
dictionary key for config secret data
"""

class EnvVarNameEnum(BetterStrEnum):
    """
    Common environment variable name enumeration.

    :after_param USER_ENV_NAME: store the current environment name, e.g.
        "devops", "sbx", "tst", "stg", "prd", etc. this environment variable
        has higher priority than the "ENV_NAME"
    :after_param USER_RUNTIME_NAME: store the name of the current runtime,
        usually you should not use this environment variable directly, instead
        let the ``runtime`` module to detect that automatically. This var
        is useful when you want to override the runtime name for testing.
    :after_param USER_GIT_BRANCH_NAME: store the name of the current git branch,
        usually you should not use this environment variable directly, instead
        let the ``git`` module to detect that automatically. This var
        is useful when you want to override the git branch name for testing.
    :after_param USER_GIT_COMMIT_ID: store the name of the current git commit id,
        usually you should not use this environment variable directly, instead
        let the ``git`` module to detect that automatically. This var
        is useful when you want to override the git branch name for testing.
    :after_param USER_GIT_COMMIT_MESSAGE: store the name of the current git commit message,
        usually you should not use this environment variable directly, instead
        let the ``git`` module to detect that automatically. This var
        is useful when you want to override the git branch name for testing.
    :after_param ENV_NAME: store the current environment name. if the USER_ENV_NAME is set,
        use USER_ENV_NAME, otherwise, use this one.
    :after_param PROJECT_NAME: store the name of the current project,
        the project name is part of the AWS resource naming convention
    :after_param PARAMETER_NAME: store the name of AWS parameter for the configuration
        this environment variable is used in application runtime to get the
        configuration data from AWS parameter store
    """

    USER_ENV_NAME = USER_ENV_NAME
    USER_RUNTIME_NAME = USER_RUNTIME_NAME
    USER_GIT_BRANCH_NAME = "USER_GIT_BRANCH_NAME"
    USER_GIT_COMMIT_ID = "USER_GIT_COMMIT_ID"
    USER_GIT_COMMIT_MESSAGE = "USER_GIT_COMMIT_MESSAGE"
    ENV_NAME = ENV_NAME
    PROJECT_NAME = "PROJECT_NAME"
    PARAMETER_NAME = "PARAMETER_NAME"
