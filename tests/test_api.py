# -*- coding: utf-8 -*-

from aws_config import api


def test():
    _ = api
    _ = api.json_loads
    _ = api.slugify
    _ = api.under2camel
    _ = api.camel2under
    _ = api.ZFILL
    _ = api.LATEST_VERSION
    _ = api.S3MetadataKeyEnum
    _ = api.AwsTagKeyEnum
    _ = api.ALL
    _ = api.DATA
    _ = api.SECRET_DATA
    _ = api.ALL
    _ = api.EnvVarNameEnum
    _ = api.S3Parameter
    _ = api.validate_project_name
    _ = api.normalize_parameter_name
    _ = api.BaseEnv
    _ = api.T_BASE_ENV
    _ = api.BaseEnvNameEnum
    _ = api.T_BASE_ENV_NAME_ENUM
    _ = api.BaseConfig
    _ = api.T_BASE_CONFIG


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.api",
        preview=False,
    )
