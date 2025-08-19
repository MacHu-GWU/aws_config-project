# -*- coding: utf-8 -*-

from aws_config.config import BaseConfig, BaseEnvNameEnum

import pytest
from pydantic import Field, ValidationError
from configcraft.api import DEFAULTS
from simple_aws_ssm_parameter_store.api import ParameterType
from aws_config.env import BaseEnv
from aws_config.paths import dir_tmp
from aws_config.tests.mock_aws import BaseMockAwsTest

dir_tmp.mkdir(parents=True, exist_ok=True)


class EnvNameEnum(BaseEnvNameEnum):
    dev = "dev"
    prod = "prod"


class Env(BaseEnv):
    username: str = Field()
    password: str = Field()


class Config(
    BaseConfig[Env, EnvNameEnum],
):
    pass


sample_data = {
    DEFAULTS: {
        "*.project_name": "my_app",
        "*.aws_region": "us-east-1",
    },
    EnvNameEnum.dev: {
        "s3uri_data": "s3://myapp-dev/data/",
        "s3uri_artifacts": "s3://myapp-dev/artifacts/",
        "username": "alice",
    },
    EnvNameEnum.prod: {
        "s3uri_data": "s3://myapp-prod/data/",
        "s3uri_artifacts": "s3://myapp-prod/artifacts/",
        "username": "bob",
    },
}
sample_secret_data = {
    EnvNameEnum.dev: {
        "aws_account_id": "111111111111",
        "password": "alicepassword",
    },
    EnvNameEnum.prod: {
        "aws_account_id": "111111111111",
        "password": "bobpassword",
    },
}


class TestConfig(BaseMockAwsTest):
    def test_happy_path(self):
        config = Config(
            data=sample_data,
            secret_data=sample_secret_data,
            EnvClass=Env,
            EnvNameEnumClass=EnvNameEnum,
            version="0.1.1",
        )
        env = config.get_env(EnvNameEnum.dev)
        assert env.env_name == "dev"
        assert env.username == "alice"
        assert env.password == "alicepassword"

        assert config.project_name_snake == "my_app"
        assert config.project_name_slug == "my-app"
        assert config.parameter_name == "my_app"

    def test_validation_error(self):
        config = Config(
            data={
                DEFAULTS: {
                    "*.project_name": "my_app",
                },
                EnvNameEnum.dev: {
                    "username": "alice",
                },
                EnvNameEnum.prod: {
                    "username": "bob",
                },
            },
            secret_data={
                EnvNameEnum.dev: {
                    "password": 123456,
                },
                EnvNameEnum.prod: {
                    "password": 654321,
                },
            },
            EnvClass=Env,
            EnvNameEnumClass=EnvNameEnum,
            version="0.1.1",
        )
        with pytest.raises(ValidationError):
            env = config.get_env(EnvNameEnum.dev)

    def test_deploy_env_parameter(self):
        config = Config(
            data=sample_data,
            secret_data=sample_secret_data,
            EnvClass=Env,
            EnvNameEnumClass=EnvNameEnum,
            version="0.1.1",
        )
        ssm_client = self.bsm.ssm_client
        s3_client = self.bsm.s3_client
        s3dir_config = self.s3dir_root.joinpath("config").to_dir()
        
        # First deployment - should create new parameter and S3 files
        result = config.deploy_env_parameter(
            ssm_client=ssm_client,
            s3_client=s3_client,
            s3dir_config=s3dir_config,
            env_name=EnvNameEnum.dev,
            type=ParameterType.SECURE_STRING,
        )
        assert result.is_ssm_deployed is True
        assert result.is_s3_deployed is True

        # Second deployment with same data - should skip (no changes)
        result = config.deploy_env_parameter(
            ssm_client=ssm_client,
            s3_client=s3_client,
            s3dir_config=s3dir_config,
            env_name=EnvNameEnum.dev,
            type=ParameterType.SECURE_STRING,
        )
        assert result.is_ssm_deployed is False
        assert result.is_s3_deployed is False

        # Deploy ALL environments - should create consolidated parameter
        result = config.deploy_env_parameter(
            ssm_client=ssm_client,
            s3_client=s3_client,
            s3dir_config=s3dir_config,
            type=ParameterType.SECURE_STRING,
        )
        assert result.is_ssm_deployed is True
        assert result.is_s3_deployed is True

        # Re-deploy ALL with same data - should skip (no changes)
        result = config.deploy_env_parameter(
            ssm_client=ssm_client,
            s3_client=s3_client,
            s3dir_config=s3dir_config,
            type=ParameterType.SECURE_STRING,
        )
        assert result.is_ssm_deployed is False
        assert result.is_s3_deployed is False


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.config",
        preview=False,
    )
