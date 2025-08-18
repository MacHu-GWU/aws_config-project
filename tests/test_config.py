# -*- coding: utf-8 -*-

from aws_config.config import BaseConfig, BaseEnvNameEnum

import pytest
from pydantic import Field, ValidationError
from configcraft.api import SHARED
from aws_config.env import BaseEnv
from aws_config.paths import dir_tmp

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


class TestConfig:
    def test_happy_path(self):
        config = Config(
            data={
                SHARED: {
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
            },
            secret_data={
                EnvNameEnum.dev: {
                    "aws_account_id": "111111111111",
                    "password": "alicepassword",
                },
                EnvNameEnum.prod: {
                    "aws_account_id": "111111111111",
                    "password": "bobpassword",
                },
            },
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
                SHARED: {
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


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.config",
        preview=False,
    )
