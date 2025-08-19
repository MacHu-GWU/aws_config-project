# -*- coding: utf-8 -*-

from aws_config.env import (
    validate_project_name,
    normalize_parameter_name,
)

import pytest
from aws_config.tests.sample_config import (
    Server,
    Servers,
    Database,
    Env,
    data,
)


def test_validate_project_name():
    good_cases = [
        "my_project",
        "my-project",
        "my_1_project",
        "my1project",
        "myproject1",
    ]
    bad_cases = [
        "my project",
        "1-my-project",
        "-my-project",
        "my-project-",
    ]
    for project_name in good_cases:
        validate_project_name(project_name)
    for project_name in bad_cases:
        with pytest.raises(ValueError):
            validate_project_name(project_name)


def test_normalize_parameter_name():
    # Test AWS prefix normalization - parameters starting with "aws" need "p-" prefix
    assert normalize_parameter_name("aws") == "p-aws"
    assert normalize_parameter_name("aws-project") == "p-aws-project"

    # Test SSM prefix normalization - parameters starting with "ssm" need "p-" prefix
    assert normalize_parameter_name("ssm") == "p-ssm"
    assert normalize_parameter_name("ssm-project") == "p-ssm-project"

    # Test normal parameters - no prefix needed
    assert normalize_parameter_name("normal-after_param") == "normal-after_param"
    assert normalize_parameter_name("my-project") == "my-project"


class TestBaseEnv:
    def test(self):
        env = Env.from_dict(data)
        print(env)
        assert env.project_name == "my_app"
        assert env.env_name == "dev"
        assert isinstance(env.servers, Servers)
        assert isinstance(env.servers.blue, Server)
        assert env.servers.black is None
        assert isinstance(env.databases[0], Database)

        assert env.project_name_snake == "my_app"
        assert env.project_name_slug == "my-app"
        assert env.prefix_name_snake == "my_app-dev"
        assert env.prefix_name_slug == "my-app-dev"
        assert env.parameter_name == "my_app-dev"

        _ = env.s3dir_env_data
        _ = env.s3dir_env_artifacts
        _ = env.s3dir_tmp_artifacts
        _ = env.s3dir_config_artifacts
        _ = env.env_vars
        _ = env.devops_aws_tags
        _ = env.workload_aws_tags
        _ = env.cloudformation_stack_name

        env1 = Env.from_dict(env.to_dict())
        assert env == env1


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.env",
        preview=False,
    )
