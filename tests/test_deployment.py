# -*- coding: utf-8 -*-

from aws_config.deployment import Deployment

from aws_config.tests.mock_aws import BaseMockAwsTest


class TestDeployment(BaseMockAwsTest):
    use_mock = True

    def test(self):
        parameter_name = "my_app-dev"
        project_name = "my_app"
        env_name = "dev"
        s3dir_config = self.s3dir_root.joinpath("config")

        deployment = Deployment(
            parameter_name=parameter_name,
            parameter_data=...,
            project_name=project_name,
            env_name=env_name,
            s3dir_config=s3dir_config,
        )


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.deployment",
        preview=False,
    )
