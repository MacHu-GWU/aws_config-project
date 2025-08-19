# -*- coding: utf-8 -*-

import json
from unittest.mock import Mock, patch
from s3pathlib import S3Path
from simple_aws_ssm_parameter_store.api import ParameterType, ParameterTier

from aws_config.s3 import S3Parameter
from aws_config.deployment import Deployment
from aws_config.constants import AwsTagKeyEnum
from aws_config.tests.mock_aws import BaseMockAwsTest


class TestDeployment(BaseMockAwsTest):
    use_mock = True

    @classmethod
    def setup_class_post_hook(cls):
        cls.create_s3_bucket(bucket_name=cls.bucket)

    def test_deployment_initialization(self):
        """Test Deployment class initialization and basic properties.

        The Deployment class encapsulates configuration data and target locations using the Command Pattern.
        This allows users to prepare deployment operations without immediately executing them, simplifying complex deployment workflows.
        Also validates that parameter_value property automatically serializes dictionaries to JSON strings for storage.
        """
        parameter_name = "my_app-dev"
        parameter_data = {"key1": "value1", "key2": {"nested": "value"}}
        project_name = "my_app"
        env_name = "dev"
        s3dir_config = self.s3dir_root.joinpath("config")

        deployment = Deployment(
            parameter_name=parameter_name,
            parameter_data=parameter_data,
            project_name=project_name,
            env_name=env_name,
            s3dir_config=s3dir_config,
        )

        assert deployment.parameter_name == parameter_name
        assert deployment.parameter_data == parameter_data
        assert deployment.project_name == project_name
        assert deployment.env_name == env_name
        assert deployment.s3dir_config == s3dir_config

        expected_json = json.dumps(parameter_data)
        assert deployment.parameter_value == expected_json

    @patch("aws_config.deployment.put_parameter_if_changed")
    def test_deploy_to_ssm_parameter_with_default_tags(self, mock_put_param):
        """Test deploy_to_ssm_parameter with default tags.

        The Command Pattern encapsulates SSM deployment logic with automatic tag management.
        Users can execute deployment commands without manually handling AWS tagging requirements, as the command automatically adds project metadata.
        """
        mock_put_param.return_value = ("before_param", "after_param")

        parameter_data = {"config": "value"}
        deployment = Deployment(
            parameter_name="test-param",
            parameter_data=parameter_data,
            project_name="test_proj",
            env_name="test_env",
            s3dir_config=self.s3dir_root.joinpath("config"),
        )

        ssm_client = Mock()
        result = deployment.deploy_to_ssm_parameter(ssm_client, tags={})

        # Verify result
        assert result == ("before_param", "after_param")

        # Verify call parameters
        mock_put_param.assert_called_once()
        call_args = mock_put_param.call_args

        assert call_args[1]["ssm_client"] == ssm_client
        assert call_args[1]["name"] == "test-param"
        assert call_args[1]["value"] == json.dumps(parameter_data)

        # Check that required tags are present
        tags = call_args[1]["tags"]
        assert AwsTagKeyEnum.project_name.value in tags
        assert AwsTagKeyEnum.env_name.value in tags
        assert AwsTagKeyEnum.config_sha256.value in tags
        assert tags[AwsTagKeyEnum.project_name.value] == "test_proj"
        assert tags[AwsTagKeyEnum.env_name.value] == "test_env"

    @patch("aws_config.deployment.put_parameter_if_changed")
    def test_deploy_to_ssm_parameter_with_custom_tags(self, mock_put_param):
        """Test deploy_to_ssm_parameter with custom tags.

        The Deployment command allows users to provide custom tags while automatically merging required metadata tags.
        This approach simplifies tag management by letting users focus on their specific tagging needs without worrying about system requirements.
        """
        mock_put_param.return_value = ("before_param", "after_param")

        deployment = Deployment(
            parameter_name="test-param",
            parameter_data={"config": "value"},
            project_name="test_proj",
            env_name="test_env",
            s3dir_config=self.s3dir_root.joinpath("config"),
        )

        ssm_client = Mock()
        custom_tags = {"custom": "tag", "another": "value"}

        deployment.deploy_to_ssm_parameter(
            ssm_client,
            description="Test description",
            type=ParameterType.SECURE_STRING,
            tier=ParameterTier.STANDARD,
            key_id="test-key",
            overwrite=True,
            allowed_pattern=".*",
            tags=custom_tags,
            policies="test-policy",
            data_type="text",
        )

        # Verify all parameters were passed
        call_args = mock_put_param.call_args
        assert call_args[1]["description"] == "Test description"
        assert call_args[1]["type"] == ParameterType.SECURE_STRING
        assert call_args[1]["tier"] == ParameterTier.STANDARD
        assert call_args[1]["key_id"] == "test-key"
        assert call_args[1]["overwrite"] == True
        assert call_args[1]["allowed_pattern"] == ".*"
        assert call_args[1]["policies"] == "test-policy"
        assert call_args[1]["data_type"] == "text"

        # Check that custom tags are preserved and required tags are added
        tags = call_args[1]["tags"]
        assert tags["custom"] == "tag"
        assert tags["another"] == "value"
        assert AwsTagKeyEnum.project_name.value in tags
        assert AwsTagKeyEnum.env_name.value in tags
        assert AwsTagKeyEnum.config_sha256.value in tags

    @patch("aws_config.deployment.put_parameter_if_changed")
    def test_deploy_to_ssm_parameter_with_none_tags(self, mock_put_param):
        """Test deploy_to_ssm_parameter with tags=None.

        The Command Pattern handles edge cases gracefully, such as when users provide None for optional parameters.
        The deployment command automatically initializes empty tag dictionaries and adds required metadata, ensuring consistent behavior regardless of user input.
        """
        mock_put_param.return_value = ("before_param", "after_param")

        deployment = Deployment(
            parameter_name="test-param-none",
            parameter_data={"config": "value"},
            project_name="test_proj",
            env_name="test_env",
            s3dir_config=self.s3dir_root.joinpath("config"),
        )

        ssm_client = Mock()
        result = deployment.deploy_to_ssm_parameter(ssm_client, tags=None)

        # Verify result
        assert result == ("before_param", "after_param")

        # Verify call parameters
        call_args = mock_put_param.call_args

        # Check that required tags are present (should be {} since tags=None -> tags = {})
        tags = call_args[1]["tags"]
        assert AwsTagKeyEnum.project_name.value in tags
        assert AwsTagKeyEnum.env_name.value in tags
        assert AwsTagKeyEnum.config_sha256.value in tags

    def test_deploy_to_s3(self):
        """Test deploy_to_s3 method.

        The Deployment command encapsulates complex S3 operations including both latest and versioned file creation.
        Users can execute S3 deployments with a single command call, while the pattern handles the underlying complexity of managing multiple file versions.
        """
        parameter_data = {"config": "value", "nested": {"key": "val"}}
        deployment = Deployment(
            parameter_name="test-s3-param",
            parameter_data=parameter_data,
            project_name="test_proj",
            env_name="test_env",
            s3dir_config=self.s3dir_root.joinpath("config"),
        )

        result = deployment.deploy_to_s3(
            s3_client=self.bsm, version=1, tags={"custom": "tag"}
        )

        # Result should be tuple of two S3Path objects
        assert isinstance(result, tuple)
        assert len(result) == 2
        s3path_latest, s3path_versioned = result

        assert isinstance(s3path_latest, S3Path)
        assert isinstance(s3path_versioned, S3Path)

        # Check that files were actually created
        assert s3path_latest.exists(bsm=self.bsm)
        assert s3path_versioned.exists(bsm=self.bsm)

        # Verify content
        latest_content = s3path_latest.read_text(bsm=self.bsm)
        versioned_content = s3path_versioned.read_text(bsm=self.bsm)
        expected_content = json.dumps(parameter_data)

        assert latest_content == expected_content
        assert versioned_content == expected_content

    def test_deploy_to_s3_with_default_tags(self):
        """Test deploy_to_s3 with default tags (None).

        The Command Pattern provides consistent behavior for S3 deployments even when users don't specify custom tags.
        The deployment command automatically handles None tag values and ensures proper project metadata is always included.
        """
        deployment = Deployment(
            parameter_name="test-s3-default",
            parameter_data={"test": "value"},
            project_name="test_proj",
            env_name="test_env",
            s3dir_config=self.s3dir_root.joinpath("config-default"),
        )

        # Call with tags=None (default)
        result = deployment.deploy_to_s3(s3_client=self.bsm, version=1, tags=None)

        assert isinstance(result, tuple)
        assert len(result) == 2

    @patch("aws_config.deployment.delete_parameter")
    def test_delete_from_ssm_parameter(self, mock_delete_param):
        """Test delete_from_ssm_parameter method.

        The Deployment command provides a unified interface for cleanup operations across different AWS services.
        Users can execute deletion commands without knowing the specific AWS API details, as the command pattern abstracts the underlying implementation.
        """
        mock_delete_param.return_value = True

        deployment = Deployment(
            parameter_name="test-param-delete",
            parameter_data={"test": "data"},
            project_name="test_proj",
            env_name="test_env",
            s3dir_config=self.s3dir_root.joinpath("config"),
        )

        ssm_client = Mock()
        result = deployment.delete_from_ssm_parameter(ssm_client)

        assert result == True
        mock_delete_param.assert_called_once_with(
            ssm_client=ssm_client, name="test-param-delete"
        )

    def test_delete_from_s3_with_version(self):
        """Test delete_from_s3 method with specific version.

        The Command Pattern allows users to execute targeted deletion operations by specifying version parameters.
        This approach simplifies S3 version management by providing a clean interface that handles the complexity of selective file deletion.
        """
        deployment = Deployment(
            parameter_name="test-s3-delete",
            parameter_data={"test": "data"},
            project_name="test_proj",
            env_name="test_env",
            s3dir_config=self.s3dir_root.joinpath("config-delete"),
        )

        # First, create some files to delete
        deployment.deploy_to_s3(s3_client=self.bsm, version=1)

        # Verify files exist
        s3_parameter = S3Parameter(
            s3dir_config=deployment.s3dir_config,
            parameter_name=deployment.parameter_name,
        )
        s3path_v1 = s3_parameter.get_s3path(version=1)
        s3path_latest = s3_parameter.get_s3path(version=None)

        assert s3path_v1.exists(bsm=self.bsm)
        assert s3path_latest.exists(bsm=self.bsm)

        # Delete specific version
        deployment.delete_from_s3(s3_client=self.bsm, version=1)

        # Verify specific version is deleted
        assert not s3path_v1.exists(bsm=self.bsm)
        # Latest should still exist
        assert s3path_latest.exists(bsm=self.bsm)

    def test_delete_from_s3_with_default_version(self):
        """Test delete_from_s3 method with default version (None).

        The Deployment command provides intuitive defaults for deletion operations when users don't specify version parameters.
        This Command Pattern implementation allows users to delete latest files with minimal code complexity.
        """
        deployment = Deployment(
            parameter_name="test-s3-delete-default",
            parameter_data={"test": "data"},
            project_name="test_proj",
            env_name="test_env",
            s3dir_config=self.s3dir_root.joinpath("config-delete-default"),
        )

        # First, create some files to delete
        deployment.deploy_to_s3(s3_client=self.bsm, version=1)

        # Delete with default version (None)
        deployment.delete_from_s3(s3_client=self.bsm, version=None)

        # Verify latest file is deleted
        s3_parameter = S3Parameter(
            s3dir_config=deployment.s3dir_config,
            parameter_name=deployment.parameter_name,
        )
        s3path_latest = s3_parameter.get_s3path(version=None)
        assert not s3path_latest.exists(bsm=self.bsm)

    def test_comprehensive_workflow(self):
        """Test a complete workflow including deployment and cleanup.

        The Command Pattern enables users to chain deployment and cleanup operations in a simple, readable workflow.
        This test demonstrates how the Deployment class simplifies complex multi-step operations into clean, understandable command sequences.
        """
        parameter_data = {
            "database_url": "postgresql://localhost:5432/myapp",
            "debug": True,
            "features": {"feature_a": True, "feature_b": False},
            "timeout": 30,
        }

        deployment = Deployment(
            parameter_name="myapp-test-config",
            parameter_data=parameter_data,
            project_name="myapp",
            env_name="test",
            s3dir_config=self.s3dir_root.joinpath("workflow-test"),
        )

        # Test S3 deployment
        s3_result = deployment.deploy_to_s3(
            s3_client=self.bsm,
            version=1,
            tags={"environment": "test", "team": "backend"},
        )

        s3path_latest, s3path_versioned = s3_result

        # Verify deployment results
        assert s3path_latest.exists(bsm=self.bsm)
        assert s3path_versioned.exists(bsm=self.bsm)

        # Verify content matches
        deployed_content = json.loads(s3path_latest.read_text(bsm=self.bsm))
        assert deployed_content == parameter_data

        # Test cleanup
        deployment.delete_from_s3(s3_client=self.bsm, version=1)
        deployment.delete_from_s3(s3_client=self.bsm, version=None)

        # Verify cleanup
        assert not s3path_versioned.exists(bsm=self.bsm)
        assert not s3path_latest.exists(bsm=self.bsm)


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.deployment",
        preview=False,
    )
