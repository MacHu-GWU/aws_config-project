# -*- coding: utf-8 -*-

from aws_config.s3 import (
    S3Parameter,
    deploy_config,
    read_config,
    delete_config,
)

import pytest
from s3pathlib import S3Path
from aws_config.tests.mock_aws import BaseMockAwsTest
from aws_config.exc import S3BucketVersionSuspendedError, S3ObjectNotExist


class TestS3Parameter(BaseMockAwsTest):
    use_mock = True
    test_bucket = "s3-test-bucket"
    test_versioned_bucket = "s3-test-versioned-bucket"

    @classmethod
    def setup_class_post_hook(cls):
        cls.create_s3_bucket(bucket_name=cls.test_bucket)
        cls.create_s3_bucket(
            bucket_name=cls.test_versioned_bucket,
            enable_versioning=True,
        )
        cls.s3bucket_test_bucket = S3Path(f"s3://{cls.test_bucket}")
        cls.s3bucket_test_versioned_bucket = S3Path(f"s3://{cls.test_versioned_bucket}")

    def _test(
        self,
        versioned: bool,
    ):
        if versioned:
            s3dir_root = self.s3bucket_test_versioned_bucket
        else:
            s3dir_root = self.s3bucket_test_bucket

        parameter_name = "myapp"
        s3dir_config = s3dir_root.joinpath("config")
        tags = {"creator": "Alice"}
        
        # Test S3Parameter creation and initial state
        s3_param = S3Parameter.new(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
        )
        
        # Test version status methods
        assert s3_param.version_status.is_enabled() == versioned
        assert s3_param.version_status.is_not_enabled() == (not versioned)
        assert s3_param.version_status.is_suspended() == False
        
        # Test reading from empty bucket
        with pytest.raises(S3ObjectNotExist):
            _ = s3_param.read_latest(bsm=self.bsm)

        with pytest.raises(S3ObjectNotExist):
            _ = read_config(
                bsm=self.bsm,
                s3folder_config=s3dir_config.uri,
                parameter_name=parameter_name,
            )

        # Test version retrieval methods for empty bucket
        if versioned:
            latest_version = s3_param.get_latest_config_version_when_version_is_enabled(bsm=self.bsm)
            assert latest_version is None
        else:
            latest_version = s3_param.get_latest_config_version_when_version_not_enabled(bsm=self.bsm)
            assert latest_version is None

        # First deployment
        s3object1 = deploy_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            config_data={"version": 1},
            tags=tags,
        )
        
        # Test S3Object properties
        assert s3object1 is not None
        assert s3object1.bucket == s3dir_root.bucket
        assert s3object1.key is not None
        assert s3object1.etag is not None
        # Test other properties that might be None
        _ = s3object1.expiration
        _ = s3object1.checksum_crc32
        _ = s3object1.checksum_crc32c
        _ = s3object1.checksum_sha1
        _ = s3object1.checksum_sha256
        _ = s3object1.server_side_encryption
        _ = s3object1.version_id
        _ = s3object1.sse_customer_algorithm
        _ = s3object1.sse_customer_key_md5
        _ = s3object1.see_kms_key_id
        _ = s3object1.sse_kms_encryption_context
        _ = s3object1.bucket_key_enabled
        _ = s3object1.request_charged
        
        # Verify first deployment
        config_data, config_version = s3_param.read_latest(bsm=self.bsm)
        assert config_data == {"version": 1}
        if versioned:
            assert config_version is not None  # Version ID from S3
        else:
            assert config_version == "1"

        config_data, config_version = read_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
        )
        assert config_data == {"version": 1}
        if versioned:
            assert config_version is not None
        else:
            assert config_version == "1"

        # Test deploying same config (should do nothing)
        s3object_same = deploy_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            config_data={"version": 1},  # Same data
            tags=tags,
        )
        assert s3object_same is None  # No deployment should happen

        # Second deployment with different data
        s3object2 = deploy_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            config_data={"version": 2},
            tags=tags,
        )
        assert s3object2 is not None
        
        # Verify second deployment
        config_data, config_version = s3_param.read_latest(bsm=self.bsm)
        assert config_data == {"version": 2}
        if versioned:
            assert config_version is not None
        else:
            assert config_version == "2"

        config_data, config_version = read_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
        )
        assert config_data == {"version": 2}
        if versioned:
            assert config_version is not None
        else:
            assert config_version == "2"

        # Test version retrieval methods after deployments
        if versioned:
            latest_version = s3_param.get_latest_config_version_when_version_is_enabled(bsm=self.bsm)
            assert latest_version is not None
        else:
            latest_version = s3_param.get_latest_config_version_when_version_not_enabled(bsm=self.bsm)
            assert latest_version == 2

        # Test delete latest only
        result = delete_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            include_history=False,
        )
        assert result == True
        
        # Verify deletion
        with pytest.raises(S3ObjectNotExist):
            _ = s3_param.read_latest(bsm=self.bsm)
        with pytest.raises(S3ObjectNotExist):
            _ = read_config(
                bsm=self.bsm,
                s3folder_config=s3dir_config.uri,
                parameter_name=parameter_name,
            )

        # Deploy again for history deletion test
        deploy_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            config_data={"version": 3},
            tags=tags,
        )
        
        # Test delete with history
        result = delete_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            include_history=True,
        )
        assert result == True
        
        # Verify complete deletion
        with pytest.raises(S3ObjectNotExist):
            _ = s3_param.read_latest(bsm=self.bsm)
        with pytest.raises(S3ObjectNotExist):
            _ = read_config(
                bsm=self.bsm,
                s3folder_config=s3dir_config.uri,
                parameter_name=parameter_name,
            )

    def test_suspended_bucket(
        self,
        disable_logger,
    ):
        """Test behavior when bucket versioning is suspended"""
        # Create a bucket and suspend versioning
        suspended_bucket = "s3-test-suspended-bucket"
        self.create_s3_bucket(bucket_name=suspended_bucket, enable_versioning=True)
        
        # Suspend versioning
        self.bsm.s3_client.put_bucket_versioning(
            Bucket=suspended_bucket,
            VersioningConfiguration={"Status": "Suspended"},
        )
        
        parameter_name = "myapp"
        s3dir_config = S3Path(f"s3://{suspended_bucket}").joinpath("config")
        
        # This should raise an error for suspended bucket
        with pytest.raises(S3BucketVersionSuspendedError):
            S3Parameter.new(
                bsm=self.bsm,
                s3folder_config=s3dir_config.uri,
                parameter_name=parameter_name,
            )

    def test_edge_cases(
        self,
        disable_logger,
    ):
        """Test edge cases and error scenarios"""
        parameter_name = "myapp-edge"
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-edge")
        
        s3_param = S3Parameter.new(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
        )
        
        # Deploy a config first
        deploy_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            config_data={"test": "data"},
        )
        
        # Delete the latest file manually to test get_latest_config_version_when_version_not_enabled
        # when latest doesn't exist but versioned files do
        s3_param.s3path_latest.delete(bsm=self.bsm)
        
        # This should find the versioned file and return its version
        latest_version = s3_param.get_latest_config_version_when_version_not_enabled(bsm=self.bsm)
        assert latest_version == 1
        
        # Clean up for next test
        s3_param.s3path_latest.parent.delete(bsm=self.bsm)

    def test_versioned_bucket_delete_marker(
        self,
        disable_logger,
    ):
        """Test versioned bucket behavior with delete markers"""
        parameter_name = "myapp-versioned"
        s3dir_config = self.s3bucket_test_versioned_bucket.joinpath("config-versioned")
        
        s3_param = S3Parameter.new(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
        )
        
        # Deploy first version
        deploy_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            config_data={"version": 1},
        )
        
        # Deploy second version
        deploy_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            config_data={"version": 2},
        )
        
        # Delete latest (this will create a delete marker)
        delete_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            include_history=False,
        )
        
        # Test get_latest_config_version_when_version_is_enabled with delete marker
        latest_version = s3_param.get_latest_config_version_when_version_is_enabled(bsm=self.bsm)
        assert latest_version is not None  # Should find the version before delete marker
        
        # Clean up completely
        delete_config(
            bsm=self.bsm,
            s3folder_config=s3dir_config.uri,
            parameter_name=parameter_name,
            include_history=True,
        )

    def test(
        self,
        disable_logger,
    ):
        self._test(versioned=False)
        self._test(versioned=True)


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.s3",
        preview=False,
    )
