# -*- coding: utf-8 -*-

from aws_config.s3 import (
    S3Parameter,
    read_text,
)

import pytest
from s3pathlib import S3Path
from aws_config.tests.mock_aws import BaseMockAwsTest
from aws_config.exc import S3ObjectNotExist


class TestS3Parameter(BaseMockAwsTest):
    use_mock = True
    test_bucket = "s3-test-bucket"

    @classmethod
    def setup_class_post_hook(cls):
        # Create bucket without versioning (versioning disabled)
        cls.create_s3_bucket(bucket_name=cls.test_bucket)
        cls.s3bucket_test_bucket = S3Path(f"s3://{cls.test_bucket}")

    def test_read_text_function(self):
        """Test the read_text helper function"""
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-read-text-func")
        parameter_name = "test-read-text"
        
        s3_param = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        
        # Test reading non-existent file - use a different path
        s3path_nonexistent = s3_param.get_s3path(999)  # Use a version that doesn't exist
        with pytest.raises(S3ObjectNotExist):
            read_text(s3path=s3path_nonexistent, bsm=self.bsm)
        
        # Write content and test reading
        test_content = '{"test": "data"}'
        s3path = s3_param.get_s3path(None)
        s3path.write_text(test_content, bsm=self.bsm)
        
        result = read_text(s3path=s3path, bsm=self.bsm)
        assert result == test_content

    def test_s3_parameter_basic_operations(self):
        """Test basic S3Parameter operations with non-versioned bucket"""
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-basic-ops")
        parameter_name = "myapp"
        
        # Create S3Parameter instance
        s3_param = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        
        # Test s3dir_param property
        expected_param_dir = s3dir_config.joinpath(parameter_name).to_dir()
        assert s3_param.s3dir_param.uri == expected_param_dir.uri
        
        # Test get_s3path method for latest
        s3path_latest = s3_param.get_s3path(None)
        expected_latest = s3_param.s3dir_param.joinpath(f"{parameter_name}-000000-LATEST.json")
        assert s3path_latest.uri == expected_latest.uri
        
        # Test get_s3path method for specific version
        s3path_v1 = s3_param.get_s3path(1)
        expected_v1 = s3_param.s3dir_param.joinpath(f"{parameter_name}-999999-1.json")
        assert s3path_v1.uri == expected_v1.uri
        
        # Test get_s3path method for version 2 (should have different pattern)
        s3path_v2 = s3_param.get_s3path(2)
        expected_v2 = s3_param.s3dir_param.joinpath(f"{parameter_name}-999998-2.json")
        assert s3path_v2.uri == expected_v2.uri

    def test_s3_parameter_write_and_read(self):
        """Test writing and reading configuration data"""
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-write-read")
        parameter_name = "myapp-wr"
        
        s3_param = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        
        # Test reading from empty location
        with pytest.raises(S3ObjectNotExist):
            s3_param.read(bsm=self.bsm, version=None)
        
        with pytest.raises(S3ObjectNotExist):
            s3_param.read(bsm=self.bsm, version=1)
        
        # Write first version
        config_data_1 = '{"version": 1, "data": "test"}'
        s3path_result = s3_param.write(
            bsm=self.bsm,
            value=config_data_1,
            version=1,
        )
        assert s3path_result is not None
        assert s3path_result.bucket == self.test_bucket
        assert "999999-1.json" in s3path_result.key
        
        # Read back the versioned data
        result = s3_param.read(bsm=self.bsm, version=1)
        assert result == config_data_1
        
        # Write latest pointer
        s3path_latest = s3_param.write(
            bsm=self.bsm,
            value=config_data_1,
            version=None,
        )
        assert s3path_latest is not None
        assert "000000-LATEST.json" in s3path_latest.key
        
        # Read latest
        result_latest = s3_param.read(bsm=self.bsm, version=None)
        assert result_latest == config_data_1
        
        # Write second version
        config_data_2 = '{"version": 2, "data": "updated"}'
        s3_param.write(
            bsm=self.bsm,
            value=config_data_2,
            version=2,
        )
        
        # Update latest pointer
        s3_param.write(
            bsm=self.bsm,
            value=config_data_2,
            version=None,
        )
        
        # Read specific versions
        result_v1 = s3_param.read(bsm=self.bsm, version=1)
        assert result_v1 == config_data_1
        
        result_v2 = s3_param.read(bsm=self.bsm, version=2)
        assert result_v2 == config_data_2
        
        # Read latest (should be version 2)
        result_latest = s3_param.read(bsm=self.bsm, version=None)
        assert result_latest == config_data_2

    def test_s3_parameter_with_write_text_kwargs(self):
        """Test S3Parameter write with additional kwargs"""
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-kwargs")
        parameter_name = "myapp-kwargs"
        
        s3_param = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        
        config_data = '{"test": "with_kwargs"}'
        write_kwargs = {
            "content_type": "application/json",
            "cache_control": "no-cache",
        }
        
        # Write with additional kwargs
        s3path_result = s3_param.write(
            bsm=self.bsm,
            value=config_data,
            version=1,
            write_text_kwargs=write_kwargs,
        )
        
        assert s3path_result is not None
        
        # Verify we can read it back
        result = s3_param.read(bsm=self.bsm, version=1)
        assert result == config_data

    def test_s3_parameter_with_read_text_kwargs(self):
        """Test S3Parameter read with additional kwargs"""
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-read-kwargs")
        parameter_name = "myapp-read-kwargs"
        
        s3_param = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        
        # Write data first
        config_data = '{"test": "read_kwargs"}'
        s3_param.write(
            bsm=self.bsm,
            value=config_data,
            version=1,
        )
        
        # Read with additional kwargs
        read_kwargs = {"encoding": "utf-8"}
        result = s3_param.read(
            bsm=self.bsm,
            version=1,
            read_text_kwargs=read_kwargs,
        )
        assert result == config_data

    def test_s3_parameter_metadata_handling(self):
        """Test that metadata is properly set"""
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-metadata")
        parameter_name = "myapp-metadata"
        
        s3_param = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        
        config_data = '{"test": "metadata", "key": "value"}'
        s3path_result = s3_param.write(
            bsm=self.bsm,
            value=config_data,
            version=1,
        )
        
        # Check that metadata was set
        s3path = s3_param.get_s3path(1)
        metadata = s3path.metadata
        
        # Should have both VERSION and SHA256 in metadata
        from aws_config.constants import S3MetadataKeyEnum
        assert S3MetadataKeyEnum.CONFIG_VERSION.value in metadata
        assert S3MetadataKeyEnum.CONFIG_SHA256.value in metadata
        
        # Verify version metadata
        assert metadata[S3MetadataKeyEnum.CONFIG_VERSION.value] == "1"
        
        # Verify SHA256 matches the content
        from aws_config.utils import sha256_of_text
        expected_sha256 = sha256_of_text(config_data)
        assert metadata[S3MetadataKeyEnum.CONFIG_SHA256.value] == expected_sha256
        
        # Test latest file metadata
        s3_param.write(
            bsm=self.bsm,
            value=config_data,
            version=None,
        )
        
        s3path_latest = s3_param.get_s3path(None)
        metadata_latest = s3path_latest.metadata
        
        # Latest should have "latest" as version
        from aws_config.constants import LATEST_VERSION
        assert metadata_latest[S3MetadataKeyEnum.CONFIG_VERSION.value] == LATEST_VERSION

    def test_filename_ordering(self):
        """Test that filename pattern ensures correct ordering"""
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-ordering")
        parameter_name = "myapp-ordering"
        
        s3_param = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        
        # Write multiple versions
        versions = [1, 2, 3, 10]
        for version in versions:
            config_data = f'{{"version": {version}}}'
            s3_param.write(
                bsm=self.bsm,
                value=config_data,
                version=version,
            )
        
        # Write latest
        s3_param.write(
            bsm=self.bsm,
            value='{"version": "latest"}',
            version=None,
        )
        
        # List objects and verify ordering
        objects = list(s3_param.s3dir_param.iter_objects(bsm=self.bsm))
        filenames = [obj.basename for obj in objects]
        
        # Latest should come first (000000), then reverse version order (higher numbers = lower values)
        assert filenames[0].endswith("000000-LATEST.json")
        
        # Verify version files are in reverse chronological order
        version_files = [f for f in filenames if not f.endswith("LATEST.json")]
        
        # Version 10 should come first (smallest padded number), then 3, 2, 1
        # Check that version 10 appears in the first few files
        assert any("10.json" in f for f in version_files[:2])
        # Check that version 1 appears in the last few files  
        assert any("1.json" in f for f in version_files[-2:])

    def test_error_handling(self):
        """Test error handling scenarios"""
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-errors")
        parameter_name = "myapp-errors"
        
        s3_param = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        
        # Test reading non-existent config
        with pytest.raises(S3ObjectNotExist):
            s3_param.read(bsm=self.bsm, version=None)
        
        with pytest.raises(S3ObjectNotExist):
            s3_param.read(bsm=self.bsm, version=1)
        
        # Test reading with None kwargs (should work)
        # First write something
        s3_param.write(
            bsm=self.bsm,
            value='{"test": "error_handling"}',
            version=1,
            write_text_kwargs=None,
        )
        
        # Then read with None kwargs
        result = s3_param.read(
            bsm=self.bsm,
            version=1,
            read_text_kwargs=None,
        )
        assert result == '{"test": "error_handling"}'

    def test(
        self,
        disable_logger,
    ):
        """Main test entry point - run simplified version"""
        # Just run a basic comprehensive test to avoid conflicts
        # when running from the main entry point
        s3dir_config = self.s3bucket_test_bucket.joinpath("config-main-test")
        parameter_name = "main-test-param"
        
        s3_param = S3Parameter(
            s3dir_config=s3dir_config,
            parameter_name=parameter_name,
        )
        
        # Test basic functionality
        # 1. Test non-existent read
        with pytest.raises(S3ObjectNotExist):
            s3_param.read(bsm=self.bsm, version=999)
        
        # 2. Test write and read
        config_data = '{"test": "main", "version": 1}'
        s3path_result = s3_param.write(
            bsm=self.bsm,
            value=config_data,
            version=1,
        )
        assert s3path_result is not None
        
        # 3. Test read back
        result = s3_param.read(bsm=self.bsm, version=1)
        assert result == config_data
        
        # 4. Test metadata
        s3path = s3_param.get_s3path(1)
        metadata = s3path.metadata
        from aws_config.constants import S3MetadataKeyEnum
        assert S3MetadataKeyEnum.CONFIG_VERSION.value in metadata
        assert S3MetadataKeyEnum.CONFIG_SHA256.value in metadata


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.s3",
        preview=False,
    )