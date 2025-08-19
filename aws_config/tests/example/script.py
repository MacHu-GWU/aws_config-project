# -*- coding: utf-8 -*-

from aws_config.tests.example.config_define import EnvNameEnum, Env, Config
import moto
from s3pathlib import S3Path
from boto_session_manager import BotoSesManager

mock_aws = moto.mock_aws()
mock_aws.start()

bsm = BotoSesManager(region_name="us-east-1")
bucket = "my-test-bucket"
bsm.s3_client.create_bucket(Bucket=bucket)
s3dir_config = S3Path(f"s3://{bucket}/config/").to_dir()
config = Config.load_from_local()
result = config.deploy_env_parameter(
    ssm_client=bsm.ssm_client,
    s3_client=bsm.s3_client,
    s3dir_config=s3dir_config,
    env_name=EnvNameEnum.dev.value,
    type="String",
)
print(f"{result.is_ssm_deployed = }")
print(f"{result.is_s3_deployed = }")
print(f"{result.parameter_name = }")
print(f"{result.version = }")
