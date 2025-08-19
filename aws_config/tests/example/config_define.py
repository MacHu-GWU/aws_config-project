# -*- coding: utf-8 -*-

import json
import aws_config.api as aws_config

from pydantic import Field
from pathlib import Path
from which_runtime.api import runtime


class EnvNameEnum(aws_config.BaseEnvNameEnum):
    dev = "dev"
    prod = "prod"


class Env(aws_config.BaseEnv):
    username: str = Field()
    password: str = Field()


dir_here = Path(__file__).absolute().parent
path_config = dir_here / "config.json"
path_secret_config = dir_here / "secret-config.json"


class Config(aws_config.BaseConfig[Env, EnvNameEnum]):
    @classmethod
    def load_from_local(cls):
        return cls(
            data=json.loads(path_config.read_text()),
            secret_data=json.loads(path_secret_config.read_text()),
            EnvClass=Env,
            EnvNameEnumClass=EnvNameEnum,
            version="0.1.1",
        )
