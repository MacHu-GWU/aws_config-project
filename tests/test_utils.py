# -*- coding: utf-8 -*-

from aws_config.utils import (
    sha256_of_text,
    sha256_of_config_data,
    encode_version,
)
from aws_config.constants import LATEST_VERSION


def test_sha256_of_text():
    _ = sha256_of_text("Hello")


def test_sha256_of_config_data():
    _ = sha256_of_config_data({"name": "Alice"})


def test_encode_version():
    assert encode_version(None) == LATEST_VERSION
    assert encode_version(LATEST_VERSION) == LATEST_VERSION
    assert encode_version(1) == "1"
    assert encode_version(999999) == "999999"
    assert encode_version("1") == "1"
    assert encode_version("000001") == "1"


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.utils",
        preview=False,
    )
