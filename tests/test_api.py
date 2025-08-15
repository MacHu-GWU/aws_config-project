# -*- coding: utf-8 -*-

from aws_config import api


def test():
    _ = api


if __name__ == "__main__":
    from aws_config.tests import run_cov_test

    run_cov_test(
        __file__,
        "aws_config.api",
        preview=False,
    )
