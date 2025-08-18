# -*- coding: utf-8 -*-

import pytest
from aws_config.logger import logger


@pytest.fixture
def disable_logger():
    with logger.disabled():
        yield
