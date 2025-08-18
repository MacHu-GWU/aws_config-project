# -*- coding: utf-8 -*-

from vislog import VisLog
from .paths import PACKAGE_NAME

logger = VisLog(
    name=PACKAGE_NAME,
    log_format="%(message)s",
)
