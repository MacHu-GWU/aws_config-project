# -*- coding: utf-8 -*-

from enum_mate.api import BetterStrEnum

ZFILL = 6
"""
Can support up to 999999 versions.
"""

LATEST_VERSION = "LATEST"


class AwsTagKeyEnum(BetterStrEnum):
    """
    Enumerate the keys of AWS tags used in the project.
    """

    CONFIG_VERSION = "config_version"
    CONFIG_SHA256 = "config_sha256"
