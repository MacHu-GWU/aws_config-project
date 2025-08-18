# -*- coding: utf-8 -*-

import json
import hashlib


def sha256_of_text(s: str) -> str:
    m = hashlib.sha256()
    m.update(s.encode("utf-8"))
    return m.hexdigest()


def sha256_of_config_data(data: dict) -> str:
    return sha256_of_text(
        json.dumps(
            data,
            sort_keys=True,
            ensure_ascii=False,
        )
    )


def encode_version(version: T.Optional[T.Union[int, str]]) -> str:
    """
    Normalize version input into standardized string format.

    Converts various version inputs into a consistent string representation
    by removing leading zeros from numeric versions while preserving the
    special LATEST version identifier.

    :param version: Version input - None, "LATEST", integer, or zero-padded string

    :returns: Normalized version string ("LATEST" or numeric without leading zeros)

    Examples::

        encode_version(None)       # → "LATEST"
        encode_version("LATEST")   # → "LATEST"
        encode_version(1)          # → "1"
        encode_version("000001")   # → "1"
        encode_version(42)         # → "42"
    """
    if version is None:
        return LATEST_VERSION
    else:
        return str(version).lstrip("0")