# -*- coding: utf-8 -*-


class S3BucketVersionSuspendedError(Exception):
    """
    We don't store config files in a bucket with 'suspended' status.
    This exception is raised when we try to deploy or get a config file from
    a bucket with 'suspended' status.
    """

    pass


class S3ObjectNotExist(Exception):
    """
    Raised when an AWS S3 object does not exist.
    """
    pass


class ParameterNotExists(Exception):
    """
    Raised when a parameter does not exist in AWS Parameter Store.
    """
    pass
