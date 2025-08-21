
.. image:: https://readthedocs.org/projects/aws-config/badge/?version=latest
    :target: https://aws-config.readthedocs.io/en/latest/
    :alt: Documentation Status

.. image:: https://github.com/MacHu-GWU/aws_config-project/actions/workflows/main.yml/badge.svg
    :target: https://github.com/MacHu-GWU/aws_config-project/actions?query=workflow:CI

.. image:: https://codecov.io/gh/MacHu-GWU/aws_config-project/branch/main/graph/badge.svg
    :target: https://codecov.io/gh/MacHu-GWU/aws_config-project

.. image:: https://img.shields.io/pypi/v/aws-config.svg
    :target: https://pypi.python.org/pypi/aws-config

.. image:: https://img.shields.io/pypi/l/aws-config.svg
    :target: https://pypi.python.org/pypi/aws-config

.. image:: https://img.shields.io/pypi/pyversions/aws-config.svg
    :target: https://pypi.python.org/pypi/aws-config

.. image:: https://img.shields.io/badge/✍️_Release_History!--None.svg?style=social&logo=github
    :target: https://github.com/MacHu-GWU/aws_config-project/blob/main/release-history.rst

.. image:: https://img.shields.io/badge/⭐_Star_me_on_GitHub!--None.svg?style=social&logo=github
    :target: https://github.com/MacHu-GWU/aws_config-project

------

.. image:: https://img.shields.io/badge/Link-API-blue.svg
    :target: https://aws-config.readthedocs.io/en/latest/py-modindex.html

.. image:: https://img.shields.io/badge/Link-Install-blue.svg
    :target: `install`_

.. image:: https://img.shields.io/badge/Link-GitHub-blue.svg
    :target: https://github.com/MacHu-GWU/aws_config-project

.. image:: https://img.shields.io/badge/Link-Submit_Issue-blue.svg
    :target: https://github.com/MacHu-GWU/aws_config-project/issues

.. image:: https://img.shields.io/badge/Link-Request_Feature-blue.svg
    :target: https://github.com/MacHu-GWU/aws_config-project/issues

.. image:: https://img.shields.io/badge/Link-Download-blue.svg
    :target: https://pypi.org/pypi/aws-config#files


Welcome to ``aws_config`` Documentation
==============================================================================
.. image:: https://aws-config.readthedocs.io/en/latest/_static/aws_config-logo.png
    :target: https://aws-config.readthedocs.io/en/latest/

``aws_config`` is a well-defined configuration management framework for multi-environment applications that uses AWS services as the configuration backend. This library is built from years of Amazon best practices for managing application configurations at scale.

The framework provides hierarchical configuration management with inheritance, automatic parameter generation for different environments (dev, test, prod), and seamless integration with AWS SSM Parameter Store and S3. It eliminates configuration drift between environments while maintaining security through proper secret management.

Key benefits include type-safe configuration classes, environment-specific parameter validation, automated deployment to AWS backend services, and built-in configuration inheritance that reduces duplication across environments.

.. code-block:: python

    from aws_config import BaseConfig, BaseEnv, BaseEnvNameEnum
    from pydantic import Field

    class EnvNameEnum(BaseEnvNameEnum):
        dev = "dev"
        prod = "prod"

    class Env(BaseEnv):
        username: str = Field()
        password: str = Field()

    class Config(BaseConfig[Env, EnvNameEnum]):
        pass

    config = Config(
        data={
            "_defaults": {
                "*.project_name": "my_app",
            }
            "dev": {
                "username": "alice"
            },
            "prod": {
                "username": "bob"
            }
        },
        secret_data={
            "dev": {
                "username": "alice-pwd"
            },
            "prod": {
                "username": "bob-pwd"
            }
        },
        EnvClass=Env,
        EnvNameEnumClass=EnvNameEnum,
    )
    env = config.get_env(EnvNameEnum.dev)


.. _install:

Install
------------------------------------------------------------------------------

``aws_config`` is released on PyPI, so all you need is to:

.. code-block:: console

    $ pip install aws-config

To upgrade to latest version:

.. code-block:: console

    $ pip install --upgrade aws-config
