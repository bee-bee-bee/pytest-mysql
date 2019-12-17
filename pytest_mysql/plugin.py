# -*- coding: utf-8 -*-

import pytest
from configparser import ConfigParser
from pytest_mysql.db import DB


def pytest_addoption(parser):
    group = parser.getgroup('pytest_mysql')
    group.addoption(
        '--config_mysql',
        action='store',
        # default='config/config.yml',
        help='relative path of config.yml'
    )


@pytest.fixture(scope="session", autouse=False)
def mysqlcmdopt(request):
    option_config = request.config.getoption("--config_mysql")
    if option_config:
        return option_config
    else:
        try:
            ini_config = request.config.inifile.strpath
            config = ConfigParser()
            config.read(ini_config)
            mysql_config = config.get('mysql', 'config')
            return mysql_config
        except Exception as e:
            raise RuntimeError("there is no mysql config in pytest.ini", e)


@pytest.fixture(scope="session", autouse=False)
def mysql(mysqlcmdopt, request):
    return DB(mysqlcmdopt, request.config.rootdir).mysql
