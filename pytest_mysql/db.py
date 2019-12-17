#!/usr/bin/env python
# coding=utf-8
import yaml
from pytest_mysql.logger import logger
from pytest_mysql.mysql_client import MysqlManager


def singleton(cls):
    def _singleton(*args, **kwargs):
        instance = cls(*args, **kwargs)
        instance.__call__ = lambda: instance
        return instance

    return _singleton


@singleton
class DB(object):
    def __init__(self, mysqlcmdopt, rootdir):
        config_path = '{0}/{1}'.format(rootdir, mysqlcmdopt)
        with open(config_path) as f:
            self.env = yaml.load(f, Loader=yaml.FullLoader)

    def __del__(self):
        for k, v in self.mysql.items():
            self.mysql[k].close()

    @property
    def mysql(self):
        mysql_dict = dict()
        try:
            for k, v in self.env.get('mysql', {}).items():
                mysql_dict[k] = MysqlManager(**v)
        except Exception as e:
            logger.error(e)
            raise ConnectionError(e)

        return mysql_dict
