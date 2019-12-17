#!/usr/bin/env python
# coding=utf-8

import traceback

import pymysql
from decimal import Decimal
import datetime
from pytest_mysql.logger import logger


class MysqlManager(object):
    def __init__(self, **kwargs):
        try:
            self.connection = pymysql.connect(**kwargs)
            self.cursor = self.connection.cursor(cursor=pymysql.cursors.DictCursor)
        except pymysql.Error as e:
            logger.error(traceback.format_exc())
            # print("Mysql Error %d: %s" % (e.args[0], e.args[1]))

    def fetch(self, table, where_model, fields=None, exclude_fields=None, order_by: str = None):
        """
        :param table:表名
        :param where_model:查询条件，字典格式
        :param fields:查询的字段，默认查询全部字段
        :param exclude_files: 排除字段
        :order_by: 排序 desc或者asc
        :return:查询获得的数据
        """
        if exclude_fields is None:
            exclude_fields = []
        try:
            if fields is None:
                fields = "*"
            where = ''
            for k, v in where_model.items():
                # k could be like 'sample_ts' or include operator like 'sample_ts>'
                symbol = '='
                for i in ['>=', '<=', '!=', '>', '<', ' in']:
                    if i in k:
                        symbol = i
                        k = k.split(symbol)[0]
                        break

                if isinstance(v, int):
                    where += "`{0}`{1}{2}".format(k, symbol, v) + " and "
                elif isinstance(v, str):
                    where += "`{0}`{1}'{2}'".format(k, symbol, pymysql.escape_string(v)) + " and "
                elif isinstance(v, list):
                    in_str = str(v).replace('[', '(').replace(']', ')')
                    where += "`{0}`{1} {2}".format(k, symbol, in_str) + " and "
            if order_by:
                order_by = ','.join([f"`{item[0]}` {' '.join(item[1:])}" for item in [item.strip().split() for item in order_by.split(',')]])
                sql = "select %s from `%s` where %s order by %s" % (",".join(fields), table, where[:-4], order_by)
            else:
                sql = "select %s from `%s` where %s" % (",".join(fields), table, where[:-4])
            logger.info(sql)
            self.cursor.execute(sql)
            data = _format(self.cursor.fetchall())
            # data = self.cursor.fetchall()
            logger.debug('Mysql中[{0}]表中的数据是:\n{1}'.format(table, data))
            if exclude_fields:
                for i in range(len(data)):
                    for item in exclude_fields:
                        data[i].pop(item)

            return data
        except pymysql.Error as e:
            # self.close()
            logger.error(traceback.format_exc())

    def fetch_one(self, table, where_model, fields=None, exclude_fields=None, order_by: str = None):
        if exclude_fields is None:
            exclude_fields = []
        try:
            if fields is None:
                fields = "*"
            where = ''
            for k, v in where_model.items():
                # k could be like 'sample_ts' or include operator like 'sample_ts>'
                symbol = '='
                for i in ['>=', '<=', '!=', '>', '<', ' in']:
                    if i in k:
                        symbol = i
                        k = k.split(symbol)[0]
                        break

                if isinstance(v, int):
                    where += "`{0}`{1}{2}".format(k, symbol, pymysql.escape_string(v)) + " and "
                elif isinstance(v, str):
                    where += "`{0}`{1}'{2}'".format(k, symbol, pymysql.escape_string(v)) + " and "
                elif isinstance(v, list):
                    in_str = str(v).replace('[', '(').replace(']', ')')
                    where += "`{0}`{1} {2}".format(k, symbol, in_str) + " and "
            if order_by:
                order_by = ','.join([f"`{item[0]}` {' '.join(item[1:])}" for item in [item.strip().split() for item in order_by.split(',')]])
                sql = "select %s from `%s` where %s order by %s" % (",".join(fields), table, where[:-4], order_by)
            else:
                sql = "select %s from `%s` where %s" % (",".join(fields), table, where[:-4])
            logger.info(sql)
            self.cursor.execute(sql)
            data = _format(self.cursor.fetchone())
            # data = self.cursor.fetchall()
            logger.debug('Mysql中[{0}]表中的数据是:\n{1}'.format(table, data))
            if exclude_fields:
                for i in range(len(data)):
                    for item in exclude_fields:
                        data[i].pop(item)
            return data
        except pymysql.Error as e:
            # self.close()
            # print("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            logger.error(traceback.format_exc())

    def delete(self, table, where_model):
        """
        从数据库删除记录
        :param table:
        :param where_model:查询条件，字典格式
        :param fields:查询的字段，默认查询全部字段
        :return:查询获得的数据，json格式的列表
        """
        try:
            where = " and ".join(["`%s`='%s'" % (k, v) for k, v in where_model.items()])
            sql = "delete from `%s` where %s" % (table, where)
            self.cursor.execute(sql)
        except pymysql.Error as e:
            # self.close()
            # print("Mysql Error %d: %s" % (e.args[0], e.args[1]))
            logger.error(traceback.format_exc())

    def insert(self, table, model: dict):
        """
        insert data into table
        :param model: <dict> key与数据库field一致
        :param table: table name
        :return: The result of insert sql execute
        """

        fields, values = [], []
        for k, v in model.items():
            fields.append(k)
            if isinstance(v, int):
                values.append("%s" % v)
            elif isinstance(v, str):
                values.append("'%s'" % pymysql.escape_string(v))
        sql = "insert into `%s` (`%s`) values (%s)" % (table, "`,`".join(fields), ",".join(values))
        try:
            logger.info(sql)
            return self.cursor.execute(sql)
        except pymysql.Error as e:
            # self.close()
            logger.error(traceback.format_exc())

    def update(self, table, where_model: dict, fields_with_data: dict):
        """
        更新
        :param table: 表名
        :param fields_with_data: <dict>key与数据库field一致
        :param where_model: 条件，类型与model相同
        :return:
        """

        if where_model and isinstance(where_model, dict):
            where = ''
            for k, v in where_model.items():
                symbol = '='
                if isinstance(v, int):
                    where += "`{0}`{1}{2}".format(k, symbol, v) + " and "
                elif isinstance(v, str):
                    where += "`{0}`{1}'{2}'".format(k, symbol, pymysql.escape_string(v)) + " and "
                elif isinstance(v, list):
                    where += "`{0}`{1} {2}".format(k, symbol, tuple(v)) + " and "
        else:
            raise Exception('where_model can not be empty, and it should be a dict')

        if fields_with_data and isinstance(fields_with_data, dict):
            set = ''
            for k, v in fields_with_data.items():
                symbol = '='
                if isinstance(v, str):
                    set += "`{0}`{1}'{2}'".format(k, symbol, pymysql.escape_string(v)) + ","
                elif v == None:
                    set += "`{0}`{1}{2}".format(k, symbol, 'null') + ","
                elif isinstance(v, int):
                    set += "`{0}`{1}{2}".format(k, symbol, v) + ","
                else:
                    raise Exception('key {0} value {1}, value type is wrong'.format(k, v))

        else:
            raise Exception('fields_with_data can not be empty, and it should be a dict')
        sql = 'update `%s` set %s where' ' %s ' % (table, set[:-1], where[:-4])
        try:
            logger.debug('Mysql sql: {}'.format(sql))
            return self.cursor.execute(sql)

        except Exception as e:
            raise Exception("Mysql update error {}".format(e))

    def close(self):
        self.cursor.close()
        self.connection.close()


def _format(data):
    if isinstance(data, dict):
        for k in data:
            if type(data[k]) is Decimal:
                data[k] = float(data[k])
            if type(data[k]) is datetime.datetime:
                data[k] = str(data[k])

    elif isinstance(data, list):
        for d in data:
            _format(d)
    return data
