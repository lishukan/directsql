#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import traceback
from typing import Iterable
import pymysql
from pymysql.cursors import DictCursor
try:
    from dbutils.pooled_db import PooledDB
except:
    from DBUtils.PooledDB import PooledDB

import time
import random
import uuid
import logging
import traceback
from sqlgenerator import SqlGenerator, MysqlSqler
import psycopg2 as pg2

import logging
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - directsql.%(name)s.py -[%(levelname)s] - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class SqlHandler(object):

    def get_connection(self):
        raise NotImplementedError("get_connection function should be called on child object ")

    def get_cursor(self):
        raise NotImplementedError("get_cursor function should be called on child object ")


class SimpleConnector(SqlGenerator):

    logger = logger

    def __init__(self, **kwargs):
        connargs = {'user': 'root', 'port': 3306, 'charset': 'utf8'}
        connargs.update(kwargs)
        self.connector = pymysql.connect(**connargs)

    def get_connection(self):
        return self.connector

    def get_cursor(self, cursor_type=None):
        try:
            conn = self.get_connection()
            return conn.cursor(DictCursor) if cursor_type == 'dict' else conn.cursor()
        except Exception as e:
            self.logger.warning("ping and reconnect ...")
            conn.ping(reconnect=True)
            return conn.cursor(DictCursor) if cursor_type == 'dict' else conn.cursor()

    def execute_sql(self, sql, param=None, cursor_type=None):
        """
        # cursor_type为游标类型（默认返回值为元祖类型），可选字典游标，将返回数据的字典形式
        # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        注意此方法 返回两个值
        如果参数是列表类型，则使用executemany方法 
        """
        conn = self.get_connection()
        cursor = conn.cursor(DictCursor) if cursor_type == 'dict' else conn.cursor()
        result = count = False
        try:
            count = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)  # 得到受影响的数据条数
            conn.commit()
            result = cursor.fetchall()  # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        except:
            self.logger.info("---------------------------------")
            self.logger.error(sql)
            self.logger.error(param)
            self.logger.info("---------------------------------")
            conn.rollback()
            traceback.print_exc()
        finally:
            return result, count

    def execute_with_return_id(self, sql, param=None):
        """
        此方法会返回插入的最后一行的id
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        result = False
        try:
            r = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)
            cursor.execute("SELECT LAST_INSERT_ID() AS id")
            result = cursor.fetchall()[0][0]
            conn.commit()
        except:
            self.logger.info("---------------------------------")
            self.logger.error(sql)
            self.logger.error(param)
            self.logger.info("---------------------------------")
            conn.rollback()
            traceback.print_exc()
        finally:
            return result

    def do_transaction(self, sql_params: list, cursor_type=None, no_params=False):
        """
        sql_params 内的元素类型为 tuple  对应 ---> （sql,params）  ， 其中 如果params 类型为list，则会使用启用游标的executemany 去执行
        """
        conn = self.get_connection()
        cursor = conn.cursor(DictCursor) if cursor_type == 'dict' else conn.cursor()
        result = count = False
        try:
            if no_params:
                for sql in sql_params:
                    count = cursor.execute(sql)
            else:
                for sql, param in sql_params:
                    count = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)
            result = cursor.fetchall()
            conn.commit()
        except:
            conn.rollback()
            traceback.print_exc()
        finally:
            return result, count

    def select(self, *args, **kwargs):
        """
        仅支持 简单的查询
        """
        sql, param = self.generate_select_sql(*args, **kwargs)
        return self.execute_sql(sql, param)

    def insert_into(self, table, data: dict or list, columns_order=None, ignroe=False, on_duplicate_key_update: str = None, return_id=False):
        """
        此方法将返回 插入后的 id
        """
        sql, param = self.generate_insert_sql(table, data, columns_order, ignroe, on_duplicate_key_update)
        return self.execute_with_return_id(sql, param) if return_id else self.execute_sql(sql, param)[1]

    def replace_into(self, *args, **kwargs):
        sql, param = self.generate_replace_into_sql(*args, **kwargs)
        return self.execute_sql(sql, param)[1]

    def update_by_primary(self, *args, **kwargs):
        sql, param = self.generate_update_sql_by_primary(*args, **kwargs)
        return self.execute_sql(sql, param)[1]

    def update(self, *args, **kwargs):
        """
        update action  only return affected_rows
        """
        sql, param = self.generate_update_sql(*args, **kwargs)
        return self.execute_sql(sql, param)[1]

    def delete_by_primary(self, table, pri_value, primary='id'):
        sql = "DELETE FROM {} WHERE `{}`=%s ".format(table, primary)
        return self.execute_sql(sql, (pri_value,))[1]

    def delete(self, *args, **kwargs):
        sql, param = self.generate_delete_sql(*args, **kwargs)
        return self.execute_sql(sql, param)[1]


class MysqlConnector(MysqlSqler, SimpleConnector):

    def merge_into(self, *args, **kwargs):
        sql, param = self.generate_merge_sql(*args, **kwargs)
        return self.execute_sql(sql, param)[1]
    


class SimplePoolConnector(SimpleConnector):

    def __init__(self, *args, **kwargs):
        self.connection_pool = PooledDB(*args, **kwargs)

    def get_connection(self):
        return self.connection_pool.connection()


class MysqlPool(SimplePoolConnector,MysqlConnector):

    def __init__(self, host, user, password, database, port=3306, charset='utf8', re_create=True, **kwargs):
        """
        # todo re_create=True 则会
        mincached 最小的连接数（实例化时最开始就实例化这个数的连接）

        连接断开时，连接池能感受到
        """
        self._host = host = host
        self._port = port
        self._user = user
        self._database = database
        self._password = password
        self._charset = charset
        connargs = {"host": self._host, "user": self._user, "password": self._password, "database": self._database, 'port': self._port, "charset": self._charset,
                    "creator": pymysql, "mincached": 3, "maxcached": 8, "maxshared": 5, "maxconnections": 10, "blocking": True, "maxusage": 0}

        connargs.update(kwargs)

        self.connection_pool = PooledDB(**connargs)


class PostgrePool(SimplePoolConnector):

    def __init__(self, host, user, password, database, port=5432, charset='utf8', re_create=True, **kwargs):
        """
        # todo re_create=True 则会
        mincached 最小的连接数（实例化时最开始就实例化这个数的连接）

        连接断开时，连接池能感受到
        """
        self._host = host
        self._port = port
        self._user = user
        self._database = database
        self._password = password
        self._charset = charset
        connargs = {"host": self._host, "user": self._user, "password": self._password, "database": self._database, 'port': self._port, "charset": self._charset,
                    "creator": pg2, "mincached": 3, "maxcached": 8, "maxshared": 5, "maxconnections": 10, "blocking": True, "maxusage": 0}

        connargs.update(kwargs)

        self.connection_pool = PooledDB(**connargs)
