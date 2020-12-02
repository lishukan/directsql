#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import traceback
from typing import Iterable
import pymysql
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
import time
import random
import uuid
import logging
import  traceback


class SimpleConnection(object):

    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)

    def __init__(self, port=3306, charset='utf8', **kwargs):
        self.connector=pymysql.connect(port=port, charset=charset, **kwargs)


    def get_cursor(self, cursor_type=None):
        try:
            return  self.connector.cursor(DictCursor) if cursor_type=='dict' else self.connector.cursor()
        except Exception as e :
            print(e)
            self.logger.warning("ping and reconnect ...")
            self.connector.ping(reconnect=True)
            return  self.connector.cursor(DictCursor) if cursor_type=='dict' else self.connector.cursor()


    def query(self, sql, param=None, cursor_type=None,):
        """
        仅仅用作执行查询语句，不提交不回滚
        """
        cursor = self.get_cursor(cursor_type)
        result = count = False
        try:
            count = cursor.execute(sql,param) 
            result = cursor.fetchall()  # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        except :
            traceback.print_exc()
        finally:
            return result,count

    def execute_sql(self, sql, param=None,many=False, cursor_type=None):
        """
        # cursor_type为游标类型（默认返回值为元祖类型），可选字典游标，将返回数据的字典形式
        # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        注意此方法 返回两个值
        """
        cursor = self.get_cursor(cursor_type)
        result = count = False
        try:
            count = cursor.executemany(sql,param) if  many else cursor.execute(sql, param)  # 得到受影响的数据条数
            self.connector.commit()
            result = cursor.fetchall()  # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        except :
            self.connector.rollback()
            traceback.print_exc()
        finally:
            return result, count

