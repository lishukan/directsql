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


class SimpleConnector(object):

    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)

    def __init__(self, port=3306, charset='utf8', **kwargs):
        self.connector=pymysql.connect(port=port, charset=charset, **kwargs)


    def get_connection(self):
        return self.connector

    def get_cursor(self, cursor_type=None):
        try:
            conn=self.get_connection()
            return  conn.cursor(DictCursor) if cursor_type=='dict' else conn.cursor()
        except Exception as e :
            print(e)
            self.logger.warning("ping and reconnect ...")
            conn.ping(reconnect=True)
            return  conn.cursor(DictCursor) if cursor_type=='dict' else conn.cursor()


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
        conn=self.get_connection()
        cursor = conn.cursor(DictCursor) if cursor_type=='dict' else conn.cursor()
        result = count = False
        try:
            count = cursor.executemany(sql,param) if  many else cursor.execute(sql, param)  # 得到受影响的数据条数
            conn.commit()
            result = cursor.fetchall()  # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        except :
            conn.rollback()
            traceback.print_exc()
        finally:
            return result, count



class SimplePoolConnector(SimpleConnector):

    def __init__(self, *args,**kwargs):
        self.connection_pool = PooledDB(*args, **kwargs)
        
    
    def get_connection(self):
        return self.connection_pool.connection()



def PoolConnector(SimplePoolConnectorobject):

    def __init__(self, host, user, password, database, port=3306, charset='utf8', **kwargs):
        self.host = host = host
        self.port = port
        self.user = user
        self.database = database
        self.password=password
        connargs = {"host": self.host, "user": self.user, "password": self.password, "database": self.database, 'port': self.port, "charset": self.charset,
             "creator":pymysql,"mincached":3,"maxcached":8,"maxshared":5,"maxconnections":10,"blocking":True,"maxusage":0 }

        connargs.update(kwargs)
        
        self.connection_pool=PooledDB(**connargs)



  