#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import traceback
import  re
import pymysql
from pymysql.cursors import DictCursor,SSCursor,SSDictCursor
try:
    from dbutils.pooled_db import PooledDB
except:
    from DBUtils.PooledDB import PooledDB

import time
from .sqlgenerator import SqlGenerator, MysqlSqler

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

    # def get_cursor(self,connection=None):
    #     raise NotImplementedError("get_cursor function should be called on child object ")


class SimpleConnector(SqlGenerator):

    logger = logger
    charset='utf8'
    def _set_conn_var(self, **kwargs):
        if kwargs.get('string_arg', None):
            connargs = self.get_conn_args_from_str(kwargs['string_arg'])
            del kwargs['string_arg']
        else:
            connargs = {'user': 'root', 'port': 3306, 'charset': 'utf8'}
        connargs.update(kwargs)

        connargs['port']=int(connargs['port'])
        self.host = connargs['host']
        self.user = connargs['user']
        self.port = int(connargs['port'])
        self.password = connargs['password']
        self.charset = connargs.get('charset', 'utf8')
        self.database = connargs['database']

        return connargs

    def __init__(self, **kwargs):
        connargs=self._set_conn_var(**kwargs)
        self.connector = pymysql.connect(**connargs)



    def get_conn_args_from_str(self, string):
        """
        @param string,connect args on terminal ,for example: mysql -h127.0.0.1 -p1234  -uroot -p123456 -Dtest_base 
        return a dict with connect args
        { "host":"127.0.0.1","port":1234,"password":"123456","db":"test_base"  }
        """
        conn_args = dict()
        patter_dict = {
            "host": "-h\s*?([\d\.]+)",
            "port": "-P\s*?(\d+)",
            "user": "-u\s*?(\S+)",
            "password": "-p\s*?(\S+)",
            "database":"-D\s*?(\S+)",
        }
        for k, v in patter_dict.items():
            result = re.findall(v, string)
            number=len(result)
            if number==1:
                conn_args[k] = result[0]
            else:
                raise ValueError("invalid param got when using {} to regxp {}".format(v,k))
        
        return conn_args

    def get_connection(self):
        return self.connector

    # def get_cursor(self, cursor_type=None,connection=None):
    #     try:
    #         conn = self.get_connection() if not connection else connection
    #         return self._choose_cursor(cursor_type,conn)
    #     except Exception as e:
    #         self.logger.warning("ping and reconnect ...")
    #         conn.ping(reconnect=True)
    #         return self._choose_cursor(cursor_type,conn)

    # def _choose_cursor(self, cursor_type, conn):
    #     if cursor_type=='dict':
    #         return conn.cursor(DictCursor)
    #     elif cursor_type == 'ss':
    #         return conn.cursor(SSCursor)
    #     elif cursor_type == 'ssdict':
    #         return conn.cursor(SSDictCursor)
    #     return conn.cursor()

    def read_ss_result(self, sql, param=None, cursor_type='ss'):
        conn = self.get_connection()
        cursor = conn.cursor(SSCursor) if cursor_type == 'ss' else conn.cursor(SSDictCursor)  #此处只支持流式游标
        try:
            count = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)  # 得到受影响的数据条数
            #conn.commit() 
            result = cursor.fetchone()
            while result is not None:
                yield result
                result = cursor.fetchone()
            cursor.close()
        except:
            conn.rollback()
            traceback.print_exc()
            return False
        
            


    def execute_sql(self, sql, param=None, cursor_type=None):
        """
        # cursor_type为游标类型（默认返回值为元祖类型），可选字典游标，将返回数据的字典形式
        # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        注意此方法 返回两个值
        如果参数是列表类型，则使用executemany方法 
        """
        conn = self.get_connection()
        cursor = conn.cursor(DictCursor) if cursor_type == 'dict' else conn.cursor() #此处由于需要返回查询结果集，所以不支持流式游标
        result = count = False
        try:
            count = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)  # 得到受影响的数据条数
            conn.commit()
            result = cursor.fetchall()  # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        except Exception as e:
            self.logger.warning("---------------------------------")
            self.logger.error(str(e))
            self.logger.error(sql)
            #self.logger.error(param)
            self.logger.error(traceback.format_exc())
            self.logger.info("---------------------------------")
            conn.rollback()
            #traceback.print_exc()
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
            #self.logger.error(param)
            self.logger.info("---------------------------------")
            conn.rollback()
            traceback.print_exc()
        finally:
            return result

    def do_transaction(self, sql_params: list, cursor_type=None):
        """
        sql_params 内的元素类型为 tuple  对应 ---> （sql,params）  ， 其中 如果params 类型为list，则会使用启用游标的executemany 去执行
        """
        conn = self.get_connection()
        cursor = conn.cursor(DictCursor) if cursor_type == 'dict' else conn.cursor()
        result = count = False
        try:
            for sql, param in sql_params:
                count = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)
            result = cursor.fetchall()
            conn.commit()
        except:
            conn.rollback()
            traceback.print_exc()
        finally:
            return result, count

    def select(self, columns='id', table=None, where=None, group_by: str = None, order_by: str = None, limit: int = None, offset=None,cursor_type=None):
        """
        仅支持 简单的查询
        """
        sql, param = self.generate_select_sql(columns, table, where, group_by, order_by, limit, offset)
        return self.execute_sql(sql, param,cursor_type)

    def insert_into(self, table, data: dict or list, columns=None, ignore=False, on_duplicate_key_update: str = None, return_id=False):
        """
        此方法将返回 插入后的 id
        """
        sql, param = self.generate_insert_sql(table, data, columns, ignore, on_duplicate_key_update)
        return self.execute_with_return_id(sql, param) if return_id else self.execute_sql(sql, param)[1]

    def replace_into(self,table, data: dict or list, columns=None):
        sql, param = self.generate_replace_into_sql(table,data,columns)
        return self.execute_sql(sql, param)[1]

    def update_by_primary(self, table, data: dict, pri_value, columns=None, primary: str = 'id'):
        sql, param = self.generate_update_sql_by_primary(table, data, pri_value, columns, primary)
        return self.execute_sql(sql, param)[1]

    def update(self,table, data: dict, where, columns: None or list = None, limit=None):
        """
        update action  only return affected_rows
        """
        sql, param = self.generate_update_sql(table, data, where, columns , limit)
        return self.execute_sql(sql, param)[1]

    def delete_by_primary(self, table, pri_value, primary='id'):
        sql = "DELETE FROM {} WHERE `{}`=%s ".format(table, primary)
        return self.execute_sql(sql, (pri_value,))[1]

    def delete(self,table, where: str or dict, limit: int = 0):
        sql, param = self.generate_delete_sql(table, where, limit)
        return self.execute_sql(sql, param)[1]

    def get_multiqueries(self,sql,params=None,cursor_type=None):
        """
        同时执行多个sql ，一次性返回所有结果集.
        sql 是以 分号； 进行分割的多条语句
        @queries : [(sql1,param1),(sql2,param2),...]
        @cursor_type : dict or None
        """
        conn = self.get_connection()
        cursor = conn.cursor(DictCursor) if cursor_type == 'dict' else conn.cursor() #此处由于需要返回查询结果集，所以不支持流式游标
        try:
            cursor.execute(sql,params)
            results=[]
            results.append(cursor.fetchall())
            while cursor.nextset():
                results.append(cursor.fetchall())
            conn.commit()#这个要在最后进行提交
            return results
        except Exception  :
            cursor.close()
            conn.rollback()
            traceback.print_exc()
            return False



class MysqlConnection(MysqlSqler, SimpleConnector):

    def merge_into(self, table, data:dict or list, columns=None, need_merge_columns: list = None):
        sql, param = self.generate_merge_sql( table, data, columns, need_merge_columns)
        return self.execute_sql(sql, param)[1]
    
    @property
    def tables(self):
        if hasattr(self,'_tables'):
            return self._tables
        all_tables, count = self.execute_sql("show tables")
        setattr(self, '_tables', [d[0] for d in all_tables])
        return self._tables
        


class SimplePoolConnector(object):

    _creator=None

    def _init_connargs(self,*args,**kwargs):
        args_dict=self._set_conn_var(**kwargs)
        #默认参数
        connargs = {"host": self.host, "user": self.user, "password": self.password, "database": self.database, 'port': self.port, "charset": self.charset,
                    "creator": self._creator, "mincached": 3, "maxcached": 8, "maxshared": 5, "maxconnections": 10, "blocking": True, "maxusage": 0}
        
        # mincached : 启动时开启的空连接数量(0代表开始时不创建连接)
        # maxcached : 连接池最大可共享连接数量(0代表不闲置连接池大小)
        # maxshared : 共享连接数允许的最大数量(0代表所有连接都是专用的)如果达到了最大数量,被请求为共享的连接将会被共享使用
        # maxconnecyions : 创建连接池的最大数量(0代表不限制)
        # blocking : 达到最大数量时是否阻塞(0或False代表返回一个错误<toMany......>; 其他代表阻塞直到连接数减少,连接被分配)
        # maxusage : 单个连接的最大允许复用次数(0或False代表不限制的复用).当达到最大数时,连接会自动重新连接(关闭和重新打开)
        # setsession : 用于传递到数据库的准备会话，如 [”set name UTF-8″]
        connargs.update(args_dict)
        return  connargs

    def __init__(self, *args, **kwargs):
        self.connargs=self._init_connargs(*args,**kwargs)
        #print(self.connargs)
        self.connection_pool = PooledDB(**self.connargs)


    def get_connection(self):
        return self.connection_pool.connection()


class MysqlPool(SimplePoolConnector, MysqlConnection):

    port = 3306
    _creator=pymysql





