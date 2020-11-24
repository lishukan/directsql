#! /usr/bin/env python3
# -*- coding: utf-8 -*-
'''
mysql连接
'''
import traceback
from typing import Iterable
import pymysql
from pymysql.cursors import DictCursor
from DBUtils.PooledDB import PooledDB
import time
import random
import uuid
import logging
# Written on 2020-06-12 15:19:42  by lishukan
MYSQL_HOST = "localhost"
MYSQL_PWD = "123456"
MYSQL_USER = "root"
MYSQL_DB_NAME = 'test'
MYSQL_PORT = 3306
MYSQL_CHARSET = "utf8"
# mincached : 启动时开启的空连接数量(0代表开始时不创建连接)
MYSQL_MIN_CACHED = 3
# maxcached : 连接池最大可共享连接数量(0代表不闲置连接池大小)
MYSQL_MAX_CACHED = 8
# maxshared : 共享连接数允许的最大数量(0代表所有连接都是专用的)如果达到了最大数量,被请求为共享的连接将会被共享使用
MYSQL_MAX_SHARED = 10
# maxconnecyions : 创建连接池的最大数量(0代表不限制)
MYSQL_MAX_CONNECYIONS = 20
# blocking : 达到最大数量时是否阻塞(0或False代表返回一个错误<toMany......>; 其他代表阻塞直到连接数减少,连接被分配)
MYSQL_BLOCKING = True
# maxusage : 单个连接的最大允许复用次数(0或False代表不限制的复用).当达到最大数时,连接会自动重新连接(关闭和重新打开)
MYSQL_MAX_USAGE = 0
# setsession : 用于传递到数据库的准备会话，如 [”set name UTF-8″]
MYSQL_SET_SESSION = None




class MysqlUtil(object):
    # MYSQL操作

    """
    这里没有采用单例模式，为需要大量连接和跨库的场景预留了操作空间。
    但是在BaseModel中显式实例化了一个指向spider_db库的MysqlUtil类，其后只需继承BaseModel类即事实上实现指向spider_db库的单例
    """
    # 单例模式,一个mysqlutil对象只初始化一次(实例化多个类将无法实现跨库)
    # def __new__(cls, *args, **kwargs):
    #     if not hasattr(cls, 'instance'):
    #         instance = super(MysqlUtil, cls).__new__(cls)
    #         setattr(cls, 'instance', instance)
    #         return instance
    #     else:
    #         return cls.__dict__['instance']

    logger = logging.getLogger(__name__)
    logger.setLevel(level=logging.INFO)

    connection_pool = None

    def __init__(self, host, user, passwd, db, port=3306, charset='utf8', pool=True):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = int(port)
        self.charset = charset
        self.need_pool = pool
        self.conn = self.create_new_connection() if not pool else self.get_connection_from_pool()
        self.cursor = self.conn.cursor()
        self.connection_pool = None

    # def __del__(self):
    #     """
    #     自己创建的连接需要手动关闭
    #     由连接池管理的连接则交给连接池
    #     """
    #     if not self.need_pool:
    #         try:
    #             self.cursor.close()
    #             self.conn.close()
    #         except:
    #             pass
    #     else:
    #         if self.connection_pool:
    #             del self.connection_pool

    def get_new_cursor(self, _type='tuple'):
        conn = self.get_connection()
        if _type == 'dict':
            cursor = conn.cursor(DictCursor)  # 可选返回字典游标
        else:
            cursor = conn.cursor()  # 默认返回普通游标
        return cursor

    def get_connection(self, need_pool=None):
        """
        根据need_pool 决定是先建立连接池再返回一个连接，还是直接返回单个连接
        如果不传入将会根据实例的need_pool 决定
        """
        if need_pool == None:
            need_pool = self.need_pool
        if not need_pool:
            return self.conn
        else:
            return self.get_connection_from_pool()

    def create_new_connection(self):
        """
        创建新的（一个）连接
        """
        for i in range(3):
            try:
                connection = pymysql.connect(self.host, self.user,
                                             self.passwd, self.db, port=self.port, charset='utf8')

                return connection
            except Exception as e:
                self.logger.error(str(e))
                self.logger.error("create a new connection failed ,reconnecting now ... ")
                # traceback.print_exc()

        raise ConnectionError("create_new_connection创建Mysql 连接 失败")

    def get_connection_from_pool(self):
        """
        @summary: 静态方法，从连接池中取出连接 ，mincached对应池中连接的数量
        @return MySQLdb.connection
        """
        connargs = {"host": self.host, "user": self.user, "passwd": self.passwd, "db": self.db,
                    'port': self.port, "charset": self.charset}
        if not self.connection_pool:
            for _ in range(3):
                try:
                    self.connection_pool = PooledDB(creator=pymysql, mincached=MYSQL_MIN_CACHED, maxcached=MYSQL_MAX_CACHED,
                                                    maxshared=MYSQL_MAX_SHARED, maxconnections=MYSQL_MAX_CONNECYIONS,
                                                    blocking=MYSQL_BLOCKING, maxusage=MYSQL_MAX_USAGE,
                                                    setsession=MYSQL_SET_SESSION, **connargs)
                    break
                except:
                    traceback.print_exc()
                    self.logger.error("connected to mysql failed ,reconnectting now ...")

        return self.connection_pool.connection()

    def query(self, table, condition, columns='id', limit=None, ):
        """
        #从table中返回符合condition条件的num条 columns对应字段的数据
        #columns为字符串(不为*)，直接返回查询结果对应字段的值
        #columns为'*'，以字典形式返回结果
        # columns为元祖/列表，返回元祖形式 查询值
        #condition 即sql中 where 后跟的条件, 例如conditon={'name':'lee','gender':1} 则对应查询 name='lee' and gender=1 ,
        #condition只接受字典和None
        """

        cursor_type = 'dict' if columns == '*' else None  # 根据需要的字段选择游标类型，字段为* 则使用字典游标，返回字典格式的数据
        # print(columns)
        sql, param = self.generate_query_sql(table, condition, columns, limit=limit)
        return self.execute_sql(sql, param, cursor_type=cursor_type)  # 可能返回元祖或None

    def update(self, table, data, _id, primary='id'):  #
        """
        更新table表内id为_id的数据，将其改为data
        默认只更新一条，更新多条需要手写sql
        """
        sql, param = self.generate_update_sql(table, data, _id, primary)
        result, count = self.execute_sql(sql, param)
        return count

    def safe_update(self, table, new_data, _id, no_use=None):
        """
        安全更新，保留旧有数据，只更新没有值的字段
        只支持按照id 主键去查询，更新

        """
        origin_data = self.query(table, {'id': _id}, columns='*')  # 查出旧有/原数据
        if not origin_data:
            return False
        if not isinstance(origin_data, dict):
            raise TypeError("safe_update 查询结果非字典游标")

        if isinstance(no_use, Iterable):
            for useless in no_use:  # 去除无法比较/不需要的字段
                if useless in origin_data:
                    del origin_data[useless]

        """
        以origin_data为基准找出差异数据且去除缺失值 
        例： origin_data={ 'name':'JackMa','gender':'male','com':'AliBaBa','city':''   }
                new_data={'name':'马云',  'country':'China','com':'AliBaba','city':'hangzhou'}

            after merge ==>  final_data={ 'country':'China' ,'city':'hangzhou'  }
            
        """
        final_data = dict()  # 最终只留下两数据存在差异的部分，此部分优先使用原有数据除非原有数据此字段值缺失
        for key in new_data.keys() | origin_data.keys():
            old = origin_data.get(key, '')
            new = new_data.get(key, '')
            if old == new:
                continue
            if new and not old:
                final_data[key] = new

        if not final_data:
            return None

        return self.update(table, final_data, _id)

    def insert(self, table, data, ignore=False):
        sql, param = self.generate_insert_sql(table, data, ignore)
        conn = self.get_connection()
        try:
            cursor = conn.cursor()
            cursor.execute(sql, param)
            conn.commit()
            return self.get_insert_id(cursor=cursor)  # 拿到当前游标的插入记录
        except Exception as e:
            traceback.print_exc()
            conn.rollback()
            self.logger.error(str(e))
            self.logger.error('-----------------------------')
            self.logger.error(sql)
            self.logger.error(str(param))
            self.logger.info('-----------------------------')
            return False

    def insert_many(self, table, column_order, values):
        """
        @summary: 向数据表插入多条记录
        @param sql:要插入的sql格式
        @param values:要插入的记录数据tuple(tuple)/list[list]
        @return: count 受影响的行数
        """
        conn = self.get_connection()
        s_s = list()
        for i in range(len(column_order)):
            s_s.append('%s')
        sql = "insert into {} ({}) values({}) ".format(table, ','.join(column_order),    ','.join(s_s))
        try:
            cursor = conn.cursor()
            print(sql)
            count = cursor.executemany(sql, values)
            conn.commit()
            return count
        except Exception as e:
            traceback.print_exc()
            self.logger.error('error happend when insert_many')
            self.logger.error(str(e))
            conn.rollback()
            return 0

    def get_insert_id(self, cursor=None):
        """
        获取当前连接最后一次插入操作生成的id,如果没有则为0
        在多连接时需要指定(对应连接的)游标，避免数据读取异常
        """
        if not cursor:
            cursor = self.cursor

        cursor.execute("SELECT @@IDENTITY AS id")
        result = cursor.fetchone()
        if result:
            return result[0]
        else:
            return None

    def execute_sql(self, sql, param=None, cursor_type=None):
        """
        #cursor_type为游标类型（默认返回值为元祖类型），可选字典游标，将返回数据的字典形式
        # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的

        此model中几乎所有读写操作最终都会调用此方法
        注意此方法 返回两个值
        """

        self.logger.debug(sql+"<---->"+str(param))
        conn = self.get_connection()
        cursor = conn.cursor(DictCursor) if cursor_type == 'dict' else conn.cursor()  # 如果cursor_type=dict则返回字典游标
        result = count = False
        try:
            count = cursor.execute(sql) if param is None else cursor.execute(sql, param)  # 得到受影响的数据条数
            conn.commit()
            result = cursor.fetchall()  # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
            if len(result) == 1:
                result = result[0]
                if isinstance(result, tuple) and len(result) == 1:
                    result = result[0]

            """
            当只查询一个字段的时候， result,count= xxxx.execute_sql("xxxx") 的返回值result就是该字段
            例如   result,count= xxxx.execute_sql("select id from table limit 1")
                   print(result)  --->   1 
            """
            # 即 如果需要指定数量的结果，应在传入的sql中就指定 limit =xx
            # if limit == 1:
            #     result = cursor.fetchone()
            # elif limit == None:
            #     result = cursor.fetchall()
            # else:
            #     result = cursor.fetchmany(limit)
        except Exception as e:
            conn.rollback()
            self.logger.error("execute sql failed :"+str(e))
            self.logger.error(sql)
            self.logger.error(param)
        finally:
            return result, count



    def get_all_column(self, table_name):
        """返回对应表的所有字段，形式为列表"""
        sql = "select COLUMN_NAME from information_schema.COLUMNS where TABLE_NAME=%s order by COLUMN_NAME;"  # 数据库内的表名不能有重复,即使在不同的库内.
        cursor = self.get_new_cursor()
        count = cursor.execute(sql, table_name)
        if count > 0:
            result = cursor.fetchall()
            list_result = []
            for r in result:
                list_result += list(r)
            return list_result
        else:
            raise TypeError('no column got from table -{},please check ', table_name)

    def replace_into(self, table, data_list, columns=None):
        """
        replace into 可插入多条数据 
        columns 为字段列表,如果指定columns，将会只replace   columns中的字段的值
        """
        if not isinstance(data_list, list):
            data_list = [data_list, ]
        if not columns:
            columns = data_list[0].keys()
        sql, param = self.generate_replace_sql(table, columns, data_list)
        return self.execute_sql(sql, param)




class SqlHandler(object):
    
    @staticmethod  # 默认只查询id,暂只支持 = ， 不支持like、大于、小于等查询
    def generate_query_sql(table, condition, columns_u_need='id', limit=None):
        """
        生成查询sql
        """
        if isinstance(columns_u_need, (tuple, list)):
            columns = ','.join(columns_u_need)
        elif isinstance(columns_u_need, str):
            columns = columns_u_need
        else:
            raise TypeError('error ! colnmns_you_need must be str or tuple or list ')

        param = None
        where_list = []
        if isinstance(condition, dict):
            param = tuple()
            for key, value in condition.items():
                where_list.append(key+'=%s')
                param = param + (value,)
            where_str = ' AND '.join(where_list)
        else:
            raise TypeError("sql condition must be dict,for example : {'id':798456}")
        sql = "SELECT {} FROM {} WHERE {} ".format(columns, table, where_str)
        if limit:
            sql = sql+' limit {} ;'.format(limit)
        # print(sql,param)
        return sql, param

    @staticmethod
    def generate_insert_sql(table, condition, ignroe=False):
        """
        插入单条数据,condition为字典形式的数据
        """
        sql = 'INSERT INTO {} ({}) VALUES ({});' if not ignroe else 'INSERT IGNORE INTO {} ({}) VALUES ({});'

        columns = list()
        s_s = list()
        param = tuple()
        for key, value in condition.items():
            columns.append('`'+key+'`')
            s_s.append('%s')
            param = param + (value,)

        columns = ','.join(columns)
        s_s = ','.join(s_s)
        sql = sql.format(table, columns, s_s)
        #print(sql,param )
        return sql, param

    @staticmethod
    def generate_update_sql(table, condition, ID, primary='id'):
        """
        更新单条数据,condition为字典形式的数据
        """
        sql = 'UPDATE {} SET {} WHERE {}='+str(ID)+' limit 1;'
        columns = []  # 以列表形式存储SET 之后的表达式，方便后期以逗号连接 例 program_name=%s,source=%s,url_name=%s
        param = tuple()  # 存储值

        for key, value in condition.items():
            columns.append('`'+key+'`'+'=%s')
            param = param+(value,)
        sql = sql.format(table, ','.join(columns), primary)
        #print(sql,param )
        return sql, param

    @staticmethod
    def generate_replace_sql(table, columns_list, data_list):
        """
        批量更新/插入数据 ，columns_list为需要进行操作的字段名(列表)
        data_list为列表。列表中的数据表现为字典形式 
        仅适用于 data_list中的所有数据的字段都在columns_list中
        """
        sql = 'REPLACE INTO {} ({}) VALUES{} '
        column_string = '`' + '`,`'.join(columns_list) + '`'
        s_s = list()
        for i in columns_list:
            s_s.append('%s')
        s_s = '(' + ','.join(s_s) + ')'

        all_s = list()
        for i in range(0, len(data_list)):
            all_s.append(s_s)
        all_s = ','.join(all_s)

        param = tuple()  # 存储值
        for data in data_list:
            for key in columns_list:
                param = param + (data[key],)

        sql = sql.format(table, column_string, all_s)

        return sql, param