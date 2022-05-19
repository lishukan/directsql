#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import copy
import traceback
import re
import pymysql
from pymysql.cursors import DictCursor, SSCursor, SSDictCursor, Cursor as DefaultCursor
try:
    from dbutils.pooled_db import PooledDB
except:
    from DBUtils.PooledDB import PooledDB
from typing import Iterable, List, Tuple, Union
from pymysql.constants import CLIENT
import logging
logger = logging.getLogger(__name__)
logger.setLevel(level=logging.INFO)
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s -%(name)s.py -[%(levelname)s] - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class OperationError(Exception):
    """
    用来查询失败时装填错误信息,同时使其bool值为False 方便识别是正常结果还是错误
    """

    def __bool__(self):
        return False


class Like(object):
    """
    用来构造 like  语句
    """

    def __init__(self, string: str) -> None:
        if '%' in string:
            string = string.replace('%', '%%%%')
        else:
            string = '%%' + string+'%%'

        self.string = string

    def __repr__(self):
        return self.string


class SqlGenerator(object):
    """
    该类下的方法的返回值必须为两个 
    第一个是sql，第二个是参数
    """

    @classmethod
    def get_all_column_sql(cls, table_name, dbname=None):
        """返回对应表的所有字段"""

        table_name = table_name if not dbname else dbname+"."+table_name
        sql = "SELECT COLUMN_NAME from information_schema.COLUMNS where TABLE_NAME=%s order by COLUMN_NAME;"
        return sql, (table_name,)

    @classmethod
    def generate_select_sql(cls, columns: Union[Tuple[str], List[str], str] = 'id', table: str = None, where=str or dict, group_by: str = None,
                            order_by: str = None, limit: int = None, offset: int = None):
        """
        设要查询两个字段id和name，则 columns 为（'id','name'） / ['id','name']  /  'id,name'
        要查询所有字段 使用  columns='*' 即可

        where 为字典 或字符串
        """
        assert table, "table不能为空"  # table 不能为空。但是参数里为了做到字段在前，table参数只能也设默认值为None
        columns = columns if isinstance(columns, str) else ','.join(columns)

        sql = "SELECT {} from `{}` ".format(columns, table)
        params = None
        if where:
            where_tags, where_param = cls.generate_where_sql(where)
            sql += "where {}".format(where_tags)
            params = where_param

        for key, fs in ((group_by, 'group by'), (order_by, 'order by'), (limit, 'limit'), (offset, 'offset')):
            if key:
                sql += ' {} {}'.format(fs, key)

        return sql, params

    @classmethod
    def _get_after_format_sql(cls, init_sql, table, data: dict or List[dict], columns: Union[Tuple[str], List[str], str] = None):
        """
        生成sql语句里需要插入的字段和对应的格式化符号
        @columns: 需要格式化的字段。默认去第一个传入的字典数据的键值对
        """
        if isinstance(columns, str):
            columns = (columns,)
        if not columns:
            if isinstance(data, dict):
                columns = data.keys()
            else:
                columns = data[0].keys()

        format_tags = ','.join(('%({})s'.format(col) for col in columns))
        final_sql = init_sql.format(table, '`'+'`,`'.join(columns)+'`', format_tags)
        return final_sql, data

    @classmethod
    def generate_insert_sql(cls, table, data: dict or List[dict], columns: Union[Tuple[str], List[str], str] = None, ignore=False, on_duplicate_key_update: str = None):
        """
        columns 为 可迭代对象 list/tuple/set/...
        插入单条数据,condition为字典形式的数据
        data 必须为字典 或者 为一个元素类型为字典的列表
        """
        sql = 'INSERT INTO `{}` ({}) VALUES ({})' if not ignore else 'INSERT IGNORE INTO {} ({}) VALUES ({})'
        if on_duplicate_key_update:
            sql += (" on duplicate key update "+on_duplicate_key_update)
        return cls._get_after_format_sql(sql, table, data, columns)

    @classmethod
    def generate_replace_into_sql(cls, table, data: dict or List[dict], columns: Union[Tuple[str], List[str], str] = None):
        """
        columns  字段顺序。如果不传入，则取第一个data的 键集合
        """
        sql = 'REPLACE INTO `{}` ({}) VALUES ({}) '
        return cls._get_after_format_sql(sql, table, data, columns)

    @classmethod
    def generate_update_sql_by_primary(cls, table, data: dict or List[dict], pri_value, columns: Union[Tuple[str], List[str], str] = None, primary: str = 'id'):
        """
        更新单条数据,condition为字典形式的数据
        columns 为 可迭代对象 list/tuple/set/...
        """
        sql = 'UPDATE `{}` SET {} WHERE `{}`=%s'
        if isinstance(columns, str):
            columns = (columns,)
        else:
            if not columns:
                columns = data.keys()

        param = tuple(data[k] for k in columns)
        columns_condi = ','.join(['`' + k + '`' + '=%s' for k in columns])

        sql = sql.format(table, columns_condi, primary)
        param += (pri_value,)
        return sql, param

    @classmethod
    def generate_where_sql(cls, condition):
        assert isinstance(condition, (str, dict)), "构造where条件的参数必须为字符串或 字典"
        where_tags = []
        condition_param = []
        if isinstance(condition, str):
            where_tags = condition
        else:
            for key, value in condition.items():
                if isinstance(value, tuple) and len(value) == 2:  # 大于小于
                    where_tags.append('( `{}` >=%s and `{}` <=%s )'.format(key, key))
                    condition_param.extend([value[0], value[1]])
                elif isinstance(value, list):
                    where_tags.append(' `{}` in ({}) '.format(key, ','.join(['%s'] * len(value))))
                    condition_param.extend(value)
                elif isinstance(value, Like):
                    where_tags.append(" `{}` like '{}' ".format(key, value))
                else:
                    where_tags.append(' `{}`= %s'.format(key))
                    condition_param.append(value)

            where_tags = ' AND '.join(where_tags)
        return where_tags, tuple(condition_param)

    @classmethod
    def generate_update_sql(cls, table, data: dict, where: str or dict , columns: Union[Tuple[str], List[str], str] = None, limit=None):
        """
        @data:新数据    例如  data= {'name':'jack', 'age':18,'school':'MIT'  }  --> update xx set name='jack',age=18,school='MIT' 
        @where   为dict时，会根据这个dict 转换为对应的where条件。 如传入 {'age':24}     --->  update xx set name='jack',age=18,school='MIT' where age=24
                     --> str :  age=88   update xx set name='jack',age=18,school='MIT' where age=88
        更新必须传入条件，避免漏传条件导致全表被更新
        """
        sql = 'UPDATE `{}` SET {} WHERE {}'
        if isinstance(columns, str):
            columns = (columns,)
        if not columns:
            columns = data.keys()
        set_tags = ','.join(('`{}`=%s'.format(col, col) for col in columns))
        param = tuple(data[k] for k in columns)
        where_tags, where_param = cls.generate_where_sql(where)
        param += where_param
        if limit:
            param += (limit,)
            sql += ' limit %s'
        final_sql = sql.format(table, set_tags, where_tags)
        return final_sql, param

    @classmethod
    def generate_delete_sql(cls, table, where: str or dict, limit: int = None):
        sql = "DELETE FROM `{}` WHERE {} "
        where_tags, where_param = cls.generate_where_sql(where)
        sql = sql.format(table, where_tags)
        if limit:
            sql += "limit {}".format(limit)
        return sql, where_param


class MysqlSqler(SqlGenerator):

    def generate_merge_sql(self, table, data: dict or List[dict], columns: Union[Tuple[str], List[str], str] = None,  merge_columns: Union[Tuple[str], List[str], str] = None):
        """
        columns 为需要插入的字段
        merge_columns 为 出现重复时需要更新的字段.如果不给值，将会把所有 columns 里的字段都更新
        如果columns 都没有值，将会读取所有data的 键值对
        """
        if isinstance(columns, str):
            columns = (columns,)
        if not columns:
            columns = data.keys() if isinstance(data, dict) else data[0].keys()

        format_tags = ','.join(('%({})s'.format(col) for col in columns))
        if isinstance(merge_columns, str):
            merge_columns = (merge_columns,)
        if not merge_columns:
            merge_columns = columns
        update_str = ','.join(['`{}`=values(`{}`)'.format(col, col) for col in merge_columns])
        sql = "INSERT INTO `{}` ({}) values({})  on duplicate key update {};"
        return sql.format(table, '`'+'`,`'.join(columns)+'`', format_tags, update_str), data


class _SimpleConnector(SqlGenerator):

    logger = logger
    charset = 'utf8'
    cursor_class = DefaultCursor
    _primary_key_cache = dict()  # 用来缓存表的主键

    def _set_conn_var(self, **kwargs):
        if kwargs.get('conn_cmd', None):
            connargs = self.get_conn_args_from_str(kwargs['conn_cmd'])
            del kwargs['conn_cmd']
        else:
            connargs = {'user': 'root', 'port': 3306, 'charset': self.charset}
        connargs.update(kwargs)

        connargs['port'] = int(connargs['port'])
        self.host = connargs['host']
        self.user = connargs['user']
        self.port = int(connargs['port'])
        self.password = connargs['password']
        self.charset = connargs.get('charset', 'utf8')
        db = connargs.get('db', None)
        self.database = db if db else connargs['database']
        return connargs

    def _choose_cursor_class(self, connargs: dict):
        """
        选择实例化连接时 选择的游标类型。 也可以在执行语句时自行选择。
        """
        cursor_class = DefaultCursor
        if 'cursorclass' in connargs.keys():
            cursor_class = connargs['cursorclass']
        else:
            if 'cursor_type' in connargs.keys():
                cursor_type = connargs.get('cursor_type', None)
                del connargs['cursor_type']
                assert isinstance(cursor_type, str), "cursor_type must in (ss,dcit,ssdict)  "
                cursor_type = cursor_type.lower()
                if cursor_type == 'ss':
                    cursor_class = SSCursor
                elif cursor_type == 'dict':
                    cursor_class = DictCursor
                elif cursor_type == 'ssdict':
                    cursor_class = SSDictCursor
        connargs['cursorclass'] = cursor_class
        self.cursor_class = cursor_class
        return connargs

    def __init__(self, **kwargs):
        connargs = self._set_conn_var(**kwargs)
        self.connargs = self._choose_cursor_class(connargs)
        self.connector = pymysql.connect(**connargs)

    def get_conn_args_from_str(self, string):
        """
        @string : mysql-client命令  mysql -h127.0.0.1 -p1234  -uroot -p123456 -Dtest_base 
        返回 参数字典   { "host":"127.0.0.1","port":1234,"password":"123456","db":"test_base"  }
        """
        conn_args = dict()
        patter_dict = {
            "host": "-h\s*?([\d\.]+)",
            "port": "-P\s*?(\d+)",
            "user": "-u\s*?(\S+)",
            "password": "-p\s*?(\S+)",
            "database": "-D\s*?(\S+)",
        }
        for k, v in patter_dict.items():
            result = re.findall(v, string)
            number = len(result)
            if number == 1:
                conn_args[k] = result[0]
            else:
                if k == 'port':
                    conn_args[k] = 3306
                elif k == 'user':
                    conn_args[k] = 'root'
                elif k == 'host':
                    conn_args[k] = 'localhost'
                else:
                    raise ValueError("解析连接命令失败，确认命令符合 mysql -uxxx -Pxxxx -pxxxx -hxxxx -Dxxxx 格式 ")

        return conn_args

    def _get_connection(self):
        return self.connector

    def _get_cursor(self, connnetion, cursor_type=None):
        if not cursor_type:
            return connnetion.cursor(self.cursor_class)
        if cursor_type == 'dict':
            return connnetion.cursor(DictCursor)
        elif cursor_type == 'ss':
            return connnetion.cursor(SSCursor)
        elif cursor_type == 'ssdict':
            return connnetion.cursor(SSDictCursor)
        return connnetion.cursor(DefaultCursor)  # default

    def read_ss_result(self, sql, param: Union[tuple, dict, List[tuple], List[dict]] = None, cursor_type: str = 'ss'):
        """
        读取流式游标的结果
        """
        conn = self._get_connection()
        assert cursor_type.lower() in ('ss', 'ssdict'), "此处只支持流式游标 ss或ssdict"
        cursor = self._get_cursor(conn, cursor_type)
        try:
            count = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)
            # conn.commit()
            result = cursor.fetchone()
            while result is not None:
                yield result
                result = cursor.fetchone()
            cursor.close()
        except Exception as e:
            conn.rollback()
            traceback.print_exc()
            return OperationError(e)

    def execute_sql(self, sql: str, param: Union[tuple, dict, List[tuple], List[dict]] = None, cursor_type: str = None,show_error=True):
        """
        核心方法,执行sql
        # cursor_type为游标类型（默认返回值为元祖类型），可选字典游标，将返回数据的字典形式
        # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        注意此方法 返回两个值
        如果参数是列表类型，则使用executemany方法 
        """
        conn = self._get_connection()
        cursor = self._get_cursor(conn, cursor_type)
        result = count = False
        try:
            count = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)  # 得到受影响的数据条数
            conn.commit()
            result = cursor.fetchall()  # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
        except Exception as e:
            if show_error:
                self.logger.warning("---------------------------------")
                self.logger.error(str(e))
                self.logger.error(sql)
                # self.logger.error(param) #参数太长了，不要打印
                self.logger.error(traceback.format_exc())
                self.logger.info("---------------------------------")
            conn.rollback()
            result = count = OperationError(e)
        finally:
            return result, count

    def execute_with_return_id(self, sql: str, param: Union[tuple, dict, List[tuple], List[dict]] = None):
        """
        此方法会返回插入的最后一行的id
        """
        conn = self._get_connection()
        cursor = self._get_cursor(conn)
        result = False
        try:
            cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)
            cursor.execute("SELECT LAST_INSERT_ID() AS id")
            result = cursor.fetchall()[0][0]
            conn.commit()
        except Exception as e:
            self.logger.info("---------------------------------")
            self.logger.error(sql)
            # self.logger.error(param)
            self.logger.info("---------------------------------")
            conn.rollback()
            traceback.print_exc()
            result = OperationError(e)
        finally:
            return result

    def do_transaction(self, sql_params: List[Tuple[str, tuple]], cursor_type: str = None):
        """
        执行事务：传入sql和params 列表     ,如下
        [  
          (  'insert into a (`id`) values(%s)',     (1,)   ),
          ('update a set `id`= %s' where `id`=%s',   (2,3)  )  
        ]
        sql_params 内的元素类型为 tuple  对应 ---> （sql,params）  ， 其中 如果params 类型为list，则会使用启用游标的executemany 去执行
        返回最后一条sql的执行结果
        """
        conn = self._get_connection()
        cursor = self._get_cursor(conn, cursor_type=cursor_type)
        result = count = False
        try:
            for sql, param in sql_params:
                count = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)
            result = cursor.fetchall()
            conn.commit()
        except Exception as e:
            conn.rollback()
            traceback.print_exc()
            result = count = OperationError(e)
        finally:
            return result, count

    def select(self, columns: Union[Tuple[str], List[str], str] = 'id', table: str = None, where: str or dict = None,
               group_by: str = None, order_by: str = None, limit: int = None, offset: int = None, **kwargs):
        """
        仅支持 简单的查询
        """
        sql, param = self.generate_select_sql(columns, table, where, group_by, order_by, limit, offset)
        return self.execute_sql(sql, param, **kwargs)[0]

    def insert_into(self, table: str,  data: dict or List[dict], columns: Union[Tuple[str], List[str], str] = None, ignore=False, on_duplicate_key_update: str = None, return_id=False, **kwargs):
        """
        @data: 字典或字典列表（批量插入）  
        @columns: 哪些字段需要被插入。默认是传入的data的所有键。当data中有多余字段时，可以通过columns指定哪些字段需要作为新数据的字段插入
        @ignore: 是否  insert ignore into 
        @on_duplicate_key_update:出现 重复时的处理 ，如  插入一条数据出现重复时，令name字段等于“重复“   则传入 on_duplicate_key_update= ”name='重复'“  即可
        @return_id:  True ->返回最后一条插入语句的id   ，默认False 返回受影响的条数
        """
        sql, param = self.generate_insert_sql(table, data, columns, ignore, on_duplicate_key_update)
        return self.execute_with_return_id(sql, param, **kwargs) if return_id else self.execute_sql(sql, param, **kwargs)[1]

    def replace_into(self, table: str, data: dict or list, columns: Union[Tuple[str], List[str], str] = None, **kwargs):
        """
        将传入的字典或字典列表 replcae into 
        返回受影响的行数
        @columns: 限定影响的字段
        """
        sql, param = self.generate_replace_into_sql(table, data, columns)
        return self.execute_sql(sql, param, **kwargs)[1]

    def update_by_primary(self, table: str, data: dict, pri_value, columns=None, cache=True,**kwargs):
        """
        通过主键去更新
        @pri_value :主键的值
        @data: 更新的目标值。传入的字典将会转化为  update xxx set  key=value,key2=value2 的 形式
        @cache: 是否缓存表的主键，避免频繁查询表的主键
        @columns: 限定影响的字段
        返回受影响的条数（这里正常只会返回 1或0)
        """
        primary = self._get_primary_key(table, cache)
        sql, param = self.generate_update_sql_by_primary(table, data, pri_value, columns, primary)
        return self.execute_sql(sql, param,**kwargs)[1]

    def update(self, table: str, data: dict, where: Union[dict, str], columns: Union[Tuple[str], List[str], str] = None, limit: int = None,**kwargs):
        """
        @data:  要被更新的数据，传入字典将会转化为  update xxx set  key=value,key2=value2 的 形式
        @columns: 限定被影响的字段。默认为空，即不限定。则传入的data字典的所有键值对都会被映射为字段和值。
        @where: where 条件，使用该条件去找出哪些数据要被更新。
                如果where 为 字典，则根据字典映射成  where key=value
                如果是str，则直接将改该字符串拼接到 where 关键字之后
                如果是 可迭代对象，则会将该对象中的所有元素key对应的 data[key] 取出，作为where条件的映射。
        例如 data={"name":"jack","age":18,"gender":0}  ,where =["name",] ,columns=["age","gender"] 
            -->  update  xx set `age`=18,`gender`=0 where `name`="jack"
        """
        sql, param = self.generate_update_sql(table, data, where, columns, limit)
        return self.execute_sql(sql, param,**kwargs)[1]

    def delete_by_primary(self, table: str, pri_value, cache=True,**kwargs):
        """
        通过主键删除数据，这里要先查出主键，不允许指定，即使大部分场景下主键是id。
        @pri_value :主键的值
        @cache: 是否缓存表的主键，避免频繁查询表的主键
        返回受影响的条数（这里正常只会返回 1或0)
        """
        primary = self._get_primary_key(table, cache)
        sql = "DELETE FROM {} WHERE `{}`=%s ".format(table, primary)
        return self.execute_sql(sql, (pri_value,),**kwargs)[1]

    def _get_primary_key(self, table: str, cache=True):
        """
        获取表的主键
        @cache： 是否缓存该表的主键
        """
        primary = None
        if cache:
            primary = self._primary_key_cache.get(table, None)
        if not primary:
            sql = "SELECT column_name FROM INFORMATION_SCHEMA.`KEY_COLUMN_USAGE` WHERE table_name='{}' AND constraint_name='PRIMARY'".format(table)
            result = self.execute_sql(sql, cursor_type='default')
            primary = result[0][0][0]
            if cache:
                self._primary_key_cache[table] = primary
        return primary

    def delete(self, table: str, where: str or dict, limit: int = None, **kwargs):
        """
        根据传入的条件 删除对应的数据
        必须传入条件，避免因为漏参数而删除整个表。
        返回受影响的条数 
        """
        sql, param = self.generate_delete_sql(table, where, limit, **kwargs)
        return self.execute_sql(sql, param, **kwargs)[1]

    def get_multiqueries(self, sql: str or list, params: Union[tuple, dict, List[tuple], List[dict]] = None, cursor_type=None):
        """
        同时执行多个sql ，一次性返回所有结果集.但是需要在初始化连接时，引入CLIENT.MULTI_STATEMENTS 作为连接参数实例化连接
            from pymysql.constants import CLIENT
            client_flag=CLIENT.MULTI_STATEMENTS
            connargs={"client_flag":client_flag}
            conn=MysqlConnection(**connargs)
        @sql： 以分号； 进行连接的多条语句 或 sql 列表
        @params: sql对应的参数列表
        @cursor_type : 游标类型
        """
        assert self.connargs.get('client_flag', None) == CLIENT.MULTI_STATEMENTS, "使用该方法必须在实例化连接时引入client_flag参数。参考:https://blog.csdn.net/qq_33634196/article/details/119883045"
        conn = self._get_connection()
        if isinstance(sql, list):
            sql = ';'.join(sql)
        if isinstance(cursor_type, str):
            assert cursor_type.lower() in ('default', 'dict', None), "仅支持默认游标(default)和字典游标(dict)"
        cursor = self._get_cursor(conn, cursor_type=cursor_type)  # 此处由于需要返回查询结果集，所以不支持流式游标
        try:
            cursor.execute(sql, params)
            results = []
            results.append(cursor.fetchall())
            while cursor.nextset():
                results.append(cursor.fetchall())
            conn.commit()  # 这个要在最后进行提交
            return results
        except Exception as e:
            cursor.close()
            conn.rollback()
            traceback.print_exc()
            return OperationError(e)


class MysqlConnection(MysqlSqler, _SimpleConnector):

    charset = "utf8mb4"

    def merge_into(self, table: str, data: dict or List[dict], columns: Union[Tuple[str], List[str], str] = None, merge_columns: Union[Tuple[str], List[str], str] = None, **kwargs):
        """
        合并数据。mysql 不支持原生的merge into。这里通过 insert into ... on duplicate key update ...来实现
        @data:需要被合并的数据
        @columns:限定被影响的字段。默认是传入的data中的所有字段被会被影响
        @merge_columns:产生重复时，需要被更新的字段，默认是全部受影响的字段都被覆盖
        返回受影响的条数
        """
        sql, param = self.generate_merge_sql(table, data, columns, merge_columns, **kwargs)
        return self.execute_sql(sql, param, **kwargs)[1]

    @property
    def tables(self):
        if hasattr(self, '_tables'):
            return self._tables
        all_tables, _ = self.execute_sql("show tables")
        setattr(self, '_tables', tuple(d[0] for d in all_tables))
        return self._tables


class _SimplePoolConnector(MysqlConnection):
    _creator = None

    def _init_connargs(self, **kwargs):
        args_dict = self._set_conn_var(**kwargs)
        # 默认参数，参照 sqlalchemy 的连接池参数，进行初始化配置,（除了 SET AUTOCOMMIT = 1 之外，因为这个类的事务都是底层手动提交的，只是看起来和自动提交一样)
        # <module 'pymysql' from '/Library/Python/3.8/site-packages/pymysql/__init__.py'> 1 20 0 0 False None ['SET AUTOCOMMIT = 1'] True None 1
        connargs = {"host": self.host, "user": self.user, "password": self.password, "database": self.database, 'port': self.port, "charset": self.charset,
                    "creator": self._creator, "mincached": 1, "maxcached": 20, "blocking": False}

        # mincached : 启动时开启的空连接数量(0代表开始时不创建连接)
        # maxcached : 连接池最大可共享连接数量(0代表不闲置连接池大小)
        # maxshared : 共享连接数允许的最大数量(0代表所有连接都是专用的)如果达到了最大数量,被请求为共享的连接将会被共享使用
        # maxconnecyions : 创建连接池的最大数量(0代表不限制)
        # blocking : 达到最大数量时是否阻塞(0或False代表返回一个错误<toMany......>; 其他代表阻塞直到连接数减少,连接被分配)
        # maxusage : 单个连接的最大允许复用次数(0或False代表不限制的复用).当达到最大数时,连接会自动重新连接(关闭和重新打开)
        # setsession : 用于传递到数据库的准备会话，如 [”set name UTF-8″]
        connargs.update(args_dict)
        return connargs

    def __init__(self, *args, **kwargs):
        self.connargs = self._init_connargs(*args, **kwargs)
        self.connection_pool = PooledDB(**self.connargs)

    def _get_connection(self):
        return self.connection_pool.connection()


class MysqlPool(_SimplePoolConnector, MysqlConnection):

    port = 3306
    _creator = pymysql

    def __init__(self, **kwargs):
        connargs = self._init_connargs(**kwargs)
        self.connargs = self._choose_cursor_class(connargs)
        self.connection_pool = PooledDB(**self.connargs)
