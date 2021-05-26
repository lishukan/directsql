from .connector import SimplePoolConnector, PooledDB
import psycopg2 as pg2 # pip3 install psycopg2   or  pip3 install psycopg2-binary
import traceback
from .sqlgenerator import SqlGenerator
from .connector import   SimpleConnector
import re
from psycopg2.extras import  DictCursor,DictConnection,RealDictCursor,RealDictConnection
class PgSqlGenerator(SqlGenerator):
    """
    pgsql sql语句 拼接类
    """
    @classmethod
    def generate_merge_sql(cls, table, data: dict or list, conflict: list or tuple, columns: tuple or list = None, need_merge_columns: list = None):
        """
       
        """
        if not columns:
            columns = data.keys()  if isinstance(data, dict) else data[0].keys()

        format_tags = ','.join(('%({})s'.format(col) for col in columns))
        if not need_merge_columns:
            need_merge_columns = columns
        update_str = ','.join([" \"{}\"=%({})s ".format(col, col) for col in need_merge_columns])
        
        sql = "INSERT INTO `{}` ({}) values({})  on conflict (`{}`) DO UPDATE SET {};".format(table,'`' + '`,`'.join(columns) + '`',format_tags,'`,`'.join(conflict),update_str)
        return sql, data
    
    @classmethod
    def generate_insert_sql(cls, table, data: dict or list, columns: tuple or list = None, conflict: list or tuple=None,do_nothing=False, on_conflict_do_update: str = None):
        """
        columns 为 可迭代对象 list/tuple/set/...
        插入单条数据,condition为字典形式的数据
        data 必须为字典 或者 为一个元素类型为字典的列表
        """
        if isinstance(data, dict):
            if not columns:
                columns = data.keys()
        else:
            if not columns:
                columns = data[0].keys()

        format_tags = ','.join(('%({})s'.format(col) for col in columns))

        if not conflict:
            sql = 'INSERT INTO `{}` (`{}`) VALUES ({})'.format(table,'`,`'.join(columns), format_tags )
        else:
            if  do_nothing:
                sql = 'INSERT  INTO `{}` (`{}`) VALUES ({}) on conflict(`{}`) DO NOTHING'.format(table,'`,`'.join(columns), format_tags, '`,`'.join(conflict))
            else:
                assert on_conflict_do_update
                sql = 'INSERT  INTO `{}` (`{}`) VALUES ({}) on conflict(`{}`) DO UPDATE SET '.format(table,'`,`'.join(columns), format_tags, '`,`'.join(conflict)) + on_conflict_do_update
                
        return sql,data


class PgConnection(PgSqlGenerator, SimpleConnector):
    """
    pg 单个连接类
    cursor_type 由初始化时设定
    """
    
    def __init__(self, **kwargs):
        self.connector = pg2.connect(**kwargs)

    def select(self, columns='id', table=None, where=None, group_by: str = None, order_by: str = None, limit: int = None, offset=None):
        """
        仅支持 简单的查询
        """
        sql, param = self.generate_select_sql(columns, table, where, group_by, order_by, limit, offset)
        return self.execute_sql(sql, param)


    def execute_with_return_id(self, sql, param=None):
        """
        此方法会返回插入的最后一行的id
        """
        conn = self.get_connection()
        cursor = conn.cursor()
        result = False
        try:
            r = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)
            conn.commit()
            return cursor.lastrowid
        except:
            self.logger.info("---------------------------------")
            self.logger.error(sql)
            #self.logger.error(param)
            self.logger.info("---------------------------------")
            conn.rollback()
            traceback.print_exc()
        finally:
            return result

    def insert_into(self, table, data: dict or list, columns: tuple or list = None, conflict: list or tuple=None,do_nothing=False, on_conflict_do_update: str = None,return_id=False):
        """
        on_conflict_do_update  为字符串  ,这里有个坑，  pgsql 字符串要用 单引号
        """
        
        sql, param = self.generate_insert_sql(table, data, columns, conflict,do_nothing, on_conflict_do_update)
        return self.execute_with_return_id(sql, param) if return_id else self.execute_sql(sql, param)[1]

    def read_ss_result(self, sql, param=None, cursor_type='ss'):
        raise AttributeError("This function is not finish develop")
        #return super().read_ss_result(sql.replace('`','"'),param,cursor_type)

    def execute_sql(self, sql, param=None, cursor_type=None):
        """
        执行单条语句
        """
        sql=sql.replace('`','"')  #上面的语句都是按照mysql 的转义字符来的，pg里统一换成 双引号
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor) if cursor_type == 'dict' else conn.cursor()  #此处由于需要返回查询结果集，所以不支持流式游标
        
        result = count = False
        try:
            cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)  # 得到受影响的数据条数
            conn.commit()
            count = cursor.rowcount
            
            if sql.startswith('SELECT'):
                result = cursor.fetchall()  # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
            else:
                result=()
        except :
            self.logger.info("---------------------------------")
            self.logger.error(sql)
            #self.logger.error(param)
            self.logger.info("---------------------------------")
            conn.rollback()
            traceback.print_exc()
        finally:
            return result, count

   

    def do_transaction(self, sql_params: list, cursor_type=None):
        """
        执行事务
        sql_params 内的元素类型为 tuple  对应 ---> （sql,params）  ， 其中 如果params 类型为list，则会使用启用游标的executemany 去执行
        """
        conn = self.get_connection()
        cursor = conn.cursor(DictCursor) if cursor_type == 'dict' else conn.cursor()
        result = count = False
        try:
            for sql, param in sql_params:
                count = cursor.executemany(sql, param) if isinstance(param, list) else cursor.execute(sql, param)

            if sql.startswith('SELECT'):
                result = cursor.fetchall()  # 此方法应直接返回 所有结果，不应去考虑fetchone还是fetchmany的问题。这是传入的sql中就应该限定的
            else:
                result=()
            conn.commit()
        except:
            conn.rollback()
            traceback.print_exc()
        finally:
            return result, count


    def merge_into(self, table, data: dict or list, conflict: list or tuple, columns: tuple or list = None, need_merge_columns: list = None):
        """
        conflict 必须指定 字段
        """
        sql, param = self.generate_merge_sql( table, data, conflict,columns, need_merge_columns)
        return self.execute_sql(sql, param)[1]

class PostgrePool(SimplePoolConnector,PgConnection):
    port = 5432
    """
    https://www.psycopg.org/docs/cursor.html?highlight=cursor#cursor
    psycopg2 游标使用文档
    pg  连接池类
    """
    
    _creator=pg2
    def __init__(self, *args, **kwargs):
        self.connargs=self._init_connargs(*args,**kwargs)
        del self.connargs['charset']#pgsql 不需要此参数
        self.connection_pool = PooledDB(**self.connargs)

    