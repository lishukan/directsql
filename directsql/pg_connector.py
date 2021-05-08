from .connector import SimplePoolConnector, PooledDB
# pip3 install psycopg2   or  pip3 install psycopg2-binary
import psycopg2 as pg2
import traceback
from .sqlgenerator import SqlGenerator
from .connector import   SimpleConnector
import re
from psycopg2.extras import  DictCursor,DictConnection
class PgSqlGenerator(SqlGenerator):
    """
    pgsql sql语句 拼接类
    """
    pass


class PgConnection(PgSqlGenerator, SimpleConnector):
    """
    pg 单个连接类
    """
    
    def __init__(self, **kwargs):
        self.connector = pg2.connect(**kwargs)

    def read_ss_result(self, sql, param=None, cursor_type='ss'):
        raise AttributeError("This function is not finish develop")
        #return super().read_ss_result(sql.replace('`','"'),param,cursor_type)
    
    def execute_sql(self, sql, param=None, cursor_type=None):
        sql=sql.replace('`','"')
        conn = self.get_connection()
        cursor = conn.cursor(cursor_factory=DictCursor) if cursor_type == 'dict' else conn.cursor()  #此处由于需要返回查询结果集，所以不支持流式游标
        
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

    