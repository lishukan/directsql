from re import split
import sys,os.path
abspath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(".."))
from directsql.sqlgenerator import *
from directsql.connector import MysqlPool,MysqlConnection
import  time


def func(*args, **kwargs):
    print("*args", *args)
    print("args", args)
    print("**kwargs", **kwargs)
    print("kwargs", kwargs)
    




if __name__ == "__main__":
    connargs =""""""
        
    # conn = MysqlPool(**connargs)
    #result,count=conn.execute_sql("select  * from  test_table where age=%s ",param=(25,))
    # print(result)
    # print(count)
    #result,count=conn.execute_sql("select  * from  test_table where age=%s ",param=(25,))
    #result, count = conn.execute_sql("select  * from  test_table where age=%(age)s ", param={'age': 25})
    #time.sleep(10)
    # result, count = conn.select('*', 'test_table')
    #print(result, count)
    #conn.select("age,name", 'test_table', where={'age': 25, 'id': 2})
    #print(result,count)
    #result, count = conn.select(['age', 'name'], 'test_table', where={'age': 25, 'id': 2})
    #result, count = conn.select('*', 'test_table', order_by='age desc',group_by='id',limit=1,offset=1)
    #print(result, count)
    # data_1 = {"age": 44, 'name': "雷cpy"}
    #count=conn.update('test_table',data_1,where={'id':22539})
    #count = conn.delete('test_table', where={'name': '雷东宝'})
    #count=conn.delete_by_primary('test_table',pri_value=22539)
    #count = conn.insert_into('test_table', data_1,on_duplicate_key_update=' name="雷copy" ')
    #print(count)
    # return_id = conn.insert_into('test_table', data_1,return_id=True)
    # print(return_id)
    
    # result=conn.read_ss_result("select * from test_table")
    # for data in result:
    #     print(data)
    # print(conn.database)
    # conn = MysqlPool(string_arg="mysql -uroot -h121.36.85.248 -P9024 -p123456  -Dspider_test")
    # print(conn.database)

    # conn_args = {
    #     'host': '121.36.85.248',
    #     'port': 9024,
    #     'password': '123456',
    #     'database':'spider_test',
    # }
    # conn = MysqlConnection(host='121.36.85.248', port=9024, password='123456', database='spider_test')
    # print(conn.database)
    # conn=MysqlPool(host='121.36.85.248', port=9024, password='123456', database='spider_test')
    # conn = MysqlConnection(**conn_args)
    # print(conn.database)
    # conn = MysqlPool(**conn_args)
    # print(conn.database)
    #=============================================================================================================
    from directsql.connector import MysqlPool,MysqlConnection
    from pymysql.constants import CLIENT
    client_flag=CLIENT.MULTI_STATEMENTS
    conn=MysqlConnection(string_arg="******",client_flag=client_flag)
    sql="""
    select id from JL_EXT_3402 limit 3;
    select count(1) from JL_EXT_3402;
    """
    #print(conn.execute_sql(sql))
    print(conn.get_multiqueries(sql))

    