import sys,os.path
abspath = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(".."))
from sqlgenerator import *
from connector import MysqlPool,MysqlConnection
import  time


def func(*args, **kwargs):
    print("*args", *args)
    print("args", args)
    print("**kwargs", **kwargs)
    print("kwargs", kwargs)
    




if __name__ == "__main__":
    connargs = {"host": '81.71.26.183', "user": 'root', "password": 'HandsomeBoyMysql', "database": 'surecan', 
                     "mincached": 30, "maxcached": 80, "maxshared": 50, "maxconnections": 10, "blocking": True, "maxusage": 0}
        
    conn = MysqlPool(**connargs)
    #result,count=conn.execute_sql("select  * from  test_table where age=%s ",param=(25,))
    # print(result)
    # print(count)
    #result,count=conn.execute_sql("select  * from  test_table where age=%s ",param=(25,))
    #result, count = conn.execute_sql("select  * from  test_table where age=%(age)s ", param={'age': 25})
    time.sleep(10)
    result, count = conn.select('*', 'test_table')
    #print(result, count)
    #conn.select("age,name", 'test_table', where={'age': 25, 'id': 2})
    #print(result,count)
    #result, count = conn.select(['age', 'name'], 'test_table', where={'age': 25, 'id': 2})
    #result, count = conn.select('*', 'test_table', order_by='age desc',group_by='id',limit=1,offset=1)
    #print(result, count)
    data_1 = {"age": 44, 'name': "雷cpy"}
    #count=conn.update('test_table',data_1,where={'id':22539})
    #count = conn.delete('test_table', where={'name': '雷东宝'})
    #count=conn.delete_by_primary('test_table',pri_value=22539)
    #count = conn.insert_into('test_table', data_1,on_duplicate_key_update=' name="雷copy" ')
    #print(count)
    # return_id = conn.insert_into('test_table', data_1,return_id=True)
    # print(return_id)
    
    result=conn.read_ss_result("select * from test_table")
    for data in result:
        print(data)
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

    