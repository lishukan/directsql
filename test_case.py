"""
测试用例
"""
from directsql.connector import MysqlPool, MysqlConnection
import threading
import time
from directsql.sqlgenerator import *
from re import split
import sys
import os.path
from pymysql.constants.CLIENT import MULTI_STATEMENTS

#=============================================================================================================
"""
第一部分:连接/连接池创建
"""

connargs = {
    'host': 'xxxxx',
    'user': 'root',
    'port': 3306,
    'database': 'test',  # 支持db和databse
    'password': 'xxxxx',
    'cursor_type': 'dict',
    'client_flag': MULTI_STATEMENTS,
}
conn = MysqlConnection(**connargs)  # 使用字典参数实例化连接
# pool = MysqlPool(**connargs)  # 使用字典参数实例化池
# pool = MysqlPool(**connargs, mincached=1)  # 使用字典参数实例化池
# string_args = "mysql -h49.235.84.230 -uroot -pHandsomeBoyMysql -Dtest -P3306"
# conn = MysqlConnection(conn_cmd="mysql -h49.235.84.230 -uroot -pHandsomeBoyMysql -Dtest -P3306")  # 使用连接命令实例化连接
# pool = MysqlPool(conn_cmd="mysql -h49.235.84.230 -uroot -pHandsomeBoyMysql -Dtest -P3306")  # 使用连接命令实例化连接池

#=============================================================================================================
"""
第二部分:功能 
"""

# conn.insert_into('cb_user', {'username': 'test1', 'password_hash': 'P@ssw0rd'})
# conn.insert_into('cb_user', {'username': 'test2', 'password_hash': 'P@ssw0rd', '没用的字段': 'sdads'}, columns=['username', 'password_hash'])
print(conn.select('*', 'cb_user', cursor_type='dict'))
data = [
    {'name': "test8", "age": 4, "gender": 1},
    {'name': "test9", "age": 5, "gender": 0},
    {'name': "test2", "age": 6, "gender": 0, "垃圾字段": "阿斯顿"}
]
#指定要插入的字段
#
# print(conn.insert_into('cb_user', data, columns=["username", "password_hash"], on_duplicate_key_update=" username='重复的11',cinfirmed=2 "))
table = 'test_table'
#删除
# print(conn.delete('cb_user', where={"username": "重复的2"}))  # 删除 username=“重复的2” 的数据
# print(conn.delete('cb_user', where='username like "test%"', limit=2))  # 删除 username like "%test 的数据，并且限制只删除两条
# print(conn.execute_sql("show tables"))
# conn._get_primary_key('cb_user')
# print(conn.delete_by_primary('cb_user', 432))
# print(conn.insert_into(table, data))
# print(conn.replace_into(table, data))
#更新
# print(conn.update('test_table', {"name": 18}, where={"name": "test2"}))
# print(conn.update(table, {"name": "lishukan", "age": 25, "gender": 1}, where="name='18'"))
# print(conn.update(table, {"name": "lishukan", "age": 25, "gender": 0}, where={"name": "lishukan"}))
# print(conn.update(table, {"name": "lishukan", "age": 25, "gender": 1}, where=["name", ]))
# print(conn.update(table, {"name": "lishukan", "age": 26, "gender": 1, "垃圾字段": "sdas"}, where=["name", ], columns=["name", "age", "gender"]))

#合并
# print(conn.merge_into(table, data={"name": "lishukan", "age": 27, "gender": 0, "垃圾字段": "sdas"}, columns=["name", "age", "gender"], merge_columns=['gender', ]))


# print(conn.tables)

# sql = "show tables;select * from test_table limit 1"
# print(conn.get_multiqueries(sql=sql))


#=============================================================================================================
