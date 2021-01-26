from sqlgenerator import *
from connector import PoolConnector

# root@        HandsomeBoy666!
# mysql -h81.71.26.183 -uroot -pHandsomeBoyMysql
if __name__ == "__main__":
    #conn = SimpleConnector(host='81.71.26.183',database='surecan', password='HandsomeBoyMysql')
    conn = PoolConnector(host='81.71.26.183', user='root', password='HandsomeBoyMysql', database='surecan', mincached=1)

    # result, count = conn.query("select * from test_table limit 10", cursor_type='dict')
    # result, count = conn.execute_sql("select * from test_table limit 10", cursor_type='dict')
    # print(result, count)
    # sql, params = gentor.generate_insert_single_sql('test', {'name': 'lishukan', 'age': 18}, ignroe=True)
    # print(sql, params)
    # print(conn.execute_sql(sql,params))
    # data_list = [
    #    {'name': 'lishukan', 'age': 18},
    #    {'name': 'jackma', 'age': 22}
    # ]
    # sql, params = gentor.generate_insert_many_sql('test', data_list, ignore=True)
    # print(sql,params)
    # print(conn.execute_sql(sql,params))
    print(conn.select('*','test',group_by='name'))
    time.sleep(20)
    print(conn.select('*','test',group_by='name'))
    #print(conn.insert_into('test', {'name': 'xuzhihao', 'age': 24}))
    #print(conn.update_by_primary('test', {'name': 'xuzhihao', 'age': 987}, pri_value=17))
    #print(conn.update('test',{'name': 'sdsd', 'age': 87},condition={'age':24},columns_order={'name'} ))
    #print( conn.delete_by_primary('test',18)   )
    # print(conn.replace_into('test',[{'name':'aobama','id':14},{'name':'987','id':15}]))
