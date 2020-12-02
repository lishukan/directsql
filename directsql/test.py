from sqlhandler import SqlGenerator
from connector import SimpleConnection

# root@        HandsomeBoy666!
# mysql -h81.71.26.183 -uroot -pHandsomeBoyMysql
if __name__ == "__main__":
    conn = SimpleConnection(host='81.71.26.183', user='root',database='surecan', password='HandsomeBoyMysql')
    result, count = conn.query("select Id from test_table limit 10",cursor_type='dict')
    print(result,count)