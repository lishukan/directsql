# directsql
​        一个简单的使用python操作mysql的工具，提供了一些类似sql语法的方法，最终拼接成sql。可以很好地处理一些常见场景，不依赖orm 的同时避免手写大量sql。 



## 安装

```shell
$ pip3 install directsql
```



## 导入

directsql   目前只提供三个外部类

```
__all__=["SqlGenerator","MysqlConnection","MysqlPool"]
```

导入方式

```python
from directsql.sqlgenerator import SqlGenerator   #该类用于生成sql语句

#下面是一个池化连接对象MysqlPool  和一个简单连接对象 MysqlConnector
from directsql.connector import MysqlConnection,MysqlConnector 

```



## 使用

### 1 创建连接

```python
 # 1. 传入有名参数
   
    conn = MysqlConnection(host='127.0.0.1', port=3306, password='123456', database='test_base')
    print(conn.database)
    conn=MysqlPool(host='127.0.0.1', port=3306, password='123456', database='test_base')
    
   # 也可使用 直接  参数字典
    conn_args = {
        'host': '127.0.0.1',
        'port': 3306,
        'password': '123456',
        'database':'test_base',
    }
    conn = MysqlConnection(**conn_args)#单个连接
    print(conn.database)
    conn = MysqlPool(**conn_args) #池化连接对象
    print(conn.database)
    
 #2 直接使用 字符串   
    #以下字符串是常用的终端 连接命令
    string_arg="mysql -uroot -h127.0.0.1 -P3306 -p123456  -Dtest_base"  
    conn = MysqlConnection(string_arg=string_arg)
    print(conn.database)
    conn = MysqlPool(string_arg=string_arg)
    print(conn.database)
   
    
```

### 2 执行sql语句

​         事实上directsql 封装了 很多 语句。可以满足很大一部分日常使用场景。但是如果有复杂的语句，仍然需要调用原生的sql 执行。而且directsql 中很多封装好的方法是先拼接sql  再 调用该语句，所以这里还是先简单介绍下,directsql 如何执行原生sql。

​         无论是   `MysqlConnection` 类 还是  `MysqlPool `类 都通过    execute_sql  方法 来执行sql。

例如 ：

| id   | name   | age  |
| ---- | ------ | ---- |
| 1    | 罗辑   | 28   |
| 2    | 庄颜   | 25   |
| 3    | 叶文洁 | 54   |
| 4    | 程心   | 25   |
| 5    | 云天明 | 27   |



```python
conn = MysqlConnection(string_arg="mysql -uroot -h127.0.0.1 -P3306 -p123456  -Dtest")
result,count=conn.execute_sql("select * from  test_table ")
print(result)
print(count)
>>> ((1, '罗辑', '28'), (2, '庄颜', '25'), (3, '叶文洁', '54'), (4, '程心', '25'), (5, '云天明', '27'))
>>> 5

 #这里默认是普通游标，你也可以指定使用字典游标：

result, count = conn.execute_sql("select  * from  test_table ", cursor_type='dict')

>>>[{'ID': 1, 'name': '罗辑', 'age': '28'}, {'ID': 2, 'name': '庄颜', 'age': '25'}, {'ID': 3, 'name': '叶文洁', 'age': '54'}, {'ID': 4, 'name': '程心', 'age': '25'}, {'ID': 5, 'name': '云天明', 'age': '27'}]
>>>5
```

***<font color="red"> execute_sql 方法 返回的是一个元组，（结果集，条数）</font>***

下文出现的所有方法无特殊说明都是返回元组，且支持dict游标

**附带参数执行语句**

这里的参数使用起来和 pymysql 提供的 execute 以及executemany 没有任何 差别，以下简单提供几个示例：

```python
#传元组
result,count=conn.execute_sql("select  * from  test_table where age=%s ",param=(25,))
#传字典
result, count = conn.execute_sql("select  * from  test_table where age=%(age)s ", param={'age': 25})

#元组列表
result, count = conn.execute_sql("insert into  test_table(`age`,`name`)values(%s,%s) ", param=[('宋运辉', 37), ('程开颜', 33)])

#字典列表
result, count = conn.execute_sql("insert into  test_table(`age`,`name`)values(%(age)s,%(name)s) ",
param=[ {"name":"宋运辉",'age':37}, {"name":"程开颜",'age':33} ])

```



### 3  <font color="green"> **select**</font> 方法

select 方法 可以接受多参数，参数列表如下。 

```python
def select(self, columns='id', table=None, where=None, group_by: str = None, order_by: str = None, limit: int = None, offset=None,cursor_type=None):
```

示例：conn 为连接实例

- select * from test_table

​       》》》` conn.select('*', 'test_table')`

- select  id from test_table where age=25

  》》》 `conn.select('*', 'test_table', where={'age': 25})`

- select  name,age from test_table where  age=25 and id=2

   多字段直接传入字符串

  》》》 `conn.select("age,name", 'test_table', where={'age': 25,'id':2})`

  传入列表/元组

  》》》` conn.select(['age','name'], 'test_table', where={'age': 25,'id':2})`

- select * from test_table group by id  order by age desc limit 1 offset 1

  》》》`conn.select('*', 'test_table', order_by='age desc',group_by='id',limit=1,offset=1)`

​    select 功能看起来甚至不如直接写原生sql 快，但是如果查询条件是在不断变化的，尤其是where条件，那么使用select 方法 会比自行拼接更方便。

​           例如，需要不断地读取一个字典变量，然后根据这个变量中的条件去查询数据，而这个字典的键个数会变化，但是键都恰好是表的字段。这个时候使用select 方法会十分简便，只需要令where参数等于那个字典即可。

​         平心而论，这个方法确实用处不大。

### 4 <font color="green"> **insert_into**</font> 方法

```python
def insert_into(self, table, data: dict or list, columns=None, ignroe=False, on_duplicate_key_update: str = None, return_id=False):
```

该方法可以接受传入字典或者 字典列表，并且可选 返回 游标影响的条数 或者是 新插入的数据的id。

<font color="red">columns 为空时，将取第一条数据的所有键，此时请确保所有数据键相同。</font>

```python
#传入 字典
data_1 = {"age": 44, 'name': "雷东宝"}
count = conn.insert_into('test_table', data_1)#默认返回受影响条数
print(count) #
>>> 1 
return_id = conn.insert_into('test_table', data_1,return_id=True)# 可选返回id
print(return_id)
>>>22533   

#传入字典列表
data_2={"age": 22, 'name': "宋运萍"}
all_data=[data_1,data_2]
count = conn.insert_into('test_table', all_data)

#限定 插入的字段。（字典有多字段，但是只需要往表里插入指定的字段时）
data_3= {"age": 44, 'name': "雷东宝","title":"村支书"} #title不需要,只要age和name
count = conn.insert_into('test_table', data_1,columns=["age","name"] )


#ignore 参数
data_1 = {"age": 44, 'name': "雷东宝","id":22539}
count = conn.insert_into('test_table',ignore=True )
print(count)
>>> 0   # 由于表中id 22539 已经存在，该条记录不会插入，影响 0条数据


#on_duplicate_key_update  参数
data_1 = {"age": 44, 'name': "雷东宝","id":22539} #id=22539 已经存在
count = conn.insert_into('test_table', data_1,on_duplicate_key_update=' name="雷copy" ')
print(count)#返回影响条数
>>>2      #尝试插入一条，但是发生重复，于是删除新数据，并更新旧数据。实际上影响了两条。



```

在insert_into 方法中提供了 on_duplicate_key_update 参数，但是实际上使用起来比较鸡肋，需要自己传入 on_duplicate_key_update 后的语句进行拼接。

如果你仅仅只是需要在发生重复时将旧数据的特定字段更新为新数据对应字段的值时。merge_into 方法更适合。



### 5<font color="green"> **merge_into**</font> 方法

在 其他关系型数据库中，提供有merge into 的语法，但是mysql 中没有提供。 不过这里我们通过insert 和  on_duplicate_key_update 语法 封装出了一个 类似merge_into 的方法。 该方法返回的是影响的条数

```def* merge_into(self, table, data, columns=None, need_merge_columns: list = None):```

<font color="red">columns 为空时，将取第一条数据的所有键，此时请确保所有数据键相同。</font>

need_merge_columns 为在发生重复时需要替换（覆盖）的字段。

```python
data_1 = {"age": 44, 'name': "雷东宝","id":22539}
data_2={"age": 22, 'name': "宋运萍","id":22540}
all_data = [data_1, data_2,]
count=conn.merge_into('test_table',all_data,need_merge_columns=['name',])
print(count)
>>>4        #两条数据正好都是重复的，插入两条又删除后修改两条 ，返回4
```



### 6<font color="green"> **replace_into**</font> 方法

该方法简单，不做过多说明。该方法 返回的是影响的条数

```def replace_into(self,table, data: dict or list, columns=None)```



```python
data_1 = {"age": 44, 'name': "雷东宝","id":22539}
data_2={"age": 22, 'name': "宋运萍","id":22540}
all_data = [data_1, data_2,]
count=conn.replace_into('test_table',all_data)
```



### 7<font color="green"> **update**</font> 方法

```def update(self,table, data: dict, where, columns: None or list = None, limit=None):```

<font color='red'> 该方法data 参数只接受传入字典。</font>该方法 返回的是影响的条数

```python
data_1 = {"age": 44, 'name': "雷copy"}
count=conn.update('test_table',data_1,where={'id':22539}) #更新 id=22539的数据为 新的data_1
print(count)
>>>1  
```

除此之外，还提供了一个衍生的方法  

```def update_by_primary(self, table, data: dict, pri_value, columns=None, primary: str = 'id'):```

用于通过主键去更新数据。pri_value 即为主键的值。primary 为主键，默认为id

```python
data_1 = {"age": 44, 'name': "雷cpy"}
count=conn.update_by_primary('test_table',data_1,pri_value=22539)

```



### 8 <font color="green">delete </font>方法

```python
def delete_by_primary(self, table, pri_value, primary='id'):
	"""
	通过主键删除数据
	"""

def delete(self,table, where: str or dict, limit: int = 0):
	"""
	通过where条件删除数据
	"""


count=conn.delete('test_table',where={'name':'雷东宝'})          #删除name=雷东宝的数据
count=conn.delete_by_primary('test_table',pri_value=22539)         #删除主键等于22539 的数据

```



### 9 使用 事务

```def do_transaction(self, sql_params: list, cursor_type=None):```



sql_params 为 元组列表。 【（sql_1,param_1），（sql_2,param_2】

如果sql 不需要参数也要传入 None ，如  【（sql_1,None），】

```
sql_params = [
        ("update test_table set name=%(name)s where  id=%(id)s ", {'name': '洛基', 'id': 22539}),
        ("update test_table set name=%(name)s where  id=%(id)s ", {'name': 'mask', 'id': 22540}),
    ]
count=conn.do_transaction(sql_params)
>>>((), 1)          #返回最后一条执行语句的 结果和影响条数
```



### 10 读取流式游标结果

``` def read_ss_result(self, sql, param=None, cursor_type='ss'):```

cursor_type  可选 ss  和 ssdict 

<font color='red'>注意，该方法返回的是 生成器对象，拿到结果需要不断进行遍历。</font>

```python
result=conn.read_ss_result("select * from test_table")
for data in result:
	print(data)
```

