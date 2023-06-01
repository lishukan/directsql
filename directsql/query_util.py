import re
import copy
from .connector import MysqlPool
"""
一个处理web请求的工具类，目的是方便在web开发中 调用查询sql。
"""

class MysqlQueryUtil(MysqlPool):

    datetime_range_pattern = {
        '~': re.compile('(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}~\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})'),
        ' - ': re.compile('(\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2}\s-\s\d{4}-\d{1,2}-\d{1,2}\s\d{1,2}:\d{1,2}:\d{1,2})'),
    }
    blank_pattern = re.compile('(\s)')

    @classmethod
    def generator_where_condition(cls, formdata: dict, exclude=('limit', 'sort', 'order_by', 'page'), **kwargs):
        """
        根据传入的表单 生成 where 条件的 sql部分
        返回 sql(where)语句和 参数列表
        @formdata :表单，其实就是一个条件字典。因此目前只支持 = in  时间范围   这三种查找
        @exclude :不参与 拼接where 语句的字段
        其他参数 ：
            1.where_conditions ： 列表，其中的元素类型为字符串 ，用于当where条件该类难以生成时，支持外部配置好，再传入。会合并进该方法的 where_conditions
            2. params :列表 ，为where条件对应的参数，用于在外部拼接特殊where条件时，传入参数。最终会和该方法的params 合并
        """
        where_conditions = [] if not kwargs.get('where_conditions') else kwargs.get('where_conditions')
        params = [] if not kwargs.get('params') else kwargs.get('params')
        fuzzy_search = kwargs.get('fuzzy_search', ())  # 支持模糊查找的字段
        for key, value in formdata.items():
            if exclude:
                if key in exclude:
                    continue
            if isinstance(value, str):
                value = value.strip()
                if cls.blank_pattern.findall(key):
                    raise ValueError("查询字段参数错误:字段中包含空格- {}".format(key))
                if ',' in value:
                    vals = value.split(',')
                    where_conditions.append(' `{}` in  ({})'.format(key, ','.join(['%s'] * len(vals))))
                    params.extend(vals)
                    continue
                elif value == '$notnull':
                    where_conditions.append(" `{}` is not null ".format(key))
                    continue
                elif value == '$null':
                    where_conditions.append(" `{}` is null ".format(key))
                    continue
                elif value == "$<>''":
                    where_conditions.append(" `{}` <> '' ".format(key))
                    continue
                elif value == "$''":
                    where_conditions.append(" `{}` = '' ".format(key))
                    continue
                elif fuzzy_search and key in fuzzy_search:
                    where_conditions.append(" `{}` like '%%{}%%' ".format(key, value))
                    continue
                else:
                    datetime_pattern = False
                    for split_text, pattern in cls.datetime_range_pattern.items():
                        if pattern.search(value):  # 时间范围字段,要符合特定格式才行
                            start_time, end_time = pattern.findall(value)[0].split(split_text)
                            where_conditions.append(" `{}` >= %s and `{}` <= %s ".format(key, key))
                            params.extend([start_time, end_time])
                            datetime_pattern = True
                            break
                    if datetime_pattern:
                        continue
            elif isinstance(value, list):
                where_conditions.append(' `{}` in  ({})'.format(key, ','.join(['%s'] * len(value))))
                params.extend(value)
                continue
            where_conditions.append(" `{}` = %s ".format(key))
            params.append(value)
        if where_conditions:
            return ' where ' + ' AND '.join(where_conditions), params
        else:
            return '', params

    @classmethod
    def generator_query_sql(cls, formdata: dict, table: str, **kwargs):
        """
        根据传入的表单拼接sql和 param
        @formdata : 表单数据。
        @table : 查询的表名
        分成三部分   select 头部  +  where 条件   + 排序和分页 部分

        @select 头部，可以通过传入字段列表  来限定  所需的字段。如 select=['id','name'] 或者 select=" `id`,`name` "。这样即只获取两个字段
        否则将按  select * 获取所有字段。
        """
        select = kwargs.get('select', '')
        if select:
            if isinstance(select, str):
                pass
            elif isinstance(select, (list, tuple)):
                select = " `" + "`,`".join(select) + "` "
        else:
            select = ' * '
        sql_head = "select {} from `{}` ".format(select, table)
        sort = formdata.get('sort', 'desc')
        if sort not in ('desc', 'asc'):
            raise ValueError("排序方向参数错误")
        order_by = formdata.get('order_by', 'id')
        if re.findall('(\s)', order_by):
            raise ValueError("排序字段参数错误:其中包含空格")  # 这里的防空格，实际上是为了避免sql注入。
        limit = int(formdata.get('limit', 20))
        page = int(formdata.get('page', 1))
        sql_tail = ' order by `{}` {} limit %s offset %s '.format(order_by, sort)
        params = []
        where_sql, params = cls.generator_where_condition(formdata, **kwargs)
        params.extend([limit, (page - 1) * limit])
        if where_sql:
            final_sql = sql_head + where_sql + sql_tail
        else:
            final_sql = sql_head + sql_tail
        return final_sql, tuple(params)

    @classmethod
    def generator_count_sql(cls, formdata: dict, table: str, **kwargs):
        """
        根据传入的表单拼接sql和 param，去查询该条件
        @formdata : 表单数据。
        @table : 查询的表名
        分成三部分   select 头部  +  where 条件   + 排序和分页 部分
        """
        sql_head = "select count(*)  as `count` from `{}` ".format(table)
        where_sql, params = cls.generator_where_condition(formdata, **kwargs)
        if where_sql:
            return sql_head + where_sql, tuple(params)
        else:
            return sql_head, tuple(params)

    def get_query_results(self, formdata: dict, table: str, **kwargs):
        """
        根据传入的表单，生成查询sql和统计的sql
        最终返回  总条数 和   分页数据
        这里用到了  同时查询多条sql 的 特殊驱动属性，减少一次网络io
        """
        formdata_origin = copy.deepcopy(formdata)
        for key, value in formdata_origin.items():  # 清除掉传入参数中为空的
            if value == '' or value is None:
                del formdata[key]
        query_params, count_params = tuple(), tuple()
        if not kwargs.get('query_sql', ''):
            query_sql, query_params = self.generator_query_sql(formdata, table, **kwargs)
        else:
            query_sql = kwargs.get('query_sql')
            query_params = kwargs.get('query_params')
        if not kwargs.get('count_sql', ''):
            count_sql, count_params = self.generator_count_sql(formdata, table, **kwargs)
        else:
            count_sql = kwargs.get('count_sql')
            count_params = kwargs.get('count_params')
        params = query_params + count_params
        results = self.get_multiqueries(';'.join([query_sql, count_sql]), params)
        if not results:
            return [], 0
        else:
            return results[0], results[1][0]['count'],

    def update_by_condition(self, table, data, where, **kwargs):
        """
        通过查询条件去更新某个表
        """
        where_copy = where.copy()
        for k, v in where_copy.items():
            if isinstance(v, list) and len(v) == 0:
                del where[k]
        where_sql, where_params = self.generator_where_condition(where, **kwargs)
        if where_sql.startswith(' where'):  # 空格打头的（上一步生成的sql里有where 打头的）
            where_sql = where_sql[6:]
        else:
            raise ValueError("批量更新必须传入筛选条件")
        sql, data_param = self.generate_update_sql(table, data, where_sql)
        last_param = data_param+tuple(where_params)
        return self.execute_sql(sql=sql, param=last_param)[1]