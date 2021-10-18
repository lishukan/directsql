#! /usr/bin/env python3
# -*- coding: utf-8 -*-
from typing import Iterable


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

    @staticmethod
    def get_columns_and_params(condition: dict, equal=False, and_join=False):
        """
        传入字典，返回 字段和参数
        equal 为True 时 加上 =%s , --->  update  xx set a=%s,b=%s.....
        and join  用于 where  a=%s AND B=%s 

        """
        join_str = ',' if not and_join else ' AND '
        key_s, params = zip(*condition.items())
        if equal:
            columns_condi = join_str.join(['`' + k + '`' + '=%s' for k in key_s])
        else:
            columns_condi = join_str.join(['`' + k + '`' for k in key_s])
        return columns_condi, tuple(params)

    @classmethod
    def generate_select_sql(cls, columns='id', table=None, where=None, group_by: str = None, order_by: str = None, limit: int = None, offset: int = None):
        """
        设要查询两个字段id和name，则 columns_u_need 为（'id','name'） / ['id','name']  /  'id,name'
        要查询所有字段 使用  columns_u_need='*' 即可

        where 为字典 或字符串

        """
        columns = columns if isinstance(columns, str) else ','.join(columns)

        sql = "SELECT {} from `{}` ".format(columns, table)
        params = None
        if where:
            if isinstance(where, dict):
                where_str, params = cls.get_columns_and_params(where, equal=True, and_join=True)
            else:
                where_str = where
            sql += "where {}".format(where_str)

        for key, fs in ((group_by, 'group by'), (order_by, 'order by'), (limit, 'limit'), (offset, 'offset')):
            if key:
                sql += ' {} {}'.format(fs, key)

        return sql, params

    @classmethod
    def _get_after_format_sql(cls, init_sql, table, data, columns: tuple or list = None):
        """
        生成sql语句里需要插入的字段和对应的格式化符号
        @columns: 需要格式化的字段。默认去第一个传入的字典数据的键值对
        """
        if isinstance(data, dict):
            if not columns:
                columns = data.keys()
        else:
            if not columns:
                columns = data[0].keys()

        format_tags = ','.join(('%({})s'.format(col) for col in columns))
        final_sql = init_sql.format(table, '`'+'`,`'.join(columns)+'`', format_tags)
        return final_sql, data

    @classmethod
    def generate_insert_sql(cls, table, data: dict or list, columns: tuple or list = None, ignore=False, on_duplicate_key_update: str = None):
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
    def generate_replace_into_sql(cls, table, data: dict or list, columns: tuple or list = None):
        """
        columns  字段顺序。如果不传入，则取第一个data的 键集合
        """
        sql = 'REPLACE INTO `{}` ({}) VALUES ({}) '
        return cls._get_after_format_sql(sql, table, data, columns)

    @classmethod
    def generate_update_sql_by_primary(cls, table, data: dict, pri_value, columns: tuple or list = None, primary: str = 'id'):
        """
        更新单条数据,condition为字典形式的数据
        columns 为 可迭代对象 list/tuple/set/...
        """
        sql = 'UPDATE `{}` SET {} WHERE `{}`=%s'
        if not columns:
            columns = data.keys()

        param = tuple(data[k] for k in columns)
        columns_condi = ','.join(['`' + k + '`' + '=%s' for k in columns])

        sql = sql.format(table, columns_condi, primary)
        param += (pri_value,)
        return sql, param

    @classmethod
    def generate_update_sql(cls, table, data: dict, condition: str or dict or Iterable, columns: tuple or list = None, limit=None):
        """
        @data:新数据    例如  data= {'name':'jack', 'age':18,'school':'MIT'  }  --> update xx set name='jack',age=18,school='MIT' 
        @condition：   为dict时，会根据这个dict 转换为对应的where条件。 如传入 {'age':24}     --->  update xx set name='jack',age=18,school='MIT' where age=24
                     --> tuple or list ... : 把参数里的键值对从data 中取出 组成where条件          ('age',) / ['age']      --->  update xx set name='jack',school='MIT' where age=18 
                     --> str :  age=88   update xx set name='jack',age=18,school='MIT' where age=88
        更新必须传入条件，避免漏传条件导致全表被更新
        """
        sql = 'UPDATE `{}` SET {} WHERE {}'
        if not columns:  # 需要更新的字段
            columns = data.keys()
        set_tags = ','.join(('`{}`=%s'.format(col, col) for col in columns))
        param = tuple(data[k] for k in columns)
        if isinstance(condition, str):
            condition_keys = set()
            where_tags = condition
            condition_param = ()
        else:
            if isinstance(condition, dict):
                condition_keys = set(condition.keys())
                condition_param = tuple(condition[k] for k in condition_keys)
            else:
                condition_keys = set(condition)
                condition_param = tuple(data[k] for k in condition_keys)

            where_tags = ' AND '.join(('`{}`=%s'.format(col) for col in condition_keys))

        param += condition_param
        if limit:
            param += (limit,)

        final_sql = sql.format(table, set_tags, where_tags)
        return final_sql, param

    @classmethod
    def generate_delete_sql(cls, table, where: str or dict, limit: int = None):
        sql = "DELETE FROM `{}` WHERE {} "
        if isinstance(where, dict):
            where_str, params = cls.get_columns_and_params(where, equal=True, and_join=True)
        else:
            where_str, params = where, None
        sql = sql.format(table, where_str)
        if limit:
            sql += "limit {}".format(limit)
        return sql, params


class MysqlSqler(SqlGenerator):

    def generate_merge_sql(self, table, data, columns: tuple or list = None,  merge_columns: tuple or list = None):
        """
        columns 为需要插入的字段
        merge_columns 为 出现重复时需要更新的字段.如果不给值，将会把所有 columns 里的字段都更新
        如果columns 都没有值，将会读取所有data的 键值对
        """
        if not columns:
            columns = data.keys() if isinstance(data, dict) else data[0].keys()

        format_tags = ','.join(('%({})s'.format(col) for col in columns))
        if not merge_columns:
            merge_columns = columns
        update_str = ','.join(['`{}`=values(`{}`)'.format(col, col) for col in merge_columns])
        sql = "INSERT INTO `{}` ({}) values({})  on duplicate key update {};"
        return sql.format(table, '`'+'`,`'.join(columns)+'`', format_tags, update_str), data
