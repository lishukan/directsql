#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import traceback
from typing import Iterable
import pymysql
import time
import random
import uuid
import logging
import sqlalchemy


class SqlGenerator(object):
    """
    该类下的方法的返回值必须为两个 
    第一个是sql，第二个是参数
    """

    @classmethod
    def get_all_column_sql(cls, table_name, dbname=None):
        """返回对应表的所有字段，形式为列表"""

        table_name = table_name if not dbname else dbname+"."+table_name
        sql = "select COLUMN_NAME from information_schema.COLUMNS where TABLE_NAME=%s order by COLUMN_NAME;"
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
    def generate_select_sql(cls, columns='id', table=None, where=None, group_by: str = None, order_by: str = None, limit: int = None, offset=None):
        """
        设要查询两个字段id和name，则 columns_u_need 为（'id','name'） / ['id','name']  /  'id,name'
        要查询所有字段 使用  columns_u_need='*' 即可

        where 为字典 或字符串

        """
        if isinstance(columns, (tuple, list)):
            columns = ','.join(columns)
        elif isinstance(columns, str):
            columns = columns
        else:
            raise TypeError('error ! colnmns_you_need must be str or tuple or list ')

        sql = "select {} from {} ".format(columns, table)
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
    def _get_after_format_sql(cls, init_sql, table, data, columns_order=None):
        if isinstance(data, dict):
            if not columns_order:
                columns_order = data.keys()
        else:
            if not columns_order:
                columns_order = data[0].keys()

        format_tags = ','.join(('%({})s'.format(col) for col in columns_order))
        final_sql = init_sql.format(table, ','.join(columns_order), format_tags)
        return final_sql, data

    @classmethod
    def generate_insert_sql(cls, table, data: dict or list, columns_order=None, ignroe=False, on_duplicate_key_update: str = None):
        """
        columns_order 为 可迭代对象 list/tuple/set/...
        插入单条数据,condition为字典形式的数据
        data 必须为字典 或者 为一个元素类型为字典的列表
        """
        sql = 'INSERT INTO {} ({}) VALUES ({})' if not ignroe else 'INSERT IGNORE INTO {} ({}) VALUES ({})'
        if on_duplicate_key_update:
            sql += (" on duplicate key update"+on_duplicate_key_update)
        return cls._get_after_format_sql(sql, table, data, columns_order)

    @classmethod
    def generate_replace_into_sql(cls, table, data: dict or list, columns_order=None):
        """
        columns_order  字段顺序。如果不传入，则取第一个data的 键集合
        """
        sql = 'REPLACE INTO {} ({}) VALUES ({}) '
        return cls._get_after_format_sql(sql, table, data, columns_order)

    @classmethod
    def generate_update_sql_by_primary(cls, table, data: dict, pri_value, columns_order=None, primary: str = 'id'):
        """
        更新单条数据,condition为字典形式的数据
        columns_order 为 可迭代对象 list/tuple/set/...
        """
        sql = 'UPDATE {} SET {} WHERE `{}`=%s'
        if not columns_order:
            columns_order = data.keys()

        param = tuple(data[k] for k in columns_order)
        columns_condi = ','.join(['`' + k + '`' + '=%s' for k in columns_order])

        sql = sql.format(table, columns_condi, primary)
        param += (pri_value,)
        return sql, param

    @classmethod
    def generate_update_sql(cls, table, data: dict, condition, columns_order: None or list = None, limit=None):
        """
        columns_order 为需要更新的字段
        data= {'name':'jack', 'age':18,'school':'MIT'  }
        condition 为dict时，会根据这个dict 转换为对应的where条件
        如果为其他类型，会将condition中的 key 在data上对应的值 取出，作为键值对并转换为where条件
        condition --> dict : {'age':24}                     --->  update xx set name='jack',age=18,school='MIT' where age=24
                  --> tuple or list or set :  ('age',) / ['age']/{'age'}    --->  update xx set name='jack',school='MIT' where age=18
                --> str :  age=88   update xx set name='jack',age=18,school='MIT' where age=88
        """
        sql = 'UPDATE {} SET {} WHERE {}'
        all_columns = data.keys()

        if isinstance(condition, str):
            condition_keys = set()
            where_tags = condition
        else:
            if isinstance(condition, dict):
                condition_keys = condition.keys()
                condition_param = tuple(condition[k] for k in condition_keys)
            else:
                condition_keys = set(condition)
                condition_param = tuple(data[k] for k in condition_keys)

            where_tags = ' AND '.join(('`{}`=%s'.format(col) for col in condition_keys))

        if not columns_order:  # 需要更新的字段
            columns_order = all_columns - condition_keys
        else:
            columns_order = set(columns_order)

        set_tags = ','.join(('`{}`=%s'.format(col, col) for col in columns_order))
        param = tuple(data[k] for k in columns_order)
        param += condition_param
        if limit:
            param += (limit,)
        final_sql = sql.format(table, set_tags, where_tags)
        return final_sql, param

    @classmethod
    def generate_delete_sql(cls, table, where: str or dict, limit: int = 0):
        sql = "DELETE FROM {} WHERE {} "
        if isinstance(where, dict):
            where_str, params = cls.get_columns_and_params(where, equal=True, and_join=True)
        else:
            where_str, params = where, None
        sql = sql.format(table, where_str)
        if limit:
            sql += "limit {}".format(limit)
        return sql, params


class MysqlSqler(SqlGenerator):

    def generate_merge_sql(self, table, data, columns=None, need_merge_columns: list = None):
        """
        columns 为需要插入的字段
        need_merge_columns 为 出现重复时需要更新的字段.如果不给值，将会把所有 columns 里的字段都更新
        """
        if isinstance(data, dict):
            if not columns:
                columns = data.keys()
        else:
            if not columns:
                columns = data[0].keys()

        format_tags = ','.join(('%({})s'.format(col) for col in columns))
        if not need_merge_columns:
            need_merge_columns = columns
        update_str = ','.join(['`{}`=values({})'.format(col,col) for col in need_merge_columns])
        sql = "insert into {} ({}) values({})  on duplicate key update {};"
        return sql.format(table, ','.join(columns), format_tags, update_str), data


