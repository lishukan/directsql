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
import  easymysql

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

        where_condition 为字典 或字符串

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
    def generate_insert_sql(cls, table, data: dict or list,columns_order=None, ignroe=False,on_duplicate_key:str =None):
        """
        columns_order 为 可迭代对象 list/tuple/set/...
        插入单条数据,condition为字典形式的数据
        """
        sql = 'INSERT INTO {} ({}) VALUES ({});' if not ignroe else 'INSERT IGNORE INTO {} ({}) VALUES ({});'
        if isinstance(data, dict):
            if not columns_order:
                columns_order=data.keys()
        else:
            if not columns_order:
                columns_order=data[0].keys()
            
        format_tags = ','.join(('%({})s'.format(col) for col in columns_order))
        final_sql = sql.format(table, ','.join(columns_order), format_tags)
        return final_sql, data

    @classmethod
    def generate_insert_many_sql(cls, table, data_list:list,columns_order=None,ignore=False,on_duplicate_key:str=None):
        """
        columns_order 为 可迭代对象 list/tuple/set/...
        columns_order 一般来说不需要传入，但是当data_list中的数据字段很多，而插入的字段是 指定的N个时。就需要指定需要插入的字段
        另外 可能会存在部分字段，一些数据没有。  fill_none =True 就会给不存在的字段赋值为None

        data_list 中的 数据必须为字典形式
        """
        sql = 'INSERT INTO {} ({}) VALUES ({});' if not ignore else 'INSERT IGNORE INTO {} ({}) VALUES ({});'
        if not columns_order:
            columns_order = data_list[0].keys()


        format_tags = ','.join(('%({})s'.format(col) for col in columns_order))

        final_sql=sql.format(table,','.join(columns_order),format_tags)
        return final_sql, data_list


    @classmethod
    def generate_update_sql_by_primary(cls, table, data:dict, pri_value, columns_order=None,primary:str='id', limit=1):
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
        param+=(pri_value,)
        if not limit:
            pass
        else:
            sql += ' limit %s;'.format(limit)
            param+=(limit,)
        return sql, param

    @classmethod
    def generate_update_sql(cls, table, data: dict, condition, columns_order:None or list=None, limit=None):
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
            where_tags=condition
        else:
            if isinstance(condition, dict):
                condition_keys = condition.keys()
                condition_param=tuple(condition[k] for k in condition_keys)
            else:
                condition_keys = set(condition)
                condition_param = tuple(data[k] for k in condition_keys)
                
            where_tags = ' AND '.join(('`{}`=%s'.format(col) for col in condition_keys))
            

        if not columns_order:#需要更新的字段
            columns_order = all_columns -condition_keys
        else:
            columns_order = set(columns_order)

        set_tags = ','.join(('`{}`=%s'.format(col, col) for col in columns_order))
        param = tuple(data[k] for k in columns_order)
        param+=condition_param
        if limit:
            param+=(limit,)
        final_sql = sql.format(table, set_tags, where_tags)
        return final_sql,param


    # @staticmethod
    # def generate_replace_into_sql(table, data_list, columns_order=None, many=True):
    #     """
    #     columns_order  字段顺序。如果不传入，则取第一个data的 键集合
    #     many 为True，则将每条数据的参数都作为一个元祖，并将所有元祖合并到一个数组内
    #     否则的话，将安装字段顺序，将所有参数加入同一个列表
    #     if many :
    #         sql='REPLACE INTO xx (c1,c2,c3,c4) VALUES(%s,%s,%s,%s) '  ,params=[(1,2.3,4),(a,c.d,c),]
    #     else:
    #         sql="REPLACE INTO xx (c1,c2,c3,c4) VALUES(%s,%s,%s,%s),(%s,%s,%s,%s)"   ,param=(1,2.3,4,a,c.d,c)

    #     """
    #     sql = 'REPLACE INTO {} ({}) VALUES{} '
    #     columns_list = columns_order if columns_order else data_list[0].keys()
    #     column_string = '`' + '`,`'.join(columns_list) + '`'
    #     s_s = '(' + ','.join(['%s']*len(columns_list)) + ')'

    #     if many:
    #         param = list()  # 存储值
    #         for data in data_list:
    #             p = tuple()
    #             for key in columns_list:
    #                 p = p + (data[key],)
    #             param.append(p)
    #         sql = sql.format(table, column_string, s_s)
    #     else:
    #         all_s = list()
    #         for i in range(0, len(data_list)):
    #             all_s.append(s_s)
    #         all_s = ','.join(all_s)
    #         param = tuple()  # 存储值
    #         for data in data_list:
    #             for key in columns_list:
    #                 param = param + (data[key],)
    #         sql = sql.format(table, column_string, all_s)

    #     return sql, param


if __name__ == "__main__":
    Model = SqlGenerator()
    #sql, params = Model.generate_select_sql('id', 'ths_industry_area', {'securitycode': '000725','a':8}, group_by='securitycode', order_by='id', limit=10, offset=0)
    sql, params = Model.generate_insert_single_sql('test', {'name': 'lishukan', 'age': 18}, ignroe=True)
    print(sql, params)
    data_list = [
       {'name': 'lishukan', 'age': 18},
       {'name': 'jackma', 'age': 22}
    ]
    print(Model.generate_insert_many_sql('test',data_list,ignore=True) )
