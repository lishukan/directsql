#! /usr/bin/env python3
# -*- coding: utf-8 -*-
import traceback
from typing import Iterable
import pymysql
import time
import random
import uuid
import logging


class SqlGenerater(object):
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

    @staticmethod  # 默认只查询id,暂只支持 = ， 不支持like、大于、小于等查询
    def generate_query_sql(table, condition, columns_u_need='id', limit=None):
        """
        生成查询sql
        """
        if isinstance(columns_u_need, (tuple, list)):
            columns = ','.join(columns_u_need)
        elif isinstance(columns_u_need, str):
            columns = columns_u_need
        else:
            raise TypeError('error ! colnmns_you_need must be str or tuple or list ')

        param = None
        where_list = []
        if isinstance(condition, dict):
            param = tuple()
            for key, value in condition.items():
                where_list.append(key+'=%s')
                param = param + (value,)
            where_str = ' AND '.join(where_list)
        else:
            raise TypeError("sql condition must be dict,for example : {'id':798456}")
        sql = "SELECT {} FROM {} WHERE {} ".format(columns, table, where_str)
        if limit:
            sql = sql+' limit {} ;'.format(limit)
        # print(sql,param)
        return sql, param


    @staticmethod
    def generate_insert_sql(table, condition, ignroe=False):
        """
        插入单条数据,condition为字典形式的数据
        """
        sql = 'INSERT INTO {} ({}) VALUES ({});' if not ignroe else 'INSERT IGNORE INTO {} ({}) VALUES ({});'

        s_s = list()
        param = tuple()
        for key, value in condition.items():
            columns.append('`'+key+'`')
            s_s.append('%s')
            param = param + (value,)

        columns = ','.join(columns)
        s_s = ','.join(s_s)
        sql = sql.format(table, columns, s_s)
        #print(sql,param )
        return sql, param

    @classmethod
    def generate_update_sql(cls, table, condition, ID, primary='id'):
        """
        更新单条数据,condition为字典形式的数据
        """
        sql = 'UPDATE {} SET {} WHERE {}='+str(ID)+' limit 1;'
        columns_condi, param = cls.get_columns_and_params(condition, update=True)
        sql = sql.format(table, columns_condi, primary)
        return sql, param

    @staticmethod
    def get_columns_and_params(condition: dict, update=False):
        key_s, params = zip(*condition.items())
        if update:
            columns_condi = ','.join(['`' + k + '`' + '=%s' for k in key_s])
        else:
            columns_condi = ','.join(['`' + k + '`' for k in key_s])
        return columns_condi, params

    @staticmethod
    def generate_replace_into_sql(table, data_list, columns_order=None, many=True):
        """
        columns_order  字段顺序。如果不传入，则取第一个data的 键集合
        many 为True，则将每条数据的参数都作为一个元祖，并将所有元祖合并到一个数组内
        否则的话，将安装字段顺序，将所有参数加入同一个列表
        if many :
            sql='REPLACE INTO xx (c1,c2,c3,c4) VALUES(%s,%s,%s,%s) '  ,params=[(1,2.3,4),(a,c.d,c),]
        else:
            sql="REPLACE INTO xx (c1,c2,c3,c4) VALUES(%s,%s,%s,%s),(%s,%s,%s,%s)"   ,param=(1,2.3,4,a,c.d,c)

        """
        sql = 'REPLACE INTO {} ({}) VALUES{} '
        columns_list = columns_order if columns_order else data_list[0].keys()
        column_string = '`' + '`,`'.join(columns_list) + '`'
        s_s = '(' + ','.join( ['%s']*len(columns_list)) + ')'

        if many:
            param = list()  # 存储值
            for data in data_list:
                p = tuple()
                for key in columns_list:
                    p = p + (data[key],)
                param.append(p)
            sql = sql.format(table, column_string, s_s)
        else:
            all_s = list()
            for i in range(0, len(data_list)):
                all_s.append(s_s)
            all_s = ','.join(all_s)
            param = tuple()  # 存储值
            for data in data_list:
                for key in columns_list:
                    param = param + (data[key],)
            sql = sql.format(table, column_string, all_s)

        return sql, param


class SqlActor(SqlGenerater):

    def select(self, columns_be_need='*', table=None,  condition=None, limit=None):
        """
        select name,age from student where  teacher='JackMa' limit 20
        --->    SqlActor().select(['name','age'],'student',{'taecher':'JackMa',20})

        """
        pass
