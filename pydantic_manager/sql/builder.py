#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project -> File   ：pydantic-manager -> builder
@IDE    ：PyCharm
@Author ：Administrator
@Date   ：2021/7/4 0004 20:01
@Desc   ：
"""
import enum
from enum import Enum


class OperatorPolicy(Enum):
    """
    操作符
    """

    def execute(self, column_name, place_holder):
        """
        执行
        :param place_holder:
        :param column_name:
        :return:
        """
        return self.value.format(column_name, place_holder)


@enum.unique
class IncreasePolicyEnum(OperatorPolicy):
    NOTHING = "{0} = {1}"
    INCREASE = "{0} = {0} + {1}"
    MULTIPLY = "{0} = {0} * {1}"
    DIVIDE = "{0} = {0} / {1}"
    MOD = "{0} = {0} % {1}"


def inc_policy_available(value):
    return isinstance(value, OperatorPolicy)


class SqlBuilder(object):
    """

    """

    def __init__(self, manager):
        """

        :param manager:
        """
        self._manager = manager

    @property
    def manager(self):
        return self._manager

    @property
    def model(self):
        return self._manager.model

    @property
    def table_name(self):
        return self.manager.table_name

    def build_insert_sql(self, sharing_pk=None):
        """
        插入
        :param sharing_pk:
        :return:
        """
        raise NotImplementedError

    @property
    def bulk_insert_sql(self):
        """
        [批量]插入
        :return:
        """
        raise NotImplementedError

    def build_update_sql(self,
                         update_fields=None,
                         inc_fields=None,
                         inc_policy=IncreasePolicyEnum.NOTHING,
                         sharing_pk=None):
        """
        更新语句
        :param sharing_pk:
        :param update_fields: 更新的字段
        :param inc_fields: 自增的字段
        :param inc_policy: 自增的策略 +/*/ \/
        :return:
        """
        raise NotImplementedError

    def build_bulk_update_sql(self,
                              update_fields=None,
                              inc_fields=None,
                              inc_policy=IncreasePolicyEnum.NOTHING):
        """
        [批量] 更新
        :param inc_fields:
        :param inc_policy:
        :param update_fields:
        :return:
        """
        raise NotImplementedError

    def build_delete_sql(self, sharing_pk=None):
        """
        删除语句
        :param sharing_pk:
        :return:
        """
        raise NotImplementedError

    @property
    def bulk_delete_sql(self):
        """
        [批量]删除语句
        :return:
        """
        raise NotImplementedError

    def build_get_sql(self,
                      fields=None,
                      sharing_pk=None):
        """
        获取个体sql[部分属性]
        :param sharing_pk:
        :param fields:
        :return:
        """
        raise NotImplementedError

    def build_count_sql(self, sql):
        """
        构建count sql
        :param sql:
        :param params:
        :return:
        """
        raise NotImplementedError

    def build_select_sql(self, selected_fields=None, sharing_pk=None):
        """
        :param selected_fields:
        :param sharing_pk:
        :return:
        """

    def build_create_table_sql(self):
        """
        表创建语句
        :return:
        """
        raise NotImplementedError

    @classmethod
    def build_count_sql(cls, sql):
        """
        :param sql:
        :return:
        """
        return "select count(0) from ({}) _count_temp".format(sql)
