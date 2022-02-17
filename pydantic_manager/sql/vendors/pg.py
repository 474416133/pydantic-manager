# -*- encoding:utf-8 -*-
"""
Copyrigth : 
Project Name： pipe
Module Name： postgresql
Author : sven
date： 19-8-11
Description : 

"""
__author__ = "sven"
__email__ = "474416133@qq.com"

from collections import Iterable

from pydantic_manager import logs
from pydantic_manager.decorators import cached_property
from pydantic_manager.sql.builder import (SqlBuilder,
                                          IncreasePolicyEnum)

logger = logs.logger


def handle_update_fields(update_fields=None,
                         inc_fields=None,
                         inc_policy=IncreasePolicyEnum.NOTHING,
                         ignore_pk=None):
    """
    处理更新字段
    :param update_fields:
    :param inc_fields:
    :param inc_policy:
    :return:
    """
    # if not inc_policy_available(inc_policy):
    #     inc_policy = IncreasePolicyEnum.NOTHING

    _update_fields = {}

    if isinstance(update_fields, Iterable):
        _update_fields.update(dict([(item1, IncreasePolicyEnum.NOTHING) for item1 in update_fields]))
    elif isinstance(update_fields, str):
        _update_fields[update_fields] = IncreasePolicyEnum.NOTHING
    # 自增字段优先
    if inc_policy != IncreasePolicyEnum.NOTHING:
        if isinstance(inc_fields, Iterable):
            _update_fields.update(dict([(item, inc_policy) for item in inc_fields]))
        elif isinstance(inc_fields, str):
            _update_fields[inc_fields] = inc_policy
    # 踢出id
    _update_fields.pop(ignore_pk, None)
    return tuple(zip(_update_fields.keys(), _update_fields.values()))


def append_set_statement(update_fields):
    """
    拼接set语句
    :param update_fields:
    :return:
    """
    logger.debug("update_fields: {}, len: {}".format(update_fields, len(update_fields)))
    return ", ".join([item[1].execute(item[0], "$%s" % (index + 2)) for index, item in enumerate(update_fields)])


class PsqlBuilder(SqlBuilder):
    """

    """
    @cached_property
    def insert_sql(self):
        """
        :return:
        """
        _insert_sql = "INSERT INTO {} ({}) VALUES ({}) " \
                      "ON CONFLICT ({}) " \
                      "DO NOTHING "
        return _insert_sql.format(self.table_name,
                                  ", ".join([field.name for field in self._manager.model_fields]),
                                  ", ".join(["$%s" % i for i in range(1, 1 + len(self._manager.model_fields))]),
                                  self._manager.model_pk.name,
                                  self._manager.model_pk.name)

    @cached_property
    def update_sql(self):
        update_fields = tuple([field.name for field in self._manager.model_fields if field != self._manager.model_pk])
        return self.build_update_sql(update_fields)

    @cached_property
    def delete_sql(self):
        return "DELETE FROM {} WHERE {} = $1".format(self.table_name,
                                                     self._manager.model_pk.name)

    @cached_property
    def get_sql(self):
        """
        获取个体
        :return:
        """
        return self.build_get_sql("*")

    def build_insert_sql(self, sharing_pk=None):
        """
        构建insert sql
        :param sharing_pk:
        :return: str
        """
        return self.insert_sql

    @cached_property
    def bulk_insert_sql(self):
        """
        构建批量insert sql
        :return: str
        """
        return self.insert_sql

    def build_update_sql(self,
                         update_fields=None,
                         inc_fields=None,
                         inc_policy=IncreasePolicyEnum.NOTHING,
                         where_field=None,
                         sharing_pk=None):
        """
        #构建更新语句
        :param where_field:
        :param update_fields: 更新的字段
        :param inc_fields: 自增的字段
        :param inc_policy: 自增的策略 +/*/ \/
        :param sharing_pk:
        :return:
        """
        pk = self.manager.model_pk.name
        where_field = where_field or pk
        _update_fields = handle_update_fields(update_fields, inc_fields, inc_policy, ignore_pk=pk)
        if not _update_fields:
            return self.update_sql
        return self._build_update_sql(_update_fields, where_field), tuple(_update_fields)

    def _build_update_sql(self, update_fields=None, where_field=None):
        """
        更新语句
        :param update_fields: 更新的字段 ("$name", 0/1) 0/1=update/increase
        :param inc_fields: 自增的字段
        :param inc_policy: 自增的策略 +/*/ \/
        :return:
        """
        set_statement = append_set_statement(update_fields)
        return "UPDATE {} SET {} WHERE {} = ${}".format(self.table_name,
                                                        set_statement,
                                                        where_field, #self._manager.model_pk.name,
                                                        1)

    @cached_property
    def bulk_update_sql(self):
        """
        [批量] 更新
        :return:
        """
        update_fields = tuple([field.name for field in self._manager.model_fields if field != self._manager.model_pk])
        return self.build_bulk_update_sql(update_fields)

    def build_bulk_update_sql(self,
                              update_fields=None,
                              inc_fields=None,
                              inc_policy=IncreasePolicyEnum.NOTHING):
        """
        [批量] 更新
        :param update_fields:
        :param inc_fields:
        :param inc_policy:
        :return:
        """
        if not update_fields:
            return self.bulk_update_sql
        pk = self.manager.model_pk.name
        _update_fields = handle_update_fields(update_fields, inc_fields, inc_policy, ignore_pk=pk)
        if not _update_fields:
            return self.bulk_update_sql
        set_statement = append_set_statement(_update_fields)
        return "UPDATE {} SET {} WHERE {} = ANY($1)".format(self.table_name,
                                                            set_statement,
                                                            self._manager.model_pk.name), tuple(_update_fields)

    def build_delete_sql(self, sharing_pk=None):
        """
        构建delete 语句
        :param sharing_pk:
        :return:
        """
        return self.delete_sql

    @cached_property
    def bulk_delete_sql(self):
        """
        [批量]构建delete 语句
        :return:
        """
        return "DELETE FROM {} WHERE {} = ANY($1)".format(self.table_name,
                                                          self._manager.model_pk.name)

    def build_get_sql(self,
                      fields=None,
                      sharing_pk=None):
        """
        获取个体sql[部分属性]
        :param fields:
        :param sharing_pk:
        :return:
        """
        if fields is None:
            return self.get_sql
        select_head = self.build_select_fields(selected_fields=fields)
        return "SELECT {} FROM {}  WHERE {} = $1".format(select_head,
                                                                 self.table_name,
                                                                 self._manager.model_pk.name)

    @cached_property
    def select_fields(self):

        fields = [field.name for field in self._manager.model_fields]
        return self.build_select_fields(fields)

    def build_select_fields(self,
                            selected_fields=None):
        """
        select [部分属性」
        :param selected_fields:
        :param alias:
        :return:
        """

        if not selected_fields or selected_fields == "*":
            return self.select_fields
        else:
            return ", ".join(
                ["{}".format(self.manager.get_model_field(item).name)
                 for item in selected_fields])

    def build_select_sql(self,
                         selected_fields=None,
                         sharing_pk=None):
        """
        构建select sql
        :param selected_fields:
        :param sharing_pk:
        :return:
        """
        return "SELECT {} FROM {} ".format(self.build_select_fields(selected_fields=selected_fields),
                                           self.table_name)


class SharingPsqlBuilder(PsqlBuilder):
    """
    分表sql builder
    """

    @property
    def sharing_policy(self):
        """
        分表策略
        :return:
        """
        return self.manager.sharing_policy

    def build_get_sql(self,
                      fields=None,
                      sharing_pk=None):
        """
        @override
        :param fields:
        :param sharing_pk:
        :return:
        """
        sql_template = super().build_get_sql(fields, sharing_pk)
        return sql_template.format(table_name=self.sharing_policy(sharing_pk))

    def build_insert_sql(self, sharing_pk=None):
        """
        @override
        :param sharing_pk:
        :return:
        """
        sql_template = super().build_insert_sql(sharing_pk)
        return sql_template.format(table_name=self.sharing_policy(sharing_pk))

    def build_update_sql(self,
                         update_fields=None,
                         inc_fields=None,
                         inc_policy=IncreasePolicyEnum.NOTHING,
                         sharing_pk=None):
        """
        @override
        :param update_fields:
        :param inc_fields:
        :param inc_policy:
        :param sharing_pk:
        :return:
        """
        sql_template = super().build_update_sql(update_fields, inc_fields, inc_policy, sharing_pk)
        return sql_template.format(table_name=self.sharing_policy(sharing_pk))

    def build_delete_sql(self, sharing_pk=None):
        """
        @override
        :param sharing_pk:
        :return:
        """
        sql_template = super().build_delete_sql(sharing_pk)
        return sql_template.format(table_name=self.sharing_policy(sharing_pk))

    def build_select_sql(self, selected_fields=None, sharing_pk=None):
        """
        构建select sql
        :param selected_fields:
        :param sharing_pk:
        :return:
        """
        sql_template = super().build_select_sql(selected_fields, sharing_pk)
        return sql_template.format(table_name=self.sharing_policy(sharing_pk))
