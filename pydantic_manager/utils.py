#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project -> File   ：pydantic-manager -> utils
@IDE    ：PyCharm
@Author ：Administrator
@Date   ：2021/7/4 0004 19:14
@Desc   ：
"""
from . import errors

ERRORS = errors.Errors


def get_model_fields(model):
    """
    获取属性列表
    :param model:
    :return:
    """
    for field_name in model.__fields__:
        yield model.__fields__[field_name]


def get_model_field(model, field_name):
    """
    根据属性名获取属性对象
    :param model:
    :param field_name:
    :return:
    """
    return model.__fields__[field_name]


def get_pk_and_validate(model):
    """
    :param model:
    :return:
    """
    hits = []
    for field in get_model_fields(model):
        extra_attrs = field.field_info.extra
        if extra_attrs.get('primary_key'):
            hits.append(field)

    hit_count = len(hits)
    if hit_count != 1:
        raise ERRORS.PROGRAMMING_ERROR.exception(
            '错误：模型{}已定义了{}个主键，分别是{}.提示：一个模型有且只有一个主键'.format(
                model,
                hit_count,
                ','.join([hit.name for hit in hits]))
        )
    return hits[0]
