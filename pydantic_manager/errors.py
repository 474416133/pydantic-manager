#-*- encoding:utf-8 -*-
"""
Copyrigth : 
Project Name： pipe
Module Name： errors
Author : sven
date： 19-5-18
Description : 

"""
__author__ = "sven"
__email__ = "474416133@qq.com"

import enum
from enum import Enum


class BizError(RuntimeError):
    """
    业务异常
    """
    __slots__ = "error_code", "error_value", "error_remark"

    def __init__(self, error_code, error_value, error_remark=None, *args):
        self.error_code = error_code
        self.error_value, self.error_remark = error_value, error_value
        if error_remark:
            self.error_remark = error_remark
            if args:
                self.error_remark = self.error_remark.format(*args)

    def __str__(self):
        """
        @overide
        :return:
        """
        return f"error_code={self.error_code}," \
               f" error_value={self.error_value}," \
               f" error_remark={self.error_remark}"

    def as_dict(self):
        """

        :return:
        """
        return {
            "error_code" : self.error_code,
            "error_value" :self.error_value,
            "error_remark" : self.error_remark
        }


class IError(Enum):
    """
    异常
    """
    def exception(self, error_remark=None, *args):
        """
        异常
        :param msg:
        :param args:
        :return: BizError对象
        """
        return BizError(self.value, self.name, error_remark, *args)

    def reraise(self, error_remark=None, *args):
        """
        抛出异常
        :param remark:
        :param args:
        :return:
        """
        raise self.exception(error_remark, *args)


@enum.unique
class Errors(IError):
    OK = 0
    UNKNOWN_ERROR = 9999

    # 模型构建错误
    MODEL_BUILDING_ERROR = 10000
    # 属性命名错误
    FIELD_NAMMING_ERROR = 10001
    # 属性不允许修改
    NOT_ALLOWED_MODIFY_FIELD = 10002
    # 属性值设置错误
    FIELD_SETTING_ERROR = 10003
    # 属性不存在错误
    FIELD_NOT_EXIST = 10004
    # 复制错误
    COPY_ERROR = 10005
    # 属性描述错误
    DESCRIPTOR_ERROR = 10006
    #比较错误
    CMP_ERROR = 10007
    # 断言错
    ASSERT_ERROR = 10999
    #调用错误
    INVOKE_ERROR = 10998

    # 参数错误
    PARAMS_ERROR = 11000
    # 检验错误
    VALIDATE_ERROR = 110001

    # 数据已存在
    ENTITY_HAD_EXIST = 20000
    # 数据不存在
    ENTITY_NOT_EXIST = 20001
    # 写操作
    WRITE_ERROR = 21000
    
    #web
    HTTP_ERROR = 30000

    #编码错误
    PROGRAMMING_ERROR = 90000


class ValidateError(BizError):

    def __init__(self, msg):
        """

        :param msg:
        """
        super().__init__(Errors.VALIDATE_ERROR.value, Errors.VALIDATE_ERROR.name, msg)


def exception(error_remark=None, *args):
    """
    异常
    :param msg:
    :param args:
    :return: BizError对象
    """
    return Errors.UNKNOWN_ERROR.exception(error_remark)


def reraise(error_remark=None, *args):
    """
    抛出异常
    :param remark:
    :param args:
    :return:
    :
    """
    Errors.UNKNOWN_ERROR.reraise(error_remark)
