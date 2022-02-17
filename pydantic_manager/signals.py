#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project -> File   ：pydantic-manager -> signals
@IDE    ：PyCharm
@Author ：Administrator
@Date   ：2021/7/4 0004 22:47
@Desc   ：
"""
import asyncio
import inspect
from functools import partial
from collections import defaultdict

from . import errors
from . import logs

Errors = errors.Errors
logger = logs.logger


def assert_hashable(obj):
    assert getattr(obj, "__hash__") is not None, "obj is not hashable"


def hashable(obj):
    return hasattr(obj, "__hash__")


def make_key(obj):
    """
    dict key
    :param obj:
    :return:
    """
    assert_hashable(obj)
    if isinstance(obj, (str, int)):
        return obj
    elif inspect.isclass(obj):
        return hash("%s:%s" % ("class", obj.__qualname__))
    else:
        _cls = getattr(obj, "__class__", None)
        return hash("%s:%s" % (_cls.__qualname__, id(obj))) if _cls else \
            hash("%s:%s" % ("object", id(obj)))


def validate_receiver_args(signal_instance, receiver_func):
    """
    校验接收者参数
    :param signal_instance:
    :param receiver_func:
    :return:
    """
    receiver_func_sign = inspect.signature(receiver_func)
    receiver_func_params = receiver_func_sign.parameters
    if len(receiver_func_params) == 1 and len(
        [key for key in receiver_func_params if receiver_func_params[key].kind == inspect.Parameter.VAR_KEYWORD]) == 1:
        return
    Errors.ASSERT_ERROR.reraise("receiver only have a **kwargs")


def log_receiver_exception(future, context=None):
    logger.error("future==========================")
    exc = future.exception()
    if exc:
        logger.error("++++++++++++++++++=erroe")


class Signal(object):

    def __init__(self, providing_args=None):
        """

        :param providing_args:
        """
        assert isinstance(providing_args, tuple), "providing_args must be a tuple"
        self.providing_args = providing_args
        setattr(self, "receiver_validator", partial(validate_receiver_args, signal_instance=self))
        self.setup()

    def setup(self):
        """

        :return:
        """
        self.receivers = defaultdict(set)

    def connect(self, receiver, receivor_id, sender=None):
        """
        链接
        :param receiver:
        :param sender: hashable
        :return:
        """
        # assert callable(receiver), "receiver must be a callable"
        if not hashable(sender) or sender in ("__default", "__receivor_ids"):
            Errors.PARAMS_ERROR.reraise("sender 参数错误")
        if not inspect.iscoroutinefunction(receiver):
            Errors.PARAMS_ERROR.reraise("receiver must be a coroutine or coroutinefunction")

        self.receiver_validator(receiver_func=receiver)
        if sender is None:
            self.receivers["__default"].add(receiver)
        else:
            self.receivers[sender].add(receiver)

        self.receivers["__receivor_ids"].add(receivor_id)

    def send(self, **named):
        """
        信号发送,
        :param sender:
        :param named:
        :return:
        """
        logger.info("send.....")
        receivers = set()
        sender = named.get("sender")
        if not sender:
            receivers.update(self.receivers["_default"])
        else:
            receivers.update(self.receivers.get(sender, {}))
        if receivers:
            cors = []
            for receiver in receivers:
                cors.append(receiver(**named))
            return asyncio.gather(*cors, return_exceptions=True)


class SingleReceiverSignal(Signal):
    """

    """

    def setup(self):
        self.receivers = {}

    def connect(self, receiver, receivor_id, sender):
        """

        :param receiver:
        :param receivor_id:
        :param sender:
        :return:
        """
        logger.info("signal connect.")
        if not hashable(sender):
            Errors.PARAMS_ERROR.reraise("sender 参数错误")
        if not inspect.iscoroutinefunction(receiver):
            Errors.PARAMS_ERROR.reraise("receiver must be a coroutine or coroutinefunction")

        self.receiver_validator(receiver_func=receiver)
        if sender in self.receivers:
            Errors.PARAMS_ERROR.reraise("sender 已设置")
        self.receivers[sender] = receiver

    def send(self, **named):
        """
        信号发送,
        :param sender:
        :param named:
        :return:
        """
        sender = named.get("sender")
        _receiver = self.receivers.get(sender)
        if not _receiver:
            return
        loop = asyncio.get_event_loop()
        task = loop.create_task(_receiver(**named))
        return task


def receiver(signal, sender, receiver_id=None):
    """
    装饰器
    :return:
    """

    def _wrapper(func):
        [s.connect(func, func.__name__, sender) for s in signal] if isinstance(signal, (tuple, list)) \
            else signal.connect(func, receiver_id or func.__name__, sender)
        return func

    return _wrapper


before_create = SingleReceiverSignal(providing_args=("sender", "instance", "manager", "db", "signal"))
after_create = Signal(providing_args=("sender", "instance", "manager", "db", "signal"))

before_update = SingleReceiverSignal(providing_args=("sender", "instance", "update_fields", "pks", "manager", "db", "multi", "signal"))
after_update = Signal(providing_args=("sender", "instance", "update_fields", "pks", "manager", "db", "multi", "signal"))

before_delete = SingleReceiverSignal(providing_args=("sender", "pk", "pks", "manager", "db", "multi", "signal"))
after_delete = Signal(providing_args=("sender", "instance", "pk", "pks", "manager", "db", "multi", "signal"))