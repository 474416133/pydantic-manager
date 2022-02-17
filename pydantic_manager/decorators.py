# -*- coding: utf-8 -*-
"""
-------------------------------------------------
   File Name：     decorators
   Description :
   Author :       sven
   date：          19-5-1
-------------------------------------------------
   Change Activity:
                   19-5-1:
-------------------------------------------------
"""
__author__ = 'sven'

import asyncio
import inspect
from functools import wraps


def singleton(maybe_class):
    """
    单例
    :param maybe_class:
    :return:
    """

    if not inspect.isclass(maybe_class):
        return maybe_class

    if not hasattr(maybe_class, "__instance"):
        setattr(maybe_class, "__instance", maybe_class())

    def __new__(cls, *args, **kwargs):
        return getattr(cls, "__instance")

    maybe_class.__new__ = __new__
    return maybe_class


class cached_property(object):

    def __init__(self, func, name=None):
        """

        :param func:
        :param name:
        """
        self.func = func
        self.name = name or func.__name__

    def __get__(self, instance, owner=None):
        """

        :param instance:
        :param owner:
        :return:
        """
        attr_name = "{}__ret".format(self.name)
        ret = getattr(instance, attr_name, None)
        if ret is None:
            ret = self.func(instance)
            setattr(instance, attr_name, ret)
        return ret


def cache(cache_manager=None, then=None):
    """
    @cache
    :return:
    """
    
    def _cache_func(func):
        """
        :param func:
        :return:
        """
        func_name = func.__name__
        cache_method = getattr(cache_manager, func_name)
        if not cache_method:
            return func
        
        @wraps(func)
        async def _cache_without_then(self, *args, **kwargs):
            instance = await cache_method(*args, **kwargs)
            if instance:
                return instance
            return await func(*args,  **kwargs)

        @wraps(func)
        async def _cache_and_then(self, *args, **kwargs):
            instance = await cache_method(*args, **kwargs)
            if instance:
                return instance
            instance = await func(*args,  **kwargs)
            asyncio.ensure_future(then(instance))
            
        if then:
            return _cache_and_then
        return _cache_without_then
    return _cache_func


def cache_put(cache_manager=None, signals=None):
    """
    pass
    :return:
    """
    
    
def cache_evict(cache_manager=None, signals=None):
    """
    pass
    :return:
    """