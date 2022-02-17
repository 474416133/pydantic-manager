#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project -> File   ：pydantic-manager -> setup
@IDE    ：PyCharm
@Author ：Administrator
@Date   ：2021/7/4 0004 14:50
@Desc   ：
"""
from setuptools import setup

setup(
    name="pydantic_manager",
    version="0.0.1",
    description="封装pydantic 对象数据库操作",
    author="474416133@qq.com",
    author_email="474416133@qq.com",
    packages=["pydantic_manager"],
    install_requires=["asyncpg", "pydantic"]
)