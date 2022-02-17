#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project -> File   ：pydantic-manager -> conftest
@IDE    ：PyCharm
@Author ：Administrator
@Date   ：2021/7/5 0005 10:07
@Desc   ：
"""
import asyncio
from logging import config
import pytest
import asyncpg
from . import logconfig

config.dictConfig(logconfig.DEFAULT_LOGGING)


@pytest.fixture(scope='module')
async def pool():
    dns = 'postgres://postgres:postgres@localhost:5432/bugu_chat'
    return await asyncpg.create_pool(dsn=dns)


@pytest.fixture(scope='module')
def event_loop():
    loop = asyncio.get_event_loop()
    yield loop
    loop.close()