#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project -> File   ：pydantic-manager -> test_manager
@IDE    ：PyCharm
@Author ：Administrator
@Date   ：2021/7/5 0005 10:24
@Desc   ：
"""
import pytest
from test.objects import room_mgr, Room, id_generator
pytestmark = pytest.mark.asyncio


class TestManager:

    async def test_create(self, pool):
        room = Room(id=id_generator(), name='ttt', avatar='://avatar.png')
        await room_mgr.create(room.dict(), pool)

    async def test_update(self, pool):
        room = Room(id='84b97fedc3b64b7f82064100246a3ea1', name='gcb3', avatar='://avatar3.png')
        await room_mgr.update(room.dict(), db=pool)

    async def test_delete(self, pool):
        await room_mgr.delete('9be66d4ce3e544e7b7c365fb9d327bda', db=pool)

    async def test_get(self, pool):
        ret = await room_mgr.get('a00afc410618461a80e4756292f591c5', db=pool)
        print('ret: {}'.format(ret))

    async def test_find(self, pool):
        ret = await room_mgr.find_by_field('name', 'test', ['avatar'], pool)
        print(ret)


if __name__ == '__main__':
    pytest.main(["-s", "test_manager.py::TestManager::test_create"])

