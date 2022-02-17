#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project -> File   ：pydantic-manager -> objects
@IDE    ：PyCharm
@Author ：Administrator
@Date   ：2021/7/4 0004 18:38
@Desc   ：
"""
import uuid
import datetime
import pydantic
from pydantic import BaseModel, Field
from pydantic_manager import utils

from pydantic_manager.managers import PSQLManager


def id_generator():
    return ''.join(str(uuid.uuid4()).split('-'))


class User(BaseModel):
    id: int = Field(title='主键', primary_key=True, alias='pk')
    name: str = ...
    birth_date: datetime.datetime = None
    city: str = 'fgf'


class Room(BaseModel):
    id: str = Field(title='主键', primary_key=True)
    name: str = ...
    avatar: str = None


room_mgr = PSQLManager(Room, 'room')

if __name__ == '__main__':
    print(utils.get_pk_and_validate(User))
    User.id
