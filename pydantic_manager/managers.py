#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project -> File   ：pydantic-manager -> managers
@IDE    ：PyCharm
@Author ：Administrator
@Date   ：2021/7/4 0004 20:54
@Desc   ：
"""

__author__ = "sven"
__email__ = "474416133@qq.com"


from collections import Iterable, defaultdict
from . import logs
from . import errors
from . import utils
from . import signals
from .sql.vendors import pg
from .sql import builder


logger = logs.logger
ERRORS = errors.Errors
IncreasePolicyEnum = builder.IncreasePolicyEnum


class ManagerBase(object):
    """
    pydantic 持久化操作工作，包括：
    - 插入数据库（单个，批量）
    - 删除（单个，批量）
    - 更新（单个， 批量）
    - 查询（单个，列表）
    - 更新或新增
    - 获取或新增
    - 计数器
    - 唯一性
    """
    get_model_fields = utils.get_model_fields
    get_model_pk = utils.get_pk_and_validate
    
    def __init__(self, 
                 model, 
                 table_name, 
                 sharing_policy=None):
        """
        :param model: 
        :param table_name: 
        """
        cls_ = self.__class__
        self._model = model
        self._table_name = table_name
        self._sharing_policy = sharing_policy
        if self._sharing_policy and not callable(sharing_policy):
            raise ERRORS.MODEL_BUILDING_ERROR.exception("sharing_policy MUST BE callable")
        self._model_primary_key = cls_.get_model_pk(self._model)
        self._model_fields = list(cls_.get_model_fields(self.model))
        
    @property
    def table_name(self):
        return self._table_name
    
    @property
    def model(self):
        return self._model
    
    @property
    def sharing_policy(self):
        return self._sharing_policy
    
    @property
    def model_pk(self):
        return self._model_primary_key
    
    @property
    def model_fields(self):
        return self._model_fields

    def get_model_field(self, field_name):
        """
        获取模型属性
        :param field_name:
        :return:
        """
        return utils.get_model_field(self._model, field_name)

    def create_instance_by_record(self, record):
        """
        从数据可对象中实例化model
        :param record:
        :return:
        """
        return self._model(**record)

    def convert_instance_to_dict(self, model_instance):
        """
        model对象转换成dict
        :param model_instance:
        :return:
        """
        return model_instance.dict()

    async def create(self, 
                     model_instance, 
                     db=None, 
                     signal=None):
        """
        创建
        :param model_instance:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def bulk_create(self, 
                          model_instances, 
                          db=None, 
                          signal=None):
        """
        批量创建
        :param model_instances:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def get(self, 
                  pk, 
                  fields=None, 
                  db=None):
        """
        获取
        :param pk:
        :param fields:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def get_or_create(self, 
                            model_instance, 
                            db=None, 
                            signal=None):
        """
        获取某记录，如果不存在，则创建
        :param signal: 
        :param model_instance:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def update(self, 
                     model_instance, 
                     update_fields=None, 
                     db=None, 
                     inc_fields=None, 
                     inc_policy=None, 
                     signal=None):
        """
        更新
        :param model_instance:
        :param update_fields: 需要更新大的字段
        :param db:
        :param inc_fields:  其他属性
        :param inc_policy: 自增/自减 ...
        :param signal: 信号
        :return:
        """
        raise NotImplementedError

    async def update_or_create(self, 
                               model_instance, 
                               update_fileds=None, 
                               db=None, 
                               signal=None):
        """
        更新或者添加
        :param model_instance:
        :param update_fileds:
        :param insert_field:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def update_by_PKs(self, 
                            model_instance, 
                            pks=None, 
                            update_fields=None, 
                            db=None, 
                            signal=None):
        """
        根据pks更新
        :param model_instance:
        :param pks:
        :param update_fields:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def delete(self, pk, db=None, signal=None):
        """
        删除
        :param pk:
        :param db:
        :return: 删除数量
        """
        raise NotImplementedError

    async def delete_by_PKs(self, pks, db=None, signal=None):
        """
        根据id列表删除
        :param pks:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def find(self, sql, params=None, db=None, **kwargs):
        """
        查询
        :param sql:
        :param params:
        :param db:
        :return: list
        """
        raise NotImplementedError

    async def find_by_PKs(self, pks, db=None):
        """
        根据pk列表查询
        :param pks:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def is_existed(self, instance, db=None):
        """
        对象是否存在
        :param instance:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def get_by_field(self, 
                           field_name, 
                           field_value, 
                           selected_fields=None, 
                           db=None):
        """

        :param field_name:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def create_table(self, ignore_existed=False):
        """

        :param ignore_existed:
        :return:
        """
        raise NotImplementedError

    async def is_unique(self, model_instance, field_name, *, db=None):
        """
        是否唯一
        :param model_instance:
        :param field_name:
        :param db:
        :return:
        """
        raise NotImplementedError

    async def validate_unique(self, model_instance, field_name, *, db=None):
        """

        :param model_instance:
        :param field_name:
        :param db:
        :return:
        """
        if not self.is_unique(model_instance, field_name, db=db):
            raise ERRORS.ValidateError(
                "{field_name}值{field_value}不唯一".format(field_name=field_name,
                                                       field_value=model_instance.get(field_name))
            )

    async def count(self, *args, db=None):
        """
        计数器
        :param args:
        :param db:
        :return:
        """
        raise NotImplemented


def parse_insert_ret(ret):
    _, _, count = ret.split()
    return count


def parse_update_ret(ret):
    """
    解析更新结果
    :param ret:
    :return:
    """
    _, count = ret.split()
    return int(count)


parse_delete_ret = parse_update_ret

    
class PSQLManager(ManagerBase):
    """
    postgre数据库基本操作
    """
    default_sql_builder_cls = pg.PsqlBuilder

    def __init__(self, 
                 model, 
                 table_name,
                 sharing_policy=None,
                 sql_builder_cls=None):
        """
        :param table_name: (str), 表名
        """
        super().__init__(model, table_name, sharing_policy)
        sql_builder_cls = sql_builder_cls or self.default_sql_builder_cls
        self.sql_builder = sql_builder_cls(self)
        
    async def create(self, model_instance, db=None, signal=None):
        """
        插入
        :param model_instance:
        :param db:
        :return:
        """
        pk = model_instance[self.model_pk.name]
        sql = self.sql_builder.build_insert_sql(pk)
        ret = await db.execute(sql, *[model_instance.get(field.name) for field in self.model_fields])
        _, _, count = ret.split()
        if count != "1":
            raise ERRORS.ENTITY_HAD_EXIST.exception(
                "table {}: id={} had exist".format(self.table_name, model_instance[self.model_pk.name]))
        
        logger.debug("[INSERT] SQL:%s ; data:%s " % (sql, model_instance))
        return pk

    async def bulk_create(self, model_instances, db=None, signal=None):
        """
        批量插入
        :param model_instances:
        :param db:
        :return:
        """
        pk0 = model_instances[0][self.model_pk.name]
        sql = self.sql_builder.build_insert_sql(pk0)
        ret = await db.executemany(sql, [[data_instance.get(field.name) for field in self.model_fields] for
                                         data_instance in model_instances])
        logger.debug("[INSERT] SQL:{}, PARAMS:{}, RET:{}".format(sql, model_instances, ret))
        return len(model_instances)

    async def get_raw(self, pk, fields=None, db=None):
        """
        根据pk获取记录
        :param pk:
        :param db:
        :return:
        """
        _sql = self.sql_builder.build_get_sql(fields, pk)
        logger.debug("[SELECT] SQL:{}, PARAMS:{}".format(_sql, pk))
        return await db.fetchrow(_sql, pk)

    async def get(self, pk, fields=None, db=None):
        """
        根据pk获取记录
        :param pk:
        :param db:
        :return:
        """
        record = await self.get_raw(pk, fields, db)
        return self.create_instance_by_record(record) if record else None

    async def is_existed(self, model_instance, *, db=None):
        """
        判断对象是否存在
        :param model_instance:
        :param db:
        :return:
        """
        pk = model_instance.get(self.model_pk.name)
        if not pk:
            return False
        sql = self.build_get_sql(sharing_pk=pk)
        count = await self.count(sql, [pk], db=db)
        return count > 0

    async def get_or_create(self, model_instance, db=None, signal=None):
        """
        获取或创建
        :param model_instance:
        :param db:
        :return:
        """
        conn = await db.acquire() if hasattr(db, "acquire") else db
        try:
            async with conn.transaction() as trans:
                instance = await self.get_raw(model_instance.get(self.model_pk.name), db=conn)
                if instance:
                    return False, model_instance
                else:
                    await self.create(model_instance, db=conn, signal=signal)
                    return True, model_instance
        finally:
            if hasattr(db, "release"):
                await db.release(conn)

    async def update(self,
                     model_instance,
                     update_fields=None,
                     db=None,
                     inc_fields=None,
                     inc_policy=IncreasePolicyEnum.NOTHING,
                     signal=None):
        """
        个体更新
        :param model_instance:
        :param update_fields:
        :param db:
        :return:
        """

        return await self.update_by_field(self.model_pk.name,
                                          model_instance,
                                          update_fields,
                                          db,
                                          inc_fields,
                                          inc_policy,
                                          signal)

    async def update_by_PKs(self,
                            model_instance,
                            pks=None,
                            update_fields=None,
                            db=None,
                            inc_fields=None,
                            inc_policy=IncreasePolicyEnum.NOTHING,
                            signal=None):
        """

        :param model_instance:
        :param pks:
        :param update_fields:
        :param db:
        :return:
        """
        _sql, _update_fields = self.sql_builder.build_bulk_update_sql(update_fields, inc_fields, inc_policy)
        _values = [model_instance.get(item[0]) for item in _update_fields]
        _values.insert(0, pks)
        ret = await db.execute(_sql, *_values)
        logger.debug("[UPDATE] SQL:%s, PARAMS:%s, RET:%s" % (_sql, _values, ret))
        del _update_fields
        del _values
        return parse_update_ret(ret)

    async def update_or_create(self,
                               model_instance,
                               update_fileds=None,
                               db=None,
                               signal=None):
        """
        更新或添加
        :param model_instance:
        :param update_fileds:
        :param insert_fields:
        :param db:
        :return: 2:update 1:create
        """
        conn = await db.acquire() if hasattr(db, "acquire") else db
        try:
            async with conn.transaction() as trans:
                ret = await self.update(model_instance, update_fileds, db=conn, signal=signal)
                if ret:
                    return 2
                else:
                    await self.create(model_instance, db=conn, signal=signal)
                    return 1
        finally:
            if hasattr(db, "release"):
                await db.release(conn)

    async def delete(self, pk, db=None, signal=None):
        """
        删除
        :param pk:
        :param db:
        :return:
        """
        _sql = self.sql_builder.build_delete_sql(pk)
        ret = await db.execute(_sql, pk)
        logger.debug("[DELETE] SQL:{}, PARAMS:{}, RET:{}".format(_sql, pk, ret))
        _, count = ret.split()
        return int(count)

    async def delete_by_PKs(self, pks, db=None, signal=None):
        """
        批量删除
        :param pks:
        :param db:
        :return:
        """
        _sql = self.sql_builder.bulk_delete_sql
        ret = await db.execute(_sql, pks)
        signals.after_delete.send(sender=self.model,
                          db=db,
                          manager=self,
                          signal=signal,
                          pks=pks,
                          multi=True)
        logger.debug("[DELETE] SQL:{}, PARAMS:{}, RET:{}".format(_sql, pks, ret))
        _, count = ret.split()
        return int(count)

    @classmethod
    async def find(cls,
                   sql,
                   params=None,
                   order_by=None,
                   limit=-1,
                   offset=0,
                   db=None):
        """
        查询
        :param sql: sql or callable
        :param params:
        :param db:
        :param kwargs:
        :return:
        """
        _sql = sql
        if order_by:
            _sql += " ORDER BY %s" % order_by
        if limit > 0:
            _sql += f' LIMIT {limit} OFFSET {offset} '

        params = params or []
        logger.debug('[SELECT] sql:{}, params:{}'.format(_sql, params))
        return await db.fetch(_sql, *params)

    @classmethod
    async def count(cls, sql, params=None, *, db=None):
        """
        计数器
        :param sql:
        :param params:
        :param db:
        :return:
        """
        _count_sql = cls.default_sql_builder_cls.build_count_sql(sql)
        logger.debug("[COUNT]count_sql: {}, params: {}".format(_count_sql, params))
        return await db.fetchval(_count_sql, *params)

    async def all(self, db=None):
        """
        获取全部
        :return:
        """
        sql = self.sql_builder.build_select_sql("*")
        logger.info("[select] {}".format(sql))
        return await self.find(sql, db=db)

    async def find_by_field(self,
                            field_name,
                            field_value,
                            selected_fields=None,
                            order_by=None,
                            limit=-1,
                            offset=0,
                            db=None):
        """
        :param field_name:
        :param db:
        :return:
        """
        sql = self.sql_builder.build_select_sql(selected_fields)
        sql += " WHERE {} = $1 ".format(field_name)
        ret = await self.find(sql, (field_value,), order_by, limit, offset, db=db)
        logger.debug('[SELECT] ')
        return ret

    async def get_by_field(self,
                           field_name,
                           field_value,
                           selected_fields=None,
                           order_by=None,
                           db=None):
        """
        :param field_name:
        :param db:
        :return:
        """
        sql = self.sql_builder.build_select_sql(selected_fields)
        if order_by:
            sql += " WHERE {} = $1 ORDER BY {}".format(self.get_model_field(field_name).name, order_by)
        else:
            sql += " WHERE {} = $1 ".format(self.get_model_field(field_name).name)
        ret = await db.fetchrow(sql, field_value)
        return self.create_instance_by_record(ret) if ret else None

    async def create_table(self,
                           ignore_existed=False,
                           db=None):
        """
        创建表
        :param ignore_existed:
        :return:
        """
        sql = self.sql_builder.build_create_table_sql(ignore_existed)
        logger.debug("[create table] sql: {}".format(sql))
        await db.execute(sql)

    async def is_unique(self,
                        model_instance,
                        field_name,
                        *,
                        db=None,
                        sharing_pk=None):
        """
        唯一检验
        :param model_instance:
        :param db:
        :return:
        """
        sql = self.sql_builder.build_select_sql(selected_fields=[self.model_pk.name],
                                                sharing_pk=sharing_pk)
        sql += "WHERE {} = $1".format(getattr(self.model, field_name))
        count = self.count(sql, model_instance.get(field_name))
        return count > 0

    async def find_by_PKs(self,
                          pks,
                          selected_fields=None,
                          db=None):
        """

        :param pks:
        :param selected_fields:
        :param db:
        :return:
        """
        sql = self.sql_builder.build_select_sql(selected_fields=selected_fields)
        sql += " WHERE %s = ANY($1)" % self.model_pk.name
        return await self.find(sql, pks, db=db)

    async def update_by_field(self,
                              field_name,
                              model_instance,
                              update_fields=None,
                              db=None,
                              inc_fields = None,
                              inc_policy=IncreasePolicyEnum.NOTHING,
                              signal=None):
        """
        根据field_name更新
        :param inc_policy:
        :param field_name:
        :param field_value:
        :param db:
        :param signal:
        :return:
        """
        _update_sql, _update_fields = self.sql_builder.build_update_sql(update_fields,
                                                                        inc_fields,
                                                                        inc_policy,
                                                                        where_field=field_name)
        _values = [model_instance.get(item[0]) for item in _update_fields]
        _values.insert(0, model_instance[field_name])
        ret = await db.execute(_update_sql, *_values)
        logger.debug('[UPDATE] sql:{}, '
                     'params: {}, '
                     'update_fields:{}, '
                     'inc_fields: {}, '
                     'compiled_update: {},'
                     'ret: {}'.format(_update_sql,
                                      _values,
                                      update_fields,
                                      inc_fields,
                                      _update_fields,
                                      ret))
        del _update_fields
        del _values
        del _update_sql
        return parse_update_ret(ret)


class MongoDBManager(ManagerBase):
    """
    mongodb基本操作封装
    """
    __slots__ = ["model", "name", "collection_name", "collection_pk_name"]

    def __init__(self, collection_name=None,
                 duplicate_error_cls=None,
                 write_error_cls=None):
        """
        :param collection_name:
        """
        self.model = None
        self.name = None
        self.collection_name = collection_name
        self.collection_pk_name = None
        self.duplicate_error_cls = duplicate_error_cls or Exception
        self.write_error_cls = write_error_cls or Exception

    async def create(self, model_instance, db=None):
        """
        插入一条记录
        :param model_instance:
        :param db:
        :return:
        """
        _doc = self.convert_instance_to_dict(model_instance) if isinstance(model_instance, self.model) else model_instance
        try:
            await db[self.collection_name].insert_one(_doc)
        except self.duplicate_error_cls as de:
            logger.error("[INSERT ONE]{}.{}({}) error：{}".format(db.name, self.collection_name, _doc, de))
            ERRORS.ENTITY_HAD_EXIST.reraise("{}".format(de))

        return model_instance[self.collection_pk_name]

    async def bulk_create(self, model_instances, db=None):
        """
        插入多条记录
        :param model_instances:
        :param db:
        :return:
        """
        await db[self.collection_name].insert_many(model_instances)

    async def get(self, pk, fields=None, db=None):
        """

        :param pk:
        :param db:
        :return:
        """
        projection = dict([(field, 1) for field in fields]) if isinstance(fields, Iterable) else None
        _doc = await db[self.collection_name].find_one({self.collection_pk_name: pk},
                                                       projection=projection)
        return self.model(data=_doc) if _doc is not None else None

    async def get_or_create(self, model_instance, db=None):
        """

        :param model_instance:
        :param db:
        :return:
        """
        # 锁
        pk = model_instance.get(self.model_pk.name)
        if not pk:
            raise RuntimeError("field '{}' value  had not present".format(self.model_pk.name))
        _doc = await self.get(pk, db=db)
        logger.debug("_doc: {}".format(_doc))
        if _doc is None:
            await self.create(model_instance, db)
            return True, model_instance
        return False, _doc

    async def update(self, model_instance, update_fields=None, db=None):
        """
        更新
        :param model_insrances:
        :param update_fields:
        :param db:
        :return:
        """
        _doc = self.convert_instance_to_dict(model_instance) if isinstance(model_instance, self.model) else model_instance
        filter_doc = {self.collection_pk_name: _doc[self.collection_pk_name]}
        update_doc = self._create_new_doc(_doc, update_fields)
        try:
            ret = await db[self.collection_name].update_one(filter_doc, {"$set": update_doc})
            return ret.modified_count
        except self.write_error_cls as we:
            logger.error("[UPDATE] {}.{}, doc:{}".format(db.name, self.collection_name, _doc))
            raise ERRORS.WRITE_ERROR.exception(str(we))
        finally:
            del update_doc
            del filter_doc

    async def update_by_PKs(self, model_instance, pks=None, update_fields=None, db=None):
        """
        根据pk列表更新
        :param model_instance:
        :param pks:
        :param update_fields:
        :param db:
        :return: -1 未处理
        """
        if not isinstance(pks, (tuple, list)):
            logger.warning("pks must be a tuple or list. return and nothing to do ")
            return -1
        _doc = self.convert_instance_to_dict(model_instance) \
            if isinstance(model_instance, self.model) else \
            model_instance
        filter_doc = {self.collection_pk_name: {"$in": pks}}
        update_doc = self._create_new_doc(_doc)
        try:
            ret = await db[self.collection_name].update_many(filter_doc, {"$set": update_doc})
            return ret.modified_count
        except self.write_error_cls as we:
            logger.error("[UPSERT] {}.{}, doc:{}".format(db.name, self.collection_name, _doc))
            raise ERRORS.WRITE_ERROR.exception(we.message)
        finally:
            del filter_doc
            del update_doc

    async def update_or_create(self, model_instance, update_fileds=None, db=None):
        """
        更新或创建
        :param model_instance:
        :param update_fileds:
        :param insert_fields:
        :param db:
        :return:
        """
        try:
            await self.create(model_instance, db=db)
            return 1
        except ERRORS.BizError as be:
            if be.error_code == ERRORS.ENTITY_HAD_EXIST.value:
                if isinstance(model_instance, self.model):
                    self.convert_instance_to_dict(model_instance).pop("_id")
                elif isinstance(model_instance, dict):
                    model_instance.pop("_id", None)
                ret = await self.update(model_instance, update_fileds, db)
                return ret if ret == -1 else 2
        except Exception as e:
            logger.error("[UPSERT] error：{}".format(e))

        return -1

    async def delete(self, pk, db=None):
        """

        :param pk:
        :param db:
        :return:
        """
        logger.debug("_doc: {}".format({self.collection_pk_name: pk}))
        ret = await db[self.collection_name].delete_one({self.collection_pk_name: pk})
        logger.debug("[DELETE] pk:{}, deleted_count：{}".format(pk, ret.deleted_count))
        return ret.deleted_count

    async def delete_by_PKs(self, pks, db=None):
        """

        :param pks:
        :param db:
        :return:
        """
        ret = await db[self.collection_name].delete_many({self.collection_pk_name: {"$in": pks}})
        logger.debug("[DELETE] pks:{}, deleted_count：{}".format(pks, ret.deleted_count))
        return ret.deleted_count

    def _create_new_doc(self, doc, update_fields=None):
        """
        返回新的更新文档
        :param data:
        :param update_fields:
        :return:
        """
        if update_fields:
            _new_doc = dict([(item, doc.get(item)) for item in update_fields if item != self.collection_pk_name])
        else:
            _new_doc = dict(doc)
            _new_doc.pop(self.collection_pk_name, None)
        return _new_doc

    async def find(self, filter_doc=None, projection=None, sorted=None, limit=None, skip=None, db=None, callback=None):
        """
        查询
        :param filter_doc:
        :param projection:
        :param db:
        :param inflate_cls: 填充类
        :return:
        """
        filter_doc = filter_doc or {}
        _c = db[self.collection_name].find(filter=filter_doc,
                                           projection=projection,
                                           sort=sorted)
        if limit > -1:
            _c.limit(limit)
            if skip:
                _c.skip(skip)
        ret = await _c.to_list(None)
        return callback(ret) if callable(callback) else ret

    async def find_by_PKs(self, pks, selected_fields=None, db=None, callback=None):
        """

        :param pks:
        :param db:
        :return:
        """
        filter_doc = {
            self.model_pk.name: {"$in": pks}
        }
        return await self.find(filter_doc, selected_fields, db=db, callback=callback)

    async def get_by_field(self, field_name, field_value, selected_fields=None, order_by=None, db=None):
        """

        :param field_name:
        :param db:
        :return:
        """
        filter_doc = {
            field_name: field_value,
        }
        return await self.find(filter_doc, selected_fields, order_by, limit=1, skip=0, db=db)

    async def is_existed(self, instance, db=None):
        """

        :param instance:
        :param db:
        :return:
        """
        filter_doc = {
            self.model_pk.name: instance.get(self.model_pk.name)
        }
        _count = await self.count(filter_doc)
        return _count > 0

    async def is_unique(self, model_instance, field_name, *, db=None):
        """

        :param model_instance:
        :param field_name:
        :param db:
        :return:
        """
        filter_doc = {
            field_name: model_instance.get(field_name)
        }
        _count = await self.count(filter_doc)
        return _count > 0

    async def count(self, filter_doc=None, db=None):
        """
        计数器
        :param filter_doc:
        :param db:
        :return:
        """
        return await db[self.collection_name].count_documents(filter=filter_doc or {})


class PSQLExtendManager(PSQLManager):
    """
    psql拓展类，适应分表,根据id分表
。	"""
    default_sql_builder_cls = pg.SharingPsqlBuilder

    def __init__(self, model, sharing_policy=None, sql_builder_cls=None):
        """

        :param model: pydantic模型
        :param sharing_policy: 分表策略
        :param sql_builder_cls: sql构建器
        """
        super().__init__(model,
                         table_name="{table_name}",
                         sharing_policy=sharing_policy,
                         sql_builder_cls=sql_builder_cls)
        self.sharing_policy = sharing_policy

    def _group_by_pks(self, pks):
        """
        根据id分组
        :param pks: id列表
        :return: dict
        """
        groups = defaultdict(list)
        for pk in pks:
            table_name = self.sharing_policy(pk)
            groups[table_name].append(pk)
        return groups

    async def bulk_create(self, model_instances, db=None):
        """
        批量插入
        :param model_instances:
        :param db:
        :return:
        """
        groups = defaultdict(list)
        for model_instance in model_instances:
            table_name = self.sharing_policy(model_instance[self.model_pk.name])
            groups[table_name].append(model_instance)

        sql_template = self.sql_builder.bulk_insert_sql
        conn = await db.acquire() if hasattr(db, "acquire") else db

        try:
            async with conn.transaction() as trans:
                for table_name in groups:
                    sql = sql_template.format(table_name=table_name)
                    ret = await conn.executemany(sql,
                                                 [[data_instance.get(field.name) for field in self.model_fields]
                                                  for data_instance in groups[table_name]])
        finally:
            if hasattr(db, "release"):
                await db.release(conn)

        return len(model_instances)

    async def update_by_PKs(self, model_instance, pks=None, update_fields=None, db=None, inc_fields=None,
                            inc_policy=IncreasePolicyEnum.NOTHING, signal=None):
        """
        @override
        :param model_instance:
        :param pks:
        :param update_fields:
        :param db:
        :return:
        """
        _sql_template = self.sql_builder.build_bulk_update_sql(update_fields, inc_fields, inc_policy)
        groups = self._group_by_pks(pks)
        _update_fields = update_fields or [field.name for field in self.model_fields]
        _values = [model_instance.get(item) for item in _update_fields]
        task = signals.before_update.send(sender=self.model,
                                  instance=model_instance,
                                  db=db,
                                  manager=self,
                                  update_fields=_update_fields,
                                  signal=signal,
                                  pks=pks,
                                  multi=True
                                  )
        if task:
            await task
        conn = await db.acquire() if hasattr(db, "acquire") else db
        rets = []
        try:
            async with conn.transaction() as trans:
                for table_name in groups:
                    _values.insert(0, groups[table_name])
                    ret = await db.execute(_sql_template.format(table_name=table_name), *_values)
                    logger.debug("[UPDATE] SQL:%s, PARAMS:%s, RET:%s" % (_sql_template, _values, ret))
                    _values.pop(0)
                    rets.append(parse_update_ret(ret))
        finally:
            if hasattr(db, "release"):
                await db.release(conn)

        signals.after_update.send(sender=self.model,
                          instance=model_instance,
                          db=db,
                          manager=self,
                          update_fields=_update_fields,
                          signal=signal,
                          pks=pks,
                          multi=True
                          )
        del _update_fields
        del _values
        return sum(rets)

    async def delete_by_PKs(self, pks, db=None, signal=None):
        """
        @override
        批量删除
        :param pks: id列表
        :param db:
        :return:
        """
        _sql_template = self.sql_builder.bulk_delete_sql
        groups = self._group_by_pks(pks)

        task = signals.before_delete.send(sender=self.model,
                                  db=db,
                                  manager=self,
                                  signal=signal,
                                  pks=pks,
                                  multi=True
                                  )
        if task:
            await task
        conn = await db.acquire() if hasattr(db, "acquire") else db
        rets = []
        try:
            async with conn.transaction() as trans:
                for table_name in groups:
                    ret = await db.execute(_sql_template.format(table_name=table_name), groups[table_name])
                    logger.debug("[DELETE] SQL:%s, PARAMS:%s, RET:%s" % (_sql_template, groups[table_name], ret))
                    _, count = ret.split()
                    rets.append(int(count))
        finally:
            if hasattr(db, "release"):
                await db.release(conn)

        signals.after_delete.send(sender=self.model,
                          db=db,
                          manager=self,
                          signal=signal,
                          pks=pks,
                          multi=True
                          )
        return sum(rets)


