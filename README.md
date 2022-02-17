# pydantic-manager
### about
基于pydantic model的实现的持久化库，目前支持postgresql数据库（基于asyncpg库），
mongodb（基于motor库）, 支持的操作如下：

-  插入数据库（单个，批量）

-   删除（单个，批量）

-   更新（单个， 批量）

-   查询（单个，列表）

-   更新或新增

-   获取或新增

-    计数器

-    唯一性 

  

### 依赖 

python >= 3.6
pydantic==1.9.0
asyncpg==0.25.0
pytest==7.0.1
motor==2.5.1

### 用例
参考test/test_manager.py

### 扩展
实现其他库（或者驱动）的基本操作，建议继承pydantic_manager.managers.ManagerBase类或子类

### 测试
略


