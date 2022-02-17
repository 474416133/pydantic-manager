#!/usr/bin/env python
# -*- coding: UTF-8 -*-
"""
@Project -> File   ：pydantic-manager -> logconfig
@IDE    ：PyCharm
@Author ：Administrator
@Date   ：2021/7/5 0005 10:09
@Desc   ：
"""
import os
BASE_DIR = os.path.dirname(__file__)
LOG_PATH = os.sep.join((BASE_DIR, 'logs'))

if not os.path.exists(LOG_PATH):
    os.mkdir(LOG_PATH)

print('log path: {}'.format(LOG_PATH))
DEFAULT_LOGGING = {
    'version': 1,
    'disable_existing_loggers': True,
    'formatters': {
        # 日志格式
        'verbose': {
            'format': '[%(asctime)s] [%(name)s] [%(module)s:%(funcName)s:%(lineno)d] '
                      '[%(levelname)s]- %(message)s'},
        'simple': {  # 简单格式
            'format': '%(message)s'
        },
    },
    # 过滤
    'filters': {
    },
    # 定义具体处理日志的方式
    'handlers': {
        # 默认记录所有日志
        'manager': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': '{}/manager.log'.format(LOG_PATH),
            'when': 'D',  # 文件大小
            'backupCount': 5,  # 备份数
            'formatter': 'verbose',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码，否则打印出来汉字乱码
        },
        'sql': {
            'level': 'DEBUG',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': '{}/sql.log'.format(LOG_PATH),
            'when': 'D',  # 文件大小
            'backupCount': 5,  # 备份数
            'formatter': 'verbose',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码，否则打印出来汉字乱码
        },
        'error': {
            'level': 'ERROR',
            'class': 'logging.handlers.TimedRotatingFileHandler',
            'filename': '{}/error.log'.format(LOG_PATH),
            'when': 'D',  # 文件大小
            'backupCount': 5,  # 备份数
            'formatter': 'verbose',  # 输出格式
            'encoding': 'utf-8',  # 设置默认编码
        },

        # 控制台输出
        'console': {
            'level': 'DEBUG',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    # 配置用哪几种 handlers 来处理日志
    'loggers': {
        # 类型 为 django 处理所有类型的日志， 默认调用
        'pydantic': {
            'handlers': ['manager', 'console', 'error', 'sql'],
            'level': 'DEBUG',
            'propagate': False
        },
        # log 调用时需要当作参数传入
        'error': {
            'handlers': ['error', 'console'],
            'level': 'ERROR',
            'propagate': False
        },

    }
}