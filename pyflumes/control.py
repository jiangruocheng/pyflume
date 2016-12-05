#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import time
import signal
import logging
import traceback
import subprocess
import configparser

import logger as log_
import pyflume

from multiprocessing import Process, Value

CONFIG = None
PID = Value('i', 0)


def _init():
    # 初始化，创建必须目录
    os.system('bash -c "mkdir -vp /tmp/pyflume/{config/,logs/,scripts/,tmp/hive}"')


def reset():
    # 初始化目录，删除全部数据
    _dirs = os.listdir('/tmp/pyflume/tmp/')
    for _ in _dirs:
        try:
            os.remove('/tmp/pyflume/tmp/'+_)
        except:
            pass

    _dirs = os.listdir('/tmp/pyflume/tmp/hive/')
    for _ in _dirs:
        try:
            os.remove('/tmp/pyflume/tmp/hive/'+_)
        except:
            pass

    _dirs = os.listdir('/tmp/pyflume/config/')
    for _ in _dirs:
        try:
            os.remove('/tmp/pyflume/config/'+_)
        except:
            pass

    _dirs = os.listdir('/tmp/pyflume/scripts/')
    for _ in _dirs:
        try:
            os.remove('/tmp/pyflume/scripts/'+_)
        except:
            pass
    return 'Success'


def _init_log():
    # 初始化log配置
    log_path = CONFIG.get('LOG', 'LOG_FILE')
    a = log_.MyTimedRotatingFileHandler
    handler = a(log_path, "midnight", 1)
    formatter = '%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(name)s - %(message)s'
    handler.setFormatter(logging.Formatter(formatter))
    level = logging.DEBUG if CONFIG.get('LOG', 'DEBUG') == 'True' else logging.INFO
    logger = logging.getLogger(CONFIG.get('LOG', 'LOG_HANDLER'))
    logger.setLevel(level)
    logger.handlers = []
    logger.addHandler(handler)


def config(content=''):
    _init()
    # 导入配置文件
    global CONFIG
    try:
        if content:
            with open('/tmp/pyflume/config/default.cfg', 'w') as f:
                f.write(content)
        CONFIG = configparser.ConfigParser()
        CONFIG.read('/tmp/pyflume/config/default.cfg')
        _init_log()
    except:
        return traceback.format_exc()

    return 'Success'


def read_config():
    # 导入配置文件
    if not os.path.exists('/tmp/pyflume/config/default.cfg'):
        return ''

    try:
        with open('/tmp/pyflume/config/default.cfg', 'r') as f:
            c = f.read()
    except:
        return traceback.format_exc()

    return c


def is_running():
    global PID
    if PID.value == 0:
        return False
    try:
        subprocess.check_output("ps aux | grep {} | grep pyflume | grep -v grep".format(PID.value), shell=True)
    except subprocess.CalledProcessError:
        PID.value = 0
        return False
    return True


def start():
    global CONFIG, PID
    if CONFIG is None:
        config()
    if is_running():
        return 'Pylfume is still running.'
    try:
        t = Process(target=pyflume.run,
                    args=(CONFIG, PID))
        t.daemon = False  # 等待子进程fork完Pyflume.run进程
        t.start()
    except:
        return traceback.format_exc()

    time.sleep(3)

    return 'Success, pyflume[{}] is running.'.format(PID.value)


def stop():
    global PID
    if not is_running():
        return 'Pylfume is not running.'
    try:
        os.kill(PID.value, signal.SIGTERM)
        while is_running():
            time.sleep(1)
    except:
        return traceback.format_exc()
    return 'Success'


def upload_script(script_name, content):
    _path = os.path.join('/tmp/pyflume/scripts/', script_name)
    with open(_path, 'w') as f:
        f.write(content)
    return 'Success'


# 对外提供的rpc调用接口
FUNCTIONS = [reset, start, stop, config, read_config, upload_script, is_running]
