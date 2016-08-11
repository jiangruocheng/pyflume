#! -*- coding:utf-8 -*-

import os
import pickle
import sys
import time
import traceback

from config import Lg, POOL_PATH, PICKLE_PATH, MAX_READ_LINE

# 将当前路径添加到系统路径中
_basedir = os.path.abspath(os.path.dirname(__file__))
if _basedir not in sys.path:
    sys.path.insert(0, _basedir)

handlers = list()
pickle_handler = None
pickle_data = None


def load_pickle():
    global pickle_handler, pickle_data
    if not os.path.exists(PICKLE_PATH):
        try:
            pickle_handler = open(PICKLE_PATH, 'w+')
        except IOError, e:
            Lg.error(e)
            exit(-1)

    pickle_handler = open(PICKLE_PATH, 'r+')
    try:
        pickle_data = pickle.load(pickle_handler)
    except EOFError:
        Lg.warning('No pickle data exits.')
        pickle_data = dict()


def get_handlers():
    _handlers = list()
    _files = os.listdir(POOL_PATH)
    for _file in _files:
        if 'COMPLETED' != _file.split('.')[-1]:
            if os.path.isfile(POOL_PATH + '/' + _file):
                try:
                    _handler = open(POOL_PATH + '/' + _file, 'r')
                    _handlers.append(_handler)
                except IOError:
                    Lg.error(traceback.format_exc())
                    return []

    return _handlers


def get_log_data_by_handler(handler):
    # 先获取文件上次读取的偏移量
    _file_name = handler.name
    try:
        _offset = pickle_data[_file_name]
    except KeyError:
        Lg.warning('Cant find "%s" in pickles.' % _file_name)
        _offset = 0
    handler.seek(_offset)
    _data = handler.readlines(MAX_READ_LINE)
    if _data:
        yield _data
    else:
        yield None


def process_data(handler, data):
    from random import random
    if random() < 0.01:
        print handler.name
        return False

    return True


def run():
    global handlers, pickle_handler
    load_pickle()
    for _handler in handlers:
        for _data in get_log_data_by_handler(_handler):
            if not _data:
                break
            _result_flag = process_data(_handler, _data)
            if _result_flag:
                # 数据已成功采集,更新offset
                pickle_data[_handler.name] = _handler.tell()

    # 从文件读取pickle_data时offset已经改变,现在重置到0
    pickle_handler.seek(0)
    pickle.dump(pickle_data, pickle_handler)
    pickle_handler.close()


if __name__ == '__main__':
    global handlers
    handlers = get_handlers()
    while True:
        time.sleep(0.1)
        run()
