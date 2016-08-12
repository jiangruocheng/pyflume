#! -*- coding:utf-8 -*-

import os
import sys
import traceback

from pyflume import Pyflume
from config import POOL_PATH, Lg

# 将当前路径添加到系统路径中
_basedir = os.path.abspath(os.path.dirname(__file__))
if _basedir not in sys.path:
    sys.path.insert(0, _basedir)


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


if __name__ == '__main__':

    while True:
        handlers = get_handlers()
        Pyflume(handlers).run()
