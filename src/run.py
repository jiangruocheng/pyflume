#! -*- coding:utf-8 -*-

import os
import sys
import signal
import logging
import argparse
import platform
import configparser

from logging.handlers import TimedRotatingFileHandler

from pyflume import InotifyPyflume, KqueuePyflume

# 将当前路径添加到系统路径中
_basedir = os.path.abspath(os.path.dirname(__file__))
if _basedir not in sys.path:
    sys.path.insert(0, _basedir)

if __name__ == '__main__':

    # 注册信号, 阻止进程接收到SIGUSR1后直接退出
    def func(*args, **kwargs):
        pass
    signal.signal(signal.SIGUSR1, func)

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--configure', help='给定配置文件路径,导入配置')
    args = parser.parse_args()
    if args.configure:
        if os.path.exists(args.configure):
            # 导入配置文件
            config = configparser.ConfigParser()
            config.read(args.configure)
        else:
            print 'No configure file exists.'
    else:
        print 'please import configure file.'
        exit()

    log_path = config.get('LOG', 'LOG_FILE')
    handler = TimedRotatingFileHandler(log_path, "midnight", 1)
    formatter = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
    handler.setFormatter(logging.Formatter(formatter))
    level = logging.DEBUG if config.get('LOG', 'DEBUG') == 'True' else logging.ERROR
    logger = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
    logger.setLevel(level)
    logger.addHandler(handler)

    if platform.system() == 'Linux':
        pyflume = InotifyPyflume(config)
    else:
        pyflume = KqueuePyflume(config)
    pyflume.run()
