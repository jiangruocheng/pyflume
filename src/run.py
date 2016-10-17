#! -*- coding:utf-8 -*-

import os
import sys
import signal
import logging
import argparse
import configparser

from logging.handlers import TimedRotatingFileHandler

from pyflume import Pyflume

# 将当前路径添加到系统路径中
_basedir = os.path.abspath(os.path.dirname(__file__))
if _basedir not in sys.path:
    sys.path.insert(0, _basedir)

if __name__ == '__main__':

    signal.signal(signal.SIGUSR1, lambda: 0)
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
        # 使用默认配置
        print 'please import configure file.'
        exit()

    log_path = config.get('LOG', 'LOG_FILE')
    handler = TimedRotatingFileHandler(log_path, "midnight", 1)
    formatter = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
    handler.setFormatter(logging.Formatter(formatter))
    level = logging.DEBUG
    logger = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
    logger.setLevel(level)
    logger.addHandler(handler)

    pyflume = Pyflume(config)
    pyflume.run()

