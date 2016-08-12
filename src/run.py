#! -*- coding:utf-8 -*-

import os
import sys
import argparse
import configuration

from pyflume import Pyflume

# 将当前路径添加到系统路径中
_basedir = os.path.abspath(os.path.dirname(__file__))
if _basedir not in sys.path:
    sys.path.insert(0, _basedir)





if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--configure', help='给定配置文件路径,导入配置')
    args = parser.parse_args()
    if args.configure:
        if os.path.exists(args.configure):
            # 导入配置文件
            _config = configuration.Configuration(args.configure)
        else:
            print 'No configure file exists.'
    else:
        # 使用默认配置
        _config = configuration.Configuration()

    _config.set_app_log_config()

    while True:
        Pyflume(_config).run()
