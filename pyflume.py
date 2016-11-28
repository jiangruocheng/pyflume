#! -*- coding:utf-8 -*-

import os
import sys

# 将当前路径添加到系统路径中
_basedir = os.path.abspath(os.path.dirname(__file__))
if _basedir not in sys.path:
    sys.path.insert(0, _basedir)

import time
import signal
import logging
import argparse
import configparser

from multiprocessing import Event

from pyflumes.channel import ChannelProxy
from pyflumes.agent import AgentProxy
from pyflumes.collector import CollectorProxy
from pyflumes.logger import MyTimedRotatingFileHandler


class Pyflume(object):
    def __init__(self, config):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        # 先初始化channel
        self.channel = ChannelProxy(config)
        self.agent = AgentProxy(config)
        self.collector = CollectorProxy(config)
        self.pids = list()
        self.processes = list()
        self.global_event = None

        signal.signal(signal.SIGTERM, self.kill)

    def kill(self, *args, **kwargs):
        self.global_event.clear()
        for _pid in self.pids:
            try:
                os.kill(_pid, signal.SIGTERM)
                time.sleep(2)
            except:
                pass

    def run(self):
        self.log.info('Pyflume starts.')
        # 向channel注册collector
        self.collector.register_collectors(self.channel)

        self.global_event = Event()
        self.global_event.set()

        # 启动channel
        self.channel.run(event=self.global_event)
        self.pids += self.channel.pids
        self.processes += self.channel.processes

        # 启动poll
        self.agent.run(channel=self.channel, event=self.global_event)
        self.pids += self.agent.pids
        self.processes += self.agent.processes

        flag = True
        while flag:
            time.sleep(30)
            for _p in self.processes:
                if not _p.is_alive():
                    self.log.warning(_p.name + ' is not alive.')
                    self.log.warning('Try to end pyflume.')
                    self.kill()
                    flag = False
                    break

        for _process in self.processes:
            _process.join()

        self.log.info('Pyflume ends.')


def start():
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
            sys.exit(0)
    else:
        print 'please import configure file.'
        sys.exit(0)

    log_path = config.get('LOG', 'LOG_FILE')
    handler = MyTimedRotatingFileHandler(log_path, "midnight", 1)
    formatter = '%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(name)s - %(message)s'
    handler.setFormatter(logging.Formatter(formatter))
    level = logging.DEBUG if config.get('LOG', 'DEBUG') == 'True' else logging.WARNING
    logger = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
    logger.setLevel(level)
    logger.addHandler(handler)

    pyflume = Pyflume(config)
    pyflume.run()

if __name__ == '__main__':

    start()
