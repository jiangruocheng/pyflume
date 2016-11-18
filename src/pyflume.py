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

from logging.handlers import TimedRotatingFileHandler
from multiprocessing import Process, Queue

from channel import ChannelProxy
from agent import AgentProxy
from collector import CollectorProxy


class Pyflume(object):
    def __init__(self, config):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        # 先初始化channel
        self.channel = ChannelProxy(config)
        self.agent = AgentProxy(config)
        self.collector = CollectorProxy(config)
        self.pids = list()
        self.processes = list()

        signal.signal(signal.SIGTERM, self.kill)

    def kill(self, *args, **kwargs):
        self.log.debug('pids:' + str(self.pids))
        self.log.info('Waiting subprocess exit...')
        for _pid in self.pids:
            try:
                os.kill(_pid, signal.SIGTERM)
                time.sleep(1)
            except:
                pass

    def run(self):
        self.log.info('Pyflume starts.')
        _collector_process = Process(name='pyflume-collector',
                                     target=self.collector.run,
                                     kwargs={'channel': self.channel})
        _collector_process.start()
        self.pids.append(_collector_process.pid)
        self.processes.append(_collector_process)

        self.agent.run(channel=self.channel)
        self.pids += self.agent.pids
        self.processes += self.agent.processes

        signal.pause()

        for _process in self.processes:
            _process.join()

        self.log.info('Pyflume ends.')


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
            exit()
    else:
        print 'please import configure file.'
        exit()

    log_path = config.get('LOG', 'LOG_FILE')
    handler = TimedRotatingFileHandler(log_path, "midnight", 1)
    formatter = '%(asctime)s - %(filename)s:%(lineno)s - %(levelname)s - %(name)s - %(message)s'
    handler.setFormatter(logging.Formatter(formatter))
    level = logging.DEBUG if config.get('LOG', 'DEBUG') == 'True' else logging.ERROR
    logger = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
    logger.setLevel(level)
    logger.addHandler(handler)

    pyflume = Pyflume(config)
    pyflume.run()
