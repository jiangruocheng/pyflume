#! -*- coding:utf-8 -*-

import os
import time
import signal
import logging

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
