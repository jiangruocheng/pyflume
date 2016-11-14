#! -*- coding:utf-8 -*-

import os
import signal
import logging

from multiprocessing import Process, Queue

from agent import AgentProxy
from collector import CollectorProxy


class Pyflume(object):

    def __init__(self, config):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.queue = Queue()
        self.agent = AgentProxy(config)
        self.collector = CollectorProxy(config)
        self.collector_pid = None

        signal.signal(signal.SIGTERM, self.kill)

    def channel(self, *args, **kwargs):

        return self.queue

    def kill(self, *args, **kwargs):
        for _pid in self.agent.pids:
            os.kill(_pid, signal.SIGTERM)
        os.kill(self.collector_pid, signal.SIGTERM)

    def run(self):
        self.log.info('Pyflume starts.')
        _collector_process = Process(name='pyflume-collector',
                                     target=self.collector.run,
                                     kwargs={'channel': self.channel})
        _collector_process.start()
        self.collector_pid = _collector_process.pid
        self.log.debug('collector pid: ' + str(self.collector_pid))

        self.agent.run(channel=self.channel)

        signal.pause()

        _collector_process.join()
        for agent_process in self.agent.processes:
            agent_process.join()

        self.log.info('Pyflume ends.')
