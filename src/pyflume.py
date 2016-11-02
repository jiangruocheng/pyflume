#! -*- coding:utf-8 -*-

import os
import signal
import logging

from multiprocessing import Process, Queue

from agent import Agent
from collector import Collector


class Pyflume(object):

    def __init__(self, config):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.queue = Queue()
        self.agent = Agent(config)
        self.collector = Collector(config)
        self.agent_pid = None
        self.collector_pid = None

        signal.signal(signal.SIGTERM, self.kill)

    def channel(self, *args, **kwargs):

        return self.queue

    def kill(self, *args, **kwargs):
        os.kill(self.agent_pid, signal.SIGTERM)
        os.kill(self.collector_pid, signal.SIGTERM)

    def run(self):
        self.log.info('Pyflume starts.')
        _agent_process = Process(name='pyflume-agent',
                                 target=self.agent.run,
                                 kwargs={'channel': self.channel})
        _collector_process = Process(name='pyflume-collector',
                                     target=self.collector.run,
                                     kwargs={'channel': self.channel})

        _agent_process.start()
        _collector_process.start()

        self.agent_pid = _agent_process.pid
        self.collector_pid = _collector_process.pid
        self.log.debug('agent pid: ' + str(self.agent_pid))
        self.log.debug('collector pid: ' + str(self.collector_pid))

        signal.pause()

        _agent_process.join()
        _collector_process.join()
        self.log.info('Pyflume ends.')
