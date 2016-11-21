#! -*- coding:utf-8 -*-

import signal
import logging

from threading import Thread

from collectors.base import event, Collector
from collectors.hive_collector import HiveCollector
from collectors.kafka_collector import KafkaCollector
from collectors.socket_collector import SockCollector


class CollectorProxy(object):
    def __init__(self, config):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.exit_flag = False
        self.collectors = dict()
        for section in config.sections():
            if section.startswith('COLLECTOR:'):
                collector_type = config.get(section, 'TYPE')
                collector_name = section[section.find(':') + 1:]
                if collector_type.lower() == 'kafka':
                    self.collectors[collector_name] = KafkaCollector(config, section)
                elif collector_type.lower() == 'socket':
                    self.collectors[collector_name] = SockCollector(config, section)
                elif collector_type.lower() == 'hive':
                    self.collectors[collector_name] = HiveCollector(config, section)
                else:
                    self.collectors[collector_name] = Collector(config, section)

    def exit(self, *args, **kwargs):
        self.exit_flag = True
        event.clear()

    def run(self, channel=None):
        self.log.info('Pyflume collector starts.')
        event.set()
        tasks = list()
        for col_name, collector in self.collectors.iteritems():
            t = Thread(target=collector.run,
                       name=col_name,
                       kwargs={'channel': channel})
            tasks.append(t)
            t.start()

        signal.signal(signal.SIGTERM, self.exit)
        while not self.exit_flag:
            signal.pause()

        for _t in tasks:
            _t.join()

        self.log.info('Pyflume collector ends.')
