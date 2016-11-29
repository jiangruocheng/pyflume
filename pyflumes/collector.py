#! -*- coding:utf-8 -*-

import logging

from collectors.base import Collector
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

    def register_collectors(self, channel_proxy):
        for c in self.collectors.itervalues():
            c.do_register(channel_proxy)
