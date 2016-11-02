#! -*- coding:utf-8 -*-

import os
import sys
import signal
import logging

from kafka import KafkaProducer
from kafka.errors import KafkaError


class Collector(object):

    def __init__(self, config=None):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.channel = None
        self.exit_flag = False

    def process_data(self):

        while not self.exit_flag:
            chn = self.channel()
            _data = chn.get()
            sys.stdout.write(_data)
            sys.stdout.flush()

    def run(self, channel=None):

        def _exit(*args, **kwargs):
            self.log.info('Received sigterm, collector is going down.')
            self.exit_flag = True
            # 解除阻塞
            self.channel().put('stop')

        signal.signal(signal.SIGTERM, _exit)

        self.log.info('Pyflume collector starts.')
        self.channel = channel
        self.process_data()
        self.log.info('Pyflume collector ends.')


class KafkaCollector(Collector):

    def __init__(self, config=None):
        super(KafkaCollector, self).__init__(config)
        self.log.debug(config.get('OUTPUT', 'SERVER'))
        self.producer = KafkaProducer(bootstrap_servers=[config.get('OUTPUT', 'SERVER')])
        self.topic = config.get('OUTPUT', 'TOPIC')

    def process_data(self, file_name='', data=None):
        _data = file_name + ': ' + str(data)
        self.log.debug('[collector]' + _data)

        future = self.producer.send(self.topic, _data)
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            self.log.exception()
            return None

        return record_metadata.topic, record_metadata.partition, record_metadata.offset
