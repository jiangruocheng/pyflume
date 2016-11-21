#! -*- coding:utf-8 -*-

import traceback

from kafka import KafkaProducer
from kafka.errors import KafkaError

from base import Collector, event


class KafkaCollector(Collector):
    def __init__(self, config, section):
        super(KafkaCollector, self).__init__(config, section)
        self.log.debug(config.get(section, 'SERVER'))
        self.channel_name = self.config.get(section, 'CHANNEL')
        self.kfk_server = self.config.get(self.section, 'SERVER')
        self.topic = config.get(section, 'TOPIC')
        self.producer = None

    def process_data(self, msg):
        _data = msg['filename'] + ': ' + msg['data']
        self.log.debug(msg['collector'] + _data)

        self.producer = KafkaProducer(bootstrap_servers=self.kfk_server)

        future = self.producer.send(self.topic, _data)
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            self.log.error(traceback.format_exc())
            return None
        finally:
            self.producer.close()

        return record_metadata.topic, record_metadata.partition, record_metadata.offset


