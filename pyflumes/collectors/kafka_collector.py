#! -*- coding:utf-8 -*-

import traceback

from kafka import KafkaProducer
from kafka.errors import KafkaError

from base import Collector


class KafkaCollector(Collector):
    def __init__(self, config, section):
        super(KafkaCollector, self).__init__(config, section)
        self.log.debug(config.get(section, 'SERVER'))
        self.kfk_server = self.config.get(self.section, 'SERVER')
        self.topic = config.get(section, 'TOPIC')

    def process_data(self, msg):
        result = 'ok'
        _data = msg['filename'] + ': ' + msg['data']
        self.log.debug(msg['collectors'] + _data)

        producer = KafkaProducer(bootstrap_servers=self.kfk_server)

        future = producer.send(self.topic, _data)
        # Block for 'synchronous' sends
        try:
            record_metadata = future.get(timeout=10)
        except KafkaError:
            # Decide what to do if produce request failed...
            self.log.error(traceback.format_exc())
            result = 'Fail'
        finally:
            producer.close()

        # return record_metadata.topic, record_metadata.partition, record_metadata.offset
        return result,


