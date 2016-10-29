#! -*- coding:utf-8 -*-

import Queue
import logging
import threading

from kafka import KafkaProducer
from kafka.errors import KafkaError


def get_collector(name=''):
    if name.lower() == 'kafka':
        return KafkaCollector
    else:
        return Collector


class Collector(object):

    def __init__(self, config=None):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.queue = Queue.Queue()

        self.get_data = threading.Thread(target=self.process_data, name='collector')
        self.get_data.start()

    def put_data(self, *args, **kwargs):
        """This function should be implemented by sub-class."""
        try:
            call_back_funtcion = kwargs['call_back_function']
            file_handler = kwargs['file_handler']
            data = kwargs['data']
        except:
            self.queue.put('stop')
        else:
            self.queue.put((call_back_funtcion, file_handler, data))

    def process_data(self):

        while True:
            result = self.queue.get()
            if 'stop' == result:
                break
            elif isinstance(result, tuple) and 3 == len(result):
                func, file_handler, data = result
            else:
                continue
            for line in data:
                _data = file_handler.name + ': ' + line.decode('utf-8')
                self.log.info(_data)
                print _data
            func(file_handler)


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
