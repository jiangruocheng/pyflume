#! -*- coding:utf-8 -*-

import sys
import time
import signal
import logging
import traceback

from socket import socket, AF_INET, SOCK_STREAM
from threading import Thread, Event
from kafka import KafkaProducer
from kafka.errors import KafkaError

event = Event()


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

    def exit(self):
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
            time.sleep(60)

        for _t in tasks:
            _t.join()

        self.log.info('Pyflume collector ends.')


class Collector(object):
    def __init__(self, config=None, section=''):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.channel_name = config.get(section, 'CHANNEL')
        self.channel = None

    def process_data(self, msg):
        _data = msg['filename'] + ': ' + msg['data']
        sys.stdout.write(_data)
        sys.stdout.flush()

    def run(self, *args, **kwargs):
        """This maybe implemented by subclass"""
        chn = kwargs.get('channel', None)
        if not chn:
            self.log.error('Channel should not be lost.')
            raise Exception('Channel should not be lost.')
        self.channel = chn(channel_name=self.channel_name)
        while event.wait():
            data = self.channel.get(timeout=10)
            self.process_data(data)


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


class SockCollector(Collector):
    def __init__(self, config, section):
        super(SockCollector, self).__init__(config, section)
        self.server_ip = config.get(section, 'SERVER_IP')
        self.server_port = int(config.get(section, 'SERVER_PORT'))

    def process_data(self, msg):
        _data = msg['filename'] + ': ' + msg['data']
        self.log.debug(msg['collector'] + _data)

        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((self.server_ip, self.server_port))

        try:
            while True:
                sock.sendall(_data + '(EOF)')
                result = sock.recv(1024)
                if 'success' != result:
                    self.log.debug('Unsuccessful sending data.')
                    time.sleep(30)
                    continue
                else:
                    break
        except:
            self.log.debug(_data)
            self.log.error(traceback.format_exc())
        finally:
            sock.close()


class HiveCollector(Collector):
    def __init__(self, config, section):
        super(HiveCollector, self).__init__(config, section)
        self.ip = config.get(section, 'HIVE_IP')
        self.port = int(config.get(section, 'HIVE_PORT'))
        self.name = config.get(section, 'HIVE_USER_NAME')
        self.channel_name = config.get(section, 'CHANNEL')

    def run(self, *args, **kwargs):
        # TODO
        pass
