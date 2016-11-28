#! -*- coding:utf-8 -*-

import sys
import logging

from threading import Event


event = Event()


class Collector(object):
    def __init__(self, config=None, section=''):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.name = section[section.find(':') + 1:]
        self.channel_name = config.get(section, 'CHANNEL')
        self.channel = None

    def do_register(self, channel_proxy):
        self.channel = channel_proxy(channel_name=self.channel_name)
        self.channel.register(self.name, self.process_data)

    def process_data(self, msg):
        sys.stdout.write(self.name + ': ' + str(msg))
        sys.stdout.flush()
        return 'ok',
