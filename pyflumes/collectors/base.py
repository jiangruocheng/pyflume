#! -*- coding:utf-8 -*-

import sys
import logging
import traceback

from multiprocessing.queues import Empty
from threading import Event


event = Event()


class Collector(object):
    def __init__(self, config=None, section=''):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.channel_name = config.get(section, 'CHANNEL')
        self.channel = None

    def process_data(self, msg):
        sys.stdout.write(msg)
        sys.stdout.flush()

    def run(self, *args, **kwargs):
        """This maybe implemented by subclass"""
        chn = kwargs.get('channel', None)
        if not chn:
            self.log.error('Channel should not be lost.')
            raise Exception('Channel should not be lost.')
        self.channel = chn(channel_name=self.channel_name)
        while event.wait(0):
            try:
                data = self.channel.get(timeout=10)
            except Empty:
                continue
            try:
                self.process_data(data)
            except:
                self.log.error(traceback.format_exc())
