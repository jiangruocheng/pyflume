#! -*- coding:utf-8 -*-

import logging

from multiprocessing import Process

from weakref import proxy

from channels.file_channel import FileChannel
from channels.memery_channel import MemoryChannel


class ChannelProxy(object):
    def __init__(self, config):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.processes = list()
        self.pids = list()
        self.channels = dict()
        for section in config.sections():
            if section.startswith('CHANNEL:'):
                channel_type = config.get(section, 'TYPE')
                channel_name = section[section.find(':') + 1:]
                if channel_type.lower() == 'memory':
                    self.channels[channel_name] = MemoryChannel(config, section)
                elif channel_type.lower() == 'file':
                    self.channels[channel_name] = FileChannel(config, section)
                else:
                    self.log.error('NotImplemented')
                    raise Exception('NotImplemented')

    def __call__(self, *args, **kwargs):
        name = kwargs.get('channel_name', '')
        if not name:
            self.log.error('未知的channel')
            raise Exception('未知的channel')
        return proxy(self.channels[name])

    def run(self, *args, **kwargs):
        event = kwargs.get('event')
        for name, channel in self.channels.iteritems():
            _channel_process = Process(name=name,
                                       target=channel.handout,
                                       args=(event,))
            _channel_process.start()
            self.processes.append(_channel_process)
            self.pids.append(_channel_process.pid)
