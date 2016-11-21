#! -*- coding:utf-8 -*-

import logging

from channels.file_channel import FileChannel
from channels.memery_channel import MemoryChannel


class ChannelProxy(object):
    def __init__(self, config):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
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
        return self.channels[name]
