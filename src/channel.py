#! -*- coding:utf-8 -*-

from multiprocessing import Queue


class MemoryChannel(object):
    def __init__(self):
        self.queue = Queue()

    def get(self, timeout=None):

        return self.queue.get(timeout=timeout)

    def put(self, data):

        self.queue.put(data)


class FileChannel(object):
    pass


class ChannelProxy(object):

    def __init__(self, config):
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
                    raise Exception('NotImplemented')

    def __call__(self, *args, **kwargs):
        name = kwargs.get('channel_name', '')
        if not name:
            raise Exception('未知的channel')
        return self.channels[name]
