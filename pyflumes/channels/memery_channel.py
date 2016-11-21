#! -*- coding:utf-8 -*-

from multiprocessing import Queue


class MemoryChannel(object):
    def __init__(self, config, section):
        self.queue = Queue()

    def get(self, timeout=None):

        return self.queue.get(timeout=timeout)

    def put(self, data):

        self.queue.put(data)
