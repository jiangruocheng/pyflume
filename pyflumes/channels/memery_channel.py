#! -*- coding:utf-8 -*-

import os
import pickle

from multiprocessing import Queue
from multiprocessing.queues import Empty


class MemoryChannel(object):
    def __init__(self, config, section):
        self.queue = Queue()
        self.message_backup = config.get(section, 'MESSAGE_BACKUP')
        self.fetch_back_up_data()

    def fetch_back_up_data(self):
        if not os.path.exists(self.message_backup):
            return None
        with open(self.message_backup, 'r') as f:
            _list = pickle.load(f)
        for one in _list:
            self.put(one)

    def get(self, timeout=None):

        return self.queue.get(timeout=timeout)

    def put(self, data, timeout=30):

        self.queue.put(data, timeout=timeout)

    def __del__(self):
        _list = list()
        while True:
            try:
                _list.append(self.get(timeout=1))
            except Empty:
                break
        with open(self.message_backup, 'w') as f:
            pickle.dump(_list, f)
