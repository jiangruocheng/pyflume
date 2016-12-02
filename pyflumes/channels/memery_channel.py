#! -*- coding:utf-8 -*-

import os
import uuid
import pickle

from multiprocessing import Queue
from multiprocessing.queues import Empty

from pyflumes.channels.base import ChannelBase
from pyflumes.wrappers import channel_lock


class MemoryChannel(ChannelBase):
    def __init__(self, config, section):
        super(MemoryChannel, self).__init__(config, section)
        self.queue = Queue()
        self.message_backup = os.path.join(config.get('GLOBAL', 'DIR'), 'tmp/'+'MESSAGE.BACKUP.'+self.name)
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

    @channel_lock
    def put(self, data, timeout=30):
        if isinstance(data, dict):
            if 'id' not in data:
                data['id'] = str(uuid.uuid4())
        self.queue.put(data, timeout=timeout)

    def __del__(self):
        _list = list()
        while True:
            try:
                _list.append(self.get(timeout=1))
            except (EOFError, Empty):
                break
        with open(self.message_backup, 'w') as f:
            pickle.dump(_list, f)
