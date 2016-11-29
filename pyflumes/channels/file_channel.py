#! -*- coding:utf-8 -*-

import os
import time
import traceback

from pyflumes.channels.base import ChannelBase


class FileChannel(ChannelBase):
    def __init__(self, config, section):
        super(FileChannel, self).__init__(config, section)
        self.store_dir = config.get(section, 'STORE_DIR')
        self.file_max_size = int(config.get(section, 'FILE_MAX_SIZE'))
        self.ignore_postfix = config.get(section, 'IGNORE_POSTFIX')
        self.handlers = dict()

    def extract_file_name(self, data):
        name = os.path.basename(data.get('filename', ''))
        if not name:
            self.log.warning('Incorrect format: ' + str(data))
        return name

    def list_files_size(self):
        f_size = dict()
        file_names = os.listdir(self.store_dir)
        for name in file_names:
            if name.endswith(self.ignore_postfix):
                continue
            size = os.path.getsize(os.path.join(self.store_dir, name))
            f_size[name] = size

        return f_size

    def fetch_full_size_file(self):
        cwm = 0
        _file_name = ''
        for name, size in self.list_files_size().iteritems():
            if size > cwm:
                cwm = size
                _file_name = name
        return _file_name, cwm

    def get(self, *args, **kwargs):
        name, size = self.fetch_full_size_file()
        if size > self.file_max_size:
            return os.path.join(self.store_dir, name)
        else:
            return ''

    def put(self, data):
        file_name = self.extract_file_name(data)
        if not file_name:
            return None

        f = self.handlers.get(file_name, '')
        if not f:
            f = open(os.path.join(self.store_dir, file_name), 'a')
            self.handlers[file_name] = f
        f.write(data.get('data'))
        f.flush()

    def handout(self, event):
        self.log.info(self.name + ' [{}] starts'.format(os.getpid()))

        while event.wait(timeout=0):
            try:
                data = self.get()
                func = self.call_backs.get('hive', '')
                # 此channel目前只支持hive collector
                func(data)
            except:
                self.log.warning(traceback.format_exc())
            time.sleep(30)
        self.log.info(self.name + ' [{}] starts'.format(os.getpid()))

    def __del__(self):
        for handler in self.handlers.itervalues():
            try:
                handler.close()
            except:
                pass
