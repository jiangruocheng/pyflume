#! -*- coding:utf-8 -*-

import os
import logging


class FileChannel(object):
    def __init__(self, config, section):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
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

    def __del__(self):
        for handler in self.handlers.itervalues():
            try:
                handler.close()
            except:
                pass
