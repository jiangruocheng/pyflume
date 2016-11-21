#! -*- coding:utf-8 -*-

import os


class FileChannel(object):
    def __init__(self, config, section):
        self.store_dir = config.get(section, 'STORE_DIR')
        self.file_max_size = int(config.get(section, 'FILE_MAX_SIZE'))
        self.ignore_postfix = config.get(section, 'IGNORE_POSTFIX')

    def extract_file_name(self, data):

        return data.get('filename', 'UnKnown')

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

    def get(self):
        name, size = self.fetch_full_size_file()
        if size > self.file_max_size:
            return os.path.join(self.store_dir, name)
        else:
            return ''

    def put(self, data):
        file_name = self.extract_file_name(data)
        with open(os.path.join(self.store_dir, file_name), 'a') as f:
            f.write(data.get('data'))
