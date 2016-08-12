#! -*- coding:utf-8 -*-

import os
import pickle
import traceback
import configuration as conf

from time import sleep
from collector import Collector


class Pyflume(object):

    def __init__(self, config):
        self.handlers = self.get_handlers(config)
        self._pickle_path = config.pickle_path
        self._max_read_line = config.max_read_line
        self.pickle_handler = None
        self._pickle_data = None

    def _load_pickle(self):
        if not os.path.exists(self._pickle_path):
            try:
                self.pickle_handler = open(self._pickle_path, 'w+')
            except IOError, e:
                conf.LG.error(e)
                exit(-1)

        self.pickle_handler = open(self._pickle_path, 'r+')
        try:
            self._pickle_data = pickle.load(self.pickle_handler)
        except EOFError:
            conf.LG.warning('No pickle data exits.')
            self._pickle_data = dict()

    def _get_log_data_by_handler(self, handler):
        # 先获取文件上次读取的偏移量
        _file_name = handler.name
        try:
            _offset = self._pickle_data[_file_name]
        except KeyError:
            conf.LG.warning('Cant find "%s" in pickles.' % _file_name)
            _offset = 0
        handler.seek(_offset)
        _data = handler.readlines(self._max_read_line)
        if _data:
            yield _data

    @staticmethod
    def get_handlers(conf):
        _handlers = list()
        _files = os.listdir(conf.pool_path)
        for _file in _files:
            if 'COMPLETED' != _file.split('.')[-1]:
                if os.path.isfile(conf.pool_path + '/' + _file):
                    try:
                        _handler = open(conf.pool_path + '/' + _file, 'r')
                        _handlers.append(_handler)
                    except IOError:
                        conf.LG.error(traceback.format_exc())
                        return []

        return _handlers
    
    def run(self):
        self._load_pickle()
        for _handler in self.handlers:
            # 现阶段防止CPU占用率过高,故在此休眠一段时间
            sleep(0.1)
            for _data in self._get_log_data_by_handler(_handler):
                if not _data:
                    break
                _result_flag = Collector().process_data(_handler, _data)
                if _result_flag:
                    # 数据已成功采集,更新offset
                    self._pickle_data[_handler.name] = _handler.tell()

        # 从文件读取_pickle_data时offset已经改变,现在重置到0
        self.pickle_handler.seek(0)
        pickle.dump(self._pickle_data, self.pickle_handler)
        self.pickle_handler.close()
