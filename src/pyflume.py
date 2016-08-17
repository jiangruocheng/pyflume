#! -*- coding:utf-8 -*-

import os
import pickle
import traceback
import configuration as conf

from time import sleep
from collector import Collector


class Pyflume(object):

    def __init__(self, config):
        self._config = config
        self._pickle_path = config.pickle_path
        self._max_read_line = config.max_read_line
        self.pickle_handler = None
        self._pickle_data = None
        self._handlers = list()
        self._time_seed = 0.2

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
        else:
            yield None

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
        _count = 0
        _sleep_time = self._time_seed
        while True:
            self._handlers = self.get_handlers(self._config)
            _threshold_value = len(self._handlers)
            self._load_pickle()
            for _handler in self._handlers:
                # 现阶段防止CPU占用率过高,故在此休眠一段时间
                if _count == _threshold_value:
                    sleep(_sleep_time)
                    _count = 0
                    if _sleep_time < 33:
                        _sleep_time *= 2
                for _data in self._get_log_data_by_handler(_handler):
                    if not _data:
                        _count += 1
                        break
                    _sleep_time = self._time_seed
                    _result_flag = Collector().process_data(_handler, _data)
                    if _result_flag:
                        # 数据已成功采集,更新offset
                        self._pickle_data[_handler.name] = _handler.tell()

            # 从文件读取_pickle_data时offset已经改变,现在重置到0
            self.pickle_handler.seek(0)
            pickle.dump(self._pickle_data, self.pickle_handler)
            self.pickle_handler.close()
