#! -*- coding:utf-8 -*-

import os
import pickle

from time import sleep
from collector import  Collector
from config import Lg, PICKLE_PATH, MAX_READ_LINE


class Pyflume(object):

    def __init__(self, handlers):
        self.handlers = handlers
        self.pickle_handler = None
        self._pickle_data = None

    def load_pickle(self):
        if not os.path.exists(PICKLE_PATH):
            try:
                self.pickle_handler = open(PICKLE_PATH, 'w+')
            except IOError, e:
                Lg.error(e)
                exit(-1)

        self.pickle_handler = open(PICKLE_PATH, 'r+')
        try:
            self._pickle_data = pickle.load(self.pickle_handler)
        except EOFError:
            Lg.warning('No pickle data exits.')
            self._pickle_data = dict()

    def get_log_data_by_handler(self, handler):
        # 先获取文件上次读取的偏移量
        _file_name = handler.name
        try:
            _offset = self._pickle_data[_file_name]
        except KeyError:
            Lg.warning('Cant find "%s" in pickles.' % _file_name)
            _offset = 0
        handler.seek(_offset)
        _data = handler.readlines(MAX_READ_LINE)
        if _data:
            yield _data

    def run(self):
        self.load_pickle()
        for _handler in self.handlers:
            # 现阶段防止CPU占用率过高,故在此休眠一段时间
            sleep(0.1)
            for _data in self.get_log_data_by_handler(_handler):
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
