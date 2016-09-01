#! -*- coding:utf-8 -*-

import os
import select
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
            return _data
        else:
            return None

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

        while True:
            self._handlers = self.get_handlers(self._config)
            self._load_pickle()
            kq = select.kqueue()
            _monitor_list = list()
            _monitor_dict = dict()
            for _handler in self._handlers:
                _monitor_list.append(
                    select.kevent(_handler.fileno(), filter=select.KQ_FILTER_READ, flags=select.KQ_EV_ADD)
                )
                _monitor_dict[_handler.fileno()] = _handler

            # 此时另一个线程负责监控文件的变动,例如新文件的移入,另一个线程会发送信号终止这个循环,重新遍历获取所有文件的句柄.
            while True:
                revents = kq.control(_monitor_list, 2)
                for event in revents:
                    if event.filter == select.KQ_FILTER_READ:
                        _in_process_file_handler = _monitor_dict[event.ident]
                        _data = self._get_log_data_by_handler(_in_process_file_handler)
                        if not _data:
                            continue
                        _result_flag = Collector().process_data(_in_process_file_handler, _data)
                        if _result_flag:
                            # 数据已成功采集,更新offset
                            self._pickle_data[_in_process_file_handler.name] = _in_process_file_handler.tell()
                            # 从文件读取_pickle_data时offset已经改变,现在重置到0
                            self.pickle_handler.seek(0)
                            pickle.dump(self._pickle_data, self.pickle_handler)

            self.pickle_handler.close()
