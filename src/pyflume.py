#! -*- coding:utf-8 -*-

import os
import sys
import select
import pickle
import signal
import logging
import threading
import traceback

from time import sleep
from collector import Collector
from select import KQ_FILTER_SIGNAL, KQ_FILTER_READ, KQ_EV_ADD


class Pyflume(object):

    def __init__(self, config):
        self._pickle_file = config.get('TEMP', 'PICKLE_FILE')
        self._max_read_line = int(config.get('POOL', 'MAX_READ_LINE'))
        self.pickle_handler = None
        self._pickle_data = None
        self._handlers = list()
        self._pool_path = config.get('POOL', 'POOL_PATH')
        self.logger = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))

    def _load_pickle(self):
        if not os.path.exists(self._pickle_file):
            try:
                self.pickle_handler = open(self._pickle_file, 'w+')
            except IOError:
                self.logger.error(traceback.format_exc())
                exit(-1)

        self.pickle_handler = open(self._pickle_file, 'r+')
        try:
            self._pickle_data = pickle.load(self.pickle_handler)
        except EOFError:
            self.logger.warning('No pickle data exits.')
            self._pickle_data = dict()

    def _get_log_data_by_handler(self, handler):
        # 先获取文件上次读取的偏移量
        _file_name = handler.name
        try:
            _offset = self._pickle_data[_file_name]
        except KeyError:
            self.logger.warning('Cant find "%s" in pickles.' % _file_name)
            _offset = 0
        handler.seek(_offset)
        _data = handler.readlines(self._max_read_line)
        if _data:
            return _data
        else:
            return None

    def get_handlers(self):
        _handlers = list()
        _files = os.listdir(self._pool_path)
        for _file in _files:
            if 'COMPLETED' != _file.split('.')[-1]:
                if os.path.isfile(self._pool_path + _file):
                    try:
                        _handler = open(self._pool_path + _file, 'r')
                        _handlers.append(_handler)
                    except IOError:
                        self.logger.error(traceback.format_exc())
                        return []

        return _handlers
    
    def run(self):

        # _thread_move = threading.Thread(target=self.monitor_file_move, name='monitor_file_move')
        _thread_content = threading.Thread(target=self.monitor_file_content, name='monitor_file_content')

        # _thread_move.start()
        _thread_content.start()

        _thread_content.join()
        # _thread_move.join()

    def monitor_file_move(self):

        while True:
            sleep(10)
            break

    def monitor_file_content(self):

        while 1:
            break_flag = True
            self._handlers = self.get_handlers()
            self._load_pickle()
            kq = select.kqueue()
            _monitor_list = list()
            _monitor_dict = dict()
            for _handler in self._handlers:
                _monitor_list.append(
                    select.kevent(_handler.fileno(), filter=KQ_FILTER_READ, flags=KQ_EV_ADD)
                )
                _monitor_dict[_handler.fileno()] = _handler
            _monitor_list.append(
                select.kevent(signal.SIGUSR1, filter=KQ_FILTER_SIGNAL, flags=KQ_EV_ADD)
            )
            try:
                # 此时另一个线程负责监控文件的变动,例如新文件的移入,另一个线程会发送信号终止这个循环,重新遍历获取所有文件的句柄.
                while break_flag:
                    revents = kq.control(_monitor_list, 3)
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
                        elif event.filter == select.KQ_FILTER_SIGNAL:
                            if event.ident == signal.SIGUSR1:
                                self.logger.info('捕捉到信号SIGUSR1,重新获取文件句柄')
                                break_flag = False
            except Exception:
                self.logger.error(traceback.format_exc())
                sys.exit(-1)
            finally:
                self.pickle_handler.close()
