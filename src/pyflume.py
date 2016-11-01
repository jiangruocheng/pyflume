#! -*- coding:utf-8 -*-

import os
import select
import pickle
import signal
import logging
import threading
import traceback
import platform

from time import sleep
from collector import get_collector
from wrappers import pickle_lock

if platform.system() == 'Linux':
    from inotify.adapters import Inotify
    from inotify.constants import IN_CREATE, IN_MOVED_FROM, IN_MOVED_TO, IN_MODIFY, IN_DELETE
else:
    from select import KQ_FILTER_SIGNAL, KQ_FILTER_READ, KQ_EV_ADD


class Pyflume(object):

    def __init__(self, config):
        self.logger = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.pickle_File = config.get('TEMP', 'PICKLE_FILE')
        self.max_read_line = int(config.get('POOL', 'MAX_READ_LINE'))
        self.pool_path = config.get('POOL', 'POOL_PATH')
        self.collector = get_collector(config.get('OUTPUT', 'TYPE'))(config)
        self.pickle_handler = None
        self.pickle_data = None
        self.handlers = list()
        self.pid = os.getpid()
        self.exit_flag = False

    @pickle_lock
    def _load_pickle(self):
        if not os.path.exists(self.pickle_File):
            try:
                self.pickle_handler = open(self.pickle_File, 'w+')
            except IOError:
                self.logger.error(traceback.format_exc())
                exit(-1)

        self.pickle_handler = open(self.pickle_File, 'r+')
        try:
            self.pickle_data = pickle.load(self.pickle_handler)
            self.logger.debug('load pickle_data:' + str(self.pickle_data))
        except EOFError:
            self.logger.warning('No pickle data exits.')
            self.pickle_data = dict()

    @pickle_lock
    def _update_pickle(self, file_handler):
        self.logger.debug('before update pickle_data:' + str(self.pickle_data))
        # 数据已成功采集,更新offset
        self.pickle_data[file_handler.name] = file_handler.tell()
        self.logger.debug('after update pickle_data:' + str(self.pickle_data))
        # 从文件读取pickle_data时offset已经改变,现在重置到0
        self.pickle_handler.seek(0)
        pickle.dump(self.pickle_data, self.pickle_handler)

    @pickle_lock
    def _reset_pickle(self, file_names):
        self.logger.debug('before reset pickle_data:' + str(self.pickle_data))
        for name in file_names:
            self.pickle_data[name] = 0
            self.pickle_handler.seek(0)
            pickle.dump(self.pickle_data, self.pickle_handler)
        self.logger.debug('after reset pickle_data:' + str(self.pickle_data))

    @pickle_lock
    def _get_pickle_data(self, file_name):

        return self.pickle_data[file_name]

    def _get_log_data_by_handler(self, handler):
        # 先获取文件上次读取的偏移量
        _file_name = handler.name
        try:
            _offset = self._get_pickle_data(_file_name)
        except KeyError:
            self.logger.warning('Cant find "%s" in pickles.' % _file_name)
            _offset = 0
        handler.seek(_offset)
        _data = handler.readlines(self.max_read_line)
        if _data:
            return _data
        else:
            return None

    def get_handlers(self):
        handlers = list()
        _files = os.listdir(self.pool_path)
        for _file in _files:
            if 'COMPLETED' != _file.split('.')[-1]:
                if os.path.isfile(os.path.join(self.pool_path, _file)):
                    try:
                        _handler = open(os.path.join(self.pool_path, _file), 'r')
                        handlers.append(_handler)
                    except IOError:
                        self.logger.error(traceback.format_exc())
                        return []

        return handlers


class KqueuePyflume(Pyflume):

    def __init__(self, config):
        super(KqueuePyflume, self).__init__(config)

        def _exit(*args, **kwargs):
            self.logger.info('Received sigterm, pyflume is going down.')
            self.exit_flag = True
            os.kill(self.pid, signal.SIGUSR1)
            self.collector.put_data() # 终止collector线程

        signal.signal(signal.SIGTERM, _exit)

    def run(self):
        self.logger.info('Pyflume start.')

        _thread_move = threading.Thread(target=self.monitor_file_move, name='monitor_file_move')
        _thread_content = threading.Thread(target=self.monitor_file_content, name='monitor_file_content')

        _thread_move.start()
        _thread_content.start()

        signal.pause()  # 阻塞这里，等待信号

        _thread_move.join()
        _thread_content.join()
        self.logger.info('Pylume stop.')

    def monitor_file_move(self):
        _is_first_time = True
        before_directories = set()

        while not self.exit_flag:
            try:
                after_directories = set(os.listdir(self.pool_path))
                xor_set = after_directories ^ before_directories
                if _is_first_time:
                    # 加入此判断是为防止pyflume启动时，pickle不为空导致数据重复导入
                    before_directories = after_directories
                    _is_first_time = False
                    continue
                if xor_set:
                    xor_set_list = [os.path.join(self.pool_path, name) for name in xor_set]
                    self._reset_pickle(xor_set_list)
                    os.kill(self.pid, signal.SIGUSR1)
                    before_directories = after_directories
                    self.logger.debug(str(xor_set) + 'These files are added or deleted;')
            except:
                self.logger.error(traceback.format_exc())
                os.kill(self.pid, signal.SIGKILL)
                exit(-1)

            sleep(10)

    def monitor_file_content(self):

        while not self.exit_flag:
            try:
                break_flag = True
                self.handlers = self.get_handlers()
                self._load_pickle()
                kq = select.kqueue()
                _monitor_list = list()
                _monitor_dict = dict()
                for _handler in self.handlers:
                    _monitor_list.append(
                        select.kevent(_handler.fileno(), filter=KQ_FILTER_READ, flags=KQ_EV_ADD)
                    )
                    _monitor_dict[_handler.fileno()] = _handler
                _monitor_list.append(
                    select.kevent(signal.SIGUSR1, filter=KQ_FILTER_SIGNAL, flags=KQ_EV_ADD)
                )
                # 此时另一个线程负责监控文件的变动,例如新文件的移入,另一个线程会发送信号终止这个循环,重新遍历获取所有文件的句柄.
                while break_flag:
                    revents = kq.control(_monitor_list, 3)
                    for event in revents:
                        if event.filter == select.KQ_FILTER_READ:
                            _in_process_file_handler = _monitor_dict[event.ident]
                            _data = self._get_log_data_by_handler(_in_process_file_handler)
                            if not _data:
                                continue
                            self.collector.put_data(
                                file_handler=_in_process_file_handler,
                                data=_data)
                            # 数据完整性由collector来保证
                            self._update_pickle(_in_process_file_handler)
                        elif event.filter == select.KQ_FILTER_SIGNAL:
                            if event.ident == signal.SIGUSR1:
                                self.logger.info(u'捕捉到信号SIGUSR1')
                                # 关闭文件句柄，避免过量问题
                                for _handler in self.handlers:
                                    _handler.close()
                                break_flag = False
            except Exception:
                self.logger.error(traceback.format_exc())
                os.kill(self.pid, signal.SIGKILL)
                exit(-1)
            finally:
                self.pickle_handler.close()


class InotifyPyflume(Pyflume):

    def run(self):
        self.logger.info('Pyflume start.')
        self.monitor_file_inotify()
        self.logger.info('Pylume stop.')

    def monitor_file_inotify(self):
        try:
            self.handlers = self.get_handlers()
            self._load_pickle()
            inotify = Inotify(block_duration_s=-1)
            inotify.add_watch(self.pool_path, IN_CREATE | IN_MOVED_FROM | IN_MOVED_TO \
                              | IN_MODIFY | IN_DELETE)
            _monitor_dict = dict()
            for _in_process_file_handler in self.handlers:
                _monitor_dict[_in_process_file_handler.name] = _in_process_file_handler
                while True:
                    _data = self._get_log_data_by_handler(_in_process_file_handler)
                    if not _data:
                        break
                    _result_flag = self.collector.process_data(
                        file_name=_in_process_file_handler.name,
                        data=_data)
                    if _result_flag:
                        self._update_pickle(_in_process_file_handler)

            for event in inotify.event_gen():
                if self.exit_flag:
                    print self.exit_flag
                    break
                if event is not None:
                    (header, type_names, watch_path, filename) = event
                    if header.mask & IN_CREATE or header.mask & IN_MOVED_TO:
                        full_filename = watch_path + filename
                        if os.path.isfile(full_filename):
                            _new_file_handler = open(full_filename, 'r')
                            _monitor_dict[full_filename] = _new_file_handler
                            self.handlers.append(_new_file_handler)
                            self._update_pickle(_new_file_handler)
                    elif header.mask & IN_MODIFY:
                        _in_process_file_handler = _monitor_dict[watch_path + filename]
                        while True:
                            _data = self._get_log_data_by_handler(_in_process_file_handler)
                            if not _data:
                                break
                            _result_flag = self.collector.process_data(
                                file_name=_in_process_file_handler.name,
                                data=_data)
                            if _result_flag:
                                self._update_pickle(_in_process_file_handler)
                    elif header.mask & IN_DELETE or header.mask & IN_MOVED_FROM:
                        _delete_file_handler = _monitor_dict[watch_path + filename]
                        _monitor_dict.pop(watch_path + filename)
                        _delete_file_handler.close()
                        self.handlers.remove(_delete_file_handler)
                        self._reset_pickle([_delete_file_handler.name])
        except Exception:
            self.logger.error(traceback.format_exc())
            os.kill(self.pid, signal.SIGKILL)
            exit(-1)
        finally:
            self.pickle_handler.close()
