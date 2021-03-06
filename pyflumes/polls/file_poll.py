#! -*- coding:utf-8 -*-

import re
import os
import signal
import select
import pickle
import platform
import traceback

from errno import EINTR

from pyflumes.wrappers import pickle_lock
from pyflumes.polls.base import PollBase


class FilePollBase(PollBase):

    def __init__(self, config, section):
        super(FilePollBase, self).__init__(config, section)
        self.pickle_File = os.path.join(config.get('GLOBAL', 'DIR'), 'tmp/'+'PICKLE_FILE.'+self.name)
        self.pool_path = config.get(section, 'POOL_PATH')
        self.filename_pattern = re.compile(config.get(section, 'FILENAME_PATTERN'))
        self.pickle_handler = None
        self.pickle_data = None
        self.handlers = list()
        self.pid = None
        self.exit_flag = False
        self.name = ''
        self.monitor_dict = dict()

    @pickle_lock
    def _load_pickle(self):
        if not os.path.exists(self.pickle_File):
            try:
                self.pickle_handler = open(self.pickle_File, 'w+')
            except IOError:
                self.log.error(traceback.format_exc())
                exit(-1)

        self.pickle_handler = open(self.pickle_File, 'r+')
        try:
            self.pickle_data = pickle.load(self.pickle_handler)
            self.log.debug('[{}]load pickle_data:'.format(self.name) + str(self.pickle_data))
        except EOFError:
            self.log.warning('[{}]No pickle data exits.'.format(self.name))
            self.pickle_data = dict()

    @pickle_lock
    def _update_pickle(self, file_handler):
        self.log.debug('[{}]before update pickle_data:'.format(self.name) + str(self.pickle_data))
        # 数据已成功采集,更新offset
        self.pickle_data[file_handler.name] = file_handler.tell()
        self.log.debug('[{}]after update pickle_data:'.format(self.name) + str(self.pickle_data))
        # 从文件读取pickle_data时offset已经改变,现在重置到0
        self.pickle_handler.seek(0)
        pickle.dump(self.pickle_data, self.pickle_handler)

    @pickle_lock
    def _reset_pickle(self, file_names):
        self.log.debug('[{}]before reset pickle_data:'.format(self.name) + str(self.pickle_data))
        for name in file_names:
            self.pickle_data[name] = 0
            self.pickle_handler.seek(0)
            pickle.dump(self.pickle_data, self.pickle_handler)
        self.log.debug('[{}]after reset pickle_data:'.format(self.name) + str(self.pickle_data))

    @pickle_lock
    def _get_pickle_data(self, file_name):

        return self.pickle_data[file_name]

    def _get_log_data_by_handler(self, handler):
        # 先获取文件上次读取的偏移量
        _file_name = handler.name
        try:
            _offset = self._get_pickle_data(_file_name)
        except KeyError:
            self.log.warning('[{}]Cant find "%s" in pickles.'.format(self.name) % _file_name)
            _offset = 0
        handler.seek(_offset)
        _data = handler.readlines()
        if _data:
            return _data
        else:
            return None

    def get_handlers(self):
        handlers = list()
        _files = os.listdir(self.pool_path)
        for _file in _files:
            if self.is_need_monitor(_file):
                if os.path.isfile(os.path.join(self.pool_path, _file)):
                    try:
                        _handler = open(os.path.join(self.pool_path, _file), 'r')
                        handlers.append(_handler)
                    except IOError:
                        self.log.error(traceback.format_exc())
                        return []

        return handlers

    def pre_process(self):
        # 预处理，先处理已有的数据
        for _in_process_file_handler in self.handlers:
            self.monitor_dict[_in_process_file_handler.name] = _in_process_file_handler
            file_size = os.path.getsize(_in_process_file_handler.name)
            # 当文件大小超过进程文件描述符最大偏移量时，超出部分的文件内容也应该被发送到channel里
            # 但此时不会触发epoll或者kq的文件可读事件，所以应该在这里做一些处理
            while _in_process_file_handler.tell() < file_size:
                _data = self._get_log_data_by_handler(_in_process_file_handler)
                if not _data:
                    break
                for _line in _data:
                    msg = self.reformate(filename=_in_process_file_handler.name, data=_line)
                    if msg:
                        self.channel.put(msg)
                        # 数据完整性由collector来保证
                self._update_pickle(_in_process_file_handler)

    def clean_handlers(self):
        for _handler in self.handlers:
            try:
                _handler.close()
            except:
                pass
        try:
            self.pickle_handler.close()
        except:
            pass

    def is_need_monitor(self, filename):
        if self.filename_pattern.match(filename):
            return True
        return False

    def reformate(self, filename, data):
        if self.filter:
            data = self.filter(data)
            if not data:
                return None
        msg = dict()
        msg['collectors'] = self.collector_set.encode('utf-8')
        msg['filename'] = filename.encode('utf-8')
        msg['data'] = data
        return msg


if platform.system() == 'Linux':
    from inotify import Inotify
    from inotify import IN_CREATE, IN_MOVED_FROM, IN_MOVED_TO, IN_MODIFY, IN_DELETE, IN_DELETE_SELF

    class InotifyPoll(FilePollBase):
        def __init__(self, config, section):
            super(InotifyPoll, self).__init__(config, section)
            self.inotify = None
            self._break_flag = True

        def on_file_modify(self, event):
            return event & IN_MODIFY

        def on_file_remove(self, event):
            return event & IN_DELETE | event & IN_MOVED_FROM

        def on_file_new(self, event):
            return event & IN_CREATE | event & IN_MOVED_TO

        def on_directory_delete(self, event):
            return event & IN_DELETE_SELF

        def monitor_file(self):

            signal.signal(signal.SIGTERM, self.exit)

            while not self.exit_flag:
                try:
                    self.handlers = self.get_handlers()
                    self._load_pickle()
                    self.inotify = Inotify()
                    self.inotify.add_watch(self.pool_path, IN_CREATE | IN_MOVED_FROM | IN_MOVED_TO |
                                           IN_MODIFY | IN_DELETE | IN_DELETE_SELF)
                    _epoll = select.epoll()
                    _epoll.register(self.inotify.fileno(), select.EPOLLIN)

                    self.pre_process()

                    self._break_flag = True
                    while not self.exit_flag and self._break_flag:
                        try:
                            for fd, epoll_event in _epoll.poll(-1):
                                if fd == self.inotify.fileno():
                                    for inotify_event in self.inotify.read_events():
                                        self.process_event(inotify_event)
                        except IOError as e:
                            if e.errno != EINTR:
                                raise e
                except Exception:
                    self.log.error(traceback.format_exc())
                finally:
                    self.clean_handlers()
                    _epoll.close()

        def process_event(self, inotify_event):
            (watch_path, mask, cookie, filename) = inotify_event
            if self.on_file_new(mask):
                if self.is_need_monitor(filename):
                    full_filename = os.path.join(watch_path, filename)
                    if os.path.isfile(full_filename):
                        _new_file_handler = open(full_filename, 'r')
                        self.monitor_dict[full_filename] = _new_file_handler
                        self.handlers.append(_new_file_handler)
                        self._update_pickle(_new_file_handler)
            elif self.on_file_modify(mask):
                _in_process_file_handler = self.monitor_dict.get(os.path.join(watch_path, filename))
                if _in_process_file_handler:
                    _data = self._get_log_data_by_handler(_in_process_file_handler)
                    if _data:
                        for _line in _data:
                            msg = self.reformate(filename=_in_process_file_handler.name, data=_line)
                            if msg:
                                self.channel.put(msg)
                            # 数据完整性由collector来保证
                        self._update_pickle(_in_process_file_handler)
            elif self.on_file_remove(mask):
                _delete_file_handler = self.monitor_dict.get(os.path.join(watch_path, filename))
                if _delete_file_handler:
                    self.monitor_dict.pop(os.path.join(watch_path, filename))
                    _delete_file_handler.close()
                    self.handlers.remove(_delete_file_handler)
                    self._reset_pickle([_delete_file_handler.name])
            elif self.on_directory_delete(mask):
                parent_path = os.path.normpath(os.path.join(self.pool_path, os.pardir))
                basename = os.path.basename(os.path.normpath(self.pool_path))
                self.inotify.remove_watch(self.pool_path)
                self.inotify.add_watch(parent_path, IN_CREATE | IN_MOVED_TO)
                self.log.debug('pool_path is deleted, waiting for pool_path to be created')
                wait_flag = False if os.path.exists(self.pool_path) else True
                while not self.exit_flag and wait_flag:
                    try:
                        for self.inotify_event in self.inotify.read_events():
                            (watch_path, mask, cookie, filename) = self.inotify_event
                            if filename == basename:
                                self.inotify.add_watch(self.pool_path, IN_CREATE | IN_MOVED_FROM | IN_MOVED_TO |
                                                       IN_MODIFY | IN_DELETE | IN_DELETE_SELF)
                                wait_flag = False
                    except IOError as e:
                        if e.errno != EINTR:
                            raise e
                self._break_flag = False
                self.inotify.remove_watch(parent_path)
                self.log.debug('stop waiting for pool_path to be created')

        def exit(self, *args, **kwargs):
            self.log.info('Received sigterm, agent[{}] is going down.'.format(self.name))
            self.exit_flag = True

    SystemPoll = InotifyPoll

elif platform.system() == 'Darwin':
    from select import KQ_FILTER_READ, KQ_FILTER_SIGNAL
    from pyflumes.polls.kq import WATCHDOG_KQ_FILTER, WATCHDOG_KQ_EV_FLAGS, WATCHDOG_KQ_FFLAGS

    class KqueuePoll(FilePollBase):
        def __init__(self, config, section):
            super(KqueuePoll, self).__init__(config, section)

        def on_file_read(self, event):
            return event.filter == select.KQ_FILTER_READ

        def on_directory_write(self, event):
            return event.fflags & select.KQ_NOTE_WRITE

        def on_directory_deleted(self, event):
            return event.fflags & select.KQ_NOTE_DELETE

        def on_directory_renamed(self, event):
            return event.fflags & select.KQ_NOTE_RENAME

        def on_signal_terminate(self, event):
            return event.filter == select.KQ_FILTER_SIGNAL \
                   and event.ident == signal.SIGTERM

        def monitor_file(self):

            while not self.exit_flag:
                try:
                    pool_path_handler = os.open(self.pool_path, 0x8000)
                    break_flag = True
                    before_directories = set(os.listdir(self.pool_path))
                    self.handlers = self.get_handlers()
                    self._load_pickle()
                    kq = select.kqueue()
                    _monitor_list = [select.kevent(pool_path_handler,
                                                   filter=WATCHDOG_KQ_FILTER,
                                                   flags=WATCHDOG_KQ_EV_FLAGS,
                                                   fflags=WATCHDOG_KQ_FFLAGS)]
                    for _handler in self.handlers:
                        _monitor_list.append(
                            select.kevent(_handler.fileno(), filter=KQ_FILTER_READ, flags=WATCHDOG_KQ_EV_FLAGS)
                        )
                        self.monitor_dict[_handler.fileno()] = _handler
                    _monitor_list.append(
                        select.kevent(signal.SIGTERM, filter=KQ_FILTER_SIGNAL, flags=WATCHDOG_KQ_EV_FLAGS)
                    )

                    self.pre_process()

                    while break_flag:
                        revents = kq.control(_monitor_list, 64)
                        for event in revents:
                            if self.on_file_read(event):
                                _in_process_file_handler = self.monitor_dict[event.ident]
                                _data = self._get_log_data_by_handler(_in_process_file_handler)
                                if not _data:
                                    continue
                                else:
                                    for _line in _data:
                                        msg = self.reformate(filename=_in_process_file_handler.name, data=_line)
                                        if msg:
                                            self.channel.put(msg)
                                # 数据完整性由collector来保证
                                self._update_pickle(_in_process_file_handler)
                            elif self.on_directory_write(event):
                                after_directories = set(os.listdir(self.pool_path))
                                xor_set = after_directories ^ before_directories
                                if xor_set:
                                    xor_set_list = [os.path.join(self.pool_path, name) for name in xor_set]
                                    self._reset_pickle(xor_set_list)
                                    before_directories = after_directories
                                    self.log.debug('[{}]'.format(self.name)
                                                      + str(xor_set)
                                                      + 'These files are added or deleted;')
                                break_flag = False
                            elif self.on_directory_deleted(event) or self.on_directory_renamed(event):
                                self._reset_pickle([_handler.name for _handler in self.handlers])
                                self.clean_handlers()
                                break_flag = False
                            elif self.on_signal_terminate(event):
                                self.exit_flag = True
                                break_flag = False
                                self.log.info('Received sigterm, agent[{}] is going down.'.format(self.name))
                except Exception:
                    self.log.error(traceback.format_exc())
                finally:
                    self.clean_handlers()
                    os.close(pool_path_handler)

    SystemPoll = KqueuePoll

else:
    raise Exception('NotImplemented')


class FilePoll(SystemPoll):

    def __init__(self, config, section):
        super(FilePoll, self).__init__(config, section)

    def run(self, *args, **kwargs):
        chn = kwargs.get('channel', None)
        if not chn:
            self.log.error('Channel should not be lost.')
            raise Exception('Channel should not be lost.')
        self.channel = chn(channel_name=self.channel_name)
        self.name = kwargs.get('name', '')
        self.pid = os.getpid()

        self.log.debug('agent[{}] pid: '.format(self.name) + str(self.pid))
        self.log.info('Pyflume agent[{}] starts.'.format(self.name))

        self.monitor_file()
        self.log.info('Pyflume agent[{}] ends.'.format(self.name))
