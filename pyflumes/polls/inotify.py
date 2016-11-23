# -*- coding: utf-8 -*-
import os
import struct
from errno import EINTR
from ctypes import CDLL, CFUNCTYPE, c_int, c_char_p, c_uint32, get_errno

_libc = CDLL("libc.so.6")
_iIII_length = struct.calcsize('iIII')


def _check_error(result, func, arguments):
    if result == -1:
        _errno = get_errno()
        raise OSError(_errno, os.strerror(_errno))
    return result


class Inotify(object):
    def __init__(self, ):
        self.__path_to_wd = dict()
        self.__wd_to_path = dict()
        self.__buffer = b''
        self.__fd = _inotify_init()

    def __del__(self):
        os.close(self.__fd)

    def fileno(self):
        return self.__fd

    def add_watch(self, path, mask):
        wd = _inotify_add_watch(self.__fd, path, mask)
        self.__path_to_wd[path] = wd
        self.__wd_to_path[wd] = path

    def remove_watch(self, path):
        wd = self.__path_to_wd.get(path)
        if wd:
            self.__path_to_wd.pop(path)
            self.__wd_to_path.pop(wd)
            _inotify_rm_watch(self.__fd, wd)

    def read_events(self, n=1024):
        """
        :param n: max number of bytes read from inotify fd
        :return: list of (watch_path, mask, cookie, name)
        """

        events = []
        while True:
            try:
                self.__buffer += os.read(self.__fd, n)
                break
            except IOError as e:
                if e.errno == EINTR:
                    continue
                raise e

        buffer_length = len(self.__buffer)
        i = 0
        while i + _iIII_length < buffer_length:
            wd, mask, cookie, length = struct.unpack_from('iIII', self.__buffer, i)
            if i + _iIII_length + length > buffer_length:
                break
            name = self.__buffer[i + _iIII_length:i + _iIII_length + length].rstrip(b'\0')
            events.append((self.__wd_to_path[wd], mask, cookie, name))
            i += _iIII_length + length

        self.__buffer = self.__buffer[i:]
        return events


_inotify_init = CFUNCTYPE(c_int, use_errno=True)(
    ("inotify_init", _libc))
_inotify_init.errcheck = _check_error

_inotify_add_watch = CFUNCTYPE(c_int, c_int, c_char_p, c_uint32, use_errno=True)(
    ("inotify_add_watch", _libc))
_inotify_add_watch.errcheck = _check_error

_inotify_rm_watch = CFUNCTYPE(c_int, c_int, c_int, use_errno=True)(
    ("inotify_rm_watch", _libc))
_inotify_rm_watch.errcheck = _check_error

IN_ACCESS = 0x00000001  # File was accessed
IN_MODIFY = 0x00000002  # File was modified
IN_ATTRIB = 0x00000004  # Metadata changed
IN_CLOSE_WRITE = 0x00000008  # Writtable file was closed
IN_CLOSE_NOWRITE = 0x00000010  # Unwrittable file closed
IN_OPEN = 0x00000020  # File was opened
IN_MOVED_FROM = 0x00000040  # File was moved from X
IN_MOVED_TO = 0x00000080  # File was moved to Y
IN_CREATE = 0x00000100  # Subfile was created
IN_DELETE = 0x00000200  # Subfile was deleted
IN_DELETE_SELF = 0x00000400  # Self was deleted
IN_MOVE_SELF = 0x00000800  # Self was moved

# the following are legal events.  they are sent as needed to any watch
IN_UNMOUNT = 0x00002000  # Backing fs was unmounted
IN_Q_OVERFLOW = 0x00004000  # Event queued overflowed
IN_IGNORED = 0x00008000  # File was ignored

# helper events
IN_CLOSE = (IN_CLOSE_WRITE | IN_CLOSE_NOWRITE)  # close
IN_MOVE = (IN_MOVED_FROM | IN_MOVED_TO)  # moves

# special flags
IN_ONLYDIR = 0x01000000  # only watch the path if it is a directory
IN_DONT_FOLLOW = 0x02000000  # don't follow a sym link
IN_EXCL_UNLINK = 0x04000000  # exclude events on unlinked objects
IN_MASK_ADD = 0x20000000  # add to the mask of an already existing watch
IN_ISDIR = 0x40000000  # event occurred against dir
IN_ONESHOT = 0x80000000  # only send event once

# All of the events - we build the list by hand so that we can add flags in
# the future and not break backward compatibility.  Apps will get only the
# events that they originally wanted.  Be sure to add new events here!
IN_ALL_EVENTS = (IN_ACCESS | IN_MODIFY | IN_ATTRIB | IN_CLOSE_WRITE |
                 IN_CLOSE_NOWRITE | IN_OPEN | IN_MOVED_FROM |
                 IN_MOVED_TO | IN_DELETE | IN_CREATE | IN_DELETE_SELF |
                 IN_MOVE_SELF)
