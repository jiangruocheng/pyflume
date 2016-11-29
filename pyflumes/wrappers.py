# -*- coding:utf-8 -*-

from threading import Lock
from multiprocessing import RLock

PICKLE_LOCK = Lock()
CHN_LOCK = RLock()


def pickle_lock(func):

    def wrapper(*args, **kwargs):
        with PICKLE_LOCK:
            result = func(*args, **kwargs)
        return result

    return wrapper


def channel_lock(func):

    def wrapper(*args, **kwargs):
        with CHN_LOCK:
            result = func(*args, **kwargs)
        return result

    return wrapper
