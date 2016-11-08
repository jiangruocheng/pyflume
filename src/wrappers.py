# -*- coding:utf-8 -*-

from threading import Lock

PICKLE_LOCK = Lock()


def pickle_lock(func):

    def wrapper(*args, **kwargs):
        with PICKLE_LOCK:
            result = func(*args, **kwargs)
        return result

    return wrapper
