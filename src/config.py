#! -*- coding:utf-8 -*-

import logging

LOG_PATH = '/tmp/pyflume/run.log'
PICKLE_PATH = '/tmp/pyflume/pickles'

formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s- %(lineno)d - %(message)s')
fh = logging.FileHandler(LOG_PATH)
fh.setFormatter(formatter)

Lg = logging.getLogger('PyFlume')
# CRITICAL > ERROR > WARNING > INFO > DEBUG > NOTSET
Lg.setLevel(logging.WARNING)
Lg.addHandler(fh)


MAX_READ_LINE = 30
POOL_PATH = '/tmp/logs'
