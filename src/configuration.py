#! -*- coding:utf-8 -*-

import logging

LG = None


class Configuration(object):

    def __init__(self, path='default'):
        self.log_path = './run.log'
        self.pickle_path = './.pickles'
        self.max_read_line = 10
        self.pool_path = ''
        self._log_level = logging.WARNING
        self._read_config(path)

    def _read_config(self, path):
        if 'default' == path:
            self.log_path = './run.log'
            self.pickle_path = './.pickles'
            self.max_read_line = 10
            self.pool_path = ''
            self._log_level = logging.WARNING
        else:
            with open(path, 'r') as _conf_handler:
                _lines = _conf_handler.readlines()
                for _line in _lines:
                    _line = _line.replace('\n', '')
                    if not _line:
                        continue
                    _line = ''.join(_line.split(' '))
                    _key_and_value = _line.split('=')
                    _key = _key_and_value[0]
                    _value = _key_and_value[1]
                    if 'LOG_PATH' == _key:
                        self.log_path = _value
                    elif 'PICKLE_PATH' == _key:
                        self.pickle_path = _value
                    elif 'MAX_READ_LINE' == _key:
                        self.max_read_line = int(_value)
                    elif 'POOL_PATH' == _key:
                        self.pool_path = _value
                    elif 'LOG_LEVEL' == _key:
                        if 'notset' == _value:
                            self._log_level = logging.NOTSET
                        elif 'debug' == _value:
                            self._log_level = logging.DEBUG
                        elif 'info' == _value:
                            self._log_level = logging.INFO
                        elif 'warn' == _value:
                            self._log_level = logging.WARN
                        elif 'error' == _value:
                            self._log_level = logging.ERROR
                        elif 'critical' == _value:
                            self._log_level = logging.CRITICAL
                        else:
                            raise Exception('Unkown log level name.')
                    else:
                        raise Exception('Unknown configure item "%s"' % _key)

    def set_app_log_config(self):
        global LG
        _formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(filename)s- %(lineno)d - %(message)s')
        _fh = logging.FileHandler(self.log_path)
        _fh.setFormatter(_formatter)

        _Lg = logging.getLogger('PyFlume')
        # CRITICAL > ERROR > WARN > INFO > DEBUG > NOTSET
        _Lg.setLevel(self._log_level)
        _Lg.addHandler(_fh)

        LG = _Lg
