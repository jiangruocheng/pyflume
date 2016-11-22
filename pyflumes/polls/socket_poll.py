#! -*- coding:utf-8 -*-

import os
import sys
import signal
import logging
import traceback
import imp

from socket import socket, error, AF_INET, SOCK_STREAM
from configparser import NoOptionError


class SocketPoll(object):
    """从socket客户端收集日志"""

    def __init__(self, config, section):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.channel_name = config.get(section, 'CHANNEL')
        self.channel = None
        self.ip = config.get(section, 'LISTEN_IP')
        self.port = int(config.get(section, 'LISTEN_PORT'))
        self.max_clients = config.get(section, 'MAX_CLIENTS')
        try:
            _content_filter_script = config.get(section, 'CONTENT_FILTER_SCRIPT')
            _m = imp.load_source('content_filter', _content_filter_script)
            self.filter = _m.content_filter
        except NoOptionError:
            self.filter = None
        except:
            self.logger.error(traceback.format_exc())
            exit(-1)

    def reformate(self, data):
        # Shoud be implemented by subclass
        _list = data.split(':')
        filename = os.path.basename(_list[0])
        _data = ''.join(_list[1:])[:-5]
        if self.filter:
            _data = self.filter(data)
            if not _data:
                return None
        return {'filename': filename, 'data': _data.lstrip()}

    def exit(self, *args, **kwargs):
        self.log.info('Socket poll is leaving.')
        sys.exit(0)

    def run(self, *args, **kwargs):
        chn = kwargs.get('channel', None)
        if not chn:
            self.log.error('Channel should not be lost.')
            raise Exception('Channel should not be lost.')

        signal.signal(signal.SIGTERM, self.exit)

        sock = socket(AF_INET, SOCK_STREAM)
        sock.bind((self.ip, self.port))
        sock.listen(int(self.max_clients))

        self.channel = chn(channel_name=self.channel_name)
        while True:
            connection, client_address = sock.accept()
            try:
                data = ''
                while True:
                    piece = connection.recv(1024)
                    data += piece
                    if data.endswith('(EOF)'):
                        break
                data = self.reformate(data)
                if data:
                    self.channel.put(data)
                connection.sendall('success')
            except:
                self.log.error(traceback.format_exc())
            finally:
                connection.close()
