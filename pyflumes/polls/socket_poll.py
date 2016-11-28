#! -*- coding:utf-8 -*-

import os
import traceback

from socket import socket, error, AF_INET, SOCK_STREAM

from pyflumes.polls.base import PollBase


class SocketPoll(PollBase):
    """从socket客户端收集日志"""

    def __init__(self, config, section):
        super(SocketPoll, self).__init__(config, section)
        self.ip = config.get(section, 'LISTEN_IP')
        self.port = int(config.get(section, 'LISTEN_PORT'))
        self.max_clients = config.get(section, 'MAX_CLIENTS')
        self.exit_flag = False

    def reformate(self, data):
        _list = data.split(':')
        filename = os.path.basename(_list[0])
        _data = ''.join(_list[1:])[:-5]
        if self.filter:
            _data = self.filter(data)
            if not _data:
                return None
        return {'collectors': self.collector_set, 'filename': filename, 'data': _data.lstrip()}

    def run(self, *args, **kwargs):
        self.log.info('Socket Agent[{}] starts'.format(os.getpid()))
        chn = kwargs.get('channel', None)
        event = kwargs.get('event')
        if not chn:
            self.log.error('Channel should not be lost.')
            raise Exception('Channel should not be lost.')

        sock = socket(AF_INET, SOCK_STREAM)
        sock.bind((self.ip, self.port))
        sock.listen(int(self.max_clients))

        self.channel = chn(channel_name=self.channel_name)
        while event.wait(timeout=0):

            try:
                connection, client_address = sock.accept()
            except error:
                continue

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
                connection.close()
            except:
                self.log.error(traceback.format_exc())
            finally:
                connection.close()

        try:
            sock.close()
        except:
            pass
        self.log.info('Socket Agent[{}] ends'.format(os.getpid()))
