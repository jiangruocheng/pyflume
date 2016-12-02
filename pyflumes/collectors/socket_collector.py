#! -*- coding:utf-8 -*-

import traceback

from socket import socket, AF_INET, SOCK_STREAM

from base import Collector


class SockCollector(Collector):
    def __init__(self, config, section):
        super(SockCollector, self).__init__(config, section)
        self.server_ip = config.get(section, 'SERVER_IP')
        self.server_port = int(config.get(section, 'SERVER_PORT'))

    def process_data(self, msg):
        result = 'ok'
        _data = msg['filename'] + ': ' + msg['data']
        self.log.debug(msg['collectors'] + _data)

        while True:
            try:
                sock = socket(AF_INET, SOCK_STREAM)
                sock.connect((self.server_ip, self.server_port))
                self.log.debug('Connected to :' + str((self.server_ip, self.server_port)))
                sock.sendall(_data + '(EOF)')
                ret = sock.recv(64)
                if 'success' != ret:
                    result = 'Fail'
            except:
                self.log.debug(_data)
                self.log.error(traceback.format_exc())
                result = 'Fail'
            finally:
                sock.close()

            break

        return result,
