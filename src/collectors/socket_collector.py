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
        _data = msg['filename'] + ': ' + msg['data']
        self.log.debug(msg['collector'] + _data)

        sock = socket(AF_INET, SOCK_STREAM)
        sock.connect((self.server_ip, self.server_port))

        try:
            while True:
                sock.sendall(_data + '(EOF)')
                result = sock.recv(1024)
                if 'success' != result:
                    self.log.debug('Unsuccessful sending data.')
                    time.sleep(30)
                    continue
                else:
                    break
        except:
            self.log.debug(_data)
            self.log.error(traceback.format_exc())
        finally:
            sock.close()


