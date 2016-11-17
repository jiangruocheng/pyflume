#! -*- coding:utf-8 -*-

import logging

from socket import socket, AF_INET, SOCK_STREAM


class SocketPoll(object):
    """从socket客户端收集日志"""

    def __init__(self, config, section):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.ip = config.get(section, 'LISTEN_IP')
        self.port = int(config.get(section, 'LISTEN_PORT'))
        self.max_clients = config.get(section, 'MAX_CLIENTS')
        self.sink_dir = config.get(section, 'SINK_DIR')

    def run(self, channel=None, name=''):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.bind((self.ip, self.port))
        sock.listen(int(self.max_clients))

        while True:
            connection, client_address = sock.accept()

            try:
                while True:
                    data = connection.recv(1024)
                    if data:
                        # 写入本地文件, 并适时导入hive
                        import os
                        os.system("echo  `date`':{}' >> ~/tmp/aaaa".format(data))
                        # TODO
                        connection.sendall('success')
                    else:
                        break

            finally:
                connection.close()
