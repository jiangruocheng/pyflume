#! -*- coding:utf-8 -*-

import os
import traceback
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

    def reformate(self, data):
        # Shoud be implemented by subclass
        return {'file_name': 'test.log', 'content': data[:-5]}

    def run(self, *args, **kwargs):
        sock = socket(AF_INET, SOCK_STREAM)
        sock.bind((self.ip, self.port))
        sock.listen(int(self.max_clients))

        while True:
            connection, client_address = sock.accept()
            try:
                data = ''
                while True:
                    piece = connection.recv(1024)
                    data += piece
                    if data.endswith('(EOF)'):
                        break
                self.log.debug(data)
                after_data = self.reformate(data)
                file_path = os.path.join(self.sink_dir, after_data['file_name'])
                # 写入本地文件, 并由hive collector适时导入hive
                with open(file_path, 'w+') as f:
                    f.write(after_data['content'])
                connection.sendall('success')
            except:
                self.log.error(traceback.format_exc())
            finally:
                connection.close()
