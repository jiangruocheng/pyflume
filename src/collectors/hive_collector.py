#! -*- coding:utf-8 -*-

import os
import time
import shutil

from pyhive import hive

from base import Collector, event


class HiveCollector(Collector):
    def __init__(self, config, section):
        super(HiveCollector, self).__init__(config, section)
        self.ip = config.get(section, 'HIVE_IP')
        self.port = int(config.get(section, 'HIVE_PORT'))
        self.name = config.get(section, 'HIVE_USER_NAME')
        self.channel_name = config.get(section, 'CHANNEL')

    def run(self, *args, **kwargs):
        """This maybe implemented by subclass"""
        chn = kwargs.get('channel', None)
        if not chn:
            self.log.error('Channel should not be lost.')
            raise Exception('Channel should not be lost.')
        self.channel = chn(channel_name=self.channel_name)
        while event.wait(0):
            file_location = self.channel.get()
            if file_location:
                self.process_data(file_location)
            # 每60秒检查是否有数据
            time.sleep(60)

    def process_data(self, file_location):
        new_file_location = file_location + '.COMPLETE'
        shutil.move(file_location, new_file_location)
        cursor = hive.connect(self.ip, port=self.port, database='test_db').cursor()
        LOAD_HSQL = "LOAD DATA LOCAL INPATH '%s' INTO TABLE TB_LOAD_TEST" % new_file_location
        self.log.debug(LOAD_HSQL)
        cursor.execute(LOAD_HSQL)
        cursor.close()
        os.remove(new_file_location)
        self.log.debug(new_file_location+' is deleted.')
