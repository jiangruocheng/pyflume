#! -*- coding:utf-8 -*-

import os
import shutil
import traceback
import json
import re

from pyhive import hive

from base import Collector


class HiveCollector(Collector):
    def __init__(self, config, section):
        super(HiveCollector, self).__init__(config, section)
        self.ip = config.get(section, 'HIVE_IP')
        self.port = int(config.get(section, 'HIVE_PORT'))
        self.hive_user_name = config.get(section, 'HIVE_USER_NAME')
        self.database = config.get(section, 'HIVE_DATABASE')
        self.table = config.get(section, 'HIVE_TABLE')
        self.table = json.loads(self.table)
        self.table_bind = dict()
        for filename, table in self.table.items():
            self.table_bind[re.compile(filename)] = table

    def do_register(self, channel_proxy):
        self.channel = channel_proxy(channel_name=self.channel_name)
        self.channel.register('hive', self.process_data)

    def get_table(self, file_location):
        filename = os.path.basename(file_location)
        for filename_pattern, table in self.table_bind.items():
            if filename_pattern.match(filename):
                return table
        return None

    def process_data(self, file_location):
        result = 'ok'
        try:
            cursor = hive.connect(self.ip,
                                  port=self.port,
                                  username=self.hive_user_name,
                                  database=self.database
                                  ).cursor()
            new_file_location = file_location + '.COMPLETE'
            shutil.move(file_location, new_file_location)
            table = self.get_table(file_location)
            assert table is not None
            LOAD_HSQL = "LOAD DATA LOCAL INPATH '%s' INTO TABLE %s" % (new_file_location, table)
            self.log.debug(LOAD_HSQL)
            cursor.execute(LOAD_HSQL)
        except:
            self.log.warning(traceback.format_exc())
            result = 'Fail'
        finally:
            cursor.close()

        if 'ok' == result:
            os.remove(new_file_location)
            self.log.info(new_file_location+' is deleted.')

        return result,
