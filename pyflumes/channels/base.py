#! -*- coding:utf-8 -*-

import os
import logging
import traceback

from multiprocessing.queues import Empty


class ChannelBase(object):

    def __init__(self, config, section):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.name = section[section.find(':') + 1:]
        self.call_backs = dict()
        self.exit_flag = False

    def register(self, name, proccess_call_back):
        # 注册collector
        self.call_backs[name] = proccess_call_back
        self.log.debug(name + ' registered in {}.'.format(self.name))

    def handout(self, event):
        self.log.info(self.name + ' [{}] starts'.format(os.getpid()))

        while event.wait(timeout=0):
            try:
                data = self.get(timeout=30)
                c_names = data.get('collectors', '').split(',')
                for c_name in c_names:
                    func = self.call_backs.get(c_name.strip(), '')
                    if not func:
                        self.log.warning('Un-registered collector: ' + c_name)
                        continue
                    result = func(data)
                    if 'ok' != result[0]:
                        data['collectors'] = c_name
                        self.put(data)
            except Empty:
                continue
            except:
                self.log.warning(traceback.format_exc())
        self.log.info(self.name + ' [{}] ends'.format(os.getpid()))
