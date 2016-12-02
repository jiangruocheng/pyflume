#! -*- coding:utf-8 -*-

import os
import time
import logging
import traceback

from copy import deepcopy
from multiprocessing.queues import Empty


class ChannelBase(object):

    def __init__(self, config, section):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.name = section[section.find(':') + 1:]
        self.call_backs = dict()
        self.exit_flag = False
        self.cache_info = dict()  # 缓存未发送成功消息

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
                    _data = deepcopy(data)
                    func = self.call_backs.get(c_name.strip(), '')
                    if not func:
                        self.log.warning('Un-registered collector: ' + c_name)
                        continue
                    result = func(_data)

                    # 若数据处理失败，重新放入队列中，并将数据id记录在案，直到词条数据重新处理成功
                    _id = data['id']
                    if 'ok' != result[0]:
                        _data['collectors'] = c_name.strip()
                        self.put(_data)
                        if _id not in self.cache_info:
                            self.cache_info[_id] = None
                        else:
                            # 防止死循环导致cpu使用率过高
                            time.sleep(30)
                    else:
                        if _id in self.cache_info:
                            self.cache_info.pop(_id)

            except Empty:
                continue
            except:
                self.log.warning(traceback.format_exc())
        self.release()
        self.log.info(self.name + ' [{}] ends'.format(os.getpid()))
