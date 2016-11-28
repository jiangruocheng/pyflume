#! -*- coding:utf-8 -*-

import sys
import imp
import logging
import traceback
import subprocess

from subprocess import PIPE


class PollBase(object):

    def __init__(self, config, section):
        self.log = logging.getLogger(config.get('LOG', 'LOG_HANDLER'))
        self.name = section[section.find(':') + 1:]
        self.collector_set = config.get(section, 'COLLECTOR')
        self.channel_name = config.get(section, 'CHANNEL')
        self.channel = None
        self.filter = None
        self.init_filter_config(config, section)

    def init_filter_config(self, config, section):
        try:
            if config.has_option(section, 'CONTENT_FILTER_SCRIPT') \
                    and config.has_option(section, 'CONTENT_FILTER_COMMAND'):
                raise Exception('Duplicate content filter: [CONTENT_FILTER_SCRIPT, CONTENT_FILTER_COMMAND]')
            if config.has_option(section, 'CONTENT_FILTER_SCRIPT'):
                _content_filter_script = config.get(section, 'CONTENT_FILTER_SCRIPT')
                _m = imp.load_source('content_filter', _content_filter_script)
                self.filter = _m.content_filter
            elif config.has_option(section, 'CONTENT_FILTER_COMMAND'):
                _filter_command = config.get(section, 'CONTENT_FILTER_COMMAND')
                filter_process = subprocess.Popen(_filter_command.split(), stdin=PIPE, stdout=PIPE)

                def __filter(line):
                    filter_process.stdin.write(line)
                    _line = filter_process.stdout.readline()
                    if _line.strip() == '':
                        return None
                    return _line
                self.filter = __filter
            else:
                self.filter = None
        except:
            self.log.error(traceback.format_exc())
            sys.exit(1)

    def reformate(self, *args, **kwargs):
        # This should be implemented by subclass
        pass
