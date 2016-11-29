#! -*- coding:utf-8 -*-

from multiprocessing import Process

from polls.file_poll import FilePoll
from polls.socket_poll import SocketPoll


class AgentProxy(object):

    def __init__(self, config):
        self.pids = list()
        self.agents = dict()
        self.processes = list()
        for section in config.sections():
            if section.startswith('POOL:'):
                poll_type = config.get(section, 'TYPE')
                poll_name = section[section.find(':') + 1:]
                if poll_type.lower() == 'file':
                    self.agents[poll_name] = FilePoll(config, section)
                elif poll_type.lower() == 'socket':
                    self.agents[poll_name] = SocketPoll(config, section)
                else:
                    raise Exception('NotImplemented')

    def run(self, channel=None, event=None):
        for name, agent in self.agents.iteritems():
            _agent_process = Process(name=name,
                                     target=agent.run,
                                     kwargs={'channel': channel,
                                             'name': name,
                                             'event': event})
            _agent_process.start()
            self.processes.append(_agent_process)
            self.pids.append(_agent_process.pid)
