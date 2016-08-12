#! -*- coding:utf-8 -*-


class Collector(object):

    def __init__(self):
        pass

    def process_data(self, handler, data):
        print handler.name, data
        return True
