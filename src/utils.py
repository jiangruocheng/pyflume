#! -*- coding:utf-8 -*-

import subprocess


def isPidExist(pid):

    s = subprocess.check_output("ps aux")
    lines = s.splitlines()
    for line in lines:
        if str(pid) == line.split()[1]:
            return True

    return False
