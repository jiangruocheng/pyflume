# -*- coding: utf-8 -*-

import os
import traceback
import xmlrpclib
import readline
import glob

SLAVE_LIST = ['10.0.6.75', '10.0.6.76', '10.0.6.77', '10.0.7.9', '10.0.7.10']


def cmd_completer(text, state):
    CMD = ['help', 'config', 'show', 'upload_script', 'start', 'stop', 'check', 'reset', 'exit']
    options = [cmd for cmd in CMD if cmd.startswith(text)]
    if state < len(options):
        return options[state]
    else:
        return None


def path_completer(text, state):
    options = map(lambda f: f + '/' if os.path.isdir(f) else f, glob.glob(text+'*'))
    if state < len(options):
        return options[state]
    else:
        return None


def run(cmd):
    if '' == cmd.strip():
        return None
    elif 'help' == cmd:
        help()
    elif 'show' == cmd:
        show()
    elif 'config' == cmd:
        config()
    elif 'upload_script' == cmd:
        upload_script()
    elif 'start' == cmd:
        start()
    elif 'stop' == cmd:
        stop()
    elif 'check' == cmd:
        check()
    elif 'reset' == cmd:
        reset()
    elif 'exit' == cmd:
        return 1
    else:
        print '找不到该命令'


# 指令function：
def help():
    print '###################################'
    print 'help: 显示可用命令列表'
    print 'exit: 退出'
    print 'show: 显示所有目标机器ip'
    print 'config: 导入配置文件'
    print 'upload_script: 导入数据处理脚本'
    print 'start：启动pyflume'
    print 'stop：停止pyflume'
    print 'check：检查各节点pyflume运行状况'
    print 'reset：重置各节点pyflume临时目录（慎用）'
    print '###################################'


def show():
    for n, ip in enumerate(SLAVE_LIST):
        print '[{}]: '.format(n), ip


def config():
    show()
    choose = raw_input('选着需要配置的ip的序号，全选输入all:')
    proxy = None
    try:
        if 'all' == choose:
            ip_list = SLAVE_LIST
        else:
            ip_list = [SLAVE_LIST[int(i)] for i in choose.split()]
        readline.set_completer(path_completer)
        _path = raw_input('请输入配置文件的地址:')
        for ip in ip_list:
            print 'IP: ', ip, 'is proccessing...'
            address = "http://{}:12001/".format(ip)
            proxy = xmlrpclib.ServerProxy(address)
            with open(_path, 'r') as f:
                print 'Result:', str(proxy.config(f.read()))
    except:
        print traceback.format_exc()
    finally:
        if proxy is not None:
            proxy('close')
    print 'Done.'


def upload_script():
    show()
    choose = raw_input('选着需要配置的ip的序号，全选输入all:')
    proxy = None
    try:
        if 'all' == choose:
            ip_list = SLAVE_LIST
        else:
            ip_list = [SLAVE_LIST[int(i)] for i in choose.split()]
        readline.set_completer(path_completer)
        _path = raw_input('请输入脚本文件的地址:')
        for ip in ip_list:
            print 'IP: ', ip, 'is proccessing...'
            address = "http://{}:12001/".format(ip)
            proxy = xmlrpclib.ServerProxy(address)
            with open(_path, 'r') as f:
                print 'Result:', str(proxy.upload_script(os.path.basename(_path), f.read()))
    except:
        print traceback.format_exc()
    finally:
        if proxy is not None:
            proxy('close')

    print 'Done.'


def start():
    show()
    choose = raw_input('选着需要启动的ip的序号，全选输入all:')
    proxy = None
    try:
        if 'all' == choose:
            ip_list = SLAVE_LIST
        else:
            ip_list = [SLAVE_LIST[int(i)] for i in choose.split()]
        for ip in ip_list:
            print 'IP: ', ip, 'is proccessing...'
            address = "http://{}:12001/".format(ip)
            proxy = xmlrpclib.ServerProxy(address)
            print 'Reuslt:', str(proxy.start())
    except:
        print traceback.format_exc()
    finally:
        if proxy is not None:
            proxy('close')

    print 'Done.'


def stop():
    show()
    choose = raw_input('选着需要停止运行的ip的序号，全选输入all:')
    proxy = None
    try:
        if 'all' == choose:
            ip_list = SLAVE_LIST
        else:
            ip_list = [SLAVE_LIST[int(i)] for i in choose.split()]
        for ip in ip_list:
            print 'IP: ', ip, 'is proccessing...'
            address = "http://{}:12001/".format(ip)
            proxy = xmlrpclib.ServerProxy(address)
            print 'Reuslt:', str(proxy.stop())
    except:
        print traceback.format_exc()
    finally:
        if proxy is not None:
            proxy('close')

    print 'Done.'


def check():
    proxy = None
    try:
        for ip in SLAVE_LIST:
            print 'IP: ', ip, 'is proccessing...'
            address = "http://{}:12001/".format(ip)
            proxy = xmlrpclib.ServerProxy(address)
            print 'Reuslt:', str(proxy.is_running())
    except:
        print traceback.format_exc()
    finally:
        if proxy is not None:
            proxy('close')

    print 'Done.'


def reset():
    show()
    choose = raw_input('选着需要重置的ip的序号，全选输入all:')
    proxy = None
    try:
        if 'all' == choose:
            ip_list = SLAVE_LIST
        else:
            ip_list = [SLAVE_LIST[int(i)] for i in choose.split()]
        for ip in ip_list:
            print 'IP: ', ip, 'is proccessing...'
            address = "http://{}:12001/".format(ip)
            proxy = xmlrpclib.ServerProxy(address)
            print 'Reuslt:', str(proxy.reset())
    except:
        print traceback.format_exc()
    finally:
        if proxy is not None:
            proxy('close')

    print 'Done.'

if __name__ == '__main__':

    readline.parse_and_bind("tab: complete")
    readline.set_completer_delims(' \t\n`!@#$^&*()=+[{]}\\|;:\'",<>?')

    help()
    while True:
        readline.set_completer(cmd_completer)
        cmd = raw_input('>> ')
        if run(cmd):
            break

    print 'Done.'
