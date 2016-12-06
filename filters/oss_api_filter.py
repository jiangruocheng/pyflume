# -*- coding: utf-8 -*-
import json


def content_filter(line):
    index = line.find('[TAG]')
    if index == -1:
        return None
    data_list = line.split('@$_$@')
    json_data = json.loads(data_list[-1])
    del data_list[-1]
    del data_list[0]

    method = json_data.get('params', {}).get('content', {}).get('body', {}).get('method') or \
        json_data.get('method') or ''
    data_list.append(method)  # method
    data_list.append(json_data.get('errorCode', ''))  # errorCode
    data_list.append(json_data.get('errorMsg', ''))  # errorMsg

    return '\1'.join(map(to_str, data_list)) + '\n'


def to_str(data):
    if isinstance(data, unicode):
        return data.encode('utf-8')
    return str(data)


if __name__ == '__main__':
    import sys
    while True:
        line = sys.stdin.readline()
        if not line:
            break
        result = content_filter(line)
        if result:
            sys.stdout.write(result)
