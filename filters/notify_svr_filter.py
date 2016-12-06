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

    data_list.append(json_data.get('companyId', ''))
    doc_code = json_data.get('documentCode') or json_data.get('docCode') or ''
    data_list.append(doc_code)
    data_list.append(json_data.get('actionId', ''))
    data_list.append(json_data.get('NO') or json_data.get('docNo') or '')
    data_list.append(json_data.get('errorCode', ''))
    data_list.append(json_data.get('errorMsg', ''))
    data_list.append(json_data.get('classId', ''))
    data_list.append(json_data.get('status', ''))
    data_list.append(json_data.get('flag', ''))
    data_list.append(json_data.get('startTime', ''))
    data_list.append(json_data.get('overTime', ''))
    data_list.append(json_data.get('channel', ''))

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
