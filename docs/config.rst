pyflume配置文档
======================

pyflume在使用前需要导入好定制的配置文件。这些配置文件根据应用环境不同而不同，例如LOG输出
配置，log pool配置，collector配置等。

基本配置
--------------------

支持pyflume运行至少需要配置五项，分别是GLOBAL、LOG、CHANNEL、COLLECTOR、POOL。其中，
GLOBAL、LOG各只用配置一例，CHANNEL、COLLECTOR、POOL根据需要可以配置多个。

配置项对应配置字段含义
----------------------------

============= ================== ==============================================
    GLOBAL        全局配置项
                                       - DIR: pyflume临时目录
============= ================== ==============================================

============= ================== ==============================================
     LOG           log配置
                                       - DEBUG: True or False
                                       - LOG_FILE: pyflume日志输出文件
                                       - LOG_HANDLER: 日志文件句柄名称
============= ================== ==============================================

============= ================== ==============================================
     POOL          数据源配置
                                       - TYPE: 目前只支持file和socket两种类型，其中
                                         - type为file的配置项有:
                                           - POOL_PATH: 监控的文件目录
                                           - FILENAME_PATTERN: 使用正则表达式匹配
                                           文件名
                                           - CONTENT_FILTER_SCRIPT: 数据过滤器
                                         - type为socket的配置项有:
                                           - LISTEN_IP: 监听IP地址
                                           - LISTEN_PORT: 监听端口
                                           - MAX_CLIENTS: 同时连接的最大客户端数量
                                       - CHANNEL: 该pool使用的channel名称
                                       - COLLECTOR: 该pool使用collector的名称，
                                       有多个用逗号分隔
============= ================== ==============================================

============= ================== ==============================================
   CHANNEL         channel配置
                                       - TYPE: channel类型,支持memory和file
                                         - type为file的配置项有：
                                           - FILE_MAX_SIZE: 整型，当缓存文件size
                                           达到此值将被导入hive中
                                           - IGNORE_POSTFIX: 忽略缓冲区中以此结尾
                                           的文件名
============= ================== ==============================================

============= ================== ==============================================
  COLLECTOR        数据沉积池配置
                                       - TYPE: 支持hive、kafka、socket
                                         - type为hive的配置项有:
                                           - HIVE_IP: HIVE服务监听IP
                                           - HIVE_PORT: HIVE服务监听端口
                                           - HIVE_USER_NAME: HIVE用户名
                                           - HIVE_DATABASE: DB名称
                                           - HIVE_TABLE: HIVE表配置，配置使用
                                           键值对形式进行匹，样例如下：
                                           HIVE_TABLE = {
                                                "^my.log$": "TB_MY_TABLE",
                                                }
                                         - type为socket的配置项有:
                                           - SERVER_IP: SERVER IP地址
                                           - SERVER_PORT: SERVER监听端口
                                         - type为kafka:
                                           - SERVER: SERVER地址
                                           - TOPIC: topic名称
                                       - CHANNEL: 该pool使用的channel名称
                                       - COLLECTOR: 该pool使用collector的名称，
                                       有多个用逗号分隔
============= ================== ==============================================

配置样例
----------------------------

配置文件1：

[GLOBAL]
DIR = /tmp/pyflume/

[LOG]
DEBUG = True
LOG_FILE = /tmp/pyflume/logs/pyflume.log
LOG_HANDLER = pyflume

[CHANNEL:memory]
TYPE = memory

[COLLECTOR:socket]
TYPE = socket
SERVER_IP = 10.0.6.75
SERVER_PORT = 12000
CHANNEL = memory

[COLLECTOR:kafka]
TYPE = kafka
SERVER = 10.0.6.75:9092
TOPIC = OSS_ANALYSIS_LOG
CHANNEL = memory

[COLLECTOR:kafka1]
TYPE = kafka
SERVER = 10.0.6.75:9092
TOPIC = OSS_LOG_API
CHANNEL = memory

[POOL:OSS_LOG_API]
TYPE = file
MAX_READ_LINE = 4096
POOL_PATH = /opt/OSS/OSS_Log_API/log
FILENAME_PATTERN = ^LogAPI.log$
CONTENT_FILTER_SCRIPT = /tmp/pyflume/scripts/oss_api_filter.py
CHANNEL = memory
COLLECTOR = socket, kafka1

[POOL:OSS_ANALYSIS]
TYPE = file
MAX_READ_LINE = 4096
POOL_PATH = /tmp/log_pool/
FILENAME_PATTERN = ^oss_analysis.log$
CHANNEL = memory
COLLECTOR = kafka

配置文件2：
[GLOBAL]
DIR = /tmp/pyflume/

[LOG]
DEBUG = True
LOG_FILE = /tmp/pyflume/logs/pyflume.log
LOG_HANDLER = pyflume

[CHANNEL:file]
TYPE = file
FILE_MAX_SIZE = 10240
IGNORE_POSTFIX = COMPLETE

[COLLECTOR:hive]
TYPE = hive
HIVE_IP = localhost
HIVE_PORT = 10000
HIVE_USER_NAME = root
HIVE_DATABASE = OSS_DB
HIVE_TABLE = {
    "^NotifyAPI.log$": "TB_OSS_NOTIFY_API_LOG",
    "^NotifySvr.log$": "TB_OSS_NOTIFY_SVR_LOG",
    "^ConfigAPI.log$": "TB_OSS_CONFIG_API_LOG",
    "^LogAPI.log$": "TB_OSS_LOG_API_LOG",
    "^LogSvr.log$": "TB_OSS_LOG_SVR_LOG"
    }
CHANNEL = file

[POOL:socket]
TYPE = socket
LISTEN_IP = 0.0.0.0
LISTEN_PORT = 12000
MAX_CLIENTS = 64
CHANNEL = file
COLLECTOR = hive


配置文件1定义了两个数据源（POOL）、一个CHANNEL、三个数据沉积池（COLLECTOR)：
配置文件2定义了一个数据源（POOL）、一个CHANNEL、一个数据沉积池（COLLECTOR），数据流向如下：

POOL:OSS_LOG_API                COLLECTOR:kafka1
                 \              /
                  \            /
                 CHANNEL:memory ---  COLLECTOR:kafka
                   /           \
                 /              \
POOL:OSS_ANALYSIS                  COLLECTOR:socket  ---->  POOL:socket
                                                               |
                                                               |
                                                            CHANNEL:file
                                                                |
                                                                |
                                                            COLLECOTOR:hive


memory channel 根据配置文件将POOL:OSS_LOG_API的数据发往socket与kafka两个collector，将POOL:OSS_ANALYSIS
的数据导入到kafka1, collector socket将数据转发给socket pool，最终数据将导入到hive。