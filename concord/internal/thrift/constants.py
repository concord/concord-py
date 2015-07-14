#
# Autogenerated by Thrift Compiler (0.9.2)
#
# DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
#
#  options string: py:json,utf8strings
#

from thrift.Thrift import TType, TMessageType, TException, TApplicationException
from ttypes import *

kBoltEnvKeyBasePath = "BOLT_BASE_PATH"
kBoltDefaultEnvBasePath = "/tmp/"
kBoltEnvKeyPathPrefix = "BOLT"
kDefaultThriftServiceIOThreads = 2
kConcordEnvKeyClientListenAddr = "CONCORD_client_listen_address"
kConcordEnvKeyClientProxyAddr = "CONCORD_client_proxy_address"
kDatabasePath = "/tmp"
kDatabaseEntryTTL = 43200
kDefaultBatchSize = 2048
kDefaultTraceSampleEveryN = 1024
kPrincipalComputationName = "principal_computation"
kIncomingMessageQueueTopic = "incoming"
kPrincipalTimerQueueTopic = "principal_timers"
kOutgoingMessageQueueTopic = "outgoing"
kQueueStreamNameToIdMapTopic = "stream_map"
kMessageQueueWatermarkTopic = "watermarks"
kMessageQueueBatchSize = 1024
kMessageQueueTTL = 21600
kBoltTraceHeader = "bolt_traces"