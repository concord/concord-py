"""Computation class for Concord
.. module:: Computation
    :synopsis: Computation class and helper function
"""

import sys
import os
import types
import threading
from thrift import Thrift
from thrift.transport import (
    TSocket, TTransport
)
from thrift.server import TServer
from thrift.protocol import TBinaryProtocol
from concord.internal.thrift import (
    ComputationService,
    BoltProxyService
)
from concord.internal.thrift.ttypes import (
    Record,
    ComputationTx,
    ComputationMetadata,
    Endpoint,
    StreamMetadata,
    StreamGrouping
)

from concord.internal.thrift.constants import (
    kConcordEnvKeyClientListenAddr,
    kConcordEnvKeyClientProxyAddr
)
import logging
import logging.handlers

logging_format_string='%(levelname)s:%(asctime)s %(filename)s:%(lineno)d] %(message)s'

# Basic Config is needed for thrift and default loggers
logging.basicConfig(format=logging_format_string)
concord_formatter = logging.Formatter(logging_format_string)
concord_logging_handle = logging.handlers.TimedRotatingFileHandler("concord_py.log")
concord_logging_handle.setFormatter(concord_formatter)

for h in logging.getLogger().handlers: h.setFormatter(concord_formatter)

ccord_logger = logging.getLogger('concord.computation')
ccord_logger.setLevel(logging.DEBUG)
ccord_logger.propagate = False
ccord_logger.addHandler(concord_logging_handle)

class Metadata:
    """High-level wrapper for `ComputationMetadata`
    """

    def __init__(self, name=None, istreams=[], ostreams=[]):
        """Create a new Metadata object

        :param name: The globally unique identifier of the computation.
        :type name: str.
        :param istreams: The list of streams to subscribe this computation to.
            If `istreams` is a string, use the default grouping (shuffle). If it
            is a pair, use the grouping passed as the second item.
        :type istreams: list(str), (str, StreamGrouping).
        :param ostreams: The list of streams this computation may produce on.
        :type ostreams: list(str).
        """
        self.name = name
        self.istreams = istreams
        self.ostreams = ostreams
        if len(self.istreams) == 0 and len(self.ostreams) == 0:
            raise Exception("Both input and output streams are empty")

def new_computation_context(tcp_proxy):
    """Creates a context object wrapping a transaction.
    :returns: (ComputationContext, ComputationTx)
    """
    transaction = ComputationTx()
    transaction.records = []
    transaction.timers = {}

    class ComputationContext:
        """Wrapper class exposing a convenient API for computation to proxy
            interactions.
        """
        def produce_record(self, stream, key, data):
            """Produce a record to be emitted down stream.

            :param stream: The stream to emit the record on.
            :type stream: str.
            :param key: The key to route this message by (only used when
                using GROUP_BY routing).
            :type key: str.
            :param data: The binary blob to emit down stream.
            :type data: str.
            """
            r = Record()
            r.key = key
            r.data = data
            r.userStream = stream
            transaction.records.append(r)

        def set_timer(self, key, time):
            """Set a timer callback for some point in the future.
            :name key: The name of the timer.
            :type key: str.
            :name time: The time (in ms) at which the callback should trigger.
            :type time: int.
            """
            transaction.timers[key] = time

        def set_state(self, key, value):
            tcp_proxy.setState(key, value)

        def get_state(self, key):
            return tcp_proxy.getState(key)

    return (ComputationContext(), transaction)

class Computation:
    """Abstract class for users to extend when making computations.
    """

    def init(ctx):
        """Called when the framework has registered the computation
            successfully. Gives users a first opportunity to schedule
            timer callbacks and produce records.
        :param ctx: The computation context object provided by the system.
        :type ctx: ComputationContext.
        """
        pass

    def destroy():
        """Called right before the concord proxy is ready to shutdown.
        Gives users an opportunity to perform some cleanup before the
        process is killed."""
        pass

    def process_record(ctx, record):
        """Process an incoming record on one of the computation's `istreams`.
        :param ctx: The computation context object provided by the system.
        :type ctx: ComputationContext.
        :param record: The `Record` to emit downstream.
        :type record: Record.
        """
        raise Exception('process_record not implemented')

    def process_timer(ctx, key, time):
        """Process a timer callback previously set via `set_timer`.
        :param ctx: The computation context object provided by the system.
        :type ctx: ComputationContext.
        :param key: The name of the timer.
        :type key: str.
        :param time: The time (in ms) for which the callback was scheduled.
        :type time: int.
        """
        raise Exception('process_timer not implemented')

    def metadata():
        """The metadata defining this computation.
        :returns: Metadata.
        """
        raise Exception('metadata not implemented')

class ComputationServiceWrapper(ComputationService.Iface):
    def __init__(self, handler):
        self.handler = handler
        self.proxy_client = None

    def init(self):
        ctx, transaction = new_computation_context(self.proxy())
        try:
            self.handler.init(ctx)
        except Exception as e:
            ccord_logger.exception(e)
            ccord_logger.critical("Exception in client init")
            sys.exit(1)

        return transaction

    def destroy(self):
        try:
            self.handler.destroy()
        except Exception as e:
            ccord_logger.exception(e)
            ccord_logger.critical("Exception in client destroy")
            sys.exit(1)

    def boltProcessRecords(self, records):
        def txfn(record):
            ctx, transaction = new_computation_context(self.proxy())
            try:
                self.handler.process_record(ctx, record)
            except Exception as e:
                ccord_logger.exception(e)
                ccord_logger.critical("Exception in process_record")
                sys.exit(1)
            return transaction
        return map(txfn, records)

    def boltProcessTimer(self, key, time):
        ctx, transaction = new_computation_context(self.proxy())
        try:
            self.handler.process_timer(ctx, key, time)
        except Exception as e:
            ccord_logger.exception(e)
            ccord_logger.critical("Exception in process_timer")
            sys.exit(1)

        return transaction

    def boltMetadata(self):
        def enrich_stream(stream):
            defaultGrouping = StreamGrouping.SHUFFLE
            sm = StreamMetadata()
            if isinstance(stream, types.TupleType):
                stream_name, grouping = stream
                sm.name = stream_name
                sm.grouping = grouping
            else:
                sm.name = stream
                sm.grouping = defaultGrouping
            return sm

        try:
            ccord_logger.info("Getting client metadata")
            md = self.handler.metadata()
        except Exception as e:
            ccord_logger.exception(e)
            ccord_logger.critical("Exception in metadata")
            sys.exit(1)

        metadata = ComputationMetadata()
        metadata.name = md.name
        metadata.istreams = list(map(enrich_stream, md.istreams))
        metadata.ostreams = md.ostreams
        ccord_logger.info("Got metadata: %s", metadata)
        return metadata

    def proxy(self):
        if not self.proxy_client:
            self.proxy_client = self.new_proxy_client()
        return self.proxy_client

    def new_proxy_client(self):
        host, port = self.proxy_address
        socket = TSocket.TSocket(host, port)
        transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocolAccelerated(transport)
        client = BoltProxyService.Client(protocol)
        transport.open()
        return client

    def set_proxy_address(self, host, port):
        md = self.boltMetadata()
        proxy_endpoint = Endpoint()
        proxy_endpoint.ip = host
        proxy_endpoint.port = port
        md.proxyEndpoint = proxy_endpoint
        self.proxy_address = (host, port)
        proxy = self.proxy()
        proxy.registerWithScheduler(md)

def serve_computation(handler):
    """Helper function. Parses environment variables and starts a thrift service
        wrapping the user-defined computation.
    :param handler: The user computation.
    :type handler: Computation.
    """
    ccord_logger.info("About to serve computation and service")
    if not 'concord_logger' in dir(handler):
        handler.concord_logger = ccord_logger

    def address_str(address):
        host, port = address.split(':')
        return (host, int(port))

    comp = ComputationServiceWrapper(handler)

    _, listen_port = address_str(
        os.environ[kConcordEnvKeyClientListenAddr])
    proxy_host, proxy_port = address_str(
        os.environ[kConcordEnvKeyClientProxyAddr])

    processor = ComputationService.Processor(comp)
    transport = TSocket.TServerSocket(host="127.0.0.1", port=listen_port)
    tfactory = TTransport.TFramedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolAcceleratedFactory()

    try:
        ccord_logger.info("Starting python service port: %d", listen_port)
        server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
        ccord_logger.info("registering with framework at: %s:%d",
                          proxy_host, proxy_port)
        comp.set_proxy_address(proxy_host, proxy_port)
        server.serve()
        concord_logger.error("Exciting service")
    except Exception as exception:
        ccord_logger.exception(exception)
        ccord_logger.critical("Exception in python client")
        sys.exit(1)
