"""Computation class for Concord
.. module:: Computation
    :synopsis: Computation class and helper function
"""



import sys
import os
import types
from thrift import Thrift
from thrift.transport import (
    TSocket, TTransport
)
from thrift.server import (
    TServer, TNonblockingServer
)
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

def new_computation_context():
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
        def produce_record(self, stream, key, value):
            """Produce a record to be emitted down stream.

            :param stream: The stream to emit the record on.
            :type stream: str.
            :param key: The key to route this message by (only used when
                using GROUP_BY routing).
            :type key: str.
            :param value: The binary blob to emit down stream.
            :type value: str.
            """
            r = Record()
            r.key = key
            r.value = value
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

class ComputationService:
    def __init__(self, handler):
        self.handler = handler
        self.proxy_client = None
        handler.get_state = self.get_state
        handler.set_state = self.set_state

    def init(self):
        ctx, transaction = new_computation_context()
        self.handler.initialize(ctx)
        return transaction

    def boltProcessRecord(self, record):
        ctx, transaction = new_computation_context()
        self.handler.process_record(ctx, record)
        return transaction

    def boltProcessTimer(self, key, time):
        ctx, transaction = new_computation_context()
        self.handler.process_timer(ctx, key, time)
        return transaction

    def boltMetadata(self):
        def enrich_stream(stream):
            defaultGrouping = StreamGrouping.SHUFFLE
            sm = StreamMetadata()
            if isinstance(stream, types.TupleType):
                stream_name, grouping = stream
                sm.name = stream_name
                sm.grouping = StreamGrouping._NAMES_TO_VALUES.get(grouping, defaultGrouping)
            else:
                sm.name = stream
                sm.grouping = defaultGrouping
            return sm

        md = self.handler.metadata()
        metadata = ComputationMetadata()
        metadata.name = md.name
        metadata.istreams = list(map(enrich_stream, md.istreams))
        metadata.ostreams = list(map(enrich_stream, md.ostreams))
        return metadata

    def set_state(self, key, value):
        proxy = self.proxy()
        proxy.setState(key, value)

    def get_state(self, key):
        proxy = self.proxy()
        return proxy.getState(key)

    def proxy(self):
        if not self.proxy_client:
            self.proxy_client = self.new_proxy_client()
        return self.proxy_client

    def new_proxy_client(self):
        host, port = self.proxy_address
        socket = TSocket.TSocket(host, port)
        transport = TTransport.TFramedTransport(socket)
        protocol = TBinaryProtocol.TBinaryProtocol(transport)
        client = BoltProxyService.Client(protocol)
        transport.open()
        return client

    def set_proxy_address(self, host, port):
        md = self.handler.metadata()
        cmd = ComputationMetadata()
        cmd.name = md.name
        def enrich(x):
            s = StreamMetadata()
            s.name = x
            return s
        cmd.istreams = list(map(enrich, md.istreams))
        cmd.ostreams = list(map(enrich, md.ostreams))
        proxy_endpoint = Endpoint()
        proxy_endpoint.ip = host
        proxy_endpoint.port = port
        cmd.proxyEndpoint = proxy_endpoint
        self.proxy_address = (host, port)
        proxy = self.proxy()
        proxy.registerWithScheduler(cmd)

def serve_computation(handler):
    """Helper function. Parses environment variables and starts a thrift service
        wrapping the user-defined computation.
    :param handler: The user computation.
    :type handler: Computation.
    """
    def address_str(address):
        host, port = address.split(':')
        return (host, int(port))

    comp = ComputationService(handler)

    _, listen_port = address_str(
        os.environ[kConcordEnvKeyClientListenAddr])
    proxy_host, proxy_port = address_str(
        os.environ[kConcordEnvKeyClientProxyAddr])

    comp.set_proxy_address(proxy_host, proxy_port)

    processor = ComputationService.Processor(comp)
    transport = TSocket.TServerSocket(port=listen_port)
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()

    # TODO(agallego) - enable TNonblockingServer written by evernote folks
    # tested to be faster than any other option for python
    tfactory = TTransport.TBufferedTransportFactory()
    server = TServer.TSimpleServer(processor, transport, tfactory, pfactory)
    # server = TNonblockingServer.TNonblockingServer(processor,
    #                                                transport,
    #                                                inputProtocolFactory=pfactory)
    # You could do one of these for a multithreaded server
    #server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    #server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
    server.serve()