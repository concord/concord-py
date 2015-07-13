import concord
import sys

class TestConsumer:
    def initialize(self, ctx):
        pass

    def process_record(self, ctx, record):
        print(record, file=sys.stderr)

    def metadata(self):
        return concord.Metadata(
                name='testy-consumer',
                istreams=['words'],
                ostreams=[])

concord.serve_computation(TestConsumer())

