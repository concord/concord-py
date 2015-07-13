import concord
import time

def timeMillis():
    return int(round(time.time() * 1000))

class TestComputation:
    def initialize(self, ctx):
        ctx.set_timer('loop', timeMillis())

    def process_timer(self, ctx, key, timer):
        ctx.produce_record('words', 'foo', '1')
        ctx.set_timer(key, timeMillis() + 1000)

    def metadata(self):
        return concord.Metadata(
            name='testy',
            istreams=[],
            ostreams=['words'])

concord.serve_computation(TestComputation())
