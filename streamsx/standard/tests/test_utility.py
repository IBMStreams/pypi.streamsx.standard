from unittest import TestCase
import time


import streamsx.standard.utility as U

from streamsx.topology.topology import Topology, PendingStream
from streamsx.topology.tester import Tester
from streamsx.topology.schema import StreamSchema

from streamsx.spl.op import Expression

class PairCheck(object):
    def __init__(self):
        self.first = True
        self.seq = 0
    def __call__(self, t):
        if self.first:
            ok = self.seq == t['seq'] and 1.0 == t['score']
            self.first = False
        else:
            ok = self.seq == t['seq'] and 2.0 == t['score']
            self.first = True
            self.seq += 1
        return ok

# input before split
# {0,7}, {1,8}, {3,9}
#
# port 0 -> {1,14}, {2,16}, {0, 18}
# port 1 -> {0,21}, {1,24}, {2, 27}
#
class PairMatchedCheck(object):
    def __init__(self):
        self.last = None
        self.seen = [0, 0, 0]
        self.p0 = [18, 14, 16]
        self.p1 = [21, 24, 27]
    def __call__(self, t):
        id_ = int(t['id'])
        if self.last is not None:
            if self.last != id_:
                return False

        if self.seen[id_] == 0:
            ok = self.p0[id_] == t['v']
            self.seen[id_] = 1
            self.last = id_
            return ok

        if self.seen[id_] == 1:
            ok = self.p1[id_] == t['v']
            self.seen[id_] = 2
            self.last = None
            return ok

        return False
            
class _Delta(object):
    def __init__(self):
        self._last = None
    def __call__(self, v):
        if v['seq'] == 0:
            self._last = v['ts']
            return None
        else:
            v['d'] = v['ts'].time() - self._last.time()
            return v

class GateCheck(object):
    def __init__(self, N, D):
        self.N = N
        self.D = D * 0.8
        self.c = 0
        self.last = 0
    def __call__(self, t):
        if self.c == 0:
            if self.last:
                ok = (t - self.last) >= self.D
            else:
                ok = True
            self.last = t
        else:
            ok = (t - self.last) < self.D
        self.c += 1
        if self.c == self.N:
            self.c = 0
            self.last = t
        return ok

class TestUtility(TestCase):
    def setUp(self):
        Tester.setup_standalone(self)
     
    def test_sequence(self):
        topo = Topology()
        s = U.sequence(topo, iterations=122)

        tester = Tester(topo)
        tester.tuple_check(s, lambda x: 'seq' in x and 'ts' in x)
        tester.tuple_count(s, 122)
        tester.test(self.test_ctxtype, self.test_config)

    def test_sequence_period(self):
        topo = Topology()
        s = U.sequence(topo, iterations=67, period=0.1)
        E = U.SEQUENCE_SCHEMA.extend(StreamSchema('tuple<float64 d>'))

        s = s.map(_Delta(), schema=E)
        tester = Tester(topo)
        tester.tuple_check(s, lambda x: x['d'] > 0.08)
        tester.tuple_count(s, 67-1)
        tester.test(self.test_ctxtype, self.test_config)

    def test_spray(self):
        topo = Topology()
        s = U.sequence(topo, iterations=2442)
        outs = []
        for so in U.spray(s, count=7):
            outs.append(so.map(lambda x : (x['seq'], x['ts']), schema=U.SEQUENCE_SCHEMA))

        s = outs[0].union(set(outs))
        
        tester = Tester(topo)
        tester.tuple_count(s, 2442)
        tester.test(self.test_ctxtype, self.test_config)

    
    def test_delay(self):
        topo = Topology()
        s = U.sequence(topo, iterations=223)
        s = U.delay(s, delay=0.4)
        
        tester = Tester(topo)
        tester.tuple_count(s, 223)
        tester.tuple_check(s, lambda t : (time.time() - t['ts'].time()) > 0.35)
        tester.test(self.test_ctxtype, self.test_config)

    def test_pair(self):
        topo = Topology()
        s = U.sequence(topo, iterations=932)
        rschema = U.SEQUENCE_SCHEMA.extend(StreamSchema('tuple<float64 score>'))
        r0 = s.map(lambda t : (t['seq'], t['ts'], 1.0), schema=rschema)
        r1 = s.map(lambda t : (t['seq'], t['ts'], 2.0), schema=rschema)

        r = U.pair(r0, r1)

        tester = Tester(topo)
        tester.tuple_count(r, 932*2)
        tester.tuple_check(r, PairCheck())
        tester.test(self.test_ctxtype, self.test_config)

    def test_pair_matched(self):
        topo = Topology()
        s = topo.source([0, 1, 2])
        rs = 'tuple<rstring id, int32 v>'
        s = s.map(lambda t : (str(t),t + 7), schema=rs)
        
        # "Reorder" tuples as well
        r0 = s.map(lambda t : ((int(t['id']) + 1)%3, t['v'] * 2), schema=rs)
        r1 = s.map(lambda t : (t['id'], t['v'] * 3), schema=rs)

        r = U.pair(r0, r1, matching='id')

        tester = Tester(topo)
        tester.tuple_count(r, 6)
        tester.tuple_check(r, PairMatchedCheck())
        tester.test(self.test_ctxtype, self.test_config)

    def test_union(self):
        topo = Topology()
        s = U.sequence(topo, iterations=932)
        A = U.SEQUENCE_SCHEMA.extend(StreamSchema('tuple<int32 a, int32 c>'))
        B = U.SEQUENCE_SCHEMA.extend(StreamSchema('tuple<int32 c, int32 b>'))
        F = U.SEQUENCE_SCHEMA.extend(StreamSchema('tuple<int32 c>'))
        r0 = s.map(lambda t : (t['seq'], t['ts'], 89, t['seq']+19), schema=A)
        r1 = s.map(lambda t : (t['seq'], t['ts'], t['seq']+5, 32), schema=B)

        r = U.union([r0,r1], schema=F)

        r19 = r.filter(lambda t : t['c'] == t['seq'] + 19)
        r5 = r.filter(lambda t : t['c'] == t['seq'] + 5)
        tester = Tester(topo)
        tester.tuple_count(r, 932*2)
        tester.tuple_count(r19, 932)
        tester.tuple_count(r5, 932)
        tester.test(self.test_ctxtype, self.test_config)

    def test_gate(self):
        N=137
        D=0.75
        G=17
        topo = Topology()
        s = topo.source(range(N))
        c = PendingStream(topo)
        g = U.gate(s, c.stream, max_unacked=G)
        g = g.map(lambda _ : time.time())
        r = U.delay(g, delay=D)
        c.complete(r)
        tester = Tester(topo)
        tester.tuple_count(r, N)
        tester.tuple_check(r, GateCheck(G,D))
        tester.test(self.test_ctxtype, self.test_config)
