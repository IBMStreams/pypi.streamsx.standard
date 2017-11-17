from unittest import TestCase


import streamsx.standard.utility as U

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester

class TestBeacon(TestCase):
    def setUp(self):
        Tester.setup_standalone(self)
     
    def test_sequence(self):
        topo = Topology()
        s = U.sequence(topo, iterations=122)

        tester = Tester(topo)
        tester.tuple_check(s, lambda x: 'seq' in x and 'ts' in x)
        tester.tuple_count(s, 122)
        tester.test(self.test_ctxtype, self.test_config)


class TestThreadedSpit(TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

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

    
