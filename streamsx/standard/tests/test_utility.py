from unittest import TestCase


import streamsx.standard.utility as U

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester

class TestBeacon(TestCase):
    def setUp(self):
        Tester.setup_standalone(self)
     
    def test_sequence(self):
        topo = Topology()
        s = U.Beacon.sequence(topo, iterations=122)

        tester = Tester(topo)
        tester.tuple_check(s, lambda x: 'seq' in x and 'ts' in x)
        tester.tuple_count(s, 122)
        tester.test(self.test_ctxtype, self.test_config)

