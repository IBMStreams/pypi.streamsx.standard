import sys
from nose.plugins.attrib import attr
from unittest import TestCase

import streamsx.standard.relational as R
import streamsx.standard.utility as U

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester

ARSCHEMA='tuple<int32 acount, int32 acount_all, uint64 amax>'

@attr(ns='relational')
class TestAggregate(TestCase):
    def setUp(self):
        Tester.setup_standalone(self)
     
    def test_batch_aggregate(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=122))

        w = s.batch(size=10)

        a = R.Aggregate.invoke(w, ARSCHEMA)
        a.acount = a.count()
        a.acount_all = a.count_all()
        a.amax = a.max('seq')

        r = a.stream

        tester = Tester(topo)
        # Mimic the aggregate processing
        expected = []
        for i in range(0, 120, 10):
            expected.append({'acount':10, 'acount_all':10, 'amax':i+10-1})
        tester.contents(r, expected)
        tester.tuple_count(r, 12)
        tester.test(self.test_ctxtype, self.test_config)


