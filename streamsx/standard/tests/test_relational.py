import sys
from nose.plugins.attrib import attr
from unittest import TestCase

import streamsx.standard.relational as R
import streamsx.standard.utility as U
from streamsx.topology.schema import StreamSchema
from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
import streamsx.topology.context

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

class TestFunctor(TestCase):
    def setUp(self):
        Tester.setup_standalone(self)
     
    def test_transform_two_outputs(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=10))
        fo = R.Functor.map(s, [StreamSchema('tuple<uint64 seq>'),StreamSchema('tuple<timestamp ts>')])
        seq = fo.outputs[0]
        ts = fo.outputs[1]
        seq.print()
        ts.print()

        tester = Tester(topo)
        tester.tuple_count(seq, 10)
        tester.tuple_count(ts, 10)
        tester.test(self.test_ctxtype, self.test_config)

     
    def test_transform_filter(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=5))
        fo = R.Functor.map(s, StreamSchema('tuple<uint64 seq>'), filter='seq>=2ul')
        r = fo.outputs[0]
        r.print()

        tester = Tester(topo)
        tester.tuple_count(r, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_transform_schema(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=10))
        A = U.SEQUENCE_SCHEMA.extend(StreamSchema('tuple<rstring a>'))
        fo = R.Functor.map(s, A)     
        fo.a = fo.output(fo.outputs[0], '"string value"')
        r = fo.outputs[0]
        r.print()

        tester = Tester(topo)
        tester.tuple_count(r, 10)
        tester.test(self.test_ctxtype, self.test_config)

    def test_transform_schema_two_outputs(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=2))
        fo = R.Functor.map(s, [StreamSchema('tuple<uint64 seq, rstring a>'),StreamSchema('tuple<timestamp ts, int32 b>')])
        fo.a = fo.output(fo.outputs[0], '"string value"')
        fo.b = fo.output(fo.outputs[1], 99)
        a = fo.outputs[0]
        b = fo.outputs[1]
        a.print()
        b.print()

        tester = Tester(topo)
        tester.tuple_count(a, 2)
        tester.tuple_count(b, 2)
        tester.test(self.test_ctxtype, self.test_config)


class TestFilter(TestCase):
    def setUp(self):
        Tester.setup_standalone(self)

     
    def test_single_output(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=4))
        matches = R.Filter.matching(s, filter='seq<2ul')

        tester = Tester(topo)
        tester.tuple_count(matches, 2)
        tester.test(self.test_ctxtype, self.test_config)

     
    def test_non_matching_output(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=4))
        matches, non_matches = R.Filter.matching(s, filter='seq<2ul', non_matching=True)

        tester = Tester(topo)
        tester.tuple_count(matches, 2)
        tester.tuple_count(non_matches, 2)
        tester.test(self.test_ctxtype, self.test_config)

    def test_filter_none(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=4))
        matches = R.Filter.matching(s, filter=None)

        tester = Tester(topo)
        tester.tuple_count(matches, 4)
        tester.test(self.test_ctxtype, self.test_config)


