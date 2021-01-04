import sys
import os
from nose.plugins.attrib import attr
from unittest import TestCase

import streamsx.standard.relational as R
import streamsx.standard.utility as U
from streamsx.topology.schema import StreamSchema
from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
import streamsx.topology.context

from streamsx.topology.context import submit, ContextTypes,ConfigParams
import streamsx.spl.op as op
import streamsx.spl.types
import streamsx.standard.files as files
from streamsx.standard import Compression, Format

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

    def test_aggregate_compile_only(self):
        topo = Topology(name='test_aggregate_compile_only')
        s = topo.source(U.Sequence(iterations=3))
        w = s.batch(size=10)
        a = R.Aggregate.invoke(w, ARSCHEMA, group='seq')
        a.acount = a.count()
        a.acount_all = a.count_all()
        a.amax = a.max('seq')
        sr = streamsx.topology.context.submit('BUNDLE', topo)
        self.assertEqual(0, sr['return_code'])
        os.remove(sr.bundlePath)
        os.remove(sr.jobConfigPath)
 
    def test_join_streams_type_check(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=10))
        fo = R.Functor.map(s, [StreamSchema('tuple<uint64 seq>'),StreamSchema('tuple<timestamp ts>')])
        seq = fo.outputs[0]
        ts = fo.outputs[1]
        # expect TypeError since two streams are given
        self.assertRaises(TypeError, R.Join.lookup, reference=seq, reference_key='seq', lookup=ts, lookup_key='ts', schema=StreamSchema('tuple<uint64 seq, timestamp ts>'))

    def test_event_time_compile_only(self):
        topo = Topology(name='VwapEventTime')

        TQRecT = 'tuple<rstring ticker,rstring date, rstring time, int32 gmtOffset, rstring ttype, rstring exCntrbID, decimal64 price, decimal64 volume, decimal64 vwap, rstring buyerID, decimal64 bidprice, decimal64 bidsize, int32 numbuyers, rstring sellerID, decimal64 askprice, decimal64 asksize, int32 numsellers, rstring qualifiers, int32 seqno, rstring exchtime, decimal64 blockTrd, decimal64 floorTrd, decimal64 PEratio, decimal64 yield, decimal64 newprice, decimal64 newvol, int32 newseqno, decimal64 bidimpvol, decimal64 askimpcol, decimal64 impvol>'
        TradeInfoT = 'tuple<decimal64 price, decimal64 volume, rstring date, rstring time, timestamp evTime, rstring ticker>'
        QuoteInfoT = 'tuple<decimal64 bidprice, decimal64 askprice, decimal64 asksize, rstring date, rstring time, timestamp evTime, rstring ticker>'
        VwapT = 'tuple<rstring ticker, decimal64 minprice, decimal64 maxprice, decimal64 avgprice, decimal64 vwap, timestamp start, timestamp end>'
        BargainIndexT = 'tuple<rstring ticker, decimal64 vwap, decimal64 askprice, decimal64 asksize, rstring date, rstring time, decimal64 index>'

        script_dir = os.path.dirname(os.path.realpath(__file__))

        input_file = 'TradesAndQuotes.csv.gz'    
        sample_file = os.path.join(script_dir, input_file)
        topo.add_file_dependency(sample_file, 'etc') # add sample file to etc dir in bundle
        fn = os.path.join('etc', input_file) # file name relative to application dir
        trade_quote_raw_stream = topo.source(files.CSVReader(schema=TQRecT, file=fn, compression=Compression.gzip.name))

        TQRecTWithEvTime = StreamSchema(TQRecT).extend(StreamSchema('tuple<timestamp evTime>'))
        fo = R.Functor.map(trade_quote_raw_stream, TQRecTWithEvTime)     
        fo.evTime = fo.output(fo.outputs[0], op.Expression.expression('timeStringToTimestamp(date, time, false)'))
        trade_quote_eventtime_stream = fo.outputs[0]
        event_time_supported = True
        try:
            trade_quote_eventtime_stream = trade_quote_eventtime_stream.set_event_time('evTime')
        except AttributeError:
            event_time_supported = False

        # split quotes and trades
        fq = R.Functor.map(trade_quote_eventtime_stream, StreamSchema(QuoteInfoT), filter='ttype=="Quote" && (ticker in {"BK", "IBM", "ANR"})', name='QuoteFilter')
        quotes_stream = fq.outputs[0]

        ft = R.Functor.map(trade_quote_eventtime_stream, StreamSchema(TradeInfoT), filter='ttype=="Trade" && (ticker in {"BK", "IBM", "ANR"})', name='TradeFilter')
        trades_stream = ft.outputs[0]

        # Aggregation over event-time intervals of 10 seconds, calculated every second
        if event_time_supported:
            w = trades_stream.time_interval(interval_duration=10.0, creation_period=1.0).partition('ticker')
        else:
            w = trades_stream.batch(size=10)
        aggregate_schema = StreamSchema(VwapT).extend(StreamSchema('tuple<decimal64 sumvolume, rstring pt>'))
        a = R.Aggregate.invoke(w, aggregate_schema, name='PreVwap')
        a.vwap = a.sum('price * volume')
        a.minprice = a.min('price')
        a.maxprice = a.max('price')
        a.avgprice = a.average('price')
        a.sumvolume = a.sum('volume')
        a.start = a.interval_start()
        a.end = a.interval_end()
        a.pt = a.pane_timing()
        pre_vwap_stream = a.stream

        f_vwap = R.Functor.map(pre_vwap_stream, StreamSchema(VwapT), name='Vwap')
        f_vwap.vwap = f_vwap.output(f_vwap.outputs[0], 'vwap / sumvolume')
        vwap_stream = f_vwap.outputs[0]

        # Join quotes with an event-time up to one second greater than the VWAP time */
        win_vwap = vwap_stream.last(size=100).partition('ticker')
        j = R.Join.lookup(
           reference=win_vwap,
           reference_key='ticker',
           lookup=quotes_stream,
           lookup_key='ticker',
           schema=BargainIndexT,
           match='(Vwap.end <= QuoteFilter.evTime) && (QuoteFilter.evTime < add(Vwap.end, (float64)1.0))',
           name='BargainIndex')
        j.index = j.output(j.outputs[0], 'vwap > askprice ? asksize * exp(vwap - askprice) : 0d')
        bargain_index_stream = j.outputs[0]

        fsink_config = {
            'format': Format.txt.name
        }
        fsink = files.FileSink(file=streamsx.spl.op.Expression.expression('"'+script_dir+'/out.txt"'), **fsink_config)
        bargain_index_stream.for_each(fsink)

        sr = streamsx.topology.context.submit('BUNDLE', topo)
        self.assertEqual(0, sr['return_code'])
        os.remove(sr.bundlePath)
        os.remove(sr.jobConfigPath)

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


