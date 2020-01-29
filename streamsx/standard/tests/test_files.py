from unittest import TestCase


import streamsx.standard.files as files
from streamsx.standard import CloseMode, Format, WriteFailureAction

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
import streamsx.topology.context

import os
import tempfile
import shutil

class TestCSV(TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        Tester.setup_standalone(self)

    def tearDown(self):
        shutil.rmtree(self.dir)
     
    def test_read_write(self):
        topo = Topology()
        s = topo.source(range(13))
        sch = 'tuple<rstring a, int32 b>'
        s = s.map(lambda v: ('A'+str(v), v+7), schema=sch)

        fn = os.path.join(self.dir, 'data.csv')
        files.csv_writer(s, fn)
        tester = Tester(topo)
        tester.tuple_count(s, 13)
        tester.test(self.test_ctxtype, self.test_config)

        self.assertTrue(os.path.isfile(fn))

        topo = Topology()
        r = files.csv_reader(topo, schema=sch, file=fn)
        expected = [ {'a':'A'+str(v), 'b':v+7} for v in range(13)]

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.test(self.test_ctxtype, self.test_config)

     
    def test_composite(self):
        topo = Topology()
        s = topo.source(range(13))
        sch = 'tuple<rstring a, int32 b>'
        s = s.map(lambda v: ('A'+str(v), v+7), schema=sch)

        fn = os.path.join(self.dir, 'data.csv')
        fsink = files.FileSink(fn)
        fsink.append = True
        fsink.flush = 1
        fsink.close_mode = CloseMode.punct.name
        fsink.flush_on_punctuation = True
        fsink.format = Format.csv.name
        fsink.has_delay_field = False
        fsink.quote_strings = False
        fsink.write_failure_action = WriteFailureAction.log.name
        fsink.write_punctuations = False
        s.for_each(fsink)

#        result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
#        print('(TOOLKIT):' + str(result))
#        assert(result.return_code == 0)
#        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
#        print('(BUNDLE):' + str(result))
#        assert(result.return_code == 0)

        tester = Tester(topo)
        tester.tuple_count(s, 13)
        tester.test(self.test_ctxtype, self.test_config)

        self.assertTrue(os.path.isfile(fn))

        topo = Topology()
        r = files.csv_reader(topo, schema=sch, file=fn)
        expected = [ {'a':'A'+str(v), 'b':v+7} for v in range(13)]

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.test(self.test_ctxtype, self.test_config)

