from unittest import TestCase


import streamsx.standard.files as files

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester

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
