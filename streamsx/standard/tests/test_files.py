from unittest import TestCase


import streamsx.standard.files as files
import streamsx.standard.utility as U
import streamsx.standard.relational as R
from streamsx.standard import CloseMode, Format, Compression, WriteFailureAction, SortOrder, SortByType

from streamsx.topology.topology import Topology
from streamsx.topology.tester import Tester
import streamsx.topology.context
from streamsx.topology.schema import CommonSchema, StreamSchema

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
        s.for_each(files.CSVWriter(fn))

        tester = Tester(topo)
        tester.tuple_count(s, 13)
        tester.test(self.test_ctxtype, self.test_config)

        self.assertTrue(os.path.isfile(fn))

        topo = Topology()
        r = topo.source(files.CSVReader(schema=sch, file=fn))
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

        tester = Tester(topo)
        tester.tuple_count(s, 13)
        tester.test(self.test_ctxtype, self.test_config)

        self.assertTrue(os.path.isfile(fn))

        topo = Topology()
        r = topo.source(files.CSVReader(schema=sch, file=fn))
        expected = [ {'a':'A'+str(v), 'b':v+7} for v in range(13)]

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.test(self.test_ctxtype, self.test_config)

    def test_composite_kwargs(self):
        topo = Topology()
        s = topo.source(range(13))
        sch = 'tuple<rstring a, int32 b>'
        s = s.map(lambda v: ('A'+str(v), v+7), schema=sch)

        fn = os.path.join(self.dir, 'data.csv')

        config = {
            'append': True,
            'flush': 1,
            'close_mode': CloseMode.punct.name,
            'flush_on_punctuation': True,
            'format': Format.csv.name,
            'has_delay_field': False,
            'quote_strings': False,
            'write_failure_action': WriteFailureAction.log.name,
            'write_punctuations': False,
        }
        fsink = files.FileSink(fn, **config)
        s.for_each(fsink)

        tester = Tester(topo)
        tester.tuple_count(s, 13)
        tester.test(self.test_ctxtype, self.test_config)

        self.assertTrue(os.path.isfile(fn))

        topo = Topology()
        r = topo.source(files.CSVReader(schema=sch, file=fn))
        expected = [ {'a':'A'+str(v), 'b':v+7} for v in range(13)]

        tester = Tester(topo)
        tester.contents(r, expected)
        tester.test(self.test_ctxtype, self.test_config)

    def test_read_file_from_application_dir(self):
        topo = Topology()
        script_dir = os.path.dirname(os.path.realpath(__file__))
        sample_file = os.path.join(script_dir, 'data.csv')
        topo.add_file_dependency(sample_file, 'etc') # add sample file to etc dir in bundle
        fn = os.path.join('etc', 'data.csv') # file name relative to application dir
        sch = 'tuple<rstring a, int32 b>'
        #fn = streamsx.spl.op.Expression.expression('getApplicationDir()+"'+'/'+fn+'"')
        r = topo.source(files.CSVReader(schema=sch, file=fn))
        r.print()

        tester = Tester(topo)
        tester.tuple_count(r, 3)
        tester.test(self.test_ctxtype, self.test_config)

    def test_filename_from_stream(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=5))
        F = U.SEQUENCE_SCHEMA.extend(StreamSchema('tuple<rstring filename>'))
        fo = R.Functor.map(s, F)     
        fo.filename = fo.output(fo.outputs[0], '"myfile_{id}.txt"')
        to_file = fo.outputs[0]

        config = {
            'format': Format.txt.name,
            'tuples_per_file': 5,
            'close_mode': CloseMode.count.name,
            'write_punctuations': True,
            'suppress': 'filename, ts'
        }
        fsink = files.FileSink(file=streamsx.spl.op.Expression.expression('"'+self.dir+'/"+'+'filename'), **config)
        to_file.for_each(fsink)

        tester = Tester(topo)
        tester.tuple_count(to_file, 5)
        tester.test(self.test_ctxtype, self.test_config)

    def test_filesreader_compile_only(self):
        topo = Topology()
        s = topo.source(files.DirectoryScan(directory='/opt/ibm/streams-ext/input', pattern='.*\.zip$')) 
        r = s.map(files.CSVFilesReader(compression='gzip'), schema=StreamSchema('tuple<rstring a, int32 b>')) 
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        assert(result.return_code == 0)
        os.remove(result.bundlePath)
        os.remove(result.jobConfigPath)

    def test_filesink_compile_only(self):
        topo = Topology()
        s = topo.source(U.Sequence(iterations=5))
        F = U.SEQUENCE_SCHEMA.extend(StreamSchema('tuple<rstring filename>'))
        fo = R.Functor.map(s, F)     
        fo.filename = fo.output(fo.outputs[0], '"myfile_{id}.txt"')
        to_file = fo.outputs[0]

        config = {
            'format': Format.txt.name,
            'encoding': 'ISO_8859-9',
            'compression': Compression.gzip.name,
            'move_file_to_directory': '/tmp',
            'close_mode': CloseMode.time.name,
            'time_per_file': '10.0'
        }
        fsink = files.FileSink(file=streamsx.spl.op.Expression.expression('"'+self.dir+'/"+'+'filename'), **config)
        to_file.for_each(fsink)

        s1 = topo.source(range(13))
        sch1 = 'tuple<rstring a>'
        s1 = s1.map(lambda v: (';A'+str(v), v+7), schema=sch1)
        config1 = {
            'format': Format.line.name,
            'eol_marker': ';',
            'close_mode': CloseMode.size.name,
            'bytes_per_file': '5'
        }
        fsink1 = files.FileSink(file='a', **config1)
        s1.for_each(fsink1)

        s2 = topo.source(U.Sequence(iterations=5))
        from streamsx.topology.state import ConsistentRegionConfig
        s2.set_consistent(ConsistentRegionConfig.periodic(20.0))
        config2 = {
            'format': Format.csv.name,
            'write_state_handler_callbacks': True,
            'truncate_on_reset': True,
            'has_delay_field': True,
            'separator': ';'
        }
        fsink2 = files.FileSink(file='b', **config2)
        s2.for_each(fsink2)

        s3 = topo.source(files.DirectoryScan(directory='/tmp', pattern='.*\.gz$'))
        file_line_stream = s3.map(files.LineFilesReader(compression=Compression.gzip.name), schema=CommonSchema.String)

        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        assert(result.return_code == 0)
        os.remove(result.bundlePath)
        os.remove(result.jobConfigPath)


class TestParams(TestCase):

    def test_filename_ok(self):
        topo = Topology()
        fn = streamsx.spl.op.Expression.expression('getApplicationDir()+"'+'/a/b"')
        topo.source(files.CSVReader(schema='tuple<rstring a, int32 b>', file=fn))
        topo.source(files.CSVReader(schema=CommonSchema.String, file="/tmp/a"))

    def test_filename_bad(self):
        topo = Topology()
        fn = 1
        self.assertRaises(TypeError, topo.source, files.CSVReader, 'tuple<rstring a>', fn) # expects str or Expression for file


class TestDirScan(TestCase):
    def setUp(self):
        self.dir = tempfile.mkdtemp()
        Tester.setup_standalone(self)

    def tearDown(self):
        shutil.rmtree(self.dir)

    def test_dir_scan(self):
        topo = Topology()
        script_dir = os.path.dirname(os.path.realpath(__file__))
        sample_file = os.path.join(script_dir, 'data.csv')
        topo.add_file_dependency(sample_file, 'etc') # add sample file to etc dir in bundle
        fn = os.path.join('etc', 'data.csv') # file name relative to application dir
        dir = streamsx.spl.op.Expression.expression('getApplicationDir()+"'+'/etc"')
        scanned = topo.source(files.DirectoryScan(directory=dir, pattern='.*\.csv$'))
        r = scanned.map(files.CSVFilesReader(file_name='filename'), schema=StreamSchema('tuple<rstring a, int32 b, rstring filename>'))
        r.print()

        #result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        #print('(TOOLKIT):' + str(result))
        #assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        assert(result.return_code == 0)
        os.remove(result.bundlePath)
        os.remove(result.jobConfigPath)


    def test_dir_scan_params(self):
        topo = Topology()
        dir = streamsx.spl.op.Expression.expression('getApplicationDir()+"'+'/etc"')

        config = {
            'init_delay': 5.0,
            'sleep_time': 5.0,
            'sort_by': 'name',
            'order': 'descending',
            'move_to_directory': '/tmp',
            'ignore_existing_files_at_startup': True,
            'ignore_dot_files': True
        }
        scanned = topo.source(files.DirectoryScan(directory=dir, pattern='.*\.csv$', **config))

        sr = streamsx.topology.context.submit('BUNDLE', topo)
        self.assertEqual(0, sr['return_code'])
        os.remove(sr.bundlePath)
        os.remove(sr.jobConfigPath)

    def test_block_reader(self):
        topo = Topology()
        script_dir = os.path.dirname(os.path.realpath(__file__))
        sample_file = os.path.join(script_dir, 'data.csv')
        topo.add_file_dependency(sample_file, 'etc') # add sample file to etc dir in bundle
        fn = os.path.join('etc', 'data.csv') # file name relative to application dir
        dir = streamsx.spl.op.Expression.expression('getApplicationDir()+"'+'/etc"')
        scanned = topo.source(files.DirectoryScan(directory=dir, pattern='.*\.csv$'))
        r = scanned.map(files.BlockFilesReader(file_name='filename'), schema=StreamSchema('tuple<blob payload, rstring filename>'))
        r.print()

        r1 = scanned.map(files.BlockFilesReader(block_size=256, file_name='conversation_id'), schema=StreamSchema('tuple<blob payload, rstring conversation_id>'))
        r1.print()

        r2 = scanned.map(files.BlockFilesReader(block_size=256, compression=Compression.gzip.name), schema=StreamSchema('tuple<blob payload>'))
        r2.print()

        #result = streamsx.topology.context.submit("TOOLKIT", topo.graph) # creates tk* directory
        #print('(TOOLKIT):' + str(result))
        #assert(result.return_code == 0)
        result = streamsx.topology.context.submit("BUNDLE", topo.graph)  # creates sab file
        assert(result.return_code == 0)
        os.remove(result.bundlePath)
        os.remove(result.jobConfigPath)


