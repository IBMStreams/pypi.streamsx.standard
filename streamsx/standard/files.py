# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
"""
Reading and writing of files.
"""

import os
import enum
import streamsx.spl.op
from streamsx.topology.schema import CommonSchema, StreamSchema
from streamsx.standard import CloseMode, Format, Compression, WriteFailureAction, SortOrder, SortByType
import streamsx.topology.composite

import streamsx.standard._version
__version__ = streamsx.standard._version.__version__


class DirectoryScan(streamsx.topology.composite.Source):
    """
    Watches a directory, and generates file names on the output, one for each file that is found in the directory.

    Example, scanning for files in application directory::

        import streamsx.standard.files as files
        from streamsx.topology.topology import Topology

        dir = streamsx.spl.op.Expression.expression('getApplicationDir()+"'+'/etc"')
        s = topo.source(files.DirectoryScan(directory=dir))

    Example, scanning for files with "csv" file extension::

        s = topo.source(files.DirectoryScan(directory='/opt/ibm/streams-ext/input', pattern='.*\.csv$'))

    Attributes
    ----------
    directory : str|Expression
        Specifies the name of the directory to be scanned
    pattern : str
        Instructs the operator to ignore file names that do not match the regular expression pattern
    schema : StreamSchema
        Output schema, defaults to CommonSchema.String
    options : kwargs
        The additional optional parameters as variable keyword arguments.
    """

    def __init__(self, directory, pattern=None, schema=CommonSchema.String, **options):
        self.directory = directory
        self.schema = schema
        self.pattern = pattern
        self.sleep_time = None
        self.init_delay = None
        self.sort_by = None
        self.order = None
        self.move_to_directory = None
        self.ignore_dot_files = None
        self.ignore_existing_files_at_startup = None
        if 'sleep_time' in options:
            self.sleep_time = options.get('sleep_time')
        if 'init_delay' in options:
            self.init_delay = options.get('init_delay')
        if 'sort_by' in options:
            self.sort_by = options.get('sort_by')
        if 'order' in options:
            self.order = options.get('order')
        if 'move_to_directory' in options:
            self.move_to_directory = options.get('move_to_directory')
        if 'ignore_dot_files' in options:
            self.ignore_dot_files = options.get('ignore_dot_files')
        if 'ignore_existing_files_at_startup' in options:
            self.ignore_existing_files_at_startup = options.get('ignore_existing_files_at_startup')
       

    @property
    def sleep_time(self):
        """
            float: Specifies the minimal time between scans of the directory, in seconds. If this parameter is not specified, the default is 5.0 seconds.
        """
        return self._sleep_time

    @sleep_time.setter
    def sleep_time(self, value):
        self._sleep_time = value

    @property
    def init_delay(self):
        """
            float: Specifies the number of seconds that the DirectoryScan operator delays before it starts to produce tuples.
        """
        return self._init_delay

    @init_delay.setter
    def init_delay(self, value):
        self._init_delay = value

    @property
    def sort_by(self):
        """
            enum: Determines the order in which file names are generated during a single scan of the directory when there are multiple valid files at the same time. The valid values are date and name. If the sort_by parameter is not specified, the default sort order is set to date.
        """
        return self._sort_by

    @sort_by.setter
    def sort_by(self, value):
        self._sort_by = value

    @property
    def order(self):
        """
            enum: Controls how the sortBy parameter sorts the files. The valid values are ascending and descending. If the order parameter is not specified, the default value is set to ascending.
        """
        return self._order

    @order.setter
    def order(self, value):
        self._order = value

    @property
    def move_to_directory(self):
        """
            str: Specifies the name of the directory to which files are moved before the output tuple is generated.
        """
        return self._move_to_directory

    @move_to_directory.setter
    def move_to_directory(self, value):
        self._move_to_directory = value

    @property
    def ignore_dot_files(self):
        """
            bool: Specifies whether the DirectoryScan operator ignores files with a leading period (.) in the directory. By default, the value is set to false and files with a leading period are processed.
        """
        return self._ignore_dot_files

    @ignore_dot_files.setter
    def ignore_dot_files(self, value):
        self._ignore_dot_files = value

    @property
    def ignore_existing_files_at_startup(self):
        """
            bool: Specifies whether the DirectoryScan operator ignores pre-existing files in the directory. By default, the value is set to false and all files are processed as usual. If set to true, any files present in the directory are marked as already processed, and not submitted.
        """
        return self._ignore_existing_files_at_startup

    @ignore_existing_files_at_startup.setter
    def ignore_existing_files_at_startup(self, value):
        self._ignore_existing_files_at_startup = value


    def populate(self, topology, name, **options):

        if self.sleep_time is not None:
            self.sleep_time = streamsx.spl.types.float64(self.sleep_time)
        if self.init_delay is not None:
            self.init_delay = streamsx.spl.types.float64(self.init_delay)
        if self.ignore_existing_files_at_startup is not None:
            if self.ignore_existing_files_at_startup is True:
                self.ignore_existing_files_at_startup = streamsx.spl.op.Expression.expression('true')
        if self.ignore_dot_files is not None:
            if self.ignore_dot_files is True:
                self.ignore_dot_files = streamsx.spl.op.Expression.expression('true')
        if self.sort_by is not None:
            self.sort_by = streamsx.spl.op.Expression.expression(self.sort_by)
        if self.order is not None:
            self.order = streamsx.spl.op.Expression.expression(self.order)

        _op = _DirectoryScan(topology=topology, \
                        schema=self.schema, \
                        directory=self.directory, \
                        pattern=self.pattern, \
                        sleepTime=self.sleep_time, \
                        initDelay=self.init_delay, \
                        sortBy=self.sort_by, \
                        order=self.order, \
                        moveToDirectory=self.move_to_directory, \
                        ignoreDotFiles=self.ignore_dot_files, \
                        ignoreExistingFilesAtStartup=self.ignore_existing_files_at_startup, \
                        name=name)

        return _op.stream

class FileSink(streamsx.topology.composite.ForEach):
    """
    Write a stream to a file

    .. note:: Only the last component of the path name is created if it does not exist. All directories in the path name up to the last component must exist.

    Example for writing a stream to a file::

        import streamsx.standard.files as files
        from streamsx.topology.topology import Topology

        topo = Topology()
        s = topo.source(['Hello', 'World!']).as_string()
        s.for_each(files.FileSink(file='/tmp/data.txt'))

    Example with specifying parameters as kwargs and construct the name of the file with the attribute ``filename`` of the input stream::

        config = {
            'format': Format.txt.name,
            'tuples_per_file': 50000,
            'close_mode': CloseMode.count.name,
            'write_punctuations': True,
            'suppress': 'filename'
        }
        fsink = files.FileSink(file=streamsx.spl.op.Expression.expression('"/tmp/"+'+'filename'), **config)
        to_file.for_each(fsink)

    .. versionadded:: 0.5

    Attributes
    ----------
    file : str
        Name of the output file.
    options : kwargs
        The additional optional parameters as variable keyword arguments.
    """

    def __init__(self, file, **options):
        self.file = file
        self.append = None
        self.bytes_per_file = None
        self.close_mode = None
        self.compression = None
        self.encoding = None
        self.eol_marker = None
        self.flush = None
        self.flush_on_punctuation = None
        self.format = None
        self.has_delay_field = None
        self.move_file_to_directory = None
        self.quote_strings = None
        self.separator = None
        self.suppress = None
        self.time_per_file = None
        self.truncate_on_reset = None
        self.tuples_per_file = None
        self.write_failure_action = None
        self.write_punctuations = None
        if 'append' in options:
            self.append = options.get('append')
        if 'bytes_per_file' in options:
            self.bytes_per_file = options.get('bytes_per_file')
        if 'close_mode' in options:
            self.close_mode = options.get('close_mode')
        if 'compression' in options:
            self.compression = options.get('compression')
        if 'encoding' in options:
            self.encoding = options.get('encoding')
        if 'eol_marker' in options:
            self.eol_marker = options.get('eol_marker')
        if 'flush' in options:
            self.flush = options.get('flush')
        if 'flush_on_punctuation' in options:
            self.flush_on_punctuation = options.get('flush_on_punctuation')
        if 'format' in options:
            self.format = options.get('format')
        if 'has_delay_field' in options:
            self.has_delay_field = options.get('has_delay_field')
        if 'move_file_to_directory' in options:
            self.move_file_to_directory = options.get('move_file_to_directory')
        if 'quote_strings' in options:
            self.quote_strings = options.get('quote_strings')
        if 'separator' in options:
            self.separator = options.get('separator')
        if 'suppress' in options:
            self.suppress = options.get('suppress')
        if 'time_per_file' in options:
            self.time_per_file = options.get('time_per_file')
        if 'truncate_on_reset' in options:
            self.truncate_on_reset = options.get('truncate_on_reset')
        if 'tuples_per_file' in options:
            self.tuples_per_file = options.get('tuples_per_file')
        if 'write_failure_action' in options:
            self.write_failure_action = options.get('write_failure_action')
        if 'write_punctuations' in options:
            self.write_punctuations = options.get('write_punctuations')

    @property
    def append(self):
        """
            bool: Specifies that the generated tuples are appended to the output file. If this parameter is false or not specified, the output file is truncated before the tuples are generated.
        """
        return self._append

    @append.setter
    def append(self, value):
        self._append = value


    @property
    def bytes_per_file(self):
        """
            int: Specifies the approximate size of the output file, in bytes. When the file size exceeds the specified number of bytes, the current output file is closed and a new file is opened. This parameter must be specified when the :py:meth:`~streamsx.standard.files.FileSink.close_mode` parameter is set to size.
        """
        return self._bytes_per_file

    @bytes_per_file.setter
    def bytes_per_file(self, value):
        self._bytes_per_file = value

    @property
    def close_mode(self):
        """
            enum: Specifies when the file closes and a new one opens. This parameter has type enum {punct, count, size, time, dynamic, never} :py:meth:`~streamsx.standard.CloseMode`. The default value is never. For any other value except dynamic, when the specified condition is satisfied, the current output file is closed and a new file is opened for writing.
The parameter value:
punct specifies to close the file when a window or final punctuation is received.
count is used with the :py:meth:`~streamsx.standard.files.FileSink.tuples_per_file` parameter to close the file when the specified number of tuples have been received.
size is used with the :py:meth:`~streamsx.standard.files.FileSink.bytes_per_file` parameter to close the file when the specified number of bytes have been received.
time is used with the :py:meth:`~streamsx.standard.files.FileSink.time_per_file` parameter to close the file when the specified time has elapsed.
If this parameter value is dynamic, the file parameter can reference input attributes and is evaluated at each input tuple to compute the file name. If the file name is different from the previous value, the output file closes, and a new file opens.
In all cases, the file parameter can contain modifiers that are used to generate the file name to be used. The supported modifiers are:
{id}: Each {id} in the file name is replaced with the file number created by the FileSink operator. It has value 0 for the first file, 1 for the second file, and so on.
{localtime:strftimeString}: The contents are replaced by the current local time, formatted as if by the strftime library call.
{gmtime:strftimeString}: The contents are replaced by the current time in the GMT timezone. They are formatted as if by the strftime library call.
The modifiers can be repeated in the string, and are all replaced with their values. If :py:meth:`~streamsx.standard.files.FileSink.close_mode` is dynamic, the file names are compared after the modifiers are substituted.
        """
        return self._close_mode

    @close_mode.setter
    def close_mode(self, value):
        self._close_mode = value

    @property
    def compression(self):
        """
             enum: Specifies the compression mode :py:meth:`~streamsx.standard.Compression`
        """
        return self._compression

    @compression.setter
    def compression(self, value):
        self._compression = value

    @property
    def encoding(self):
        """
             str: Specifies the character set encoding that is used in the output file. Data that is written to the output file is converted from the UTF-8 character set to the specified character set before any compression is performed. The encoding parameter is not valid with formats bin or block.
        """
        return self._encoding

    @encoding.setter
    def encoding(self, value):
        self._encoding = value

    @property
    def eol_marker(self):
        """
             str: Specifies the end of line marker.
        """
        return self._eol_marker

    @eol_marker.setter
    def eol_marker(self, value):
        self._eol_marker = value

    @property
    def flush(self):
        """
            int: Specifies the number of tuples after which to flush the output file. By default no flushing on tuple numbers is performed. 
        """
        return self._flush

    @flush.setter
    def flush(self, value):
        self._flush = value

    @property
    def flush_on_punctuation(self):
        """
            bool: Specifies to flush the output file when a window or final punctuation is received. This parameter defaults to true. 
        """
        return self._flush_on_punctuation

    @flush_on_punctuation.setter
    def flush_on_punctuation(self, value):
        self._flush_on_punctuation = value

    @property
    def format(self):
        """
            enum: Specifies the format of the data :py:meth:`~streamsx.standard.Format`
        """
        return self._format

    @format.setter
    def format(self, value):
        self._format = value

    @property
    def has_delay_field(self):
        """
            bool: Specifies whether to output an extra attribute per tuple, which specifies the inter-arrival delays between the input tuples
        """
        return self._has_delay_field

    @has_delay_field.setter
    def has_delay_field(self, value):
        self._has_delay_field = value

    @property
    def move_file_to_directory(self):
        """
            str: Specifies that the file is moved to the named directory after the file is closed. Any existing file with same name is removed before the file is moved to the move_file_to_directory directory.
        """
        return self._move_file_to_directory

    @move_file_to_directory.setter
    def move_file_to_directory(self, value):
        self._move_file_to_directory = value

    @property
    def quote_strings(self):
        """
            bool: Controls the quoting of top-level rstrings. This parameter can be used only with the csv format. This parameter value is true by default.
        """
        return self._quote_strings

    @quote_strings.setter
    def quote_strings(self, value):
        self._quote_strings = value

    @property
    def separator(self):
        """
            str: Separator between records (defaults to comma ``,``).
        """
        return self._separator

    @separator.setter
    def separator(self, value):
        self._separator = value

    @property
    def suppress(self):
        """
            str: Specifies input attributes to be omitted from the output file. This parameter accepts one or more input attributes as values. Those named attributes are not output to the file. For example, you can use this parameter to omit file name inputs from the output file.
        """
        return self._suppress

    @suppress.setter
    def suppress(self, value):
        self._suppress = value

    @property
    def time_per_file(self):
        """
            float: Specifies the approximate time, in seconds, after which the current output file is closed and a new file is opened. If the :py:meth:`~streamsx.standard.files.FileSink.close_mode` parameter is set to time, this parameter must be specified.
        """
        return self._time_per_file

    @time_per_file.setter
    def time_per_file(self, value):
        self._time_per_file = value

    @property
    def truncate_on_reset(self):
        """
            bool: Specifies to truncate the file when a consistent region is reset.
        """
        return self._truncate_on_reset

    @truncate_on_reset.setter
    def truncate_on_reset(self, value):
        self._truncate_on_reset = value

    @property
    def tuples_per_file(self):
        """
            int: Specifies the maximum number of tuples that can be received for each output file. When the specified number of tuples are received, the current output file is closed and a new file is opened for writing. This parameter must be specified when the :py:meth:`~streamsx.standard.files.FileSink.close_mode` parameter is set to count.
        """
        return self._tuples_per_file

    @tuples_per_file.setter
    def tuples_per_file(self, value):
        self._tuples_per_file = value

    @property
    def write_failure_action(self):
        """
            enum: Specifies the action to take when file write fails. This parameter has values of ignore, log, and terminate of type :py:meth:`~streamsx.standard.WriteFailureAction`
        """
        return self._write_failure_action

    @write_failure_action.setter
    def write_failure_action(self, value):
        self._write_failure_action = value

    @property
    def write_punctuations(self):
        """
            bool: Specifies to write punctuations to the output file. It is false by default. This parameter can be used only with txt, csv, and bin formats.
        """
        return self._write_punctuations

    @write_punctuations.setter
    def write_punctuations(self, value):
        self._write_punctuations = value

    def populate(self, topology, stream, name, **options) -> streamsx.topology.topology.Sink:

        if self.append is not None:
            if self.append is True:
                self.append = streamsx.spl.op.Expression.expression('true')
        if self.bytes_per_file is not None:
            self.bytes_per_file = streamsx.spl.types.uint32(self.bytes_per_file)
        if self.close_mode is not None:
            self.close_mode = streamsx.spl.op.Expression.expression(self.close_mode)
        if self.compression is not None:
            self.compression = streamsx.spl.op.Expression.expression(self.compression)
        if self.flush is not None:
            self.flush = streamsx.spl.types.uint32(self.flush)
        if self.flush_on_punctuation is not None:
            if self.flush_on_punctuation is True:
                self.flush_on_punctuation = streamsx.spl.op.Expression.expression('true')
            else:
                self.flush_on_punctuation = streamsx.spl.op.Expression.expression('false')
        if self.format is not None:
            self.format = streamsx.spl.op.Expression.expression(self.format)
        if self.has_delay_field is not None:
            if self.has_delay_field is True:
                self.has_delay_field = streamsx.spl.op.Expression.expression('true')
            else:
                self.has_delay_field = streamsx.spl.op.Expression.expression('false')
        if self.quote_strings is not None:
            if self.quote_strings is True:
                self.quote_strings = streamsx.spl.op.Expression.expression('true')
            else:
                self.quote_strings = streamsx.spl.op.Expression.expression('false')
        if self.suppress is not None:
            self.suppress = streamsx.spl.op.Expression.expression(self.suppress)
        if self.time_per_file is not None:
            self.time_per_file = streamsx.spl.types.float64(self.time_per_file)
        if self.truncate_on_reset is not None:
            if self.truncate_on_reset is True:
                self.truncate_on_reset = streamsx.spl.op.Expression.expression('true')
            else:
                self.truncate_on_reset = streamsx.spl.op.Expression.expression('false')
        if self.tuples_per_file is not None:
            self.tuples_per_file = streamsx.spl.types.uint32(self.tuples_per_file)
        if self.write_failure_action is not None:
            self.write_failure_action = streamsx.spl.op.Expression.expression(self.write_failure_action)
        if self.write_punctuations is not None:
            if self.write_punctuations is True:
                self.write_punctuations = streamsx.spl.op.Expression.expression('true')
            else:
                self.write_punctuations = streamsx.spl.op.Expression.expression('false')

        _op = _FileSink(stream=stream, \
                        file=self.file, \
                        append=self.append, \
                        bytesPerFile=self.bytes_per_file, \
                        closeMode=self.close_mode, \
                        compression=self.compression, \
                        encoding=self.encoding, \
                        eolMarker=self.eol_marker, \
                        flush=self.flush, \
                        flushOnPunctuation=self.flush_on_punctuation, \
                        format=self.format, \
                        hasDelayField=self.has_delay_field, \
                        moveFileToDirectory=self.move_file_to_directory, \
                        quoteStrings=self.quote_strings, \
                        separator=self.separator, \
                        suppress=self.suppress, \
                        timePerFile=self.time_per_file, \
                        truncateOnReset=self.truncate_on_reset, \
                        tuplesPerFile=self.tuples_per_file, \
                        writeFailureAction=self.write_failure_action, \
                        writePunctuations=self.write_punctuations, \
                        name=name)

        return streamsx.topology.topology.Sink(_op)


class CSVReader(streamsx.topology.composite.Source):
    """Read a comma separated value file as a stream.

    The file defined by `file` is read and mapped to a stream
    with a structured schema of `schema`.

    .. note:: Reads a single file only

    .. seealso:: Use :py:meth:`~streamsx.standard.files.CSVFilesReader` for reading multiple files, each input tuple holds the file name to be read

    Example for reading a file from application directory (file is part of application bundle)::

        import streamsx.standard.files as files
        from streamsx.topology.topology import Topology

        topo = Topology()
        sample_file = '/tmp/local/data.csv' # local file
        topo.add_file_dependency(sample_file, 'etc') # add sample file to etc dir in bundle
        fn = os.path.join('etc', 'data.csv') # file name relative to application dir
        sch = 'tuple<rstring a, int32 b>'
        r = topo.source(files.CSVReader(schema=sch, file=fn))

    Example for reading a file from file system accessible from the running job, for example persistent volume claim (Cloud Pak for Data)::

        import streamsx.standard.files as files
        from streamsx.topology.topology import Topology

        topo = Topology()
        sample_file = '/opt/ibm/streams-ext/data.csv' # file location accessible from running Streams application 
        r = topo.source(files.CSVReader(schema='tuple<rstring a, int32 b>', file=sample_file))

    Args:
        schema(StreamSchema): Schema of the returned stream.
        file(str|Expression): Name of the source file. File name in relative path is expected in application directory, for example the file is added to the application bundle.
        header: Does the file contain a header.
        encoding: Specifies the character set encoding that is used in the output file.
        separator(str): Separator between records (defaults to comma ``,``).
        ignoreExtraFields(bool):  When `True` then if the file contains more
            fields than `schema` has attributes they will be ignored.
            Otherwise if there are extra fields an error is raised.
        hot(bool): Specifies whether the input file is hot, which means it is appended continuously.

    Return:
        (Stream): Stream containing records from the file.
    """
    def __init__(self, schema, file, header=False, encoding=None, separator=None, ignoreExtraFields=False, hot=False):
        self.schema = schema
        self.file = file
        self.header = header
        self.encoding = encoding
        self.separator = separator
        self.ignoreExtraFields = ignoreExtraFields
        self.hot = hot

    def populate(self, topology, name, **options):
        fe = streamsx.spl.op.Expression.expression(Format.csv.name)
        if self.file is None:
            raise ValueError('file must not be None')
        if isinstance(self.file, streamsx.spl.op.Expression) is False:
            if isinstance(self.file, str):
                if os.path.isabs(self.file) is False:       
                    self.file = streamsx.spl.op.Expression.expression('getApplicationDir()+"'+'/'+self.file+'"')
                    print("file="+str(self.file))
            else:
                raise TypeError(self.file)
        _op = _FileSource(topology, schemas=self.schema, file=self.file, format=fe, hotFile=self.hot, encoding=self.encoding, separator=self.separator, hasHeaderLine=self.header, ignoreExtraCSVValues=self.ignoreExtraFields)
        return _op.outputs[0]


class BlockFilesReader(streamsx.topology.composite.Map):
    """Reads files given by input stream and generates tuples with the file content (block of binary data) on the output stream.

    .. note:: Each input tuple holds the file name to be read

    Example, scanning for files with "wav" file extension and reading them::

        import streamsx.standard.files as files
        from streamsx.topology.topology import Topology

        topo = Topology()
        s = topo.source(files.DirectoryScan(directory='/opt/ibm/streams-ext/input', pattern='.*\.wav$'))
        r = s.map(files.BlockFilesReader(block_size=512, file_name='filename'), schema=StreamSchema('tuple<blob speech, rstring filename>'))

    Example, scanning for files with "zip" file extension and reading them::

        import streamsx.standard.files as files
        from streamsx.standard import Compression
        from streamsx.topology.topology import Topology

        topo = Topology()
        s = topo.source(files.DirectoryScan(directory='/opt/ibm/streams-ext/input', pattern='.*\.zip$'))
        r = s.map(files.BlockFilesReader(block_size=1024, compression=Compression.gzip.name), schema=StreamSchema('tuple<blob data>'))

    Args:
        block_size(int): Specifies the block size. If the block_size parameter is not specified, the entire file is read into a single tuple.
        compression(str): Specifies that the source file is compressed. There are three valid values, representing available compression algorithms. These values are: zlib, gzip, and bzip2.
        file_name(str): Each output tuple contains the name of the file that the tuple is read from. Ensure that the name given with this parameter is part of the output schema.
    """
    def __init__(self, block_size=None, compression=None, file_name=None):
        self.block_size = block_size
        self.compression = compression
        self.file_name = file_name

    def populate(self, topology, stream, schema, name, **options):
        fe = streamsx.spl.op.Expression.expression(Format.block.name)
        if self.block_size is not None:
            self.block_size = streamsx.spl.types.uint32(self.block_size)
        if self.compression is not None:
            self.compression = streamsx.spl.op.Expression.expression(self.compression)
        _op = _FileSource(topology, schemas=schema, stream=stream, format=fe, blockSize=self.block_size, compression=self.compression)
        if self.file_name is not None:
            setattr(_op, self.file_name, _op.output(_op.outputs[0], _op.expression('FileName()')))
        return _op.outputs[0]


class CSVFilesReader(streamsx.topology.composite.Map):
    """Reads files given by input stream and generates tuples with the file content on the output stream.

    .. note:: Each input tuple holds the file name to be read

    .. seealso:: Use :py:meth:`~streamsx.standard.files.CSVReader` for single file given as parameter

    Example, scanning for files with "csv" file extension and reading them::

        import streamsx.standard.files as files
        from streamsx.topology.topology import Topology

        topo = Topology()
        s = topo.source(files.DirectoryScan(directory='/opt/ibm/streams-ext/input', pattern='.*\.csv$'))
        r = s.map(files.CSVFilesReader(), schema=StreamSchema('tuple<rstring a, int32 b>'))

    Args:
        header: Does the file contain a header.
        encoding: Specifies the character set encoding that is used in the output file.
        separator(str): Separator between records (defaults to comma ``,``).
        ignoreExtraFields(bool):  When `True` then if the file contains more
            fields than `schema` has attributes they will be ignored.
            Otherwise if there are extra fields an error is raised.
        file_name(str): Each output tuple contains the name of the file that the tuple is read from. Ensure that the name given with this parameter is part of the output schema.
    """
    def __init__(self, header=False, encoding=None, separator=None, ignoreExtraFields=False, file_name=None):
        self.header = header
        self.encoding = encoding
        self.separator = separator
        self.ignoreExtraFields = ignoreExtraFields
        self.file_name = file_name

    def populate(self, topology, stream, schema, name, **options):
        fe = streamsx.spl.op.Expression.expression(Format.csv.name)
        _op = _FileSource(topology, schemas=schema, stream=stream, format=fe, encoding=self.encoding, separator=self.separator, hasHeaderLine=self.header, ignoreExtraCSVValues=self.ignoreExtraFields)
        if self.file_name is not None:
            setattr(_op, self.file_name, _op.output(_op.outputs[0], _op.expression('FileName()')))
        return _op.outputs[0]


class CSVWriter(streamsx.topology.composite.ForEach):
    """Write a stream as a comma separated value file.

    The file defined by `file` is used as output file.

    Example for writing lines to a file::

        import streamsx.standard.files as files
        from streamsx.topology.topology import Topology

        topo = Topology()
        s = topo.source(range(13))
        sch = 'tuple<rstring a, int32 b>'
        s = s.map(lambda v: ('A'+str(v), v+7), schema=sch)
        s.for_each(files.CSVWriter(file='/tmp/data.txt'))

    .. note:: Only the last component of the path name is created if it does not exist. All directories in the path name up to the last component must exist.

    Args:
        file(str|Expression): Name of the output file. File name in relative path is relative to data directory.
        append(bool): Specifies that the generated tuples are appended to the output file. If this parameter is false or not specified, the output file is truncated before the tuples are generated.
        encoding: Specifies the character set encoding that is used in the output file.
        separator(str): Separator between records (defaults to comma ``,``).
        flush(int): Specifies the number of tuples after which to flush the output file. By default no flushing on tuple numbers is performed. 
    Return:
        (streamsx.spl.op.Invoke): Sink operator

    """
    def __init__(self, file, append=None, encoding=None, separator=None, flush=None):
        self.file = file
        self.append = append
        self.encoding = encoding
        self.separator = separator
        self.flush = flush

    def populate(self, topology, stream, name, **options) -> streamsx.topology.topology.Sink:
        fe = streamsx.spl.op.Expression.expression(Format.csv.name)
        _op = _FileSink(stream, self.file, format=fe, append=self.append, encoding=self.encoding, separator=self.separator, flush=self.flush, name=name)
        return streamsx.topology.topology.Sink(_op)


class _DirectoryScan(streamsx.spl.op.Source):
    def __init__(self, topology, schema, directory, pattern=None, sleepTime=None, initDelay=None, sortBy=None, order=None, moveToDirectory=None, ignoreDotFiles=None, ignoreExistingFilesAtStartup=None, name=None):
        kind="spl.adapter::DirectoryScan"
        inputs=None
        schemas=schema
        params = dict()
        params['directory'] = directory
        if pattern is not None:
            params['pattern'] = pattern
        if sleepTime is not None:
            params['sleepTime'] = sleepTime
        if initDelay is not None:
            params['initDelay'] = initDelay
        if sortBy is not None:
            params['sortBy'] = sortBy
        if order is not None:
            params['order'] = order
        if moveToDirectory is not None:
            params['moveToDirectory'] = moveToDirectory
        if ignoreDotFiles is not None:
            params['ignoreDotFiles'] = ignoreDotFiles
        if ignoreExistingFilesAtStartup is not None:
            params['ignoreExistingFilesAtStartup'] = ignoreExistingFilesAtStartup
        super(_DirectoryScan, self).__init__(topology,kind,schemas,params,name)


class _FileSource(streamsx.spl.op.Invoke):
    
    def __init__(self, topology, schemas, stream=None, file=None, format=None, defaultTuple=None, parsing=None, hasDelayField=None, compression=None, eolMarker=None, blockSize=None, initDelay=None, hotFile=None, deleteFile=None, moveFileToDirectory=None, separator=None, encoding=None, hasHeaderLine=None, ignoreOpenErrors=None, readPunctuations=None, ignoreExtraCSVValues=None, name=None):
        kind="spl.adapter::FileSource"
        inputs=stream
        params = dict()
        if file is not None:
            params['file'] = file
        if format is not None:
            params['format'] = format
        if defaultTuple is not None:
            params['defaultTuple'] = defaultTuple
        if parsing is not None:
            params['parsing'] = parsing
        if hasDelayField is not None:
            params['hasDelayField'] = hasDelayField
        if compression is not None:
            params['compression'] = compression
        if eolMarker is not None:
            params['eolMarker'] = eolMarker
        if blockSize is not None:
            params['blockSize'] = blockSize
        if initDelay is not None:
            params['initDelay'] = initDelay
        if hotFile is not None:
            params['hotFile'] = hotFile
        if deleteFile is not None:
            params['deleteFile'] = deleteFile
        if moveFileToDirectory is not None:
            params['moveFileToDirectory'] = moveFileToDirectory
        if separator is not None:
            params['separator'] = separator
        if encoding is not None:
            params['encoding'] = encoding
        if hasHeaderLine is not None:
            params['hasHeaderLine'] = hasHeaderLine
        if ignoreOpenErrors is not None:
            params['ignoreOpenErrors'] = ignoreOpenErrors
        if readPunctuations is not None:
            params['readPunctuations'] = readPunctuations
        if ignoreExtraCSVValues is not None:
            params['ignoreExtraCSVValues'] = ignoreExtraCSVValues
        super(_FileSource, self).__init__(topology,kind,inputs,schemas,params,name)
   

class _FileSink(streamsx.spl.op.Invoke):
    def __init__(self, stream, file, schema=None, format=None, flush=None, flushOnPunctuation=None, eolMarker=None, writePunctuations=None, hasDelayField=None, compression=None, separator=None, encoding=None, quoteStrings=None, closeMode=None, tuplesPerFile=None, timePerFile=None, bytesPerFile=None, moveFileToDirectory=None, append=None, writeFailureAction=None, suppress=None, truncateOnReset=None, writeStateHandlerCallbacks=None, name=None):
        topology = stream.topology
        kind="spl.adapter::FileSink"
        inputs=stream
        schemas=schema
        params = dict()
        params['file'] = file
        if format is not None:
            params['format'] = format
        if flush is not None:
            params['flush'] = flush
        if flushOnPunctuation is not None:
            params['flushOnPunctuation'] = flushOnPunctuation
        if eolMarker is not None:
            params['eolMarker'] = eolMarker
        if writePunctuations is not None:
            params['writePunctuations'] = writePunctuations
        if hasDelayField is not None:
            params['hasDelayField'] = hasDelayField
        if compression is not None:
            params['compression'] = compression
        if separator is not None:
            params['separator'] = separator
        if encoding is not None:
            params['encoding'] = encoding
        if quoteStrings is not None:
            params['quoteStrings'] = quoteStrings
        if closeMode is not None:
            params['closeMode'] = closeMode
        if tuplesPerFile is not None:
            params['tuplesPerFile'] = tuplesPerFile
        if timePerFile is not None:
            params['timePerFile'] = timePerFile
        if bytesPerFile is not None:
            params['bytesPerFile'] = bytesPerFile
        if moveFileToDirectory is not None:
            params['moveFileToDirectory'] = moveFileToDirectory
        if append is not None:
            params['append'] = append
        if writeFailureAction is not None:
            params['writeFailureAction'] = writeFailureAction
        if suppress is not None:
            params['suppress'] = suppress
        if truncateOnReset is not None:
            params['truncateOnReset'] = truncateOnReset
        if writeStateHandlerCallbacks is not None:
            params['writeStateHandlerCallbacks'] = writeStateHandlerCallbacks
        super(_FileSink, self).__init__(topology,kind,inputs,schema,params,name)
