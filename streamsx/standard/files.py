# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
"""
Reading and writing of files.
"""

import enum
import streamsx.spl.op
from streamsx.topology.schema import StreamSchema
from streamsx.standard import Format, Compression

import streamsx.standard._version
__version__ = streamsx.standard._version.__version__


def csv_reader(topology, schema, file, header=False, encoding=None, separator=None, ignoreExtraFields=False, hot=False, name=None):
    """Read a comma separated value file as a stream.

    The file defined by `file` is read and mapped to a stream
    with a structured schema of `schema`.

    Args:
        topology(Topology): Topology to contain the returned stream.
        schema(StreamSchema): Schema of the returned stream.
        header: Does the file contain a header.
        encoding: TBD
        separator(str): Separator between records (defaults to comma ``,``).
        ignoreExtraFields:  When `True` then if the file contains more
            fields than `schema` has attributes they will be ignored.
            Otherwise if there are extra fields an error is raised.
        hot(bool): TBD
        name(str): Name of the stream, defaults to a generated name.

    Return:
        (Stream): Stream containing records from the file.
    """
    fe = streamsx.spl.op.Expression.expression(Format.csv.name)
    _op = _FileSource(topology, schema, file=file, format=fe, hotFile=hot,encoding=encoding,separator=separator,ignoreExtraCSVValues=ignoreExtraFields)
    return _op.outputs[0]

def csv_writer(stream, file, append=None, encoding=None, separator=None, flush=None, name=None):
    """Write a stream as a comma separated value file.
    """
    fe = streamsx.spl.op.Expression.expression(Format.csv.name)
    _op = _FileSink(stream, file, format=fe, append=append, encoding=encoding, separator=separator, flush=flush, name=name)

class _DirectoryScan(streamsx.spl.op.Source):
    def __init__(self, topology, schema,directory, pattern=None, sleepTime=None, initDelay=None, sortBy=None, order=None, moveToDirectory=None, ignoreDotFiles=None, ignoreExistingFilesAtStartup=None, name=None):
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
    
    def __init__(self, topology, schemas, file=None, format=None, defaultTuple=None, parsing=None, hasDelayField=None, compression=None, eolMarker=None, blockSize=None, initDelay=None, hotFile=None, deleteFile=None, moveFileToDirectory=None, separator=None, encoding=None, hasHeaderLine=None, ignoreOpenErrors=None, readPunctuations=None, ignoreExtraCSVValues=None, name=None):
        kind="spl.adapter::FileSource"
        inputs=None
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
