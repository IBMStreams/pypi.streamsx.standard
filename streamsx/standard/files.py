# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import enum
import streamsx.spl.op
from streamsx.topology.schema import StreamSchema
from streamsx.standard.adapter import FileSink, FileSource, Format, Compression



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
        name(str): Name of the stream.

    Return:
        (Stream): Stream containing records from the file.
    """
    fe = streamsx.spl.op.Expression.expression(Format.csv.name)
    _op = FileSource(topology, schema, file=file, format=fe, hotFile=hot,encoding=encoding,separator=separator,ignoreExtraCSVValues=ignoreExtraFields)
    return _op.outputs[0]

def csv_writer(stream, file, append=None, encoding=None, separator=None, flush=None, name=None):
    """Write a stream as a comma separated value file.
    """
    fe = streamsx.spl.op.Expression.expression(Format.csv.name)
    _op = FileSink(stream, file, format=fe, append=append, encoding=encoding, separator=separator, flush=flush, name=name)
