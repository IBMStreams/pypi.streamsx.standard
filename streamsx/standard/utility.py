# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017
"""
Standard utilities for processing streams.
"""

import streamsx.spl.op
from streamsx.topology.schema import StreamSchema
from streamsx.spl.types import float64, uint32, uint64

SEQUENCE_SCHEMA = StreamSchema('tuple<uint64 seq, timestamp ts>')
"""Structured schema containing a sequence identifier and a timestamp.

``'tuple<uint64 seq, timestamp ts>'``
"""

def sequence(topology, period=None, iterations=None, delay=None, name=None):
    """Create a sequence stream.

    Creates a structured stream with schema :py:const:`SEQUENCE_SCHEMA` with
    the ``seq`` attribute starting at zero and monotonically increasing and
    ``ts`` attribute set to the time the tuple was generated.

    Args:
        period(float): Period of tuple generation in seconds, if `None` then tuples are generated as fast as possible.
        iterations(int): Number of tuples on the stream, if `None` then the stream is infinite.
        delay(float): Delay in seconds before the first tuple is submitted, if `None` then the tuples are submitted as soon as possible.
        name(str): Name of the stream, if `None` a generated name is used.

    Returns:
        Stream: Structured stream containing an ever increasing ``seq`` attribute.
    """
    if iterations is not None:
        iterations = int(iterations)
    if period is not None:
        period = float(period)
    if name is None:
        name = 'Sequence'
        if iterations is not None:
            name = name + '({:d})'.format(iterations)
        if period is not None:
            name = name + ':period={:.3f}s'.format(period)

    _op = _Beacon(topology, SEQUENCE_SCHEMA, period=period, iterations=iterations, delay=delay, name=name)
    _op.seq = _op.output('IterationCount()')
    _op.ts = _op.output('getTimestamp()')
    return _op.stream

class _Beacon(streamsx.spl.op.Source):
    def __init__(self, topology, schema, *, period=None, iterations=None, delay=None, triggerCount=None, name=None):
        kind="spl.utility::Beacon"
        inputs=None
        schemas=schema
        params = dict()
        if period is not None:
            params['period'] = float64(period)
        if iterations is not None:
            params['iterations'] = uint32(iterations)
        if delay is not None:
            params['initDelay'] = float64(delay)
        if triggerCount is not None:
            params['triggerCount'] = triggerCount
        super(_Beacon, self).__init__(topology,kind,schemas,params,name)


def spray(stream, count, queue=1000, name=None):
    """Spray tuples to a number of streams.
    Each tuple on `stream` is sent to one (and only one)
    of the returned streams.
    The stream for a specific tuple is not defined,
    instead each stream has a dedicated thread and the
    first available thread will take the tuple and
    submit it.

    Each tuple on `stream` is placed on internal queue before it
    is submitted to an output stream. If the queue fills up
    then processing of the input stream is blocked until there
    is space in the queue.

    Args:
        count(int): Number of output streams the input stream will be sprayed across.
        queue(int): Maximum queue size.
        name(str): Name of the stream, if `None` a generated name is used.

    Returns:
        list(Stream) : List of output streams.
    """
    _op = _ThreadedSplit(stream, count, queue,name=name)
    return _op.outputs


class _ThreadedSplit (streamsx.spl.op.Invoke):
    def __init__(self, stream, count, queue=1000, name=None):
        topology = stream.topology
        kind="spl.utility::ThreadedSplit"
        inputs=stream
        schemas=[stream.oport.schema] * count
        params = dict()
        params['bufferSize'] = uint32(queue)
        super(_ThreadedSplit, self).__init__(topology,kind,inputs,schemas,params,name)


def throttle(stream, rate, precise=False, name=None):
    """Throttle the rate of a stream.

    Args:
         rate(float): Throttled rate of the returned stream in tuples/second.
         precise(bool): Try to make the rate precise at the cost of increased overhead.
         name(str): Name of the stream, if `None` a generated name is used.
    """
    _op = _Throttle(stream, rate, precise=precise, name=name)
    return _op.stream

class _Throttle (streamsx.spl.op.Map):
    """Stream throttle capability
    """
    def __init__(self, stream, rate, *, period=None, includePunctuations=None, precise=None, name=None):
        kind="spl.utility::Throttle"
        params = dict()
        params['rate'] = float64(rate)
        if period is not None:
            params['period'] = float64(period)
        if includePunctuations is not None:
            params['includePunctuations'] = includePunctuations
        if precise is not None:
            params['precise'] = precise
        super(_Throttle, self).__init__(kind,stream,params=params,name=name)


def union(inputs, schema, name=None):
    """Union structured streams with disparate schemas.

    Each tuple on any of the streams in `inputs` results in
    a tuple on the returned stream.

    All attributes of the output tuple are set from the input tuple,
    thus the schema of each input must include attributes matching
    (name and type) the output schema.

    The order of attributes in the input schemas need not match
    the output schemas and the input schemas may contain additional
    attributes which will be discarded.

    Args:
        inputs(list[Stream]): Streams to be unioned.
        schema(StreamSchema): Schema of output stream
        name(str): Name of the stream, if `None` a generated name is used.

    Returns:
        Stream: Stream that is a union of `inputs`.

    """
    _op = _Union(inputs, schema, name=name)
    return _op.stream

class _Union (streamsx.spl.op.Invoke):
    """Union structured streams with disparate schemas.
    """

    def __init__(self, inputs, schema, name=None):
        topology = inputs[0].topology
        kind="spl.utility::Union"
        schemas=schema
        params = None
        super(Union, self).__init__(topology,kind,inputs,schemas,params,name)


def deduplicate(stream, count=None, period=None, name=None):
    """Deduplicate tuples on a stream.

    If a tuple on `stream` is followed by a duplicate tuple
    within `count` tuples or `period` number of seconds
    then the duplicate is discarded from the returned stream.

    Only one of `count` or `period` can be set.

    Args:
        stream(Stream): Stream to be deduplicated.
        count(int): Number of tuples.
        period(float): Time period to check for duplicates.
        name(str): Name of resultant stream, defaults to a generated name.

    Returns:
        Stream: Deduplicated stream.
    """
    if count and period:
        raise ValueError("Cannot set count and period")

    _op = _DeDuplicate(stream, count=count, period=period, name=name)
    return _op.stream

class _DeDuplicate (streamsx.spl.op.Map):
    def __init__(self, stream, *, timeOut=None, count=None, deltaAttribute=None, delta=None, key=None, resetOnDuplicate=None, flushOnPunctuation=None, name=None):
        kind="spl.utility::DeDuplicate"
        params = dict()
        if timeOut is not None:
            params['timeOut'] = float64(timeOut)
        if count is not None:
            params['count'] = int(count)
        if deltaAttribute is not None:
            params['deltaAttribute'] = deltaAttribute
        if delta is not None:
            params['delta'] = delta
        if key is not None:
            params['key'] = key
        if resetOnDuplicate is not None:
            params['resetOnDuplicate'] = resetOnDuplicate
        if flushOnPunctuation is not None:
            params['flushOnPunctuation'] = flushOnPunctuation
        super(DeDuplicate, self).__init__(kind,stream,params=params,name=name)

