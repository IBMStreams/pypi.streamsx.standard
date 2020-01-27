# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018
"""
Standard utilities for processing streams.
"""

import streamsx.spl.op
from streamsx.topology.schema import StreamSchema
from streamsx.spl.types import float64, uint32, uint64

import streamsx.standard._version
__version__ = streamsx.standard._version.__version__

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
    def __init__(self, topology, schema, period=None, iterations=None, delay=None, triggerCount=None, name=None):
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
    def __init__(self, stream, rate, period=None, includePunctuations=None, precise=None, name=None):
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

    .. note:: 
        This method differs from ``Stream.union`` in that 
        the schemas of input and output streams can differ, while
        ``Stream.union`` requires matching input and output schemas.

    Args:
        inputs(list[Stream]): Streams to be unioned.
        schema(StreamSchema): Schema of output stream
        name(str): Name of the stream, if `None` a generated name is used.

    Returns:
        Stream: Stream that is a union of `inputs`.

    """
    _op = _Union(inputs, schema, name=name)
    return _op.outputs[0]

class _Union (streamsx.spl.op.Invoke):
    """Union structured streams with disparate schemas.
    """

    def __init__(self, inputs, schema, name=None):
        topology = inputs[0].topology
        kind="spl.utility::Union"
        schemas=schema
        params = None
        super(_Union, self).__init__(topology,kind,inputs,schemas,params,name)


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
    def __init__(self, stream, timeOut=None, count=None, deltaAttribute=None, delta=None, key=None, resetOnDuplicate=None, flushOnPunctuation=None, name=None):
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
        super(_DeDuplicate, self).__init__(kind,stream,params=params,name=name)

def delay(stream, delay, max_delayed=1000, name=None):
    """Delay tuples on a stream.

    Delays tuples on `stream` maintaining inter-arrival times
    of tuples and punctuation.

    Example delaying a stream ``ss`` by 0.5 seconds::

        import streamsx.standard.utility as U

        readings = U.delay(readings, delay=1.5)

    Args:
        stream(Stream): Stream to be delayed.
        delay(float): Seconds to delay each tuple.
        max_delayed(int): Number of items that can be delayed before upstream processing is blocked.
        name(str): Name of resultant stream, defaults to a generated name.

    Returns:
        Stream: Delayed stream.
    """
    _op = _Delay(stream, delay, max_delayed, name)
    return _op.stream

class _Delay(streamsx.spl.op.Map):
    def __init__(self, stream, delay, max_delayed=1000, name=None):
        topology = stream.topology
        kind="spl.utility::Delay"
        params = dict()
        params['delay'] = float64(delay)
        if max_delayed is not None:
            params['bufferSize'] = uint32(max_delayed)
        super(_Delay, self).__init__(kind,stream,params=params,name=name)

def pair(stream0, stream1, matching=None, name=None):
    """Pair tuples across two streams.

    This method is used to merge results from performing
    parallel tasks on the same stream, for example peform multiple
    model scoring on the same stream.

    Holds tuples on the two input streams until a matched tuple has been
    received by both input streams. Once matching tuples have received
    the two tuples are submitted to the returned stream with the
    tuple from ``stream0`` followed by the one from ``stream1``.

    Tuples are matched according to the ``matching`` parameter which
    is an attribute name from the input tuple schema,
    typically representing the application key of the tuple.

    If ``matching`` is ``None`` then a match occurs when
    a tuple is received, so that tuples are emitted when a tuple
    has been received by both input streams.
    
    ``stream0`` and ``stream1`` must have the same schema and the resultant
    stream has the same schema.

    These schemas are not supported when ``matching`` is specified.

       * ``CommonSchema.Python``
       * ``CommonSchema.Json``

    This is equivalent to ``merge([stream0, stream1], matching, name)``.

    Example of scoring in parallel::

        import streamsx.standard.utility as U
        
        # Stream of customer information with customer identifier
        # as the id attribute.
        customers = ...
        score_schema = schema.extend(StreamSchema('tuple<float64 score>'))
    
        # Score each tuple on customers in parallel
        cust_churn = s.map(customer_churn_score, schema=score_schema)
        cust_renew = s.map(customer_renew_score, schema=score_schema)
        
        # Pair back as single stream
        # cust_churn_renew stream will contain two tuples for
        # each customer, the churn score followed by the renew score.
        cust_churn_renew = U.pair(cust_churn, cust_renew, matching='id');

    Args:
        stream0(Stream): First input stream.
        stream1(Stream): Second input stream.
        matching(str): Attribute name for matching tuples.
        name(str): Name of resultant stream, defaults to a generated name.

    Returns:
        Stream: Paired stream.
    """
    return merge([stream0, stream1], matching, name)

def merge(inputs, matching=None, name=None):
    """Merge tuples across two (or more) streams.

    This method is used to merge results from performing
    parallel tasks on the same stream, for example peform multiple
    model scoring on the same stream.

    Holds tuples on the input streams until a matched tuple has been
    received by each input stream. Once matching tuples have received
    for all input streams the tuples are submitted to the returned
    stream in order of the input ports.

    Tuples are matched according to the ``matching`` parameter which
    is an attribute name from the input tuple schema,
    typically representing the application key of the tuple.

    If ``matching`` is ``None`` then a match occurs when
    a tuple is received, so that tuples are emitted when a tuple
    has been received by each input port.
    
    All input streams must have the same schema and the resultant
    stream has the same schema.

    These schemas are not supported when ``matching`` is specified.

       * ``CommonSchema.Python``
       * ``CommonSchema.Json``

    Args:
        inputs(list[Stream]): Input streams to be matched.
        matching(str): Attribute name for matching.
        name(str): Name of resultant stream, defaults to a generated name.

    Returns:
        Stream: Merged stream.
    """
    _op = _Pair(inputs, matching, name=name)
    return _op.outputs[0]

class _Pair(streamsx.spl.op.Invoke):
    def __init__(self, inputs, matching=None, buffer_size=None, name=None):
        topology = inputs[0].topology
        kind="spl.utility::Pair"
        schema=inputs[0].oport.schema
        params = dict()
        if buffer_size is not None:
            params['bufferSize'] = uint32(buffer_size)
        super(_Pair, self).__init__(topology,kind,inputs,[schema],params,name)
        if matching is not None:
            for port_idx in range(len(inputs)):
                self.params['partitionBy' + str(port_idx)] = self.attribute(inputs[port_idx], matching)

def gate(stream, control, max_unacked=1, ack_count=1, name=None):
    """Gate tuple flow based upon downstream processing.

    Tuples on `stream` are passed through unmodified to the returned
    stream but the flow is gated by tuples on the `control` stream.

    Up to `max_unacked` tuples flow through the gate before `stream` is
    is blocked. Each tuple arriving on `control` acknowledges `ack_count`
    tuples and thus unblocks `stream` until the number of unacknowledged
    tuples reaches `max_unacked` again.

    The output of some downstream processing is typically used as `control`
    and thus `control` is usually a stream obtained from a `streamsx.topology.topology.PendingStream`.

    Args:
        stream(Stream): Stream to be gated.
        control(Stream): Controlling stream.
        max_unacked(int): Maximum of tuples allowed through the gate without acknowledgement.
        ack_count(int): Count of tuples to acknowledge with each tuple arriving on `control`.
        name(str): Name of resultant stream, defaults to a generated name.
    
    Returns:
        Stream: Gated stream.
    """
    ack_count = uint32(ack_count)
    _op = _Gate([stream,control], maxUnackedTupleCount=max_unacked, numTuplesToAck=ack_count,name=name)
    return _op.outputs[0]

class _Gate(streamsx.spl.op.Invoke):
    def __init__(self, inputs, maxUnackedTupleCount, numTuplesToAck=None, name=None):
        topology = inputs[0].topology
        kind="spl.utility::Gate"
        schema=inputs[0].oport.schema
        params = dict()
        if maxUnackedTupleCount is not None:
            params['maxUnackedTupleCount'] = uint32(maxUnackedTupleCount)
        if numTuplesToAck is not None:
            params['numTuplesToAck'] = numTuplesToAck
        super(_Gate, self).__init__(topology,kind,inputs,[schema],params,name)
