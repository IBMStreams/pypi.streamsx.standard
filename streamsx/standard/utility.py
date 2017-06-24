# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import streamsx.spl.op
from streamsx.topology.schema import StreamSchema
from streamsx.spl.types import float64, uint32, uint64

class Beacon(streamsx.spl.op.Source):
    SEQUENCE_SCHEMA = StreamSchema('tuple<uint64 seq, timestamp ts>')
    """Structured schema containing a sequence identifier and a timestamp.

    ``'tuple<uint64 seq, timestamp ts>'``
    """

    @staticmethod
    def sequence(topology, *, period=None, iterations=None, delay=None, name=None):
        """Create a sequenced stream.

        Creates a structured stream with schema :py:const:`SEQUENCE_SCHEMA` with
        the `seq` attribute starting at zero and monotonically increasing and
        `ts` attribute set to the time the tuple was generated.

        Args:
            period(float): Period of tuple generation in seconds, if `None` then tuples
            are generated as fast as possible.
            iterations(int): Number of tuples on the stream, if `None` then the stream
                is infinite.
            delay(float): Delay in seconds before the first tuple is submitted, if `None` then the
            tuples are submitted as soon as possible.
            name(str): Name of the stream, if `None` a generated name is used.

        Returns:
            Stream: Structured stream containing an ever increasing `seq` attribute.
        """
        _op = Beacon(topology, Beacon.SEQUENCE_SCHEMA, period=period, iterations=iterations, delay=delay, name=name)
        _op.seq = _op.output('IterationCount()')
        _op.ts = _op.output('getTimestamp()')
        return _op.stream

    def __init__(self, topology, schema, *, period=None, iterations=None, delay=None, triggerCount=None, name=None):
        kind="spl.utility::Beacon"
        inputs=None
        schemas=schema
        params = dict()
        if period is not None:
            params['period'] = float64(period)
        if iterations is not None:
            params['iterations'] = uint64(iterations)
        if delay is not None:
            params['initDelay'] = float64(delay)
        if triggerCount is not None:
            params['triggerCount'] = triggerCount
        super(Beacon, self).__init__(topology,kind,schemas,params,name)

class ThreadedSplit (streamsx.spl.op.Invoke):

    @staticmethod
    def spray(stream, count, queue=1000, name=None):
        """Spray tuples to the output ports.
        Each tuple on `stream` is sent to one (and only one)
        of the output streams returned by this method.
        The output port for a specific tuple is not defined,
        instead each output stream has a dedicated thread and the
        first available thread will take the tuple and
        submit it to its output.

        Each input tuple is placed on internal queue before it
        is submitted to an output stream. If the queue fills up
        then processing of the input stream is blocked until there
        is space in the queue.

        Args:
            count(int): Number of output streams the input stream
            will be sprayed across.
            queue(int): Maximum queue size.
            name(str): Name of the stream, if `None` a generated name is used.

        Returns:
            list(Stream) : List of output streams
        """
        _op = ThreadedSplit(stream, count, queue=1000,name=name)
        return _op.outputs
    
    def __init__(self, stream, count, queue=1000, name=None):
        topology = stream.topology
        kind="spl.utility::ThreadedSplit"
        inputs=stream
        schemas=[stream.oport.schema] * count
        params = dict()
        params['bufferSize'] = uint32(queue)
        super(ThreadedSplit, self).__init__(topology,kind,inputs,schemas,params,name)


class Throttle (streamsx.spl.op.Map):
    """Stream throttle capability
    """

    @staticmethod
    def at_rate(stream, rate, precise=False, name=None):
        """Throttle the rate of a stream.
        Args:
             name(str): Name of the stream, if `None` a generated name is used.
        """
        _op = Throttle(stream, rate, precise=precise, name=name)
        return _op.stream

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
        super(Throttle, self).__init__(kind,stream,params=params,name=name)


class Union (streamsx.spl.op.Invoke):
    """Union structured streams with disparate schemas.
    """

    @staticmethod
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

        Args
            inputs: Streams to be unioned.
            schema: Schema of output stream
            name(str): Name of the stream, if `None` a generated name is used.

        Returns:
            Stream: Stream that is a union of `inputs`.

        """
        _op = Union(inputs, schema, name=name)
        return _op.stream

    def __init__(self, inputs, schema, *, name=None):
        topology = inputs[0].topology
        kind="spl.utility::Union"
        schemas=schema
        params = None
        super(Union, self).__init__(topology,kind,inputs,schemas,params,name)


class DeDuplicate (streamsx.spl.op.Map):
    """Deduplicate tuples on a stream.
    """

    @staticmethod
    def within_count(stream, count, name=None):
        """Deduplicate tuples within a number of tuples.
        """
        _op = DeDuplicate(stream, count=count, name=name)
        return _op.stream

    @staticmethod
    def within_period(stream, period, name=None):
        """Deduplicate tuples within time period.
        """
        _op = DeDuplicate(stream, timeOut=period, name=name)
        return _op.stream

    def __init__(self, stream, *, timeOut=None, count=None, deltaAttribute=None, delta=None, key=None, resetOnDuplicate=None, flushOnPunctuation=None, name=None):
        kind="spl.utility::DeDuplicate"
        params = dict()
        if timeOut is not None:
            params['timeOut'] = float64(timeOut)
        if count is not None:
            params['count'] = count
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

