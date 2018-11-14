# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017,2018

from streamsx.spl.op import Invoke, Map

class Aggregate(Map):
    """Aggregation against a window of a structured schema stream.

    The resuting stream (attribute ``stream``) will contain aggregations
    of the window defined by the methods invoked against the instance.

    In all examples, an input schema of ``tuple<int32 id, timestamp ts, float64 reading>`` is assumed representing an input stream of sensor readings.

    Example of aggregating sensor readings to produce a stream containing
    the maximum, minimum and average reading over the last ten minutes
    updating every minute and grouped by sensor::

        # Declare the window
        win = readings.last(datetime.timedelta(minutes=10)).trigger(datetime.timedelta(minutes=1))

        # Declare the output schema
        schema = tuple<int32 id, timestamp ts, float64 max_reading, float64 min_reading, float64 avg_reading>


        # Invoke the aggregation
        agg = Aggregate.invoke(win, schema, group='id')

        # Declare the output attribute assignments.
        agg.min_reading = agg.min('reading')
        agg.max_reading = agg.max('reading')
        agg.avg_reading = agg.average('reading')

        # resulting stream is agg.stream
        agg.stream.print()

    When an output attribute is not assigned and it has a matching
    input attribute the value in the last tuple for the group is used.
    In the example above ``id`` will be set to the sensor identifier
    of the group and ``ts`` will be set to the timestamp of the most
    recent tuple for the group.

    The aggregation is implemented using the ``spl.relational::Aggregate``
    SPL primitive operator from the SPL Standard toolkit.
    """
    @staticmethod
    def invoke(window, schema, group=None, name=None):
        _op = Aggregate(window, schema, group=group, name=name)
        return _op
  
    def _output_func(self, name, attribute=None):
        _eofn = name + '('
        if attribute is not None:
            _eofn = _eofn + attribute
        _eofn = _eofn + ')'
        return self.output(self.expression(_eofn))
        
    def __init__(self, window, schema, group=None, partitionBy=None, aggregateIncompleteWindows=None, aggregateEvictedPartitions=None, name=None):
        topology = window.topology
        kind="spl.relational::Aggregate"
        params = dict()
        if group is not None:
            params['groupBy'] = group
        if partitionBy is not None:
            params['partitionBy'] = partitionBy
        if aggregateIncompleteWindows is not None:
            params['aggregateIncompleteWindows'] = aggregateIncompleteWindows
        if aggregateEvictedPartitions is not None:
            params['aggregateEvictedPartitions'] = aggregateEvictedPartitions
        super(Aggregate, self).__init__(kind,window,schema,params,name)

    def count(self):
        """Count of tuples in the group.

        Returns an output expression of type ``int32`` to be assigned
        to a field of this object that will map to an output attribute.
        If the invocation is not grouped then the expression is number
        of tuples in the window.

        Example::

            # Count the number of tuples grouped by sensor id in the last minute
            schema = 'tuple<int32 id, timestamp ts, int32 n>'
            agg = Aggregate.invoke(s.last(datetime.timedelta(minutes=1)), schema, group='id')
            agg.n = agg.count()
        """
        return self._output_func('Count')

    def count_all(self):
        """Count of all tuples in the window.
        """
        return self._output_func('CountAll')
    def count_groups(self):
        return self._output_func('CountGroups')

    def max(self, attribute):
        """Maximum value for an input attribute.

        Args:
            attribute(str):
        """
        return self._output_func('Max', attribute)

    def min(self, attribute):
        return self._output_func('Min', attribute)

    def sum(self, attribute):
        return self._output_func('Sum', attribute)

    def average(self, attribute):
        return self._output_func('Average', attribute)

    def first(self, attribute):
        return self._output_func('First', attribute)

    def last(self, attribute):
        return self._output_func('Last', attribute)

    def std(self, attribute, sample=False):
        name = 'SampleStdDev' if sample else 'PopulationStdDev'
        return self._output_func(name, attribute)


class _Filter(Invoke):
    @staticmethod
    def matching(stream, filter, name=None):
        _op = Filter(stream, name=name)
        _op.params['filter'] = _op.expression(filter);
        return _op.outputs[0]

    def __init__(self, stream, filter=None, non_matching=False, name=None):
        topology = stream.topology
        kind="spl.relational::Filter"
        inputs=stream
        schema = stream.oport.schema
        schemas = [schema,schema] if non_matching else schema
        params = dict()
        if filter is not None:
            params['filter'] = filter
        super(Filter, self).__init__(topology,kind,inputs,schemas,params,name)


class _Functor(Invoke):
    @staticmethod
    def map(stream, schema, filter=None, name=None):
        _op = Functor(stream, schema, name=name)
        if filter is not None:
            _op.params['filter'] = _op.expression(filter);
        return _op
   
    def __init__(self, stream, schemas, filter=None, name=None):
        topology = stream.topology
        kind="spl.relational::Functor"
        inputs=stream
        params = dict()
        if filter is not None:
            params['filter'] = filter
        super(Functor, self).__init__(topology,kind,inputs,schemas,params,name)


class _Join(Invoke):
    @staticmethod
    def lookup(reference, reference_key, lookup, lookup_key, schema, name=None):
        _op = Join(reference, lookup.last(0), schemas=schema, name=name)
        _op.params['equalityLHS'] = _op.attribute(reference.stream, reference_key)
        _op.params['equalityRHS'] = _op.attribute(lookup, lookup_key)
        return _op.outputs[0]
    """
    Lookup.
    """

    def __init__(self, left, right, schemas, match=None, algorithm=None, defaultTupleLHS=None, defaultTupleRHS=None, equalityLHS=None, equalityRHS=None, partitionByLHS=None, partitionByRHS=None, name=None):
        topology = left.topology
        kind="spl.relational::Join"
        inputs = [left, right]
        params = dict()
        if match is not None:
            params['match'] = match
        if algorithm is not None:
            params['algorithm'] = algorithm
        if defaultTupleLHS is not None:
            params['defaultTupleLHS'] = defaultTupleLHS
        if defaultTupleRHS is not None:
            params['defaultTupleRHS'] = defaultTupleRHS
        if equalityLHS is not None:
            params['equalityLHS'] = equalityLHS
        if equalityRHS is not None:
            params['equalityRHS'] = equalityRHS
        if partitionByLHS is not None:
            params['partitionByLHS'] = partitionByLHS
        if partitionByRHS is not None:
            params['partitionByRHS'] = partitionByRHS
        super(Join, self).__init__(topology,kind,inputs,schemas,params,name)
