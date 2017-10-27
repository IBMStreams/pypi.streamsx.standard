# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import streamsx.spl.op

class Aggregate(streamsx.spl.op.Map):
    @staticmethod
    def aggregate(window, schema, groupBy=None, name=None):
        _op = Aggregate(window, schema, aggregateIncompleteWindows=True, name=name)
        if groupBy is not None:
            _op.params['groupBy'] = _op.attribute(window.stream, groupBy)
        return _op
  
    def output_func(self, name, attribute=None):
        _eofn = name + '('
        if attribute is not None:
            _eofn = _eofn + attribute
        _eofn = _eofn + ')'
        return self.output(self.expression(_eofn))
        
    def __init__(self, window, schema, groupBy=None, partitionBy=None, aggregateIncompleteWindows=None, aggregateEvictedPartitions=None, name=None):
        topology = window.topology
        kind="spl.relational::Aggregate"
        params = dict()
        if groupBy is not None:
            params['groupBy'] = groupBy
        if partitionBy is not None:
            params['partitionBy'] = partitionBy
        if aggregateIncompleteWindows is not None:
            params['aggregateIncompleteWindows'] = aggregateIncompleteWindows
        if aggregateEvictedPartitions is not None:
            params['aggregateEvictedPartitions'] = aggregateEvictedPartitions
        super(Aggregate, self).__init__(kind,window,schema,params,name)

    def Count(self):
        return self.output_func('Count')
    def Count_all(self):
        return self.output_func('CountAll')
    def Count_groups(self):
        return self.output_func('CountGroups')
    def Max(self, attribute):
        return self.output_func('Max', attribute)
    def Min(self, attribute):
        return self.output_func('Min', attribute)
    def Sum(self, attribute):
        return self.output_func('Sum', attribute)
    def Average(self, attribute):
        return self.output_func('Average', attribute)
    def Std(self, attribute, sample=False):
        name = 'SampleStdDev' if sample else 'PopulationStdDev'
        return self.output_func(name, attribute)


class Filter(streamsx.spl.op.Invoke):
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


class Functor(streamsx.spl.op.Invoke):
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


class Join(streamsx.spl.op.Invoke):
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
