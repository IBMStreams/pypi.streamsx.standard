# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import streamsx.spl.op

class Join(streamsx.spl.op.Invoke):
    @staticmethod
    def lookup(reference, reference_key, lookup, lookup_key, schema, name=None):
        _op = Join(reference, lookup.last(0), equalityLHS=reference_key, equalityRHS=lookup_key, schemas=schema, name=name)
        return _op

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
