# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

import streamsx.spl.op
from streamsx.topology.schema import StreamSchema

import enum
Format = enum.Enum('Format', 'csv txt bin block line')
Compression = enum.Enum('Compression', 'zlib gzip bzip2')
CloseMode = enum.Enum('CloseMode', 'punct count size time dynamic never')
WriteFailureAction = enum.Enum('WriteFailureAction', 'ignore log terminate')

