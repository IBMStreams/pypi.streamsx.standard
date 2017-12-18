# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

"""
Functionality common to multiple modules.
"""

import enum

__all__ = ['Format', 'Compression']

@enum.unique
class Format(enum.Enum):
    """Formats for adapters."""
    csv = 0
    """Comma separated values."""
    txt = 1
    """Streams character representation of a tuple."""
    bin = 2
    """Streams binary representation of a tuple."""
    block = 3
    """Block of binary data."""
    line = 4
    """Line of text."""

@enum.unique
class Compression(enum.Enum):
    """Supported data compression algorithms."""
    zlib = 0
    """`zlib <https:://en.wikipedia.org/wiki/Zlib>`_ data compression."""
    gzip = 1
    """`gzip <https:://en.wikipedia.org/wiki/Gzip>`_ data compression."""
    bzip2 = 2
    """`bzip2 <https:://en.wikipedia.org/wiki/Bzip2>`_ data compression."""
