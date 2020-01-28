# coding=utf-8
# Licensed Materials - Property of IBM
# Copyright IBM Corp. 2017

"""
Functionality common to multiple modules.
"""

import enum

__all__ = ['CloseMode', 'WriteFailureAction', 'Format', 'Compression']

@enum.unique
class CloseMode(enum.Enum):
    """Write close modes."""
    punct = 0
    """Specifies to close the file when a window or final punctuation is received.."""
    count = 1
    """count is used with the tuples_per_file parameter to close the file when the specified number of tuples have been received."""
    size = 2
    """size is used with the bytes_per_file parameter to close the file when the specified number of bytes have been received.."""
    time = 3
    """time is used with the time_per_file parameter to close the file when the specified time has elapsed."""
    dynamic = 4
    """The file parameter can reference input attributes and is evaluated at each input tuple to compute the file name. If the file name is different from the previous value, the output file closes, and a new file opens."""
    never = 5
    """Default close mode."""

@enum.unique
class WriteFailureAction(enum.Enum):
    """Write failure actions."""
    ignore = 0
    """No action is taken on a write failure, and all future writes fail as well."""
    log = 1
    """The error is logged, and the error condition is cleared."""
    terminate = 2
    """The error is logged, and the operator terminates."""

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
