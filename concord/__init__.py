"""Concord's Python client library

.. moduleauthor:: Cole Brown <cole@concord.io>
"""

from .internal.thrift import ttypes
from .computation import (
    Computation,
    Metadata,
    serve_computation
    )
