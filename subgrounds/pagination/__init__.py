""" This module contains all code related to automatic pagination.

The ``pagination`` module contains the pagination algorithms (both regular and iterative)
that make use of ``PaginationStrategies``.

The ``preprocess`` and ``strategties`` modules implement the currently supported ``PaginationStrategies``:
``LegacyStrategy`` and ``ShallowStrategy``.

The ``utils`` module contains some generic functions that are useful in the context of pagination.
"""

from subgrounds.pagination.pagination import (
  paginate,
  paginate_iter,
  PaginationError,
  PaginationStrategy
)

from subgrounds.pagination.strategies import LegacyStrategy, ShallowStrategy

from subgrounds.pagination.preprocess import PaginationNode, generate_pagination_nodes, normalize, prune_doc
