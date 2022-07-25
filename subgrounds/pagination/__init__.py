""" Pagination module docs
"""

from subgrounds.pagination.pagination import (
  paginate,
  paginate_iter,
  PaginationError,
  PaginationStrategy
)

from subgrounds.pagination.strategies import LegacyStrategy, ShallowStrategy

from subgrounds.pagination.preprocess import PaginationNode, generate_pagination_nodes, normalize, prune_doc
