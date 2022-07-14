""" Pagination module

This module implements functions and data structures used to perform automatic
pagination on user-specified Subgrounds GraphQL queries.

Pagination is done in two steps:
1. The input query is transformed such that every field selection in the query
which yields a list of entities has:

#. An ordering (i.e.: ``orderBy`` and ``orderDirection`` are specified)
#. A ``first`` argument set to the ``firstN`` variable
#. A ``skip`` argument set to the ``skipN`` variable
#. A ``where`` filter with the filter name derived from the ordering and the
   value being a variable named ``lastOrderingValueN``

In other words, the query will be transformed in a form which allows Subgrounds
to paginate automatically by simply setting the set of pagination variables
(i.e.: ``firstN``, ``skipN`` and ``lastOrderingValueN``) to different
values. Each field that requires pagination (i.e.: each field that yields a list)
will have its own set of variables, hence the ``N`` post-fix.

Example:
The initial query

.. code-block:: none

  query {
    items(
      orderBy: timestamp,
      orderDirection: desc,
      first: 10000
    ) {
      foo
    }
  }

will be transformed to

.. code-block:: none

  query($first0: Int, $skip0: Int, $lastOrderingValue0: BigInt) {
    items(
      orderBy: timestamp,
      orderDirection: desc,
      first: $first0,
      skip: $skip0,
      where: {
        timestamp_lt: $lastOrderingValue0
      }
    ) {
      foo
    }
  }

As part of this step, a tree of PaginationNode objects is also created, which
mirrors the selection tree of the initial query but only includes fields that
require pagination.

See :class:`PaginationNode`, :func:`preprocess_selection` and
:func:`preprocess_document`.

2. Using the PaginationNode tree, a "cursor" (ish) tree is initialized which
provides a cursor that is used to iterate through the set of pagination arguments
values (i.e.: ``firstN``, ``skipN``, ``lastOrderingValueN``). This
"cursor" maintains a pagination state for each of the pagination nodes in the
pagination node tree that keeps track (amongst other things) of the number of
entities queried for each list fields. The cursor is moved forward based on
the response from the query executed with the variable values of the cursor's
previous state.

By looping through the "cursor" states until enough entities are queried, we can
get a sequence of response data which, when merged, are equivalen to the initial
request.

See :class:`Cursor`, :func:`trim_document` and :func:`paginate`.
"""

from __future__ import annotations

from typing import Any, Callable, Iterator, Protocol, Tuple

from subgrounds.pagination.preprocess import PaginationNode, generate_pagination_nodes, normalize, prune_doc
from subgrounds.pagination.utils import merge
from subgrounds.pagination.strategies import Cursor, legacy_strategy, greedy_strategy

from subgrounds.query import Document
import subgrounds.client as client
from subgrounds.schema import SchemaMeta
from subgrounds.utils import extract_data




class PaginationError(RuntimeError):
  def __init__(self, message: Any, cursor: Cursor):
    super().__init__(message)
    self.cursor = cursor


class PaginationStrategy(Protocol):
  def __init__(self, page_node: PaginationNode) -> None: ...

  @property
  def is_leaf(self) -> bool: ...

  def update(self, data: dict) -> None: ...

  def step(self, data: dict) -> None: ...

  def args(self) -> dict: ...


def paginate(
  schema: SchemaMeta,
  doc: Document,
  pagination_strategy: Callable[[Cursor, dict[str, Any]], Tuple[Cursor, dict[str, Any]]]
) -> dict[str, Any]:
  """ Executes the request document `doc` based on the GraphQL schema `schema` and returns
  the response as a JSON dictionary.

  Args:
    schema (SchemaMeta): The GraphQL schema on which the request document is based
    doc (Document): The request document

  Returns:
    dict[str, Any]: The response data as a JSON dictionary
  """
  pagination_nodes = generate_pagination_nodes(schema, doc)

  if pagination_nodes == []:
    return client.query(doc.url, doc.graphql, variables=doc.variables)
  else:
    normalized_doc = normalize(schema, doc, pagination_nodes)
    data: dict[str, Any] = {}
    for page_node in pagination_nodes:
      cursor = Cursor.from_pagination_nodes(page_node)
      cursor, args = pagination_strategy(cursor)

      while True:
        try:
          trimmed_doc = prune_doc(normalized_doc, args)
          page_data = client.query(
            url=trimmed_doc.url,
            query_str=trimmed_doc.graphql,
            variables=trimmed_doc.variables | args
          )
          data = merge(data, page_data)
          cursor, args = pagination_strategy(cursor, page_data)
        except StopIteration:
          break

    return data


def paginate_iter(
  schema: SchemaMeta,
  doc: Document,
  pagination_strategy: Callable[[Cursor, dict[str, Any]], Tuple[Cursor, dict[str, Any]]]
) -> Iterator[dict[str, Any]]:
  """ Executes the request document `doc` based on the GraphQL schema `schema` and returns
  the response as a JSON dictionary.

  Args:
    schema (SchemaMeta): The GraphQL schema on which the request document is based
    doc (Document): The request document

  Returns:
    dict[str, Any]: The response data as a JSON dictionary
  """
  pagination_nodes = generate_pagination_nodes(schema, doc)
  # new_doc, pagination_nodes = preprocess_document(schema, doc)

  if pagination_nodes == []:
    yield client.query(doc.url, doc.graphql, variables=doc.variables)
  else:
    normalized_doc = normalize(schema, doc, pagination_nodes)

    for page_node in pagination_nodes:
      cursor = Cursor.from_pagination_nodes(page_node)
      cursor, args = pagination_strategy(cursor)

      while True:
        try:
          trimmed_doc = prune_doc(normalized_doc, args)
          page_data = client.query(trimmed_doc.url, trimmed_doc.graphql, variables=trimmed_doc.variables | args)
          yield page_data
          cursor, args = pagination_strategy(cursor, page_data)
        except StopIteration:
          break
        except Exception as exn:
          raise PaginationError(exn.args[0], cursor)
