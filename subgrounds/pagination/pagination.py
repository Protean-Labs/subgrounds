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

from typing import Any, Callable, Iterator, Protocol, Tuple, Type, Optional

from subgrounds.pagination.preprocess import PaginationNode, generate_pagination_nodes, normalize, prune_doc
from subgrounds.pagination.strategies import SkipPagination, StopPagination
from subgrounds.pagination.utils import merge
# from subgrounds.pagination.strategies import Cursor

from subgrounds.query import Document
import subgrounds.client as client
from subgrounds.schema import SchemaMeta
from subgrounds.utils import extract_data


class PaginationError(RuntimeError):
  def __init__(self, message: Any, strategy: PaginationStrategy):
    super().__init__(message)
    self.strategy = strategy


class PaginationStrategy(Protocol):
  def __init__(self, schema: SchemaMeta, document: Document) -> None:
    """ Initializes the pagination strategy. If there is no need for pagination given
    the provided :class:`Document` ``document``, then the constructor should raise a 
    :class:`SkipPagination` exception.

    Args:
        schema (SchemaMeta): The schema of the API against which ``document`` will be executed
        document (Document): The query document
    """
    ...

  def step(self, page_data: Optional[dict[str, Any]] = None) -> Tuple[Document, dict[str, Any]]:
    """ Returns the new query document and its variables which will be executed to get the next 
    page of data. If this is the first query made as part of the pagination strategy, then
    ``page_data`` will be ``None``.

    If pagination should be interupted (e.g.: if enough entities have been queried), then this method
    should raise a :class:`StopPagination` exception.

    Args:
        page_data (Optional[dict[str, Any]], optional): The previous query's response data.
        If this is the first query (i.e.: the first page of data), then it will be None.
        Defaults to None.

    Returns:
        Tuple[Document, dict[str, Any]]: A tuple containing the query document
        to fetch the next page of data and the variables for that document.
    """
    ...


def paginate(
  schema: SchemaMeta,
  doc: Document,
  pagination_strategy: Type[PaginationStrategy]
) -> dict[str, Any]:
  """ Executes the request document `doc` based on the GraphQL schema `schema` and returns
  the response as a JSON dictionary.

  Args:
    schema (SchemaMeta): The GraphQL schema on which the request document is based
    doc (Document): The request document

  Returns:
    dict[str, Any]: The response data as a JSON dictionary
  """

  try:
    strategy = pagination_strategy(schema, doc)

    data: dict[str, Any] = {}
    doc, args = strategy.step()

    while True:
      try:
        page_data = client.query(
          url=doc.url,
          query_str=doc.graphql,
          variables=doc.variables | args
        )
        data = merge(data, page_data)
        doc, args = strategy.step(page_data)
      except StopPagination:
        break

    return data

  except SkipPagination:
    return client.query(doc.url, doc.graphql, variables=doc.variables)


def paginate_iter(
  schema: SchemaMeta,
  doc: Document,
  pagination_strategy: Type[PaginationStrategy]
) -> Iterator[dict[str, Any]]:
  """ Executes the request document `doc` based on the GraphQL schema `schema` and returns
  the response as a JSON dictionary.

  Args:
    schema (SchemaMeta): The GraphQL schema on which the request document is based
    doc (Document): The request document

  Returns:
    dict[str, Any]: The response data as a JSON dictionary
  """

  try:
    strategy = pagination_strategy(schema, doc)

    doc, args = strategy.step()

    while True:
      try:
        page_data = client.query(
          url=doc.url,
          query_str=doc.graphql,
          variables=doc.variables | args
        )
        yield page_data
        doc, args = strategy.step(page_data)
      except StopPagination:
        break
      except Exception as exn:
        raise PaginationError(exn.args[0], strategy)

  except SkipPagination:
    return client.query(doc.url, doc.graphql, variables=doc.variables)