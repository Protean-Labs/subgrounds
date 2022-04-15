""" Toplevel Subgrounds module

This module implements the toplevel API that most developers will be using when
querying The Graph with Subgrounds.
"""

from dataclasses import dataclass, field
from functools import reduce
from typing import Any, Optional
from pipe import map, groupby, traverse, where
import os
import json
import pandas as pd
import logging
import warnings

from subgrounds.dataframe_utils import df_of_json
from subgrounds.query import DataRequest, Document, Query
from subgrounds.schema import mk_schema
from subgrounds.subgraph.fieldpath import FieldPath
from subgrounds.subgraph.subgraph import Subgraph
from subgrounds.transform import DEFAULT_GLOBAL_TRANSFORMS, DEFAULT_SUBGRAPH_TRANSFORMS, DocumentTransform, RequestTransform
import subgrounds.client as client
import subgrounds.pagination as pagination

logger = logging.getLogger('subgrounds')
warnings.simplefilter('default')


@dataclass
class Subgrounds:
  global_transforms: list[RequestTransform] = field(default_factory=lambda: DEFAULT_GLOBAL_TRANSFORMS)
  subgraphs: dict[str, Subgraph] = field(default_factory=dict)

  def load_subgraph(self, url: str, save_schema: bool = False) -> Subgraph:
    """Performs introspection on the provided GraphQL API `url` to get the
    schema, stores the schema if :attr:`save_schema` is `True` and returns a
    generated class representing the subgraph with all its entities.

    Args:
      url (str): The url of the API
      save_schema (bool, optional): Flag indicating whether or not the schema
        should be saved to disk. Defaults to False.

    Returns:
      Subgraph: A generated class representing the subgraph and its entities
    """
    filename = url.split("/")[-1] + ".json"
    if os.path.isfile(filename):
      with open(filename) as f:
        schema = json.load(f)
    else:
      schema = client.get_schema(url)
      if save_schema:
        with open(filename, mode="w") as f:
          json.dump(schema, f)

    sg = Subgraph(url, mk_schema(schema), DEFAULT_SUBGRAPH_TRANSFORMS)
    self.subgraphs[url] = sg
    return sg

  def load_api(self, url: str, save_schema: bool = False) -> Subgraph:
    """Performs introspection on the provided GraphQL API `url` to get the
    schema, stores the schema if :attr:`save_schema` is `True` and returns a
    generated class representing the GraphQL endpoint with all its entities.

    Args:
      url (str): The url of the API
      save_schema (bool, optional): Flag indicating whether or not the schema
        should be saved to disk. Defaults to False.

    Returns:
      Subgraph: A generated class representing the subgraph and its entities
    """
    filename = url.split("/")[-1] + ".json"
    if os.path.isfile(filename):
      with open(filename) as f:
        schema = json.load(f)
    else:
      schema = client.get_schema(url)
      if save_schema:
        with open(filename, mode="w") as f:
          json.dump(schema, f)

    sg = Subgraph(url, mk_schema(schema), DEFAULT_SUBGRAPH_TRANSFORMS, False)
    self.subgraphs[url] = sg
    return sg

  def mk_request(self, fpaths: list[FieldPath]) -> DataRequest:
    """Creates a :class:`DataRequest` object by combining multiple
    :class:`FieldPath` objects.

    Args:
      fpaths (list[FieldPath]): The :class:`FieldPath` objects that should be
        included in the request

    Returns:
      DataRequest: A new :class:`DataRequest` object
    """
    fpaths = list(fpaths | map(FieldPath._auto_select) | traverse)

    return DataRequest(documents=list(
      fpaths
      | groupby(lambda fpath: fpath._subgraph._url)
      | map(lambda group: Document(
        url=group[0],
        query=reduce(Query.add, group[1] | map(FieldPath._selection), Query())
      ))
    ))

  def execute(self, req: DataRequest, auto_paginate: bool = True) -> list[dict]:
    """ Executes a :class:`DataRequest` object, sending the underlying
    query(ies) to the server and returning a data blob (list of Python
    dictionaries, one per actual query).

    Args:
      req (DataRequest): The :class:`DataRequest` object to be executed
      auto_paginate (bool, optional): Flag indicating whether or not Subgrounds
        should automatically paginate the query. Useful for querying non-subgraph
        APIs since automatic pagination is only supported for subgraph APIs.
        Defaults to True.

    Returns:
      list[dict]: The reponse data
    """
    def execute_document(doc: Document) -> dict:
      subgraph: Subgraph = next(
        self.subgraphs.values()
        | where(lambda sg: sg._url == doc.url)
      )
      if auto_paginate and subgraph._is_subgraph:
        return pagination.paginate(subgraph._schema, doc)
      else:
        return client.query(doc.url, doc.graphql, variables=doc.variables)

    def transform_doc(transforms: list[DocumentTransform], doc: Document) -> dict:
      logger.debug(f'execute.transform_doc: doc = \n{doc.graphql}')
      match transforms:
        case []:
          return execute_document(doc)
        case [transform, *rest]:
          new_doc = transform.transform_document(doc)
          data = transform_doc(rest, new_doc)
          return transform.transform_response(doc, data)

      assert False  # Suppress mypy missing return statement warning

    def transform_req(transforms: list[RequestTransform], req: DataRequest) -> list[dict]:
      match transforms:
        case []:
          return list(req.documents | map(lambda doc: transform_doc(self.subgraphs[doc.url]._transforms, doc)))
        case [transform, *rest]:
          new_req = transform.transform_request(req)
          data = transform_req(rest, new_req)
          return transform.transform_response(req, data)

      assert False  # Suppress mypy missing return statement warning

    return transform_req(self.global_transforms, req)

  def query_json(
    self,
    fpaths: list[FieldPath],
    auto_paginate: bool = True
  ) -> list[dict[str, Any]]:
    """Combines `Subgrounds.mk_request` and `Subgrounds.execute` into one function.

    Args:
      fpaths (list[FieldPath]): The `FieldPath` objects that should be included in the request
      auto_paginate (bool, optional): Flag indicating whether or not Subgrounds
        should automatically paginate the query. Useful for querying non-subgraph
        APIs since automatic pagination is only supported for subgraph APIs.
        Defaults to True.

    Returns:
      list[dict[str, Any]]: The reponse data
    """
    fpaths = list(fpaths | map(FieldPath._auto_select) | traverse)
    req = self.mk_request(fpaths)
    return self.execute(req, auto_paginate=auto_paginate)

  def query_df(
    self,
    fpaths: list[FieldPath],
    columns: Optional[list[str]] = None,
    concat: bool = False,
    auto_paginate: bool = True
  ) -> pd.DataFrame | list[pd.DataFrame]:
    """Same as :func:`Subgrounds.query` but formats the response data into a
    Pandas DataFrame. If the response data cannot be flattened to a single query
    (e.g.: when querying multiple list fields that return different entities),
    then multiple dataframes are returned

    :attr:`fpaths` is a list of :class:`FieldPath` objects that indicate which
    data must be queried.

    :attr:`columns` is an optional argument used to rename the dataframes(s)
    columns. The length of :attr:`columns` must be the same as the number of columns
    of *all* returned dataframes.

    :attr:`concat` indicates whether or not the resulting dataframes should be
    concatenated together. The dataframes must have the same number of columns,
    as well as the same column names and types (the names can be set using the
    :attr:`columns` argument).

    Args:
      fpaths (list[FieldPath]): The `FieldPath` objects that should be included
        in the request
      columns (Optional[list[str]], optional): The column labels. Defaults to None.
      merge (bool, optional): Whether or not to merge resulting dataframes.
      auto_paginate (bool, optional): Flag indicating whether or not Subgrounds
        should automatically paginate the query. Useful for querying non-subgraph
        APIs since automatic pagination is only supported for subgraph APIs.
        Defaults to True.

    Returns:
      pd.DataFrame | list[pd.DataFrame]: A DataFrame containing the reponse data

    Example:

    >>> from subgrounds.subgrounds import Subgrounds
    >>> sg = Subgrounds()
    >>> univ3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')
    >>> univ3.Swap.price = abs(univ3.Swap.amount0) / abs(univ3.Swap.amount1)
    >>> eth_usdc = univ3.Query.swaps(
    ...   orderBy=univ3.Swap.timestamp,
    ...   orderDirection='desc',
    ...   first=10,
    ...   where=[
    ...     univ3.Swap.pool == '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
    ...   ]
    ... )
    >>> sg.query_df([
    ...   eth_usdc.timestamp,
    ...   eth_usdc.price
    ... ])
      swaps_timestamp  swaps_price
    0       1643213811  2618.886394
    1       1643213792  2618.814281
    2       1643213792  2617.500494
    3       1643213763  2615.458495
    4       1643213763  2615.876574
    5       1643213739  2615.352390
    6       1643213678  2615.205713
    7       1643213370  2614.115746
    8       1643213210  2613.077301
    9       1643213196  2610.686563
    """
    fpaths = list(fpaths | map(FieldPath._auto_select) | traverse)
    json_data = self.query_json(fpaths, auto_paginate=auto_paginate)
    return df_of_json(json_data, fpaths, columns, concat)

  def query(
    self,
    fpath: FieldPath | list[FieldPath],
    unwrap: bool = True,
    auto_paginate: bool = True
  ) -> str | int | float | bool | list | tuple | None:
    """Executes one or multiple `FieldPath` objects immediately and return the data (as a tuple if multiple `FieldPath` objects are provided).

    Args:
      fpath (FieldPath): The `FieldPath` object(s) to query.
      unwrap (bool, optional): Flag indicating whether or not, in the case where
        the returned data is a list of one element, the element itself should be
        returned instead of the list. Defaults to True.
      auto_paginate (bool, optional): Flag indicating whether or not Subgrounds
        should automatically paginate the query. Useful for querying non-subgraph
        APIs since automatic pagination is only supported for subgraph APIs.
        Defaults to True.
    Returns:
      [type]: The `FieldPath` object(s) data

    Example:

    >>> from subgrounds.subgrounds import Subgrounds
    >>> sg = Subgrounds()
    >>> univ3 = sg.load_subgraph('https://api.thegraph.com/subgraphs/name/uniswap/uniswap-v3')
    >>> univ3.Swap.price = abs(univ3.Swap.amount0) / abs(univ3.Swap.amount1)
    >>> eth_usdc_last = univ3.Query.swaps(
    ...   orderBy=univ3.Swap.timestamp,
    ...   orderDirection='desc',
    ...   first=1,
    ...   where=[
    ...     univ3.Swap.pool == '0x8ad599c3a0ff1de082011efddc58f1908eb6e6d8'
    ...   ]
    ... ).price
    >>> sg.query(eth_usdc_last)
    2628.975030015892

    """
    fpaths = list(fpath | map(FieldPath._auto_select) | traverse)
    blob = self.query_json(fpaths, auto_paginate=auto_paginate)

    def f(fpath: FieldPath) -> dict[str, Any]:
      data = fpath._extract_data(blob)
      if type(data) == list and len(data) == 1 and unwrap:
        return data[0]
      else:
        return data

    data = tuple(fpaths | map(f))

    if len(data) == 1:
      return data[0]
    else:
      return data

  # def query_timeseries(
  #   self,
  #   x: FieldPath,
  #   y: FieldPath | list[FieldPath],
  #   interval: str,
  #   cumulative: bool
  # ):
  #   # fpaths = list([x, y] | traverse)
  #   # df = self.query_df(fpaths)[0]

  #   # match interval:
  #   #   case 'hour':
  #   #     tmin

  #   raise NotImplementedError
