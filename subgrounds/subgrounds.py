from dataclasses import dataclass, field
from enum import Enum
from functools import partial, reduce
from typing import Any, Optional
from pipe import map, groupby, traverse, where, dedup
import os
import json
import math
from datetime import datetime

import warnings
from subgrounds.dataframe_utils import df_of_json, timeseries_of_df

from subgrounds.utils import flatten_dict
warnings.simplefilter('default')

import logging
logger = logging.getLogger('subgrounds')

import pandas as pd

from subgrounds.query import DataRequest, Document, Query
from subgrounds.schema import mk_schema
from subgrounds.subgraph import FieldPath, Subgraph
from subgrounds.transform import DEFAULT_GLOBAL_TRANSFORMS, DEFAULT_SUBGRAPH_TRANSFORMS, DocumentTransform, RequestTransform
import subgrounds.client as client
import subgrounds.pagination as pagination


class TimeseriesInterval(str, Enum):
  HOUR = 'H'
  DAY = 'D'
  WEEK = 'W-SUN'
  MONTH = 'MS'


class AggregateMethod(str, Enum):
  MEAN = 'mean'
  SUM = 'sum'
  FIRST = 'first'
  LAST = 'last'
  MEDIAN = 'median'
  MIN = 'min'
  MAX = 'max'
  COUNT = 'count'


class NaInterpolationMethod(str, Enum):
  FORDWARD_FILL = 'ffill'
  BACKWARD_FILL = 'bfill'


@dataclass
class Subgrounds:
  global_transforms: list[RequestTransform] = field(default_factory=lambda: DEFAULT_GLOBAL_TRANSFORMS)
  subgraphs: dict[str, Subgraph] = field(default_factory=dict)

  def load_subgraph(self, url: str, save_schema: bool = False) -> Subgraph:
    """Performs introspection on the provided GraphQL API `url` to get the schema, 
    stores the schema if `save_schema` is `True` and returns a generated class representing 
    the subgraph with all its entities.

    Args:
        url (str): The url of the API
        save_schema (bool, optional): Flag indicating whether or not the schema should be saved to disk. Defaults to False.

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

  def mk_request(self, fpaths: list[FieldPath]) -> DataRequest:
    """Creates a `DataRequest` object by combining multiple `FieldPath` objects

    Args:
        fpaths (list[FieldPath]): The `FieldPath` objects that should be included in the request

    Returns:
        DataRequest: A new `DataRequest` object
    """
    return DataRequest(documents=list(
      fpaths
      | groupby(lambda fpath: fpath.subgraph.url)
      | map(lambda group: Document(
        url=group[0],
        query=reduce(Query.add_selection, group[1] | map(FieldPath.selection), Query())
      ))
    ))

  def execute(self, req: DataRequest) -> list[dict]:
    """Executes a `DataRequest` object, sending the underlying query(ies) to the server and returning 
    a data blob (list of Python dictionaries, one per actual query).

    Args:
        req (DataRequest): The `DataRequest` object to be executed

    Returns:
        list[dict]: The reponse data
    """
    def execute_document(doc: Document) -> dict:
      return pagination.paginate(doc)
    
    def transform_doc(transforms: list[DocumentTransform], doc: Document) -> dict:
      logger.debug(f'execute.transform_doc: doc = \n{doc.graphql}')
      match transforms:
        case []:
          return execute_document(doc)
        case [transform, *rest]:
          new_doc = transform.transform_document(doc)
          data = transform_doc(rest, new_doc)
          return transform.transform_response(doc, data)

    def transform_req(transforms: list[RequestTransform], req: DataRequest) -> list[dict]:
      match transforms:
        case []:
          return list(req.documents | map(lambda doc: transform_doc(self.subgraphs[doc.url].transforms, doc)))
        case [transform, *rest]:
          new_req = transform.transform_request(req)
          data = transform_req(rest, new_req)
          return transform.transform_response(req, data)
    
    return transform_req(self.global_transforms, req)

  def query_json(self, fpaths: list[FieldPath]) -> list[dict]:
    """Combines `Subgrounds.mk_request` and `Subgrounds.execute` into one function.

    Args:
        fpaths (list[FieldPath]): The `FieldPath` objects that should be included in the request

    Returns:
        list[dict]: The reponse data
    """
    req = self.mk_request(fpaths)
    return self.execute(req)

  def query_df(
    self,
    fpaths: list[FieldPath],
    columns: Optional[list[str]] = None,
    merge: bool = False,
  ) -> pd.DataFrame | list[pd.DataFrame]:
    """Same as `Subgrounds.query` but formats the response as a DataFrame. If the request
    involves making multiple different GraphQL queries (e.g.: when querying data from different
    subgraphs), then the function returns multiple dataframe (one per query).
    
    If `merge` is `True`, then the function will attempt to merge the DataFrames. Merging
    requires that all DataFrames have the same number of columns of the same type. If the
    column names differ, an explicit list of columns can be provided with the `columns` argument.

    Args:
        fpaths (list[FieldPath]): The `FieldPath` objects that should be included in the request
        columns (Optional[list[str]], optional): The column labels. Defaults to None.
        merge (bool, optional): Whether or not to merge resulting dataframes.

    Returns:
        pd.DataFrame | list[pd.DataFrame]: A DataFrame containing the reponse data

    Example:
    ```python
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
    ```
    """
    json_data = self.query_json(fpaths)
    return df_of_json(json_data, fpaths, columns, merge)


  def query(self, fpath: FieldPath | list[FieldPath], unwrap: bool = True) -> str | int | float | bool | list | tuple | None:
    """Executes one or multiple `FieldPath` objects immediately and return the data (as a tuple if multiple `FieldPath` objects are provided).

    Args:
      fpath (FieldPath): The `FieldPath` object(s) to query.

    Returns:
        [type]: The `FieldPath` object(s) data

    Example:
    ```python
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
    ```
    """
    fpaths = list([fpath] | traverse)
    blob = self.query_json(fpaths)

    def f(fpath):
      data = fpath.extract_data(blob)
      if type(data) == list and len(data) == 1 and unwrap:
        return data[0]
      else:
        return data

    data = tuple(fpaths | map(f))

    if len(data) == 1:
      return data[0]
    else:
      return data

  def query_timeseries(
    self,
    x: FieldPath,
    y: FieldPath | list[FieldPath],
    interval: TimeseriesInterval,
    aggregation: AggregateMethod | list[AggregateMethod],
    na_fill: Optional[NaInterpolationMethod | Any | list[NaInterpolationMethod | Any]] = None,
  ):
    # List-ify arguments
    y = list([y] | traverse)

    # Check args length
    if type(aggregation) == list and len(aggregation) != len(y):
      raise Exception(f'query_timeseries: len(aggregation) != len(y), {len(aggregation)} != {len(y)}')

    if type(na_fill) == list and len(na_fill) != len(y):
      raise Exception(f'query_timeseries: len(na_fill) != len(y), {len(na_fill)} != {len(y)}')

    def _last_day_of_month(any_day):
      next_month = any_day.replace(day=28) + datetime.timedelta(days=4)
      return next_month - datetime.timedelta(days=next_month.day)

    fpaths = list([x, y] | traverse)
    dfs = self.query_df(fpaths)
    df = dfs[0] if type(dfs) == list else dfs

    return timeseries_of_df(
      df,
      x,
      y,
      interval,
      aggregation,
      na_fill
    )


def to_dataframe(data: list[dict]) -> pd.DataFrame | list[pd.DataFrame]:
  """ Formats the dictionary `data` into a pandas DataFrame using some
  heuristics when no clear "flattening" scheme is present.
  """
  warnings.warn("`to_dataframe` will be deprecated! Use `Subgrounds`'s `query_df` instead", DeprecationWarning)

  def columns(data, prefix: str = '') -> list[str]:
    match data:
      case dict():
        return list(
          list(data.keys())
          | map(lambda key: columns(data[key], f'{prefix}_{key}' if prefix != '' else key))
          | traverse
        )
      case list():
        return columns(data[0], prefix)
      case _:
        return prefix

  def rows(data, prefix: str = '', partial_row: dict = {}) -> list[dict[str, Any]]:
    def merge(data: dict, item: dict | list[dict]) -> dict | list[dict]:
      match item:
        case dict():
          return data | item
        case list():
          return list(item | map(lambda item: merge(data, item)))

    match data:
      case dict():
        row_items = list(
          list(data.keys())
          | map(lambda key: rows(data[key], f'{prefix}_{key}' if prefix != '' else key, partial_row))
        )

        return reduce(merge, row_items, partial_row)

      case list():
        return list(data | map(lambda row: rows(row, prefix, partial_row)))
      case value:
        return {prefix: value}

  def flatten(data: list[list[Any]]) -> list[Any]:
    match data[0]:
      case dict():
        return data
      case list():
        return reduce(lambda l1, l2: l1 + flatten(l2), data, [])

  if len(data) == 1:
    return pd.DataFrame(columns=columns(data), data=flatten(rows(data)))
  else:
    return list(data | map(lambda data: pd.DataFrame(columns=columns(data), data=flatten(rows(data)))))
