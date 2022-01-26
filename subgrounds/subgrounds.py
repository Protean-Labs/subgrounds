from dataclasses import dataclass, field
from functools import reduce
from typing import Any, Optional
from pipe import map, groupby, traverse, where
import os
import json
import pandas as pd

from subgrounds.query import DataRequest, Document, Query
from subgrounds.schema import mk_schema
from subgrounds.subgraph import FieldPath, Subgraph
from subgrounds.transform import DEFAULT_GLOBAL_TRANSFORMS, DEFAULT_SUBGRAPH_TRANSFORMS, DocumentTransform, RequestTransform
import subgrounds.client as client


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
      match doc.variables:
        case []:
          return client.query(doc.url, doc.graphql_string)
        case [args]:
          return client.query(doc.url, doc.graphql_string, args)
        case args_list:
          return client.repeat(doc.url, doc.graphql_string, args_list)
    
    def transform_doc(transforms: list[DocumentTransform], doc: Document) -> dict:
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

  def query(self, fpaths: list[FieldPath]) -> list[dict]:
    """Combines `Subgrounds.mk_request` and `Subgrounds.execute` into one function.

    Args:
        fpaths (list[FieldPath]): The `FieldPath` objects that should be included in the request

    Returns:
        list[dict]: The reponse data
    """
    req = self.mk_request(fpaths)
    return self.execute(req)

  def query_df(self, fpaths: list[FieldPath], columns: Optional[list[str]] = None) -> pd.DataFrame:
    """Same as `Subgrounds.query` but formats the response as a DataFrame.

    Args:
        fpaths (list[FieldPath]): The `FieldPath` objects that should be included in the request
        columns (Optional[list[str]], optional): The column labels. Defaults to None.

    Returns:
        pd.DataFrame: A DataFrame containing the reponse data
    """
    if columns is None:
      columns = list(fpaths | map(lambda fpath: fpath.longname))
    
    data = self.query(fpaths)
    col_fpaths = zip(columns, fpaths)
    
    return pd.DataFrame({key: fpath.extract_data(data) for key, fpath in col_fpaths})

  def oneshot(self, fpath: FieldPath) -> str | int | float | bool | list | None:
    """Executes a single `FieldPath` query.

    Args:
      fpath (FieldPath): The `FieldPath` object to query.

    Returns:
        [type]: The single `FieldPath` data
    """
    blob = self.query([fpath])
    data = fpath.extract_data(blob)
    if type(data) == list and len(data) == 1:
      return data[0]
    else:
      return data


def to_dataframe(data: list[dict]) -> pd.DataFrame | list[pd.DataFrame]:
  """ Formats the dictionary `data` into a pandas DataFrame using some
  heuristics when no clear "flattening" scheme is present.
  """

  def dict_contains_list(data: dict) -> bool:
    for _, value in data.items():
      match value:
        case list():
          return True
        case dict():
          return dict_contains_list(value)
        case _:
          pass
    
    return False
  
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
