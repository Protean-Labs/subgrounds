from dataclasses import dataclass, field
from functools import reduce
from re import L
from typing import Any
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

  def load_subgraph(self, url: str) -> Subgraph:
    filename = url.split("/")[-1] + ".json"
    if os.path.isfile(filename):
      with open(filename) as f:
        schema = json.load(f)
    else:
      schema = client.get_schema(url)
      with open(filename, mode="w") as f:
        json.dump(schema, f)

    sg = Subgraph(url, mk_schema(schema), DEFAULT_SUBGRAPH_TRANSFORMS)
    self.subgraphs[url] = sg
    return sg

  def mk_request(self, fpaths: list[FieldPath]) -> DataRequest:
    return DataRequest(documents=list(
      fpaths
      | groupby(lambda fpath: fpath.subgraph.url)
      | map(lambda group: Document(
        url=group[0],
        query=reduce(Query.add_selection, group[1] | map(FieldPath.selection), Query())
      ))
    ))

  def execute(self, req: DataRequest) -> dict:
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

    def transform_req(transforms: list[RequestTransform], req: DataRequest) -> dict:
      match transforms:
        case []:
          return list(req.documents | map(lambda doc: transform_doc(self.subgraphs[doc.url].transforms, doc)))
        case [transform, *rest]:
          new_req = transform.transform_request(req)
          data = transform_req(rest, new_req)
          return transform.transform_response(req, data)
    
    return transform_req(self.global_transforms, req)


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
