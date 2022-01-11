from functools import reduce
from pipe import *

from subgrounds.query import DataRequest, Document, Query
from subgrounds.subgraph import FieldPath
from subgrounds.transform import Transform
import subgrounds.client as client


def execute_request(request: DataRequest) -> list:
  def f(doc: Document) -> dict:
    match doc.variables:
      case []:
        return client.query(doc.url, doc.graphql_string)
      case [args]:
        return client.query(doc.url, doc.graphql_string, args)
      case args_list:
        return client.repeat(doc.url, doc.graphql_string, args_list)

  return list(request.documents | map(f))


def execute(req: DataRequest) -> dict:
  def f(transforms, req):
    match transforms:
      case []:
        return execute_request(req)
      case [transform, *rest]:
        new_req = transform.transform_request(req)
        data = execute(rest, new_req)
        return transform.transform_response(req, data)


def mk_request(fpaths: list[FieldPath]) -> DataRequest:
  return DataRequest(
    list(
      fpaths
      | groupby(lambda fpath: fpath.subgraph.url)
      | map(lambda group: Document(
        url=group[0],
        query=reduce(Query.add_selection, group[1] | map(FieldPath.selection), Query())
      ))
    )
  )
