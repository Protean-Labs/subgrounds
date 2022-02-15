from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Callable, Tuple, TYPE_CHECKING
from functools import partial
from pipe import map, traverse

import logging
logger = logging.getLogger('subgrounds')

from subgrounds.query import Argument, DataRequest, Document, InputValue, Query, Selection, VariableDefinition, pagination_args
from subgrounds.schema import TypeMeta, TypeRef

if TYPE_CHECKING:
  from subgrounds.subgraph import FieldPath, Subgraph

from subgrounds.utils import flatten, union


def transform_request(fmeta: TypeMeta.FieldMeta, replacement: list[Selection], req: DataRequest) -> DataRequest:
  def transform(select: Selection) -> Selection:
    match select:
      case Selection(TypeMeta.FieldMeta(name), _, _, [] | None) if name == fmeta.name:
        return replacement
      case Selection(_, _, _, [] | None):
        return [select]
      case Selection(TypeMeta.FieldMeta(name) as select_fmeta, alias, args, inner_select):
        new_inner_select = list(inner_select | map(transform) | traverse)
        return Selection(select_fmeta, alias, args, new_inner_select)
      case _:
        raise Exception(f"transform_request: unhandled selection {select}")

  return DataRequest.transform(
    req,
    lambda doc: Document.transform(doc, query_f=lambda query: Query.transform(query, selection_f=transform))
  )


def select_data(select: Selection, data: dict) -> list[Any]:
  match (select, data):
    case (Selection(TypeMeta.FieldMeta(name), None, _, [] | None) | Selection(TypeMeta.FieldMeta(), name, _, [] | None), dict() as data) if name in data:
      return [data[name]]

    case (Selection(TypeMeta.FieldMeta(name), None, _, inner_select) | Selection(TypeMeta.FieldMeta(), name, _, inner_select), dict() as data) if name in data:
      return list(inner_select | map(partial(select_data, data=data[name])) | traverse)

    case (select, data):
      raise Exception(f"select_data: invalid selection {select} for data {data}")


class RequestTransform(ABC):
  @abstractmethod
  def transform_request(self, req: DataRequest) -> DataRequest:
    return req

  @abstractmethod
  def transform_response(self, req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return data


class DocumentTransform(ABC):
  @abstractmethod
  def transform_document(self, req: Document) -> Document:
    return req

  @abstractmethod
  def transform_response(self, req: Document, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return data


class TypeTransform(DocumentTransform):
  def __init__(self, type_, f) -> None:
    self.type_ = type_
    self.f = f
    super().__init__()

  def transform_document(self, query: Query) -> Query:
    return query

  def transform_response(self, doc: Document, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def transform(select: Selection, data: dict) -> None:
      # TODO: Handle NonNull and List more graciously (i.e.: without using TypeRef.root_type_name)
      match (select, data):
        case (Selection(TypeMeta.FieldMeta(name, _, _, ftype), None, _, [] | None) | Selection(TypeMeta.FieldMeta(_, _, _, ftype), name, _, [] | None), dict() as data) if TypeRef.root_type_name(self.type_) == TypeRef.root_type_name(ftype):
          data[name] = self.f(data[name])
        case (Selection(_, _, _, [] | None), dict()):
          pass
        case (Selection(TypeMeta.FieldMeta(name), None, _, inner_select) | Selection(TypeMeta.FieldMeta(), name, _, inner_select), dict() as data):
          match data[name]:
            case list() as elts:
              for elt in elts:
                for select in inner_select:
                  transform(select, elt)
            case dict() as elt:
              for select in inner_select:
                transform(select, elt)
            case None:
              return None
            case _:
              raise Exception(f"transform_data_type: data for selection {select} is neither list or dict {data[name]}")

        case (select, data):
          raise Exception(f"transform_data_type: invalid selection {select} for data {data}")

    for select in doc.query.selection:
      transform(select, data)

    return data


class LocalSyntheticField(DocumentTransform):
  def __init__(
    self,
    subgraph: Subgraph,
    fmeta: TypeMeta.FieldMeta,
    type_: TypeMeta.ObjectMeta | TypeMeta.InterfaceMeta,
    fpath_selection: Selection,
    f: Callable,
    default: Any,
    args: list[Selection]
  ) -> None:
    self.subgraph = subgraph
    self.fmeta = fmeta
    self.type_ = type_
    self.fpath_selection = fpath_selection
    self.f = f
    self.default = default
    self.args = args

  def transform_document(self, doc: Document) -> Document:
    logger.debug(f'LocalSyntheticField.transform_document: fmeta = {self.fmeta}, object = {self.type_}, fpath_selection = {self.fpath_selection}')
    def transform(select: Selection) -> Selection | list[Selection]:
      logger.debug(f'LocalSyntheticField.transform_document.transform: select = {select}, args = {self.args}')
      match select:
        # case Selection(TypeMeta.FieldMeta(name) as fmeta, _, _, [] | None) if name == self.fmeta.name and fmeta.type_.name == self.type_.name:
        case Selection(TypeMeta.FieldMeta(name), _, _, [] | None) if name == self.fmeta.name:
          return Selection.consolidate(self.args)
        case Selection(_, _, _, [] | None):
          return [select]
        case Selection(TypeMeta.FieldMeta(name) as select_fmeta, alias, args, inner_select):
          new_inner_select = list(inner_select | map(transform) | traverse)
          return Selection(select_fmeta, alias, args, new_inner_select)
        case _:
          raise Exception(f"transform_document: unhandled selection {select}")

    def transform_on_type(select: Selection) -> Selection:
      match select:
        case Selection(TypeMeta.FieldMeta(_, _, _, type_) as select_fmeta, alias, args, inner_select) if type_.name == self.type_.name:
          new_inner_select = Selection.consolidate(list(inner_select | map(transform) | traverse))
          return Selection(select_fmeta, alias, args, new_inner_select)

        case Selection(fmeta, alias, args, inner_select):
          return Selection(fmeta, alias, args, list(inner_select | map(transform_on_type)))

    if self.subgraph.url == doc.url:
      return Document.transform(doc, query_f=lambda query: Query.transform(query, selection_f=transform_on_type))
    else:
      return doc

  def transform_response(self, doc: Document, data: dict[str, Any]) -> list[dict[str, Any]]:
    logger.debug(f'LocalSyntheticField.transform_response: fmeta = {self.fmeta}, object = {self.type_}, fpath_selection = {self.fpath_selection}')
    def transform(select: Selection, data: dict) -> None:
      logger.debug(f'LocalSyntheticField.transform_response.transform: select = {select}, data = {data}')
      match (select, data):
        case (Selection(TypeMeta.FieldMeta(name), None, _, [] | None) | Selection(TypeMeta.FieldMeta(), name, _, [] | None), dict() as data) if name == self.fmeta.name and name not in data:
          arg_values = flatten(list(self.args | map(partial(select_data, data=data))))
          
          try:
            data[name] = self.f(*arg_values)
          except ZeroDivisionError:
            data[name] = self.default

        case (Selection(TypeMeta.FieldMeta(name), None, _, [] | None) | Selection(TypeMeta.FieldMeta(), name, _, [] | None), dict() as data):
          pass
        case (Selection(TypeMeta.FieldMeta(name), None, _, inner_select) | Selection(TypeMeta.FieldMeta(), name, _, inner_select), dict() as data) if name in data:
          match data[name]:
            case list() as elts:
              for elt in elts:
                for select in inner_select:
                  transform(select, elt)
            case dict() as elt:
              for select in inner_select:
                transform(select, elt)
            case _:
              raise Exception(f"transform_response: data for selection {select} is neither list or dict {data[name]}")

        case (select, data):
          raise Exception(f"transform_response: invalid selection {select} for data {data}")

    def transform_on_type(select: Selection, data: dict) -> None:
      logger.debug(f'LocalSyntheticField.transform_response.transform_on_type: select = {select}, data = {data}')
      match select:
        case Selection(TypeMeta.FieldMeta(_, _, _, type_), None, _, _) | Selection(TypeMeta.FieldMeta(_, _, _, type_), _, _, _) if type_.name == self.type_.name:
          # for select in inner_select:
          #   transform(select, data[name])
          match data:
            case list():
              for d in data:
                transform(select, d)
            case dict():
              transform(select, data)

        case (Selection(TypeMeta.FieldMeta(name), None, _, inner_select) | Selection(_, name, _, inner_select)):
          match data:
            case list():
              for d in data:
                list(inner_select | map(partial(transform_on_type, data=d[name])))
            case dict():
              list(inner_select | map(partial(transform_on_type, data=data[name])))


    if self.subgraph.url == doc.url:
      for select in doc.query.selection:
        transform_on_type(select, data)

    return data

# TODO: Test split transform
class SplitTransform(RequestTransform):
  def __init__(self, query: Query) -> None:
    self.query = query

  def transform_request(self, req: DataRequest) -> DataRequest:
    def split(doc: Document) -> list[Document]:
      if Query.contains(doc.query, self.query):
        return [
          Document(doc.url, Query.remove(doc.query, self.query), doc.fragments),
          Document(doc.url, Query.select(doc.query, self.query), doc.fragments)
        ]
      else:
        return [doc]

    return DataRequest(
      documents=list(req.documents | map(split) | traverse)
    )

  # TODO: Fix transform_response
  def transform_response(self, req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def merge_data(data1: dict | list | Any, data2: dict | list | Any) -> dict:
      match (data1, data2):
        case (dict(), dict()):
          return dict(union(
            list(data1.items()),
            list(data2.items()),
            key=lambda item: item[0],
            combine=lambda item1, item2: (item1[0], merge_data(item1[1], item2[1]))
          ))
        
        case (list(), list()):
          return list(
            zip(data1, data2)
            | map(lambda tup: merge_data(tup[0], tup[1]))
          )

        case (value, _):
          return value


    def transform(docs: list[Document], data: list[dict[str, Any]], acc: list[dict[str, Any]]) -> list[dict[str, Any]]:
      match (docs, data):
        case ([doc, *docs_rest], [d1, d2, *data_rest]) if Query.contains(doc.query, self.query):
          return transform(docs_rest, data_rest, [*acc, merge_data(d1, d2)])
        case ([], []):
          return acc

    return transform(req.documents, data, [])


DEFAULT_GLOBAL_TRANSFORMS: list[RequestTransform] = []

DEFAULT_SUBGRAPH_TRANSFORMS: list[DocumentTransform] = [
  TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal) if bigdecimal is not None else None),
  TypeTransform(TypeRef.Named('BigInt'), lambda bigint: int(bigint) if bigint is not None else None),
]
