from abc import ABC, abstractmethod
from typing import Any, Callable
from functools import partial

from subgrounds.query import DataRequest, Document, Query, Selection
from subgrounds.schema import TypeMeta, TypeRef
from subgrounds.utils import flatten
import subgrounds.client as client


class Transform(ABC):
  @abstractmethod
  def transform_request(self, req: DataRequest) -> DataRequest:
    return req

  @abstractmethod
  def transform_response(self, req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return data


def transform_request(fmeta: TypeMeta.FieldMeta, replacement: list[Selection], req: DataRequest) -> DataRequest:
  def transform(select: Selection) -> Selection:
    match select:
      case Selection(TypeMeta.FieldMeta(name), _, _, [] | None) if name == fmeta.name:
        return replacement
      case Selection(_, _, _, [] | None):
        return [select]
      case Selection(TypeMeta.FieldMeta(name) as select_fmeta, alias, args, inner_select):
        new_inner_select = flatten(list(map(transform, inner_select)))
        return Selection(select_fmeta, alias, args, new_inner_select)
      case _:
        raise Exception(f"transform_request: unhandled selection {select}")

  return DataRequest.transform(
    req,
    lambda doc: Document.transform(doc, query_f=lambda query: Query.transform(query, selection_f=transform))
  )

  # return Query(selection=list(map(transform, query.selection)))


def select_data(select: Selection, data: dict) -> list[Any]:
  match (select, data):
    case (Selection(TypeMeta.FieldMeta(name), _, _, [] | None), dict() as data) if name in data:
      return [data[name]]
    case (Selection(TypeMeta.FieldMeta(name), _, _, inner_select), dict() as data) if name in data:
      return flatten(list(map(partial(select_data, data=data[name]), inner_select)))
    case (select, data):
      raise Exception(f"select_data: invalid selection {select} for data {data}")


def transform_response(fmeta: TypeMeta.FieldMeta, func: Callable, args: list[Selection], req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
  def transform(select: Selection, data: dict) -> None:
    match (select, data):
      case (Selection(TypeMeta.FieldMeta(name), _, _, [] | None), dict() as data) if name == fmeta.name and name not in data:
        arg_values = flatten(list(map(partial(select_data, data=data), args)))
        data[name] = func(*arg_values)
      case (Selection(TypeMeta.FieldMeta(name), _, _, [] | None), dict() as data):
        pass
      case (Selection(TypeMeta.FieldMeta(name), _, _, inner_select), dict() as data) if name in data:
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

  for (doc, data_) in zip(req.documents, data):
    for select in doc.query.selection:
      transform(select, data_)

  return data


def transform_data_type(type_: TypeRef.T, f: Callable, req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
  def transform(select: Selection, data: dict) -> None:
    # TODO: Handle NonNull and List more graciously (i.e.: without using TypeRef.root_type_name)
    match (select, data):
      case (Selection(TypeMeta.FieldMeta(name, _, _, ftype), _, _, [] | None), dict() as data) if TypeRef.root_type_name(type_) == TypeRef.root_type_name(ftype):
        data[name] = f(data[name])
      case (Selection(_, _, _, [] | None), dict()):
        pass
      case (Selection(TypeMeta.FieldMeta(name), _, _, inner_select), dict() as data):
        match data[name]:
          case list() as elts:
            for elt in elts:
              for select in inner_select:
                transform(select, elt)
          case dict() as elt:
            for select in inner_select:
              transform(select, elt)
          case _:
            raise Exception(f"transform_data_type: data for selection {select} is neither list or dict {data[name]}")

      case (select, data):
        raise Exception(f"transform_data_type: invalid selection {select} for data {data}")

  for (doc, data_) in zip(req.documents, data):
    for select in doc.query.selection:
      transform(select, data_)

  return data


def chain_transforms(transforms: list[Transform], req: DataRequest) -> dict:
  match transforms:
    case []:
      return list(map(lambda doc: client.query(doc.url, doc.graphql_string), req.documents))
    case [transform, *rest]:
      new_req = transform.transform_request(req)
      data = chain_transforms(rest, new_req)
      return transform.transform_response(req, data)


class TypeTransform(Transform):
  def __init__(self, type_, f) -> None:
    self.type_ = type_
    self.f = f
    super().__init__()

  def transform_request(self, query: Query) -> Query:
    return query

  def transform_response(self, req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def transform(select: Selection, data: dict) -> None:
      # TODO: Handle NonNull and List more graciously (i.e.: without using TypeRef.root_type_name)
      match (select, data):
        case (Selection(TypeMeta.FieldMeta(name, _, _, ftype), _, _, [] | None), dict() as data) if TypeRef.root_type_name(self.type_) == TypeRef.root_type_name(ftype):
          data[name] = self.f(data[name])
        case (Selection(_, _, _, [] | None), dict()):
          pass
        case (Selection(TypeMeta.FieldMeta(name), _, _, inner_select), dict() as data):
          match data[name]:
            case list() as elts:
              for elt in elts:
                for select in inner_select:
                  transform(select, elt)
            case dict() as elt:
              for select in inner_select:
                transform(select, elt)
            case _:
              raise Exception(f"transform_data_type: data for selection {select} is neither list or dict {data[name]}")

        case (select, data):
          raise Exception(f"transform_data_type: invalid selection {select} for data {data}")

    for (doc, data_) in zip(req.documents, data):
      for select in doc.query.selection:
        transform(select, data_)

    return data


class LocalSyntheticField(Transform):
  def __init__(self, subgraph, fmeta: TypeMeta.FieldMeta, f: Callable, args: list[Selection]) -> None:
    self.subgraph = subgraph
    self.fmeta = fmeta
    self.f = f
    self.args = args

  def transform_request(self, req: DataRequest) -> DataRequest:
    def transform(select: Selection) -> Selection:
      match select:
        case Selection(TypeMeta.FieldMeta(name), _, _, [] | None) if name == self.fmeta.name:
          return self.args
        case Selection(_, _, _, [] | None):
          return [select]
        case Selection(TypeMeta.FieldMeta(name) as select_fmeta, alias, args, inner_select):
          new_inner_select = flatten(list(map(transform, inner_select)))
          return Selection(select_fmeta, alias, args, new_inner_select)
        case _:
          raise Exception(f"transform_request: unhandled selection {select}")

    return DataRequest.transform(
      req,
      lambda doc: Document.transform(doc, query_f=lambda query: Query.transform(query, selection_f=transform))
    )

  def transform_response(self, req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def transform(select: Selection, data: dict) -> None:
      match (select, data):
        case (Selection(TypeMeta.FieldMeta(name), _, _, [] | None), dict() as data) if name == self.fmeta.name and name not in data:
          arg_values = flatten(list(map(partial(select_data, data=data), self.args)))
          data[name] = self.f(*arg_values)
        case (Selection(TypeMeta.FieldMeta(name), _, _, [] | None), dict() as data):
          pass
        case (Selection(TypeMeta.FieldMeta(name), _, _, inner_select), dict() as data) if name in data:
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

    for (doc, data_) in zip(req.documents, data):
      for select in doc.query.selection:
        transform(select, data_)

    return data


# TODO: Test split transform
class SplitTransform(Transform):
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
      documents=flatten(map(split, req.documents))
    )

  # TODO: Fix transform_response
  def transform_response(self, req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    def transform(docs: list[Document], data: list[dict[str, Any]], acc: list[dict[str, Any]]) -> list[dict[str, Any]]:
      match (docs, data):
        case ([doc, *docs_rest], [d1, d2, *data_rest]) if Query.contains(doc.query, self.query):
          return transform(docs_rest, data_rest, [*acc, client.merge_data(d1, d2)])
        case ([], []):
          return acc

    return transform(req.documents, data, [])


# class PaginationTransform(Transform):
#   def transform_request(self, req: DataRequest) -> DataRequest:
#     config = {'counter': 0}

#     def replace_first_with_var(selection: Selection, values: dict[str, int]) -> None:
#       try:
#         arg = next(filter(lambda arg: arg.name == 'first', selection.arguments))
#         values[f'first{config["counter"]}'] = arg.value
#         arg.value = InputValue.Variable(name=f'first{config["counter"]}')
#         config['counter'] += 1
#       except StopIteration:
#         pass

#       for inner_select in selection.selection:
#         replace_first_with_var(inner_select, values)

#     values = {}
#     for selection in query.selection:
#       replace_first_with_var(selection, values)
#       for key, value in values.items():
#         query.variables.append((VariableDefinition(name=key, type_=TypeRef.Named("Int")), value))

#     return query

#   def transform_response(self, req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
#     return data


DEFAULT_TRANSFORMS: list[Transform] = [
  TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal)),
  TypeTransform(TypeRef.Named('BigInt'), lambda bigint: int(bigint)),
]
