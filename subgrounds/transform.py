from abc import ABC, abstractmethod
from typing import Any, Callable
from functools import partial
from pipe import map, traverse

from subgrounds.query import Argument, DataRequest, Document, InputValue, Query, Selection, VariableDefinition, execute, pagination_args
from subgrounds.schema import TypeMeta, TypeRef
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

  # return Query(selection=list(map(transform, query.selection)))


def select_data(select: Selection, data: dict) -> list[Any]:
  match (select, data):
    case (Selection(TypeMeta.FieldMeta(name), _, _, [] | None), dict() as data) if name in data:
      return [data[name]]
    case (Selection(TypeMeta.FieldMeta(name), _, _, inner_select), dict() as data) if name in data:
      return list(inner_select | map(partial(select_data, data=data[name])) | traverse)
    case (select, data):
      raise Exception(f"select_data: invalid selection {select} for data {data}")


def transform_response(fmeta: TypeMeta.FieldMeta, func: Callable, args: list[Selection], req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
  def transform(select: Selection, data: dict) -> None:
    match (select, data):
      case (Selection(TypeMeta.FieldMeta(name), _, _, [] | None), dict() as data) if name == fmeta.name and name not in data:
        arg_values = list(args | map(partial(select_data, data=data)) | traverse)
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


# def chain_transforms(transforms: list[Transform], req: DataRequest) -> dict:
#   match transforms:
#     case []:
#       return execute(req)
#       # return list(map(lambda doc: client.query(doc.url, doc.graphql_string), req.documents))
#     case [transform, *rest]:
#       new_req = transform.transform_request(req)
#       data = chain_transforms(rest, new_req)
#       return transform.transform_response(req, data)


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

    for select in doc.query.selection:
      transform(select, data)

    return data


class LocalSyntheticField(DocumentTransform):
  def __init__(self, subgraph, fmeta: TypeMeta.FieldMeta, f: Callable, args: list[Selection]) -> None:
    self.subgraph = subgraph
    self.fmeta = fmeta
    self.f = f
    self.args = args

  def transform_document(self, doc: Document) -> Document:
    def transform(select: Selection) -> Selection | list[Selection]:
      match select:
        case Selection(TypeMeta.FieldMeta(name), _, _, [] | None) if name == self.fmeta.name:
          return self.args
        case Selection(_, _, _, [] | None):
          return [select]
        case Selection(TypeMeta.FieldMeta(name) as select_fmeta, alias, args, inner_select):
          new_inner_select = list(inner_select | map(transform) | traverse)
          return Selection(select_fmeta, alias, args, new_inner_select)
        case _:
          raise Exception(f"transform_document: unhandled selection {select}")

    return Document.transform(doc, query_f=lambda query: Query.transform(query, selection_f=transform))

  def transform_response(self, doc: Document, data: dict[str, Any]) -> list[dict[str, Any]]:
    def transform(select: Selection, data: dict) -> None:
      match (select, data):
        case (Selection(TypeMeta.FieldMeta(name), _, _, [] | None), dict() as data) if name == self.fmeta.name and name not in data:
          arg_values = flatten(list(self.args | map(partial(select_data, data=data))))
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

    for select in doc.query.selection:
      transform(select, data)

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


class PaginationTransform(RequestTransform):
  def __init__(self, page_size) -> None:
    self.page_size = page_size

  def requires_pagination(self, doc: Document) -> bool:
    return (
      Query.contains_argument(doc.query, 'first')
      and Query.get_argument(doc.query, 'first').value.is_number
      and Query.get_argument(doc.query, 'first').value.value > self.page_size
      and not Query.contains_argument(doc.query, 'skip')
    )

  def transform_request(self, req: DataRequest) -> DataRequest:
    """
    for doc in req.documents:
      if doc.query.selection contains 'first' argument
        and 'first' argument is a value larger than page_size
        and no 'skip' argument
      then
        return Document(
          Query(
            name=doc.query.name
            selection=[
              replace 'first' value with 'first' and 'skip' variables for selection in doc.query.selection
            ],
            variables: [
              VariableDefinition('first', 'Int!'),
              VariableDefinition('skip', 'Int!')
            ]
          ),
          variables = [
            {'first': page_size, 'skip': 0 * page_size},
            {'first': page_size, 'skip': 1 * page_size},
            {'first': page_size, 'skip': 2 * page_size},
            {'first': page_size, 'skip': 3 * page_size},
            ...
          ]
        )
      else
        return doc
    """

    def transform_doc_with_first_argument(doc: Document) -> Document:
      args = pagination_args(self.page_size, Query.get_argument(doc.query, 'first').value.value)

      query_new_args = Query.substitute_arg(doc.query, 'first', [
        Argument('first', InputValue.Variable('first')),
        Argument('skip', InputValue.Variable('skip'))
      ])

      return Document(
        url=doc.url,
        query=Query(
          name=query_new_args.name,
          selection=query_new_args.selection,
          variables=[
            VariableDefinition('first', TypeRef.non_null('Int')),
            VariableDefinition('skip', TypeRef.non_null('Int'))
          ]
        ),
        fragments=doc.fragments,
        variables=args if len(doc.variables) == 0 else [{**args, **doc.variables[0]} for args in args]
      )

    def transform_doc(doc: Document) -> Document:
      if (
        Query.contains_argument(doc.query, 'first')
        and Query.get_argument(doc.query, 'first').value.is_number
        and Query.get_argument(doc.query, 'first').value.value > self.page_size
        and not Query.contains_argument(doc.query, 'skip')
      ):
        return transform_doc_with_first_argument(doc)
      else:
        return doc

    return DataRequest.transform(req, transform_doc)

  def transform_response(self, req: DataRequest, data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return data


DEFAULT_GLOBAL_TRANSFORMS: list[RequestTransform] = [
  PaginationTransform(page_size=200)
]

DEFAULT_SUBGRAPH_TRANSFORMS: list[DocumentTransform] = [
  TypeTransform(TypeRef.Named('BigDecimal'), lambda bigdecimal: float(bigdecimal)),
  TypeTransform(TypeRef.Named('BigInt'), lambda bigint: int(bigint)),
]
