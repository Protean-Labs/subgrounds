from __future__ import annotations

from dataclasses import dataclass, field
from functools import partial, reduce
from itertools import count
import math
import operator
from pipe import map, traverse, where
from typing import Any, Optional, Tuple

from subgrounds.query import Argument, Document, InputValue, Selection, Query, VariableDefinition
import subgrounds.client as client
from subgrounds.schema import TypeRef

DEFAULT_NUM_ENTITIES = 100
PAGE_SIZE = 200


@dataclass(frozen=True)
class PaginationNode:
  first_name: str
  skip_name: str
  first_value: Optional[InputValue.T] = None
  skip_value: Optional[InputValue.T] = None
  inner: list[PaginationNode] = field(default_factory=list)

  def args(self, page_size: int, inner_size: int = 0) -> list[dict[str, int]]:
    num_entities = self.first_value.value if self.first_value is not None else 100
    num_pages = math.ceil(num_entities / page_size)
    skip0 = self.skip_value.value if self.skip_value is not None else 0

    return [
      # If we are at the last page and there is a remainder
      {self.first_name: num_entities % page_size, self.skip_name: skip0 + i * page_size} 
      if (i == num_pages - 1 and num_entities % page_size != 0)

      # If we are not at the last page or there is no remainder
      else {self.first_name: page_size, self.skip_name: skip0 + i * page_size}
      for i in range(0, num_pages)
    ]


def preprocess_document(
  document: Document,
) -> Tuple[Document, list[PaginationNode]]:
  counter = count(0)

  def preprocess_selection(select: Selection) -> Tuple[Selection, PaginationNode]:
    def preprocess_arg(arg: Argument, n: int) -> Argument:
      match arg.name:
        case 'first' | 'skip' as argname:
          return Argument(name=argname, value=InputValue.Variable(f'{argname}{n}'))
        case _:
          return arg
    
    def fold(
      acc: Tuple[list[Selection], list[PaginationNode]],
      select: Selection
    ) -> Tuple[list[Selection], list[PaginationNode]]:
      new_select, pagination_node = preprocess_selection(select)
      return ([*acc[0], new_select], [*acc[1], pagination_node])

    new_selections, pagination_nodes = reduce(fold, select.selection, ([], []))

    if select.fmeta.type_.is_list:
      n = next(counter)
      new_arguments = list(select.arguments | map(partial(preprocess_arg, n=n)))

      try:
        first_arg_value = next(select.arguments | where(lambda arg: arg.name == 'first')).value
      except StopIteration:
        first_arg_value = None
        new_arguments.append(Argument(name='first', value=InputValue.Variable(f'first{n}')))

      try:
        skip_arg_value = next(select.arguments | where(lambda arg: arg.name == 'skip')).value
      except StopIteration:
        skip_arg_value = None
        new_arguments.append(Argument(name='skip', value=InputValue.Variable(f'skip{n}')))

      return (
        Selection(
          fmeta=select.fmeta,
          alias=select.alias,
          arguments=new_arguments,
          selection=new_selections
        ),
        PaginationNode(
          first_name=f'first{n}',
          skip_name=f'skip{n}',
          first_value=first_arg_value,
          skip_value=skip_arg_value,
          inner=list(pagination_nodes | traverse)
        )
      )
    else:
      return (
        Selection(
          fmeta=select.fmeta,
          alias=select.alias,
          arguments=select.arguments,
          selection=new_selections
        ),
        list(pagination_nodes | traverse)
      )

  def f(
    item: Document | Query | Selection
  ) -> Tuple[Document | Query | Selection, PaginationNode]:
    match item:
      case Document(url, query, fragments, variables) as doc:
        if query is None:
          return (doc, [])
        else:
          new_query, pagination_nodes = f(query)
          return (
            Document(
              url=url,
              query=new_query,
              fragments=fragments,
              variables=variables
            ),
            list(pagination_nodes | traverse)
          )

      case Query(name, selection, variables):
        def fold(
          acc: Tuple[list[Selection], list[PaginationNode]],
          select: Selection
        ) -> Tuple[list[Selection], list[PaginationNode]]:
          new_select, pagination_node = f(select)
          return ([*acc[0], new_select], [*acc[1], pagination_node])

        new_selections, pagination_nodes = reduce(fold, selection, ([], []))
        return (
          Query(
            name=name,
            selection=new_selections,
            variables=variables
          ),
          list(pagination_nodes | traverse)
        )

      case Selection() as selection:
        return preprocess_selection(selection)

  return f(document)


def flatten(l: list[list | dict]) -> list[dict]:
  match l[0]:
    case list():
      return reduce(lambda acc, l: acc + flatten(l), l, [])
    case _:
      return l


def pagination_args(node: PaginationNode) -> list[dict]:
  match node.inner:
    case []:
      return node.args(PAGE_SIZE)

    case l:
      return flatten(list(
        range(0, node.first_value.value if node.first_value is not None else DEFAULT_NUM_ENTITIES)
        | map(lambda i: list(
          l | map(lambda nested_pagination: list(
            pagination_args(nested_pagination)
            | map(lambda nested_args: {node.first_name: 1, node.skip_name: i} | nested_args)
          ))
        ))
      ))


def trim_document(document: Document, pagination_args: dict[str, int]) -> Query:
  def trim_selection(selection: Selection) -> Optional[Selection]:
    try:
      arg = next(selection.arguments | where(lambda arg: arg.name == 'first'))
      if arg.value.name in pagination_args:
        return Selection(
          selection.fmeta,
          selection.alias,
          selection.arguments,
          list(selection.selection | map(trim_selection) | where(lambda val: val is not None))
        )
      else:
        return None
    except StopIteration:
      return Selection(
        selection.fmeta,
        selection.alias,
        selection.arguments,
        list(selection.selection | map(trim_selection) | where(lambda val: val is not None))
      )

  return Document(
    url=document.url,
    query=Query(
      name=document.query.name,
      selection=list(
        document.query.selection
        | map(trim_selection)
        | where(lambda val: val is not None)
      ),
      variables=[VariableDefinition(key, TypeRef.Named('Int')) for key in pagination_args] + document.query.variables
    ),
    fragments=document.fragments,
    variables=document.variables | pagination_args
  )


def execute(doc: Document) -> dict[str, Any]:
  match doc.variables:
    case []:
      return client.query(doc.url, doc.graphql)
    case [args]:
      return client.query(doc.url, doc.graphql, args)
    case args_list:
      return client.repeat(doc.url, doc.graphql, args_list)


def merge(data1, data2):
  match (data1, data2):
    case (list(), list()):
      return data1 + data2

    case (dict(), dict()):
      data = {}
      for key in data1:
        if key in data2:
          data[key] = merge(data1[key], data2[key])
        else:
          data[key] = data1[key]

      for key in data2:
        if key not in data:
          data[key] = data2[key]

      return data

    case (val1, _):
      return val1


def paginate(doc: Document) -> dict[str, Any]:
  new_doc, pagination_nodes = preprocess_document(doc)

  if pagination_nodes == []:
    return execute(doc)
  else:
    return reduce(merge, flatten(list(
      pagination_nodes
      | map(lambda node: list(
        pagination_args(node)
        | map(lambda args: client.query(new_doc.url, new_doc.graphql, variables=new_doc.variables[0] | args if new_doc.variables else args))
      ))
    )))
    