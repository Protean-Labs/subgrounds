from abc import ABC, abstractmethod
from typing import Any, Callable, Dict, List
from functools import partial

from subgrounds.query import Query, Selection
from subgrounds.schema import FieldMeta, TypeRef
from subgrounds.utils import flatten


class Transform(ABC):
  @abstractmethod
  def transform_selection(self, query: Query) -> Query:
    pass

  @abstractmethod
  def transform_data(self, query: Query, data: Dict[str, Any]) -> Dict[str, Any]:
    pass


def transform_selection(fmeta: FieldMeta, replacement: List[Selection], query: Query) -> Query:
  def transform(select: Selection):
    match select:
      case Selection(FieldMeta(name), _, _, [] | None) if name == fmeta.name:
        return replacement
      case Selection(_, _, _, [] | None):
        return [select]
      case Selection(FieldMeta(name) as select_fmeta, alias, args, inner_select):
        new_inner_select = flatten(list(map(transform, inner_select)))
        return Selection(select_fmeta, alias, args, new_inner_select)
      case _:
        raise Exception(f"transform_selection: unhandled selection {select}")

  return Query(selection=list(map(transform, query.selection)))


def select_data(select: Selection, data: dict) -> list[Any]:
  match (select, data):
    case (Selection(FieldMeta(name), _, _, [] | None), dict() as data) if name in data:
      return [data[name]]
    case (Selection(FieldMeta(name), _, _, inner_select), dict() as data) if name in data:
      return flatten(list(map(partial(select_data, data=data[name]), inner_select)))
    case (select, data):
      raise Exception(f"select_data: invalid selection {select} for data {data}")


def transform_data(fmeta: FieldMeta, func: Callable, args: List[Selection], query: Query, data: dict) -> dict:
  def transform(select: Selection, data: dict) -> None:
    match (select, data):
      case (Selection(FieldMeta(name), _, _, [] | None), dict() as data) if name == fmeta.name and name not in data:
        arg_values = flatten(list(map(partial(select_data, data=data), args)))
        data[name] = func(*arg_values)
      case (Selection(FieldMeta(name), _, _, [] | None), dict() as data):
        pass
      case (Selection(FieldMeta(name), _, _, inner_select), dict() as data) if name in data:
        match data[name]:
          case list() as elts:
            for elt in elts:
              for select in inner_select:
                transform(select, elt)
          case dict() as elt:
            for select in inner_select:
              transform(select, elt)
          case _:
            raise Exception(f"transform: data for selection {select} is neither list or dict {data[name]}")

      case (select, data):
        raise Exception(f"transform: invalid selection {select} for data {data}")

  for select in query.selection:
    transform(select, data)

  return data


def transform_data_scalar(f: Callable, type_: TypeRef.T, query: Query, data: dict) -> dict:
  def transform(select: Selection, data: dict) -> None:
    match (select, data):
      case (Selection(FieldMeta(name, _, _, ftype), _, _, [] | None), dict() as data) if type_ == ftype:
        data[name] = f(data[name])
      case 

class SubgraphTypesTransform(Transform):
  def __init__(self) -> None:
    super().__init__()

  def transform_selection(self, query: Query) -> Query:
    return query

  def transform_data(self, query: Query, data: Dict[str, Any]) -> Dict[str, Any]:


class LocalSyntheticField(Transform):
  def __init__(self, subgraph, fmeta: FieldMeta, func: Callable, args: List[Selection]) -> None:
    self.subgraph = subgraph
    self.fmeta = fmeta
    self.func = func
    self.args = args

  def transform_selection(self, query: Query) -> Query:
    return transform_selection(self.fmeta, self.args, query)

  def transform_data(self, query: Query, data: Dict[str, Any]) -> Dict[str, Any]:
    return transform_data(self.fmeta, self.func, self.args, query, data)
