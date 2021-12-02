from abc import ABC, abstractmethod
from typing import Any, Dict, List

from subgrounds.query import Query, Selection
from subgrounds.schema import TypeMeta
from subgrounds.utils import flatten

class Transform(ABC):
  @abstractmethod
  def transform_request(self, query: Query) -> Query:
    pass

  @abstractmethod
  def transform_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
    pass

def transform_selection(query: Query, fmeta: TypeMeta.FieldMeta, fname: str, replacement: List[Selection]) -> Query:
  def replace(select: Selection):
    match select:
      case Selection(name, _, _, [] | None) if name == fname:
        return replacement
      case Selection(_, _, _, [] | None):
        return [select]
      case Selection(name, alias, args, inner_select):
        new_inner_select = flatten(list(map(replace, inner_select)))
        return Selection(name, alias, args, new_inner_select)
      case _:
        raise Exception(f"replace_selection: unhandled selection {select}")

  return Query(selection=list(map(replace, query.selection)))

class LocalSyntheticField(Transform):
  def __init__(self, subgraph, func, args) -> None:
    self.subgraph = subgraph
    self.func = func
    self.args = args

  # def 