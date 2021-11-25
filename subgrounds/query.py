from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
import typing

# ================================================================
# Query definitions, data structures and types
# ================================================================

class InputValue:
  class T(ABC):
    pass

  @dataclass
  class Null(T):
    pass

  @dataclass
  class Int(T):
    value: int
    
  @dataclass
  class Float(T):
    value: float

  @dataclass
  class String(T):
    value: str

  @dataclass
  class Boolean(T):
    value: bool

  @dataclass
  class Enum(T):
    value: str

  @dataclass
  class List(T):
    value: typing.List[InputValue.T]

  @dataclass
  class Object(T):
    value: typing.Dict[str, InputValue.T]

  @staticmethod
  def graphql_string(input_value: T):
    match input_value:
      case InputValue.Null():
        return "null"
      case InputValue.Int(value) | InputValue.Float(value):
        return str(value)
      case InputValue.String(value):
        return f"\"{value}\""
      case InputValue.Boolean(value):
        return str(value).lower()
      case InputValue.Enum(value):
        return value
      case InputValue.List(value):
        return f"[{', '.join([InputValue.graphql_string(value) for value in value])}]"
      case InputValue.Object(value):
        return f"{{{', '.join([f'{key}: {InputValue.graphql_string(value)}' for key, value in value.items()])}}}"

@dataclass
class Argument:
  name: str
  value: InputValue

  def graphql_string(self) -> str:
    return f"{self.name}: {InputValue.graphql_string(self.value)}"

@dataclass
class Selection:
  name: str
  alias: typing.Optional[str] = None
  arguments: typing.Optional[typing.List[Argument]] = None
  selection: typing.Optional[typing.List[Selection]] = None

  def graphql_string(self, level:int=0) -> str:
    indent = "  " * level

    match (self.arguments, self.selection):
      case (None | [], None | []):
        return f"{indent}{self.name}"
      case (args, None | []):
        args_str = "(" + ", ".join([arg.graphql_string() for arg in args]) + ")"
        return f"{indent}{self.name}{args_str}"
      case (None | [], inner_selection):
        inner_str = f"\n".join([f.graphql_string(level=level+1) for f in inner_selection])
        return f"{indent}{self.name} {{\n{inner_str}\n{indent}}}"
      case (args, inner_selection):
        args_str = "(" + ", ".join([arg.graphql_string() for arg in args]) + ")"
        inner_str = f"\n".join([f.graphql_string(level=level+1) for f in inner_selection])
        return f"{indent}{self.name}{args_str} {{\n{inner_str}\n{indent}}}"

  def add_selection(self, new_selection):
    if self.selection is None:
      self.selection = []

    try:
      select = next(filter(lambda select: select.name == new_selection.name, self.selection))
      for s in new_selection.selection:
        select.add_selection(s)
    except:
      self.selection.append(new_selection)

  def add_selections(self, new_selections):
    for s in new_selections:
      self.add_selection(s)

@dataclass
class Query:
  selection: typing.Optional[typing.List[Selection]] = None

  def graphql_string(self) -> str:
    selection_str = "\n".join([f.graphql_string(level=1) for f in self.selection])
    return f"""query {{\n{selection_str}\n}}"""

  def add_selection(self, new_selection):
    if self.selection is None:
      self.selection = []
      
    try:
      select = next(filter(lambda select: select.name == new_selection.name, self.selection))
      for s in new_selection.selection:
        select.add_selection(s)
    except:
      self.selection.append(new_selection)

  def add_selections(self, new_selections):
    for s in new_selections:
      self.add_selection(s)
