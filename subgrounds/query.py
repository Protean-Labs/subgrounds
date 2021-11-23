from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
import typing

# from subgrounds.schema import FieldPath

class InputValue(ABC):
  @abstractmethod
  def graphql_string(self) -> str:
    pass

@dataclass
class Iv_Null(InputValue):
  def graphql_string(self) -> str:
    return "null"

@dataclass
class Iv_Int(InputValue):
  value: int

  def graphql_string(self) -> str:
    return str(self.value)
  
@dataclass
class Iv_Float(InputValue):
  value: float

  def graphql_string(self) -> str:
    return str(self.value)

@dataclass
class Iv_String(InputValue):
  value: str

  def graphql_string(self) -> str:
    return f"\"{self.value}\""

@dataclass
class Iv_Boolean(InputValue):
  value: bool

  def graphql_string(self) -> str:
    return str(self.value)

@dataclass
class Iv_Enum(InputValue):
  value: str

  def graphql_string(self) -> str:
    return self.value

@dataclass
class Iv_List(InputValue):
  value: typing.List[InputValue]

  def graphql_string(self) -> str:
    return f"[{', '.join([value.graphql_string() for value in self.value])}]"

@dataclass
class Iv_Object(InputValue):
  value: typing.Dict[str, InputValue]

  def graphql_string(self) -> str:
    return f"{{{', '.join([f'{key}: {value.graphql_string()}' for key, value in self.value.items()])}}}"

@dataclass
class Argument:
  name: str
  value: InputValue

  def graphql_string(self) -> str:
    return f"{self.name}: {self.value.graphql_string()}"

@dataclass
class Selection:
  name: str
  alias: typing.Optional[str] = None
  arguments: typing.Optional[typing.List[Argument]] = None
  selection: typing.Optional[typing.List[Selection]] = None

  def graphql_string(self, level:int=0) -> str:
    indent = "  " * level

    if self.arguments:
      args_str = "(" + ", ".join([arg.graphql_string() for arg in self.arguments]) + ")"
    else:
      args_str = ""

    if self.selection == None:
      return f"{indent}{self.name}{args_str}"
    else:
      selection_str = f"{indent}{indent}\n".join([f.graphql_string(level=level+1) for f in self.selection])
      return f"{indent}{self.name}{args_str} {{\n{selection_str}\n{indent}}}"

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
  selection: typing.List[Selection]

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
