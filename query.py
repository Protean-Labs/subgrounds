from __future__ import annotations
from enum import Enum, auto
from typing import Any, List, Union
from dataclasses import dataclass, field
import json

import client

@dataclass
class Argument:
  class InputValue(Enum):
    NULL    = auto()
    INT     = auto()
    FLOAT   = auto()
    STRING  = auto()
    BOOLEAN = auto()
    ENUM    = auto()
    LIST    = auto()
    OBJECT  = auto()

  name: str
  type_: InputValue
  value: Any

  def compile(self):
    if self.type_ == Argument.InputValue.NULL:
      return f"{self.name}: null"
    elif self.type_ == Argument.InputValue.INT:
      return f"{self.name}: {self.value}"
    elif self.type_ == Argument.InputValue.FLOAT:
      return f"{self.name}: {self.value}"
    elif self.type_ == Argument.InputValue.STRING:
      return f"{self.name}: \"{self.value}\""
    elif self.type_ == Argument.InputValue.BOOLEAN:
      return f"{self.name}: {str(self.value).lowercase()}"
    elif self.type_ == Argument.InputValue.ENUM:
      return f"{self.name}: {self.value}"
    elif self.type_ == Argument.InputValue.LIST:
      # TODO: Validate this
      return f"{self.name}: {self.value}"
    elif self.type_ == Argument.InputValue.OBJECT:
      return f"{self.name}: {json.dumps(self.value)}"

@dataclass
class Field:
  name: str
  alias: Union[str, None] = None
  arguments: List[Argument] = field(default_factory=list)
  selection: Union[List[Field], None] = None

  def compile(self, level=0):
    indent = "  " * level

    if self.arguments != []:
      args_str = "(" + ", ".join([arg.compile() for arg in self.arguments]) + ")"
    else:
      args_str = ""

    if self.selection == None:
      return f"""{indent}{self.name}{args_str}"""
    else:
      fields_str = f"{indent}{indent}\n".join([f.compile(level=level+1) for f in self.selection])
      return f"""{indent}{self.name}{args_str} {{\n{fields_str}\n{indent}}}"""

@dataclass
class Query:
  name: str
  selection: List[Field] = field(default_factory=list)

  def compile(self):
    fields_str = "\n".join([f.compile(level=1) for f in self.selection])
    return f"""query {self.name} {{\n{fields_str}\n}}"""

  def query(self, url):
    return client.query(url, self.compile())

# query = Query("MyQuery", [
#   Field("objectA", 
#     arguments=[
#       Argument("id", Argument.InputValue.STRING, "123"),
#       Argument("where", Argument.InputValue.OBJECT, {"field1_lt": 1, "field2_gt": 10}),
#       Argument("order_by", Argument.InputValue.ENUM, "number"),
#       Argument("order_direction", Argument.InputValue.ENUM, "asc"),
#     ],
#     selection=[
#       Field("objectB", selection=[
#         Field("fieldA"),
#         Field("fieldB"),
#         Field("fieldC"),
#       ]),
#       Field("fieldD"),
#       Field("fieldE"),
#     ]
#   )
# ])

# print(query.compile())