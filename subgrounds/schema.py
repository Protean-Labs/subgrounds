from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import Field, dataclass, field
import typing
import operator
from enum import Enum, auto

from subgrounds.utils import flatten
from subgrounds.query2 import *

# ================================================================
# Schema definitions and data structures
# ================================================================
class TypeRef(ABC):
  def root_type_name(self) -> str:
    match self:
      case NonNull(inner) | List(inner):
        return inner.root_type_name()
      case Named(name):
        return name

@dataclass
class Named(TypeRef):
  name: str

@dataclass
class NonNull(TypeRef):
  inner: TypeRef

@dataclass
class List(TypeRef):
  inner: TypeRef

# Argument
@dataclass
class ArgumentMeta:
  name: str
  type_: TypeRef
  description: str = ""

@dataclass
class FieldMeta:
  name: str
  arguments: typing.List[ArgumentMeta]
  type_: TypeRef
  description: str = ""

@dataclass
class ScalarMeta:
  name: str
  description: str = ""

@dataclass
class ObjectMeta:
  name: str
  fields: typing.List[FieldMeta]
  interfaces: typing.List[str] = field(default_factory=list)
  description: str = ""

@dataclass
class EnumValueMeta:
  name: str
  description: str = ""

@dataclass
class EnumMeta:
  name: str
  values: typing.List[EnumValueMeta]
  description: str = ""

@dataclass
class InterfaceMeta:
  name: str
  fields: typing.List[FieldMeta]
  description: str = ""

@dataclass
class UnionMeta:
  name: str
  types: typing.List[str]
  description: str = ""

@dataclass
class InputObjectMeta:
  name: str
  input_fields: typing.List[ArgumentMeta]
  description: str = ""

class TypeError(Exception):
  pass

@dataclass
class TypeMeta(ABC):
  meta: ScalarMeta | ObjectMeta | EnumValueMeta | EnumMeta | InterfaceMeta | UnionMeta | InputObjectMeta

  def get_field(self, fname: str, parent: typing.Optional[FieldPath] = None) -> FieldMeta:
    raise TypeError(f"Cannot get field {fname} on non-Object type")

  def __getattribute__(self, __name: str) -> typing.Any:
    try:
      return super().__getattribute__(__name)
    except:
      return self.get_field(__name, None)

@dataclass
class Scalar(TypeMeta):
  meta: ScalarMeta

@dataclass
class Object(TypeMeta):
  meta: ObjectMeta
  schema: typing.Optional[Schema] = None

  def get_field(self, fname: str, parent: typing.Optional[FieldPath] = None) -> FieldMeta:
    try:
      field = next(filter(lambda field: field.name == fname, self.meta.fields))
      new_field_path = FieldPath(schema=self.schema, object_=self, field=field, parent=parent)
      if parent is not None:
        parent.child = new_field_path
      return new_field_path
    except:
      try:
        field = self.__dict__[fname]
        if parent is not None:
          parent.child = field
          field.parent = parent
        return field
      except:
        raise TypeError(f"Object {self.meta.name} has no field {fname}")

  # TODO: Set attribute to add SyntheticField
  # def __setattr__(self, __name: str, __value: Any) -> None:
  #     return super().__setattr__(__name, __value)

@dataclass
class Enum(TypeMeta):
  meta: EnumMeta

@dataclass
class Interface(TypeMeta):
  meta: InterfaceMeta
  schema: typing.Optional[Schema] = None

  def get_field(self, fname: str, parent: typing.Optional[FieldPath] = None) -> FieldMeta:
    try:
      field = next(filter(lambda field: field.name == fname, self.meta.fields))
      new_field_path = FieldPath(schema=self.schema, object_=self, field=field, parent=parent)
      parent.child = new_field_path
      return new_field_path
    except:
      raise TypeError(f"Interface {self.meta.name} has no field {fname}")

  # TODO: Set attribute to add SyntheticField
  # def __setattr__(self, __name: str, __value: Any) -> None:
  #     return super().__setattr__(__name, __value)

@dataclass
class Union(TypeMeta):
  meta: UnionMeta

@dataclass
class InputObject(TypeMeta):
  meta: InputObjectMeta

@dataclass
class SchemaMeta:
  query_type: str
  mutation_type: typing.Optional[str] = None
  subscription_type: typing.Optional[str] = None

@dataclass
class Schema:
  meta: SchemaMeta
  type_map: typing.Dict[str, TypeMeta]

  def __getattribute__(self, __name: str) -> typing.Any:
    try:
      return super().__getattribute__(__name)
    except:
      return self.type_map[__name]

# ================================================================
# Schema parsing
# ================================================================
class ParsingError(Exception):
  pass

def mk_schema(json):
  def mk_type_ref(json: dict) -> TypeRef:
    match json:
      case {"kind": "NON_NULL", "ofType": inner}:
        return NonNull(mk_type_ref(inner))
      case {"kind": "LIST", "ofType": inner}:
        return List(mk_type_ref(inner))
      case {"kind": "SCALAR" | "OBJECT" | "INTERFACE" | "ENUM" | "INPUT_OBJECT", "name": name}:
        return Named(name)
      case _ as json:
        raise ParsingError(f"mk_type_ref: {json}")

  def mk_argument_meta(json: dict) -> ArgumentMeta:
    match json:
      case {"name": name, "description": desc, "type": type_}:
        return ArgumentMeta(name, mk_type_ref(type_), description=desc)
      case _ as json:
        raise ParsingError(f"mk_argument_meta: {json}")

  def mk_field_meta(json: dict) -> FieldMeta:
    match json:
      case {"name": name, "description": desc, "args": args, "type": type_}:
        return FieldMeta(name, arguments=[mk_argument_meta(arg) for arg in args], type_=mk_type_ref(type_), description=desc)
      case _ as json:
        raise ParsingError(f"mk_field_meta: {json}")

  def mk_enum_value(json: dict) -> EnumValueMeta:
    match json:
      case {"name": name, "description": desc}:
        return EnumValueMeta(name, description=desc)
      case _ as json:
        raise ParsingError(f"mk_enum_value: {json}")

  def mk_type_meta(json: dict) -> TypeMeta:
    match json:
      case {"kind": "SCALAR", "name": name, "description": desc}:
        return Scalar(ScalarMeta(name, description=desc))
      case {"kind": "OBJECT", "name": name, "description": desc, "fields": fields, "interfaces": intfs}:
        return Object(ObjectMeta(name, fields=[mk_field_meta(f) for f in fields], interfaces=intfs, description=desc))
      case {"kind": "ENUM", "name": name, "description": desc, "enumValues": enum_values}:
        return Enum(EnumMeta(name, values=[mk_enum_value(val) for val in enum_values], description=desc))
      case {"kind": "INTERFACE", "name": name, "description": desc, "fields": fields}:
        return Interface(InterfaceMeta(name, fields=[mk_field_meta(f) for f in fields], description=desc))
      case {"kind": "UNION", "name": name, "description": desc, "possibleTypes": types}:
        return Union(UnionMeta(name, types=types, description=desc))
      case {"kind": "INPUT_OBJECT", "name": name, "description": desc, "inputFields": input_feilds}:
        return InputObject(InputObjectMeta(name,  input_fields=[mk_argument_meta(f) for f in input_feilds], description=desc))
      case _ as json:
        raise ParsingError(f"mk_type_meta: {json}")

  match json["__schema"]:
    case {"queryType": query_type, "mutationType": mutation_type, "subscriptionType": subscription_type, "types": types}:
      types_meta = [mk_type_meta(type_) for type_ in types]
      schema = Schema(
        meta=SchemaMeta(query_type, mutation_type, subscription_type),
        type_map={type_.meta.name: type_ for type_ in types_meta}
      )

      # Set schema reference to objects and interfaces
      for tname in schema.type_map:
        match schema.type_map[tname]:
          case Object(_) | Interface(_):
            schema.type_map[tname].schema = schema
          case _:
            pass

      return schema

    case _ as json:
      raise ParsingError(f"mk_schema: {json}")

# ================================================================
# Query building
# ================================================================
@dataclass
class Where:
  filters: typing.List[Filter]

  class Operator(Enum):
    EQ  = auto()
    NEQ = auto()
    LT  = auto()
    LTE = auto()
    GT  = auto()
    GTE = auto()

  @dataclass
  class Filter:
    name: str
    type_: Where.Operator
    value: typing.Any

  @staticmethod
  def and_(wheres):
    return Where(flatten([w.filters for w in wheres]))

  def argument(self):
    def name_of_arg(filter):
      if filter.type_ == Where.Operator.EQ:
        return filter.name
      else:
        return f'{filter.name}_{filter.type_.name.lower()}'
    
    return {name_of_arg(f): f.value for f in self.filters}

class QueryField(ABC):
  pass

@dataclass
class FieldPath(QueryField):
  schema: Schema
  object_: Object | Interface
  field: FieldMeta
  parent: typing.Optional[FieldPath] = None
  child: typing.Optional[FieldPath] = None
  args: typing.Dict[str, typing.Any] = field(default_factory=dict)

  def get_field(self, fname: str) -> FieldPath:
    tname = self.field.type_.root_type_name()
    return self.schema.type_map[tname].get_field(fname, self)

  def path(self) -> typing.List[FieldPath]:
    if self.parent is not None:
      path = self.parent.path()
      path.append(self)
      return path
    else:
      return [self]

  def path_string(self) -> List[str]:
    if self.parent is not None:
      path = self.parent.path()
      path.append(self.field.name)
      return path
    else:
      return [self.object_.meta.name, self.field.name]

  def get_selection(self) -> Selection:
    return self.path()[0].to_selection()

  def to_selection(self) -> Selection:
    if self.child is not None:
      select = self.child.to_selection()
    else:
      select = None

    args = [Argument(key, value) for key, value in self.args.items()]

    return [Selection(name=self.field.name, arguments=args, selection=select)]

  def __call__(self, **kwargs: typing.Any) -> typing.Any:
    def validate_args(args: typing.Dict[str, typing.Any]):
      for key, _ in args.items():
        try:
          next(filter(lambda arg_meta: arg_meta.name == key, self.field.arguments))
        except:
          raise TypeError(f"Invalid argument {key} for Field {'.'.join(self.path_string())}")

    def mk_input_value(value: typing.Any, arg: ArgumentMeta):
      arg_typ = self.schema.type_map[arg.type_.root_type_name()]
      match (value, arg_typ, arg.name):
        case (None, _, _):
          return Iv_Null
        case (int(), Scalar(meta=ScalarMeta(name="BigInt")), _):
          return Iv_String(str(value))
        case (int(), _, _):
          return Iv_Int(value)
        case (float(), Scalar(meta=ScalarMeta(name="BigDecimal")), _):
          return Iv_String(str(value))
        case (float(), _, _):
          return Iv_Float(value)
        case (str(), Enum(_), _):
          return Iv_Enum(value)
        case (str(), _, _):
          return Iv_String(value)
        case (bool(), _, _):
          return Iv_Boolean(value)
        # case (list(), _, "where"):    # If list of Where objects
        #   if isinstance(value[0], Where):
        #     Where.and_(value).argument
        #   return Iv_Object({key: mk_input_value(v) for key, v in value.items()})
        case (list(), _, _):
          return Iv_List([mk_input_value(v) for v in value])
        case (dict(), _, _):
          return Iv_Object({key: mk_input_value(v) for key, v in value.items()})

    if self.field.arguments:
      if not self.args:
        validate_args(kwargs)
        for arg in self.field.arguments:
          if arg.name in kwargs:
            self.args[arg.name] = mk_input_value(kwargs[arg.name], arg)

      return self
    else:
      raise TypeError(f"Field {'.'.join(self.path_string())} takes no arguments!")

  def __getattribute__(self, __name: str) -> typing.Any:
    try:
      return super().__getattribute__(__name)
    except:
      return self.get_field(__name)

  # Filter construction
  # def __eq__(self, value: typing.Any) -> Where:
  #   return Where([Where.Filter(self.graphql_name(), Where.Operator.EQ, value)])

  # def __lt__(self, value: typing.Any) -> Where:
  #   return Where([Where.Filter(self.graphql_name(), Where.Operator.LT, value)])

  # def __lte__(self, value: typing.Any) -> Where:
  #   return Where([Where.Filter(self.graphql_name(), Where.Operator.LTE, value)])

  # def __gt__(self, value: typing.Any) -> Where:
  #   return Where([Where.Filter(self.graphql_name(), Where.Operator.GT, value)])

  # def __gte__(self, value: typing.Any) -> Where:
  #   return Where([Where.Filter(self.graphql_name(), Where.Operator.GTE, value)])

  # SyntheticField arithmetic
  def __add__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.add, self, other)

  def __sub__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.sub, self, other)

  def __mul__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.mul, self, other)

  def __truediv__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.truediv, self, other)

  def __pow__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.pow, self, other)

  def __neg__(self) -> SyntheticField:
    return SyntheticField(operator.neg, self)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(operator.abs, self)

@dataclass
class SyntheticField(QueryField):
  counter: typing.ClassVar[int] = 0
  
  func: function
  args: List[QueryField]
  parent: typing.Optional[FieldPath] = None

  def __init__(self, func: function, *args: typing.List[typing.Any]) -> None:
    self.func = func
    self.args = args
    self.name = f"SyntheticField_{SyntheticField.counter}"
    SyntheticField.counter += 1 

  def path(self) -> typing.List[QueryField]:
    if self.parent is not None:
      path = self.parent.path()
      path.append(self)
      return path
    else:
      return [self]

  def get_selection(self) -> Selection:
    return self.path()[0].to_selection()

  def to_selection(self) -> Selection:
    return flatten([field.to_selection() for field in self.args if isinstance(field, QueryField)])

  def __add__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.add, self, other)

  def __sub__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.sub, self, other)

  def __mul__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.mul, self, other)

  def __truediv__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.truediv, self, other)

  def __pow__(self, other: typing.Any) -> SyntheticField:
    return SyntheticField(operator.pow, self, other)

  def __neg__(self) -> SyntheticField:
    return SyntheticField(operator.neg, self)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(operator.abs, self)