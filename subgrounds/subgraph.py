from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import Field, dataclass, field
from enum import Enum, auto
import typing
import operator
import os
import json

from subgrounds.utils import flatten
import subgrounds.query as q
import subgrounds.client as client

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

  def fmt_value(self, value):
    def mk_input_value(value, type_, non_null=False):    
      match (value, type_, non_null):
        # Only allow Null values when non_null=True
        case (None, _, False):
          return q.Iv_Null
        
        # If type is non_null, recurse with non_null=True
        case (_, NonNull(t), _):
          return mk_input_value(value, t, non_null=True)

        case (list(), List(t), _):
          return q.Iv_List([mk_input_value(val, t, non_null) for val in value])          

        case (int(), Named("BigInt"), _):
          return q.Iv_String(str(value))
        case (int(), Named("Int"), _):
          return q.Iv_Int(value)
        case (int() | float(), Named("BigDecimal"), _):
          return q.Iv_String(str(float(value)))
        case (int() | float(), Named("BigDecimal"), _):
          return q.Iv_Float(float(value))
        case (str(), Named("String" | "Bytes"), _):
          return q.Iv_String(value)
        case (str(), _, _):
          return q.Iv_Enum(value)
        case (bool(), Named("Boolean"), _):
          return q.Iv_Boolean(value)
        case (value, typ, non_null):
          raise TypeError(f"mk_input_value({value}, {typ}, {non_null})")
    
    return mk_input_value(value, self.type_)

@dataclass
class FieldMeta:
  name: str
  arguments: typing.List[ArgumentMeta]
  type_: TypeRef
  description: str = ""

  def process(self, value: dict):
    match self.type_:
      case Named("BigInt") | NonNull(Named("BigInt")):
        return int(value)
      case Named("BigDecimal") | NonNull(Named("BigDecimal")):
        return float(value)
      case _:
        return value

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

  def get_field(self, fname: str) -> FieldMeta | SyntheticField:
    raise TypeError(f"Cannot get field {fname} on non-Object type")

  def __getattribute__(self, __name: str) -> typing.Any:
    try:
      return super().__getattribute__(__name)
    except:
      match self.get_field(__name):
        case FieldMeta(_) as field:
          return FieldPath(schema=self.schema, object_=self, path=[FieldPath.FieldData(field)])
        case SyntheticField(_) as field:
          return FieldPath(schema=self.schema, object_=self, path=[field])

@dataclass
class Scalar(TypeMeta):
  meta: ScalarMeta

@dataclass
class Object(TypeMeta):
  meta: ObjectMeta
  schema: typing.Optional[Schema] = None
  synthetic_fields: typing.List[SyntheticField] = field(default_factory=list)

  def get_field(self, fname: str) -> FieldMeta | SyntheticField:
    try:
      # When fname refers to a "native" field
      return next(filter(lambda field: field.name == fname, self.meta.fields))
    except:
      try:
        # When fname refers to a synthetic field
        return next(filter(lambda field: field.name == fname, self.synthetic_fields))
      except:
        raise TypeError(f"Object {self.meta.name} has no field {fname}")

  def __setattr__(self, __name: str, __value: typing.Any) -> None:
    match __value:
      case SyntheticField(_):
        __value.name = __name
        self.synthetic_fields.append(__value)
      case _:
        return super().__setattr__(__name, __value)

@dataclass
class Enum(TypeMeta):
  meta: EnumMeta

@dataclass
class Interface(TypeMeta):
  meta: InterfaceMeta
  schema: typing.Optional[Schema] = None
  synthetic_fields: typing.List[SyntheticField] = field(default_factory=list)

  def get_field(self, fname: str, parent: typing.Optional[FieldPath] = None) -> FieldMeta:
    try:
      # When fname refers to a "native" field
      return next(filter(lambda field: field.name == fname, self.meta.fields))
    except:
      try:
        # When fname refers to a synthetic field
        return next(filter(lambda field: field.name == fname, self.synthetic_fields))
      except:
        raise TypeError(f"Object {self.meta.name} has no field {fname}")

  def __setattr__(self, __name: str, __value: typing.Any) -> None:
    match __value:
      case SyntheticField(_):
        __value.name = __name
        self.synthetic_fields.append(__value)
      case _:
        return super().__setattr__(__name, __value)

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
  subgraph: typing.Optional[Subgraph] = None

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
    case {"queryType": query_type, "types": types}:
      try:
        mutation_type = json["__schema"]["mutationType"]["name"]
      except:
        mutation_type = None

      try:
        subscription_type = json["__schema"]["subscriptionType"]["name"]
      except:
        subscription_type = None

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
class SyntheticField:
  counter: typing.ClassVar[int] = 0
  
  func: function
  args: typing.List[typing.Any]

  def __init__(self, func: function, *args: typing.List[typing.Any]) -> None:
    self.func = func
    self.args = args
    self.name = f"SyntheticField_{SyntheticField.counter}"
    SyntheticField.counter += 1

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
    field: FieldPath
    op: Where.Operator
    value: typing.Any

    @property
    def name(self):
      match self.op:
        case Where.Operator.EQ:
          return self.field.leaf.field.name
        case Where.Operator.NEQ:
          return f"{self.field.leaf.field.name}_not"
        case Where.Operator.LT:
          return f"{self.field.leaf.field.name}_lt"
        case Where.Operator.GT:
          return f"{self.field.leaf.field.name}_gt"
        case Where.Operator.LTE:
          return f"{self.field.leaf.field.name}_lte"
        case Where.Operator.GTE:
          return f"{self.field.leaf.field.name}_gte"

  @staticmethod
  def and_(wheres):
    return Where(flatten([w.filters for w in wheres]))

  @property
  def argument(self):
    return dict([f.argument for f in self.filters])

class QueryElement(ABC):
  pass

@dataclass
class FieldPath(QueryElement):
  schema: Schema
  object_: Object | Interface
  path: typing.List[FieldData | SyntheticField]

  @dataclass
  class FieldData:
    field: FieldMeta
    args: typing.Dict[str, typing.Any] = field(default_factory=dict)

    @property
    def name(self):
      return self.field.name

  @property
  def root(self):
    return self.path[0]

  @property
  def leaf(self):
    return self.path[-1]

  def add_field(self, fname: str) -> FieldPath:
    tname = self.leaf.field.type_.root_type_name()
    match self.schema.type_map[tname].get_field(fname):
      case FieldMeta(_) as field:
        self.path.append(FieldPath.FieldData(field))
      case SyntheticField(_) as field:
        self.path.append(field)

    return self

  def path_string(self) -> List[str]:
    return f"{self.object_.meta.name}." + '.'.join([fdata.field.name for fdata in self.path])

  def __call__(self, **kwargs: typing.Any) -> typing.Any:
    def validate_args(args: typing.Dict[str, typing.Any]):
      for key, _ in args.items():
        try:
          next(filter(lambda arg_meta: arg_meta.name == key, self.leaf.field.arguments))
        except:
          raise TypeError(f"Invalid argument {key} for Field {'.'.join(self.path_string())}")

    def fmt_where(where: Where) -> typing.Tuple[str, q.InputValue]:
      def get_where_input_object():
        where_arg = next(filter(lambda arg: arg.name == 'where', self.leaf.field.arguments))
        return self.schema.type_map[where_arg.type_.root_type_name()]

      input_object = get_where_input_object()

      where_object = {}
      for f in where.filters:
        try:
          arg_meta = next(filter(lambda arg: arg.name == f.name, input_object.meta.input_fields))
          arg_value = arg_meta.fmt_value(f.value)
          where_object[f.name] = arg_value
        except:
          raise TypeError(f"Field {self.path_string()}: 'where' argument does not support field {f.name}")

      return where_object

    def mk_input_value(value: typing.Any, arg: ArgumentMeta):
      arg_typ = self.schema.type_map[arg.type_.root_type_name()]
      match (value, arg_typ, arg.name):
        case (None, _, _):
          return q.Iv_Null
        case (int(), Scalar(meta=ScalarMeta(name="BigInt")), _):
          return q.Iv_String(str(value))
        case (int(), _, _):
          return q.Iv_Int(value)
        case (float(), Scalar(meta=ScalarMeta(name="BigDecimal")), _):
          return q.Iv_String(str(value))
        case (float(), _, _):
          return q.Iv_Float(value)
        case (str(), Enum(_), _):
          return q.Iv_Enum(value)
        case (str(), _, _):
          return q.Iv_String(value)
        case (bool(), _, _):
          return q.Iv_Boolean(value)
        case (list(), _, "where"):    # If list of Where objects
          if isinstance(value[0], Where):
            return q.Iv_Object(fmt_where(Where.and_(value)))
        case (FieldPath(_), _, "orderBy"):
          return q.Iv_Enum(value.leaf.field.name)
        case (list(), _, _):
          return q.Iv_List([mk_input_value(v) for v in value])
        case (dict(), _, _):
          return q.Iv_Object({key: mk_input_value(v) for key, v in value.items()})

    if self.leaf.field.arguments:
      if not self.leaf.args:
        validate_args(kwargs)
        for arg in self.leaf.field.arguments:
          if arg.name in kwargs:
            self.leaf.args[arg.name] = mk_input_value(kwargs[arg.name], arg)

      return self
    else:
      raise TypeError(f"Field {'.'.join(self.path_string())} takes no arguments!")

  def __getattribute__(self, __name: str) -> typing.Any:
    try:
      return super().__getattribute__(__name)
    except AttributeError:
      return self.add_field(__name)

  # Filter construction
  def __eq__(self, value: typing.Any) -> Where:
    return Where([Where.Filter(self, Where.Operator.EQ, value)])

  def __lt__(self, value: typing.Any) -> Where:
    return Where([Where.Filter(self, Where.Operator.LT, value)])

  def __lte__(self, value: typing.Any) -> Where:
    return Where([Where.Filter(self, Where.Operator.LTE, value)])

  def __gt__(self, value: typing.Any) -> Where:
    return Where([Where.Filter(self, Where.Operator.GT, value)])

  def __gte__(self, value: typing.Any) -> Where:
    return Where([Where.Filter(self, Where.Operator.GTE, value)])

  # SyntheticField arithmetic
  def mk_synthetic_field(self, op: function, other: typing.Any) -> SyntheticField:
    match other:
      case FieldPath(_):
        return SyntheticField(op, self, other)
      case _:
        return SyntheticField(op, self, other)

  def __add__(self, other: typing.Any) -> SyntheticField:
    return self.mk_synthetic_field(operator.add, other)

  def __sub__(self, other: typing.Any) -> SyntheticField:
    return self.mk_synthetic_field(operator.sub, other)

  def __mul__(self, other: typing.Any) -> SyntheticField:
    return self.mk_synthetic_field(operator.mul, other)

  def __truediv__(self, other: typing.Any) -> SyntheticField:
    return self.mk_synthetic_field(operator.truediv, other)

  def __pow__(self, other: typing.Any) -> SyntheticField:
    return self.mk_synthetic_field(operator.pow, other)

  def __neg__(self) -> SyntheticField:
    return SyntheticField(operator.neg, self.leaf)

  def __abs__(self) -> SyntheticField:
    return SyntheticField(operator.abs, self.leaf)

def mk_selection(field: FieldPath) -> q.Selection:
  def mk_path_selection(path: typing.List[FieldPath.FieldData | SyntheticField]) -> typing.List[q.Selection]:
    match path:
      case [FieldPath.FieldData(field, args), *rest]:
        args = [q.Argument(key, value) for key, value in args.items()]
        return [q.Selection(name=field.name, arguments=args, selection=mk_path_selection(rest))]
      case [SyntheticField(args=args), *_]:
        return [q.Selection(name=fdata.field.name, arguments=fdata.args, selection=None) for fdata in args if isinstance(fdata, FieldPath.FieldData)]
      case []:
        return None

  return mk_path_selection(field.path)

@dataclass
class Query:
  url: str
  query: q.Query
  data: dict
  toplevel: typing.Optional[FieldPath] = None
  fields: typing.List[FieldPath] = field(default_factory=list)

  def __init__(self, toplevel: typing.Optional[FieldPath] = None, fields: typing.List[FieldPath] = []) -> None:
    self.toplevel = toplevel
    self.fields = fields

    # Set url
    if toplevel:
      self.url = self.toplevel.schema.subgraph.url
    else:
      self.url = self.fields[0].schema.subgraph.url

    if toplevel:
      toplevel_selection = mk_selection(toplevel)[0]

      for field in fields:
        toplevel_selection.add_selections(mk_selection(field))

      self.query = q.Query([toplevel_selection])
    else:
      self.query = q.Query([])

      for field in fields:
        self.query.add_selections(mk_selection(field))

  def execute(self):
    def get_field_data(path: typing.List[FieldPath.FieldData | SyntheticField], data: dict) -> typing.Any:
      match path:
        case [FieldPath.FieldData(field=field)]:
          return data[field.name]
        case [FieldPath.FieldData(field=field), *rest]:
          match data[field.name]:
            case list():
              return [get_field_data(rest, d) for d in data[field.name]]
            case dict():
              return get_field_data(rest, data)
        case [SyntheticField(name=name)]:
          return data[name]

    def process_data(fields: typing.List[FieldPath.FieldData | SyntheticField], data: dict):
      # print(f"process_data: fields = {fields}, data = {data}")
      match fields:
        case [FieldPath.FieldData(field=field)]:
          data[field.name] = field.process(data[field.name])

        case [FieldPath.FieldData(field=field), *rest]:
          match data[field.name]:
            case list():
              for d in data[field.name]:
                process_data(rest, d)
            case dict():
              process_data(rest, data)

        case [SyntheticField(args=args) as sfield]:
          arg_values = []
          for field in args:
            match field:
              case SyntheticField(_):
                process_data([field], data)
                arg_values.append(get_field_data([field], data))
              case FieldPath(path=path):
                process_data(path, data)
                arg_values.append(get_field_data(path, data))
              case _:
                arg_values.append(field)

          data[sfield.name] = sfield.func(*arg_values)
        case _:
          pass
    
    data = client.query(self.url, self.query.graphql_string())
    if self.toplevel:
      for field in self.fields:
        path = [self.toplevel.leaf, *field.path]
        process_data(path, data)
    else:
      for field in self.fields:
        process_data(field.path, data)

    return data

@dataclass
class Subgraph:
  url: str
  schema: Schema

  def __init__(self, url: str) -> None:
    filename = url.split("/")[-1] + ".json"
    if os.path.isfile(filename):
      with open(filename) as f:
        schema = json.load(f)
    else:
      schema = client.get_schema(url)
      with open(filename, mode="w") as f:
        json.dump(schema, f)

    self.url = url
    self.schema = mk_schema(schema)
    self.schema.subgraph = self

  def query(self, query: Query) -> dict:
    return client.query(self.url, query.graphql_string())

  def __getattribute__(self, __name: str) -> typing.Any:
    try:
      return super().__getattribute__(__name)
    except:
      return self.schema.__getattribute__(__name)