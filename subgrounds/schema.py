from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
import typing

# Type reference
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
  interfaces: typing.List[str] = field(default_factory=[])
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
      return FieldPath(self.schema, self, field, parent)
    except:
      raise TypeError(f"Object {self.meta.name} has no field {fname}")

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
      return FieldPath(self.schema, self, field, parent)
    except:
      raise TypeError(f"Interface {self.meta.name} has no field {fname}")

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

      # Set schema reference
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
class FieldPath:
  schema: Schema
  object_: Object | Interface
  field: FieldMeta
  parent: typing.Optional[FieldPath] = None

  def get_field(self, fname: str) -> FieldPath:
    tname = self.field.type_.root_type_name()
    return self.schema.type_map[tname].get_field(fname, self)

  def path(self) -> List[str]:
    if self.parent is not None:
      path = self.parent.path()
      path.append(self.field.name)
      return path
    else:
      return [self.object_.meta.name, self.field.name]

  def __getattribute__(self, __name: str) -> typing.Any:
    try:
      return super().__getattribute__(__name)
    except:
      return self.get_field(__name)