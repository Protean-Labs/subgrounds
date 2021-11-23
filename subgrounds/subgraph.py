from dataclasses import dataclass
from typing import Any
import os
import json

import subgrounds.client as client
from subgrounds.query import Query
from subgrounds.schema import Schema, mk_schema

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

  def query(self, query: Query) -> dict:
    return client.query(self.url, query.graphql_string())

  def __getattribute__(self, __name: str) -> Any:
    try:
      return super().__getattribute__(__name)
    except:
      return self.schema.__getattribute__(__name)