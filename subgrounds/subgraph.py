from typing import Any
from sgqlc.endpoint.http import HTTPEndpoint
from sgqlc.introspection import query, variables
from sgqlc.codegen.schema import CodeGen
from sgqlc.operation import Selector
import imp

from subgrounds.query import Entity

def gen_module(name, url):
  endpoint = HTTPEndpoint(url, base_headers={"Content-Type": "application/json"})

  data = endpoint(query, variables(
    include_description=True,
    include_deprecated=True,
  ))

  out_file = open(f"{name}.py", "w")
  gen = CodeGen(name, data["data"]["__schema"], out_file.write, False)
  gen.write()
  out_file.close()

class Subgraph:
  def __init__(self, name, url) -> None:
    self.name = name
    self.url = url
    self.endpoint = HTTPEndpoint(url, {"Content-Type": "application/json"})

    try:
      fp, path, desc = imp.find_module(name)
    except:
      gen_module(name, url)
      fp, path, desc = imp.find_module(name)

    # load_modules loads the module 
    # dynamically and takes the filepath
    # module and description as parameter
    self.module = imp.load_module(name, fp, path, desc)

  def __getattribute__(self, __name: str) -> Any:
    # if type(getattr(self.module, __name)) == Selector:
    #   return Field()
    try:
      return Entity(self, getattr(self.module, __name))
    except:
      return super().__getattribute__(__name)

  def schema(self):
    return getattr(self.module, self.name)