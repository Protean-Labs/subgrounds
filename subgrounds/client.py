from typing import Any
import requests
from functools import reduce

import logging
logger = logging.getLogger('subgrounds')

INTROSPECTION_QUERY: str = """
  query IntrospectionQuery {
    __schema {
      queryType { name }
      mutationType { name }
      types {
        ...FullType
      }
      directives {
        name
        description
        locations
        args {
          ...InputValue
        }
      }
    }
  }
  fragment FullType on __Type {
    kind
    name
    description
    fields(includeDeprecated: true) {
      name
      description
      args {
        ...InputValue
      }
      type {
        ...TypeRef
      }
      isDeprecated
      deprecationReason
    }
    inputFields {
      ...InputValue
    }
    interfaces {
      ...TypeRef
    }
    enumValues(includeDeprecated: true) {
      name
      description
      isDeprecated
      deprecationReason
    }
    possibleTypes {
      ...TypeRef
    }
  }
  fragment InputValue on __InputValue {
    name
    description
    type { ...TypeRef }
    defaultValue
  }
  fragment TypeRef on __Type {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                }
              }
            }
          }
        }
      }
    }
  }
"""


def get_schema(url: str) -> dict[str, Any]:
  resp = requests.post(
    url,
    json={"query": INTROSPECTION_QUERY},
    headers={"Content-Type": "application/json"}
  ).json()

  try:
    return resp["data"]
  except KeyError as exn:
    raise Exception(resp["errors"]) from exn


def query(url: str, query_str: str, variables: dict[str, Any] = {}) -> dict[str, Any]:
  logger.debug(f'client.query: url = {url}, variables = {variables}\n{query_str}')
  resp = requests.post(
    url,
    json={'query': query_str} if variables == {} else {'query': query_str, 'variables': variables},
    headers={'Content-Type': 'application/json'}
  ).json()

  try:
    return resp['data']
  except KeyError as exn:
    raise Exception(resp['errors']) from exn


def merge_data(d1: dict, d2: dict) -> dict[str, Any]:
  match (d1, d2):
    case (list(), list()):
      return d1 + d2

    case (dict(), dict()):
      return [d1, d2]

    case (val1, _):
      return val1


def repeat(url: str, query_str: str, variables: list[dict[str, Any]]) -> dict[str, Any]:
  def merge(data1, data2):
    match (data1, data2):
      case (list(), list()):
        return data1 + data2
      case (dict(), dict()):
        data = {}
        for key in data1:
          data[key] = merge_data(data1[key], data2[key])
        return data
      case (val1, _):
        return val1

  return reduce(
    merge,
    [query(url, query_str, vars) for vars in variables]
  )


def paginate(url: str, query_str: str, n: int, page_size: int = 200) -> dict[str, Any]:
  vars = [{'first': page_size, 'skip': i * page_size} for i in range(0, n % page_size + 1)]
  return repeat(url, query_str, variables=vars)
