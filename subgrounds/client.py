import requests

INTROSPECTION_QUERY = """
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


def get_schema(url: str) -> dict:
  resp = requests.post(
    url,
    json={"query": INTROSPECTION_QUERY},
    headers={"Content-Type": "application/json"}
  ).json()

  try:
    return resp["data"]
  except KeyError as exn:
    raise Exception(resp["errors"]) from exn


def query(url: str, query_str: str) -> dict:
  # print(f"Query:\n{query}")
  resp = requests.post(
    url,
    json={"query": query_str}, 
    headers={"Content-Type": "application/json"}
  ).json()

  try:
    return resp["data"]
  except KeyError as exn:
    raise Exception(resp["errors"]) from exn
