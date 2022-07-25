import pytest

from subgrounds.query import Argument, Document, InputValue, Query, Selection, VariableDefinition
from subgrounds.schema import TypeMeta, TypeRef


def doc0():
  """
  query {
    pairs {
      id
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[Selection(
      fmeta=TypeMeta.FieldMeta('pairs', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
      ], TypeRef.non_null_list('Pair')),
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
        )
      ]
    )]
  ))


def doc1():
  """
  query {
    pairs {
      swaps {
        id
      }
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        selection=[
          Selection(
            fmeta=TypeMeta.FieldMeta('swaps', '', [
              TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None)
            ], TypeRef.non_null_list('Swap')),
            selection=[
              Selection(
                fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
              )
            ]
          )
        ]
      )]
    ))


def doc2():
  """
  query {
    pairs {
      swaps {
        id
      }
    }
    swaps {
      id
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[
      Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        selection=[
          Selection(
            fmeta=TypeMeta.FieldMeta('swaps', '', [
              TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None)
            ], TypeRef.non_null_list('Swap')),
            selection=[
              Selection(
                fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
              )
            ]
          ),
        ]
      ),
      Selection(
        fmeta=TypeMeta.FieldMeta('swaps', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None)
        ], TypeRef.non_null_list('Swap')),
        selection=[
          Selection(
            fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
          )
        ]
      ),
    ]
  ))


def doc3():
  """
  query {
    pairs(first: 455) {
      id
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[Selection(
      fmeta=TypeMeta.FieldMeta('pairs', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
      ], TypeRef.non_null_list('Pair')),
      arguments=[
        Argument('first', InputValue.Int(455)),
      ],
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
        )
      ]
    )]
  ))


def doc4():
  """
  query {
    pairs(skip: 10) {
      id
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[Selection(
      fmeta=TypeMeta.FieldMeta('pairs', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
      ], TypeRef.non_null_list('Pair')),
      arguments=[
        Argument('skip', InputValue.Int(10)),
      ],
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
        )
      ]
    )]
  ))


def doc5():
  """
  query {
    pairs(orderBy: createdAtTimestamp) {
      id
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[Selection(
      fmeta=TypeMeta.FieldMeta('pairs', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
      ], TypeRef.non_null_list('Pair')),
      arguments=[
        Argument('orderBy', InputValue.Enum('createdAtTimestamp')),
      ],
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
        )
      ]
    )]
  ))


def doc6():
  """
  query {
    pairs(orderDirection: desc) {
      id
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[Selection(
      fmeta=TypeMeta.FieldMeta('pairs', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
      ], TypeRef.non_null_list('Pair')),
      arguments=[
        Argument('orderDirection', InputValue.Enum('desc')),
      ],
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
        )
      ]
    )]
  ))


def doc7():
  """
  query {
    pairs(
      orderBy: createdAtTimestamp,
      orderDirection: desc
    ) {
      id
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[Selection(
      fmeta=TypeMeta.FieldMeta('pairs', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
      ], TypeRef.non_null_list('Pair')),
      arguments=[
        Argument('orderBy', InputValue.Enum('createdAtTimestamp')),
        Argument('orderDirection', InputValue.Enum('desc')),
      ],
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
        )
      ]
    )]
  ))


def doc8():
  """
  query {
    pairs {
      foo {
        swaps {
          id
        }
      }
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[Selection(
      fmeta=TypeMeta.FieldMeta('pairs', '', [
        TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
        TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
      ], TypeRef.non_null_list('Pair')),
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('foo', '', [], TypeRef.Named('Foo')),
          selection=[
            Selection(
              fmeta=TypeMeta.FieldMeta('swaps', '', [
                TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
                TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
                TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None)
              ], TypeRef.non_null_list('Swap')),
              selection=[
                Selection(
                  fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
                )
              ]
            )
          ]
        )
      ]
    )]
  ))


def doc9():
  """
  query {
    pair(id: "0x786b582a0bbcac5d192d7e039f49c116ac5f05a8") {
      id
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[Selection(
      fmeta=TypeMeta.FieldMeta('pair', '', [
        TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None)
      ], TypeRef.Named('Pair')),
      arguments=[
        Argument('id', InputValue.String('0x786b582a0bbcac5d192d7e039f49c116ac5f05a8'))
      ],
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String')),
          selection=[]
        )
      ]
    )]
  ))


def doc10():
  """
  query {
    pair(id: "0x786b582a0bbcac5d192d7e039f49c116ac5f05a8") {
      swaps {
        id
      }
    }
  }
  """
  return Document(url='www.abc.xyz/graphql', query=Query(
    name=None,
    selection=[Selection(
      fmeta=TypeMeta.FieldMeta('pair', '', [
        TypeMeta.ArgumentMeta('id', '', TypeRef.Named('String'), None)
      ], TypeRef.Named('Pair')),
      arguments=[
        Argument('id', InputValue.String('0x786b582a0bbcac5d192d7e039f49c116ac5f05a8'))
      ],
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('swaps', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None)
          ], TypeRef.non_null_list('Swap')),
          selection=[
            Selection(
              fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
            )
          ]
        )
      ]
    )]
  ))