import pytest
from subgrounds.pagination.preprocess import PaginationNode, generate_pagination_nodes, normalize
from subgrounds.query import Argument, Document, InputValue, Query, Selection, VariableDefinition
from subgrounds.schema import SchemaMeta, TypeMeta, TypeRef


import tests.queries as queries


@pytest.mark.parametrize(['document', 'expected'], [
  (
    queries.doc0(),
    [
      PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [])
    ]
  ),
  (
    queries.doc1(),
    [
      PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'swaps'], [])
      ])
    ]
  ),
  (
    queries.doc2(),
    [
      PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'swaps'], [])
      ]),
      PaginationNode(2, 'id', 100, 0, None, TypeRef.Named('String'), ['swaps'], [])
    ]
  ),
  (
    queries.doc3(),
    [
      PaginationNode(0, 'id', 455, 0, None, TypeRef.Named('String'), ['pairs'], [])
    ]
  ),
  (
    queries.doc4(),
    [
      PaginationNode(0, 'id', 100, 10, None, TypeRef.Named('String'), ['pairs'], [])
    ]
  ),
  (
    queries.doc5(),
    [
      PaginationNode(0, 'createdAtTimestamp', 100, 0, None, TypeRef.Named('BigInt'), ['pairs'], [])
    ]
  ),
  (
    queries.doc6(),
    [
      PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [])
    ]
  ),
  (
    queries.doc7(),
    [
      PaginationNode(0, 'createdAtTimestamp', 100, 0, None, TypeRef.Named('BigInt'), ['pairs'], [])
    ]
  ),
  (
    queries.doc8(),
    [
      PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'foo', 'swaps'], [])
      ])
    ]
  ),
  (
    queries.doc9(),
    []
  ),
  (
    queries.doc10(),
    [
      PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pair', 'swaps'], [])
    ]
  )
])
def test_gen_pagination_nodes(
  schema: SchemaMeta,
  document: Document,
  expected: PaginationNode
):
  assert generate_pagination_nodes(schema, document) == expected


@pytest.mark.parametrize(['document', 'pagination_nodes', 'expected'], [
  (
    queries.doc0(),
    [
      PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        arguments=[
          Argument('first', InputValue.Variable('first0')),
          Argument('skip', InputValue.Variable('skip0')),
          Argument('orderBy', InputValue.Enum('id')),
          Argument('orderDirection', InputValue.Enum('asc')),
          Argument('where', InputValue.Object({
            'id_gt': InputValue.Variable('lastOrderingValue0')
          })),
        ],
        selection=[
          Selection(
            fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
          )
        ]
      )],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('String')),
      ]
    ))
  ),
  (
    queries.doc1(),
    [
      PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'swaps'], [])
      ])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        arguments=[
          Argument('first', InputValue.Variable('first1')),
          Argument('skip', InputValue.Variable('skip1')),
          Argument('orderBy', InputValue.Enum('id')),
          Argument('orderDirection', InputValue.Enum('asc')),
          Argument('where', InputValue.Object({
            'id_gt': InputValue.Variable('lastOrderingValue1')
          })),
        ],
        selection=[
          Selection(
            fmeta=TypeMeta.FieldMeta('swaps', '', [
              TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
              TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None)
            ], TypeRef.non_null_list('Swap')),
            arguments=[
              Argument('first', InputValue.Variable('first0')),
              Argument('skip', InputValue.Variable('skip0')),
              Argument('orderBy', InputValue.Enum('id')),
              Argument('orderDirection', InputValue.Enum('asc')),
              Argument('where', InputValue.Object({
                'id_gt': InputValue.Variable('lastOrderingValue0')
              })),
            ],
            selection=[
              Selection(
                fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
              )
            ]
          ),
          Selection(fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
        ]
      )],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('String')),
        VariableDefinition('first1', TypeRef.Named('Int')),
        VariableDefinition('skip1', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue1', TypeRef.Named('String')),
      ]
    ))
  ),
  (
    queries.doc2(),
    [
      PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'swaps'], [])
      ]),
      PaginationNode(2, 'id', 100, 0, None, TypeRef.Named('String'), ['swaps'], [])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[
        Selection(
          fmeta=TypeMeta.FieldMeta('pairs', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
          ], TypeRef.non_null_list('Pair')),
          arguments=[
            Argument('first', InputValue.Variable('first1')),
            Argument('skip', InputValue.Variable('skip1')),
            Argument('orderBy', InputValue.Enum('id')),
            Argument('orderDirection', InputValue.Enum('asc')),
            Argument('where', InputValue.Object({
              'id_gt': InputValue.Variable('lastOrderingValue1')
            })),
          ],
          selection=[
            Selection(
              fmeta=TypeMeta.FieldMeta('swaps', '', [
                TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
                TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
                TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None)
              ], TypeRef.non_null_list('Swap')),
              arguments=[
                Argument('first', InputValue.Variable('first0')),
                Argument('skip', InputValue.Variable('skip0')),
                Argument('orderBy', InputValue.Enum('id')),
                Argument('orderDirection', InputValue.Enum('asc')),
                Argument('where', InputValue.Object({
                  'id_gt': InputValue.Variable('lastOrderingValue0')
                })),
              ],
              selection=[
                Selection(
                  fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
                )
              ]
            ),
            Selection(fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
          ]
        ),
        Selection(
          fmeta=TypeMeta.FieldMeta('swaps', '', [
            TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
            TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Swap_filter'), None)
          ], TypeRef.non_null_list('Swap')),
          arguments=[
            Argument('first', InputValue.Variable('first2')),
            Argument('skip', InputValue.Variable('skip2')),
            Argument('orderBy', InputValue.Enum('id')),
            Argument('orderDirection', InputValue.Enum('asc')),
            Argument('where', InputValue.Object({
              'id_gt': InputValue.Variable('lastOrderingValue2')
            })),
          ],
          selection=[
            Selection(
              fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
            )
          ]
        ),
      ],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('String')),
        VariableDefinition('first1', TypeRef.Named('Int')),
        VariableDefinition('skip1', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue1', TypeRef.Named('String')),
        VariableDefinition('first2', TypeRef.Named('Int')),
        VariableDefinition('skip2', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue2', TypeRef.Named('String')),
      ]
    ))
  ),
  (
    queries.doc3(),
    [
      PaginationNode(0, 'id', 455, 0, None, TypeRef.Named('String'), ['pairs'], [])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        arguments=[
          Argument('first', InputValue.Variable('first0')),
          Argument('skip', InputValue.Variable('skip0')),
          Argument('orderBy', InputValue.Enum('id')),
          Argument('orderDirection', InputValue.Enum('asc')),
          Argument('where', InputValue.Object({
            'id_gt': InputValue.Variable('lastOrderingValue0')
          })),
        ],
        selection=[
          Selection(
            fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
          )
        ]
      )],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('String')),
      ]
    ))
  ),
  (
    queries.doc4(),
    [
      PaginationNode(0, 'id', 100, 10, None, TypeRef.Named('String'), ['pairs'], [])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        arguments=[
          Argument('first', InputValue.Variable('first0')),
          Argument('skip', InputValue.Variable('skip0')),
          Argument('orderBy', InputValue.Enum('id')),
          Argument('orderDirection', InputValue.Enum('asc')),
          Argument('where', InputValue.Object({
            'id_gt': InputValue.Variable('lastOrderingValue0')
          })),
        ],
        selection=[
          Selection(
            fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
          )
        ]
      )],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('String')),
      ]
    ))
  ),
  (
    queries.doc5(),
    [
      PaginationNode(0, 'createdAtTimestamp', 100, 0, None, TypeRef.Named('BigInt'), ['pairs'], [])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        arguments=[
          Argument('first', InputValue.Variable('first0')),
          Argument('skip', InputValue.Variable('skip0')),
          Argument('orderBy', InputValue.Enum('createdAtTimestamp')),
          Argument('orderDirection', InputValue.Enum('asc')),
          Argument('where', InputValue.Object({
            'createdAtTimestamp_gt': InputValue.Variable('lastOrderingValue0')
          })),
        ],
        selection=[
          Selection(fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
          Selection(fmeta=TypeMeta.FieldMeta('createdAtTimestamp', '', [], TypeRef.Named('BigInt')))
        ]
      )],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('BigInt')),
      ]
    ))
  ),
  (
    queries.doc6(),
    [
      PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        arguments=[
          Argument('first', InputValue.Variable('first0')),
          Argument('skip', InputValue.Variable('skip0')),
          Argument('orderBy', InputValue.Enum('id')),
          Argument('orderDirection', InputValue.Enum('desc')),
          Argument('where', InputValue.Object({
            'id_lt': InputValue.Variable('lastOrderingValue0')
          })),
        ],
        selection=[
          Selection(fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
        ]
      )],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('String')),
      ]
    ))
  ),
  (
    queries.doc7(),
    [
      PaginationNode(0, 'createdAtTimestamp', 100, 0, None, TypeRef.Named('BigInt'), ['pairs'], [])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        arguments=[
          Argument('first', InputValue.Variable('first0')),
          Argument('skip', InputValue.Variable('skip0')),
          Argument('orderBy', InputValue.Enum('createdAtTimestamp')),
          Argument('orderDirection', InputValue.Enum('desc')),
          Argument('where', InputValue.Object({
            'createdAtTimestamp_lt': InputValue.Variable('lastOrderingValue0')
          })),
        ],
        selection=[
          Selection(fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
          Selection(fmeta=TypeMeta.FieldMeta('createdAtTimestamp', '', [], TypeRef.Named('BigInt')))
        ]
      )],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('BigInt')),
      ]
    ))
  ),
  (
    queries.doc8(),
    [
      PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'foo', 'swaps'], [])
      ])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
      name=None,
      selection=[Selection(
        fmeta=TypeMeta.FieldMeta('pairs', '', [
          TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
          TypeMeta.ArgumentMeta('where', '', TypeRef.Named('Pair_filter'), None)
        ], TypeRef.non_null_list('Pair')),
        arguments=[
          Argument('first', InputValue.Variable('first1')),
          Argument('skip', InputValue.Variable('skip1')),
          Argument('orderBy', InputValue.Enum('id')),
          Argument('orderDirection', InputValue.Enum('asc')),
          Argument('where', InputValue.Object({
            'id_gt': InputValue.Variable('lastOrderingValue1')
          })),
        ],
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
                arguments=[
                  Argument('first', InputValue.Variable('first0')),
                  Argument('skip', InputValue.Variable('skip0')),
                  Argument('orderBy', InputValue.Enum('id')),
                  Argument('orderDirection', InputValue.Enum('asc')),
                  Argument('where', InputValue.Object({
                    'id_gt': InputValue.Variable('lastOrderingValue0')
                  })),
                ],
                selection=[
                  Selection(
                    fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
                  )
                ]
              )
            ]
          ),
          Selection(fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))),
        ]
      )],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('String')),
        VariableDefinition('first1', TypeRef.Named('Int')),
        VariableDefinition('skip1', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue1', TypeRef.Named('String')),
      ]
    ))
  ),
  (
    queries.doc10(),
    [
      PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pair', 'swaps'], [])
    ],
    Document(url='www.abc.xyz/graphql', query=Query(
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
            arguments=[
              Argument('first', InputValue.Variable('first0')),
              Argument('skip', InputValue.Variable('skip0')),
              Argument('orderBy', InputValue.Enum('id')),
              Argument('orderDirection', InputValue.Enum('asc')),
              Argument('where', InputValue.Object({
                'id_gt': InputValue.Variable('lastOrderingValue0')
              })),
            ],
            selection=[
              Selection(
                fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
              )
            ]
          )
        ]
      )],
      variables=[
        VariableDefinition('first0', TypeRef.Named('Int')),
        VariableDefinition('skip0', TypeRef.Named('Int')),
        VariableDefinition('lastOrderingValue0', TypeRef.Named('String')),
      ]
    ))
  )
])
def test_normalize_doc(
  schema: SchemaMeta,
  document: Document,
  pagination_nodes: list[PaginationNode],
  expected: PaginationNode
):
  assert normalize(schema, document, pagination_nodes) == expected
