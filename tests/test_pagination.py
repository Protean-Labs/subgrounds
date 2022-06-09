import pytest

from subgrounds.pagination import (Cursor, PaginationNode, merge,
                                   preprocess_document)
from subgrounds.query import (Argument, Document, InputValue, Query, Selection,
                              VariableDefinition)
from subgrounds.schema import TypeMeta, TypeRef
from tests.utils import *


@pytest.mark.parametrize("test_input, expected", [
  # Test normalization with no args specified, no nested lists
  (
    Document(url='www.abc.xyz/graphql', query=Query(
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
    )),
    (
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
      )),
      [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [])
      ]
    )
  ),
  # Test normalization with no args specified, with 1 nested list
  (
    Document(url='www.abc.xyz/graphql', query=Query(
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
    )),
    (
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
      )),
      [PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'swaps'], [])
      ])]
    )
  ),
  # Test normalization with no args specified, with 1 nested list, 1 neighbor list
  (
    Document(url='www.abc.xyz/graphql', query=Query(
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
    )),
    (
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
      )),
      [
        PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
          PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'swaps'], [])
        ]),
        PaginationNode(2, 'id', 100, 0, None, TypeRef.Named('String'), ['swaps'], [])
      ]
    )
  ),
  # Test normalization with `first` arg specified, no nested lists
  (
    Document(url='www.abc.xyz/graphql', query=Query(
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
    )),
    (
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
      )),
      [PaginationNode(0, 'id', 455, 0, None, TypeRef.Named('String'), ['pairs'], [])]
    )
  ),
  # Test normalization with `skip` arg specified, no nested lists
  (
    Document(url='www.abc.xyz/graphql', query=Query(
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
    )),
    (
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
      )),
      [
        PaginationNode(0, 'id', 100, 10, None, TypeRef.Named('String'), ['pairs'], [])
      ]
    )
  ),
  # Test normalization with `orderBy` arg specified, no nested lists
  (
    Document(url='www.abc.xyz/graphql', query=Query(
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
    )),
    (
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
      )),
      [
        PaginationNode(0, 'createdAtTimestamp', 100, 0, None, TypeRef.Named('BigInt'), ['pairs'], [])
      ]
    )
  ),
  # Test normalization with `orderDirection` arg specified, no nested lists
  (
    Document(url='www.abc.xyz/graphql', query=Query(
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
    )),
    (
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
      )),
      [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [])
      ]
    )
  ),
  # Test normalization with `orderDirection` and `orderBy` args specified, no nested lists
  (
    Document(url='www.abc.xyz/graphql', query=Query(
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
    )),
    (
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
      )),
      [PaginationNode(0, 'createdAtTimestamp', 100, 0, None, TypeRef.Named('BigInt'), ['pairs'], [])]
    )
  ),
  # Test normalization with no args specified, 1 nested lists 1 level down
  (
    Document(url='www.abc.xyz/graphql', query=Query(
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
    )),
    (
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
      )),
      [
        PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
          PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'foo', 'swaps'], [])
        ])
      ]
    )
  )
])
def test_normalize_doc(schema, test_input, expected):
  assert preprocess_document(schema, test_input) == expected


def gen_swaps(pair_id, n):
  for i in range(n):
    yield {'id': f'swap_{pair_id}{i}', 'timestamp': i}


def gen_users(pair_id, n):
  for i in range(n):
    yield {'id': f'user_{pair_id}{i}', 'volume': i}


# TODO: Add assertion to check cursor state contained in StopIteration exception
@pytest.mark.parametrize("data_and_stop, page_node, expected", [
  # Test cursor, 1 pagination node, no args, 2 pages
  (
    [
      ({'swaps': list(gen_swaps('a', 900))}, False),
      ({'swaps': []}, True)
    ],
    PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=1100,
      skip_value=0,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    ),
    [
      {'first0': 900, 'skip0': 0},
      {'first0': 200, 'skip0': 0, 'lastOrderingValue0': 'swap_a899'}
    ]
  ),
  # Test cursor, 1 pagination node, no args, 1 page
  (
    [
      ({'swaps': list(gen_swaps('a', 10))}, True),
    ],
    PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=1100,
      skip_value=0,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    ),
    [
      {'first0': 900, 'skip0': 0},
    ]
  ),
  # Test cursor, 1 pagination node, no args, 1 page below limit
  (
    [
      ({'swaps': list(gen_swaps('a', 100))}, True),
    ],
    PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=100,
      skip_value=0,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    ),
    [
      {'first0': 100, 'skip0': 0},
    ]
  ),
  # Test cursor, nested pagination nodes, no args
  (
    [
      ({'pairs': [{'id': 'a', 'swaps': list(gen_swaps('a', 900))}]}, False),
      ({'pairs': [{'id': 'a', 'swaps': list(gen_swaps('a', 100))}]}, False),
      ({'pairs': [{'id': 'b', 'swaps': list(gen_swaps('b', 100))}]}, False),
      ({'pairs': [{'id': 'c', 'swaps': list(gen_swaps('c', 10))}]}, False),
      ({'pairs': [{'id': 'd', 'swaps': list(gen_swaps('d', 0))}]}, True),
    ],
    PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=4,
      skip_value=0,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['pairs'],
      inner=[
        PaginationNode(
          node_idx=1,
          filter_field='timestamp',
          first_value=7000,
          skip_value=0,
          filter_value=None,
          filter_value_type=TypeRef.Named('BigInt'),
          key_path=['pairs', 'swaps'],
          inner=[]
        )
      ]
    ),
    [
      {'first0': 1, 'skip0': 0, 'first1': 900, 'skip1': 0},
      {'first0': 1, 'skip0': 0, 'first1': 900, 'skip1': 0, 'lastOrderingValue1': 899},
      {'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'a', 'first1': 900, 'skip1': 0},
      {'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'b', 'first1': 900, 'skip1': 0},
      {'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'c', 'first1': 900, 'skip1': 0},
    ]
  ),
  # Test cursor, nested pagination nodes with neighbors, no args
  (
    [
      ({'pairs': [{'id': 'a', 'swaps': list(gen_swaps('a', 900))}]}, False),
      ({'pairs': [{'id': 'a', 'swaps': list(gen_swaps('a', 100))}]}, False),
      ({'pairs': [{'id': 'a', 'users': list(gen_users('a', 9))}]}, False),
      ({'pairs': [{'id': 'b', 'swaps': list(gen_swaps('b', 100))}]}, False),
      ({'pairs': [{'id': 'b', 'users': list(gen_users('b', 9))}]}, False),
      ({'pairs': [{'id': 'c', 'swaps': list(gen_swaps('c', 10))}]}, False),
      ({'pairs': [{'id': 'c', 'users': list(gen_users('c', 10))}]}, False),
      ({'pairs': [{'id': 'd', 'swaps': list(gen_swaps('d', 0))}]}, False),
      ({'pairs': [{'id': 'd', 'users': list(gen_users('d', 5))}]}, True),
    ],
    PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=4,
      skip_value=0,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['pairs'],
      inner=[
        PaginationNode(
          node_idx=1,
          filter_field='timestamp',
          first_value=7000,
          skip_value=0,
          filter_value=None,
          filter_value_type=TypeRef.Named('BigInt'),
          key_path=['pairs', 'swaps'],
          inner=[],
        ),
        PaginationNode(
          node_idx=2,
          filter_field='volume',
          first_value=10,
          skip_value=0,
          filter_value=None,
          filter_value_type=TypeRef.Named('BigInt'),
          key_path=['pairs', 'users'],
          inner=[],
        ),
      ]
    ),
    [
      {'first0': 1, 'skip0': 0, 'first1': 900, 'skip1': 0},
      {'first0': 1, 'skip0': 0, 'first1': 900, 'skip1': 0, 'lastOrderingValue1': 899},
      {'first0': 1, 'skip0': 0, 'first2': 10, 'skip2': 0},
      {'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'a', 'first1': 900, 'skip1': 0},
      {'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'a', 'first2': 10, 'skip2': 0},
      {'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'b', 'first1': 900, 'skip1': 0},
      {'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'b', 'first2': 10, 'skip2': 0},
      {'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'c', 'first1': 900, 'skip1': 0},
      {'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'c', 'first2': 10, 'skip2': 0},
    ]
  ),
  # Test cursor, 1 pagination node, `skip` argument specified
  (
    [
      ({'swaps': list(gen_swaps('a', 900))}, False),
      ({'swaps': list(gen_swaps('a', 500))}, True),
    ],
    PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=1500,
      skip_value=10,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    ),
    [
      {'first0': 900, 'skip0': 10},
      {'first0': 600, 'skip0': 0, 'lastOrderingValue0': 'swap_a899'},
    ]
  ),
  # Test cursor, 1 pagination node, `where` argument specified
  (
    [
      ({'swaps': list(gen_swaps('a', 900))}, False),
      ({'swaps': list(gen_swaps('a', 500))}, True),
    ],
    PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=1500,
      skip_value=0,
      filter_value='0',
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    ),
    [
      {'first0': 900, 'skip0': 0, 'lastOrderingValue0': '0'},
      {'first0': 600, 'skip0': 0, 'lastOrderingValue0': 'swap_a899'},
    ]
  )
])
def test_pagination_cursor(data_and_stop, page_node, expected):
  cursor = Cursor(page_node)

  for args, (data, stop) in zip(expected, data_and_stop):
    assert cursor.args() == args

    if stop:
      with pytest.raises(StopIteration):
        cursor.step(data)

    else:
      cursor.step(data)


# TODO: Add tests for `subgrounds.pagination.trim_document`

@pytest.mark.parametrize("data1, data2, expected", [
  (
    {},
    {
      'pairs': [
        {'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ]
    },
    {
      'pairs': [
        {'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ]
    }
  ),
  (
    {
      'pairs': [
        {'id': 'p0', 'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'id': 'p1', 'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'id': 'p2', 'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ]
    },
    {
      'pairs': [
        {'id': 'p3', 'swaps': [
          {'id': 'D1'},
          {'id': 'D2'},
          {'id': 'D3'},
          {'id': 'D4'},
        ]},
        {'id': 'p4', 'swaps': [
          {'id': 'E1'},
          {'id': 'E2'},
          {'id': 'E3'},
          {'id': 'E4'},
        ]},
        {'id': 'p5', 'swaps': [
          {'id': 'F1'},
          {'id': 'F2'},
          {'id': 'F3'},
          {'id': 'F4'},
          {'id': 'F5'},
        ]},
      ]
    },
    {
      'pairs': [
        {'id': 'p0', 'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'id': 'p1', 'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'id': 'p2', 'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
        {'id': 'p3', 'swaps': [
          {'id': 'D1'},
          {'id': 'D2'},
          {'id': 'D3'},
          {'id': 'D4'},
        ]},
        {'id': 'p4', 'swaps': [
          {'id': 'E1'},
          {'id': 'E2'},
          {'id': 'E3'},
          {'id': 'E4'},
        ]},
        {'id': 'p5', 'swaps': [
          {'id': 'F1'},
          {'id': 'F2'},
          {'id': 'F3'},
          {'id': 'F4'},
          {'id': 'F5'},
        ]},
      ]
    }
  ),
  (
    {
      'pairs': [
        {'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ]
    },
    {
      'mints': [
        {'id': 'M1'},
        {'id': 'M2'},
        {'id': 'M3'},
        {'id': 'M4'},
        {'id': 'M5'},
      ]
    },
    {
      'pairs': [
        {'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ],
      'mints': [
        {'id': 'M1'},
        {'id': 'M2'},
        {'id': 'M3'},
        {'id': 'M4'},
        {'id': 'M5'},
      ]
    }
  ),
  (
    {
      'pairs': [
        {'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ],
      'mints': [
        {'id': 'M1'},
        {'id': 'M2'},
        {'id': 'M3'},
        {'id': 'M4'},
        {'id': 'M5'},
      ]
    },
    {
      'mints': [
        {'id': 'N1'},
        {'id': 'N2'},
        {'id': 'N3'},
        {'id': 'N4'},
        {'id': 'N5'},
      ]
    },
    {
      'pairs': [
        {'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ],
      'mints': [
        {'id': 'M1'},
        {'id': 'M2'},
        {'id': 'M3'},
        {'id': 'M4'},
        {'id': 'M5'},
        {'id': 'N1'},
        {'id': 'N2'},
        {'id': 'N3'},
        {'id': 'N4'},
        {'id': 'N5'},
      ]
    }
  ),
  (
    {
      'token': {
        'symbol': 'USDC'
      },
      'pairs': [
        {'id': 'p0', 'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'id': 'p1', 'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'id': 'p2', 'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ]
    },
    {
      'token': {
        'symbol': 'USDC'
      },
      'pairs': [
        {'id': 'p3', 'swaps': [
          {'id': 'D1'},
          {'id': 'D2'},
          {'id': 'D3'},
          {'id': 'D4'},
        ]},
        {'id': 'p4', 'swaps': [
          {'id': 'E1'},
          {'id': 'E2'},
          {'id': 'E3'},
          {'id': 'E4'},
        ]},
        {'id': 'p5', 'swaps': [
          {'id': 'F1'},
          {'id': 'F2'},
          {'id': 'F3'},
          {'id': 'F4'},
          {'id': 'F5'},
        ]},
      ]
    },
    {
      'token': {
        'symbol': 'USDC'
      },
      'pairs': [
        {'id': 'p0', 'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'id': 'p1', 'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'id': 'p2', 'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
        {'id': 'p3', 'swaps': [
          {'id': 'D1'},
          {'id': 'D2'},
          {'id': 'D3'},
          {'id': 'D4'},
        ]},
        {'id': 'p4', 'swaps': [
          {'id': 'E1'},
          {'id': 'E2'},
          {'id': 'E3'},
          {'id': 'E4'},
        ]},
        {'id': 'p5', 'swaps': [
          {'id': 'F1'},
          {'id': 'F2'},
          {'id': 'F3'},
          {'id': 'F4'},
          {'id': 'F5'},
        ]},
      ]
    }
  ),
  (
    {
      'pairs': [
        {'id': 'p0', 'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'id': 'p1', 'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'id': 'p2', 'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ]
    },
    {
      'pairs': []
    },
    {
      'pairs': [
        {'id': 'p0', 'swaps': [
          {'id': 'A1'},
          {'id': 'A2'},
          {'id': 'A3'},
          {'id': 'A4'},
        ]},
        {'id': 'p1', 'swaps': [
          {'id': 'B1'},
          {'id': 'B2'},
          {'id': 'B3'},
          {'id': 'B4'},
        ]},
        {'id': 'p2', 'swaps': [
          {'id': 'C1'},
          {'id': 'C2'},
          {'id': 'C3'},
          {'id': 'C4'},
          {'id': 'C5'},
        ]},
      ]
    }
  ),
  (
    {
      'pairs': [
        {
          'id': 'abc',
          'swaps': [
            {'id': 'A1'},
            {'id': 'A2'},
            {'id': 'A3'},
            {'id': 'A4'},
          ]
        },
        {
          'id': 'xyz',
          'swaps': [
            {'id': 'B1'},
            {'id': 'B2'},
            {'id': 'B3'},
            {'id': 'B4'},
          ]
        },
      ]
    },
    {
      'pairs': [
        {
          'id': 'abc',
          'mints': [
            {'id': 'M1'},
            {'id': 'M2'},
            {'id': 'M3'},
            {'id': 'M4'},
          ]
        },
        {
          'id': 'xyz',
          'mints': [
            {'id': 'N1'},
            {'id': 'N2'},
            {'id': 'N3'},
            {'id': 'N4'},
          ]
        },
      ]
    },
    {
      'pairs': [
        {
          'id': 'abc',
          'swaps': [
            {'id': 'A1'},
            {'id': 'A2'},
            {'id': 'A3'},
            {'id': 'A4'},
          ],
          'mints': [
            {'id': 'M1'},
            {'id': 'M2'},
            {'id': 'M3'},
            {'id': 'M4'},
          ]
        },
        {
          'id': 'xyz',
          'swaps': [
            {'id': 'B1'},
            {'id': 'B2'},
            {'id': 'B3'},
            {'id': 'B4'},
          ],
          'mints': [
            {'id': 'N1'},
            {'id': 'N2'},
            {'id': 'N3'},
            {'id': 'N4'},
          ]
        },
      ]
    }
  )
])
def test_merge_pages(data1, data2, expected):
  assert merge(data1, data2) == expected
