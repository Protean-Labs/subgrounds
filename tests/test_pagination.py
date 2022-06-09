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
