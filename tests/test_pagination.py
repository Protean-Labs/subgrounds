import unittest

from subgrounds.pagination.pagination import (
  Cursor,
  PaginationNode,
  generate_pagination_nodes,
  normalize,
  merge
)
from subgrounds.pagination.strategies import greedy_strategy, legacy_strategy
from subgrounds.query import Argument, Document, InputValue, Query, Selection, VariableDefinition
from subgrounds.schema import TypeMeta, TypeRef

from tests.utils import schema


class TestPaginationNode(unittest.TestCase):
  def test_gen_pagenodes_noargs(self):
    doc = Document(url='www.abc.xyz/graphql', query=Query(
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
    
    expected = [
      PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [])
    ]

    self.assertEqual(generate_pagination_nodes(schema(), doc), expected)

  def test_normalize_doc_no_args_nested_1(self):
    expected = [
      PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'swaps'], [])
      ])
    ]

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    self.assertEqual(generate_pagination_nodes(schema(), doc), expected)

  def test_normalize_doc_no_args_nested_2(self):
    expected = [
      PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'swaps'], [])
      ]),
      PaginationNode(2, 'id', 100, 0, None, TypeRef.Named('String'), ['swaps'], [])
    ]

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    self.assertEqual(generate_pagination_nodes(schema(), doc), expected)

  def test_normalize_doc_first_arg(self):
    expected = [
      PaginationNode(0, 'id', 455, 0, None, TypeRef.Named('String'), ['pairs'], [])
    ]

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    self.assertEqual(generate_pagination_nodes(schema(), doc), expected)

  def test_normalize_doc_skip_arg(self):
    expected = [
      PaginationNode(0, 'id', 100, 10, None, TypeRef.Named('String'), ['pairs'], [])
    ]

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    self.assertEqual(generate_pagination_nodes(schema(), doc), expected)

  def test_normalize_doc_orderby_arg(self):
    expected = [
      PaginationNode(0, 'createdAtTimestamp', 100, 0, None, TypeRef.Named('BigInt'), ['pairs'], [])
    ]

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    self.assertEqual(generate_pagination_nodes(schema(), doc), expected)

  def test_normalize_doc_orderdir_arg(self):
    expected = [
      PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [])
    ]

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    self.assertEqual(generate_pagination_nodes(schema(), doc), expected)

  def test_normalize_doc_orderby_orderdir_arg(self):
    expected = [
      PaginationNode(0, 'createdAtTimestamp', 100, 0, None, TypeRef.Named('BigInt'), ['pairs'], [])
    ]

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    self.assertEqual(generate_pagination_nodes(schema(), doc), expected)

  def test_normalize_doc_key_path(self):
    expected = [
      PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs'], [
        PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('String'), ['pairs', 'foo', 'swaps'], [])
      ])
    ]

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    self.assertEqual(generate_pagination_nodes(schema(), doc), expected)

class TestNormalizeDocument(unittest.TestCase):
  def test_normalize_doc_no_args(self):
    expected = Document(url='www.abc.xyz/graphql', query=Query(
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

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    schema_ = schema()
    page_nodes = generate_pagination_nodes(schema_, doc)
    self.assertEqual(normalize(schema_, doc, page_nodes), expected)

  def test_normalize_doc_no_args_nested_1(self):
    expected = Document(url='www.abc.xyz/graphql', query=Query(
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

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    schema_ = schema()
    page_nodes = generate_pagination_nodes(schema_, doc)
    self.assertEqual(normalize(schema_, doc, page_nodes), expected)

  def test_normalize_doc_no_args_nested_2(self):
    expected = Document(url='www.abc.xyz/graphql', query=Query(
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

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    schema_ = schema()
    page_nodes = generate_pagination_nodes(schema_, doc)
    # import pprint
    # pprint.pp(normalize(schema_, doc, page_nodes))
    self.assertEqual(normalize(schema_, doc, page_nodes), expected)


  def test_normalize_doc_first_arg(self):
    expected = Document(url='www.abc.xyz/graphql', query=Query(
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

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    schema_ = schema()
    page_nodes = generate_pagination_nodes(schema_, doc)
    self.assertEqual(normalize(schema_, doc, page_nodes), expected)

  def test_normalize_doc_skip_arg(self):
    expected = Document(url='www.abc.xyz/graphql', query=Query(
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

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    schema_ = schema()
    page_nodes = generate_pagination_nodes(schema_, doc)
    self.assertEqual(normalize(schema_, doc, page_nodes), expected)

  def test_normalize_doc_orderby_arg(self):
    expected = Document(url='www.abc.xyz/graphql', query=Query(
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

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    schema_ = schema()
    page_nodes = generate_pagination_nodes(schema_, doc)
    self.assertEqual(normalize(schema_, doc, page_nodes), expected)

  def test_normalize_doc_orderdir_arg(self):
    expected = Document(url='www.abc.xyz/graphql', query=Query(
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

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    schema_ = schema()
    page_nodes = generate_pagination_nodes(schema_, doc)
    self.assertEqual(normalize(schema_, doc, page_nodes), expected)

  def test_normalize_doc_orderby_orderdir_arg(self):
    expected = Document(url='www.abc.xyz/graphql', query=Query(
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

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    schema_ = schema()
    page_nodes = generate_pagination_nodes(schema_, doc)
    self.assertEqual(normalize(schema_, doc, page_nodes), expected)

  def test_normalize_doc_key_path(self):
    expected = Document(url='www.abc.xyz/graphql', query=Query(
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

    doc = Document(url='www.abc.xyz/graphql', query=Query(
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

    schema_ = schema()
    page_nodes = generate_pagination_nodes(schema_, doc)
    self.assertEqual(normalize(schema_, doc, page_nodes), expected)


class TestPaginationArgs(unittest.TestCase):
  @staticmethod
  def swaps_gen(pair_id, n):
    for i in range(n):
      yield {'id': f'swap_{pair_id}{i}', 'timestamp': i}

  @staticmethod
  def users_gen(pair_id, n):
    for i in range(n):
      yield {'id': f'user_{pair_id}{i}', 'volume': i}

  def __test_args(self, cursor, strategy, expected, data_and_fails):
    cursor_, args_ = strategy(cursor)
    for ((args, cursor), (data, fails)) in zip(expected, data_and_fails):
      self.assertEqual(cursor_, cursor)
      self.assertEqual(args_, args)

      if fails:
        with self.assertRaises(StopIteration):
          strategy(cursor_, data)
      else:
        cursor_, args_ = strategy(cursor_, data)

  def test_pagination_args_single_node_no_args_2pages(self):
    page_node = PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=1100,
      skip_value=0,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    )

    expected = [
      ({'first0': 900, 'skip0': 0}, Cursor(page_node, [])),
      ({'first0': 200, 'skip0': 0, 'lastOrderingValue0': 'swap_a899'}, Cursor(page_node, [], 0, 'swap_a899', 900, 1, set()))
    ]

    data_and_fails = [
      ({'swaps': list(TestPaginationArgs.swaps_gen('a', 900))}, False),
      ({'swaps': []}, True)
    ]

    cursor = Cursor.from_pagination_nodes(page_node)

    self.__test_args(cursor, greedy_strategy, expected, data_and_fails)

  def test_pagination_args_single_node_no_args_1page(self):
    page_node = PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=1100,
      skip_value=0,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    )

    expected = [
      ({'first0': 900, 'skip0': 0}, Cursor(page_node, [])),
    ]

    data_and_fails = [
      ({'swaps': list(TestPaginationArgs.swaps_gen('a', 10))}, True),
    ]

    cursor = Cursor.from_pagination_nodes(page_node)
    self.__test_args(cursor, greedy_strategy, expected, data_and_fails)

  def test_pagination_args_single_node_no_args_1page_below_limit(self):
    page_node = PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=100,
      skip_value=0,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    )
    
    expected = [
      ({'first0': 100, 'skip0': 0}, Cursor(page_node, [])),
    ]

    data_and_fails = [
      ({'swaps': list(TestPaginationArgs.swaps_gen('a', 100))}, True),
    ]

    cursor = Cursor.from_pagination_nodes(page_node)
    self.__test_args(cursor, greedy_strategy, expected, data_and_fails)

  def test_pagination_args_nested_no_args(self):
    page_node = PaginationNode(
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
    )

    expected = [
      ({'first0': 1, 'skip0': 0, 'first1': 900, 'skip1': 0}, Cursor.from_pagination_nodes(page_node)),
      ({'first0': 1, 'skip0': 0, 'first1': 900, 'skip1': 0, 'lastOrderingValue1': 899}, ),
      ({'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'a', 'first1': 900, 'skip1': 0}, ),
      ({'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'b', 'first1': 900, 'skip1': 0}, ),
      ({'first0': 1, 'skip0': 0, 'lastOrderingValue0': 'c', 'first1': 900, 'skip1': 0}, ),
    ]

    data_and_fails = [
      ({'pairs': [{'id': 'a', 'swaps': list(TestPaginationArgs.swaps_gen('a', 900))}]}, False),
      ({'pairs': [{'id': 'a', 'swaps': list(TestPaginationArgs.swaps_gen('a', 100))}]}, False),
      ({'pairs': [{'id': 'b', 'swaps': list(TestPaginationArgs.swaps_gen('b', 100))}]}, False),
      ({'pairs': [{'id': 'c', 'swaps': list(TestPaginationArgs.swaps_gen('c', 10))}]}, False),
      ({'pairs': [{'id': 'd', 'swaps': list(TestPaginationArgs.swaps_gen('d', 0))}]}, True),
    ]



    cursor = Cursor.from_pagination_nodes(page_node)
    self.__test_args(cursor, legacy_strategy, expected, data_and_fails)

  def test_pagination_args_nested_no_args_2(self):
    expected = [
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

    data_and_fails = [
      ({'pairs': [{'id': 'a', 'swaps': list(TestPaginationArgs.swaps_gen('a', 900))}]}, False),
      ({'pairs': [{'id': 'a', 'swaps': list(TestPaginationArgs.swaps_gen('a', 100))}]}, False),
      ({'pairs': [{'id': 'a', 'users': list(TestPaginationArgs.users_gen('a', 9))}]}, False),
      ({'pairs': [{'id': 'b', 'swaps': list(TestPaginationArgs.swaps_gen('b', 100))}]}, False),
      ({'pairs': [{'id': 'b', 'users': list(TestPaginationArgs.users_gen('b', 9))}]}, False),
      ({'pairs': [{'id': 'c', 'swaps': list(TestPaginationArgs.swaps_gen('c', 10))}]}, False),
      ({'pairs': [{'id': 'c', 'users': list(TestPaginationArgs.users_gen('c', 10))}]}, False),
      ({'pairs': [{'id': 'd', 'swaps': list(TestPaginationArgs.swaps_gen('d', 0))}]}, False),
      ({'pairs': [{'id': 'd', 'users': list(TestPaginationArgs.users_gen('d', 5))}]}, True),
    ]

    page_node = PaginationNode(
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
    )

    arg_gen = Cursor(page_node)

    self.__test_args(arg_gen, expected, data_and_fails)

  def test_pagination_args_single_node_skip_arg(self):
    expected = [
      {'first0': 900, 'skip0': 10},
      {'first0': 600, 'skip0': 0, 'lastOrderingValue0': 'swap_a899'},
    ]

    data_and_fails = [
      ({'swaps': list(TestPaginationArgs.swaps_gen('a', 900))}, False),
      ({'swaps': list(TestPaginationArgs.swaps_gen('a', 500))}, True),
    ]

    page_node = PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=1500,
      skip_value=10,
      filter_value=None,
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    )

    arg_gen = Cursor(page_node)

    self.__test_args(arg_gen, expected, data_and_fails)

  def test_pagination_args_single_node_filter_arg(self):
    expected = [
      {'first0': 900, 'skip0': 0, 'lastOrderingValue0': '0'},
      {'first0': 600, 'skip0': 0, 'lastOrderingValue0': 'swap_a899'},
    ]

    data_and_fails = [
      ({'swaps': list(TestPaginationArgs.swaps_gen('a', 900))}, False),
      ({'swaps': list(TestPaginationArgs.swaps_gen('a', 500))}, True),
    ]

    page_node = PaginationNode(
      node_idx=0,
      filter_field='id',
      first_value=1500,
      skip_value=0,
      filter_value='0',
      filter_value_type=TypeRef.Named('String'),
      key_path=['swaps'],
      inner=[]
    )

    arg_gen = Cursor(page_node)

    self.__test_args(arg_gen, expected, data_and_fails)


class TestMerge(unittest.TestCase):
  def test_merge_empty(self):
    expected = {
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

    data1 = {}
    data2 = {
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

    self.assertEqual(merge(data1, data2), expected)

  def test_merge_overlap(self):
    expected = {
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

    data1 = {
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

    data2 = {
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
    }

    self.assertEqual(merge(data1, data2), expected)

  def test_merge_no_overlap(self):
    expected = {
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

    data1 = {
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

    data2 = {
      'mints': [
        {'id': 'M1'},
        {'id': 'M2'},
        {'id': 'M3'},
        {'id': 'M4'},
        {'id': 'M5'},
      ]
    }

    self.assertEqual(merge(data1, data2), expected)

  def test_merge_partial_overlap(self):
    expected = {
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

    data1 = {
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

    data2 = {
      'mints': [
        {'id': 'N1'},
        {'id': 'N2'},
        {'id': 'N3'},
        {'id': 'N4'},
        {'id': 'N5'},
      ]
    }

    self.assertEqual(merge(data1, data2), expected)

  def test_merge_non_list_data(self):
    expected = {
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

    data1 = {
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
    }

    data2 = {
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
    }
    self.assertEqual(merge(data1, data2), expected)

  def test_merge_empty_list(self):
    expected = {
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

    data1 = {
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

    data2 = {
      'pairs': []
    }

    self.assertEqual(merge(data1, data2), expected)

  def test_merge_union(self):
    expected = {
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

    data1 = {
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
    }

    data2 = {
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
    }

    self.assertEqual(merge(data1, data2), expected)
