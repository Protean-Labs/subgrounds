from pprint import pp
import unittest
from subgrounds.client import query

from subgrounds.pagination import (
  PaginationNode,
  preprocess_document,
  trim_document,
  merge
)
from subgrounds.query import Argument, Document, InputValue, Query, Selection, VariableDefinition
from subgrounds.schema import TypeMeta, TypeRef

from tests.utils import schema


class TestPreprocessDocument(unittest.TestCase):
  def test_pp_doc_no_args(self):
    expected = (
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

    self.assertEqual(preprocess_document(schema(), doc), expected)

  def test_pp_doc_no_args_nested_1(self):
    expected = (
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

    self.assertEqual(preprocess_document(schema(), doc), expected)

  def test_pp_doc_no_args_nested_2(self):
    expected = (
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
        PaginationNode(1, 'id', 100, 0, None, TypeRef.Named('Int'), ['pairs'], [
          PaginationNode(0, 'id', 100, 0, None, TypeRef.Named('Int'), ['pairs', 'swaps'], [])
        ]),
        PaginationNode(2, 'id', 100, 0, None, TypeRef.Named('Int'), ['swaps'], [])
      ]
    )

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

    self.assertEqual(preprocess_document(schema(), doc), expected)

  def test_pp_doc_with_args_1(self):
    expected = (
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

    self.assertEqual(preprocess_document(schema(), doc), expected)

  def test_pp_doc_with_args_2(self):
    expected = (
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
      [PaginationNode(0, 'id', 100, 10, None, TypeRef.Named('String'), ['pairs'], [])]
    )

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

    self.assertEqual(preprocess_document(schema(), doc), expected)

  def test_pp_doc_key_path(self):
    expected = (
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

    self.assertEqual(preprocess_document(schema(), doc), expected)


# class TestPaginationArgs(unittest.TestCase):
#   def test_pagination_args_1(self):
#     expected = [
#       {'first0': 20, 'skip0': 0},
#       {'first0': 20, 'skip0': 20},
#       {'first0': 20, 'skip0': 40},
#       {'first0': 20, 'skip0': 60},
#       {'first0': 20, 'skip0': 80},
#     ]

#     page_nodes = [
#       PaginationNode('first0', 'skip0', [], None, None, [])
#     ]

#     self.assertEqual(pagination_args(page_nodes, 20), expected)

#   def test_pagination_args_2(self):
#     expected = [
#       {'first0': 20, 'skip0': 0},
#       {'first0': 20, 'skip0': 20},
#       {'first0': 20, 'skip0': 40},
#       {'first0': 20, 'skip0': 60},
#       {'first0': 20, 'skip0': 80},
#       {'first1': 20, 'skip1': 0},
#       {'first1': 20, 'skip1': 20},
#       {'first1': 20, 'skip1': 40},
#       {'first1': 20, 'skip1': 60},
#       {'first1': 20, 'skip1': 80},
#     ]

#     page_nodes = [
#       PaginationNode('first0', 'skip0', [], None, None, []),
#       PaginationNode('first1', 'skip1', [], None, None, [])
#     ]

#     self.assertEqual(pagination_args(page_nodes, 20), expected)

#   def test_pagination_args_nested(self):
#     expected = [
#       {'first1': 1, 'skip1': 0, 'first0': 20, 'skip0': 0},
#       {'first1': 1, 'skip1': 0, 'first0': 20, 'skip0': 20},
#       {'first1': 1, 'skip1': 0, 'first0': 20, 'skip0': 40},
#       {'first1': 1, 'skip1': 0, 'first0': 20, 'skip0': 60},
#       {'first1': 1, 'skip1': 0, 'first0': 20, 'skip0': 80},
#       {'first1': 1, 'skip1': 1, 'first0': 20, 'skip0': 0},
#       {'first1': 1, 'skip1': 1, 'first0': 20, 'skip0': 20},
#       {'first1': 1, 'skip1': 1, 'first0': 20, 'skip0': 40},
#       {'first1': 1, 'skip1': 1, 'first0': 20, 'skip0': 60},
#       {'first1': 1, 'skip1': 1, 'first0': 20, 'skip0': 80},
#       {'first1': 1, 'skip1': 2, 'first0': 20, 'skip0': 0},
#       {'first1': 1, 'skip1': 2, 'first0': 20, 'skip0': 20},
#       {'first1': 1, 'skip1': 2, 'first0': 20, 'skip0': 40},
#       {'first1': 1, 'skip1': 2, 'first0': 20, 'skip0': 60},
#       {'first1': 1, 'skip1': 2, 'first0': 20, 'skip0': 80},
#       {'first1': 1, 'skip1': 3, 'first0': 20, 'skip0': 0},
#       {'first1': 1, 'skip1': 3, 'first0': 20, 'skip0': 20},
#       {'first1': 1, 'skip1': 3, 'first0': 20, 'skip0': 40},
#       {'first1': 1, 'skip1': 3, 'first0': 20, 'skip0': 60},
#       {'first1': 1, 'skip1': 3, 'first0': 20, 'skip0': 80},
#       {'first1': 1, 'skip1': 4, 'first0': 20, 'skip0': 0},
#       {'first1': 1, 'skip1': 4, 'first0': 20, 'skip0': 20},
#       {'first1': 1, 'skip1': 4, 'first0': 20, 'skip0': 40},
#       {'first1': 1, 'skip1': 4, 'first0': 20, 'skip0': 60},
#       {'first1': 1, 'skip1': 4, 'first0': 20, 'skip0': 80},
#       {'first2': 20, 'skip2': 0},
#       {'first2': 20, 'skip2': 20},
#       {'first2': 20, 'skip2': 40},
#       {'first2': 20, 'skip2': 60},
#       {'first2': 20, 'skip2': 80},
#     ]

#     page_nodes = [
#       PaginationNode('first1', 'skip1', [], InputValue.Int(5), None, [
#         PaginationNode('first0', 'skip0', [], None, None, [])
#       ]),
#       PaginationNode('first2', 'skip2', [], None, None, [])
#     ]

#     self.assertEqual(pagination_args(page_nodes, 20), expected)


# class TestTrimDocument(unittest.TestCase):
#   def test_trim_docs_1(self):
#     expected = Document(url='www.abc.xyz/graphql', query=Query(
#       name=None,
#       selection=[
#         Selection(
#           fmeta=TypeMeta.FieldMeta('pairs', '', [
#             TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
#             TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
#           ], TypeRef.non_null_list('Pair')),
#           arguments=[
#             Argument('first', InputValue.Variable('first1')),
#             Argument('skip', InputValue.Variable('skip1')),
#           ],
#           selection=[
#             Selection(
#               fmeta=TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
#               arguments=[
#                 Argument('first', InputValue.Variable('first0')),
#                 Argument('skip', InputValue.Variable('skip0')),
#               ],
#               selection=[
#                 Selection(
#                   fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
#                 )
#               ]
#             )
#           ]
#         ),
#       ],
#       variables=[
#         VariableDefinition('first1', TypeRef.Named('Int')),
#         VariableDefinition('skip1', TypeRef.Named('Int')),
#         VariableDefinition('first0', TypeRef.Named('Int')),
#         VariableDefinition('skip0', TypeRef.Named('Int')),
#       ]
#     ), variables={'first1': 1, 'skip1': 0, 'first0': 20, 'skip0': 0})

#     doc = Document(url='www.abc.xyz/graphql', query=Query(
#       name=None,
#       selection=[
#         Selection(
#           fmeta=TypeMeta.FieldMeta('pairs', '', [
#             TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
#             TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
#           ], TypeRef.non_null_list('Pair')),
#           arguments=[
#             Argument('first', InputValue.Variable('first1')),
#             Argument('skip', InputValue.Variable('skip1')),
#           ],
#           selection=[
#             Selection(
#               fmeta=TypeMeta.FieldMeta('swaps', '', [], TypeRef.non_null_list('Swap')),
#               arguments=[
#                 Argument('first', InputValue.Variable('first0')),
#                 Argument('skip', InputValue.Variable('skip0')),
#               ],
#               selection=[
#                 Selection(
#                   fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
#                 )
#               ]
#             )
#           ]
#         ),
#         Selection(
#           fmeta=TypeMeta.FieldMeta('swaps', '', [
#             TypeMeta.ArgumentMeta('first', '', TypeRef.Named('Int'), None),
#             TypeMeta.ArgumentMeta('skip', '', TypeRef.Named('Int'), None),
#           ], TypeRef.non_null_list('Swap')),
#           arguments=[
#             Argument('first', InputValue.Variable('first2')),
#             Argument('skip', InputValue.Variable('skip2')),
#           ],
#           selection=[
#             Selection(
#               fmeta=TypeMeta.FieldMeta('id', '', [], TypeRef.Named('String'))
#             )
#           ]
#         ),
#       ]
#     ))

#     self.assertEqual(
#       trim_document(doc, {'first1': 1, 'skip1': 0, 'first0': 20, 'skip0': 0}),
#       expected
#     )


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
