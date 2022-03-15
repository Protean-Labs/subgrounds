from __future__ import annotations
from dataclasses import dataclass
from typing import Optional
from functools import partial, reduce

from pipe import dedup, groupby, map, traverse, where
import pandas as pd

from subgrounds.query import Query, Selection
from subgrounds.subgraph import FieldPath
from subgrounds.utils import flatten_dict, contains_list, loop_generator, snd, union


def gen_columns(data: list | dict, prefix: str = '') -> list[str]:
  match data:
    case dict():
      return list(
        list(data.keys())
        | map(lambda key: gen_columns(data[key], f'{prefix}_{key}' if prefix != '' else key))
        | traverse
      )
    case list():
      return gen_columns(data[0], prefix)
    case _:
      return prefix


def fmt_cols(df: pd.DataFrame, col_map: dict[str, str]) -> pd.DataFrame:
  df = df.rename(col_map, axis='columns')
  cols = list(col_map.values() | dedup | where(lambda name: name in df.columns))
  return df[cols]


@dataclass
class DataFrameColumns:
  """ Helper class that holds data related to the shape of a DataFrame """
  key: str
  fpaths: list[str]

  def combine(self: DataFrameColumns, other: DataFrameColumns) -> DataFrameColumns:
    """ Returns new DataFrameColumns containing the union of `self` and `other`'s columns

    Args:
      self (DataFrameColumns): DataFrameColumns object
      other (DataFrameColumns): DataFrameColumns object

    Returns:
      DataFrameColumns: New DataFrameColumns containing the union of `self` and `other`
    """
    return DataFrameColumns(self.key, union(self.fpaths, other.fpaths))

  def mk_df(
    self: DataFrameColumns,
    data: dict,
    path_map: dict[str, FieldPath]
  ) -> pd.DataFrame:
    """ Formats the JSON data `data` into a DataFrame containing the columns defined in `self`.

    Args:
      self (DataFrameColumns): A DataFrameColumns object
      data (dict): The JSON data to be formatted into a dataframe
      path_map (dict[str, FieldPath]): A dictionary of `key-FieldPath` pairs

    Returns:
      pd.DataFrame: The JSON data formatted into a DataFrame
    """
    cols_data = {col: path_map[col].extract_data(data) for col in self.fpaths if col in path_map}

    rows_data = []

    def mk_rows(data: dict, row: dict = {}):
      if all([type(d) != list for d in list(data.values())]):
        rows_data.append(data | row)
      else:
        non_list_items = {key: value for key, value in data.items() if type(value) != list}
        list_items = {key: value for key, value in data.items() if type(value) == list}
        length = len(list(list_items.values())[0])
        for i in range(length):
          mk_rows(data={key: value[i] for key, value in list_items.items()}, row=row | non_list_items)

    mk_rows(cols_data, row={})
    return pd.DataFrame(data=rows_data)


# def columns_of_json(data: dict) -> list[str]:
#   # Helper function to combine result of list items columns
#   def merge(
#     cols1: DataFrameColumns | list[DataFrameColumns],
#     cols2: DataFrameColumns | list[DataFrameColumns]
#   ) -> DataFrameColumns | list[DataFrameColumns]:
#     # print(f'merge: cols1 = {cols1}, cols2 = {cols2}')
#     match cols1, cols2:
#       case list(), list():
#         return list(zip(cols1, cols2) | map(lambda cols: merge(cols[0], cols[1])))
#       case DataFrameColumns(), DataFrameColumns():
#         return DataFrameColumns.combine(cols1, cols2)

#   def columns_of_json(data: dict, keys: list[str] = [], fpaths: list[str] = []):
#     # Subset of the `data` dictionary containing only key-value pairs whose value
#     # if not a list, nor contains a nested list
#     values_dict = flatten_dict({key: value for key, value in data.items() if not contains_list(value)})
    
#     # List names identifiyng the values in `values_dict`
#     values_fpaths = ['_'.join([*keys, key]) for key in values_dict]

#     # Subset of the `data` dictionary containing only key-value pairs whose value
#     # either is a list, or contains a nested list
#     list_dict = {key: value for key, value in data.items() if contains_list(value) and value != []}

#     if list_dict == {}:
#       return [DataFrameColumns('_'.join(keys), values_fpaths + fpaths)]
#     else:
#       dfs: list[DataFrameColumns] = []
#       for key, value in data.items():
#         match value:
#           case dict():
#             dfs.append(columns_of_json(value, [*keys, key], fpaths + values_fpaths))
#           case list() if len(value) == 0:
#             continue
#           case list() if len(value) == 1:
#             dfs.append(columns_of_json(value[0], [*keys, key], fpaths + values_fpaths))
#           case list():
#             inner = list(
#               value
#               | map(partial(columns_of_json, keys=[*keys, key], fpaths=fpaths + values_fpaths))
#               | map(lambda l: list(l | traverse) if type(l) == list else l)
#             )
#             dfs.append(reduce(
#               merge,
#               list(
#                 value
#                 | map(partial(columns_of_json, keys=[*keys, key], fpaths=fpaths + values_fpaths))
#                 | map(lambda l: list(l | traverse) if type(l) == list else l)
#               )
#             ))
      
#     return dfs

#   return list(columns_of_json(data) | traverse)


def columns_of_selections(selections: list[Selection]) -> list[DataFrameColumns]:
  """ Generates a list of DataFrame columns specifications based on a list of Selection trees.

  Args:
      selections (list[Selection]): The selection trees

  Returns:
      list[DataFrameColumns]: The list of DataFrame columns specifications
  """
  def columns_of_selections(selections: list[Selection], keys: list[str] = [], fpaths: list[str] = []) -> list[DataFrameColumns]:
    non_list_selections = [select for select in selections if not select.contains_list()]
    non_list_fpaths = list(
      non_list_selections
      | map(lambda select: ['_'.join([*keys, *path]) for path in select.data_paths])
      | traverse
    )

    list_selections = [select for select in selections if select.contains_list()]

    if list_selections == []:
      return [DataFrameColumns('_'.join(keys), fpaths + non_list_fpaths)]
    else:
      return list(
        list_selections
        | map(lambda select: columns_of_selections(select.selection, keys=[*keys, select.key], fpaths=fpaths + non_list_fpaths))
      )

  return list(columns_of_selections(selections) | traverse)


def df_of_json(
  json_data: dict,
  fpaths: list[FieldPath],
  columns: Optional[list[str]] = None,
  merge: bool = False,
) -> pd.DataFrame | list[pd.DataFrame]:
  if columns is None:
    columns = list(fpaths | map(lambda fpath: fpath.longname))
  
  col_fpaths = zip(fpaths, loop_generator(columns))
  col_map = {fpath.dataname: colname for fpath, colname in col_fpaths}

  path_map = {fpath.dataname: fpath for fpath in fpaths}

  dfs = list(
    fpaths
    | groupby(lambda fpath: fpath.subgraph.url)
    | map(lambda group: FieldPath.merge(group[1]))
    | map(columns_of_selections)
    | traverse
    | map(partial(DataFrameColumns.mk_df, data=json_data, path_map=path_map))
  )

  match (len(dfs), merge):
    case (0, _):
      return pd.DataFrame(columns=columns, data=[])
    case (1, _):
      return fmt_cols(dfs[0], col_map)
    case (_, False):
      return list(dfs | map(lambda df: fmt_cols(df, col_map)))
    case (_, True):
      dfs = list(dfs | map(lambda df: fmt_cols(df, col_map)))
      return pd.concat(dfs, ignore_index=True)
