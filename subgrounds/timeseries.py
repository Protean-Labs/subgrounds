from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from subgrounds.subgraph import FieldPath, Filter

# TODO: Interval definition
# class TimeseriesInterval(str, Enum):
#     HOURLY = ("H",)
#     DAILY = ("D",)
#     WEEKLY = ("W-SUN",)
#     MONTHLY = "MS"

# TODO: Aggregation method
# class AggregateMethod(str, Enum):
#     MEAN = ("mean",)
#     SUM = ("sum",)
#     FIRST = ("first",)
#     LAST = ("last",)
#     MEDIAN = ("median",)
#     MIN = ("min",)
#     MAX = ("max",)
#     COUNT = "count"

# TODO: Interpolation
# class NaInterpolationMethod(str, Enum):
#     FORDWARD_FILL = ("ffill",)
#     BACKWARD_FILL = ("bfill",)

# TODO: Column type
# class ColumnType(str, Enum):
#     int = ("int32",)
#     str = ("str",)
#     float = ("float64",)
#     bigdecimal = ("float64",)

@dataclass(frozen=True)
class Timeseries:
  @staticmethod
  def from_selection(
    # Query args
    entrypoint: FieldPath,
    value: FieldPath,
    timestamp: FieldPath,
    first: int = 100,
    orderBy: Optional[FieldPath] = None,
    orderDirection: Optional[str] = None,
    where: Optional[list[Filter]] = None,

    # Timeseries args

  ) -> Timeseries:
    # TODO: Add validation to make sure `value` and `timestamp` selections are valid for timeseries
    entrypoint = entrypoint(
      first=first,
      orderBy=orderBy,
      orderDirection=orderDirection,
      where=where
    )
    selection = [FieldPath.extend(entrypoint, s) for s in [value, timestamp]]

    query = entrypoint.subgraph.mk_request(selection)
    data = entrypoint.subgraph.query(query)[0][entrypoint.root.name]

    # Generate table
    # cols = list(columns(data[0]))
    # data = [dict(values(row)) for row in data]
