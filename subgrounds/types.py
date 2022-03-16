from __future__ import annotations

SCALAR_T = int | float | str | bool | None

OBJECT_T = dict[str, SCALAR_T | 'OBJECT_T' | list['OBJECT_T']]

RESPONSE_T = list[dict[str, SCALAR_T | OBJECT_T | list[OBJECT_T]]]
