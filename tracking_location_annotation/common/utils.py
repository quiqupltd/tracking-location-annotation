"""
module to define helper functions
"""
import math
from typing import Any, Optional

import pandas as pd


def maybe_int(x: Any) -> Optional[int]:
    """
    return if given object is int or not
    """
    if pd.isnull(x):
        return None
    if not x:
        return None
    if math.isnan(x):
        return None
    return int(x)


def add_if_not_on_top(mylist: list, item: Any) -> None:
    """
    method used to add item to list
    exept if item is on top of the stack (list[-1] == item)
    """
    if len(mylist) >= 1:
        if mylist[-1] == item:
            return
    mylist.append(item)
