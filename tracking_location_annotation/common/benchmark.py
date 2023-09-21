"""
Defining a decorator to measure the performance if the code
"""
import time
import tracemalloc
from collections import defaultdict
from contextlib import ContextDecorator, contextmanager
from typing import TypeVar

import numpy as np
import pandas as pd

from tracking_location_annotation.common.constants import BENCHMARK

Func = TypeVar("Func")
traces = defaultdict(lambda: [])


@contextmanager
def memory_usage():
    """
    Prints memory usage of the yielded code.
    """

    tracemalloc.start()

    yield

    current, peak = tracemalloc.get_traced_memory()
    print(f"Current memory usage is {current / 10**6}MB; Peak was {peak / 10**6}MB")
    tracemalloc.stop()


class measure(ContextDecorator):  # pylint: disable =invalid-name
    """
    Class that can be used as a decorator or as context to measure the performance of the code.

    Example as decorator:

    >>> @measure()
    >>> def myfunc():
    >>>    ...

    Examples as context:

    >>> with measure('some.identifier'):
    >>>     ...

    Don't forget to call measure.print_stats() at the end to print the statistics.
    """

    def __init__(self, label=None):
        self.label = label
        self.start_time = None

    def __call__(self, func: Func) -> Func:
        if self.label is None:
            self.label = func.__name__  # type: ignore
            if hasattr(func, "__module__"):
                self.label = f"{func.__module__}.{func.__name__}"  # type: ignore

        return super().__call__(func)  # type: ignore

    def __enter__(self):
        if BENCHMARK:
            self.start_time = time.monotonic_ns()
        return self

    def __exit__(self, *exc):
        if BENCHMARK:
            duration = time.monotonic_ns() - self.start_time
            traces[self.label].append(duration)
        return False


def reset():
    "Remove all the traces callected."
    traces.clear()


def print_stats(sort_by="sum", ascending=False):
    """
    Print statistics of the traces collected to the stdout.

    Args:
        sort_by: metric to sort by, available metrics: count, sum, mean, std, p90, p99
        ascending: if the sort should be ascending or descending, default is False.
    """

    if not BENCHMARK:
        return

    dataframe = pd.DataFrame(
        [
            pd.Series(runs, name=name).agg(
                {
                    "count": len,
                    "sum": np.sum,
                    "mean": np.mean,
                    "std": np.std,
                    "p90": lambda x: x.quantile(0.9),
                    "p99": lambda x: x.quantile(0.99),
                }
            )
            for name, runs in traces.items()
        ]
    )
    dataframe[["mean", "std", "sum", "p90", "p99"]] = dataframe[["mean", "std", "sum", "p90", "p99"]] / 1_000_000_000
    print(dataframe.sort_values(by=sort_by, ascending=ascending))
