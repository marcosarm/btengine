from __future__ import annotations

from pathlib import Path

import pyarrow.fs as fs
import pyarrow.parquet as pq


def resolve_filesystem_and_path(path_or_uri: str | Path) -> tuple[fs.FileSystem | None, str]:
    """Resolve a local path or URI into (filesystem, path).

    - For local filesystem paths, returns (None, <path_str>) so pyarrow defaults apply.
    - For URIs like s3://..., returns (filesystem, <path_without_scheme_and_bucket>).
    """

    s = str(path_or_uri)
    if "://" not in s:
        return None, s

    filesystem, path = fs.FileSystem.from_uri(s)
    return filesystem, path


def resolve_path(path_or_uri: str | Path) -> str:
    """Resolve a local path or URI into a path string usable with an explicit filesystem."""

    s = str(path_or_uri)
    if "://" not in s:
        return s

    # Avoid creating new filesystem objects if we only need the path part.
    if s.startswith("s3://"):
        return s[len("s3://") :]

    filesystem, path = fs.FileSystem.from_uri(s)
    return path


def parquet_column_is_monotonic_non_decreasing(pf: pq.ParquetFile, column: str) -> bool:
    """Check monotonic non-decreasing order for a parquet column across all row groups."""

    prev_last: int | float | None = None
    for rg in range(pf.num_row_groups):
        t = pf.read_row_group(rg, columns=[column])
        arr = t[column].to_numpy(zero_copy_only=False)
        if len(arr) == 0:
            continue

        if len(arr) > 1 and not bool((arr[1:] >= arr[:-1]).all()):
            return False

        first = arr[0]
        last = arr[-1]
        if prev_last is not None and first < prev_last:
            return False
        prev_last = last

    return True
