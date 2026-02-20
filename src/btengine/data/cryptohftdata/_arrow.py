from __future__ import annotations

import os
from pathlib import Path

import pyarrow.fs as fs
import pyarrow.parquet as pq

DEFAULT_SORT_ROW_LIMIT = 10_000_000
SORT_ROW_LIMIT_ENV_VAR = "BTENGINE_SORT_ROW_LIMIT"


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


def ensure_in_memory_sort_within_row_limit(
    pf: pq.ParquetFile,
    *,
    row_limit: int | None,
    context: str,
) -> None:
    """Fail fast when an in-memory full-file sort would exceed a row limit."""

    if row_limit is None:
        return
    limit = int(row_limit)
    if limit <= 0:
        return

    rows = int(pf.metadata.num_rows or 0)
    if rows <= limit:
        return

    raise MemoryError(
        f"{context}: in-memory sort requires {rows} rows, exceeds limit={limit}. "
        "Use a smaller time window or pre-sort parquet upstream."
    )


def resolve_sort_row_limit(row_limit: int | None) -> int | None:
    """Resolve an explicit limit or fallback to env/default.

    Priority:
    1) explicit `row_limit` argument
    2) env `BTENGINE_SORT_ROW_LIMIT`
    3) code default `DEFAULT_SORT_ROW_LIMIT`
    """

    if row_limit is not None:
        return int(row_limit)

    raw = os.getenv(SORT_ROW_LIMIT_ENV_VAR)
    if raw is None or raw.strip() == "":
        return DEFAULT_SORT_ROW_LIMIT

    token = raw.strip().replace("_", "")
    try:
        return int(token)
    except ValueError as exc:
        raise ValueError(
            f"invalid {SORT_ROW_LIMIT_ENV_VAR}={raw!r}; expected integer row count"
        ) from exc
