from __future__ import annotations

import pytest

from btengine.data.cryptohftdata._arrow import DEFAULT_SORT_ROW_LIMIT, resolve_sort_row_limit


def test_resolve_sort_row_limit_uses_explicit_value(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BTENGINE_SORT_ROW_LIMIT", "20000000")
    assert resolve_sort_row_limit(123) == 123


def test_resolve_sort_row_limit_uses_env_when_unspecified(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BTENGINE_SORT_ROW_LIMIT", "20000000")
    assert resolve_sort_row_limit(None) == 20_000_000


def test_resolve_sort_row_limit_uses_default_when_env_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BTENGINE_SORT_ROW_LIMIT", raising=False)
    assert resolve_sort_row_limit(None) == DEFAULT_SORT_ROW_LIMIT


def test_resolve_sort_row_limit_rejects_invalid_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BTENGINE_SORT_ROW_LIMIT", "abc")
    with pytest.raises(ValueError):
        resolve_sort_row_limit(None)
