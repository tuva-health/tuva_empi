from typing import Any, Iterable, Mapping


def select_keys(d: Mapping[str, Any], keys: Iterable[str]) -> dict[str, Any]:
    return {k: d[k] for k in keys if k in d}
