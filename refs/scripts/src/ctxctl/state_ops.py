from __future__ import annotations

from typing import Any

import yaml

from .db import add_state_event, utc_now


def load_state(path):
    if not path.exists():
        return {"current": {}, "history": []}
    data = yaml.safe_load(path.read_text()) or {}
    data.setdefault("current", {})
    data.setdefault("history", [])
    return data


def write_state(path, state: dict[str, Any]) -> None:
    path.write_text(yaml.safe_dump(state, sort_keys=False))


def set_dotted_key(target: dict[str, Any], dotted_key: str, value: Any) -> None:
    parts = dotted_key.split(".")
    node = target
    for part in parts[:-1]:
        nxt = node.get(part)
        if not isinstance(nxt, dict):
            nxt = {}
            node[part] = nxt
        node = nxt
    node[parts[-1]] = value


def parse_set_arg(raw: str) -> tuple[str, Any]:
    if "=" not in raw:
        raise ValueError(f"Invalid --set value '{raw}', expected key=value")
    key, value = raw.split("=", 1)
    # basic type coercion
    low = value.lower()
    if low in {"true", "false"}:
        coerced: Any = low == "true"
    else:
        try:
            coerced = int(value)
        except ValueError:
            try:
                coerced = float(value)
            except ValueError:
                coerced = value
    return key.strip(), coerced


def update_state(path, summary: str, set_items: list[str], updates: list[str], conn=None) -> dict[str, Any]:
    state = load_state(path)

    for raw in set_items:
        key, value = parse_set_arg(raw)
        set_dotted_key(state["current"], key, value)

    ts = utc_now()
    state["current"]["updated_at"] = ts
    state["history"].append(
        {
            "ts": ts,
            "summary": summary,
            "updates": updates,
        }
    )

    write_state(path, state)

    if conn is not None:
        add_state_event(conn, summary=summary, updates=updates)

    return state
