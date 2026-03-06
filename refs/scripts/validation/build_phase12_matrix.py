#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "pass", "passed"}
    return False


def main() -> int:
    ap = argparse.ArgumentParser(description="Build phase12 matrix outputs")
    ap.add_argument("--run-dir", required=True)
    ap.add_argument("--out-json", default=None)
    ap.add_argument("--out-csv", default=None)
    args = ap.parse_args()

    run_dir = Path(args.run_dir)

    multiraft = load_json(run_dir / "phase12-multiraft-summary.json")
    watch = load_json(run_dir / "phase12-watch-fanout-summary.json")
    list_stream = load_json(run_dir / "phase12-list-stream-summary.json")
    cache = load_json(run_dir / "phase12-semantic-cache-summary.json")
    crucible = load_json(run_dir / "phase12-limitless-crucible-summary.json")
    tombstone = load_json(run_dir / "phase12-tombstone-soak-summary.json")

    gates = {
        "multi_raft_scale": {
            "target": "prefix-routed sharding shows horizontal write scale gain",
            "actual": multiraft.get("scale_gain"),
            "pass": as_bool(multiraft.get("pass")),
        },
        "watch_fanout_delegate": {
            "target": "watch fan-out delegated to followers with bounded causal lag",
            "actual": watch.get("pass"),
            "pass": as_bool(watch.get("pass")),
        },
        "list_stream_memory": {
            "target": "streamed LIST serves target cardinality with bounded peak RSS",
            "actual": list_stream.get("pass"),
            "pass": as_bool(list_stream.get("pass")),
        },
        "semantic_cache_qol": {
            "target": "semantic cache improves GET latency/throughput under informer-like load",
            "actual": cache.get("pass"),
            "pass": as_bool(cache.get("pass")),
        },
        "limitless_state_recovery": {
            "target": "large payload ingest + wiped follower rejoins without leader spike",
            "actual": crucible.get("pass"),
            "pass": as_bool(crucible.get("pass")),
        },
        "tombstone_soak": {
            "target": "sustained churn shows disk plateau trend and stable LIST tail latency",
            "actual": tombstone.get("pass"),
            "pass": as_bool(tombstone.get("pass")),
        },
    }

    overall_pass = all(bool(v.get("pass")) for v in gates.values())
    matrix = {
        "run_dir": str(run_dir),
        "multiraft": multiraft,
        "watch_fanout": watch,
        "list_stream": list_stream,
        "semantic_cache": cache,
        "limitless_crucible": crucible,
        "tombstone_soak": tombstone,
        "gates": gates,
        "overall_pass": overall_pass,
    }

    out_json = Path(args.out_json) if args.out_json else run_dir / "phase12-matrix.json"
    out_csv = Path(args.out_csv) if args.out_csv else run_dir / "phase12-matrix.csv"
    out_json.write_text(json.dumps(matrix, indent=2), encoding="utf-8")

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(["gate", "pass", "target", "actual"])
        for name, gate in gates.items():
            w.writerow([name, gate.get("pass"), gate.get("target"), gate.get("actual")])
        w.writerow(["overall_pass", overall_pass, "", overall_pass])

    print(
        json.dumps(
            {
                "matrix_json": str(out_json),
                "matrix_csv": str(out_csv),
                "overall_pass": overall_pass,
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
