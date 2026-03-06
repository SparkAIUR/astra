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


def path_get(obj: dict[str, Any], *path: str) -> Any:
    cur: Any = obj
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
        if cur is None:
            return None
    return cur


def as_bool(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return bool(v)
    if isinstance(v, str):
        return v.strip().lower() in {"1", "true", "yes", "pass", "passed"}
    return False


def main() -> int:
    parser = argparse.ArgumentParser(description="Build phase10 matrix outputs")
    parser.add_argument("--run-dir", required=True, help="phase10 run directory")
    parser.add_argument("--out-json", default=None, help="output matrix json path")
    parser.add_argument("--out-csv", default=None, help="output matrix csv path")
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    qos = load_json(run_dir / "phase10-qos-swapoff-summary.json")
    auto = load_json(run_dir / "phase10-auto-governor-summary.json")
    eco = load_json(run_dir / "phase10-ecosystem-summary.json")
    gray = load_json(run_dir / "phase10-gray-failure-summary.json")

    gates = {
        "qos_proof": {
            "target": "swapoff replay keeps k3s lease alive",
            "actual": path_get(qos, "pass"),
            "pass": as_bool(path_get(qos, "pass")),
        },
        "auto_tuning_matrix": {
            "target": "auto governor applies >=2 effective profiles",
            "actual": path_get(auto, "pass"),
            "pass": as_bool(path_get(auto, "pass")),
        },
        "ecosystem_cni": {
            "target": "CNI churn without manual defrag and no latency cliff",
            "actual": path_get(eco, "cni", "pass"),
            "pass": as_bool(path_get(eco, "cni", "pass")),
        },
        "ecosystem_gateway": {
            "target": "gateway read storm sustains read RPS + heartbeat stability",
            "actual": path_get(eco, "gateway", "pass"),
            "pass": as_bool(path_get(eco, "gateway", "pass")),
        },
        "ecosystem_patroni": {
            "target": "tier0 lease latency <50ms under write pressure",
            "actual": path_get(eco, "patroni", "pass"),
            "pass": as_bool(path_get(eco, "patroni", "pass")),
        },
        "gray_failure_survival": {
            "target": "leader p99 stable under poisoned follower disk",
            "actual": path_get(gray, "pass"),
            "pass": as_bool(path_get(gray, "pass")),
        },
    }
    overall_pass = all(bool(v.get("pass")) for v in gates.values())

    matrix = {
        "run_dir": str(run_dir),
        "qos": qos,
        "auto": auto,
        "ecosystem": eco,
        "gray_failure": gray,
        "gates": gates,
        "overall_pass": overall_pass,
    }

    out_json = Path(args.out_json) if args.out_json else run_dir / "phase10-matrix.json"
    out_csv = Path(args.out_csv) if args.out_csv else run_dir / "phase10-matrix.csv"
    out_json.write_text(json.dumps(matrix, indent=2), encoding="utf-8")

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["gate", "pass", "target", "actual"])
        for name, gate in gates.items():
            writer.writerow([name, gate.get("pass"), gate.get("target"), gate.get("actual")])
        writer.writerow(["overall_pass", overall_pass, "", overall_pass])

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
