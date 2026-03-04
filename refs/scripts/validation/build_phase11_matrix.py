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
    parser = argparse.ArgumentParser(description="Build phase11 matrix outputs")
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--out-json", default=None)
    parser.add_argument("--out-csv", default=None)
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    forge = load_json(run_dir / "phase11-forge-ux-summary.json")
    fleet = load_json(run_dir / "phase11-fleet-cutover-summary.json")
    docs = load_json(run_dir / "phase11-docs-sandbox-summary.json")

    converge_log_exists = Path((forge.get("converge_log") or "")).exists()
    real_boot_count = int(fleet.get("real_boot_count") or 0)

    gates = {
        "forge_converge": {
            "target": "5-to-1 converge migration with per-tenant parity",
            "actual": forge.get("pass"),
            "pass": as_bool(forge.get("pass")),
        },
        "forge_ci_log": {
            "target": "detailed migration log artifact is present",
            "actual": converge_log_exists,
            "pass": converge_log_exists,
        },
        "fleet_cutover": {
            "target": "fleet stateless boot + no-loss tenant checks",
            "actual": fleet.get("pass"),
            "pass": as_bool(fleet.get("pass")),
        },
        "fleet_real_boot_density": {
            "target": ">=4 real omni boots in external etcd mode",
            "actual": real_boot_count,
            "pass": real_boot_count >= 4,
        },
        "docs_sandbox": {
            "target": "book skeleton builds and sandbox assets exist",
            "actual": docs.get("pass"),
            "pass": as_bool(docs.get("pass")),
        },
    }

    overall_pass = all(bool(v.get("pass")) for v in gates.values())
    matrix = {
        "run_dir": str(run_dir),
        "forge": forge,
        "fleet_cutover": fleet,
        "docs_sandbox": docs,
        "gates": gates,
        "overall_pass": overall_pass,
    }

    out_json = Path(args.out_json) if args.out_json else run_dir / "phase11-matrix.json"
    out_csv = Path(args.out_csv) if args.out_csv else run_dir / "phase11-matrix.csv"
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
