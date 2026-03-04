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
    return json.loads(path.read_text(encoding="utf-8"))


def metric(summary: dict[str, Any], group: str, name: str) -> Any:
    return (summary.get(group) or {}).get(name)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build phase6 Astra vs etcd matrix outputs")
    parser.add_argument("--run-dir", required=True, help="phase6 run directory")
    parser.add_argument("--out-json", default=None, help="output matrix json path")
    parser.add_argument("--out-csv", default=None, help="output matrix csv path")
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    astra = load_json(run_dir / "phase6-k3s-astra-summary.json")
    etcd = load_json(run_dir / "phase6-k3s-etcd-summary.json")
    omni = load_json(run_dir / "phase6-omni-summary.json")
    friction = load_json(run_dir / "phase6-friction-summary.json")

    matrix = {
        "run_dir": str(run_dir),
        "astra": astra,
        "etcd": etcd,
        "delta": {
            "put_p99_ms": None,
            "get_p99_ms": None,
            "list_p99_ms": None,
            "churn_ms": None,
            "cold_scan_first_ms": None,
            "disk_iops_peak": None,
            "cpu_iowait_p99": None,
            "peak_container_mem_mb": None,
        },
        "omni": omni,
        "friction": friction,
    }

    if astra and etcd:
        for key, src in [
            ("put_p99_ms", ("latency_ms", "put", "p99_ms")),
            ("get_p99_ms", ("latency_ms", "get", "p99_ms")),
            ("list_p99_ms", ("latency_ms", "list", "p99_ms")),
            ("churn_ms", ("workloads", "churn_scale_ms")),
            ("cold_scan_first_ms", ("workloads", "cold_scan_first_ms")),
            ("disk_iops_peak", ("resource", "disk_iops_peak")),
            ("cpu_iowait_p99", ("resource", "cpu_iowait_p99")),
            ("peak_container_mem_mb", ("resource", "peak_container_mem_mb")),
        ]:
            if len(src) == 3:
                ag = ((astra.get(src[0]) or {}).get(src[1]) or {}).get(src[2])
                eg = ((etcd.get(src[0]) or {}).get(src[1]) or {}).get(src[2])
            else:
                ag = (astra.get(src[0]) or {}).get(src[1])
                eg = (etcd.get(src[0]) or {}).get(src[1])
            if ag is not None and eg is not None:
                matrix["delta"][key] = ag - eg

    out_json = Path(args.out_json) if args.out_json else run_dir / "phase6-k3s-matrix.json"
    out_csv = Path(args.out_csv) if args.out_csv else run_dir / "phase6-k3s-matrix.csv"
    out_json.write_text(json.dumps(matrix, indent=2), encoding="utf-8")

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "metric",
                "astra",
                "etcd",
                "delta_astra_minus_etcd",
            ]
        )
        rows = [
            ("put_p99_ms", metric(astra, "latency_ms", "put"), metric(etcd, "latency_ms", "put")),
            ("get_p99_ms", metric(astra, "latency_ms", "get"), metric(etcd, "latency_ms", "get")),
            ("list_p99_ms", metric(astra, "latency_ms", "list"), metric(etcd, "latency_ms", "list")),
            ("churn_scale_ms", metric(astra, "workloads", "churn_scale_ms"), metric(etcd, "workloads", "churn_scale_ms")),
            ("cold_scan_first_ms", metric(astra, "workloads", "cold_scan_first_ms"), metric(etcd, "workloads", "cold_scan_first_ms")),
            ("disk_iops_peak", metric(astra, "resource", "disk_iops_peak"), metric(etcd, "resource", "disk_iops_peak")),
            ("cpu_iowait_p99", metric(astra, "resource", "cpu_iowait_p99"), metric(etcd, "resource", "cpu_iowait_p99")),
            ("peak_container_mem_mb", metric(astra, "resource", "peak_container_mem_mb"), metric(etcd, "resource", "peak_container_mem_mb")),
        ]

        for name, a, e in rows:
            # latency_ms entries are nested dicts
            if isinstance(a, dict):
                a = a.get("p99_ms")
            if isinstance(e, dict):
                e = e.get("p99_ms")
            d = None
            if a is not None and e is not None:
                d = a - e
            writer.writerow([name, a, e, d])

    print(json.dumps({"matrix_json": str(out_json), "matrix_csv": str(out_csv)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
