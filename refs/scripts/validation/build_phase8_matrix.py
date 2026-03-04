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


def path_get(obj: dict[str, Any], *path: str) -> Any:
    cur: Any = obj
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
        if cur is None:
            return None
    return cur


def main() -> int:
    parser = argparse.ArgumentParser(description="Build phase8 RC1 matrix outputs")
    parser.add_argument("--run-dir", required=True, help="phase8 run directory")
    parser.add_argument("--out-json", default=None, help="output matrix json path")
    parser.add_argument("--out-csv", default=None, help="output matrix csv path")
    parser.add_argument("--iops-gate", type=float, default=5000.0)
    parser.add_argument("--iowait-gate", type=float, default=10.0)
    parser.add_argument("--list-gap-gate-ms", type=float, default=100.0)
    parser.add_argument("--pod-target", type=int, default=50000)
    args = parser.parse_args()

    run_dir = Path(args.run_dir)
    astra = load_json(run_dir / "phase6-k3s-astra-summary.json")
    etcd = load_json(run_dir / "phase6-k3s-etcd-summary.json")
    omni = load_json(run_dir / "phase6-omni-summary.json")
    friction = load_json(run_dir / "phase6-friction-summary.json")

    astra_list_p99 = path_get(astra, "latency_ms", "list", "p99_ms")
    etcd_list_p99 = path_get(etcd, "latency_ms", "list", "p99_ms")
    list_gap_ms = (
        (astra_list_p99 - etcd_list_p99)
        if astra_list_p99 is not None and etcd_list_p99 is not None
        else None
    )
    astra_iops_peak = path_get(astra, "resource", "disk_iops_peak")
    astra_iowait_p99 = path_get(astra, "resource", "cpu_iowait_p99")
    astra_pods = path_get(astra, "actual", "pods")
    etcd_pods = path_get(etcd, "actual", "pods")

    gates = {
        "iops_collapse": {
            "target": f"< {args.iops_gate}",
            "actual": astra_iops_peak,
            "pass": (astra_iops_peak is not None and astra_iops_peak < args.iops_gate),
        },
        "iowait_return": {
            "target": f"< {args.iowait_gate}",
            "actual": astra_iowait_p99,
            "pass": (astra_iowait_p99 is not None and astra_iowait_p99 < args.iowait_gate),
        },
        "list_latency_parity": {
            "target": f"gap <= {args.list_gap_gate_ms}ms",
            "actual": list_gap_ms,
            "pass": (list_gap_ms is not None and list_gap_ms <= args.list_gap_gate_ms),
        },
        "pod_cardinality_astra": {
            "target": args.pod_target,
            "actual": astra_pods,
            "pass": (astra_pods is not None and int(astra_pods) >= args.pod_target),
        },
        "pod_cardinality_etcd": {
            "target": args.pod_target,
            "actual": etcd_pods,
            "pass": (etcd_pods is not None and int(etcd_pods) >= args.pod_target),
        },
    }
    overall_pass = all(bool(v["pass"]) for v in gates.values())

    matrix = {
        "run_dir": str(run_dir),
        "astra": astra,
        "etcd": etcd,
        "delta": {
            "put_p99_ms": (
                path_get(astra, "latency_ms", "put", "p99_ms")
                - path_get(etcd, "latency_ms", "put", "p99_ms")
                if path_get(astra, "latency_ms", "put", "p99_ms") is not None
                and path_get(etcd, "latency_ms", "put", "p99_ms") is not None
                else None
            ),
            "get_p99_ms": (
                path_get(astra, "latency_ms", "get", "p99_ms")
                - path_get(etcd, "latency_ms", "get", "p99_ms")
                if path_get(astra, "latency_ms", "get", "p99_ms") is not None
                and path_get(etcd, "latency_ms", "get", "p99_ms") is not None
                else None
            ),
            "list_p99_ms": list_gap_ms,
            "disk_iops_peak": (
                astra_iops_peak - path_get(etcd, "resource", "disk_iops_peak")
                if astra_iops_peak is not None
                and path_get(etcd, "resource", "disk_iops_peak") is not None
                else None
            ),
            "cpu_iowait_p99": (
                astra_iowait_p99 - path_get(etcd, "resource", "cpu_iowait_p99")
                if astra_iowait_p99 is not None
                and path_get(etcd, "resource", "cpu_iowait_p99") is not None
                else None
            ),
            "pod_realization_ratio_astra": (
                astra_pods / args.pod_target if astra_pods is not None else None
            ),
            "pod_realization_ratio_etcd": (
                etcd_pods / args.pod_target if etcd_pods is not None else None
            ),
        },
        "omni": omni,
        "friction": friction,
        "gates": gates,
        "overall_pass": overall_pass,
    }

    out_json = Path(args.out_json) if args.out_json else run_dir / "phase8-k3s-matrix.json"
    out_csv = Path(args.out_csv) if args.out_csv else run_dir / "phase8-k3s-matrix.csv"
    out_json.write_text(json.dumps(matrix, indent=2), encoding="utf-8")

    with out_csv.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["metric", "value", "target", "pass"])
        for gate_name, gate in gates.items():
            writer.writerow([gate_name, gate.get("actual"), gate.get("target"), gate.get("pass")])
        writer.writerow(["overall_pass", overall_pass, "", overall_pass])

    print(json.dumps({"matrix_json": str(out_json), "matrix_csv": str(out_csv)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

