from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
COMPACT_LOG = REPO_ROOT / "refs/scripts/validation/compact_log.py"
BUILD_REPORT_MSG = REPO_ROOT / "refs/scripts/validation/build_validation_report_msg.py"


def test_compact_log_filters_debug_and_compacts_repeats(tmp_path: Path):
    input_log = tmp_path / "sample.log"
    input_log.write_text(
        "\n".join(
            [
                "node1 | \x1b[2m2026-02-27T17:00:00.000000Z\x1b[0m INFO booting",
                "node1 | \x1b[2m2026-02-27T17:00:01.000000Z\x1b[0m DEBUG noisy flush",
                "node1 | \x1b[2m2026-02-27T17:00:02.000000Z\x1b[0m INFO logline",
                "node1 | \x1b[2m2026-02-27T17:00:03.000000Z\x1b[0m INFO logline",
                "node1 | \x1b[2m2026-02-27T17:00:04.000000Z\x1b[0m INFO logline",
                "node1 | \x1b[2m2026-02-27T17:00:05.000000Z\x1b[0m INFO newlogline",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    output_log = tmp_path / "sample-cleaned.log"
    subprocess.run(
        [sys.executable, str(COMPACT_LOG), str(input_log), "--output", str(output_log)],
        check=True,
    )

    content = output_log.read_text(encoding="utf-8")
    assert "DEBUG noisy flush" not in content
    assert "INFO logline" in content
    assert "... repeated 2 more times" in content
    assert "INFO newlogline" in content


def test_build_validation_report_msg_inlines_unique_references(tmp_path: Path):
    report = tmp_path / "phase4-validation-report.md"
    ref_a = tmp_path / "refs/scripts/validation/results/a.txt"
    ref_b = tmp_path / "refs/scripts/validation/results/b.json"
    ref_a.parent.mkdir(parents=True, exist_ok=True)
    ref_a.write_text("alpha\n", encoding="utf-8")
    ref_b.write_text('{"ok": true}\n', encoding="utf-8")

    report.write_text(
        "\n".join(
            [
                "# Report",
                "Evidence: `refs/scripts/validation/results/a.txt`",
                "Evidence: `refs/scripts/validation/results/b.json`",
                "Evidence repeat: `refs/scripts/validation/results/a.txt`",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    output = tmp_path / "phase4-validation-report-msg.md"
    subprocess.run(
        [
            sys.executable,
            str(BUILD_REPORT_MSG),
            str(report),
            "--repo-root",
            str(tmp_path),
            "--output",
            str(output),
        ],
        check=True,
    )

    content = output.read_text(encoding="utf-8")
    assert content.startswith("```markdown\n# Report\n")
    assert content.count("\n---\n`refs/scripts/validation/results/a.txt`\n```\nalpha\n```") == 1
    assert content.count('\n---\n`refs/scripts/validation/results/b.json`\n```\n{"ok": true}\n```') == 1
    assert "alpha" in content
    assert '{"ok": true}' in content


def test_build_validation_report_msg_prefers_existing_cleaned_log(tmp_path: Path):
    report = tmp_path / "phase4-validation-report.md"
    raw_log = tmp_path / "refs/scripts/validation/results/phase4-scenario-b-astra-logs.txt"
    cleaned_log = tmp_path / "refs/scripts/validation/results/phase4-scenario-b-astra-logs-cleaned.txt"
    raw_log.parent.mkdir(parents=True, exist_ok=True)
    raw_log.write_text("raw-line\n", encoding="utf-8")
    cleaned_log.write_text("clean-line\n", encoding="utf-8")
    report.write_text(
        "Evidence: `refs/scripts/validation/results/phase4-scenario-b-astra-logs.txt`\n",
        encoding="utf-8",
    )

    output = tmp_path / "phase4-validation-report-msg.md"
    subprocess.run(
        [
            sys.executable,
            str(BUILD_REPORT_MSG),
            str(report),
            "--repo-root",
            str(tmp_path),
            "--output",
            str(output),
        ],
        check=True,
    )

    content = output.read_text(encoding="utf-8")
    assert "`refs/scripts/validation/results/phase4-scenario-b-astra-logs-cleaned.txt`" in content
    assert "clean-line" in content
    assert "raw-line" not in content


def test_build_validation_report_msg_cleans_referenced_logs(tmp_path: Path):
    report = tmp_path / "phase4-validation-report.md"
    raw_log = tmp_path / "refs/scripts/validation/results/phase4-scenario-b-astra-logs.txt"
    cleaned_log = tmp_path / "refs/scripts/validation/results/phase4-scenario-b-astra-logs-cleaned.txt"
    raw_log.parent.mkdir(parents=True, exist_ok=True)
    raw_log.write_text(
        "\n".join(
            [
                "node1 | \x1b[2m2026-02-27T17:00:00.000000Z\x1b[0m DEBUG noisy",
                "node1 | \x1b[2m2026-02-27T17:00:01.000000Z\x1b[0m INFO stable",
                "node1 | \x1b[2m2026-02-27T17:00:02.000000Z\x1b[0m INFO stable",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    report.write_text(
        "Evidence: `refs/scripts/validation/results/phase4-scenario-b-astra-logs.txt`\n",
        encoding="utf-8",
    )

    output = tmp_path / "phase4-validation-report-msg.md"
    subprocess.run(
        [
            sys.executable,
            str(BUILD_REPORT_MSG),
            str(report),
            "--repo-root",
            str(tmp_path),
            "--clean-referenced-logs",
            "--output",
            str(output),
        ],
        check=True,
    )

    assert cleaned_log.exists()
    cleaned_content = cleaned_log.read_text(encoding="utf-8")
    assert "DEBUG noisy" not in cleaned_content
    assert "... repeated 1 more times" in cleaned_content

    msg_content = output.read_text(encoding="utf-8")
    assert "`refs/scripts/validation/results/phase4-scenario-b-astra-logs-cleaned.txt`" in msg_content
