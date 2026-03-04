from __future__ import annotations

from .db import utc_now


def append_kb_markdown(
    kb_path,
    entry_type: str,
    title: str,
    content: str,
    evidence: str,
    impact: str,
    follow_up: str,
) -> None:
    section = "\n".join(
        [
            f"- Date: `{utc_now()}`",
            f"- Type: `{entry_type}`",
            f"- Title: `{title}`",
            f"- Context: {content}",
            f"- Evidence: {evidence}",
            f"- Impact: {impact}",
            f"- Follow-up: {follow_up}",
            "",
        ]
    )

    text = kb_path.read_text() if kb_path.exists() else "# Astra Knowledge Base\n\n"
    kb_path.write_text(text.rstrip() + "\n\n" + section)
