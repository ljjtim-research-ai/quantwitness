"""VerificationResult 报告的规范输出。"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Mapping

from research_pipeline.platform.exclusive_output import write_text_exclusive_atomic


REPORT_DOCUMENT_VERSION = "research-verification-report-v1"


def write_verification_report(
    destination: str | Path,
    *,
    output_format: str,
    markdown: str,
    report: Mapping[str, object],
) -> Path:
    """在目标同目录原子发布报告，且绝不覆盖既有文件。"""

    if output_format not in {"markdown", "json"}:
        raise ValueError("报告格式仅支持 markdown 或 json")
    if output_format == "markdown":
        content = markdown.rstrip("\n") + "\n"
    else:
        content = json.dumps(
            {
                "contract_version": REPORT_DOCUMENT_VERSION,
                "report": dict(report),
            },
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
    try:
        return write_text_exclusive_atomic(destination, content)
    except FileExistsError as exc:
        raise FileExistsError(f"报告输出已存在: {Path(destination).resolve()}") from exc


__all__ = ["REPORT_DOCUMENT_VERSION", "write_verification_report"]
