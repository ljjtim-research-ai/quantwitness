from __future__ import annotations

import os
from pathlib import Path

import yaml

from research_pipeline.cli.capability_containment import (
    evaluate_public_capability_release, load_remediation_baseline,
)


ROOT = Path(__file__).resolve().parents[1]


def test_public_release_requires_actual_evidence_for_sealed_capabilities() -> None:
    baseline_path = os.environ.get("QUANTWITNESS_CAPABILITY_BASELINE")
    result = evaluate_public_capability_release(
        project_root=ROOT,
        baseline=load_remediation_baseline(baseline_path) if baseline_path else None,
        release_evidence_root=os.environ.get("QUANTWITNESS_RELEASE_EVIDENCE_ROOT"),
        release_candidate_id=os.environ.get("QUANTWITNESS_RELEASE_CANDIDATE_ID"),
    )
    assert result["status"] == "pass", result["issues"]


def test_public_ci_uses_example_dependency_lock_and_current_build_receipt() -> None:
    projects = (
        "equity_cross_section", "etf_time_series", "event_study", "futures_term_structure",
    )
    locks = {
        yaml.safe_load((ROOT / "examples" / name / "extension/operator.yaml").read_text(encoding="utf-8"))["dependency_lock"]["pyarrow"]
        for name in projects
    }
    assert len(locks) == 1
    version = locks.pop()
    workflow = yaml.safe_load((ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8"))
    job = workflow["jobs"]["public-boundary"]
    assert job["strategy"]["matrix"]["python-version"] == ["3.10", "3.13"]
    steps = {item["name"]: item for item in job["steps"]}
    source_install = steps["Install"]["run"]
    build = steps["Build release wheel"]["run"]
    installed = steps["Test isolated wheel and four formal example workflows"]["run"]
    assert f"pyarrow=={version}" in source_install
    assert f"pyarrow=={version}" in installed
    assert "pip check" in source_install and "pip check" in installed
    assert "quantwitness-build.json" in build and "GITHUB_OUTPUT" in build
    assert "steps.release.outputs.wheel" in installed
    assert "quantwitness-release/*.whl" not in installed
