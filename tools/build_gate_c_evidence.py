"""从四项研究的自包含 Result 和独立 VerificationResult 生成 Gate C 证据。

输入采用 research-gate-c-input-v2，路径相对输入文件解析。data_scope 必须显式
选择 synthetic 或 real；synthetic 只证明工程闭环。当前输入由
tools/run_release_workflows.py 生成，候选与 BuildManifest 通过命令行参数绑定。
"""

from __future__ import annotations

import argparse
import ast
from datetime import date
import hashlib
import json
import os
from pathlib import Path
import sys


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SOURCE_ROOT = PROJECT_ROOT / "src"
if os.environ.get("RESEARCH_PIPELINE_ACCEPTANCE_RUNTIME") != "installed" and str(SOURCE_ROOT) not in sys.path:
    sys.path.insert(0, str(SOURCE_ROOT))

import research_pipeline  # noqa: E402
from research_pipeline.evidence import load_verified_result_context  # noqa: E402
from research_pipeline.extensions.verifier_bundle import ProjectVerifierBundleManifest  # noqa: E402
from research_pipeline.platform import typed_canonical_hash  # noqa: E402
from research_pipeline.results import ResultStore  # noqa: E402

from release_evidence_binding import release_evidence_binding  # noqa: E402


INPUT_VERSION = "research-gate-c-input-v2"
EVIDENCE_VERSION = "research-gate-c-evidence-v2"


def _load_json(path: Path) -> dict[str, object]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"JSON 无法读取: {path}") from exc
    if not isinstance(payload, dict):
        raise ValueError(f"JSON 顶层必须是对象: {path}")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _text(item: dict[str, object], field: str) -> str:
    value = item.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"Gate C 缺少非空字段: {field}")
    return value


def _path(item: dict[str, object], field: str, base: Path) -> Path:
    return (base / _text(item, field)).resolve()


def _assert_oracle_independent(path: Path) -> str:
    try:
        source = path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(path))
    except (OSError, UnicodeDecodeError, SyntaxError) as exc:
        raise ValueError(f"独立 oracle 源码无法审计: {path}") from exc
    imports = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.extend(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.append(node.module)
    if any(name == "research_pipeline" or name.startswith("research_pipeline.") for name in imports):
        raise ValueError(f"独立 oracle 不得导入生产核心: {path}")
    return _sha256(path)


def _request_windows(
    plan: dict[str, object], maximum_days: int, study_request_ids: set[str],
) -> list[dict[str, object]]:
    requests = plan.get("requests")
    if not isinstance(requests, list) or not requests:
        raise ValueError("Gate C 正式计划必须包含数据请求")
    windows = []
    observed = set()
    for item in requests:
        if not isinstance(item, dict) or not isinstance(item.get("query"), dict):
            raise ValueError("Gate C 计划请求 schema 无效")
        query = item["query"]
        request_id = _text(item, "request_id")
        if request_id in observed:
            raise ValueError("Gate C 计划 request_id 重复")
        observed.add(request_id)
        time_range = query.get("time_range")
        if not isinstance(time_range, dict):
            raise ValueError("Gate C 请求缺少显式时间窗口")
        try:
            start = date.fromisoformat(str(time_range["start"]))
            end = date.fromisoformat(str(time_range["end"]))
        except (KeyError, ValueError) as exc:
            raise ValueError("Gate C 请求时间窗口无效") from exc
        is_study_window = request_id in study_request_ids
        days = (end - start).days + 1
        if days <= 0 or (is_study_window and days > maximum_days):
            raise ValueError("Gate C 研究窗口超出批准范围或日期倒置")
        windows.append({
            "request_id": request_id,
            "dataset_id": _text(query, "dataset_id"),
            "start": start.isoformat(), "end": end.isoformat(),
            "calendar_days": days, "study_window": is_study_window,
        })
    if not study_request_ids or not study_request_ids <= observed:
        raise ValueError("Gate C 小窗口请求集合与批准输入不一致")
    return windows


def _database_evidence(item: dict[str, object], *, scope: str, result_id: str) -> dict[str, object]:
    evidence = item.get("database_evidence")
    if not isinstance(evidence, dict):
        raise ValueError("Gate C 缺少只读数据库证据")
    if (
        evidence.get("data_scope") != scope
        or evidence.get("result_id") != result_id
        or evidence.get("read_only") is not True
        or evidence.get("database_unchanged") is not True
    ):
        raise ValueError("Gate C 数据库只读证据与结果或数据范围不一致")
    _text(evidence, "database_path")
    before, after = evidence.get("before"), evidence.get("after")
    if (
        not isinstance(before, dict) or not isinstance(after, dict)
        or set(before) != {"size_bytes", "mtime_ns"}
        or before != after
        or any(type(value) is not int or value <= 0 for value in before.values())
    ):
        raise ValueError("Gate C 数据库前后大小和修改时间证据无效或发生变化")
    return evidence


def _validate_reference(
    item: dict[str, object], maximum_days: int, *, scope: str, base: Path,
) -> dict[str, object]:
    plan_path = _path(item, "plan", base)
    result_directory = _path(item, "result_directory", base)
    result_root = _path(item, "result_store", base)
    verification_path = _path(item, "verification_result", base)
    oracle_script = _path(item, "oracle_script", base)
    plan = _load_json(plan_path)
    if plan.get("contract_version") != "research-operator-graph-package-v2":
        raise ValueError("Gate C 必须使用正式算子图计划")
    plan_hash = plan.get("plan_hash")
    if plan_hash != typed_canonical_hash({key: value for key, value in plan.items() if key != "plan_hash"}):
        raise ValueError("Gate C plan 内容与 plan_hash 不一致")
    recipe = plan.get("recipe")
    if not isinstance(recipe, dict) or recipe.get("graph_id") != _text(item, "expected_graph_id"):
        raise ValueError("Gate C DAG 身份与批准输入不一致")

    # 报告消费者只读指标表；Gate C 额外完整检查诊断表、支持材料及内嵌 Verifier。
    store = ResultStore(result_root, create=False)
    bundle = store.verify(result_directory)
    if bundle.contract_version != "research-result-v3" or bundle.status != "finalized":
        raise ValueError("Gate C 只接受已 finalize 的 research-result-v3")
    if bundle.result_id != _text(item, "expected_result_id"):
        raise ValueError("Gate C Result 身份与批准输入不一致")
    if (
        bundle.plan_hash != plan_hash or bundle.package_hash != plan.get("package_hash")
        or bundle.result_spec.to_dict() != plan.get("result_spec")
        or bundle.verification.run.fixed_clock != plan.get("fixed_clock")
    ):
        raise ValueError("Gate C plan 与 Result 冻结合同不一致")
    namespace = result_directory.parent
    if {path.name for path in namespace.iterdir() if path.is_dir()} != {bundle.result_id}:
        raise ValueError("每个成功 Run 必须恰好只有一个正式 ResultBundle")

    context = load_verified_result_context(verification_path, result_store=result_root)
    verification = context.verification
    if context.snapshot.bundle.result_id != bundle.result_id:
        raise ValueError("Gate C VerificationResult 与指定 Result 不匹配")
    if verification.status != "pass" or verification.validity_status != "pass":
        raise ValueError("Gate C 独立 VerificationResult 必须通过")
    closure = bundle.verification
    if (
        not closure.verifier_bundle_path or not closure.verifier_identity
        or not verification.project_verifier_outcome_hash
    ):
        raise ValueError("Gate C 缺少绑定 Result 的独立 oracle")
    verifier_root = result_directory / closure.verifier_bundle_path
    verifier = ProjectVerifierBundleManifest.from_dict(_load_json(verifier_root / "manifest.json"))
    entry_path = verifier.entry_module.replace(".", "/") + ".py"
    entry = next((row for row in verifier.source_files if row["path"] == entry_path), None)
    oracle_hash = _assert_oracle_independent(oracle_script)
    if entry is None or oracle_hash != entry["sha256"]:
        raise ValueError("Gate C oracle 源码不属于 Result 冻结的 Verifier 入口")
    for source in verifier.source_files:
        if str(source["path"]).endswith(".py"):
            _assert_oracle_independent(verifier_root / "sources" / str(source["path"]))

    request_ids = item.get("study_request_ids")
    if (
        not isinstance(request_ids, list) or not request_ids
        or any(not isinstance(value, str) or not value for value in request_ids)
        or len(request_ids) != len(set(request_ids))
    ):
        raise ValueError("Gate C study_request_ids 必须是非空且唯一的请求列表")
    if not set(request_ids) <= set(bundle.formal_input_request_ids):
        raise ValueError("Gate C 研究窗口必须绑定 Result 的正式输入")
    windows = _request_windows(plan, maximum_days, set(request_ids))
    if {row["request_id"] for row in windows} != {row.request_id for row in bundle.input_revisions}:
        raise ValueError("Gate C plan 请求与 Result 输入修订不一致")
    if scope == "real" and any(str(row["dataset_id"]).startswith("quantwitness.synthetic_") for row in windows):
        raise ValueError("Gate C 公开合成数据不得声明为真实市场证据")
    database = _database_evidence(item, scope=scope, result_id=bundle.result_id)
    return {
        "reference": _text(item, "reference"), "dag_family": _text(item, "dag_family"),
        "graph_id": recipe["graph_id"], "plan_hash": bundle.plan_hash,
        "package_hash": bundle.package_hash, "project_id": bundle.project_id,
        "run_id": bundle.run_id, "result_id": bundle.result_id,
        "result_contract": bundle.contract_version, "result_bundle_count_for_run": 1,
        "table_count": len(bundle.tables), "input_revision_count": len(bundle.input_revisions),
        "request_windows": windows,
        "verification_contract": verification.contract_version,
        "verification_hash": verification.verification_hash,
        "verification_status": verification.status,
        "oracle_identity": dict(closure.verifier_identity),
        "oracle_outcome_hash": verification.project_verifier_outcome_hash,
        "oracle_script_hash": oracle_hash,
        "data_scope": scope, "database_evidence": database,
    }


def build_gate_c_evidence(
    *, input_path: Path, output: Path,
    release_candidate_id: str | None = None, build_manifest_path: Path | None = None,
) -> dict[str, object]:
    if output.exists():
        raise ValueError("Gate C 输出目录必须不存在")
    inputs = _load_json(input_path)
    if inputs.get("contract_version") != INPUT_VERSION:
        raise ValueError("Gate C input 必须使用 v2；历史 v1 收据不能重新贴签")
    scope = inputs.get("data_scope")
    if scope not in {"synthetic", "real"}:
        raise ValueError("Gate C 必须显式声明 synthetic 或 real 数据范围")
    references = inputs.get("references")
    if not isinstance(references, list) or len(references) != 4 or any(not isinstance(item, dict) for item in references):
        raise ValueError("Gate C 必须完整覆盖四项研究")
    if len({_text(item, "reference") for item in references}) != 4:
        raise ValueError("Gate C 四项研究 reference 必须唯一")
    maximum_days = inputs.get("maximum_window_days")
    minimum_families = inputs.get("minimum_dag_families")
    if type(maximum_days) is not int or maximum_days <= 0:
        raise ValueError("Gate C maximum_window_days 必须是正整数")
    if type(minimum_families) is not int or not 3 <= minimum_families <= 4:
        raise ValueError("Gate C 至少需要三种不同 DAG family")
    binding = release_evidence_binding(
        release_candidate_id=release_candidate_id,
        build_manifest_path=build_manifest_path, project=PROJECT_ROOT,
    )
    results = [_validate_reference(item, maximum_days, scope=scope, base=input_path.resolve().parent) for item in references]
    families = sorted({str(item["dag_family"]) for item in results})
    if len(families) < minimum_families or len({item["graph_id"] for item in results}) < minimum_families:
        raise ValueError("Gate C 至少需要三种不同 DAG family 和对应实际图")
    if len({(item["project_id"], item["run_id"]) for item in results}) != 4:
        raise ValueError("Gate C 四项研究必须来自四个独立 Run")
    evidence = {
        "contract_version": EVIDENCE_VERSION, "gate_id": "gate-c", "status": "pass", **binding,
        "data_scope": scope,
        "acceptance_scope": "engineering" if scope == "synthetic" else "real_market_window",
        "runtime_origin": str(Path(research_pipeline.__file__).resolve()),
        "input_hash": _sha256(input_path), "reference_count": len(results),
        "dag_family_count": len(families), "dag_families": families,
        "all_results_contract": "research-result-v3",
        "all_verifications_contract": "research-verification-result-v4",
        "each_successful_run_has_one_result_bundle": True,
        "independent_oracle_count": len(results), "references": results,
    }
    output.mkdir(parents=True)
    (output / "gate-c-evidence.json").write_text(
        json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")), encoding="utf-8",
    )
    return evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="生成四项研究的 Gate C 证据，显式区分合成与真实数据")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--release-candidate-id")
    parser.add_argument("--build-manifest")
    args = parser.parse_args()
    evidence = build_gate_c_evidence(
        input_path=Path(args.input).resolve(), output=Path(args.output).resolve(),
        release_candidate_id=args.release_candidate_id,
        build_manifest_path=Path(args.build_manifest).resolve() if args.build_manifest else None,
    )
    print(json.dumps({key: evidence[key] for key in (
        "status", "reference_count", "dag_family_count", "data_scope", "acceptance_scope", "evidence_scope",
    )}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
