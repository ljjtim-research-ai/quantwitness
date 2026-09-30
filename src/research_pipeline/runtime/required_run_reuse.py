"""在 Runtime 启动前准备调用方明确要求复用的完成态 checkpoint。"""

from __future__ import annotations

import json
import os
from datetime import datetime
from pathlib import Path

from research_pipeline.platform import canonical_json

from .checkpoint import CheckpointStore
from .errors import RuntimeIntegrityError
from .execution_service import RuntimeExecutionService
from .external_artifact import ExternalArtifactStore
from .graph import DagSpec
from .identity import DeterminismContext
from .operator_registry import NODE_IDENTITY_PROJECTION_CURRENT
from .operator_runtime import RuntimeNodeOutputs


REQUIRED_RUN_REUSE_VERSION = "research-required-run-reuse-v1"


def prepare_required_run_reuse(
    *,
    service: RuntimeExecutionService,
    dag: DagSpec,
    source_run_roots: tuple[str | Path, ...],
    target_run_root: str | Path,
    root_seed: int,
    fixed_clock: str,
    required_node_ids: tuple[str, ...],
    node_identity_projection: str,
) -> dict[str, object]:
    """完整复验并导入指定节点及其上游，失败时不得启动 Worker。"""

    required = frozenset(required_node_ids)
    if not required or len(required) != len(required_node_ids):
        raise RuntimeIntegrityError("要求复用的节点必须非空且不得重复")
    node_map = {node.node_id: node for node in dag.nodes}
    unknown = sorted(required - set(node_map))
    if unknown:
        raise RuntimeIntegrityError(
            "要求复用的节点不在当前 DAG: " + ", ".join(unknown)
        )
    if node_identity_projection != NODE_IDENTITY_PROJECTION_CURRENT:
        raise RuntimeIntegrityError("要求节点复用只接受现行节点局部身份计划")
    target = Path(target_run_root).resolve()
    sources = service._open_reuse_sources(
        source_run_roots,
        target_root=target,
        node_identity_projection=node_identity_projection,
    )
    if not sources:
        raise RuntimeIntegrityError("要求节点复用时缺少完成态来源 run")
    needed = _ancestor_closure(dag, required)
    context = DeterminismContext(root_seed, datetime.fromisoformat(fixed_clock))
    checkpoints = CheckpointStore(target)
    external = ExternalArtifactStore(target / "external-artifacts")
    outputs: dict[str, RuntimeNodeOutputs] = {}
    prepared: list[str] = []
    source_by_node: dict[str, str] = {}
    for node_id in dag.topological_order():
        if node_id not in needed:
            continue
        node = node_map[node_id]
        values_by_port = service._inputs(dag, node, outputs)
        semantic_inputs = tuple(
            value.artifact_ref
            for _, value in sorted(values_by_port.items())
        )
        identity, expectation = service._identity(node, semantic_inputs, context)
        reused = service._reuse_cross_run_checkpoint(
            node=node,
            expectation=expectation,
            identity=identity,
            sources=sources,
            target_checkpoints=checkpoints,
            target_external=external,
            root_seed=root_seed,
            fixed_clock=fixed_clock,
        )
        if reused is None:
            raise RuntimeIntegrityError(
                f"要求跨运行复用的节点未通过启动前预检: {node_id}"
            )
        source_run_id, _manifest, node_outputs = reused
        outputs[node_id] = node_outputs
        prepared.append(node_id)
        source_by_node[node_id] = source_run_id
    document = {
        "contract_version": REQUIRED_RUN_REUSE_VERSION,
        "required_nodes": sorted(required),
        "prepared_nodes": prepared,
        "source_run_ids_by_node": dict(sorted(source_by_node.items())),
    }
    _write_or_verify(target / "required-reuse-plan.json", document)
    return document


def _ancestor_closure(dag: DagSpec, required: frozenset[str]) -> frozenset[str]:
    incoming: dict[str, set[str]] = {node.node_id: set() for node in dag.nodes}
    for edge in dag.edges:
        incoming[edge.target_node].add(edge.source_node)
    needed = set(required)
    pending = list(required)
    while pending:
        node_id = pending.pop()
        for parent in incoming[node_id]:
            if parent in needed:
                continue
            needed.add(parent)
            pending.append(parent)
    return frozenset(needed)


def _write_or_verify(path: Path, payload: dict[str, object]) -> None:
    if path.exists():
        try:
            existing = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeIntegrityError("要求复用计划无法读取") from exc
        if existing != payload:
            raise RuntimeIntegrityError("要求复用计划与当前输入不一致")
        return
    temporary = path.with_suffix(f"{path.suffix}.tmp")
    temporary.write_text(canonical_json(payload), encoding="utf-8")
    os.replace(temporary, path)


__all__ = ["REQUIRED_RUN_REUSE_VERSION", "prepare_required_run_reuse"]
