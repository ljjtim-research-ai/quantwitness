"""validity 算子族及其直接共享实现。"""

from __future__ import annotations

from typing import Mapping
from research_pipeline.platform import canonical_json
from ..operator_graph_evidence import build_dataset_observation_validity_facts
from ..operator_runtime import OperatorRuntimeContext, RuntimeNodeOutputs, RuntimeNodeValue
from .common import (
    _environment,
    _external_result,
    _input_admitted_plans,
    _input_external_payload,
    _parameters,
)


def execute_data_catalog_admission_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeOutputs:
    parameters = _parameters(context)
    configuration_hash = context.node.configuration_hash
    if not isinstance(configuration_hash, str):
        raise ValueError("Catalog admission 节点缺少配置身份")
    return RuntimeNodeOutputs.single(
        RuntimeNodeValue.inline(
            name=context.node.output_types[0][0],
            artifact_type=context.node.output_types[0][1],
            content=canonical_json(
                {
                    "contract_version": "data-catalog-admission-artifact-v2",
                    "node_configuration_hash": configuration_hash,
                    "request_ids": list(parameters["request_ids"]),
                    "catalog_hash": parameters["catalog_hash"],
                    "pit_contract_hash": parameters["pit_contract_hash"],
                    "drift_proof_hash": parameters["drift_proof_hash"],
                    "execution_estimates_hash": parameters[
                        "execution_estimates_hash"
                    ],
                }
            ).encode("utf-8"),
        )
    )


def execute_research_validity_data_observation_v1(
    context: OperatorRuntimeContext,
) -> RuntimeNodeValue:
    """消费普通列式数据观察，生成 Result/verify 直接使用的 validity facts。"""

    environment = _environment(context)
    data_artifact = _input_external_payload(context, "data")
    data_bundle = data_artifact.get("data_bundle")
    if not isinstance(data_bundle, Mapping):
        raise ValueError("日频数据观察 validity 缺少 data bundle")
    facts = build_dataset_observation_validity_facts(
        admitted_plans=_input_admitted_plans(environment, data_bundle),
        data_bundle=data_bundle,
        observations=data_artifact.get("observations"),
        fixed_clock=environment.fixed_clock,
    )
    return _external_result(context, facts)
