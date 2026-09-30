"""Walk-forward 七阶段正式算子的列式工件执行。"""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import shutil
from typing import Callable, Iterable, Mapping

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from research_pipeline.platform import canonical_json, typed_canonical_hash
from research_pipeline.platform.canonical import typed_canonical_hash_streamed
from research_pipeline.research.modeling import (
    CandidateFitRejected,
    ModelMainlineError,
    WALK_FORWARD_MODEL_VERSION,
    assemble_daily_model_samples,
    evaluate_locked_holdout,
    fit_fold_preprocessor,
    fit_model_candidate,
    model_dependency_preflight,
    normalize_model_candidates,
    predict_model,
    score_model,
)
from research_pipeline.research.validation import (
    SplitFold,
    SplitManifest,
    TrialLedger,
    build_search_manifest,
    build_walk_forward,
    select_by_validation,
)
from research_pipeline.research.dataframe_budget import (
    PandasFrameBudget,
    PandasFrameBudgetError,
    pandas_frame_bytes,
)


def execute_model_split_artifact(
    *,
    feature_root: str | Path,
    label_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    fixed_clock: str | datetime,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    features, feature_metadata = _load_table(
        feature_root, "features", frame_budget=frame_budget,
    )
    holdout_start = pd.Timestamp(_text(parameters, "holdout_start"))
    if holdout_start.tzinfo is None:
        raise ModelMainlineError("holdout_start 必须包含时区")
    holdout_scan_time = pa.scalar(
        holdout_start.tz_convert("UTC").to_pydatetime(),
        type=pa.timestamp("ns", tz="UTC"),
    )
    horizon_sessions = int(parameters["horizon_sessions"])
    target_field = _text(parameters, "target_field")
    _validate_label_row_group_boundaries(label_root)
    development_labels, label_metadata = _load_label_slice(
        label_root,
        columns=(
            "entity_id",
            "observation_session",
            "decision_time",
            "label_start_time",
            "label_end_time",
            "available_time",
            "horizon_sessions",
            target_field,
            "lineage_hash",
        ),
        predicate=(ds.field("label_end_time") < holdout_scan_time)
        & (ds.field("available_time") <= holdout_scan_time)
        & (ds.field("horizon_sessions") == horizon_sessions),
        frame_budget=frame_budget,
        label="Walk-forward development labels",
    )
    if feature_metadata.get("semantics_hash") != label_metadata.get("semantics_hash"):
        raise ModelMainlineError("Walk-forward Feature/Label 语义身份不一致")
    samples, feature_columns = assemble_daily_model_samples(
        features, development_labels, horizon_sessions=horizon_sessions,
        target_field=target_field,
    )
    frame_budget.reserve_frame(samples, label="Walk-forward development samples")
    frame_budget.release_frame(development_labels)
    visible_at = pd.Timestamp(fixed_clock)
    if visible_at.tzinfo is None:
        raise ModelMainlineError("模型 Runtime fixed_clock 必须包含时区")
    if (pd.to_datetime(samples["label_available_time"], utc=True) > visible_at).any():
        raise ModelMainlineError("模型 Runtime fixed_clock 早于样本 Label 的真实可见时间")
    holdout_labels, _ = _load_label_slice(
        label_root,
        columns=(
            "entity_id",
            "observation_session",
            "decision_time",
            "available_time",
            "horizon_sessions",
        ),
        predicate=(ds.field("decision_time") >= holdout_scan_time)
        & (ds.field("horizon_sessions") == horizon_sessions),
        frame_budget=frame_budget,
        label="Walk-forward holdout label index",
    )
    if samples.empty or holdout_labels.empty:
        raise ModelMainlineError("development/locked holdout 必须同时非空")
    if holdout_labels.duplicated(["entity_id", "observation_session"]).any():
        raise ModelMainlineError("locked holdout 的目标 horizon Label 不唯一")
    holdout_index = pd.DataFrame(
        {
            "sample_id": (
                holdout_labels["entity_id"].astype(str)
                + ":"
                + pd.to_datetime(holdout_labels["observation_session"]).dt.strftime("%Y-%m-%d")
                + f":h{horizon_sessions}"
            ),
            "label_available_time": pd.to_datetime(
                holdout_labels["available_time"], utc=True, errors="raise",
            ),
            "observation_time": pd.to_datetime(
                holdout_labels["observation_session"], utc=True, errors="raise",
            ),
        }
    )
    frame_budget.reserve_frame(holdout_index, label="Walk-forward holdout index")
    frame_budget.release_frame(holdout_labels)
    if holdout_index["sample_id"].duplicated().any():
        raise ModelMainlineError("locked holdout sample_id 必须唯一")
    last_development_session = samples["observation_time"].max().date()
    calendar = tuple(
        session for session in (pd.Timestamp(value).date() for value in parameters["calendar_sessions"])
        if session <= last_development_session
    )
    split = build_walk_forward(
        samples.loc[:, ["sample_id", "observation_time", "decision_time", "label_start_time", "label_end_time", "label_available_time"]].rename(
            columns={"label_start_time": "label_start", "label_end_time": "label_end"}
        ),
        calendar=calendar,
        train_sessions=int(parameters["train_sessions"]),
        validation_sessions=int(parameters["validation_sessions"]),
        test_sessions=int(parameters["test_sessions"]),
        step_sessions=int(parameters["step_sessions"]),
        embargo_sessions=int(parameters["embargo_sessions"]),
        expanding=_boolean(parameters, "expanding"),
    )
    return _write_artifact(
        output_root,
        {"samples": samples, "holdout_index": holdout_index, "split_audit": split.audit},
        status="model_split_succeeded",
        extra={
            "feature_columns": list(feature_columns),
            "split_manifest": _split_payload(split),
            "semantics_hash": feature_metadata.get("semantics_hash"),
            "holdout_start": holdout_start.isoformat(),
            "holdout_end": pd.to_datetime(
                holdout_index["observation_time"], utc=True
            ).max().date().isoformat(),
            "fixed_clock": visible_at.isoformat(),
            "horizon_sessions": horizon_sessions,
            "target_field": target_field,
            "source_feature_table_hash": feature_metadata["table_hashes"]["features"],
            "source_label_table_hash": label_metadata["table_hashes"]["labels"],
        },
    )


def execute_model_preprocess_artifact(
    *,
    split_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    root_seed: int,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    samples, metadata = _load_table(
        split_root, "samples", frame_budget=frame_budget,
    )
    audit, _ = _load_table(
        split_root, "split_audit", frame_budget=frame_budget,
    )
    split = _split_from_payload(metadata["split_manifest"], audit)
    feature_columns = tuple(str(value) for value in metadata["feature_columns"])
    frame_budget.require_additional(
        pandas_frame_bytes(samples),
        label="Walk-forward sample index 副本",
    )
    index = samples.set_index("sample_id", drop=False)
    frame_budget.reserve_frame(index, label="Walk-forward sample index")
    frame_budget.release_frame(samples)
    del samples
    preprocessor_rows: list[dict[str, object]] = []
    def transformed_partitions() -> Iterable[pd.DataFrame]:
        for fold in split.folds:
            train = index.loc[list(fold.train_ids)].copy()
            evaluation_ids = (*fold.validation_ids, *fold.test_ids)
            evaluation = index.loc[list(evaluation_ids)].copy()
            frame_budget.reserve_frame(train, label=f"{fold.fold_id} train 输入")
            frame_budget.reserve_frame(
                evaluation,
                label=f"{fold.fold_id} evaluation 输入",
            )
            transformed, artifact = fit_fold_preprocessor(
                train, evaluation, feature_columns=feature_columns,
                feature_selection_k=_optional_positive_int(parameters, "feature_selection_k"),
                preprocessing=_text(parameters, "preprocessing"), split_manifest=split,
                fold_id=fold.fold_id, root_seed=root_seed,
                research_identity_hash=_hash(parameters, "research_identity_hash"),
            )
            transformed_train = transformed["train"]
            transformed_evaluation = transformed["evaluation"]
            frame_budget.reserve_frame(
                transformed_train,
                label=f"{fold.fold_id} transformed train 原始输出",
            )
            frame_budget.reserve_frame(
                transformed_evaluation,
                label=f"{fold.fold_id} transformed evaluation 原始输出",
            )
            frame_budget.require_additional(
                pandas_frame_bytes(transformed_train),
                label=f"{fold.fold_id} train 分区副本",
            )
            train_frame = transformed_train.copy()
            train_frame["fold_id"] = fold.fold_id
            train_frame["fold_role"] = "train"
            frame_budget.reserve_frame(train_frame, label=f"{fold.fold_id} train 输出")
            frame_budget.require_additional(
                pandas_frame_bytes(transformed_evaluation),
                label=f"{fold.fold_id} evaluation 分区副本",
            )
            evaluation_frame = transformed_evaluation.copy()
            evaluation_frame["fold_id"] = fold.fold_id
            validation_ids = set(fold.validation_ids)
            evaluation_frame["fold_role"] = evaluation_frame["sample_id"].map(
                lambda value: "validation" if value in validation_ids else "test"
            )
            frame_budget.reserve_frame(
                evaluation_frame,
                label=f"{fold.fold_id} evaluation 输出",
            )
            frame_budget.require_additional(
                2 * pandas_frame_bytes(evaluation_frame),
                label=f"{fold.fold_id} validation/test 分区副本",
            )
            validation_frame = evaluation_frame.loc[
                evaluation_frame["fold_role"] == "validation"
            ].copy()
            test_frame = evaluation_frame.loc[
                evaluation_frame["fold_role"] == "test"
            ].copy()
            frame_budget.reserve_frame(
                validation_frame,
                label=f"{fold.fold_id} validation 输出",
            )
            frame_budget.reserve_frame(
                test_frame,
                label=f"{fold.fold_id} test 输出",
            )
            frame_budget.release_frame(train)
            frame_budget.release_frame(evaluation)
            frame_budget.release_frame(transformed_train)
            frame_budget.release_frame(transformed_evaluation)
            frame_budget.release_frame(evaluation_frame)
            preprocessor_rows.append({
                "fold_id": fold.fold_id,
                "fit_time": pd.to_datetime(validation_frame["decision_time"], utc=True).min().isoformat(),
                "preprocessor_hash": artifact["preprocessor_hash"],
                "fit_scope_certificate_hash": artifact["fit_scope_certificate_hash"],
                "fit_binding_hash": artifact["fit_binding_hash"],
                "selected_columns_json": canonical_json(artifact["selected_columns"]),
                "preprocessor_json": canonical_json(artifact),
            })
            yield train_frame
            frame_budget.release_frame(train_frame)
            yield validation_frame
            frame_budget.release_frame(validation_frame)
            yield test_frame
            frame_budget.release_frame(test_frame)

    return _write_partitioned_artifact(
        output_root,
        partitioned_tables={"transformed_samples": transformed_partitions()},
        tables=lambda: {"preprocessors": pd.DataFrame(preprocessor_rows)},
        status="model_preprocess_succeeded",
        extra={
            "split_manifest": metadata["split_manifest"],
            "holdout_start": metadata["holdout_start"],
            "feature_columns": list(feature_columns),
            "semantics_hash": metadata.get("semantics_hash"),
            "root_seed": root_seed,
            "research_identity_hash": _hash(parameters, "research_identity_hash"),
            "preprocessing": _text(parameters, "preprocessing"),
            "feature_selection_k": _optional_positive_int(parameters, "feature_selection_k"),
        },
    )


def execute_model_fit_artifact(
    *,
    preprocess_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    root_seed: int,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    metadata = _read_artifact_metadata(preprocess_root)
    preprocessors, _ = _load_table(
        preprocess_root,
        "preprocessors",
        frame_budget=frame_budget,
    )
    candidates = _candidates(parameters)
    if (
        metadata.get("preprocessing") != _text(parameters, "preprocessing")
        or metadata.get("feature_selection_k") != _optional_positive_int(parameters, "feature_selection_k")
        or metadata.get("research_identity_hash") != _hash(parameters, "research_identity_hash")
        or metadata.get("root_seed") != root_seed
    ):
        raise ModelMainlineError("模型拟合参数与预处理工件身份不一致")
    simple_gate = _boolean(parameters, "simple_model_gate_passed")
    thread_count = int(parameters["thread_count"])
    preflight = model_dependency_preflight(
        candidates, simple_model_gate_passed=simple_gate, thread_count=thread_count,
    )
    manifest = _search_manifest(candidates, parameters)
    fit_ledger = TrialLedger(manifest)
    candidate_ids = {item.parameter_hash: item.candidate_id for item in manifest.candidates}
    model_rows: list[dict[str, object]] = []
    candidate_rows = {item.candidate_id: [] for item in manifest.candidates}
    failures: dict[str, str] = {}
    active_candidates = []
    for candidate in candidates:
        candidate_id = candidate_ids[typed_canonical_hash(candidate)]
        if candidate["model_id"] == "lightgbm" and not simple_gate:
            candidate_rows[candidate_id].append({
                "candidate_id": candidate_id, "fold_id": "all", "status": "NOT_RUN",
                "reason_code": "simple_model_gate_not_passed", "model_hash": None,
                "model_json": None,
            })
        else:
            active_candidates.append((candidate_id, candidate))
    seen_folds: set[str] = set()
    if active_candidates:
        for train in _iter_table_frames(
            preprocess_root, "transformed_samples", frame_budget=frame_budget,
            fold_role="train",
        ):
            fold_id = str(train["fold_id"].iloc[0])
            if fold_id in seen_folds:
                raise ModelMainlineError(f"重复的训练 fold 分区: {fold_id}")
            seen_folds.add(fold_id)
            preprocessor = preprocessors.loc[preprocessors["fold_id"] == fold_id]
            if len(preprocessor) != 1:
                raise ModelMainlineError("每个 fold 必须恰好有一个预处理工件")
            selected_columns = tuple(json.loads(preprocessor.iloc[0]["selected_columns_json"]))
            for candidate_id, candidate in active_candidates:
                try:
                    model = fit_model_candidate(
                        train, candidate=candidate, target_kind=_text(parameters, "target_kind"),
                        feature_columns=selected_columns, root_seed=root_seed,
                        thread_count=thread_count, preflight_hash=str(preflight["preflight_hash"]),
                    )
                    candidate_rows[candidate_id].append({
                        "candidate_id": candidate_id, "fold_id": fold_id, "status": "fitted",
                        "reason_code": None, "model_hash": model["model_hash"],
                        "model_json": canonical_json(model),
                    })
                except CandidateFitRejected as exc:
                    failures.setdefault(candidate_id, exc.reason_code)
                    candidate_rows[candidate_id].append({
                        "candidate_id": candidate_id, "fold_id": fold_id, "status": "failed",
                        "reason_code": exc.reason_code, "model_hash": None, "model_json": None,
                    })
        if seen_folds != set(preprocessors["fold_id"].astype(str)):
            raise ModelMainlineError("训练 fold 分区集合与预处理工件不一致")
    # 拟合按 fold 共享输入；输出与账本仍按冻结候选顺序提交。
    for candidate in candidates:
        candidate_id = candidate_ids[typed_canonical_hash(candidate)]
        fit_ledger.start(candidate_id)
        model_rows.extend(candidate_rows[candidate_id])
        if candidate_rows[candidate_id][0]["status"] == "NOT_RUN":
            fit_ledger.prune(candidate_id, reason_code="simple_model_gate_not_passed")
        elif candidate_id in failures:
            fit_ledger.fail(candidate_id, reason_code=failures[candidate_id])
    return _write_artifact(
        output_root,
        {
            "models": pd.DataFrame(model_rows),
            "trial_events": pd.DataFrame([event.__dict__ for event in fit_ledger.events]),
        },
        status="model_fit_succeeded",
        extra={
            "search_manifest": _search_payload(manifest),
            "preflight": preflight,
            "target_kind": _text(parameters, "target_kind"),
            "objective": _text(parameters, "objective"),
            "preprocess_artifact_hash": metadata["artifact_hash"],
            "fit_trial_ledger_hash": fit_ledger.ledger_hash,
            "research_identity_hash": metadata["research_identity_hash"],
            "preprocessing": metadata["preprocessing"],
            "feature_selection_k": metadata["feature_selection_k"],
            "root_seed": root_seed,
        },
    )


def execute_model_predict_artifact(
    *,
    preprocess_root: str | Path,
    model_root: str | Path,
    output_root: str | Path,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    preprocess_metadata = _read_artifact_metadata(preprocess_root)
    models, model_metadata = _load_table(
        model_root,
        "models",
        frame_budget=frame_budget,
    )
    if model_metadata.get("preprocess_artifact_hash") != preprocess_metadata.get("artifact_hash"):
        raise ModelMainlineError("模型工件与预处理工件身份不一致")
    def prediction_partitions() -> Iterable[pd.DataFrame]:
        fitted = models.loc[models["status"] == "fitted"]
        seen_folds: set[str] = set()
        for fold_frame in _iter_table_frames(
            preprocess_root, "transformed_samples", frame_budget=frame_budget,
            fold_role="validation",
        ):
            fold_id = str(fold_frame["fold_id"].iloc[0])
            if fold_id in seen_folds:
                raise ModelMainlineError(f"重复的 validation fold 分区: {fold_id}")
            seen_folds.add(fold_id)
            for model_row in fitted.loc[fitted["fold_id"].astype(str) == fold_id].itertuples(index=False):
                predictions = predict_model(json.loads(model_row.model_json), fold_frame)
                output = _prediction_frame(
                    fold_frame, predictions, str(model_row.candidate_id),
                    fold_id, "validation", str(model_row.model_hash),
                )
                frame_budget.reserve_frame(output, label=f"{model_row.candidate_id}/{fold_id} validation 预测")
                yield output
                frame_budget.release_frame(output)
        if not set(fitted["fold_id"].astype(str)) <= seen_folds:
            raise ModelMainlineError("模型缺少 validation fold 分区")

    return _write_partitioned_artifact(
        output_root,
        partitioned_tables={"validation_predictions": prediction_partitions()},
        partition_order={
            "validation_predictions": {
                (str(row.candidate_id), str(row.fold_id)): index
                for index, row in enumerate(models.loc[models["status"] == "fitted"].itertuples(index=False))
            },
        },
        tables=lambda: {},
        status="model_validation_prediction_succeeded",
        extra={
            "search_manifest": model_metadata["search_manifest"],
            "target_kind": model_metadata["target_kind"],
            "objective": model_metadata["objective"],
        },
    )


def execute_model_fold_metrics_artifact(
    *,
    prediction_root: str | Path,
    model_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    prediction_metadata = _read_artifact_metadata(prediction_root)
    models, model_metadata = _load_table(
        model_root,
        "models",
        frame_budget=frame_budget,
    )
    if prediction_metadata.get("search_manifest") != model_metadata.get("search_manifest"):
        raise ModelMainlineError("预测与模型的 SearchManifest 身份不一致")
    objective = _text(parameters, "objective")
    target_kind = _text(parameters, "target_kind")
    manifest = _search_from_payload(prediction_metadata["search_manifest"])
    if (
        objective != model_metadata.get("objective")
        or objective != manifest.objective
        or target_kind != model_metadata.get("target_kind")
    ):
        raise ModelMainlineError("fold metrics 的目标合同与模型 SearchManifest 不一致")
    metric_rows: list[dict[str, object]] = []
    seen_groups: set[tuple[str, str]] = set()
    for frame in _iter_table_frames(
        prediction_root,
        "validation_predictions",
        frame_budget=frame_budget,
    ):
        candidate_ids = set(frame["candidate_id"].astype(str))
        fold_ids = set(frame["fold_id"].astype(str))
        if len(candidate_ids) != 1 or len(fold_ids) != 1:
            raise ModelMainlineError("validation 预测分区必须只包含一个候选和 fold")
        candidate_id = next(iter(candidate_ids))
        fold_id = next(iter(fold_ids))
        group = (candidate_id, fold_id)
        if group in seen_groups:
            raise ModelMainlineError("validation 预测候选/fold 分区重复")
        seen_groups.add(group)
        metric_rows.append({
            "candidate_id": candidate_id, "fold_id": fold_id, "stage": "validation",
            objective: score_model(
                frame["actual"].to_numpy(float), frame["prediction"].to_numpy(float),
                objective=objective, target_kind=target_kind,
            ),
            "sample_count": len(frame),
            "label_end_time": pd.to_datetime(frame["label_end_time"], utc=True).max().isoformat(),
            "label_available_time": pd.to_datetime(frame["label_available_time"], utc=True).max().isoformat(),
        })
    metrics = pd.DataFrame(metric_rows)
    all_aggregate = metrics.groupby("candidate_id", as_index=False).agg(
        stage=("stage", "first"), **{objective: (objective, "mean")},
    )
    ledger = _replay_ledger(manifest, models, all_aggregate, objective)
    completed_ids = {candidate_id for candidate_id, state in ledger.states.items() if state == "completed"}
    aggregate = all_aggregate.loc[all_aggregate["candidate_id"].isin(completed_ids)].reset_index(drop=True)
    return _write_artifact(
        output_root,
        {"fold_metrics": metrics, "candidate_metrics": aggregate, "trial_events": pd.DataFrame([event.__dict__ for event in ledger.events])},
        status="model_fold_metrics_succeeded",
        extra={
            "search_manifest": prediction_metadata["search_manifest"],
            "trial_ledger_hash": ledger.ledger_hash,
            "objective": objective,
            "target_kind": target_kind,
        },
    )


def execute_model_selection_artifact(
    *,
    metrics_root: str | Path,
    preprocess_root: str | Path,
    model_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    candidate_metrics, metrics_metadata = _load_table(
        metrics_root,
        "candidate_metrics",
        frame_budget=frame_budget,
    )
    preprocess_metadata = _read_artifact_metadata(preprocess_root)
    models, model_metadata = _load_table(
        model_root,
        "models",
        frame_budget=frame_budget,
    )
    if (
        metrics_metadata.get("search_manifest") != model_metadata.get("search_manifest")
        or model_metadata.get("preprocess_artifact_hash") != preprocess_metadata.get("artifact_hash")
    ):
        raise ModelMainlineError("选择阶段的 SearchManifest 或预处理身份不一致")
    objective = _text(parameters, "objective")
    manifest = _search_from_payload(metrics_metadata["search_manifest"])
    if (
        objective != metrics_metadata.get("objective")
        or objective != model_metadata.get("objective")
        or _text(parameters, "target_kind") != metrics_metadata.get("target_kind")
        or _text(parameters, "target_kind") != model_metadata.get("target_kind")
        or _text(parameters, "direction") != manifest.direction
    ):
        raise ModelMainlineError("选择阶段的目标、方向或 target_kind 合同不一致")
    selected = select_by_validation(
        candidate_metrics, objective=objective, direction=_text(parameters, "direction"),
    )
    winner = str(selected["candidate_id"])
    fold_metrics, _ = _load_table(metrics_root, "fold_metrics", frame_budget=frame_budget)
    preprocessors, _ = _load_table(preprocess_root, "preprocessors", frame_budget=frame_budget)
    fit_times = dict(zip(preprocessors["fold_id"].astype(str), pd.to_datetime(preprocessors["fit_time"], utc=True)))
    metric_available = pd.to_datetime(fold_metrics["label_available_time"], utc=True)
    metric_end = pd.to_datetime(fold_metrics["label_end_time"], utc=True)
    final_selection_time = metric_available.max()
    if final_selection_time > pd.Timestamp(preprocess_metadata["holdout_start"]):
        raise ModelMainlineError("最终候选选择晚于 locked holdout 起点")
    test_metric_rows: list[dict[str, object]] = []
    fold_selection_rows: list[dict[str, object]] = []
    fold_trial_rows: list[dict[str, object]] = []
    final_ledger = _replay_ledger(manifest, models, candidate_metrics, objective)
    selected_parameters = next(dict(item.parameters) for item in manifest.candidates if item.candidate_id == winner)
    extra = {
        "split_manifest_hash": preprocess_metadata["split_manifest"]["manifest_hash"],
        "semantics_hash": preprocess_metadata.get("semantics_hash"),
        "final_fit_contract": {
            "preprocessing": model_metadata["preprocessing"],
            "feature_selection_k": model_metadata["feature_selection_k"],
            "target_kind": model_metadata["target_kind"],
            "objective": model_metadata["objective"],
            "research_identity_hash": model_metadata["research_identity_hash"],
            "thread_count": int(model_metadata["preflight"]["thread_count"]),
            "root_seed": model_metadata["root_seed"],
        },
    }

    def test_prediction_partitions() -> Iterable[pd.DataFrame]:
        seen_folds: set[str] = set()
        for fold_frame in _iter_table_frames(
            preprocess_root, "transformed_samples", frame_budget=frame_budget,
            fold_role="test",
        ):
            fold_id = str(fold_frame["fold_id"].iloc[0])
            if fold_id in seen_folds:
                raise ModelMainlineError(f"重复的 test fold 分区: {fold_id}")
            seen_folds.add(fold_id)
            selection_time = pd.to_datetime(fold_frame["decision_time"], utc=True).min()
            # 全个 validation 分数可见后才能参与选择；未来 fold 的成败也不可回填。
            visible_metrics = fold_metrics.loc[
                (metric_available <= selection_time) & (metric_end < selection_time)
            ]
            aggregate = visible_metrics.groupby("candidate_id", as_index=False).agg(
                stage=("stage", "first"), **{objective: (objective, "mean")},
            )
            visible_models = models.loc[
                models["fold_id"].astype(str).map(
                    lambda value: value == "all" or fit_times[value] <= selection_time
                )
            ]
            ledger = _replay_ledger(manifest, visible_models, aggregate, objective)
            completed_ids = {key for key, state in ledger.states.items() if state == "completed"}
            current_models = models.loc[
                (models["fold_id"].astype(str) == fold_id) & (models["status"] == "fitted")
            ]
            eligible_ids = completed_ids & set(current_models["candidate_id"])
            local_selected = select_by_validation(
                aggregate.loc[aggregate["candidate_id"].isin(eligible_ids)],
                objective=objective, direction=manifest.direction,
            )
            local_winner = str(local_selected["candidate_id"])
            model_rows = current_models.loc[current_models["candidate_id"] == local_winner]
            if len(model_rows) != 1:
                raise ModelMainlineError("每个 test fold 必须绑定唯一已拟合模型")
            model_row = model_rows.iloc[0]
            predictions = predict_model(json.loads(model_row["model_json"]), fold_frame)
            output = _prediction_frame(
                fold_frame, predictions, local_winner, fold_id, "test", str(model_row["model_hash"]),
            )
            metric = score_model(
                output["actual"].to_numpy(float), output["prediction"].to_numpy(float),
                objective=objective, target_kind=_text(parameters, "target_kind"),
            )
            test_metric_rows.append({
                "candidate_id": local_winner, "fold_id": fold_id, "stage": "test",
                objective: metric, "sample_count": len(output),
            })
            frozen_ledger_hash = ledger.ledger_hash
            ledger.record_final_evaluation(local_winner, stage="test", metric_value=metric)
            fold_selection_rows.append({
                "fold_id": fold_id, "selected_candidate_id": local_winner,
                "selection_time": selection_time.isoformat(),
                "validation_metric": float(local_selected[objective]),
                "validation_fold_count": int((visible_metrics["candidate_id"] == local_winner).sum()),
                "selection_ledger_hash": frozen_ledger_hash,
                "trial_ledger_hash": ledger.ledger_hash,
            })
            fold_trial_rows.extend({"fold_id": fold_id, **event.__dict__} for event in ledger.events)
            frame_budget.reserve_frame(output, label=f"{local_winner}/{fold_id} test 预测")
            yield output
            frame_budget.release_frame(output)
        if seen_folds != set(preprocessors["fold_id"].astype(str)):
            raise ModelMainlineError("test fold 分区集合与预处理工件不一致")

    def selection_tables() -> Mapping[str, pd.DataFrame]:
        if not test_metric_rows:
            raise ModelMainlineError("没有可用的逐时点 test 预测")
        selection = {
            "selected_candidate_id": winner,
            "selected_parameters": selected_parameters,
            "validation_metric": float(selected[objective]),
            "test_metric": float(pd.DataFrame(test_metric_rows)[objective].mean()),
            "test_metric_scope": "walk_forward_frozen_candidates",
            "selection_scope": "subsequent_locked_holdout",
            "selection_time": final_selection_time.isoformat(),
            "objective": objective, "direction": manifest.direction,
            "search_manifest_hash": manifest.manifest_hash,
            "trial_ledger_hash": final_ledger.ledger_hash,
        }
        selection["selection_hash"] = typed_canonical_hash(selection)
        extra["selection"] = selection
        return {
            "selection": pd.DataFrame([{**selection, "selected_parameters_json": canonical_json(selected_parameters)}]),
            "fold_selections": pd.DataFrame(fold_selection_rows),
            "test_metrics": pd.DataFrame(test_metric_rows),
            "trial_events": pd.DataFrame([event.__dict__ for event in final_ledger.events]),
            "fold_trial_events": pd.DataFrame(fold_trial_rows),
        }

    return _write_partitioned_artifact(
        output_root,
        partitioned_tables={"test_predictions": test_prediction_partitions()},
        tables=selection_tables,
        status="model_selection_succeeded",
        extra=extra,
    )


def execute_model_locked_holdout_artifact(
    *,
    split_root: str | Path,
    selection_root: str | Path,
    feature_root: str | Path,
    label_root: str | Path,
    parameters: Mapping[str, object],
    output_root: str | Path,
    ledger_root: str | Path,
    root_seed: int,
    max_memory_bytes: int,
) -> dict[str, object]:
    frame_budget = PandasFrameBudget(max_memory_bytes)
    development_samples, split_metadata = _load_table(
        split_root, "samples", frame_budget=frame_budget,
    )
    holdout_index, _ = _load_table(
        split_root, "holdout_index", frame_budget=frame_budget,
    )
    selection_table, selection_metadata = _load_table(
        selection_root, "selection", frame_budget=frame_budget,
    )
    if len(selection_table) != 1:
        raise ModelMainlineError("locked holdout 必须绑定唯一 selection")
    if selection_metadata.get("split_manifest_hash") != split_metadata["split_manifest"]["manifest_hash"]:
        raise ModelMainlineError("locked holdout 的 SplitManifest 与 selection 不一致")
    expected_fit_contract = {
        "preprocessing": _text(parameters, "preprocessing"),
        "feature_selection_k": _optional_positive_int(parameters, "feature_selection_k"),
        "target_kind": _text(parameters, "target_kind"),
        "objective": _text(parameters, "objective"),
        "research_identity_hash": _hash(parameters, "research_identity_hash"),
        "thread_count": int(parameters["thread_count"]),
        "root_seed": root_seed,
    }
    if selection_metadata.get("final_fit_contract") != expected_fit_contract:
        raise ModelMainlineError("locked holdout 的最终拟合合同与 selection 不一致")
    selected_parameters = json.loads(selection_table.iloc[0]["selected_parameters_json"])

    def preflight_holdout_samples() -> Mapping[str, object]:
        feature_schema = _preflight_table(feature_root, "features")
        label_schema = _preflight_table(label_root, "labels")
        required_feature = {
            "entity_id", "observation_session", "observation_time",
            "available_time", "window_sessions", "feature_id", "value",
            "status", "lineage_hash",
        }
        required_label = {
            "entity_id", "observation_session", "decision_time",
            "label_start_time", "label_end_time", "available_time",
            "horizon_sessions", str(split_metadata["target_field"]), "lineage_hash",
        }
        if not required_feature <= set(feature_schema["columns"]):
            raise ModelMainlineError("locked holdout Feature schema 不闭合")
        if not required_label <= set(label_schema["columns"]):
            raise ModelMainlineError("locked holdout Label schema 不闭合")
        return {"features": feature_schema, "labels": label_schema}

    def load_holdout_samples() -> pd.DataFrame:
        features, feature_metadata = _load_table(
            feature_root, "features", frame_budget=frame_budget,
        )
        labels, label_metadata = _load_table(
            label_root, "labels", frame_budget=frame_budget,
        )
        semantics_hash = split_metadata.get("semantics_hash")
        if (
            feature_metadata.get("semantics_hash") != semantics_hash
            or label_metadata.get("semantics_hash") != semantics_hash
        ):
            raise ModelMainlineError("locked holdout 的 Feature/Label 语义身份与 split 不一致")
        if (
            feature_metadata.get("table_hashes", {}).get("features")
            != split_metadata.get("source_feature_table_hash")
            or label_metadata.get("table_hashes", {}).get("labels")
            != split_metadata.get("source_label_table_hash")
        ):
            raise ModelMainlineError("locked holdout 的原始 Feature/Label 工件与 split 不一致")
        samples, feature_columns = assemble_daily_model_samples(
            features,
            labels,
            horizon_sessions=int(split_metadata["horizon_sessions"]),
            target_field=str(split_metadata["target_field"]),
        )
        frame_budget.reserve_frame(samples, label="Walk-forward holdout samples")
        if list(feature_columns) != list(split_metadata["feature_columns"]):
            raise ModelMainlineError("locked holdout 的 Feature 列与 split 不一致")
        wanted = set(holdout_index["sample_id"].astype(str))
        selected_samples = samples.loc[
            samples["sample_id"].astype(str).isin(wanted)
        ].copy()
        frame_budget.reserve_frame(
            selected_samples,
            label="Walk-forward selected holdout samples",
        )
        frame_budget.release_frame(samples)
        frame_budget.release_frame(features)
        frame_budget.release_frame(labels)
        return selected_samples

    result = evaluate_locked_holdout(
        development_samples,
        holdout_preflight=preflight_holdout_samples,
        holdout_loader=load_holdout_samples,
        development_ids=tuple(development_samples["sample_id"]),
        holdout_ids=tuple(holdout_index["sample_id"]),
        holdout_start=str(split_metadata["holdout_start"]),
        holdout_end=str(split_metadata["holdout_end"]),
        feature_columns=tuple(split_metadata["feature_columns"]),
        selected_candidate=selected_parameters,
        target_kind=_text(parameters, "target_kind"),
        objective=_text(parameters, "objective"),
        preprocessing=_text(parameters, "preprocessing"),
        feature_selection_k=_optional_positive_int(parameters, "feature_selection_k"),
        research_identity_hash=_hash(parameters, "research_identity_hash"),
        data_snapshot_hash=typed_canonical_hash(
            {
                "feature_table": split_metadata["source_feature_table_hash"],
                "label_table": split_metadata["source_label_table_hash"],
                "holdout_ids": sorted(holdout_index["sample_id"].astype(str)),
            }
        ),
        selection_hash=str(selection_metadata["selection"]["selection_hash"]),
        package_hash=_hash(parameters, "package_hash"),
        implementation_hash=_hash(parameters, "implementation_hash"),
        actor=_text(parameters, "holdout_actor"),
        reason=_text(parameters, "holdout_reason"),
        unlock_at=pd.Timestamp(_text(parameters, "holdout_unlock_at")).to_pydatetime(),
        fixed_clock=pd.Timestamp(_text(parameters, "fixed_clock")).to_pydatetime(),
        ledger_root=ledger_root,
        root_seed=root_seed,
        thread_count=int(parameters["thread_count"]),
    )
    receipt = {key: value for key, value in result.items() if key != "predictions"}
    persistent_ledger_root = (
        Path(ledger_root) / str(result["holdout_identity_hash"])
    )
    ledger_destination = Path(output_root) / "holdout-ledger"
    ledger_destination.mkdir(parents=True, exist_ok=False)
    for name in ("plan.json", "prepared.json", "opened.json", "terminal.json"):
        source = persistent_ledger_root / name
        if not source.is_file():
            raise ModelMainlineError(f"locked holdout 缺少持久账本文件: {name}")
        shutil.copy2(source, ledger_destination / name)
    return _write_artifact(
        output_root,
        {"holdout_predictions": pd.DataFrame(result["predictions"]), "holdout_receipt": pd.DataFrame([receipt])},
        status="model_locked_holdout_succeeded",
        extra={"holdout": receipt},
    )


def _replay_ledger(manifest: object, models: pd.DataFrame, aggregate: pd.DataFrame, objective: str) -> TrialLedger:
    ledger = TrialLedger(manifest)
    for candidate in manifest.candidates:
        candidate_id = candidate.candidate_id
        candidate_models = models.loc[models["candidate_id"] == candidate_id]
        ledger.start(candidate_id)
        if not candidate_models.empty and set(candidate_models["status"]) == {"NOT_RUN"}:
            ledger.prune(candidate_id, reason_code="simple_model_gate_not_passed")
        elif candidate_models.empty or "failed" in set(candidate_models["status"]):
            reason = next((str(value) for value in candidate_models["reason_code"] if pd.notna(value)), "model_fit_failed")
            ledger.fail(candidate_id, reason_code=reason)
        else:
            metric = aggregate.loc[aggregate["candidate_id"] == candidate_id, objective]
            if len(metric) != 1:
                ledger.fail(candidate_id, reason_code="validation_metric_missing")
            else:
                ledger.complete(candidate_id, validation_metric=float(metric.iloc[0]))
    ledger.require_terminal()
    return ledger


def _search_manifest(candidates: list[dict[str, object]], parameters: Mapping[str, object]):
    return build_search_manifest(
        search_id=_text(parameters, "search_id"), candidates=candidates, method="grid",
        max_trials=len(candidates), max_parallel=1,
        stopping_condition="complete_declared_candidate_universe",
        objective=_text(parameters, "objective"), direction=_text(parameters, "direction"),
        frozen_at=pd.Timestamp(_text(parameters, "search_frozen_at")).to_pydatetime(),
    )


def _search_payload(manifest: object) -> dict[str, object]:
    return {
        "search_id": manifest.search_id,
        "candidates": [
            {
                "candidate_id": item.candidate_id, "parameters": dict(item.parameters),
                "parameter_hash": item.parameter_hash,
            }
            for item in manifest.candidates
        ],
        "method": manifest.method, "max_trials": manifest.max_trials,
        "max_parallel": manifest.max_parallel, "stopping_condition": manifest.stopping_condition,
        "objective": manifest.objective, "direction": manifest.direction,
        "frozen_at": manifest.frozen_at.isoformat(), "stage": manifest.stage,
        "manifest_hash": manifest.manifest_hash,
    }


def _search_from_payload(payload: Mapping[str, object]):
    manifest = build_search_manifest(
        search_id=str(payload["search_id"]),
        candidates=[dict(item["parameters"]) for item in payload["candidates"]],
        method=str(payload["method"]), max_trials=int(payload["max_trials"]),
        max_parallel=int(payload["max_parallel"]),
        stopping_condition=str(payload["stopping_condition"]), objective=str(payload["objective"]),
        direction=str(payload["direction"]), frozen_at=datetime.fromisoformat(str(payload["frozen_at"])),
        stage=str(payload["stage"]),
    )
    if manifest.manifest_hash != payload["manifest_hash"]:
        raise ModelMainlineError("SearchManifest 工件发生漂移")
    return manifest


def _split_payload(split: SplitManifest) -> dict[str, object]:
    return {
        "method": split.method, "calendar_hash": split.calendar_hash,
        "manifest_hash": split.manifest_hash,
        "folds": [
            {
                "fold_id": fold.fold_id, "train_ids": list(fold.train_ids),
                "validation_ids": list(fold.validation_ids), "test_ids": list(fold.test_ids),
                "purged_ids": list(fold.purged_ids), "embargoed_ids": list(fold.embargoed_ids),
            }
            for fold in split.folds
        ],
    }


def _split_from_payload(payload: Mapping[str, object], audit: pd.DataFrame) -> SplitManifest:
    folds = tuple(
        SplitFold(
            str(item["fold_id"]), tuple(item["train_ids"]), tuple(item["validation_ids"]),
            tuple(item["test_ids"]), tuple(item["purged_ids"]), tuple(item["embargoed_ids"]),
        )
        for item in payload["folds"]
    )
    split = SplitManifest(
        str(payload["method"]), folds, audit, str(payload["calendar_hash"]), str(payload["manifest_hash"]),
    )
    if _split_payload(split) != payload:
        raise ModelMainlineError("SplitManifest 工件发生漂移")
    return split


def _write_artifact(
    output_root: str | Path,
    tables: Mapping[str, pd.DataFrame],
    *,
    status: str,
    extra: Mapping[str, object],
) -> dict[str, object]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    table_hashes: dict[str, str] = {}
    row_counts: dict[str, int] = {}
    for name, frame in sorted(tables.items()):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(directory / "part-00000.parquet", index=False)
        row_counts[name] = len(frame)
        table_hashes[name] = typed_canonical_hash_streamed(_records(frame))
    payload = {
        "contract_version": WALK_FORWARD_MODEL_VERSION, "status": status,
        "row_counts": row_counts, "table_hashes": table_hashes, **dict(extra),
    }
    payload["artifact_hash"] = typed_canonical_hash(payload)
    (root / "artifact-metadata.json").write_text(canonical_json(payload), encoding="utf-8")
    return payload


def _write_partitioned_artifact(
    output_root: str | Path,
    *,
    partitioned_tables: Mapping[str, Iterable[pd.DataFrame]],
    tables: Callable[[], Mapping[str, pd.DataFrame]],
    status: str,
    extra: Mapping[str, object],
    partition_order: Mapping[str, Mapping[tuple[str, str], int]] | None = None,
) -> dict[str, object]:
    root = Path(output_root)
    root.mkdir(parents=True, exist_ok=True)
    table_hashes: dict[str, str] = {}
    table_hash_modes: dict[str, str] = {}
    table_partitions: dict[str, list[dict[str, object]]] = {}
    row_counts: dict[str, int] = {}
    for name, frames in sorted(partitioned_tables.items()):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        partition_hashes: list[str] = []
        descriptors: list[dict[str, object]] = []
        row_count = 0
        for index, frame in enumerate(frames):
            if partition_order is not None and name in partition_order:
                index = partition_order[name][(str(frame["candidate_id"].iloc[0]), str(frame["fold_id"].iloc[0]))]
            frame.to_parquet(directory / f"part-{index:05d}.parquet", index=False)
            row_count += len(frame)
            partition_hashes.append(typed_canonical_hash_streamed(_records(frame)))
            descriptor = {
                "path": f"part-{index:05d}.parquet", "row_count": len(frame),
                "records_hash": partition_hashes[-1],
            }
            for field in ("fold_id", "fold_role"):
                if field in frame:
                    values = frame[field].unique()
                    if len(values) != 1:
                        raise ModelMainlineError(f"模型分区必须只包含一个 {field}")
                    descriptor[field] = str(values[0])
            descriptors.append(descriptor)
        descriptors.sort(key=lambda item: item["path"])
        table_partitions[name] = descriptors
        partition_hashes = [item["records_hash"] for item in descriptors]
        if not partition_hashes:
            raise ModelMainlineError(f"模型 Runtime 分区表为空: {name}")
        row_counts[name] = row_count
        table_hashes[name] = typed_canonical_hash(partition_hashes)
        table_hash_modes[name] = "partition-records-v1"
    for name, frame in sorted(tables().items()):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        frame.to_parquet(directory / "part-00000.parquet", index=False)
        row_counts[name] = len(frame)
        table_hashes[name] = typed_canonical_hash_streamed(_records(frame))
    payload = {
        "contract_version": WALK_FORWARD_MODEL_VERSION,
        "status": status,
        "row_counts": row_counts,
        "table_hashes": table_hashes,
        "table_hash_modes": table_hash_modes,
        "table_partitions": table_partitions,
        **dict(extra),
    }
    payload["artifact_hash"] = typed_canonical_hash(payload)
    (root / "artifact-metadata.json").write_text(
        canonical_json(payload),
        encoding="utf-8",
    )
    return payload


def _read_artifact_metadata(
    root: str | Path, *, require_model_contract: bool = True,
) -> dict[str, object]:
    try:
        metadata = json.loads(
            (Path(root) / "artifact-metadata.json").read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise ModelMainlineError("模型 Runtime 输入元数据不可读") from exc
    if require_model_contract and metadata.get("contract_version") != WALK_FORWARD_MODEL_VERSION:
        raise ModelMainlineError("模型 Runtime 输入合同版本不受支持，请重新运行上游节点")
    for name, mode in metadata.get("table_hash_modes", {}).items():
        if mode != "partition-records-v1":
            continue
        descriptors = metadata.get("table_partitions", {}).get(name)
        if not isinstance(descriptors, list) or not descriptors or any(
            not isinstance(item, dict) or not {"path", "records_hash", "row_count"} <= set(item)
            for item in descriptors
        ):
            raise ModelMainlineError(f"模型 Runtime 输入缺少分区描述: {name}")
    return metadata


def _table_paths(root: str | Path, name: str) -> tuple[Path, ...]:
    paths = tuple(sorted((Path(root) / name).glob("*.parquet")))
    if not paths:
        raise ModelMainlineError(f"模型 Runtime 输入缺少表: {name}")
    return paths


def _load_table(
    root: str | Path,
    name: str,
    *,
    frame_budget: PandasFrameBudget,
) -> tuple[pd.DataFrame, Mapping[str, object]]:
    paths = _table_paths(root, name)
    metadata = _read_artifact_metadata(
        root, require_model_contract=name not in {"features", "labels"},
    )
    try:
        frame_budget.require_parquet_materialization(
            paths,
            label=f"模型表 {name}",
        )
        if metadata.get("table_hash_modes", {}).get(name) == "partition-records-v1":
            frames = [
                frame_budget.collect_arrow_batches(
                    pq.ParquetFile(path).iter_batches(batch_size=65_536),
                    label=f"模型表 {name}/{path.name}",
                )
                for path in paths
            ]
            partition_hashes = [typed_canonical_hash_streamed(_records(frame)) for frame in frames]
            frame = frame_budget.concat_reserved_frames(frames, label=f"模型表 {name}")
            actual_hash = typed_canonical_hash(partition_hashes)
        else:
            frame = frame_budget.collect_arrow_batches(
                (
                    batch
                    for path in paths
                    for batch in pq.ParquetFile(path).iter_batches(batch_size=65_536)
                ),
                label=f"模型表 {name}",
            )
            actual_hash = typed_canonical_hash_streamed(_records(frame))
    except (OSError, PandasFrameBudgetError) as exc:
        raise ModelMainlineError(str(exc)) from exc
    if actual_hash != metadata.get("table_hashes", {}).get(name):
        raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}")
    return frame, metadata


def _load_label_slice(
    root: str | Path,
    *,
    columns: tuple[str, ...],
    predicate: ds.Expression,
    frame_budget: PandasFrameBudget,
    label: str,
) -> tuple[pd.DataFrame, Mapping[str, object]]:
    """只物化显式列和满足条件的 Label；完整表校验留给 opened 后的 loader。"""

    paths = _table_paths(root, "labels")
    metadata = _read_artifact_metadata(root, require_model_contract=False)
    try:
        dataset = ds.dataset([str(path) for path in paths], format="parquet")
        frame = frame_budget.collect_arrow_batches(
            dataset.to_batches(
                columns=list(columns),
                filter=predicate,
                batch_size=65_536,
            ),
            label=label,
        )
    except (OSError, ValueError, pa.ArrowException, PandasFrameBudgetError) as exc:
        raise ModelMainlineError(str(exc)) from exc
    return frame, metadata


def _validate_label_row_group_boundaries(root: str | Path) -> None:
    """读取 footer，确保目标扫描可在物理 row group 边界排除 holdout。"""

    paths = _table_paths(root, "labels")
    required = ("label_end_time", "horizon_sessions")
    found_row_group = False
    try:
        for path in paths:
            parquet = pq.ParquetFile(path)
            column_names = tuple(parquet.schema.names)
            missing = set(required).difference(column_names)
            if missing:
                raise ModelMainlineError(
                    f"Label row group 边界字段缺失: {sorted(missing)}"
                )
            indexes = {name: column_names.index(name) for name in required}
            for row_group_index in range(parquet.metadata.num_row_groups):
                found_row_group = True
                row_group = parquet.metadata.row_group(row_group_index)
                for name, column_index in indexes.items():
                    statistics = row_group.column(column_index).statistics
                    if (
                        statistics is None
                        or not statistics.has_min_max
                        or statistics.null_count != 0
                        or statistics.min != statistics.max
                    ):
                        raise ModelMainlineError(
                            "Label row group 必须只包含单一的 "
                            f"{name}: {path.name}#{row_group_index}"
                        )
    except ModelMainlineError:
        raise
    except (OSError, ValueError, pa.ArrowException) as exc:
        raise ModelMainlineError("Label row group footer 不可读") from exc
    if not found_row_group:
        raise ModelMainlineError("Label row group 为空")


def _iter_table_frames(
    root: str | Path,
    name: str,
    *,
    frame_budget: PandasFrameBudget,
    fold_role: str | None = None,
) -> Iterable[pd.DataFrame]:
    paths = _table_paths(root, name)
    metadata = _read_artifact_metadata(root)
    mode = metadata.get("table_hash_modes", {}).get(name)
    if mode != "partition-records-v1":
        frame, _ = _load_table(root, name, frame_budget=frame_budget)
        if fold_role is None or set(frame["fold_role"].astype(str)) == {fold_role}:
            yield frame
        frame_budget.release_frame(frame)
        return
    descriptors = metadata["table_partitions"][name]
    if [item["path"] for item in descriptors] != [path.name for path in paths]:
        raise ModelMainlineError(f"模型 Runtime 输入分区发生漂移: {name}")
    if typed_canonical_hash([item["records_hash"] for item in descriptors]) != metadata["table_hashes"][name]:
        raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}")
    if sum(item["row_count"] for item in descriptors) != metadata["row_counts"][name]:
        raise ModelMainlineError(f"模型 Runtime 输入表行数发生漂移: {name}")
    try:
        for path, descriptor in zip(paths, descriptors):
            if fold_role is not None and descriptor.get("fold_role") != fold_role:
                continue
            frame = frame_budget.collect_arrow_batches(
                pq.ParquetFile(path).iter_batches(batch_size=65_536),
                label=f"模型分区 {name}/{path.name}",
            )
            try:
                if typed_canonical_hash_streamed(_records(frame)) != descriptor["records_hash"]:
                    raise ModelMainlineError(f"模型 Runtime 输入表发生漂移: {name}/{path.name}")
                if len(frame) != descriptor["row_count"]:
                    raise ModelMainlineError(f"模型 Runtime 输入表行数发生漂移: {name}/{path.name}")
                for field in ("fold_id", "fold_role"):
                    if field in descriptor and set(frame[field].astype(str)) != {descriptor[field]}:
                        raise ModelMainlineError(f"模型 Runtime 分区 {field} 发生漂移")
                yield frame
            finally:
                frame_budget.release_frame(frame)
    except (OSError, PandasFrameBudgetError) as exc:
        raise ModelMainlineError(str(exc)) from exc


def _preflight_table(root: str | Path, name: str) -> dict[str, object]:
    """只读取 Parquet footer/schema，不返回任何 holdout 列值。"""

    import pyarrow.parquet as pq

    root = Path(root)
    paths = sorted((root / name).glob("*.parquet"))
    if not paths:
        raise ModelMainlineError(f"模型 Runtime 输入缺少表: {name}")
    try:
        metadata = json.loads(
            (root / "artifact-metadata.json").read_text(encoding="utf-8")
        )
        schemas = [pq.ParquetFile(path).schema_arrow for path in paths]
    except (OSError, json.JSONDecodeError, ValueError) as exc:
        raise ModelMainlineError("模型 Runtime holdout 预检失败") from exc
    if any(schema != schemas[0] for schema in schemas[1:]):
        raise ModelMainlineError(f"模型 Runtime holdout 分区 schema 漂移: {name}")
    declared_rows = metadata.get("row_counts", {}).get(name)
    footer_rows = sum(pq.ParquetFile(path).metadata.num_rows for path in paths)
    if declared_rows is not None and declared_rows != footer_rows:
        raise ModelMainlineError(f"模型 Runtime holdout footer 行数漂移: {name}")
    return {
        "format": "parquet",
        "columns": schemas[0].names,
        "row_count": footer_rows,
        "partition_count": len(paths),
    }


def _prediction_frame(
    frame: pd.DataFrame,
    predictions: np.ndarray,
    candidate_id: str,
    fold_id: str,
    stage: str,
    model_hash: str,
) -> pd.DataFrame:
    if len(frame) != len(predictions):
        raise ModelMainlineError("模型预测行数与样本不一致")
    return pd.DataFrame({
        "candidate_id": candidate_id,
        "fold_id": fold_id,
        "stage": stage,
        "sample_id": frame["sample_id"].astype(str).to_numpy(),
        "actual": frame["target"].astype(float).to_numpy(),
        "prediction": np.asarray(predictions, dtype=float),
        "model_hash": model_hash,
        "label_start_time": frame["label_start_time"].map(
            lambda value: pd.Timestamp(value).isoformat()
        ).to_numpy(),
        "label_end_time": frame["label_end_time"].map(
            lambda value: pd.Timestamp(value).isoformat()
        ).to_numpy(),
        "label_available_time": frame["label_available_time"].map(
            lambda value: pd.Timestamp(value).isoformat()
        ).to_numpy(),
    })


def _records(frame: pd.DataFrame) -> Iterable[dict[str, object]]:
    """逐行保持原 records 编码，不复制整表对象树。"""
    columns = list(frame.columns)
    for values in frame.itertuples(index=False, name=None):
        row = {}
        for key, value in zip(columns, values):
            if isinstance(value, np.ndarray):
                value = value.tolist()
            elif value is None or (not isinstance(value, (list, tuple, dict)) and pd.isna(value)):
                value = None
            elif isinstance(value, pd.Timestamp) or hasattr(value, "isoformat"):
                value = value.isoformat()
            elif isinstance(value, np.generic):
                value = value.item()
            row[key] = value
        yield row


def _candidates(parameters: Mapping[str, object]) -> list[dict[str, object]]:
    try:
        raw = [json.loads(str(value)) for value in parameters["candidate_jsons"]]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise ModelMainlineError("candidate_jsons 必须是规范 JSON 对象列表") from exc
    return normalize_model_candidates(raw)


def _text(parameters: Mapping[str, object], field: str) -> str:
    value = parameters.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ModelMainlineError(f"{field} 必须是非空字符串")
    return value.strip()


def _hash(parameters: Mapping[str, object], field: str) -> str:
    value = _text(parameters, field)
    if len(value) != 64 or any(char not in "0123456789abcdef" for char in value):
        raise ModelMainlineError(f"{field} 必须是 sha256")
    return value


def _boolean(parameters: Mapping[str, object], field: str) -> bool:
    value = parameters.get(field)
    if type(value) is not bool:
        raise ModelMainlineError(f"{field} 必须是布尔值")
    return value


def _optional_positive_int(parameters: Mapping[str, object], field: str) -> int | None:
    value = int(parameters[field])
    if value < 0:
        raise ModelMainlineError(f"{field} 不能为负")
    return None if value == 0 else value


__all__ = [
    "execute_model_fit_artifact", "execute_model_fold_metrics_artifact",
    "execute_model_locked_holdout_artifact", "execute_model_predict_artifact",
    "execute_model_preprocess_artifact", "execute_model_selection_artifact",
    "execute_model_split_artifact",
]
