"""ETF 日频规则、费用、成交和结算桶独立复核。"""

from __future__ import annotations

from typing import Mapping

from research_pipeline.domain import CorporateAction

from research_pipeline.platform import typed_canonical_hash
from research_pipeline.platform.market_rule_defaults import (
    CnEtfDailyMarketRuleProfile,
    require_cn_etf_daily_market_rule_profile_payload,
)
from research_pipeline.results import CANONICAL_SIMULATION_SCHEMA_IDS

from ..errors import EvidenceContractError
from ..oracle_workspace import OracleTable
from .common import (
    date_value as _date_value,
    integer as _integer,
)


def verify_daily_etf_financial_context(
    *,
    context: Mapping[str, object],
    canonical: Mapping[str, list[dict[str, object]]],
    simulation_manifest: Mapping[str, object],
    oracle_input: Mapping[str, object],
) -> None:
    """从 Result 内事实独立复核 ETF 日频受控规则与佣金假设。"""

    expected = {
        "contract_version",
        "market_rule_profile_id",
        "market_rule_profile",
        "market_rule_profile_hash",
        "rule_bundle",
        "rule_bundle_hash",
        "instrument_classification",
        "cost_assumptions",
        "cost_model_hash",
        "source_simulation_hash",
        "simulation_result_hash",
        "source_ledger_hash",
        "context_hash",
        "corporate_actions",
    }
    if set(context) != expected or context.get("contract_version") != (
        "research-daily-etf-financial-context-v2"
    ):
        raise EvidenceContractError("ETF 日频金融上下文必须使用 v2，并携带完整公司行动事实")
    raw_actions = context.get("corporate_actions")
    if not isinstance(raw_actions, list) or any(not isinstance(item, Mapping) for item in raw_actions):
        raise EvidenceContractError("ETF 日频公司行动必须是列表")
    try:
        actions = tuple(CorporateAction.from_dict(item) for item in raw_actions)
    except (ValueError, TypeError, KeyError) as exc:
        raise EvidenceContractError(f"ETF 日频公司行动事实无效: {exc}") from exc
    if len({item.action_id for item in actions}) != len(actions):
        raise EvidenceContractError("ETF 日频公司行动身份重复或修订未冻结")
    unsigned = {key: value for key, value in context.items() if key != "context_hash"}
    if typed_canonical_hash(unsigned) != context.get("context_hash"):
        raise EvidenceContractError("ETF 日频金融上下文身份不一致")
    profile_id = context.get("market_rule_profile_id")
    if not isinstance(profile_id, str):
        raise EvidenceContractError("ETF 日频金融上下文缺少 profile ID")
    try:
        profile = require_cn_etf_daily_market_rule_profile_payload(
            profile_id=profile_id,
            payload=context.get("market_rule_profile"),
            profile_hash=context.get("market_rule_profile_hash"),
        )
    except ValueError as exc:
        raise EvidenceContractError(f"ETF 日频受控规则 profile 无效: {exc}") from exc
    if (
        context.get("source_simulation_hash")
        != simulation_manifest.get("source_simulation_hash")
        or context.get("simulation_result_hash")
        != simulation_manifest.get("result_hash")
        or context.get("source_ledger_hash")
        != oracle_input.get("source_ledger_hash")
    ):
        raise EvidenceContractError("ETF 日频金融上下文未绑定正式仿真或账本")

    raw_classification = context.get("instrument_classification")
    if (
        not isinstance(raw_classification, Mapping)
        or set(raw_classification) != {"bond_etf_codes", "equity_etf_codes"}
    ):
        raise EvidenceContractError("ETF 日频品类分类 schema 无效")
    bonds = _sorted_string_list(
        raw_classification["bond_etf_codes"], "bond_etf_codes"
    )
    equities = _sorted_string_list(
        raw_classification["equity_etf_codes"], "equity_etf_codes"
    )
    if set(bonds) & set(equities) or not set(bonds) | set(equities):
        raise EvidenceContractError("ETF 日频债券与股票分类不互斥或为空")
    category_by_code = {
        **{code: "bond" for code in bonds},
        **{code: "equity" for code in equities},
    }

    raw_costs = context.get("cost_assumptions")
    if not isinstance(raw_costs, Mapping) or set(raw_costs) != {
        "assumption_id",
        "commission_ppm",
        "min_commission_units",
        "currency",
        "source_mode",
    }:
        raise EvidenceContractError("ETF 日频佣金假设 schema 无效")
    if (
        raw_costs.get("assumption_id") != "cn.etf.user-commission-assumption.v1"
        or raw_costs.get("currency") != "CNY"
        or raw_costs.get("source_mode") != "user_research_assumption"
    ):
        raise EvidenceContractError("ETF 日频佣金假设冒充市场规则来源")
    commission_ppm = _integer(
        raw_costs.get("commission_ppm"), "commission_ppm", minimum=0
    )
    min_commission_units = _integer(
        raw_costs.get("min_commission_units"),
        "min_commission_units",
        minimum=0,
    )
    expected_cost_hash = typed_canonical_hash({
        "commission_ppm": commission_ppm,
        "min_commission_units": min_commission_units,
        "sell_tax_ppm": profile.sell_tax_ppm,
        "transfer_fee_ppm": profile.transfer_fee_ppm,
    })
    if context.get("cost_model_hash") != expected_cost_hash:
        raise EvidenceContractError("ETF 日频佣金假设 hash 不一致")

    raw_rules = context.get("rule_bundle")
    if not isinstance(raw_rules, Mapping) or set(raw_rules) != set(category_by_code):
        raise EvidenceContractError("ETF 日频规则未精确覆盖声明标的")
    normalized_rules: dict[str, Mapping[str, object]] = {}
    for code, raw_rule in raw_rules.items():
        if not isinstance(raw_rule, Mapping):
            raise EvidenceContractError("ETF 日频规则条目必须是映射")
        _verify_daily_etf_rule_entry(
            code=str(code),
            rule=raw_rule,
            category=category_by_code[str(code)],
            profile=profile,
            commission_ppm=commission_ppm,
            min_commission_units=min_commission_units,
        )
        normalized_rules[str(code)] = raw_rule
    expected_rule_hash = typed_canonical_hash({
        code: dict(rule) for code, rule in sorted(normalized_rules.items())
    })
    if context.get("rule_bundle_hash") != expected_rule_hash:
        raise EvidenceContractError("ETF 日频规则 bundle hash 不一致")
    policy = oracle_input.get("policy")
    if (
        not isinstance(policy, Mapping)
        or policy.get("asset_class") != "cn_etf"
        or policy.get("bar_frequency") != "daily"
        or policy.get("rule_snapshot_hash") != expected_rule_hash
    ):
        raise EvidenceContractError("ETF 日频 TCA 未绑定同一规则 bundle")

    external_tables = {
        name: rows for name, rows in canonical.items()
        if isinstance(rows, OracleTable)
    }
    if set(external_tables) == set(CANONICAL_SIMULATION_SCHEMA_IDS):
        workspace = next(iter(external_tables.values())).workspace
        session_bounds = workspace.execute(" UNION ALL ".join(
            f"SELECT min(session), max(session) FROM {external_tables[name].name}"
            for name in ("orders", "fills", "positions")
        )).fetchall()
        observed_sessions = tuple(
            value
            for row in session_bounds
            for value in row
            if value is not None
        )
    else:
        observed_sessions = tuple(
            _date_value(row["session"], "session")
            for table in (
                canonical["orders"],
                canonical["fills"],
                canonical["positions"],
            )
            for row in table
        )
    if observed_sessions:
        try:
            profile.require_covers(min(observed_sessions), max(observed_sessions))
        except ValueError as exc:
            raise EvidenceContractError(str(exc)) from exc
    if set(external_tables) == set(CANONICAL_SIMULATION_SCHEMA_IDS):
        _verify_external_daily_etf_fills_and_settlement(
            canonical=external_tables,
            category_by_code=category_by_code,
            profile=profile,
            commission_ppm=commission_ppm,
            min_commission_units=min_commission_units,
            corporate_actions=actions,
        )
        return
    _verify_daily_etf_fills_and_settlement(
        canonical=canonical,
        category_by_code=category_by_code,
        profile=profile,
        commission_ppm=commission_ppm,
        min_commission_units=min_commission_units,
        corporate_actions=actions,
    )


def _verify_external_daily_etf_fills_and_settlement(
    *,
    canonical: Mapping[str, OracleTable],
    category_by_code: Mapping[str, str],
    profile: CnEtfDailyMarketRuleProfile,
    commission_ppm: int,
    min_commission_units: int,
    corporate_actions: tuple[CorporateAction, ...] = (),
) -> None:
    """外部表以有界批次按会话复核，仅保留当前持仓与尚未到账权益。"""
    _verify_daily_etf_fills_and_settlement(
        canonical=canonical,
        category_by_code=category_by_code,
        profile=profile,
        commission_ppm=commission_ppm,
        min_commission_units=min_commission_units,
        corporate_actions=corporate_actions,
    )


def _verify_daily_etf_rule_entry(
    *,
    code: str,
    rule: Mapping[str, object],
    category: str,
    profile: CnEtfDailyMarketRuleProfile,
    commission_ppm: int,
    min_commission_units: int,
) -> None:
    expected_fields = {
        "rule_id",
        "version",
        "market",
        "instrument_type",
        "effective_start",
        "effective_end",
        "available_time",
        "official_source_id",
        "evidence_url",
        "parameters",
    }
    if set(rule) != expected_fields:
        raise EvidenceContractError(f"ETF 日频规则 schema 无效: {code}")
    if (
        rule.get("rule_id")
        != f"{profile.profile_id}.{category}.execution-policy"
        or rule.get("version") != profile.profile_version
        or rule.get("market") != "cn_etf"
        or rule.get("instrument_type") != "etf"
        or rule.get("effective_start") != profile.effective_start.isoformat()
        or rule.get("effective_end") != profile.effective_end.isoformat()
        or rule.get("available_time") != profile.rule_available_at.isoformat()
        or rule.get("official_source_id") != profile.source_id
        or rule.get("evidence_url") != profile.source_reference
    ):
        raise EvidenceContractError(f"ETF 日频规则来源或有效区间漂移: {code}")
    raw_parameters = rule.get("parameters")
    if (
        not isinstance(raw_parameters, list)
        or any(not isinstance(item, list) or len(item) != 2 for item in raw_parameters)
    ):
        raise EvidenceContractError(f"ETF 日频规则 parameters 无效: {code}")
    parameters = {str(item[0]): item[1] for item in raw_parameters}
    if len(parameters) != len(raw_parameters) or list(parameters) != sorted(parameters):
        raise EvidenceContractError(f"ETF 日频规则 parameters 重复或未排序: {code}")
    expected_parameters = {
        "commission_ppm": commission_ppm,
        "etf_category": category,
        "lot_size": profile.lot_size,
        "min_commission_units": min_commission_units,
        "sell_tax_ppm": profile.sell_tax_ppm,
        "settlement_days": profile.settlement_days_for(category),
        "transfer_fee_ppm": profile.transfer_fee_ppm,
    }
    if parameters != expected_parameters:
        raise EvidenceContractError(f"ETF 日频规则内容与 profile/成本假设不一致: {code}")


def _verify_daily_etf_fills_and_settlement(
    *, canonical, category_by_code, profile, commission_ppm,
    min_commission_units, corporate_actions=(),
) -> None:
    from .daily_holdings import verify_daily_etf_holdings

    verify_daily_etf_holdings(
        canonical=canonical, category_by_code=category_by_code, profile=profile,
        commission_ppm=commission_ppm, min_commission_units=min_commission_units,
        corporate_actions=corporate_actions,
    )


def _sorted_string_list(value: object, field: str) -> tuple[str, ...]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item for item in value
    ):
        raise EvidenceContractError(f"{field} 必须是字符串列表")
    values = tuple(value)
    if values != tuple(sorted(set(values))):
        raise EvidenceContractError(f"{field} 必须唯一并规范排序")
    return values


__all__ = ["verify_daily_etf_financial_context"]
