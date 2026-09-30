"""由正式成交回放生成分钟期货结算证据。"""

from __future__ import annotations

from datetime import date
from fractions import Fraction
from typing import Mapping

from research_pipeline.domain import MinuteRuleResolver, PortfolioTarget
from research_pipeline.platform import typed_canonical_hash

from .events import FinancialEvent
from .ledger import FuturesPosition


def build_minute_futures_settlement_events(
    *,
    result,
    bundle,
    targets: tuple[PortfolioTarget, ...],
    instrument_hashes: Mapping[str, str] | None = None,
) -> list[dict[str, object]]:
    """把仿真已消费的逐日结算事实完整写入 ResultStore 上下文。"""

    if result.semantics.asset_class != "cn_future":
        return []
    resolver = MinuteRuleResolver(bundle)
    settlement_rule_ids = (
        "rule.cn_futures.contract_multiplier.v1",
        "rule.cn_futures.margin.v1",
        "rule.cn_futures.price_tick.v1",
        "rule.cn_futures.session.v1",
        "rule.cn_futures.settlement.v1",
    )
    positions: dict[str, FuturesPosition] = {}
    resolved_instrument_hashes = dict(instrument_hashes or {})
    for item in targets:
        instrument = item.entries[0].instrument
        existing = resolved_instrument_hashes.setdefault(
            instrument.instrument_id, instrument.instrument_hash
        )
        if existing != instrument.instrument_hash:
            raise ValueError("分钟期货结算标的身份漂移")
    fills_by_session: dict[date, list[object]] = {}
    for fill in result.tables["fills"].itertuples(index=False):
        fills_by_session.setdefault(fill.session, []).append(fill)
    events = []
    for cash in result.tables["cash"].itertuples(index=False):
        session = cash.session
        session_fills = sorted(fills_by_session.get(session, []), key=lambda item: (item.fill_time, item.instrument_id, item.position_effect, item.fill_id))
        for fill in session_fills:
            instrument_id = str(fill.instrument_id)
            price = int(fill.execution_price_units)
            old = positions.get(instrument_id, FuturesPosition(resolved_instrument_hashes[instrument_id], 0, price))
            direction = 1 if str(fill.side) == "buy" else -1
            remainder = Fraction(old.pnl_remainder_numerator, old.pnl_remainder_denominator)
            expected_realized = 0
            if str(fill.position_effect) == "close":
                expected_realized, remainder = old.realize(
                    price_units=price, contracts=-direction * int(fill.quantity),
                    multiplier=int(fill.contract_multiplier),
                )
            if int(fill.realized_pnl_units) != expected_realized:
                raise ValueError("分钟期货 fill 已实现盈亏与精确成本不一致")
            positions[instrument_id] = old.with_fill(
                contracts_delta=direction * int(fill.quantity), price_units=price,
                pnl_remainder=remainder, margin_units=0,
            )

        instrument_ids = {
            instrument_id
            for instrument_id, position in positions.items()
            if position.contracts != 0
        }
        if not instrument_ids:
            if (
                int(cash.non_trade_cash_change_units) != 0
                or int(cash.margin_units) != 0
                or str(cash.non_trade_source_hash) != typed_canonical_hash([])
            ):
                raise ValueError("分钟期货空仓会话包含伪结算或保证金")
            continue
        facts = []
        for instrument_id in sorted(instrument_ids):
            settlement_candidates = [
                item for item in bundle.rules
                if item.instrument_id == instrument_id
                and item.rule_id == "rule.cn_futures.settlement.v1"
                and item.effective_from <= session <= item.effective_to
            ]
            if (
                len(settlement_candidates) != 1
                or settlement_candidates[0].available_at is None
            ):
                raise ValueError("分钟期货结算上下文缺少唯一收盘事件")
            settlement_time = settlement_candidates[0].available_at
            bindings = tuple(
                resolver.resolve(
                    rule_id=rule_id,
                    instrument_id=instrument_id,
                    effective_on=session,
                    as_of=settlement_time,
                )
                for rule_id in settlement_rule_ids
            )
            parameters = {
                key: value
                for binding in bindings
                for key, value in binding.rule.parameters
            }
            settlement_price = int(parameters["settlement_price_units"])
            multiplier = int(parameters["contract_unit_kg"])
            margin_ppm = int(parameters["speculative_initial_margin_ppm"])
            position = positions[instrument_id]
            quantity, basis = position.contracts, position.settlement_price_units
            pnl_units, _ = position.realize(price_units=settlement_price, contracts=quantity, multiplier=multiplier)
            required_margin = (
                settlement_price * multiplier * abs(quantity) * margin_ppm
                + 999_999
            ) // 1_000_000
            rule_hash = typed_canonical_hash({
                "bundle_hash": bundle.bundle_hash,
                "bindings": [item.identity_hash for item in bindings],
            })
            facts.append({
                "instrument_id": instrument_id,
                "instrument_hash": resolved_instrument_hashes.get(instrument_id),
                "settlement_time": settlement_time,
                "settlement_price_units": settlement_price,
                "price_scale": int(parameters["price_scale"]),
                "position_contracts_before": quantity,
                "previous_settlement_price_units": basis,
                "contract_multiplier": multiplier,
                "speculative_margin_ppm": margin_ppm,
                "pnl_units": pnl_units,
                "required_margin_units": required_margin,
                "rule_hash": rule_hash,
                "rule_snapshot_hashes": [
                    item.rule.snapshot_hash for item in bindings
                ],
            })
        settlement_times = {item["settlement_time"] for item in facts}
        if len(settlement_times) != 1:
            raise ValueError("分钟期货同一账户结算时点不一致")
        aggregate_margin = sum(int(item["required_margin_units"]) for item in facts)
        session_events = []
        for fact in facts:
            event = FinancialEvent(
                f"minute-settlement:{fact['instrument_id']}:{session.isoformat()}",
                "mark_to_market",
                fact["settlement_time"],
                session.isoformat(),
                "minute-default-cn-futures",
                str(fact["rule_hash"]),
                (
                    ("pnl_units", int(fact["pnl_units"])),
                    ("required_margin_units", aggregate_margin),
                ),
            )
            positions[str(fact["instrument_id"])] = positions[str(fact["instrument_id"])].after_settlement(
                price_units=int(fact["settlement_price_units"]),
                multiplier=int(fact["contract_multiplier"]),
                margin_units=int(fact["required_margin_units"]),
            )
            session_events.append({
                **fact,
                "settlement_time": fact["settlement_time"].isoformat(),
                "aggregate_required_margin_units": aggregate_margin,
                "event": event.to_dict(),
                "event_hash": event.event_hash,
            })
        event_hashes = [str(item["event_hash"]) for item in session_events]
        if (
            typed_canonical_hash(sorted(event_hashes))
            != str(cash.non_trade_source_hash)
            or sum(int(item["pnl_units"]) for item in session_events)
            != int(cash.non_trade_cash_change_units)
            or aggregate_margin != int(cash.margin_units)
        ):
            raise ValueError("分钟期货结算上下文与正式现金账本不一致")
        events.extend(session_events)
    return events

