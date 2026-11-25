"""Pure zone-role transition reconstruction and entry attenuation policy."""

from __future__ import annotations

from bisect import bisect_left
from dataclasses import dataclass
from typing import Callable, Dict, List, Mapping, Optional, Sequence, Tuple

import math
import numpy as np
import pandas as pd


TF_RANK = {"1h": 1, "4h": 2, "1d": 3}
TF_DELTA = {
    "1h": pd.Timedelta(hours=1),
    "4h": pd.Timedelta(hours=4),
    "1d": pd.Timedelta(days=1),
}
POLICY_STATE_VERSION = 2


class PolicyHistoryError(ValueError):
    """Raised when closed candles cannot reconstruct the policy safely."""


@dataclass(frozen=True)
class BreakoutRule:
    penetration_atr: float
    body_atr: float
    close_location: float


@dataclass(frozen=True)
class BullishReversalRule:
    min_lower_wick_range_ratio: float = 0.50
    max_reclaim_distance: float = 300.0


@dataclass(frozen=True)
class PolicySetting:
    ttl_1h_bars: int
    ttl_4h_bars: int
    ttl_1d_bars: int
    multiplier_1h: float
    multiplier_4h: float
    multiplier_1d: float
    countertrend_limit: int = 1
    strong_strength_atr: float = 0.30
    moderate_strength_multiplier: float = 0.50
    strong_strength_multiplier: float = 0.25

    def ttl_bars(self, timeframe: str) -> int:
        return {
            "1h": self.ttl_1h_bars,
            "4h": self.ttl_4h_bars,
            "1d": self.ttl_1d_bars,
        }[timeframe]

    def multiplier(self, timeframe: str) -> float:
        return {
            "1h": self.multiplier_1h,
            "4h": self.multiplier_4h,
            "1d": self.multiplier_1d,
        }[timeframe]

    def strength_multiplier(self, strength: float) -> float:
        return (
            self.strong_strength_multiplier
            if strength >= self.strong_strength_atr
            else self.moderate_strength_multiplier
        )


@dataclass(frozen=True)
class Zone:
    zone_id: str
    source_tf: str
    original_side: str
    low: float
    high: float
    active_from: pd.Timestamp
    anchor_time: pd.Timestamp
    invalidated_at: Optional[pd.Timestamp] = None
    anchor_open: float = math.nan
    anchor_high: float = math.nan
    anchor_low: float = math.nan
    anchor_close: float = math.nan
    impulse_time: Optional[pd.Timestamp] = None
    kind: str = "ob"
    boundary_times: Tuple[pd.Timestamp, ...] = ()
    boundary_values: Tuple[float, ...] = ()
    ends_at: Optional[pd.Timestamp] = None


@dataclass(frozen=True)
class EntryZone:
    zone_id: str
    source_tf: str
    side: str
    low: float
    high: float
    bullish_reversal: bool = False
    lower_wick_range_ratio: float = 0.0


@dataclass(frozen=True)
class TransitionEvent:
    zone_id: str
    event_time: pd.Timestamp
    timeframe: str
    effective_side: str
    direction: str
    strength: float
    event_kind: str
    role_epoch: int


@dataclass(frozen=True)
class AttenuationDecision:
    multiplier: float
    blocked: bool
    block_reason: str
    zone_id: str = ""
    timeframe: str = ""
    role_epoch: int = 0
    quota_key: str = ""
    strength: float = 0.0
    entry_zone_id: str = ""
    reversal_started: bool = False


def utc_timestamp(value) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if pd.isna(timestamp):
        raise ValueError("timestamp must not be NaT")
    return timestamp.tz_localize("UTC") if timestamp.tzinfo is None else timestamp.tz_convert("UTC")


def fresh_policy_state(started_at) -> Dict[str, object]:
    return {
        "schema_version": POLICY_STATE_VERSION,
        "started_at": utc_timestamp(started_at).isoformat(),
        "quota": {},
        "trend_reset": None,
    }


def _normalize_trend_reset(payload) -> Optional[Dict[str, object]]:
    if payload is None:
        return None
    if not isinstance(payload, dict) or payload.get("direction") != "up":
        raise ValueError("trend reset is malformed")
    event_time = utc_timestamp(payload.get("event_time")).isoformat()
    pivot_zone_id = str(payload.get("pivot_zone_id") or "")
    source_zone_id = str(payload.get("source_zone_id") or "")
    low = float(payload.get("pivot_low"))
    high = float(payload.get("pivot_high"))
    if not pivot_zone_id or not source_zone_id:
        raise ValueError("trend reset zone identity is missing")
    if not all(math.isfinite(value) for value in (low, high)) or low > high:
        raise ValueError("trend reset bounds are invalid")
    return {
        "direction": "up",
        "event_time": event_time,
        "pivot_zone_id": pivot_zone_id,
        "pivot_low": low,
        "pivot_high": high,
        "source_zone_id": source_zone_id,
        "source_role_epoch": int(payload.get("source_role_epoch") or 0),
    }


def normalize_policy_state(payload, started_at) -> Tuple[Dict[str, object], bool]:
    """Return a valid policy state and whether a malformed/missing state reset."""
    if not isinstance(payload, dict):
        return fresh_policy_state(started_at), True
    schema_version = payload.get("schema_version")
    if schema_version not in {1, POLICY_STATE_VERSION}:
        return fresh_policy_state(started_at), True
    try:
        normalized_started_at = utc_timestamp(payload.get("started_at")).isoformat()
    except (TypeError, ValueError, OverflowError):
        return fresh_policy_state(started_at), True
    quota = payload.get("quota")
    if not isinstance(quota, dict):
        return fresh_policy_state(started_at), True
    normalized_quota: Dict[str, Dict[str, str]] = {}
    for key, record in quota.items():
        if not isinstance(key, str) or not key or not isinstance(record, dict):
            return fresh_policy_state(started_at), True
        status = record.get("status")
        if status not in {"reserved", "consumed"}:
            return fresh_policy_state(started_at), True
        normalized_quota[key] = {
            "status": str(status),
            "signal_id": str(record.get("signal_id") or ""),
            "updated_at": str(record.get("updated_at") or ""),
        }
    try:
        trend_reset = (
            None
            if schema_version == 1
            else _normalize_trend_reset(payload.get("trend_reset"))
        )
    except (TypeError, ValueError, OverflowError):
        return fresh_policy_state(started_at), True
    return {
        "schema_version": POLICY_STATE_VERSION,
        "started_at": normalized_started_at,
        "quota": normalized_quota,
        "trend_reset": trend_reset,
    }, False


def record_bullish_trend_reset(
    state: Dict[str, object],
    decision: AttenuationDecision,
    entry_zone: EntryZone,
    when,
) -> None:
    if not decision.reversal_started or decision.entry_zone_id != entry_zone.zone_id:
        raise ValueError("decision does not start the supplied bullish reversal")
    state["trend_reset"] = {
        "direction": "up",
        "event_time": utc_timestamp(when).isoformat(),
        "pivot_zone_id": entry_zone.zone_id,
        "pivot_low": float(entry_zone.low),
        "pivot_high": float(entry_zone.high),
        "source_zone_id": decision.zone_id,
        "source_role_epoch": int(decision.role_epoch),
    }


def reserve_quota(state: Dict[str, object], quota_key: str, signal_id: str, when) -> None:
    if not quota_key:
        return
    quota = state.get("quota")
    if not isinstance(quota, dict):
        raise ValueError("policy quota state is malformed")
    quota[quota_key] = {
        "status": "reserved",
        "signal_id": str(signal_id),
        "updated_at": utc_timestamp(when).isoformat(),
    }


def consume_quota(state: Dict[str, object], quota_key: str, signal_id: str, when) -> None:
    if not quota_key:
        return
    quota = state.get("quota")
    if not isinstance(quota, dict):
        raise ValueError("policy quota state is malformed")
    quota[quota_key] = {
        "status": "consumed",
        "signal_id": str(signal_id),
        "updated_at": utc_timestamp(when).isoformat(),
    }


def _closed_frame(frame: pd.DataFrame, timeframe: str, visible_at) -> pd.DataFrame:
    required = {"timestamp", "open", "high", "low", "close"}
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise PolicyHistoryError(f"{timeframe} frame missing columns: {missing}")
    out = frame.copy()
    out["timestamp"] = pd.to_datetime(out["timestamp"], utc=True)
    if "close_time" in out.columns:
        out["close_time"] = pd.to_datetime(out["close_time"], utc=True)
    else:
        out["close_time"] = out["timestamp"] + TF_DELTA[timeframe]
    out = out[out["close_time"] <= utc_timestamp(visible_at)].copy()
    out = out.sort_values("close_time", kind="stable").drop_duplicates("close_time", keep="last")
    return out.reset_index(drop=True)


def _validate_setting(rule: BreakoutRule, setting: PolicySetting) -> None:
    values = (
        rule.penetration_atr, rule.body_atr, rule.close_location,
        setting.multiplier_1h, setting.multiplier_4h, setting.multiplier_1d,
        setting.strong_strength_atr,
        setting.moderate_strength_multiplier,
        setting.strong_strength_multiplier,
    )
    if not all(math.isfinite(value) for value in values):
        raise ValueError("zone-role policy values must be finite")
    if rule.penetration_atr < 0 or rule.body_atr < 0 or not 0 <= rule.close_location <= 1:
        raise ValueError("invalid breakout rule")
    if any(setting.ttl_bars(tf) <= 0 for tf in TF_RANK):
        raise ValueError("TTL bars must be positive")
    if any(not 0 <= setting.multiplier(tf) <= 1 for tf in TF_RANK):
        raise ValueError("multipliers must be between zero and one")
    if setting.strong_strength_atr < rule.penetration_atr:
        raise ValueError("strong strength threshold must not be below breakout penetration")
    if not 0 <= setting.strong_strength_multiplier <= setting.moderate_strength_multiplier <= 1:
        raise ValueError("strength multipliers must satisfy 0 <= strong <= moderate <= 1")
    if setting.countertrend_limit != 1:
        raise ValueError("production countertrend limit must be one")


def qualifies_bullish_reversal_candle(
    candle: Mapping[str, object],
    entry_zone: EntryZone,
    rule: BullishReversalRule,
) -> Tuple[bool, float]:
    values = []
    for key in ("open", "high", "low", "close"):
        try:
            values.append(float(candle[key]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(f"reversal candle is missing {key}") from exc
    open_price, high_price, low_price, close_price = values
    if not all(math.isfinite(value) for value in values):
        raise ValueError("reversal candle values must be finite")
    if entry_zone.side != "support":
        return False, 0.0
    if not 0 < rule.min_lower_wick_range_ratio < 1:
        raise ValueError("lower-wick range ratio must be between zero and one")
    if not math.isfinite(rule.max_reclaim_distance) or rule.max_reclaim_distance < 0:
        raise ValueError("max reclaim distance must be finite and nonnegative")
    candle_range = high_price - low_price
    if candle_range <= 0:
        return False, 0.0
    if high_price < max(open_price, close_price) or low_price > min(open_price, close_price):
        raise ValueError("reversal candle OHLC ordering is invalid")
    body_low = min(open_price, close_price)
    lower_wick = body_low - low_price
    lower_wick_ratio = lower_wick / candle_range
    qualifies = (
        low_price < entry_zone.low
        and body_low >= entry_zone.low
        and entry_zone.high <= close_price <= entry_zone.high + rule.max_reclaim_distance
        and lower_wick_ratio >= rule.min_lower_wick_range_ratio
    )
    return qualifies, float(lower_wick_ratio)


def _prepare_frames(
    frames: Mapping[str, pd.DataFrame],
    visible_at,
    add_atr: Callable[[pd.DataFrame, int], pd.DataFrame],
    atr_len: int,
    setting: PolicySetting,
) -> Dict[str, pd.DataFrame]:
    prepared: Dict[str, pd.DataFrame] = {}
    for timeframe in TF_RANK:
        if timeframe not in frames:
            raise PolicyHistoryError(f"missing {timeframe} frame")
        closed = _closed_frame(frames[timeframe], timeframe, visible_at)
        required_bars = atr_len + setting.ttl_bars(timeframe)
        if len(closed) < required_bars:
            raise PolicyHistoryError(
                f"{timeframe} requires {required_bars} closed bars for ATR+TTL; got {len(closed)}"
            )
        with_atr = add_atr(closed, atr_len).copy()
        if "atr" not in with_atr.columns:
            raise PolicyHistoryError(f"{timeframe} ATR column missing")
        prepared[timeframe] = with_atr
    return prepared


def build_zones(
    source_frames: Mapping[str, pd.DataFrame],
    extract_order_blocks: Callable[[pd.DataFrame], List[Dict]],
) -> List[Zone]:
    zones: List[Zone] = []
    for timeframe in ("1h", "4h", "1d"):
        frame = source_frames[timeframe]
        extracted = extract_order_blocks(frame)
        selected = []
        for side in ("bull", "bear"):
            selected.extend(
                [order_block for order_block in extracted if order_block["side"] == side]
            )
        closes = frame["close"].to_numpy(dtype=float)
        close_times = pd.to_datetime(frame["close_time"], utc=True).to_numpy()
        for order_block in selected:
            impulse_index = int(order_block.get("impulse_idx", -1))
            if impulse_index < 0 or impulse_index >= len(frame):
                continue
            anchor_time = utc_timestamp(order_block["anchor_time"])
            active_from = utc_timestamp(frame.iloc[impulse_index]["close_time"])
            original_side = "support" if order_block["side"] == "bull" else "resistance"
            invalidated_at: Optional[pd.Timestamp] = None
            future_start = impulse_index + 1
            future = closes[future_start:]
            broken = (
                future < float(order_block["low"])
                if order_block["side"] == "bull"
                else future > float(order_block["high"])
            )
            offsets = np.flatnonzero(broken)
            if len(offsets):
                invalidated_at = utc_timestamp(
                    close_times[future_start + int(offsets[0])]
                )
            zones.append(Zone(
                zone_id=f"{timeframe}:ob:{order_block['side']}:{anchor_time.isoformat()}",
                source_tf=timeframe,
                original_side=original_side,
                low=float(min(order_block["low"], order_block["high"])),
                high=float(max(order_block["low"], order_block["high"])),
                active_from=active_from,
                anchor_time=anchor_time,
                invalidated_at=invalidated_at,
                anchor_open=float(order_block.get("anchor_open", math.nan)),
                anchor_high=float(order_block.get("anchor_high", math.nan)),
                anchor_low=float(order_block.get("anchor_low", math.nan)),
                anchor_close=float(order_block.get("anchor_close", math.nan)),
                impulse_time=utc_timestamp(order_block.get("impulse_time", active_from)),
            ))
    return sorted(zones, key=lambda zone: zone.active_from)


def _candidate_events(zone: Zone, frame: pd.DataFrame, timeframe: str, rule: BreakoutRule) -> List[Dict]:
    subset = frame[frame["close_time"] > zone.active_from]
    if subset.empty:
        return []
    opens = subset["open"].to_numpy(dtype=float)
    highs = subset["high"].to_numpy(dtype=float)
    lows = subset["low"].to_numpy(dtype=float)
    closes = subset["close"].to_numpy(dtype=float)
    previous = frame["close"].shift().loc[subset.index].to_numpy(dtype=float)
    lower = np.full(len(subset), zone.low)
    upper = np.full(len(subset), zone.high)
    if zone.boundary_times:
        indices = np.searchsorted(
            pd.DatetimeIndex(zone.boundary_times).asi8,
            pd.DatetimeIndex(subset["timestamp"]).asi8, side="right",
        ) - 1
        values = np.asarray(zone.boundary_values)
        lower = upper = np.where(indices >= 0, values[np.maximum(indices, 0)], np.nan)
    atrs = subset["atr"].to_numpy(dtype=float)
    ranges = np.maximum(highs - lows, 1e-9)
    valid = np.isfinite(atrs) & (atrs > 0)
    safe_atrs = np.where(valid, atrs, 1.0)
    valid &= np.abs(closes - opens) / safe_atrs >= rule.body_atr
    up_strength = (closes - upper) / safe_atrs
    down_strength = (lower - closes) / safe_atrs
    up_indices = np.flatnonzero(
        valid
        & (previous <= upper)
        & (opens <= upper)
        & (up_strength >= rule.penetration_atr)
        & ((closes - lows) / ranges >= rule.close_location)
    )
    down_indices = np.flatnonzero(
        valid
        & (previous >= lower)
        & (opens >= lower)
        & (down_strength >= rule.penetration_atr)
        & ((highs - closes) / ranges >= rule.close_location)
    )
    records: List[Dict] = []
    for index in up_indices:
        records.append({
            "event_time": utc_timestamp(subset.iloc[int(index)]["close_time"]),
            "timeframe": timeframe,
            "direction": "up",
            "effective_side": "support",
            "strength": float(up_strength[index]),
        })
    for index in down_indices:
        records.append({
            "event_time": utc_timestamp(subset.iloc[int(index)]["close_time"]),
            "timeframe": timeframe,
            "direction": "down",
            "effective_side": "resistance",
            "strength": float(down_strength[index]),
        })
    return records


def build_transition_events(
    zones: Sequence[Zone],
    frames: Mapping[str, pd.DataFrame],
    rule: BreakoutRule,
) -> Dict[str, List[TransitionEvent]]:
    result: Dict[str, List[TransitionEvent]] = {}
    for zone in zones:
        candidates: List[Dict] = []
        # One observation clock: later HTF closes cannot upgrade an old break.
        for timeframe, frame in [("1h", frames["1h"])]:
            candidates.extend(_candidate_events(zone, frame, timeframe, rule))
        candidates.sort(key=lambda item: (item["event_time"], TF_RANK[item["timeframe"]]))
        role = zone.original_side
        confirmation_rank = 0
        epoch = 0
        events: List[TransitionEvent] = []
        for candidate in candidates:
            new_role = str(candidate["effective_side"])
            rank = TF_RANK[str(candidate["timeframe"])]
            if new_role != role:
                if epoch > 0:
                    events.append(TransitionEvent(
                        zone_id=zone.zone_id,
                        event_time=candidate["event_time"],
                        timeframe=str(candidate["timeframe"]),
                        effective_side=new_role,
                        direction=str(candidate["direction"]),
                        strength=float(candidate["strength"]),
                        event_kind="deleted",
                        role_epoch=epoch + 1,
                    ))
                    break
                role = new_role
                confirmation_rank = rank
                epoch += 1
                event_kind = "transition"
            else:
                continue
            events.append(TransitionEvent(
                zone_id=zone.zone_id,
                event_time=candidate["event_time"],
                timeframe=str(candidate["timeframe"]),
                effective_side=role,
                direction=str(candidate["direction"]),
                strength=float(candidate["strength"]),
                event_kind=event_kind,
                role_epoch=epoch,
            ))
        result[zone.zone_id] = events
    return result


def reconstruct_context(
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    visible_at,
    extract_order_blocks: Callable[[pd.DataFrame], List[Dict]],
    add_atr: Callable[[pd.DataFrame, int], pd.DataFrame],
    atr_len: int,
    rule: BreakoutRule,
    setting: PolicySetting,
) -> Tuple[List[Zone], Dict[str, List[TransitionEvent]]]:
    _validate_setting(rule, setting)
    frames = _prepare_frames(
        {"1h": df_1h, "4h": df_4h, "1d": df_1d}, visible_at, add_atr, atr_len, setting
    )
    zones = _select_recent_zones(build_zones(frames, extract_order_blocks), visible_at)
    from zone_cloud_policy import build_cloud_policy_zones
    cloud_frames = {
        tf: frame.loc[pd.to_datetime(frame["timestamp"], utc=True) <= utc_timestamp(visible_at)]
        for tf, frame in (("4h", df_4h), ("1d", df_1d))
    }
    zones.extend(build_cloud_policy_zones(cloud_frames, Zone))
    return zones, build_transition_events(zones, frames, rule)


def state_at(events: Sequence[TransitionEvent], when) -> Optional[TransitionEvent]:
    if not events:
        return None
    target = utc_timestamp(when)
    position = bisect_left([event.event_time for event in events], target) - 1
    return events[position] if position >= 0 else None


def event_expiry(event: TransitionEvent, setting: PolicySetting) -> pd.Timestamp:
    return event.event_time + TF_DELTA[event.timeframe] * setting.ttl_bars(event.timeframe)


def _select_recent_zones(zones: Sequence[Zone], when) -> List[Zone]:
    target = utc_timestamp(when)
    return [zone for zone in zones if zone.active_from < target
            and (zone.ends_at is None or target < zone.ends_at)]


def active_flipped_zone_states(
    zones: Sequence[Zone],
    event_map: Mapping[str, Sequence[TransitionEvent]],
    when,
) -> List[Tuple[Zone, TransitionEvent, TransitionEvent]]:
    """Return live flipped roles and the event that started each role epoch.

    TTL limits countertrend attenuation only.  A flipped zone itself remains
    tradable until its second confirmed opposite breakout emits ``deleted``.
    """
    active: List[Tuple[Zone, TransitionEvent, TransitionEvent]] = []
    target = utc_timestamp(when)
    for zone in _select_recent_zones(zones, target):
        events = event_map.get(zone.zone_id, ())
        current = state_at(events, target)
        if (
            current is None
            or current.event_kind == "deleted"
            or current.effective_side == zone.original_side
        ):
            continue
        transition = next(
            (
                event
                for event in events
                if event.role_epoch == current.role_epoch
                and event.event_kind == "transition"
            ),
            None,
        )
        if transition is None:
            raise PolicyHistoryError(
                f"flipped zone is missing its transition event: {zone.zone_id} epoch={current.role_epoch}"
            )
        active.append((zone, current, transition))
    return active


def attenuation_multiplier(event: TransitionEvent, setting: PolicySetting) -> float:
    """Use strength at the crossing, never the source or confirmation TF."""
    if not math.isfinite(event.strength) or event.strength < 0:
        raise ValueError(f"transition strength is invalid: {event.strength}")
    return setting.strength_multiplier(event.strength)


def _validated_entry_zones(entry_zones: Sequence[EntryZone]) -> List[EntryZone]:
    validated: List[EntryZone] = []
    for entry_zone in entry_zones:
        if entry_zone.source_tf not in TF_RANK:
            raise ValueError(f"invalid entry zone timeframe: {entry_zone.source_tf}")
        if entry_zone.side not in {"support", "resistance"}:
            raise ValueError(f"invalid entry zone side: {entry_zone.side}")
        if not all(math.isfinite(value) for value in (
            entry_zone.low,
            entry_zone.high,
            entry_zone.lower_wick_range_ratio,
        )):
            raise ValueError("entry zone values must be finite")
        if entry_zone.low > entry_zone.high:
            raise ValueError("entry zone low must not exceed high")
        if not 0 <= entry_zone.lower_wick_range_ratio <= 1:
            raise ValueError("entry zone lower-wick ratio must be between zero and one")
        validated.append(entry_zone)
    return validated


def _bullish_reversal_entry_zone(
    regimes: Sequence[Tuple[float, TransitionEvent, Zone]],
    entry_zones: Sequence[EntryZone],
) -> Optional[EntryZone]:
    """Return a wick-confirmed lower B support beneath every bearish A regime."""
    if not regimes:
        return None
    candidates = [
        entry_zone
        for entry_zone in _validated_entry_zones(entry_zones)
        if entry_zone.side == "support"
        and entry_zone.bullish_reversal
        and all(entry_zone.high < regime_zone.low for _m, _e, regime_zone in regimes)
    ]
    if not candidates:
        return None
    return max(
        candidates,
        key=lambda entry_zone: (
            entry_zone.lower_wick_range_ratio,
            entry_zone.high,
        ),
    )


def decide_attenuation(
    side: str,
    signal_time,
    zones: Sequence[Zone],
    event_map: Mapping[str, Sequence[TransitionEvent]],
    setting: PolicySetting,
    quota: Mapping[str, object],
    *,
    entry_zones: Sequence[EntryZone] = (),
    trend_reset: Optional[Mapping[str, object]] = None,
) -> AttenuationDecision:
    if side not in {"long", "short"}:
        raise ValueError(f"invalid signal side: {side}")
    if not isinstance(quota, Mapping):
        raise ValueError("policy quota is malformed")
    normalized_reset = _normalize_trend_reset(trend_reset)
    reset_at = (
        utc_timestamp(normalized_reset["event_time"])
        if normalized_reset is not None
        else None
    )
    countertrend_regimes: List[Tuple[float, TransitionEvent, Zone]] = []
    bearish_trend_regimes: List[Tuple[float, TransitionEvent, Zone]] = []
    target = utc_timestamp(signal_time)
    for zone, _current, event in active_flipped_zone_states(zones, event_map, target):
        if reset_at is not None and event.event_time <= reset_at:
            continue
        if reset_at is None and zone.source_tf == "1h":
            continue
        if (
            normalized_reset is not None
            and side == "short"
            and event.effective_side == "support"
            and zone.low <= float(normalized_reset["pivot_high"])
        ):
            continue
        aligned_side = "long" if event.effective_side == "support" else "short"
        if side == aligned_side:
            continue
        regime = (attenuation_multiplier(event, setting), event, zone)
        if side == "long" and event.effective_side == "resistance":
            bearish_trend_regimes.append(regime)
        if target < event_expiry(event, setting):
            countertrend_regimes.append(regime)

    reversal_entry = (
        _bullish_reversal_entry_zone(bearish_trend_regimes, entry_zones)
        if side == "long"
        else None
    )
    if reversal_entry is not None:
        _multiplier, event, zone = min(
            bearish_trend_regimes,
            key=lambda item: (item[0], -TF_RANK[item[1].timeframe]),
        )
        return AttenuationDecision(
            multiplier=1.0,
            blocked=False,
            block_reason="bullish_reversal_started",
            zone_id=zone.zone_id,
            timeframe=event.timeframe,
            role_epoch=event.role_epoch,
            strength=float(event.strength),
            entry_zone_id=reversal_entry.zone_id,
            reversal_started=True,
        )
    if not countertrend_regimes:
        return AttenuationDecision(1.0, False, "no_active_countertrend_regime")
    multiplier, event, zone = min(
        countertrend_regimes,
        key=lambda item: (item[0], -TF_RANK[item[1].timeframe]),
    )
    quota_key = f"{zone.zone_id}|epoch={event.role_epoch}"
    if multiplier <= 0:
        reason = "zero_multiplier"
        blocked = True
    elif quota_key in quota:
        reason = "epoch_quota_exhausted"
        blocked = True
    else:
        reason = "attenuated_countertrend"
        blocked = False
    return AttenuationDecision(
        multiplier=float(multiplier),
        blocked=blocked,
        block_reason=reason,
        zone_id=zone.zone_id,
        timeframe=event.timeframe,
        role_epoch=event.role_epoch,
        quota_key=quota_key,
        strength=float(event.strength),
    )
