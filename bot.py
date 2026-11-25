import ccxt
import pandas as pd
import numpy as np
import time
import json
import math
import os
import logging
import traceback
import sqlite3
import urllib.parse
import urllib.request
import re
import uuid
import sys
from collections import deque
from threading import Lock, Thread

from datetime import datetime, timezone
from dataclasses import dataclass, asdict
from typing import Any, Optional, List, Dict, Union, Tuple, Sequence

from dotenv import load_dotenv
import telegram_messages
import telegram_commands
from f_slot_accounting import AccountingPending, confirmed_execution, exit_reason_label, summarize_slot, write_execution_event
from zone_role_attenuation import (
    AttenuationDecision,
    BreakoutRule as ZoneRoleBreakoutRule,
    BullishReversalRule,
    EntryZone as ZoneRoleEntryZone,
    PolicySetting as ZoneRolePolicySetting,
    active_flipped_zone_states,
    consume_quota as consume_zone_role_quota,
    decide_attenuation,
    normalize_policy_state,
    qualifies_bullish_reversal_candle,
    record_bullish_trend_reset,
    reconstruct_context as reconstruct_zone_role_context,
    reserve_quota as reserve_zone_role_quota,
    state_at as zone_role_state_at,
)

load_dotenv()

# =========================
# 로깅
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler("bot.log"), logging.StreamHandler()]
)
log = logging.getLogger(__name__)

LOG_FILE = "bot.log"
TELEGRAM_MAX_LEN = 3500
TELEGRAM_RATE_WINDOW_SEC = 300
TELEGRAM_RATE_MAX_MESSAGES = 6
FETCH_LIMIT = 200
FETCH_PAUSE_SEC = 0.35
MAX_FETCH_PAGES = 100
RECENT_REFETCH_BARS = 3
DATA_CACHE: Dict[str, pd.DataFrame] = {}
HEARTBEAT_INTERVAL_SEC = 12 * 60 * 60
F_SIGNAL_STATUS_INTERVAL_SEC = int(os.getenv("BOT1_F_SIGNAL_STATUS_INTERVAL_SEC", "3600"))

# =========================
# 환경변수
# =========================
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY", "")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "")
TESTNET = os.getenv("BYBIT_TESTNET", "true").lower() == "true"

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
BOT_DASHBOARD_DB_PATH = os.getenv("BOT_DASHBOARD_DB_PATH", os.path.join("bot_dashboard", "bot_dashboard.db"))
BOT_STRATEGY_NAME = os.getenv("BOT_STRATEGY_NAME", "early_divergence")
BOT_NAME = os.getenv("BOT_NAME", "bybit-btc-bot")
AUTO_SET_LEVERAGE = os.getenv("AUTO_SET_LEVERAGE", "false").lower() == "true"

# =========================
# 전략 파라미터
# =========================
SYMBOL = os.getenv("SYMBOL", "BTC/USDT:USDT")
ENTRY_TF = "15m"
HTF_1 = "4h"
HTF_2 = "1d"
MONTHS_BACK = 3

POSITION_QTY_DECIMALS = 3
POSITION_MIN_LEG_SIZE_BTC = 0.001
TP1_POSITION_RATIO = 0.5
TP2_POSITION_RATIO = 0.5


def round_qty(qty: float) -> float:
    return float(round(qty, POSITION_QTY_DECIMALS))


def derive_position_split(
    total_size: float,
    tp1_ratio: float = TP1_POSITION_RATIO,
    tp2_ratio: float = TP2_POSITION_RATIO,
) -> Tuple[float, float]:
    if not all(math.isfinite(value) for value in (total_size, tp1_ratio, tp2_ratio)):
        raise ValueError(
            f"position size and split ratios must be finite: "
            f"total={total_size} tp1={tp1_ratio} tp2={tp2_ratio}"
        )
    total = round_qty(total_size)
    if total <= 0:
        raise ValueError(f"POSITION_SIZE_BTC must be positive: {total_size}")
    if tp1_ratio <= 0 or tp2_ratio <= 0 or tp1_ratio + tp2_ratio > 1.0 + 1e-9:
        raise ValueError(
            f"invalid position split ratios: tp1={tp1_ratio} tp2={tp2_ratio}"
        )

    tp1_size = round_qty(total * tp1_ratio)
    if abs(tp1_ratio + tp2_ratio - 1.0) <= 1e-9:
        tp2_size = round_qty(total - tp1_size)
    else:
        tp2_size = round_qty(total * tp2_ratio)

    if tp1_size < POSITION_MIN_LEG_SIZE_BTC or tp2_size < POSITION_MIN_LEG_SIZE_BTC:
        raise ValueError(
            f"derived TP leg is below minimum: total={total} tp1={tp1_size} tp2={tp2_size} "
            f"(POSITION_SIZE_BTC must be at least {2 * POSITION_MIN_LEG_SIZE_BTC:g} BTC)"
        )
    if tp1_size + tp2_size > total + POSITION_MIN_LEG_SIZE_BTC / 2:
        raise ValueError(
            f"derived TP split exceeds total: total={total} tp1={tp1_size} tp2={tp2_size}"
        )
    return tp1_size, tp2_size


_RAW_POSITION_SIZE_BTC = os.getenv("POSITION_SIZE_BTC", "").strip()
if not _RAW_POSITION_SIZE_BTC:
    raise SystemExit(
        "POSITION_SIZE_BTC is not set. Copy .env.example to .env and set the "
        "total campaign size in BTC (minimum 0.002; start small)."
    )
POSITION_SIZE_BTC = round_qty(float(_RAW_POSITION_SIZE_BTC))
TP1_SIZE, TP2_SIZE = derive_position_split(POSITION_SIZE_BTC)
POSITION_SIZE_TOLERANCE = POSITION_MIN_LEG_SIZE_BTC

# F live mode: three independent entry slots sharing one Bybit one-way
# position. The legacy single-position path remains available behind the
# environment switch for rollback, but production defaults to F.
F_LIVE_MODE = os.getenv("BOT1_F_LIVE_MODE", "1").strip().lower() not in {"0", "false", "no"}
F_TOTAL_SIZE_BTC = POSITION_SIZE_BTC
F_SLOT_WEIGHTS = (0.60, 0.25, 0.15)
F_TP1_RATIO = 0.30
F_TP2_RATIO = 0.70
F_ATR_SL_MULT = 5.0
F_ATR_TP1_MULT = 5.0
F_ATR_TP2_MULT = 10.0
F_TP1_STOP_RATIO = 0.25
F_STATE_FILE = os.getenv("BOT1_F_STATE_FILE", "bot1_f_state.json")
DCA_LIVE_MODE = os.getenv("BOT1_DCA_LIVE_MODE", "1").strip().lower() not in {"0", "false", "no"}
DCA_STATE_FILE = os.getenv("BOT1_DCA_STATE_FILE", "bot1_dca_state.json")
F_SESSION_LOCK_FILE = os.getenv("BOT1_F_SESSION_LOCK_FILE", "bot1_f_session.lock")

if not BYBIT_API_KEY or not BYBIT_API_SECRET:
    raise SystemExit(
        "BYBIT_API_KEY / BYBIT_API_SECRET are not set. "
        "Copy .env.example to .env and fill in your own Bybit API key."
    )
# 자동 레버리지 설정은 사용하지 않지만,
# 잔고 사전 체크에서 fallback 용도로만 사용한다.
# 거래소에서 실제 레버리지를 읽어오지 못할 때만 이 값을 쓴다.
ENTRY_LEVERAGE = int(os.getenv("ENTRY_LEVERAGE", "100"))
ENTRY_MARGIN_BUFFER_RATIO = 1.01
# Bybit VIP2 linear-perpetual standard rates. Exchange-reported fees remain
# authoritative for realized PnL; these values are fallback estimates only.
ENTRY_TAKER_FEE_RATE = 0.000375
ENTRY_MAKER_FEE_RATE = 0.00016

ZONE_ROLE_BREAKOUT_RULE = ZoneRoleBreakoutRule(
    penetration_atr=0.15,
    body_atr=0.30,
    close_location=0.60,
)
ZONE_ROLE_POLICY_SETTING = ZoneRolePolicySetting(
    ttl_1h_bars=12,
    ttl_4h_bars=3,
    ttl_1d_bars=5,
    multiplier_1h=0.50,
    multiplier_4h=0.25,
    multiplier_1d=0.00,
    countertrend_limit=1,
    strong_strength_atr=0.30,
    moderate_strength_multiplier=0.50,
    strong_strength_multiplier=0.25,
)
ZONE_ROLE_QUICK_EXIT_PROFILES = {
    "1h": {
        "name": "quick_1h",
        "sl_dist": 420.0,
        "tp1_dist": 210.0,
        "tp2_dist": 390.0,
        "tp1_stop_dist": 0.0,
        "max_hold_bars": 24,
        "max_hold_after_tp1": True,
    },
    "4h": {
        "name": "quick_4h",
        "sl_dist": 220.0,
        "tp1_dist": 140.0,
        "tp2_dist": 260.0,
        "tp1_stop_dist": 0.0,
        "max_hold_bars": 12,
        "max_hold_after_tp1": True,
    },
}

FIXED_SL = 550.0
TP1_DIST = 350.0
TP2_DIST = 650.0
TRAIL_ATR_MULT = 1.0
ATR_LEN = 14

HUNT_BUFFER = 300.0
BULLISH_REVERSAL_RULE = BullishReversalRule(
    min_lower_wick_range_ratio=0.50,
    max_reclaim_distance=HUNT_BUFFER,
)
WAVE_RESET_MOVE = 500.0
ZONE_MAX_VALID_TOUCHES = 2
ZONE_WAVE_PIVOT_ORDER = 2
MAX_HOLD_BARS = 96
ZONE_TOUCH_LOOKBACK = 16
DOJI_MAX_BODY = 50.0
DOJI_CLOUD_UPPER_BUFFER = 100.0
HUNT_EXPIRE_4H = 5
COOLDOWN_AFTER_MAXHOLD_4H = 8
DIVERGENCE_PIVOT = 3
EARLY_DIV_MIN_SEPARATION = 8
EARLY_DIV_MAX_SEPARATION = 192
EARLY_SWING_LOOKBACK = 8
EARLY_SWING_RESET_ATR_MULT = 0.5
EARLY_SWING_RESET_RSI = 5.0
EARLY_DIV_WARMUP = EARLY_DIV_MAX_SEPARATION + EARLY_SWING_LOOKBACK + 50

IMPULSE_MULT = 1.8
BODY_AVG_LEN = 20
OB_SEARCH_BACK = 3

STATE_FILE = "bot_state.json"
LOOP_SLEEP = 1
TP_POLL_SEC = 5  # TP 체결 즉시 감지 폴링 주기 (초)
POSITION_SYNC_RETRY_SEC = 1.0

EXCLUDED_SETUPS = {
    ("4h_ob", "early_bear_div_candidate"),
    ("4h_cloud", "early_bear_div_candidate"),
}

EXIT_PROFILE_PRESETS = {
    "expansion": {"tp1_dist": TP1_DIST, "tp2_dist": TP2_DIST},
    "balanced": {"tp1_dist": TP1_DIST, "tp2_dist": TP2_DIST},
    "conservative": {"tp1_dist": TP1_DIST, "tp2_dist": TP2_DIST},
    "four_hour_ob": {"tp1_dist": TP1_DIST, "tp2_dist": TP2_DIST},
}

SPLIT_PROFILE_PRESETS = {
    "default": {"tp1_ratio": TP1_POSITION_RATIO, "tp2_ratio": TP2_POSITION_RATIO},
    "front_loaded": {"tp1_ratio": TP1_POSITION_RATIO, "tp2_ratio": TP2_POSITION_RATIO},
}

STOP_PROFILE_PRESETS = {
    "default": {"sl_dist": FIXED_SL, "tp1_stop_dist": 50.0},
    "wide_4h_ob": {"sl_dist": 650.0, "tp1_stop_dist": 50.0},
}

STRATEGY_EXIT_PROFILE = {
    ("4h_ob", "early_bull_div_candidate"): "four_hour_ob",
    ("1d_ob", "early_bull_div_candidate"): "expansion",
    ("overlap", "early_bull_div_candidate"): "expansion",
    ("1d_ob", "early_bear_div_candidate"): "expansion",
    ("1d_cloud", "early_bear_div_candidate"): "expansion",
    ("overlap", "early_bear_div_candidate"): "expansion",
    ("4h_cloud_doji_short", "4h_cloud_doji_short"): "expansion",
}

STRATEGY_SPLIT_PROFILE = {
    ("4h_ob", "early_bull_div_candidate"): "front_loaded",
    ("1d_cloud", "early_bear_div_candidate"): "front_loaded",
}

STRATEGY_STOP_PROFILE = {
    ("4h_ob", "early_bull_div_candidate"): "wide_4h_ob",
}

# =========================
# 거래소 초기화
# =========================
exchange = ccxt.bybit({
    "apiKey": BYBIT_API_KEY,
    "secret": BYBIT_API_SECRET,
    "enableRateLimit": True,
    "options": {
        "defaultType": "linear",
    }
})

if TESTNET:
    exchange.set_sandbox_mode(True)
    log.info("🧪 테스트넷 모드")

EXCHANGE_NAME = os.getenv("EXCHANGE_NAME", getattr(exchange, "id", "unknown"))
TELEGRAM_SEND_TIMES = deque()
TELEGRAM_RATE_SUPPRESSED = 0
STRATEGY9_COOLDOWN_UNTIL: Optional[pd.Timestamp] = None
BOT1_RUNTIME: Dict[str, object] = {
    "pending_setups": [],
    "shadow_trades": [],
    "last_processed_closed_ts": None,
    "consumed_signal_ids": [],
    "zone_role_policy": None,
    "zone_role_policy_error": "",
    "buffer_zone_alert_ids": [],
}
ZONE_ROLE_CONTEXT_CACHE: Dict[str, object] = {"key": None, "context": None}
CONSUMED_SIGNAL_ID_LIMIT = 256

# =========================
# 텔레그램
# =========================
def can_send_telegram() -> bool:
    global TELEGRAM_RATE_SUPPRESSED
    now = time.time()
    while TELEGRAM_SEND_TIMES and now - TELEGRAM_SEND_TIMES[0] > TELEGRAM_RATE_WINDOW_SEC:
        TELEGRAM_SEND_TIMES.popleft()
    if len(TELEGRAM_SEND_TIMES) >= TELEGRAM_RATE_MAX_MESSAGES:
        TELEGRAM_RATE_SUPPRESSED += 1
        return False
    TELEGRAM_SEND_TIMES.append(now)
    return True

def consume_suppressed_telegram_notice() -> str:
    global TELEGRAM_RATE_SUPPRESSED
    if TELEGRAM_RATE_SUPPRESSED <= 0:
        return ""
    suppressed = TELEGRAM_RATE_SUPPRESSED
    TELEGRAM_RATE_SUPPRESSED = 0
    return f"⚠️ 이전에 {suppressed}개 알림이 전송 제한으로 생략됐습니다.\n\n"


def get_exit_profile(bucket: str, reason_key: str) -> Dict[str, float]:
    profile_name = STRATEGY_EXIT_PROFILE.get((bucket, reason_key), "expansion")
    return EXIT_PROFILE_PRESETS[profile_name]


def get_split_profile(
    bucket: str,
    reason_key: str,
    total_size: Optional[float] = None,
) -> Dict[str, float]:
    profile_name = STRATEGY_SPLIT_PROFILE.get((bucket, reason_key), "default")
    ratios = SPLIT_PROFILE_PRESETS[profile_name]
    tp1_size, tp2_size = derive_position_split(
        POSITION_SIZE_BTC if total_size is None else total_size,
        ratios["tp1_ratio"],
        ratios["tp2_ratio"],
    )
    return {
        "tp1_size": tp1_size,
        "tp2_size": tp2_size,
        "tp1_ratio": ratios["tp1_ratio"],
        "tp2_ratio": ratios["tp2_ratio"],
    }


def get_stop_profile(bucket: str, reason_key: str) -> Dict[str, float]:
    profile_name = STRATEGY_STOP_PROFILE.get((bucket, reason_key), "default")
    return STOP_PROFILE_PRESETS[profile_name]


def normalize_reason_key(reason_key: str) -> str:
    if reason_key.endswith("_NO_ENTRY_SL"):
        return reason_key[: -len("_NO_ENTRY_SL")]
    return reason_key


def resolve_entry_execution_profile(
    bucket: str,
    reason_key: str,
    zone_role_decision: Optional[AttenuationDecision] = None,
) -> Dict[str, Union[str, float, int, bool]]:
    exit_profile = get_exit_profile(bucket, reason_key)
    stop_profile = get_stop_profile(bucket, reason_key)
    profile: Dict[str, Union[str, float, int, bool]] = {
        "name": "current",
        "sl_dist": float(stop_profile["sl_dist"]),
        "tp1_dist": float(exit_profile["tp1_dist"]),
        "tp2_dist": float(exit_profile["tp2_dist"]),
        "tp1_stop_dist": float(stop_profile["tp1_stop_dist"]),
        "max_hold_bars": MAX_HOLD_BARS,
        "max_hold_after_tp1": False,
    }
    if zone_role_decision is None:
        return profile
    decision = validate_zone_role_decision(zone_role_decision)
    if decision.blocked:
        raise ValueError("blocked zone-role decision cannot create an entry profile")
    if decision.multiplier >= 1.0:
        return profile
    quick_profile = ZONE_ROLE_QUICK_EXIT_PROFILES.get(decision.timeframe)
    if quick_profile is None:
        raise ValueError(f"quick exit profile missing for timeframe: {decision.timeframe}")
    return dict(quick_profile)

def send_telegram(msg: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    if not can_send_telegram():
        log.warning("텔레그램 전송 rate limit 적용: 메시지 전송 생략")
        return
    msg = format_telegram_bullets(
        localize_telegram_message(f"{consume_suppressed_telegram_notice()}{msg}")
    )
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": telegram_messages.strip_card_mark(msg),
        }).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=10) as _:
            pass
    except Exception as e:
        log.warning(f"텔레그램 전송 실패: {e}")

def send_telegram_chunks(msg: str, chunk_size: int = TELEGRAM_MAX_LEN):
    if not msg:
        return
    for i in range(0, len(msg), chunk_size):
        send_telegram(msg[i:i + chunk_size])

def notify(msg: str):
    localized = format_telegram_bullets(localize_telegram_message(msg))
    log.info(telegram_messages.strip_card_mark(localized))
    send_telegram(localized)

def read_recent_log_lines(lines: int = 80) -> str:
    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            return "".join(f.readlines()[-lines:]).strip()
    except Exception as e:
        return f"(최근 로그 조회 실패: {e})"

def redact_secret_value(text: str, secret: str, replacement: str = "[redacted]") -> str:
    if not text or not secret:
        return text
    return text.replace(secret, replacement)

def sanitize_telegram_diagnostic_text(text: str) -> str:
    if not text:
        return text

    sanitized = str(text)

    for secret in (
        BYBIT_API_KEY,
        BYBIT_API_SECRET,
        TELEGRAM_BOT_TOKEN,
        TELEGRAM_CHAT_ID,
    ):
        sanitized = redact_secret_value(sanitized, secret)

    redaction_rules = [
        (r"\b[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\b", "[order-id-redacted]"),
        (r"bot[0-9]{8,}:[A-Za-z0-9_-]{20,}", "[telegram-token-redacted]"),
        (r"(?i)(api[_ -]?key|secret|token|chat[_ -]?id)\s*[:=]\s*[^\s\n]+", r"\1=[redacted]"),
        (r"(?i)(order(Link)?Id|trade_id|event_id|entry_order_id|tp1_order_id|tp2_order_id|sl_order_id)\s*[:=]\s*[^\s\n]+", r"\1=[redacted]"),
        (r"(?i)\bsize=\d+(?:\.\d+)?\s*BTC\b", "size=[redacted] BTC"),
        (r"(?i)\bremain=\d+(?:\.\d+)?\s*BTC\b", "remain=[redacted] BTC"),
        (r"(?i)\bqty=\d+(?:\.\d+)?\b", "qty=[redacted]"),
    ]
    for pattern, replacement in redaction_rules:
        sanitized = re.sub(pattern, replacement, sanitized)

    return sanitized


def localize_telegram_message(text: str) -> str:
    """Convert internal alert fields to user-facing Korean labels."""
    return telegram_messages.localize(text)


def format_telegram_bullets(text: str) -> str:
    """Render one alert as a compact title followed by bullet points."""
    return telegram_messages.bulletize(text)


def build_exception_report(context: str, exc: Exception) -> str:
    """사용자 알림은 상황과 다음 행동만 담고, 상세 진단은 로그에 남긴다."""
    tb = sanitize_telegram_diagnostic_text(traceback.format_exc().strip())
    safe_message = sanitize_telegram_diagnostic_text(str(exc))
    log.error(f"{context} | detail={safe_message}\n{tb}")
    return (
        f"❗ 오류 발생: {context}\n"
        f"내용: {safe_message}\n"
        "봇은 자동 재시도를 계속합니다. 반복되면 확인이 필요합니다."
    )

def _stored_buffer_zone_alert_ids() -> set[str]:
    stored = BOT1_RUNTIME.get("buffer_zone_alert_ids")
    if not isinstance(stored, list):
        stored = []
    normalized = {
        item.strip()
        for item in stored
        if isinstance(item, str) and item.strip()
    }
    BOT1_RUNTIME["buffer_zone_alert_ids"] = sorted(normalized)
    return normalized


def format_heartbeat_alert(has_position: bool, now=None) -> str:
    """Render existing diagnostics only; never reconstruct or schedule observers."""
    from datetime import timedelta

    def aware_time(value):
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError("heartbeat timestamp requires timezone")
        return parsed.astimezone(timezone.utc)

    kst = timezone(timedelta(hours=9))
    invalid_now = False
    try:
        current = datetime.now(timezone.utc) if now is None else aware_time(now)
        current_kst = current.astimezone(kst)
    except (TypeError, ValueError, OverflowError):
        invalid_now = True
        current = datetime.now(timezone.utc)
        current_kst = current.astimezone(kst)
    runtime = F_RUNTIME if F_LIVE_MODE else BOT1_RUNTIME
    last_closed_text = "-"
    last_closed = None
    try:
        last_closed = aware_time(runtime.get("last_processed_closed_ts"))
        start = last_closed.astimezone(kst)
        end = start + timedelta(minutes=15)
        last_closed_text = f"{start:%Y-%m-%d %H:%M} ~ {end:%Y-%m-%d %H:%M} KST"
    except (TypeError, ValueError, OverflowError):
        last_closed = None
    if F_LIVE_MODE:
        zone_text = ""
        try:
            diagnostics = runtime.get("last_signal_diagnostics")
            bar_time = aware_time(diagnostics["bar_time"])
            direction = diagnostics["direction"]
            count = diagnostics["eligible_zones"]
            stage = diagnostics["stage"]
            if (
                not invalid_now and last_closed is not None and bar_time == last_closed
                and 900 <= (current - bar_time).total_seconds() <= 1800
                and direction in {"long", "short"}
                and type(count) is int and count > 0
                and stage == "zone_ready"
            ):
                zone_text = f" · 진입 가능 존 {count}"
        except (TypeError, ValueError, KeyError, OverflowError, AttributeError):
            pass
    else:
        buffered = len(_stored_buffer_zone_alert_ids())
        zone_text = f" · 관찰 존 {buffered}" if buffered else ""
    stamp = current_kst.strftime("%m-%d %H:%M")
    return (
        f"🟢 정상 가동 · {stamp} KST\n"
        f"최근봉 {last_closed_text or '-'}{zone_text}"
    )


def maybe_send_heartbeat(next_heartbeat_at: float, has_position: bool) -> float:
    now = time.time()
    if now < next_heartbeat_at:
        return next_heartbeat_at

    if has_position:
        log.info("💓 생존신고 보류 | 포지션 추적 중")
        return next_heartbeat_at

    notify(format_heartbeat_alert(has_position))
    return now + HEARTBEAT_INTERVAL_SEC

def iso_utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


ENTRY_REASON_LABELS = {
    "early_bull_div_candidate": "상승 진입 조건",
    "early_bear_div_candidate": "하락 진입 조건",
    "4h_cloud_doji_short": "4시간 구름대 도지",
    "f_volume_ema_or_ema50": "거래량 급증 그리고 이평 돌파",
}

ENTRY_BUCKET_LABELS = {
    "1h_ob": "1시간 OB",
    "1h_cloud": "1시간 구름대",
    "4h_cloud": "4시간 구름대",
    "4h_ob": "4시간 OB",
    "1d_ob": "일봉 OB",
    "flipped_4h_ob": "전환 4시간 OB",
    "flipped_1d_ob": "전환 일봉 OB",
    "overlap": "중첩 존",
    "4h_cloud_doji_short": "4시간 구름대",
}


def format_kst_candle_window(timestamp, timeframe: str) -> str:
    """Return an exact candle start/end range in KST for Telegram."""
    try:
        start = pd.Timestamp(timestamp)
        if pd.isna(start):
            return "-"
        if start.tzinfo is None:
            start = start.tz_localize("UTC")
        else:
            start = start.tz_convert("UTC")
        end = start + pd.to_timedelta(timeframe_to_ms(timeframe), unit="ms")
        start_text = start.tz_convert("Asia/Seoul").strftime("%Y-%m-%d %H:%M")
        end_text = end.tz_convert("Asia/Seoul").strftime("%Y-%m-%d %H:%M")
        return f"{start_text} ~ {end_text} KST"
    except (TypeError, ValueError, OverflowError):
        return str(timestamp)


def format_zone_origin_candle(zone: Dict) -> str:
    """존이 만들어진 원본 봉을 KST 구간으로 돌려준다.

    오더블럭은 `anchor_time`, 구름대 선은 `segment_start`가 원본 시점이다.
    존의 기준 시간대(`source_tf` 또는 `tf`)로 봉 길이를 계산한다.
    """
    if not isinstance(zone, dict):
        return "-"
    origin = (
        zone.get("anchor_time")
        or zone.get("segment_start")
        or zone.get("active_from")
    )
    if origin is None:
        return "-"
    timeframe = str(zone.get("source_tf") or zone.get("tf") or ENTRY_TF)
    return format_kst_candle_window(origin, timeframe)


def _format_alert_price(value) -> str:
    try:
        return f"{float(value):,.1f}"
    except (TypeError, ValueError):
        return "-"


def _format_alert_qty(value) -> str:
    try:
        return f"{float(value):.4f}".rstrip("0").rstrip(".")
    except (TypeError, ValueError):
        return "-"


def _detail_value(detail: str, key: str) -> str:
    match = re.search(rf"{re.escape(key)}=([^,\]]+)", detail)
    return match.group(1).strip() if match else ""


def _format_ob_evidence(details: str) -> List[str]:
    lines: List[str] = []
    for detail in str(details or "").split(" | "):
        if "anchor_time=" not in detail:
            continue
        name_match = re.match(r"([^\(]+)\(", detail)
        name = name_match.group(1).strip() if name_match else "OB"
        source_tf = _detail_value(detail, "source_tf") or "4h"
        ob_type = _detail_value(detail, "ob_type")
        direction = "상승" if ob_type == "bull" else "하락" if ob_type == "bear" else "OB"
        label = {
            "4h": "4시간",
            "1d": "일봉",
        }.get(source_tf, source_tf)
        anchor_time = _detail_value(detail, "anchor_time")
        impulse_time = _detail_value(detail, "impulse_time")
        touch_time = _detail_value(detail, "touch_time")
        ohlc = " / ".join(
            f"{label} {_format_alert_price(_detail_value(detail, key))}"
            for label, key in (
                ("O", "anchor_open"),
                ("H", "anchor_high"),
                ("L", "anchor_low"),
                ("C", "anchor_close"),
            )
        )
        lines.extend([
            f"{label} {direction} OB ({name})",
            f"원본봉: {format_kst_candle_window(anchor_time, source_tf)}",
            f"원본봉 OHLC: {ohlc}",
            f"확인봉: {format_kst_candle_window(impulse_time, source_tf)}",
        ])
        if touch_time and touch_time != "na":
            lines.append(f"재터치봉: {format_kst_candle_window(touch_time, ENTRY_TF)}")
    return lines


def format_entry_evidence(details: str, signal_bar_time) -> str:
    """Format source candles and the final closed trigger candle for Telegram."""
    detail_text = str(details or "").strip()
    lines: List[str] = []
    ob_lines = _format_ob_evidence(detail_text)
    if ob_lines:
        lines.extend(["[근거 OB]", *ob_lines])
    elif "구름대" in detail_text or "도지" in detail_text:
        condition = detail_text.split("|", 1)[0]
        lines.extend(["[근거 구름대/도지]", f"조건: {condition}"])
        doji_candle = _detail_value(detail_text, "doji_candle")
        if doji_candle:
            lines.append(f"도지봉: {doji_candle}")
    else:
        lines.extend(["[근거]", f"조건: {detail_text or '-'}"])
    lines.append(f"진입봉: {format_kst_candle_window(signal_bar_time, ENTRY_TF)}")
    return "\n".join(lines)


def format_entry_alert(
    side: str,
    entry_price: float,
    entry_size: float,
    stop_price: float,
    tp1_price: float,
    tp1_size: float,
    tp2_price: float,
    tp2_size: float,
    reason: str,
    bucket: str,
    details: str,
    signal_bar_time,
    sl_id: str = "",
    tp1_id: str = "",
    tp2_id: str = "",
    zone_role_decision: Optional[AttenuationDecision] = None,
) -> str:
    side_label = "롱" if side == "long" else "숏"
    symbol_label = SYMBOL.replace(":USDT", "")
    reason_label = ENTRY_REASON_LABELS.get(reason, reason or "-")
    bucket_label = ENTRY_BUCKET_LABELS.get(bucket, bucket or "-")
    protection = (
        f"SL {'✅' if sl_id else '❌'}  "
        f"TP1 {'✅' if tp1_id else '❌'}  "
        f"TP2 {'✅' if tp2_id else '❌'}"
    )
    attenuation_lines: List[str] = []
    if zone_role_decision is not None:
        decision = validate_zone_role_decision(zone_role_decision)
        if decision.reversal_started:
            attenuation_lines = [
                "🔄 긴 아래꼬리 상승 반전",
                "감쇠: 해제 · 정상 수량",
                f"반전 기준 존: {decision.entry_zone_id}",
                "상단 존 돌파 전에는 숏 감쇠 미적용",
            ]
        elif decision.multiplier < 1.0 and not decision.blocked:
            attenuation_lines = [
                "⚠️ 감쇠 발동",
                (
                    f"감쇠: {decision.timeframe} x{decision.multiplier:.2f} "
                    f"({POSITION_SIZE_BTC:g} BTC → {entry_size:g} BTC)"
                ),
                f"돌파 강도: {decision.strength:.2f} ATR",
                f"감쇠 사유: {decision.block_reason or 'countertrend'}",
            ]
    return format_telegram_bullets(localize_telegram_message("\n".join([
        f"{'🟢' if side == 'long' else '🔴'} {BOT_NAME} · {symbol_label} · {side_label} 진입 완료",
        "",
        f"진입가: {_format_alert_price(entry_price)}",
        f"수량: {_format_alert_qty(entry_size)} BTC",
        f"조건: {reason_label} · {bucket_label}",
        f"손절: {_format_alert_price(stop_price)}",
        f"익절 1: {_format_alert_price(tp1_price)} · {_format_alert_qty(tp1_size)} BTC",
        f"익절 2: {_format_alert_price(tp2_price)} · {_format_alert_qty(tp2_size)} BTC",
        f"보호주문: {protection}",
        *attenuation_lines,
        "",
        format_entry_evidence(details, signal_bar_time),
    ])))

def open_dashboard_db() -> sqlite3.Connection:
    conn = sqlite3.connect(BOT_DASHBOARD_DB_PATH, timeout=5.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn

def add_column_if_missing(conn: sqlite3.Connection, table_name: str, column_name: str, column_def: str):
    existing = {
        row[1]
        for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    }
    if column_name in existing:
        return
    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_def}")

def ensure_dashboard_db():
    if not BOT_DASHBOARD_DB_PATH:
        return
    db_dir = os.path.dirname(BOT_DASHBOARD_DB_PATH)
    if db_dir:
        os.makedirs(db_dir, exist_ok=True)
    with open_dashboard_db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trades (
                trade_id TEXT PRIMARY KEY,
                bot_name TEXT NOT NULL,
                exchange_name TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                status TEXT NOT NULL,
                entry_time TEXT,
                exit_time TEXT,
                entry_price REAL,
                exit_price REAL,
                size_initial REAL,
                size_remaining REAL,
                gross_pnl REAL,
                fees_total REAL,
                fees_currency TEXT,
                realized_pnl REAL,
                strategy_name TEXT,
                entry_reason TEXT,
                leader_bucket TEXT,
                leader_details TEXT,
                sl_dist REAL,
                tp1_dist REAL,
                tp2_dist REAL,
                tp1_stop_dist REAL,
                tp1_size REAL,
                tp2_size REAL,
                entry_order_id TEXT,
                tp1_order_id TEXT,
                tp2_order_id TEXT,
                sl_order_id TEXT,
                close_reason TEXT,
                notes TEXT,
                updated_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS trade_events (
                event_id TEXT PRIMARY KEY,
                trade_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                event_time TEXT NOT NULL,
                price REAL,
                size REAL,
                pnl_delta REAL,
                fee_amount REAL,
                fee_currency TEXT,
                note TEXT,
                order_id TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        add_column_if_missing(conn, "trades", "gross_pnl", "REAL")
        add_column_if_missing(conn, "trades", "fees_total", "REAL")
        add_column_if_missing(conn, "trades", "fees_currency", "TEXT")
        add_column_if_missing(conn, "trades", "sl_dist", "REAL")
        add_column_if_missing(conn, "trades", "tp1_dist", "REAL")
        add_column_if_missing(conn, "trades", "tp2_dist", "REAL")
        add_column_if_missing(conn, "trades", "tp1_stop_dist", "REAL")
        add_column_if_missing(conn, "trades", "tp1_size", "REAL")
        add_column_if_missing(conn, "trades", "tp2_size", "REAL")
        add_column_if_missing(conn, "trade_events", "fee_amount", "REAL")
        add_column_if_missing(conn, "trade_events", "fee_currency", "TEXT")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trades_entry_time ON trades(entry_time DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_trade_events_trade_id ON trade_events(trade_id, event_time)")

def estimate_order_fill_price(order: Optional[Dict], fallback_price: Optional[float]) -> Optional[float]:
    if not order:
        return None if fallback_price is None else float(fallback_price)
    candidates = [
        order.get("average"),
        order.get("price"),
        (order.get("info") or {}).get("avgPrice"),
        (order.get("info") or {}).get("price"),
    ]
    for candidate in candidates:
        try:
            if candidate is None:
                continue
            value = float(candidate)
            if value > 0:
                return value
        except (TypeError, ValueError):
            continue
    return None if fallback_price is None else float(fallback_price)

def estimate_order_fill_amount(order: Optional[Dict], fallback_amount: float) -> float:
    if not order:
        return float(fallback_amount)
    candidates = [
        order.get("filled"),
        order.get("amount"),
        (order.get("info") or {}).get("cumExecQty"),
        (order.get("info") or {}).get("qty"),
    ]
    for candidate in candidates:
        try:
            if candidate is None:
                continue
            value = float(candidate)
            if value > 0:
                return value
        except (TypeError, ValueError):
            continue
    return float(fallback_amount)

def estimate_order_fee(order: Optional[Dict]) -> Tuple[float, str]:
    if not order:
        return 0.0, ""
    fee = order.get("fee") or {}
    cost_candidates = [
        fee.get("cost"),
        (order.get("info") or {}).get("cumExecFee"),
        (order.get("info") or {}).get("execFee"),
    ]
    currency_candidates = [
        fee.get("currency"),
        (order.get("info") or {}).get("feeCurrency"),
    ]
    fee_amount = 0.0
    for candidate in cost_candidates:
        try:
            if candidate is None:
                continue
            fee_amount = abs(float(candidate))
            break
        except (TypeError, ValueError):
            continue
    fee_currency = ""
    for candidate in currency_candidates:
        if candidate:
            fee_currency = str(candidate)
            break
    return fee_amount, fee_currency

def fetch_order_record(order_id: str) -> Dict:
    """Use Bybit's history endpoint instead of its guarded recent-order API."""
    if exchange.id == "bybit" and exchange.has.get("fetchClosedOrder"):
        try:
            order = exchange.fetch_closed_order(order_id, SYMBOL)
        except Exception as exc:
            if type(exc).__name__ != "OrderNotFound":
                raise
            order = exchange.fetch_open_order(order_id, SYMBOL)
    else:
        order = exchange.fetch_order(order_id, SYMBOL)
    if str(order.get("id") or "") != str(order_id):
        raise ValueError("order response ID mismatch")
    return order


def resolve_order_fill(order_id: str, fallback_price: Optional[float], fallback_amount: float) -> Tuple[Optional[float], float, float, str]:
    if not order_id:
        return (None if fallback_price is None else float(fallback_price)), float(fallback_amount), 0.0, ""

    if exchange.has.get("fetchOrder"):
        try:
            order = fetch_order_record(order_id)
            price = estimate_order_fill_price(order, fallback_price)
            amount = estimate_order_fill_amount(order, fallback_amount)
            fee_amount, fee_currency = estimate_order_fee(order)
            if price is not None and price > 0 and amount > 0:
                return price, amount, fee_amount, fee_currency
        except Exception as e:
            log.warning(f"fetch_order 체결가 조회 실패({order_id}): {e}")

    if exchange.has.get("fetchMyTrades"):
        def _trades_once():
            """예외는 호출부로 전파(API 에러는 retry 금지). 빈 결과만 None."""
            trades = exchange.fetch_my_trades(SYMBOL, None, 50, {"orderId": order_id})
            total_qty = 0.0
            total_value = 0.0
            total_fee = 0.0
            fee_currency = ""
            for trade in trades:
                try:
                    qty = float(trade.get("amount") or trade.get("info", {}).get("execQty") or 0.0)
                    price = float(trade.get("price") or trade.get("info", {}).get("execPrice") or 0.0)
                except (TypeError, ValueError):
                    continue
                if qty <= 0 or price <= 0:
                    continue
                total_qty += qty
                total_value += qty * price
                fee = trade.get("fee") or {}
                fee_candidate = fee.get("cost") if fee else None
                if fee_candidate is None:
                    fee_candidate = trade.get("info", {}).get("execFee")
                try:
                    if fee_candidate is not None:
                        total_fee += abs(float(fee_candidate))
                except (TypeError, ValueError):
                    pass
                if not fee_currency:
                    fee_currency = str(
                        fee.get("currency")
                        or trade.get("info", {}).get("feeCurrency")
                        or ""
                    )
            if total_qty > 0 and total_value > 0:
                return total_value / total_qty, total_qty, total_fee, fee_currency
            return None
        try:
            result = _trades_once()
        except Exception as e:
            log.warning(f"fetch_my_trades 체결가 조회 실패({order_id}): {e}")
            result = None
        else:
            if result is None:
                time.sleep(1.0)
                try:
                    result = _trades_once()
                except Exception as e:
                    log.warning(f"fetch_my_trades 체결가 retry 실패({order_id}): {e}")
                    result = None
                else:
                    if result is not None:
                        log.info(f"resolve_order_fill retry 성공 ({order_id})")
        if result is not None:
            return result

    return (None if fallback_price is None else float(fallback_price)), float(fallback_amount), 0.0, ""


def order_has_confirmed_fill(order_id: str, expected_amount: float) -> bool:
    if not order_id or expected_amount <= 0:
        return False

    if exchange.has.get("fetchOrder"):
        try:
            order = fetch_order_record(order_id)
            candidates = [
                order.get("filled"),
                (order.get("info") or {}).get("cumExecQty"),
            ]
            for candidate in candidates:
                try:
                    if candidate is not None and round_qty(float(candidate)) >= round_qty(expected_amount):
                        return True
                except (TypeError, ValueError):
                    continue
        except Exception as e:
            log.warning(f"fetch_order 체결 확인 실패({order_id}): {e}")

    if exchange.has.get("fetchMyTrades"):
        try:
            trades = exchange.fetch_my_trades(SYMBOL, None, 100, {"orderId": order_id})
            total_filled = 0.0
            for trade in trades:
                try:
                    total_filled += float(
                        trade.get("amount")
                        or (trade.get("info") or {}).get("execQty")
                        or 0.0
                    )
                except (TypeError, ValueError):
                    continue
            return round_qty(total_filled) >= round_qty(expected_amount)
        except Exception as e:
            log.warning(f"fetch_my_trades 체결 확인 실패({order_id}): {e}")
    return False


def fetch_closed_pnl(since_entry_time: Optional[str] = None, limit: int = 20) -> List[Dict]:
    """Bybit getClosedPnl API — 진입 이후 모든 청산 PnL 레코드 반환"""
    try:
        params: Dict = {
            "category": "linear",
            "symbol": SYMBOL.replace("/", "").replace(":USDT", ""),
            "limit": str(limit),
        }
        if since_entry_time:
            try:
                params["startTime"] = str(int(pd.Timestamp(since_entry_time).timestamp() * 1000))
            except Exception:
                pass
        resp = exchange.private_get_v5_position_closed_pnl(params)
        return (resp.get("result") or {}).get("list") or []
    except Exception as e:
        log.warning(f"getClosedPnl 조회 실패: {e}")
        return []


def sum_closed_pnl(since_entry_time: Optional[str] = None) -> Tuple[Optional[float], Optional[float]]:
    """진입 이후 거래소 확정 PnL 합산. 반환: (realized_pnl, fees_total). 실패 시 (None, None)."""
    records = fetch_closed_pnl(since_entry_time)
    if not records:
        return None, None
    total_pnl = 0.0
    total_fee = 0.0
    for rec in records:
        total_pnl += float(rec.get("closedPnl") or 0)
        entry_val = abs(float(rec.get("cumEntryValue") or 0))
        exit_val = abs(float(rec.get("cumExitValue") or 0))
        total_fee += (entry_val + exit_val) * ENTRY_TAKER_FEE_RATE
    return total_pnl, total_fee


def calc_leg_pnl(side: str, entry_price: float, exit_price: float, size: float) -> float:
    move = exit_price - entry_price if side == "long" else entry_price - exit_price
    return move * size

def estimate_total_fees(pos: "Position", close_fee: float = 0.0) -> float:
    return abs(pos.entry_fee or 0.0) + abs(pos.tp1_fee or 0.0) + abs(pos.tp2_fee or 0.0) + abs(close_fee or 0.0)


def estimate_realized_pnl(pos: "Position", exit_price: float, exit_size: Optional[float] = None) -> float:
    """이미 체결된 legs(TP1/TP2) + 최종 청산을 모두 합산해 gross PnL 추정."""
    pnl = 0.0
    if pos.tp1_hit:
        tp1_price = pos.tp1_fill_price if pos.tp1_fill_price is not None else pos.tp1_price
        tp1_size = pos.tp1_fill_size if pos.tp1_fill_size is not None else pos.tp1_size
        pnl += calc_leg_pnl(pos.side, pos.entry_price, tp1_price, tp1_size)
    if pos.tp2_hit:
        tp2_price = pos.tp2_fill_price if pos.tp2_fill_price is not None else pos.tp2_price
        tp2_size = pos.tp2_fill_size if pos.tp2_fill_size is not None else pos.tp2_size
        pnl += calc_leg_pnl(pos.side, pos.entry_price, tp2_price, tp2_size)
    final_size = round_qty(exit_size if exit_size is not None else pos.size_remaining)
    if final_size > 0:
        pnl += calc_leg_pnl(pos.side, pos.entry_price, exit_price, final_size)
    return pnl

def dashboard_upsert_trade(
    pos: "Position",
    status: str = "open",
    exit_price: Optional[float] = None,
    exit_time: Optional[str] = None,
    gross_pnl: Optional[float] = None,
    fees_total: Optional[float] = None,
    fees_currency: str = "",
    realized_pnl: Optional[float] = None,
    close_reason: str = "",
    notes: str = "",
    conn: Optional[sqlite3.Connection] = None,
):
    if not BOT_DASHBOARD_DB_PATH or not pos.trade_id:
        return
    ensure_dashboard_db()
    updated_at = iso_utc_now()
    should_close = conn is None
    conn = conn or open_dashboard_db()
    try:
        conn.execute(
            """
            INSERT INTO trades (
                trade_id, bot_name, exchange_name, symbol, side, status,
                entry_time, exit_time, entry_price, exit_price,
                size_initial, size_remaining, gross_pnl, fees_total, fees_currency, realized_pnl,
                strategy_name, entry_reason, leader_bucket, leader_details,
                sl_dist, tp1_dist, tp2_dist, tp1_stop_dist, tp1_size, tp2_size,
                entry_order_id, tp1_order_id, tp2_order_id, sl_order_id,
                close_reason, notes, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(trade_id) DO UPDATE SET
                bot_name=excluded.bot_name,
                exchange_name=excluded.exchange_name,
                symbol=excluded.symbol,
                side=excluded.side,
                status=excluded.status,
                entry_time=excluded.entry_time,
                exit_time=excluded.exit_time,
                entry_price=excluded.entry_price,
                exit_price=excluded.exit_price,
                size_initial=excluded.size_initial,
                size_remaining=excluded.size_remaining,
                gross_pnl=excluded.gross_pnl,
                fees_total=excluded.fees_total,
                fees_currency=excluded.fees_currency,
                realized_pnl=excluded.realized_pnl,
                strategy_name=excluded.strategy_name,
                entry_reason=excluded.entry_reason,
                leader_bucket=excluded.leader_bucket,
                leader_details=excluded.leader_details,
                sl_dist=excluded.sl_dist,
                tp1_dist=excluded.tp1_dist,
                tp2_dist=excluded.tp2_dist,
                tp1_stop_dist=excluded.tp1_stop_dist,
                tp1_size=excluded.tp1_size,
                tp2_size=excluded.tp2_size,
                entry_order_id=excluded.entry_order_id,
                tp1_order_id=excluded.tp1_order_id,
                tp2_order_id=excluded.tp2_order_id,
                sl_order_id=excluded.sl_order_id,
                close_reason=excluded.close_reason,
                notes=excluded.notes,
                updated_at=excluded.updated_at
            """,
            (
                pos.trade_id,
                BOT_NAME,
                EXCHANGE_NAME,
                SYMBOL,
                pos.side,
                status,
                pos.entry_time,
                exit_time,
                pos.entry_price,
                exit_price,
                get_position_entry_size(pos),
                0.0 if status == "closed" else pos.size_remaining,
                gross_pnl,
                fees_total,
                fees_currency,
                realized_pnl,
                pos.leader_bucket or BOT_STRATEGY_NAME,
                pos.entry_reason,
                pos.leader_bucket,
                pos.leader_details,
                pos.sl_dist,
                pos.tp1_dist,
                pos.tp2_dist,
                pos.tp1_stop_dist,
                pos.tp1_size,
                pos.tp2_size,
                pos.entry_order_id,
                pos.tp1_order_id,
                pos.tp2_order_id,
                pos.sl_order_id,
                close_reason,
                notes,
                updated_at,
            ),
        )
        if should_close:
            conn.commit()
    finally:
        if should_close:
            conn.close()

def dashboard_append_event(
    pos: "Position",
    event_type: str,
    event_time: Optional[str] = None,
    price: Optional[float] = None,
    size: Optional[float] = None,
    pnl_delta: Optional[float] = None,
    fee_amount: Optional[float] = None,
    fee_currency: str = "",
    note: str = "",
    order_id: str = "",
    event_key: str = "",
    conn: Optional[sqlite3.Connection] = None,
):
    if not BOT_DASHBOARD_DB_PATH or not pos.trade_id:
        return
    ensure_dashboard_db()
    event_time = event_time or iso_utc_now()
    event_id = event_key or f"{pos.trade_id}:{event_type}:{event_time}"
    should_close = conn is None
    conn = conn or open_dashboard_db()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO trade_events (
                event_id, trade_id, event_type, event_time,
                price, size, pnl_delta, fee_amount, fee_currency, note, order_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id,
                pos.trade_id,
                event_type,
                event_time,
                price,
                size,
                pnl_delta,
                fee_amount,
                fee_currency,
                note,
                order_id,
                iso_utc_now(),
            ),
        )
        if should_close:
            conn.commit()
    finally:
        if should_close:
            conn.close()

def dashboard_record_entry(pos: "Position", note: str = ""):
    with open_dashboard_db() as conn:
        dashboard_upsert_trade(
            pos,
            status="open",
            gross_pnl=0.0,
            fees_total=abs(pos.entry_fee or 0.0),
            fees_currency=pos.fees_currency,
            realized_pnl=-(abs(pos.entry_fee or 0.0)),
            notes=note,
            conn=conn,
        )
        dashboard_append_event(
            pos,
            "entry",
            event_time=pos.entry_time,
            price=pos.entry_price,
            size=get_position_entry_size(pos),
            fee_amount=abs(pos.entry_fee or 0.0),
            fee_currency=pos.fees_currency,
            note=note or "시장가 진입",
            order_id=pos.entry_order_id,
            event_key=f"{pos.trade_id}:entry",
            conn=conn,
        )
        conn.commit()

def dashboard_record_close(
    pos: "Position",
    close_reason: str,
    exit_price: Optional[float],
    exit_size: Optional[float] = None,
    close_fee: float = 0.0,
    fee_currency: str = "",
    note: str = "",
    order_id: str = "",
    event_time: Optional[str] = None,
):
    if not pos.trade_id:
        return
    event_time = event_time or iso_utc_now()
    # 거래소 확정 PnL 조회 — orderId 화이트리스트로 현재 trade 레코드만 필터 (E023)
    exch_records_all = fetch_closed_pnl(pos.entry_time)
    current_order_ids = {oid for oid in (
        pos.entry_order_id, pos.tp1_order_id, pos.tp2_order_id, pos.sl_order_id, order_id,
    ) if oid}
    exch_records = [r for r in (exch_records_all or []) if (r.get("orderId") or "") in current_order_ids] if current_order_ids else []
    # E027: whitelist 밖 close-side 레코드를 orphan으로 합산. 순수 수동 종료 + partial TP 후
    # 수동 종료 모두 처리한다. E023 재회귀 방지는 `createdTime >= pos.entry_time_ms`로 — 이 trade
    # 진입 이전에 생성된 주문(이전 trade의 SL 등)은 같은 side라도 제외된다.
    try:
        entry_ms_for_orphan = int(pd.Timestamp(pos.entry_time).timestamp() * 1000) if pos.entry_time else 0
    except Exception:
        entry_ms_for_orphan = 0
    if exch_records_all and entry_ms_for_orphan:
        close_side = "Sell" if pos.side == "long" else "Buy"
        already_oids = {(r.get("orderId") or "") for r in exch_records}
        orphans = []
        for r in exch_records_all:
            oid = r.get("orderId") or ""
            if not oid or oid in current_order_ids or oid in already_oids:
                continue
            if (r.get("side") or "") != close_side:
                continue
            try:
                c_ms = int(r.get("createdTime") or 0)
            except (TypeError, ValueError):
                c_ms = 0
            if c_ms < entry_ms_for_orphan:
                continue
            orphans.append(r)
        if orphans:
            exch_records = list(exch_records) + orphans
            if exit_price is None:
                tot_qty = sum(abs(float(r.get("closedSize") or 0)) for r in orphans)
                tot_val = sum(abs(float(r.get("cumExitValue") or 0)) for r in orphans)
                if tot_qty > 0:
                    exit_price = tot_val / tot_qty
            log.info(
                f"수동 종료 orphan 감지 | orphans={len(orphans)} "
                f"oids={[str(r.get('orderId', ''))[:8] for r in orphans]}"
            )
    # 로컬 추정치 (TP1/TP2 이미 체결된 legs 포함) — exit_price가 orphan으로 보강된 뒤 계산
    local_gross = estimate_realized_pnl(pos, exit_price, exit_size) if exit_price is not None else None
    local_fees = estimate_total_fees(pos, close_fee or 0.0)
    local_realized = (local_gross - local_fees) if local_gross is not None else None
    exch_pnl_raw = sum(float(r.get("closedPnl") or 0) for r in exch_records) if exch_records else None
    exch_fee_raw = sum(
        (abs(float(r.get("cumEntryValue") or 0)) + abs(float(r.get("cumExitValue") or 0))) * ENTRY_TAKER_FEE_RATE
        for r in exch_records
    ) if exch_records else None
    expected_legs = (1 if pos.tp1_hit else 0) + (1 if pos.tp2_hit else 0) + 1
    exch_leg_count = len(exch_records)
    exch_total_count = len(exch_records_all or [])
    # 화이트리스트로 이미 현재 trade 레코드만 남았으므로 >= expected_legs 충족 시 거래소 경로 사용
    # (Bybit가 하나의 orderId에 대해 복수 close-pnl row를 반환하는 엣지 케이스를 허용)
    if exch_pnl_raw is not None and exch_leg_count >= expected_legs:
        realized_pnl = exch_pnl_raw
        fees_total = exch_fee_raw or 0.0
        gross_pnl = realized_pnl + fees_total
        log.info(f"거래소 확정 PnL 사용 | matched={exch_leg_count} expected={expected_legs} total={exch_total_count} realized={realized_pnl:.2f}")
    elif local_realized is not None:
        gross_pnl = local_gross
        fees_total = local_fees
        realized_pnl = local_realized
        log.info(f"로컬 추정 PnL 사용 (TP legs 포함) | matched={exch_leg_count} expected={expected_legs} total={exch_total_count} realized={realized_pnl:.2f}")
    else:
        gross_pnl = calc_leg_pnl(pos.side, pos.entry_price, exit_price, exit_size or pos.size_remaining) if exit_price is not None else None
        fees_total = abs(pos.entry_fee or 0.0) + abs(close_fee or 0.0)
        realized_pnl = (gross_pnl - fees_total) if gross_pnl is not None else None
    with open_dashboard_db() as conn:
        dashboard_upsert_trade(
            pos,
            status="closed",
            exit_price=exit_price,
            exit_time=event_time,
            gross_pnl=gross_pnl,
            fees_total=fees_total,
            fees_currency=fee_currency or pos.fees_currency,
            realized_pnl=realized_pnl,
            close_reason=close_reason,
            notes=note,
            conn=conn,
        )
        dashboard_append_event(
            pos,
            close_reason,
            event_time=event_time,
            price=exit_price,
            size=round_qty(exit_size if exit_size is not None else pos.size_remaining),
            pnl_delta=realized_pnl,
            fee_amount=abs(close_fee or 0.0),
            fee_currency=fee_currency or pos.fees_currency,
            note=note,
            order_id=order_id,
            event_key=f"{pos.trade_id}:{close_reason}",
            conn=conn,
        )
        conn.commit()

def ensure_position_trade_id(pos: "Position", prefix: str = "TRADE") -> "Position":
    if pos.trade_id:
        return pos
    seed = pos.entry_order_id or make_order_link_id(prefix)
    pos.trade_id = str(seed)
    return pos

# =========================
# 공통 유틸
# =========================
def make_order_link_id(prefix: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_-]", "", prefix or "")
    uniq = uuid.uuid4().hex[:12]
    base = f"{safe}_{uniq}" if safe else uniq
    return base[:36]

# =========================
# 데이터 수집
# =========================
def months_to_ms(m: float) -> int:
    return int(m * 30.44 * 24 * 3600 * 1000)

def is_rate_limit_error(exc: Exception) -> bool:
    msg = str(exc)
    markers = (
        "Too many visits",
        "Exceeded the API Rate Limit",
        "retCode:10006",
        '"retCode":10006',
    )
    return any(marker in msg for marker in markers)

def fetch_ohlcv_page(symbol: str, tf: str, since_ms: int, limit: int, client=None):
    client = exchange if client is None else client
    for attempt in range(4):
        try:
            return client.fetch_ohlcv(symbol, tf, since=since_ms, limit=limit)
        except Exception as e:
            if not is_rate_limit_error(e) or attempt == 3:
                raise
            wait_sec = 5 * (attempt + 1)
            log.warning(
                f"OHLCV rate limit | tf={tf} since={since_ms} limit={limit} "
                f"-> {wait_sec}초 후 재시도"
            )
            time.sleep(wait_sec)

def timeframe_to_ms(tf: str) -> int:
    return int(exchange.parse_timeframe(tf) * 1000)

def trim_ohlcv_window(df: pd.DataFrame, since_ms: int) -> pd.DataFrame:
    if df.empty:
        return df
    trimmed = df[df["ts"] >= since_ms].copy()
    return trimmed.drop_duplicates("timestamp", keep="last").sort_values("timestamp").reset_index(drop=True)

def fetch_ohlcv_all(symbol: str, tf: str, since_ms: int, limit: int = FETCH_LIMIT, client=None) -> pd.DataFrame:
    original_since_ms = since_ms
    all_data = []
    prev_last_ts = None

    for page in range(1, MAX_FETCH_PAGES + 1):
        data = (fetch_ohlcv_page(symbol, tf, since_ms, limit) if client is None else
                fetch_ohlcv_page(symbol, tf, since_ms, limit, client=client))
        if not data:
            break
        all_data.extend(data)

        last_ts = int(data[-1][0])
        next_since_ms = last_ts + 1
        if prev_last_ts is not None and last_ts <= prev_last_ts:
            log.warning(
                f"OHLCV pagination stalled | tf={tf} page={page} "
                f"since={since_ms} last_ts={last_ts}"
            )
            break

        prev_last_ts = last_ts
        since_ms = next_since_ms
        time.sleep(FETCH_PAUSE_SEC)
    else:
        log.warning(
            f"OHLCV max pages reached | tf={tf} pages={MAX_FETCH_PAGES} "
            f"since={original_since_ms}"
        )

    if not all_data:
        return pd.DataFrame(columns=["ts", "open", "high", "low", "close", "volume", "timestamp"])

    df = pd.DataFrame(all_data, columns=["ts", "open", "high", "low", "close", "volume"])
    df["timestamp"] = pd.to_datetime(df["ts"], unit="ms", utc=True)
    return trim_ohlcv_window(df, original_since_ms)

def fetch_ohlcv_incremental(symbol: str, tf: str, since_ms: int, *, client=None, cache=None) -> pd.DataFrame:
    cache = DATA_CACHE if cache is None else cache
    cached = cache.get(tf)
    kwargs = {} if client is None else {"client": client}

    if cached is None or cached.empty:
        df = fetch_ohlcv_all(symbol, tf, since_ms, **kwargs)
        cache[tf] = df
        return df

    delta = timeframe_to_ms(tf) if client is None else int(client.parse_timeframe(tf) * 1000)
    refetch_since = max(since_ms, int(cached["ts"].iloc[-1]) - delta * RECENT_REFETCH_BARS)
    recent_df = fetch_ohlcv_all(symbol, tf, refetch_since, **kwargs)
    merged = pd.concat([cached, recent_df], ignore_index=True)
    merged = trim_ohlcv_window(merged, since_ms)
    cache[tf] = merged
    return merged

def create_market_client():
    client = ccxt.bybit({"enableRateLimit": True, "timeout": 10000,
                         "options": {"defaultType": "linear"}})
    if TESTNET:
        client.set_sandbox_mode(True)
    return client


def load_data(*, client=None, cache=None):
    now_ms = (exchange if client is None else client).milliseconds()
    kwargs = {} if client is None and cache is None else {"client": client, "cache": cache}
    df_15m = fetch_ohlcv_incremental(SYMBOL, ENTRY_TF, now_ms - months_to_ms(MONTHS_BACK), **kwargs)
    df_1h = fetch_ohlcv_incremental(SYMBOL, "1h", now_ms - months_to_ms(MONTHS_BACK), **kwargs)
    df_4h = fetch_ohlcv_incremental(SYMBOL, HTF_1, now_ms - months_to_ms(MONTHS_BACK + 3), **kwargs)
    df_1d = fetch_ohlcv_incremental(SYMBOL, HTF_2, now_ms - months_to_ms(MONTHS_BACK + 12), **kwargs)
    log.info(
        f"데이터 로드 | 15m:{len(df_15m)} 1h:{len(df_1h)} 4h:{len(df_4h)} 1d:{len(df_1d)} "
        f"(incremental)"
    )
    return df_15m, df_1h, df_4h, df_1d

# =========================
# 지표
# =========================
def _wilder_rma(s: pd.Series, period: int) -> pd.Series:
    """TradingView ta.rma(): seed with first SMA, then Wilder recursion."""
    values = s.values.astype(float)
    out = np.full_like(values, np.nan, dtype=float)
    first_valid = 0
    while first_valid < len(values) and np.isnan(values[first_valid]):
        first_valid += 1
    seed_pos = first_valid + period - 1
    if seed_pos >= len(values):
        return pd.Series(out, index=s.index)
    out[seed_pos] = np.nanmean(values[first_valid:seed_pos + 1])
    alpha = 1.0 / period
    prev = out[seed_pos]
    for i in range(seed_pos + 1, len(values)):
        value = values[i]
        if np.isnan(value):
            out[i] = prev
            continue
        prev = prev * (1 - alpha) + value * alpha
        out[i] = prev
    return pd.Series(out, index=s.index)

def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    d = series.diff()
    gain = _wilder_rma(d.clip(lower=0), period)
    loss = _wilder_rma(-d.clip(upper=0), period)
    with np.errstate(divide="ignore", invalid="ignore"):
        rs = gain / loss
    return 100 - 100 / (1 + rs)

def add_ichimoku(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    h9 = df["high"].rolling(9).max()
    l9 = df["low"].rolling(9).min()
    h26 = df["high"].rolling(26).max()
    l26 = df["low"].rolling(26).min()
    h52 = df["high"].rolling(52).max()
    l52 = df["low"].rolling(52).min()

    tenkan = (h9 + l9) / 2
    kijun = (h26 + l26) / 2
    sA = ((tenkan + kijun) / 2).shift(26)
    sB = ((h52 + l52) / 2).shift(26)

    df["cloud_span_a"] = sA
    df["cloud_span_b"] = sB
    df["cloud_top"] = pd.concat([sA, sB], axis=1).max(axis=1)
    df["cloud_bottom"] = pd.concat([sA, sB], axis=1).min(axis=1)
    return df

def add_atr(df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
    df = df.copy()
    hl = df["high"] - df["low"]
    hc = (df["high"] - df["close"].shift()).abs()
    lc = (df["low"] - df["close"].shift()).abs()
    df["atr"] = pd.concat([hl, hc, lc], axis=1).max(axis=1).rolling(period).mean()
    return df

def extract_order_blocks(df: pd.DataFrame) -> List[Dict]:
    """E026: 직전 봉의 몸통(open~close)을 현재 봉의 몸통이 완전히 덮을 때만 OB 형성.

    - Bull OB: 직전=음봉, 현재=양봉, 현재.open ≤ 직전.close AND 현재.close ≥ 직전.open
    - Bear OB: 직전=양봉, 현재=음봉, 현재.open ≥ 직전.close AND 현재.close ≤ 직전.open
    - OB 영역 = 직전 봉 몸통 구간 [min(open,close), max(open,close)]
    - OB_SEARCH_BACK / IMPULSE_MULT 미사용 (engulfing 자체가 유의미성 필터)
    """
    obs = []

    for i in range(1, len(df)):
        prev = df.iloc[i - 1]
        cur = df.iloc[i]
        po, pc = float(prev["open"]), float(prev["close"])
        co, cc = float(cur["open"]), float(cur["close"])

        prev_bear = pc < po
        prev_bull = pc > po
        cur_bull = cc > co
        cur_bear = cc < co

        # Bull engulfing: 직전 음봉 몸통을 현재 양봉 몸통이 완전히 덮음
        if prev_bear and cur_bull and co <= pc and cc >= po:
            obs.append({
                "side": "bull",
                "low": pc,                         # 직전 음봉 close (= 몸통 하단)
                "high": po,                        # 직전 음봉 open (= 몸통 상단)
                "anchor_idx": i - 1,
                "impulse_idx": i,
                "anchor_time": prev["timestamp"],
                "anchor_open": po,
                "anchor_high": float(prev["high"]),
                "anchor_low": float(prev["low"]),
                "anchor_close": pc,
                "impulse_time": cur["timestamp"],
            })

        # Bear engulfing: 직전 양봉 몸통을 현재 음봉 몸통이 완전히 덮음
        if prev_bull and cur_bear and co >= pc and cc <= po:
            obs.append({
                "side": "bear",
                "low": po,                         # 직전 양봉 open (= 몸통 하단)
                "high": pc,                        # 직전 양봉 close (= 몸통 상단)
                "anchor_idx": i - 1,
                "impulse_idx": i,
                "anchor_time": prev["timestamp"],
                "anchor_open": po,
                "anchor_high": float(prev["high"]),
                "anchor_low": float(prev["low"]),
                "anchor_close": pc,
                "impulse_time": cur["timestamp"],
            })

    return obs

def pivot_points(series: list, order: int, mode: str) -> List[bool]:
    n = len(series)
    result = [False] * n
    for i in range(order, n - order):
        w = series[i - order:i + order + 1]
        if mode == "low" and series[i] == min(w):
            result[i] = True
        if mode == "high" and series[i] == max(w):
            result[i] = True
    return result

# 연구/검증용 확정 다이버전스.
# 라이브 진입 트리거에는 쓰지 않고, 비교 실험/검증용 참조로만 유지한다.
def add_divergence(df: pd.DataFrame, order: int = 3) -> pd.DataFrame:
    df = df.copy()
    df["rsi"] = rsi(df["close"], 14)

    lows_ = df["low"].tolist()
    highs_ = df["high"].tolist()
    rsi_v = df["rsi"].tolist()

    piv_lo = pivot_points(lows_, order, "low")
    piv_hi = pivot_points(highs_, order, "high")

    bull = [False] * len(df)
    bear = [False] * len(df)

    prev_li = None
    prev_hi = None

    for i in range(len(df)):
        if piv_lo[i] and not pd.isna(rsi_v[i]):
            if prev_li is not None and lows_[i] < lows_[prev_li] and rsi_v[i] > rsi_v[prev_li]:
                bull[i] = True
            prev_li = i

        if piv_hi[i] and not pd.isna(rsi_v[i]):
            if prev_hi is not None and highs_[i] > highs_[prev_hi] and rsi_v[i] < rsi_v[prev_hi]:
                bear[i] = True
            prev_hi = i

    df["bullish_div"] = bull
    df["bearish_div"] = bear
    return df

def add_early_divergence_candidates(
    df: pd.DataFrame,
    min_separation: int = EARLY_DIV_MIN_SEPARATION,
    max_separation: int = EARLY_DIV_MAX_SEPARATION,
    swing_lookback: int = EARLY_SWING_LOOKBACK,
    swing_reset_atr_mult: float = EARLY_SWING_RESET_ATR_MULT,
    swing_reset_rsi: float = EARLY_SWING_RESET_RSI,
) -> pd.DataFrame:
    """
    Live-usable pseudo swings use only trailing data.

    A pseudo swing low is recorded when the current bar prints the lowest low of
    the trailing `swing_lookback` bars. A pseudo swing high is the symmetric
    highest high of the same trailing window. This is intentionally early: it
    does not wait for future-bar confirmation.

    The "most recent meaningful prior pseudo swing" is the latest same-type
    pseudo swing inside the loose separation bounds where price or RSI moved away
    from that earlier extreme before the current one. That prevents comparing
    every adjacent grind-down/grind-up bar as a new divergence pair.
    """
    df = df.copy()
    if "rsi" not in df.columns:
        df["rsi"] = rsi(df["close"], 14)
    if "atr" not in df.columns:
        df = add_atr(df, ATR_LEN)

    lows = df["low"].tolist()
    highs = df["high"].tolist()
    rsi_v = df["rsi"].tolist()
    atr_v = df["atr"].tolist()

    pseudo_low = [False] * len(df)
    pseudo_high = [False] * len(df)
    bull = [False] * len(df)
    bear = [False] * len(df)
    bull_ref_idx: List[Optional[int]] = [None] * len(df)
    bear_ref_idx: List[Optional[int]] = [None] * len(df)

    low_candidates: List[int] = []
    high_candidates: List[int] = []

    def segment_rsi(values: List[float]) -> List[float]:
        return [v for v in values if not pd.isna(v)]

    def moved_away_from_low(prior_idx: int, cur_idx: int) -> bool:
        if cur_idx <= prior_idx + 1:
            return False
        price_bounce = max(highs[prior_idx + 1:cur_idx + 1]) - lows[prior_idx]
        atr_ref = atr_v[prior_idx]
        if pd.isna(atr_ref):
            atr_ref = atr_v[cur_idx]
        if pd.isna(atr_ref):
            atr_ref = 0.0

        rebound_price = price_bounce >= atr_ref * swing_reset_atr_mult
        rsi_segment = segment_rsi(rsi_v[prior_idx + 1:cur_idx + 1])
        rebound_rsi = (
            (not pd.isna(rsi_v[prior_idx]))
            and bool(rsi_segment)
            and (max(rsi_segment) - rsi_v[prior_idx] >= swing_reset_rsi)
        )
        return rebound_price or rebound_rsi

    def moved_away_from_high(prior_idx: int, cur_idx: int) -> bool:
        if cur_idx <= prior_idx + 1:
            return False
        price_pullback = highs[prior_idx] - min(lows[prior_idx + 1:cur_idx + 1])
        atr_ref = atr_v[prior_idx]
        if pd.isna(atr_ref):
            atr_ref = atr_v[cur_idx]
        if pd.isna(atr_ref):
            atr_ref = 0.0

        pullback_price = price_pullback >= atr_ref * swing_reset_atr_mult
        rsi_segment = segment_rsi(rsi_v[prior_idx + 1:cur_idx + 1])
        pullback_rsi = (
            (not pd.isna(rsi_v[prior_idx]))
            and bool(rsi_segment)
            and (rsi_v[prior_idx] - min(rsi_segment) >= swing_reset_rsi)
        )
        return pullback_price or pullback_rsi

    for i in range(len(df)):
        if i < swing_lookback - 1:
            continue

        win_start = max(0, i - swing_lookback + 1)
        cur_low = float(lows[i])
        cur_high = float(highs[i])

        if cur_low <= min(lows[win_start:i + 1]):
            pseudo_low[i] = True
            for j in reversed(low_candidates):
                dist = i - j
                if dist < min_separation:
                    continue
                if dist > max_separation:
                    break
                if not moved_away_from_low(j, i):
                    continue
                # 중간 저점 체크: prior~current 사이에 현재보다 더 낮은 저점이 있으면 무효
                intermediate_lows = lows[j + 1:i]
                if intermediate_lows and min(intermediate_lows) < cur_low:
                    continue
                if (
                    (not pd.isna(rsi_v[i]))
                    and (not pd.isna(rsi_v[j]))
                    and cur_low < float(lows[j])
                    and float(rsi_v[i]) > float(rsi_v[j])
                ):
                    bull[i] = True
                    bull_ref_idx[i] = j
                    break

            low_candidates.append(i)

        if cur_high >= max(highs[win_start:i + 1]):
            pseudo_high[i] = True
            for j in reversed(high_candidates):
                dist = i - j
                if dist < min_separation:
                    continue
                if dist > max_separation:
                    break
                if not moved_away_from_high(j, i):
                    continue
                # 중간 고점 체크: prior~current 사이에 현재보다 더 높은 고점이 있으면 무효
                intermediate_highs = highs[j + 1:i]
                if intermediate_highs and max(intermediate_highs) > cur_high:
                    continue
                if (
                    (not pd.isna(rsi_v[i]))
                    and (not pd.isna(rsi_v[j]))
                    and cur_high > float(highs[j])
                    and float(rsi_v[i]) < float(rsi_v[j])
                ):
                    bear[i] = True
                    bear_ref_idx[i] = j
                    break

            high_candidates.append(i)

    df["pseudo_swing_low"] = pseudo_low
    df["pseudo_swing_high"] = pseudo_high
    df["early_bullish_div_candidate"] = bull
    df["early_bearish_div_candidate"] = bear
    df["early_bull_ref_idx"] = bull_ref_idx
    df["early_bear_ref_idx"] = bear_ref_idx
    return df

def prepare_live_signal_frame(df_15m: pd.DataFrame) -> pd.DataFrame:
    df_live = add_atr(df_15m, ATR_LEN)
    if len(df_live) > EARLY_DIV_WARMUP:
        df_live = df_live.iloc[-EARLY_DIV_WARMUP:].reset_index(drop=True)
    return add_early_divergence_candidates(
        df_live,
        min_separation=EARLY_DIV_MIN_SEPARATION,
        max_separation=EARLY_DIV_MAX_SEPARATION,
        swing_lookback=EARLY_SWING_LOOKBACK,
        swing_reset_atr_mult=EARLY_SWING_RESET_ATR_MULT,
        swing_reset_rsi=EARLY_SWING_RESET_RSI,
    )


EARLY_DIVERGENCE_SIGNAL_COLUMNS = {
    "early_bull_div_candidate": (
        "early_bullish_div_candidate",
        "early_bull_ref_idx",
    ),
    "early_bear_div_candidate": (
        "early_bearish_div_candidate",
        "early_bear_ref_idx",
    ),
}


class EntrySignal(tuple):
    """Five-field legacy signal tuple with attached live entry-zone context."""

    def __new__(
        cls,
        side: str,
        bucket: str,
        details: str,
        reason: str,
        atr: float,
        entry_zones: Sequence[Dict] = (),
    ):
        instance = super().__new__(cls, (side, bucket, details, reason, atr))
        instance.entry_zones = tuple(
            {
                "zone_id": str(zone.get("zone_id") or zone.get("name") or ""),
                "source_tf": str(zone.get("source_tf") or zone.get("tf") or ""),
                "side": str(zone.get("side") or ""),
                "low": min(float(zone["low"]), float(zone["high"])),
                "high": max(float(zone["low"]), float(zone["high"])),
                "bullish_reversal": bool(zone.get("bullish_reversal", False)),
                "lower_wick_range_ratio": float(
                    zone.get("lower_wick_range_ratio", 0.0)
                ),
            }
            for zone in entry_zones
        )
        return instance


def to_zone_role_entry_zones(
    entry_zones: Sequence[Dict],
) -> Tuple[ZoneRoleEntryZone, ...]:
    converted: List[ZoneRoleEntryZone] = []
    for zone in entry_zones:
        zone_id = str(zone.get("zone_id") or "")
        source_tf = str(zone.get("source_tf") or zone.get("tf") or "")
        side = str(zone.get("side") or "")
        if (
            not zone_id
            or source_tf not in {"1h", "4h", "1d"}
            or side not in {"support", "resistance"}
        ):
            raise ValueError(f"entry zone context is incomplete: {zone}")
        converted.append(ZoneRoleEntryZone(
            zone_id=zone_id,
            source_tf=source_tf,
            side=side,
            low=min(float(zone["low"]), float(zone["high"])),
            high=max(float(zone["low"]), float(zone["high"])),
            bullish_reversal=bool(zone.get("bullish_reversal", False)),
            lower_wick_range_ratio=float(
                zone.get("lower_wick_range_ratio", 0.0)
            ),
        ))
    return tuple(converted)


def normalize_zone_role_policy_runtime(
    started_at: Optional[str] = None,
    *,
    log_reset: bool = True,
    allow_missing: bool = True,
    raise_on_unsafe: bool = True,
) -> Dict[str, object]:
    """Keep policy migration isolated from the persisted Position payload."""
    fallback_started_at = started_at or iso_utc_now()
    unsafe_reason = str(BOT1_RUNTIME.get("zone_role_policy_error") or "").strip()
    payload = BOT1_RUNTIME.get("zone_role_policy")
    if unsafe_reason:
        normalized, _ = normalize_policy_state(payload, fallback_started_at)
        BOT1_RUNTIME["zone_role_policy"] = normalized
        if raise_on_unsafe:
            raise ValueError(f"zone-role policy state is unsafe: {unsafe_reason}")
        return normalized
    normalized, reset = normalize_policy_state(payload, fallback_started_at)
    BOT1_RUNTIME["zone_role_policy"] = normalized
    unsafe_reset = reset and (payload is not None or not allow_missing)
    if unsafe_reset:
        unsafe_reason = "missing_current_policy" if payload is None else "malformed_current_policy"
        BOT1_RUNTIME["zone_role_policy_error"] = unsafe_reason
        log.error(f"zone-role policy fail-closed | reason={unsafe_reason}")
        if raise_on_unsafe:
            raise ValueError(f"zone-role policy state is unsafe: {unsafe_reason}")
    elif reset:
        BOT1_RUNTIME["zone_role_policy_error"] = ""
    if reset and log_reset and not unsafe_reset:
        log.warning(
            "zone-role policy state initialized | "
            f"started_at={normalized['started_at']} quota=0"
        )
    return normalized


def mark_zone_role_policy_unsafe(reason: str) -> None:
    BOT1_RUNTIME["zone_role_policy_error"] = str(reason or "unknown_policy_state_error")
    log.error(f"zone-role policy fail-closed | reason={BOT1_RUNTIME['zone_role_policy_error']}")


def validate_zone_role_decision(decision: AttenuationDecision) -> AttenuationDecision:
    if not isinstance(decision, AttenuationDecision):
        raise ValueError("zone-role decision has invalid type")
    if not math.isfinite(decision.multiplier) or not 0.0 <= decision.multiplier <= 1.0:
        raise ValueError(f"zone-role multiplier is invalid: {decision.multiplier}")
    if decision.multiplier < 1.0 and not decision.blocked:
        if not decision.zone_id or not decision.timeframe or decision.role_epoch <= 0 or not decision.quota_key:
            raise ValueError("attenuated zone-role decision is incomplete")
    if decision.blocked and not decision.block_reason:
        raise ValueError("blocked zone-role decision is missing a reason")
    if decision.reversal_started:
        if decision.multiplier != 1.0 or not decision.entry_zone_id:
            raise ValueError("bullish reversal decision is incomplete")
    return decision


def resolve_zone_role_entry_decision(
    side: str,
    signal_close_time,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    entry_zones: Sequence[Dict] = (),
) -> AttenuationDecision:
    state = normalize_zone_role_policy_runtime()
    quota = state.get("quota")
    if not isinstance(quota, dict):
        raise ValueError("zone-role quota state is malformed")
    zones, event_map = reconstruct_zone_role_context_cached(
        df_1h,
        df_4h,
        df_1d,
        signal_close_time,
    )
    return validate_zone_role_decision(decide_attenuation(
        side,
        signal_close_time,
        zones,
        event_map,
        ZONE_ROLE_POLICY_SETTING,
        quota,
        entry_zones=to_zone_role_entry_zones(entry_zones),
        trend_reset=state.get("trend_reset"),
    ))


def reconstruct_zone_role_context_cached(
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    visible_at,
):
    target = pd.Timestamp(visible_at)
    if target.tzinfo is None:
        target = target.tz_localize("UTC")
    else:
        target = target.tz_convert("UTC")
    def closed_signature(frame: pd.DataFrame, timeframe: str):
        timestamps = pd.to_datetime(frame["timestamp"], utc=True)
        if "close_time" in frame.columns:
            close_times = pd.to_datetime(frame["close_time"], utc=True)
        else:
            close_times = timestamps + pd.to_timedelta(
                timeframe_to_ms(timeframe), unit="ms"
            )
        visible = frame.loc[close_times <= target]
        if visible.empty:
            return (0, None)
        row = visible.iloc[-1]
        return (
            len(visible),
            int(close_times.loc[visible.index[-1]].value),
            *(float(row[column]) for column in ("open", "high", "low", "close")),
        )

    cache_key = (
        closed_signature(df_1h, "1h"),
        closed_signature(df_4h, "4h"),
        closed_signature(df_1d, "1d"),
    )
    if ZONE_ROLE_CONTEXT_CACHE.get("key") == cache_key:
        cached = ZONE_ROLE_CONTEXT_CACHE.get("context")
        if isinstance(cached, tuple) and len(cached) == 2:
            return cached
    context = reconstruct_zone_role_context(
        df_1h,
        df_4h,
        df_1d,
        target,
        extract_order_blocks,
        add_atr,
        ATR_LEN,
        ZONE_ROLE_BREAKOUT_RULE,
        ZONE_ROLE_POLICY_SETTING,
    )
    ZONE_ROLE_CONTEXT_CACHE["key"] = cache_key
    ZONE_ROLE_CONTEXT_CACHE["context"] = context
    return context


def zone_role_target_entry_size(decision: AttenuationDecision) -> float:
    decision = validate_zone_role_decision(decision)
    if decision.blocked:
        return 0.0
    target_entry_size = round_qty(POSITION_SIZE_BTC * decision.multiplier)
    if target_entry_size <= 0:
        raise ValueError(f"zone-role target size rounded to zero: {target_entry_size}")
    derive_position_split(target_entry_size)
    return target_entry_size


def resolve_target_entry_size_for_signal(
    reason: str,
    side: str,
    signal_close_time,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    entry_zones: Sequence[Dict] = (),
) -> Tuple[float, Optional[AttenuationDecision]]:
    if reason not in EARLY_DIVERGENCE_SIGNAL_COLUMNS:
        return POSITION_SIZE_BTC, None
    decision = resolve_zone_role_entry_decision(
        side,
        signal_close_time,
        df_1h,
        df_4h,
        df_1d,
        entry_zones,
    )
    return zone_role_target_entry_size(decision), decision


def zone_role_decision_context(
    decision: Optional[AttenuationDecision],
    target_entry_size: float,
) -> str:
    if decision is None:
        return (
            f"multiplier=1.0 timeframe=none epoch=0 "
            f"base_size={POSITION_SIZE_BTC} target_size={target_entry_size}"
        )
    return (
        f"multiplier={decision.multiplier} timeframe={decision.timeframe or 'none'} "
        f"epoch={decision.role_epoch} strength_atr={decision.strength:.4f} "
        f"base_size={POSITION_SIZE_BTC} "
        f"target_size={target_entry_size} policy_reason={decision.block_reason} "
        f"reversal_started={decision.reversal_started} "
        f"entry_zone={decision.entry_zone_id or 'none'}"
    )


def persist_zone_role_reservation(
    decision: AttenuationDecision,
    signal_id: str,
    when,
) -> None:
    if not decision.quota_key:
        return
    state = normalize_zone_role_policy_runtime()
    reserve_zone_role_quota(state, decision.quota_key, signal_id, when)
    consume_signal_id(signal_id)
    save_state(None)


def persist_bullish_reversal_reset(
    decision: AttenuationDecision,
    entry_zones: Sequence[Dict],
    when,
) -> None:
    if not decision.reversal_started:
        return
    converted = to_zone_role_entry_zones(entry_zones)
    entry_zone = next(
        (zone for zone in converted if zone.zone_id == decision.entry_zone_id),
        None,
    )
    if entry_zone is None:
        raise ValueError("bullish reversal entry zone is missing from the signal")
    state = normalize_zone_role_policy_runtime()
    record_bullish_trend_reset(state, decision, entry_zone, when)
    save_state(None)


def persist_zone_role_blocked_signal(signal_id: str) -> None:
    if not str(BOT1_RUNTIME.get("zone_role_policy_error") or "").strip():
        normalize_zone_role_policy_runtime()
    consume_signal_id(signal_id)
    save_state(None)


def persist_zone_role_consumption(
    decision: AttenuationDecision,
    signal_id: str,
    when,
) -> None:
    if not decision.quota_key:
        return
    state = normalize_zone_role_policy_runtime()
    consume_zone_role_quota(state, decision.quota_key, signal_id, when)
    save_state(None)


def build_early_divergence_signal_id_from_frame(
    df_live: pd.DataFrame,
    signal_idx: int,
    reason_key: str,
) -> str:
    """Identify one divergence episode by direction and its reference pivot."""
    columns = EARLY_DIVERGENCE_SIGNAL_COLUMNS.get(reason_key)
    if columns is None or signal_idx < 0 or signal_idx >= len(df_live):
        return ""

    candidate_column, reference_column = columns
    if candidate_column not in df_live.columns or reference_column not in df_live.columns:
        return ""

    row = df_live.iloc[signal_idx]
    if not bool(row[candidate_column]) or pd.isna(row[reference_column]):
        return ""

    reference_idx = int(row[reference_column])
    if reference_idx < 0 or reference_idx >= len(df_live):
        return ""

    reference_ts = pd.Timestamp(df_live.iloc[reference_idx]["timestamp"])
    if reference_ts.tzinfo is None:
        reference_ts = reference_ts.tz_localize("UTC")
    else:
        reference_ts = reference_ts.tz_convert("UTC")
    return f"{reason_key}:{reference_ts.strftime('%Y-%m-%dT%H:%M:%SZ')}"


def build_early_divergence_signal_id(
    df_15m: pd.DataFrame,
    signal_ts,
    reason_key: str,
) -> str:
    if reason_key not in EARLY_DIVERGENCE_SIGNAL_COLUMNS:
        return ""
    df_live = prepare_live_signal_frame(df_15m)
    timestamps = pd.to_datetime(df_live["timestamp"], utc=True)
    target = pd.Timestamp(signal_ts)
    if target.tzinfo is None:
        target = target.tz_localize("UTC")
    else:
        target = target.tz_convert("UTC")
    matches = df_live.index[timestamps == target]
    if len(matches) == 0:
        return ""
    return build_early_divergence_signal_id_from_frame(
        df_live,
        int(matches[-1]),
        reason_key,
    )

def detect_signal_from_frame(
    df_live: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    signal_idx: Optional[int] = None,
) -> Optional[Tuple[str, str, str, str, float]]:
    if len(df_live) < max(DIVERGENCE_PIVOT * 2 + 10, EARLY_SWING_LOOKBACK + 2):
        return None

    source_df_4h = df_4h
    source_df_1d = df_1d
    if any(col not in df_4h.columns for col in ["cloud_top", "cloud_bottom", "cloud_span_a", "cloud_span_b"]):
        df_4h = add_ichimoku(df_4h)
    if any(col not in df_1d.columns for col in ["cloud_top", "cloud_bottom", "cloud_span_a", "cloud_span_b"]):
        df_1d = add_ichimoku(df_1d)

    ci = len(df_live) - 2 if signal_idx is None else signal_idx
    if ci < 0 or ci >= len(df_live):
        return None

    row = df_live.iloc[ci]
    ts = row["timestamp"]
    price = float(row["close"])
    atr_v = float(row["atr"]) if not pd.isna(row.get("atr", np.nan)) else 200.0

    if not row["early_bullish_div_candidate"] and not row["early_bearish_div_candidate"]:
        return None

    signal_close_time = pd.Timestamp(ts) + pd.to_timedelta(
        timeframe_to_ms(ENTRY_TF), unit="ms"
    )
    zone_role_context = reconstruct_zone_role_context_cached(
        df_1h,
        source_df_4h,
        source_df_1d,
        signal_close_time,
    )
    zones = get_active_zones(
        df_live,
        df_4h,
        df_1d,
        ts,
        df_1h=df_1h,
        zone_role_context=zone_role_context,
        role_visible_at=signal_close_time,
    )

    for div_type, side_filter, entry_side, reason_key in [
        ("early_bullish_div_candidate", "support", "long", "early_bull_div_candidate"),
        ("early_bearish_div_candidate", "resistance", "short", "early_bear_div_candidate"),
    ]:
        if not row[div_type]:
            continue

        matched = zones_touched_in_lookback(df_live, df_1h, zones, ci, side_filter)
        if entry_side == "long":
            matched = exclude_long_cloud_zones(matched)
        if not matched:
            continue

        immediate = [
            zone
            for zone in matched
            if not zone.get("deep_penetration")
            or bool(zone.get("bullish_reversal"))
        ]
        if not immediate:
            signal_id = build_early_divergence_signal_id_from_frame(
                df_live,
                ci,
                reason_key,
            )
            queue_deep_penetration_setups(
                matched,
                entry_side,
                reason_key,
                atr_v,
                ts,
                signal_id=signal_id,
            )
            continue
        matched = immediate

        bucket, details = classify_and_score(matched, price, df_live, ts)
        if (bucket, reason_key) in EXCLUDED_SETUPS:
            log.info(f"⛔ 제외 셋업: {bucket} + {reason_key}")
            continue

        log.info(
            f"{'🟢' if entry_side == 'long' else '🔴'} 신호 감지\n"
            f"{SYMBOL}\n"
            f"side={entry_side}\n"
            f"bucket={bucket}\n"
            f"reason={reason_key}\n"
            f"{details}"
        )
        return EntrySignal(
            entry_side,
            bucket,
            details,
            reason_key,
            atr_v,
            entry_zones=matched,
        )

    return None


def add_close_time(df: pd.DataFrame, tf: str) -> pd.DataFrame:
    out = df.copy()
    out["close_time"] = out["timestamp"] + pd.to_timedelta(timeframe_to_ms(tf), unit="ms")
    return out


def prepare_4h_cloud_doji_frame(df_4h: pd.DataFrame) -> pd.DataFrame:
    out = add_ichimoku(df_4h)
    out = add_close_time(out, HTF_1).sort_values("timestamp").reset_index(drop=True)
    out["body_size"] = (out["close"] - out["open"]).abs()
    out["is_doji"] = out["body_size"] <= DOJI_MAX_BODY
    out["is_near_cloud_top"] = (
        (out["close"] >= out["cloud_top"] - HUNT_BUFFER)
        & (out["close"] <= out["cloud_top"] + HUNT_BUFFER)
    )
    out["is_near_cloud_bottom"] = (
        (out["close"] >= out["cloud_bottom"] - HUNT_BUFFER)
        & (out["close"] <= out["cloud_bottom"] + HUNT_BUFFER)
    )
    out["hunt_pattern"] = np.select(
        [
            out["is_near_cloud_top"] & ~out["is_near_cloud_bottom"],
            out["is_near_cloud_bottom"] & ~out["is_near_cloud_top"],
            out["is_near_cloud_top"] & out["is_near_cloud_bottom"],
        ],
        ["cloud_top_touch", "cloud_bottom_touch", "cloud_both_touch"],
        default="none",
    )
    return out


def apply_4h_cloud_doji_hunt_state(df4h: pd.DataFrame) -> pd.DataFrame:
    out = df4h.copy()
    hunt_active = []
    activation_pattern = []
    activation_cloud_top = []
    activation_cloud_bottom = []

    state_active = False
    state_pattern = "none"
    state_cloud_top = np.nan
    state_cloud_bottom = np.nan
    bars_in_state = 0

    for row in out.itertuples():
        close_price = float(row.close)
        current_pattern = str(row.hunt_pattern)

        if state_active:
            in_buffer = (
                (not pd.isna(state_cloud_bottom) and abs(close_price - float(state_cloud_bottom)) <= HUNT_BUFFER)
                or (not pd.isna(state_cloud_top) and abs(close_price - float(state_cloud_top)) <= HUNT_BUFFER)
            )
            expired = bars_in_state >= HUNT_EXPIRE_4H
            if (not in_buffer) or expired:
                state_active = False
                state_pattern = "none"
                state_cloud_top = np.nan
                state_cloud_bottom = np.nan
                bars_in_state = 0

        if (not state_active) and current_pattern != "none":
            state_active = True
            state_pattern = current_pattern
            state_cloud_top = float(row.cloud_top) if not pd.isna(row.cloud_top) else np.nan
            state_cloud_bottom = float(row.cloud_bottom) if not pd.isna(row.cloud_bottom) else np.nan
            bars_in_state = 0

        hunt_active.append(state_active)
        activation_pattern.append(state_pattern if state_active else "none")
        activation_cloud_top.append(state_cloud_top if state_active else np.nan)
        activation_cloud_bottom.append(state_cloud_bottom if state_active else np.nan)

        if state_active:
            bars_in_state += 1

    out["hunt_active_4h"] = hunt_active
    out["activation_pattern"] = activation_pattern
    out["activation_cloud_top"] = activation_cloud_top
    out["activation_cloud_bottom"] = activation_cloud_bottom

    bottom_entry_ok = out["close"] <= (out["cloud_bottom"] + DOJI_CLOUD_UPPER_BUFFER)
    top_entry_ok = out["close"] <= (out["cloud_top"] + DOJI_CLOUD_UPPER_BUFFER)
    out["entry_trigger_4h"] = (
        out["hunt_active_4h"]
        & out["is_doji"]
        & np.select(
            [
                out["activation_pattern"] == "cloud_bottom_touch",
                out["activation_pattern"] == "cloud_top_touch",
                out["activation_pattern"] == "cloud_both_touch",
            ],
            [
                bottom_entry_ok,
                top_entry_ok,
                bottom_entry_ok | top_entry_ok,
            ],
            default=False,
        )
    )
    return out


def detect_4h_cloud_doji_signal(
    df_15m: pd.DataFrame,
    df_4h: pd.DataFrame,
    signal_idx: Optional[int] = None,
) -> Optional[Tuple[str, str, str, str, float]]:
    global STRATEGY9_COOLDOWN_UNTIL

    if len(df_15m) < 2 or len(df_4h) < 60:
        return None

    target_idx = len(df_15m) - 2 if signal_idx is None else signal_idx
    if target_idx is None:
        return None
    latest_15m = df_15m.iloc[target_idx]
    signal_close_time = pd.Timestamp(latest_15m["timestamp"]) + pd.to_timedelta(timeframe_to_ms(ENTRY_TF), unit="ms")
    atr_v = float(latest_15m["atr"]) if not pd.isna(latest_15m.get("atr", np.nan)) else 200.0

    if STRATEGY9_COOLDOWN_UNTIL is not None and signal_close_time <= STRATEGY9_COOLDOWN_UNTIL:
        return None

    # 4h 봉 마감 직후 첫 15분봉에서만 평가 (동일 4h봉 반복 진입 방지)
    last_confirmed_4h_close = signal_close_time.floor("4h")
    if signal_close_time != last_confirmed_4h_close:
        return None

    df4h = prepare_4h_cloud_doji_frame(df_4h)
    df4h = df4h[df4h["close_time"] <= last_confirmed_4h_close].copy().reset_index(drop=True)
    if df4h.empty:
        return None

    df4h = apply_4h_cloud_doji_hunt_state(df4h)
    row_df = df4h[df4h["close_time"] == last_confirmed_4h_close]
    if row_df.empty:
        log.debug(f"4h_cloud_doji: last_confirmed_4h_close={last_confirmed_4h_close} 봉 없음 -> 신호 미발생")
        return None

    row = row_df.iloc[-1]
    if not bool(row.get("entry_trigger_4h", False)):
        return None

    bucket = "4h_cloud_doji_short"
    reason = "4h_cloud_doji_short"
    debug_details = (
        f"pattern={row['activation_pattern']} "
        f"close={float(row['close']):.1f} "
        f"body={float(row['body_size']):.1f} "
        f"cloud_top={float(row['cloud_top']):.1f} "
        f"cloud_bottom={float(row['cloud_bottom']):.1f}"
    )
    details = (
        "4시간 구름대 + 도지 구간|"
        f"doji_candle={format_kst_candle_window(row['timestamp'], HTF_1)}"
    )

    log.info(
        f"🔴 신호 감지\n"
        f"{SYMBOL}\n"
        f"side=short\n"
        f"bucket={bucket}\n"
        f"reason={reason}\n"
        f"{debug_details}"
    )
    return ("short", bucket, details, reason, atr_v)


def ensure_strategy9_cooldown_after_exit(latest_bar_time, source: str) -> None:
    global STRATEGY9_COOLDOWN_UNTIL

    latest_close_time = pd.Timestamp(latest_bar_time) + pd.to_timedelta(
        timeframe_to_ms(ENTRY_TF), unit="ms"
    )
    if (
        STRATEGY9_COOLDOWN_UNTIL is not None
        and pd.Timestamp(STRATEGY9_COOLDOWN_UNTIL) > latest_close_time
    ):
        return
    STRATEGY9_COOLDOWN_UNTIL = latest_close_time.floor("4h") + pd.Timedelta(hours=4)
    log.info(
        f"strategy9 cooldown set | until={STRATEGY9_COOLDOWN_UNTIL.isoformat()} "
        f"source={source}"
    )

# =========================
# 존 관련
# =========================
def zone_distance(price: float, zl: float, zh: float) -> float:
    a, b = min(zl, zh), max(zl, zh)
    if a <= price <= b:
        return 0.0
    return min(abs(price - a), abs(price - b))

def calc_ob_volume(ob: Dict, df_15m: pd.DataFrame, ts, lookback: int = 96) -> float:
    sub = df_15m[df_15m["timestamp"] <= ts].tail(lookback)
    overlap = sub[(sub["low"] <= ob["high"]) & (sub["high"] >= ob["low"])]
    return float(overlap["volume"].sum())

def get_valid_obs(df_htf: pd.DataFrame, ts) -> List[Dict]:
    obs = extract_order_blocks(df_htf)
    cur_idx = len(df_htf[df_htf["timestamp"] <= ts]) - 1
    # 미확정 봉 제외: 마지막 봉은 아직 마감 전일 수 있으므로 확정 봉까지만 검증
    confirmed_idx = max(cur_idx - 1, 0)
    valid = []

    for ob in obs:
        impulse_idx = int(ob.get("impulse_idx", ob.get("bar_idx", -1)))
        if impulse_idx > confirmed_idx:
            continue
        bad = False
        for k in range(impulse_idx + 1, confirmed_idx + 1):
            c = float(df_htf.iloc[k]["close"])
            if ob["side"] == "bull" and c < ob["low"]:
                bad = True
                break
            if ob["side"] == "bear" and c > ob["high"]:
                bad = True
                break
        if not bad:
            valid.append(ob)

    return valid

def get_active_zones(
    df_15m: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    ts,
    *,
    df_1h: Optional[pd.DataFrame] = None,
    zone_role_context=None,
    role_visible_at=None,
    valid_obs_by_tf=None,
    cloud_zones_by_tf=None,
) -> List[Dict]:
    zones: List[Dict] = []

    def append_ob_zone(ob: Dict, tf: str, label: str) -> None:
        anchor_time = ob.get("anchor_time")
        zone_id = f"{tf}:ob:{ob['side']}:{format_meta_timestamp(anchor_time)}"
        zones.append({
            "zone_id": zone_id,
            "name": f"{tf}_{ob['side']}_ob",
            "tf": tf,
            "kind": "ob",
            "side": label,
            "low": float(ob["low"]),
            "high": float(ob["high"]),
            "ob_type": ob["side"],
            "source_tf": tf,
            "anchor_time": anchor_time,
            "anchor_open": ob.get("anchor_open"),
            "anchor_high": ob.get("anchor_high"),
            "anchor_low": ob.get("anchor_low"),
            "anchor_close": ob.get("anchor_close"),
            "impulse_time": ob.get("impulse_time"),
            "active_from": pd.Timestamp(ob.get("impulse_time", anchor_time)) + pd.to_timedelta(timeframe_to_ms(tf), unit="ms"),
            "entry_wave_from": ob.get("impulse_time", anchor_time),
            "reversal_only": tf == "1h",
        })

    def append_cloud_lines(df_htf: pd.DataFrame, tf: str) -> None:
        if cloud_zones_by_tf is not None:
            zones.extend(cloud_zones_by_tf[tf])
            return
        sub = df_htf[df_htf["timestamp"] <= ts].copy()
        if sub.empty:
            return
        if "cloud_span_a" not in sub.columns or "cloud_span_b" not in sub.columns:
            sub = add_ichimoku(sub)
        valid = sub.dropna(subset=["cloud_span_a", "cloud_span_b"])
        if valid.empty:
            return
        polarity_series = np.where(
            valid["cloud_span_a"] >= valid["cloud_span_b"], "bull", "bear"
        )
        current_polarity = str(polarity_series[-1])
        segment_pos = len(valid) - 1
        while segment_pos > 0 and str(polarity_series[segment_pos - 1]) == current_polarity:
            segment_pos -= 1
        segment_start = pd.Timestamp(valid.iloc[segment_pos]["timestamp"])
        current = valid.iloc[-1]
        segment = valid.iloc[segment_pos:]
        segment_key = f"{tf}:cloud:{current_polarity}:{format_meta_timestamp(segment_start)}"
        for role, side, column in [
            ("top", "support", "cloud_top"),
            ("bottom", "resistance", "cloud_bottom"),
        ]:
            value = float(current[column])
            zones.append({
                "zone_id": f"{segment_key}:{role}",
                "name": f"{tf}_cloud_{role}",
                "tf": tf,
                "kind": "cloud_line",
                "side": side,
                "low": value,
                "high": value,
                "cloud_role": role,
                "cloud_polarity": current_polarity,
                "segment_start": segment_start,
                "active_from": segment_start,
                "_line_timestamps": pd.DatetimeIndex(segment["timestamp"]),
                "_line_values": segment[column].astype(float).to_numpy(),
            })

    if df_1h is not None:
        valid_1h = valid_obs_by_tf["1h"] if valid_obs_by_tf is not None else get_valid_obs(df_1h, ts)
        for side, label in [("bull", "support"), ("bear", "resistance")]:
            for ob in [item for item in valid_1h if item["side"] == side]:
                append_ob_zone(ob, "1h", label)

    valid_4h = valid_obs_by_tf["4h"] if valid_obs_by_tf is not None else get_valid_obs(df_4h, ts)
    for side, label in [("bull", "support"), ("bear", "resistance")]:
        cands = [o for o in valid_4h if o["side"] == side]
        for ob in cands:
            append_ob_zone(ob, "4h", label)

    append_cloud_lines(df_4h, "4h")

    valid_1d = valid_obs_by_tf["1d"] if valid_obs_by_tf is not None else get_valid_obs(df_1d, ts)
    for side, label in [("bull", "support"), ("bear", "resistance")]:
        cands = [o for o in valid_1d if o["side"] == side]
        if not cands:
            continue
        for ob in cands:
            append_ob_zone(ob, "1d", label)

    if zone_role_context is not None:
        if role_visible_at is None:
            raise ValueError("role_visible_at is required with zone_role_context")
        role_zones, event_map = zone_role_context
        transitioned_keys = {
            (policy_zone.source_tf, format_meta_timestamp(policy_zone.anchor_time))
            for policy_zone in role_zones
            if policy_zone.kind == "ob"
            if zone_role_state_at(event_map.get(policy_zone.zone_id, ()), role_visible_at) is not None
        }
        zones = [
            zone
            for zone in zones
            if not (
                zone.get("kind") == "ob"
                and (zone.get("tf"), format_meta_timestamp(zone.get("anchor_time")))
                in transitioned_keys
            )
        ]
        for policy_zone, current_event, transition_event in active_flipped_zone_states(
            role_zones,
            event_map,
            role_visible_at,
        ):
            if policy_zone.kind != "ob":
                continue
            flipped_type = "bull" if current_event.effective_side == "support" else "bear"
            zones.append({
                "zone_id": policy_zone.zone_id,
                "name": f"{policy_zone.source_tf}_flipped_{flipped_type}_ob",
                "tf": policy_zone.source_tf,
                "kind": "ob",
                "side": current_event.effective_side,
                "low": policy_zone.low,
                "high": policy_zone.high,
                "ob_type": f"flipped_{flipped_type}",
                "source_tf": policy_zone.source_tf,
                "anchor_time": policy_zone.anchor_time,
                "anchor_open": policy_zone.anchor_open,
                "anchor_high": policy_zone.anchor_high,
                "anchor_low": policy_zone.anchor_low,
                "anchor_close": policy_zone.anchor_close,
                "impulse_time": policy_zone.impulse_time,
                "active_from": transition_event.event_time,
                "role_epoch": current_event.role_epoch,
                "transition_timeframe": current_event.timeframe,
                "transition_strength": current_event.strength,
                "reversal_only": policy_zone.source_tf == "1h",
            })

    append_cloud_lines(df_1d, "1d")

    return zones

def effective_side(zone: Dict, price: float) -> Optional[str]:
    if zone["side"] is not None:
        return zone["side"]

    zl, zh = min(zone["low"], zone["high"]), max(zone["low"], zone["high"])
    if price > zh:
        return "support"
    if price < zl:
        return "resistance"
    return None

def score_zone(zone: Dict, price: float, df_15m: pd.DataFrame, ts) -> float:
    tf_score = {"1d": 40, "4h": 20}.get(zone.get("tf", ""), 10)

    vol_score = 0.0
    if zone["kind"] == "ob":
        vol = calc_ob_volume(zone, df_15m, ts)
        sub = df_15m[df_15m["timestamp"] <= ts].tail(96)
        avg_vol = float(sub["volume"].mean()) * 10 if len(sub) > 0 else 1.0
        vol_score = min(30.0, 30.0 * vol / max(avg_vol, 1.0))

    zl, zh = min(zone["low"], zone["high"]), max(zone["low"], zone["high"])
    dist = zone_distance(price, zl, zh)
    dist_score = max(0.0, 30.0 * (1 - dist / HUNT_BUFFER)) if dist <= HUNT_BUFFER else 0.0

    return round(tf_score + vol_score + dist_score, 1)

def wave_range_label(z: Dict) -> str:
    wr = z.get("wave_range", 0)
    return f"wave${wr:.0f}"


def format_meta_timestamp(ts) -> str:
    if ts is None:
        return "na"
    try:
        t = pd.Timestamp(ts)
    except Exception:
        return str(ts)
    if pd.isna(t):
        return "na"
    if t.tzinfo is None:
        return t.strftime("%Y-%m-%dT%H:%M:%SZ")
    return t.tz_convert("UTC").strftime("%Y-%m-%dT%H:%M:%SZ")


def format_zone_detail(z: Dict, score: float) -> str:
    base = f"{z['name']}(score={score:.0f},wr={wave_range_label(z)})"
    if z.get("kind") != "ob":
        return base
    transition = ""
    if z.get("role_epoch"):
        transition = (
            f",role_epoch={int(z['role_epoch'])}"
            f",transition_tf={z.get('transition_timeframe', 'na')}"
            f",transition_strength={float(z.get('transition_strength', 0.0)):.2f}"
        )
    return (
        f"{base}["
        f"ob_type={z.get('ob_type', 'na')},"
        f"source_tf={z.get('source_tf', z.get('tf', 'na'))},"
        f"zone_top={max(float(z['low']), float(z['high'])):.1f},"
        f"zone_bottom={min(float(z['low']), float(z['high'])):.1f},"
        f"anchor_time={format_meta_timestamp(z.get('anchor_time'))},"
        f"anchor_open={float(z.get('anchor_open', np.nan)):.1f},"
        f"anchor_high={float(z.get('anchor_high', np.nan)):.1f},"
        f"anchor_low={float(z.get('anchor_low', np.nan)):.1f},"
        f"anchor_close={float(z.get('anchor_close', np.nan)):.1f},"
        f"impulse_time={format_meta_timestamp(z.get('impulse_time'))},"
        f"touch_time={format_meta_timestamp(z.get('touch_time'))}"
        f"{transition}"
        f"]"
    )


def zone_matches_current_buffer(zone: Dict, price: float, side_filter: str) -> bool:
    zl = min(zone["low"], zone["high"])
    zh = max(zone["low"], zone["high"])
    eff = effective_side(zone, price)

    if eff is not None and eff != side_filter:
        return False

    return abs(price - zl) <= HUNT_BUFFER or abs(price - zh) <= HUNT_BUFFER


def format_buffer_zone_alert(
    zones: List[Dict],
    price: float,
    signal_bar_time,
) -> str:
    """Format one notification for newly entered zone buffers."""
    if not zones:
        return ""

    lines = [
        "🟡 관찰 구간 진입",
        f"{SYMBOL}",
        "status=buffer_entered",
        f"price={_format_alert_price(price)}",
        f"buffer=±{_format_alert_price(HUNT_BUFFER)}",
        f"기준봉: {format_kst_candle_window(signal_bar_time, ENTRY_TF)}",
    ]
    for zone in zones:
        zl = min(float(zone["low"]), float(zone["high"]))
        zh = max(float(zone["low"]), float(zone["high"]))
        side = "지지" if zone.get("side") == "support" else "저항"
        distance = zone_distance(float(price), zl, zh)
        lines.append(
            f"존: {zone.get('name', 'zone')} ({side}) · "
            f"범위 {_format_alert_price(zl)}~{_format_alert_price(zh)} · "
            f"거리 {_format_alert_price(distance)}"
        )
        lines.append(f"위 존의 원본봉: {format_zone_origin_candle(zone)}")
    lines.append("signal=진입 조건 대기")
    return format_telegram_bullets(localize_telegram_message("\n".join(lines)))


def update_buffer_zone_alerts(
    df_15m: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    price: float,
    signal_bar_time,
    *,
    df_1h: Optional[pd.DataFrame] = None,
) -> List[Dict]:
    """Return entry-eligible zones newly reached by the confirmed close.

    Raw proximity is not enough: role transitions, touch exhaustion, direction
    exclusions, and long-cloud exclusions mirror the actual entry path before
    an alert can claim that the bot is waiting for a divergence.
    """
    previous_ids = _stored_buffer_zone_alert_ids()
    signal_time = pd.Timestamp(signal_bar_time)
    signal_close_time = signal_time + pd.to_timedelta(
        timeframe_to_ms(ENTRY_TF), unit="ms"
    )
    target = signal_time
    if target.tzinfo is None:
        target = target.tz_localize("UTC")
    else:
        target = target.tz_convert("UTC")
    source_timestamps = pd.to_datetime(df_15m["timestamp"], utc=True)
    lifecycle_frame = df_15m.loc[source_timestamps <= target].tail(
        EARLY_DIV_WARMUP
    ).reset_index(drop=True)
    if lifecycle_frame.empty:
        BOT1_RUNTIME["buffer_zone_alert_ids"] = []
        return []
    if df_1h is None:
        zones = get_active_zones(lifecycle_frame, df_4h, df_1d, signal_time)
    else:
        zone_role_context = reconstruct_zone_role_context_cached(
            df_1h,
            df_4h,
            df_1d,
            signal_close_time,
        )
        zones = get_active_zones(
            lifecycle_frame,
            df_4h,
            df_1d,
            signal_time,
            df_1h=df_1h,
            zone_role_context=zone_role_context,
            role_visible_at=signal_close_time,
        )
    signal_idx = len(lifecycle_frame) - 1
    zones, _exhausted = select_entry_lifecycle_zones(
        lifecycle_frame,
        zones,
        signal_idx,
    )
    current_ids = set()
    entered: List[Dict] = []

    for zone in zones:
        side_filter = zone.get("side")
        if side_filter not in {"support", "resistance"}:
            continue
        if zone.get("reversal_only"):
            continue
        if not zone_matches_current_buffer(zone, float(price), side_filter):
            continue
        if not _cloud_approach_matches(
            lifecycle_frame,
            signal_idx,
            zone,
            side_filter,
        ):
            continue
        if (
            side_filter == "support"
            and zone.get("kind") == "cloud_line"
            and zone.get("tf") in {"4h", "1d"}
        ):
            continue
        reason_key = (
            "early_bull_div_candidate"
            if side_filter == "support"
            else "early_bear_div_candidate"
        )
        bucket = (
            f"flipped_{zone['tf']}_ob"
            if zone.get("kind") == "ob" and zone.get("role_epoch")
            else f"{zone['tf']}_ob"
            if zone.get("kind") == "ob"
            else f"{zone['tf']}_cloud"
        )
        if (bucket, reason_key) in EXCLUDED_SETUPS:
            continue

        zone_id = str(zone.get("zone_id") or zone.get("name") or "")
        if not zone_id:
            continue
        current_ids.add(zone_id)
        if zone_id not in previous_ids:
            entered.append(zone)

    BOT1_RUNTIME["buffer_zone_alert_ids"] = sorted(current_ids)
    return entered


def _zone_bounds_at_timestamp(zone: Dict, ts) -> Tuple[float, float]:
    values = zone.get("_line_values")
    timestamps = zone.get("_line_timestamps")
    if zone.get("kind") == "cloud_line" and values is not None and timestamps is not None and len(values):
        pos = int(timestamps.searchsorted(pd.Timestamp(ts), side="right")) - 1
        if pos >= 0:
            value = float(values[min(pos, len(values) - 1)])
            return value, value
    return min(float(zone["low"]), float(zone["high"])), max(float(zone["low"]), float(zone["high"]))


def _bar_intersects_zone(row: pd.Series, zone: Dict) -> bool:
    zl, zh = _zone_bounds_at_timestamp(zone, row["timestamp"])
    return float(row["high"]) >= zl - HUNT_BUFFER and float(row["low"]) <= zh + HUNT_BUFFER


def _bar_deeply_penetrates_zone(row: pd.Series, zone: Dict, side_filter: str) -> bool:
    zl, zh = _zone_bounds_at_timestamp(zone, row["timestamp"])
    if side_filter == "support":
        return float(row["low"]) < zl - HUNT_BUFFER
    return float(row["high"]) > zh + HUNT_BUFFER


def _cloud_approach_matches(df_15m: pd.DataFrame, signal_idx: int, zone: Dict, side_filter: str) -> bool:
    if zone.get("kind") != "cloud_line":
        return True
    if signal_idx <= 0:
        return False
    previous_bar = df_15m.iloc[signal_idx - 1]
    previous_close = float(previous_bar["close"])
    line, _ = _zone_bounds_at_timestamp(zone, previous_bar["timestamp"])
    if side_filter == "support":
        return zone.get("cloud_role") == "top" and previous_close >= line
    return zone.get("cloud_role") == "bottom" and previous_close <= line


def count_distinct_wave_touches(
    df_15m: pd.DataFrame,
    zone: Dict,
    signal_idx: int,
    side_filter: str,
) -> int:
    """Count leave/pivot/return touch waves, not candles rubbing a zone."""
    if signal_idx < 0:
        return 0
    # Work only on the visible prefix; a pivot is still used after `order`
    # following bars confirm it. Local arrays avoid sharing mutable worker caches.
    visible = df_15m.iloc[:signal_idx + 1]
    timestamps = pd.DatetimeIndex(pd.to_datetime(visible["timestamp"]))
    active_from = zone.get("active_from")
    start = int(timestamps.searchsorted(pd.Timestamp(active_from), side="left")) if active_from is not None else 0
    highs = visible["high"].to_numpy(dtype=float)
    lows = visible["low"].to_numpy(dtype=float)
    lower = np.full(len(visible), min(float(zone["low"]), float(zone["high"])))
    upper = np.full(len(visible), max(float(zone["low"]), float(zone["high"])))
    values, line_times = zone.get("_line_values"), zone.get("_line_timestamps")
    if zone.get("kind") == "cloud_line" and values is not None and line_times is not None and len(values):
        indices = line_times.searchsorted(timestamps, side="right") - 1
        known = indices >= 0
        lower[known] = upper[known] = np.asarray(values, dtype=float)[np.minimum(indices[known], len(values) - 1)]

    order = ZONE_WAVE_PIVOT_ORDER
    exact_touch = zone.get("kind") == "ob"
    touches = ((highs >= lower) & (lows <= upper)) if exact_touch else (
        (highs >= lower - HUNT_BUFFER) & (lows <= upper + HUNT_BUFFER)
    )
    support = side_filter == "support"
    extremes = np.full(len(visible), np.nan)
    width = order * 2 + 1
    if len(visible) >= width:
        windows = np.lib.stride_tricks.sliding_window_view(highs if support else lows, width)
        windows = np.where(np.isnan(windows), -np.inf if support else np.inf, windows)
        extremes[order:len(visible) - order] = windows.max(axis=1) if support else windows.min(axis=1)
    if support:
        pivots = (highs >= extremes) & (highs > upper + HUNT_BUFFER)
        if exact_touch:
            pivots &= lows > upper
    else:
        pivots = (lows <= extremes) & (lows < lower - HUNT_BUFFER)
        if exact_touch:
            pivots &= highs < lower
    touch_count = 0
    armed = True
    last_touch_idx = -1
    for i in range(start, signal_idx + 1):
        if not armed and i >= start + order * 2:
            pivot_idx = i - order
            if pivot_idx > last_touch_idx and pivots[pivot_idx]:
                armed = True
        if armed and touches[i]:
            touch_count += 1
            armed = False
            last_touch_idx = i

    return touch_count


def count_entry_wave_touches(
    df_15m: pd.DataFrame,
    zone: Dict,
    signal_idx: int,
    side_filter: str,
) -> int:
    """Keep proximity-wave identity compatible with already consumed F keys."""
    if zone.get("kind") != "ob":
        return count_distinct_wave_touches(df_15m, zone, signal_idx, side_filter)
    proximity = {
        **zone,
        "kind": "entry_proximity",
        "active_from": zone.get("entry_wave_from", zone.get("active_from")),
    }
    return count_distinct_wave_touches(df_15m, proximity, signal_idx, side_filter)


def select_entry_lifecycle_zones(
    df_15m: pd.DataFrame,
    zones: Sequence[Dict],
    signal_idx: int,
    *,
    wave_book=None,
) -> Tuple[List[Dict], List[Dict]]:
    """Apply touch exhaustion before the per-timeframe recency limits."""
    live: List[Dict] = []
    exhausted: List[Dict] = []
    for zone in zones:
        side = str(zone.get("side") or "")
        if side not in {"support", "resistance"}:
            continue
        touch_count = (wave_book.count(zone, side) if wave_book is not None else
                       count_distinct_wave_touches(df_15m, zone, signal_idx, side))
        candidate = {
            **zone,
            "touch_count": touch_count,
            "entry_wave_count": (wave_book.count(zone, side, proximity=True) if wave_book is not None else
                                 count_entry_wave_touches(df_15m, zone, signal_idx, side)),
        }
        if touch_count > ZONE_MAX_VALID_TOUCHES:
            exhausted.append(candidate)
        else:
            live.append(candidate)

    selected: List[Dict] = [zone for zone in live if zone.get("kind") != "ob"]
    for timeframe, limit in (("1h", 4), ("4h", 4), ("1d", 1)):
        for side in ("support", "resistance"):
            candidates = [
                zone
                for zone in live
                if zone.get("kind") == "ob"
                and zone.get("tf") == timeframe
                and zone.get("side") == side
            ]
            candidates.sort(
                key=lambda zone: (
                    pd.Timestamp(zone.get("active_from") or zone.get("anchor_time")).value
                    if zone.get("active_from") is not None
                    or zone.get("anchor_time") is not None
                    else -1
                )
            )
            selected.extend(candidates[-limit:])
    return selected, exhausted


def zones_touched_in_lookback(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    zones: List[Dict],
    signal_idx: int,
    side_filter: str,
    lifecycle=None,
) -> List[Dict]:
    if signal_idx < 0 or signal_idx >= len(df_15m):
        return []
    row = df_15m.iloc[signal_idx]
    matched: List[Dict] = []
    eligible_zones, exhausted_zones = (lifecycle if lifecycle is not None else
                                       select_entry_lifecycle_zones(df_15m, zones, signal_idx))
    for zone in exhausted_zones:
        if zone.get("side") != side_filter:
            continue
        if not _cloud_approach_matches(df_15m, signal_idx, zone, side_filter):
            continue
        if _bar_intersects_zone(row, zone):
            log.info(
                f"entry zone exhausted | zone={zone.get('zone_id', zone['name'])} "
                f"touches={zone['touch_count']}"
            )

    for zone in eligible_zones:
        if zone.get("side") != side_filter:
            continue
        if not _cloud_approach_matches(df_15m, signal_idx, zone, side_filter):
            continue
        if not _bar_intersects_zone(row, zone):
            continue
        reversal_entry_zone = ZoneRoleEntryZone(
            zone_id=str(zone.get("zone_id") or zone.get("name") or ""),
            source_tf=str(zone.get("source_tf") or zone.get("tf") or ""),
            side=str(zone.get("side") or ""),
            low=min(float(zone["low"]), float(zone["high"])),
            high=max(float(zone["low"]), float(zone["high"])),
        )
        bullish_reversal, lower_wick_ratio = qualifies_bullish_reversal_candle(
            row,
            reversal_entry_zone,
            BULLISH_REVERSAL_RULE,
        )
        if zone.get("reversal_only") and not (
            side_filter == "support" and bullish_reversal
        ):
            continue
        touch_count = int(zone["touch_count"])
        matched.append({
            **zone,
            "effective_side": side_filter,
            "touch_count": touch_count,
            "touch_time": row["timestamp"],
            "deep_penetration": _bar_deeply_penetrates_zone(row, zone, side_filter),
            "bullish_reversal": bullish_reversal,
            "lower_wick_range_ratio": lower_wick_ratio,
        })
    return matched

def classify_and_score(matched: List[Dict], price: float, df_15m: pd.DataFrame, ts) -> tuple:
    if not matched:
        return "none", ""

    scored = sorted(
        [(z, score_zone(z, price, df_15m, ts)) for z in matched],
        key=lambda x: -x[1]
    )

    kinds = sorted({
        (
            f"flipped_{z['tf']}_ob"
            if z["kind"] == "ob" and z.get("role_epoch")
            else f"{z['tf']}_ob"
            if z["kind"] == "ob"
            else f"{z['tf']}_cloud"
        )
        for z, _ in scored
    })
    bucket = kinds[0] if len(scored) == 1 else "overlap"

    details = " | ".join(format_zone_detail(z, s) for z, s in scored)
    return bucket, details


def exclude_long_cloud_zones(zones: List[Dict]) -> List[Dict]:
    return [z for z in zones if not (z.get("kind") == "cloud_line" and z.get("tf") in {"4h", "1d"})]


def _setup_confirmation_time(signal_ts, tf: str) -> pd.Timestamp:
    ts = pd.Timestamp(signal_ts)
    delta = pd.to_timedelta(timeframe_to_ms(tf), unit="ms")
    return ts.floor(tf) + delta


def queue_deep_penetration_setups(
    zones: List[Dict],
    entry_side: str,
    reason_key: str,
    atr_v: float,
    signal_ts,
    signal_id: str = "",
) -> None:
    pending = BOT1_RUNTIME.setdefault("pending_setups", [])
    if not isinstance(pending, list):
        pending = []
        BOT1_RUNTIME["pending_setups"] = pending
    for zone in zones:
        zone_id = str(zone.get("zone_id", zone["name"]))
        setup_key = f"{format_meta_timestamp(signal_ts)}:{entry_side}:{zone_id}"
        if any(item.get("setup_key") == setup_key for item in pending if isinstance(item, dict)):
            continue
        pending.append({
            "setup_key": setup_key,
            "signal_time": str(pd.Timestamp(signal_ts)),
            "confirm_at": str(_setup_confirmation_time(signal_ts, str(zone["tf"]))),
            "entry_side": entry_side,
            "reason_key": reason_key,
            "signal_id": signal_id,
            "atr": float(atr_v),
            "zone": {
                key: value for key, value in zone.items()
                if key != "wave_range" and not key.startswith("_")
            },
        })
        log.info(f"깊은 관통 setup 보류 | key={setup_key} confirm_at={pending[-1]['confirm_at']}")


def evaluate_pending_setups(
    df_15m: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    latest_bar: pd.Series,
    df_1h: Optional[pd.DataFrame] = None,
) -> Optional[Tuple[str, str, str, str, float]]:
    pending = BOT1_RUNTIME.get("pending_setups")
    if not isinstance(pending, list) or not pending:
        return None
    latest_close_time = pd.Timestamp(latest_bar["timestamp"]) + pd.to_timedelta(
        timeframe_to_ms(ENTRY_TF), unit="ms"
    )
    zone_role_context = None
    if df_1h is not None:
        zone_role_context = reconstruct_zone_role_context_cached(
            df_1h,
            df_4h,
            df_1d,
            latest_close_time,
        )
    if zone_role_context is None:
        active_zones = get_active_zones(
            df_15m,
            df_4h,
            df_1d,
            latest_bar["timestamp"],
            df_1h=df_1h,
        )
    else:
        active_zones = get_active_zones(
            df_15m,
            df_4h,
            df_1d,
            latest_bar["timestamp"],
            df_1h=df_1h,
            zone_role_context=zone_role_context,
            role_visible_at=latest_close_time,
        )
    active_by_id = {str(z.get("zone_id", z["name"])): z for z in active_zones}
    retained: List[Dict] = []
    triggered: Optional[Tuple[str, str, str, str, float]] = None

    for setup in pending:
        if not isinstance(setup, dict):
            continue
        confirm_at = pd.Timestamp(setup["confirm_at"])
        if latest_close_time < confirm_at:
            retained.append(setup)
            continue
        zone_snapshot = setup.get("zone") or {}
        zone_id = str(zone_snapshot.get("zone_id", zone_snapshot.get("name", "")))
        zone = active_by_id.get(zone_id)
        if zone is None:
            log.info(f"보류 setup 폐기: 존 무효/생명주기 종료 | zone={zone_id}")
            continue
        price = float(latest_bar["close"])
        zl = min(float(zone["low"]), float(zone["high"]))
        zh = max(float(zone["low"]), float(zone["high"]))
        side_filter = "support" if setup["entry_side"] == "long" else "resistance"
        if side_filter == "support":
            reclaimed_and_near = zl <= price <= zh + HUNT_BUFFER
        else:
            reclaimed_and_near = zl - HUNT_BUFFER <= price <= zh
        if not reclaimed_and_near:
            log.info(f"보류 setup 폐기: HTF 마감 시 떠난 기차/미회복 | zone={zone_id} close={price:.1f}")
            continue
        if triggered is None:
            kind = f"{zone['tf']}_ob" if zone["kind"] == "ob" else f"{zone['tf']}_cloud"
            details = (
                f"HTF reclaim after deep penetration | {zone['name']} | "
                f"signal={setup['signal_time']} | signal_id={setup.get('signal_id', '')}"
            )
            triggered = EntrySignal(
                str(setup["entry_side"]),
                kind,
                details,
                str(setup["reason_key"]),
                float(setup.get("atr", 200.0)),
                entry_zones=[zone],
            )

    BOT1_RUNTIME["pending_setups"] = retained
    return triggered

# =========================
# 상태
# =========================
@dataclass
class Position:
    side: str
    entry_price: float
    entry_time: str
    stop_price: float
    size_remaining: float
    entry_size: float = 0.0
    tp1_price: float = 0.0
    tp2_price: float = 0.0
    tp1_size: float = 0.0
    tp2_size: float = 0.0
    sl_dist: float = 0.0
    tp1_dist: float = 0.0
    tp2_dist: float = 0.0
    tp1_stop_dist: float = 0.0
    max_hold_bars: int = MAX_HOLD_BARS
    max_hold_after_tp1: bool = False
    exit_profile_name: str = "current"

    tp1_hit: bool = False
    tp2_hit: bool = False
    trail_active: bool = False
    trail_extreme: Optional[float] = None
    entry_atr: float = 200.0

    leader_bucket: str = ""
    leader_details: str = ""
    entry_reason: str = ""
    signal_id: str = ""

    bars_held: int = 0
    trail_close_fail_count: int = 0
    entry_fee: float = 0.0
    tp1_fee: float = 0.0
    tp2_fee: float = 0.0
    fees_currency: str = ""
    tp1_fill_price: Optional[float] = None
    tp1_fill_size: Optional[float] = None
    tp2_fill_price: Optional[float] = None
    tp2_fill_size: Optional[float] = None

    trade_id: str = ""
    entry_order_id: str = ""
    tp1_order_id: str = ""
    tp2_order_id: str = ""
    sl_order_id: str = ""
    slot_index: int = 0
    slot_weight: float = 1.0
    zone_key: str = ""


def infer_position_entry_size(pos: "Position") -> float:
    tp1_filled_size = 0.0
    if pos.tp1_hit:
        tp1_filled_size = float(
            pos.tp1_fill_size if pos.tp1_fill_size is not None else pos.tp1_size
        )
    tp2_filled_size = 0.0
    if pos.tp2_hit:
        tp2_filled_size = float(
            pos.tp2_fill_size if pos.tp2_fill_size is not None else pos.tp2_size
        )
    observed_total = round_qty(pos.size_remaining + tp1_filled_size + tp2_filled_size)
    split_total = round_qty(pos.tp1_size + pos.tp2_size)
    return max(observed_total, split_total)


def get_position_entry_size(pos: "Position") -> float:
    if pos.entry_size > 0:
        return round_qty(pos.entry_size)
    return infer_position_entry_size(pos)


def position_size_after_tp1(pos: "Position") -> float:
    return round_qty(max(0.0, get_position_entry_size(pos) - pos.tp1_size))


def position_size_after_tp2(pos: "Position") -> float:
    return round_qty(
        max(0.0, get_position_entry_size(pos) - pos.tp1_size - pos.tp2_size)
    )


def position_size_reached_target(
    current_size: float,
    target_size: float,
    order_id: str = "",
    expected_fill: float = 0.0,
) -> bool:
    current = round_qty(current_size)
    target = round_qty(target_size)
    if current == target:
        return True
    if current > target:
        return False
    return order_has_confirmed_fill(order_id, expected_fill)


def resolve_position_tp1_stop_dist(pos: "Position") -> float:
    if pos.tp1_stop_dist != 0.0 or pos.exit_profile_name != "current":
        return float(pos.tp1_stop_dist)
    reason_key = normalize_reason_key(pos.entry_reason)
    return float(get_stop_profile(pos.leader_bucket, reason_key)["tp1_stop_dist"])


def hydrate_position_profiles(pos: "Position") -> "Position":
    reason_key = normalize_reason_key(pos.entry_reason)
    exit_profile = get_exit_profile(pos.leader_bucket, reason_key)
    split_profile = get_split_profile(pos.leader_bucket, reason_key)
    stop_profile = get_stop_profile(pos.leader_bucket, reason_key)

    if pos.tp1_price <= 0:
        pos.tp1_price = (
            pos.entry_price + exit_profile["tp1_dist"]
            if pos.side == "long"
            else pos.entry_price - exit_profile["tp1_dist"]
        )
    if pos.tp2_price <= 0:
        pos.tp2_price = (
            pos.entry_price + exit_profile["tp2_dist"]
            if pos.side == "long"
            else pos.entry_price - exit_profile["tp2_dist"]
        )
    if pos.tp1_size <= 0:
        pos.tp1_size = split_profile["tp1_size"]
    if pos.tp2_size <= 0:
        pos.tp2_size = split_profile["tp2_size"]
    if pos.sl_dist <= 0:
        pos.sl_dist = stop_profile["sl_dist"]
    if pos.tp1_dist <= 0:
        pos.tp1_dist = exit_profile["tp1_dist"]
    if pos.tp2_dist <= 0:
        pos.tp2_dist = exit_profile["tp2_dist"]
    pos.tp1_stop_dist = resolve_position_tp1_stop_dist(pos)
    if pos.max_hold_bars <= 0:
        pos.max_hold_bars = MAX_HOLD_BARS
    if pos.entry_size <= 0:
        pos.entry_size = infer_position_entry_size(pos)
    return pos


def consumed_signal_ids() -> List[str]:
    stored = BOT1_RUNTIME.get("consumed_signal_ids")
    if not isinstance(stored, list):
        stored = []
    normalized = list(
        dict.fromkeys(
            str(item) for item in stored
            if item is not None and str(item)
        )
    )
    normalized = normalized[-CONSUMED_SIGNAL_ID_LIMIT:]
    BOT1_RUNTIME["consumed_signal_ids"] = normalized
    return normalized


def is_signal_consumed(signal_id: str) -> bool:
    return bool(signal_id) and signal_id in consumed_signal_ids()


def consume_signal_id(signal_id: str) -> None:
    if not signal_id:
        return
    stored = consumed_signal_ids()
    if signal_id in stored:
        stored.remove(signal_id)
    stored.append(signal_id)
    BOT1_RUNTIME["consumed_signal_ids"] = stored[-CONSUMED_SIGNAL_ID_LIMIT:]


def resolve_entry_signal_id(
    df_15m: pd.DataFrame,
    signal_bar_time,
    details: str,
    reason_key: str,
) -> str:
    if reason_key not in EARLY_DIVERGENCE_SIGNAL_COLUMNS:
        return ""

    embedded = re.search(r"(?:^|\|)\s*signal_id=([^|]+)", str(details or ""))
    if embedded and embedded.group(1).strip():
        return embedded.group(1).strip()

    source_time = signal_bar_time
    pending_source = re.search(r"(?:^|\|)\s*signal=([^|]+)", str(details or ""))
    if pending_source and pending_source.group(1).strip():
        source_time = pending_source.group(1).strip()
    return build_early_divergence_signal_id(df_15m, source_time, reason_key)


def ensure_position_signal_consumed(
    pos: "Position",
    df_15m: pd.DataFrame,
) -> "Position":
    if pos.signal_id:
        consume_signal_id(pos.signal_id)
        return pos
    signal_id = resolve_entry_signal_id(
        df_15m,
        pos.entry_time,
        pos.leader_details,
        normalize_reason_key(pos.entry_reason),
    )
    if signal_id:
        pos.signal_id = signal_id
        consume_signal_id(signal_id)
        log.info(f"활성 포지션 신호 ID 복원 | signal_id={signal_id}")
    return pos

def quarantine_state_file(reason: str, detail: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    corrupt_path = f"{STATE_FILE}.corrupt.{timestamp}"
    try:
        os.replace(STATE_FILE, corrupt_path)
        msg = (
            f"⚠️ bot_state.json 무효 상태 감지\n"
            f"reason={reason}\n"
            f"backup={corrupt_path}\n"
            f"detail={detail}\n"
            f"state 초기화 후 계속 진행"
        )
    except OSError as move_error:
        msg = (
            f"⚠️ bot_state.json 무효 상태 감지\n"
            f"reason={reason}\n"
            f"detail={detail}\n"
            f"backup_failed={move_error}\n"
            f"state 초기화 후 계속 진행"
        )
    log.error(msg)
    notify(msg)

def save_state(pos: Optional["Position"]):
    payload = {
        "version": 4,
        "position": asdict(pos) if pos else None,
        "runtime": BOT1_RUNTIME,
    }
    tmp_path = f"{STATE_FILE}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, default=str, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, STATE_FILE)

def load_state() -> Optional["Position"]:
    if not os.path.exists(STATE_FILE):
        normalize_zone_role_policy_runtime()
        return None
    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        mark_zone_role_policy_unsafe("state_json_decode_error")
        quarantine_state_file("json_decode_error", str(e))
        return None
    except OSError as e:
        mark_zone_role_policy_unsafe(f"state_read_error:{type(e).__name__}")
        log.error(f"state 파일 읽기 오류: {e}")
        notify(
            f"⚠️ state 파일 읽기 오류\n"
            f"path={STATE_FILE}\n"
            f"detail={e}\n"
            f"state 없이 계속 진행"
        )
        return None
    if data is None:
        mark_zone_role_policy_unsafe("state_payload_null")
        return None
    if not isinstance(data, dict):
        mark_zone_role_policy_unsafe(f"invalid_state_type:{type(data).__name__}")
        quarantine_state_file("invalid_state_type", f"type={type(data).__name__}")
        return None
    global BOT1_RUNTIME
    if "position" in data:
        runtime = data.get("runtime")
        if isinstance(runtime, dict):
            BOT1_RUNTIME.update(runtime)
            consumed_signal_ids()
        try:
            state_version = int(data.get("version") or 1)
        except (TypeError, ValueError):
            state_version = 4
        normalize_zone_role_policy_runtime(
            allow_missing=state_version < 4,
            raise_on_unsafe=False,
        )
        pos_data = data.get("position")
    else:
        # version 1 compatibility: the root object was the Position itself.
        normalize_zone_role_policy_runtime()
        pos_data = data
    if pos_data is None:
        return None
    fields = Position.__dataclass_fields__
    try:
        pos = Position(**{k: v for k, v in pos_data.items() if k in fields})
        return hydrate_position_profiles(pos)
    except TypeError as e:
        quarantine_state_file("invalid_state_fields", str(e))
        return None

# =========================
# 거래소 헬퍼
# =========================
def set_leverage(lev: int = 1):
    try:
        exchange.set_leverage(lev, SYMBOL)
        log.info(f"레버리지 {lev}x 설정")
    except Exception as e:
        log.warning(f"레버리지 설정 스킵: {e}")

def get_available_quote_balance(quote_currency: str = "USDT") -> Optional[float]:
    try:
        balance = exchange.fetch_balance()
    except Exception as e:
        log.warning(f"잔고 조회 오류: {type(e).__name__}: {e}")
        return None

    candidate_sources = [
        (balance.get(quote_currency) or {}).get("free"),
        (balance.get("free") or {}).get(quote_currency),
        (balance.get("total") or {}).get(quote_currency),
        (balance.get(quote_currency) or {}).get("total"),
    ]
    for candidate in candidate_sources:
        try:
            if candidate is None:
                continue
            value = float(candidate)
            if value >= 0:
                return value
        except (TypeError, ValueError):
            continue
    return None

def get_effective_entry_leverage(cached_positions: Optional[List[Dict]] = None) -> float:
    """
    자동으로 레버리지를 '설정'하지는 않는다.
    대신 현재 거래소에 이미 적용되어 있는 레버리지를 읽어서
    사전 잔고 체크에 반영한다.

    우선순위:
    0) cached_positions가 주어졌으면 그 안에서 추출 (신규 진입 경로의 중복 API 콜 제거)
    1) 현재 열린 포지션의 leverage
    2) fetch_positions([SYMBOL])에 잡히는 심볼 레버리지
    3) 실패 시 ENTRY_LEVERAGE fallback
    """
    if cached_positions is not None:
        for p in cached_positions:
            candidates = [
                p.get("leverage"),
                (p.get("info") or {}).get("leverage"),
                (p.get("info") or {}).get("buyLeverage"),
                (p.get("info") or {}).get("sellLeverage"),
            ]
            for v in candidates:
                try:
                    fv = float(v)
                    if fv > 0:
                        return fv
                except (TypeError, ValueError):
                    continue
        return max(float(ENTRY_LEVERAGE), 1.0)

    try:
        p = get_exchange_position()
        if p is not None:
            candidates = [
                p.get("leverage"),
                (p.get("info") or {}).get("leverage"),
                (p.get("info") or {}).get("buyLeverage"),
                (p.get("info") or {}).get("sellLeverage"),
            ]
            for v in candidates:
                try:
                    fv = float(v)
                    if fv > 0:
                        return fv
                except (TypeError, ValueError):
                    continue
    except Exception:
        pass

    try:
        positions = exchange.fetch_positions([SYMBOL])
        for p in positions:
            candidates = [
                p.get("leverage"),
                (p.get("info") or {}).get("leverage"),
                (p.get("info") or {}).get("buyLeverage"),
                (p.get("info") or {}).get("sellLeverage"),
            ]
            for v in candidates:
                try:
                    fv = float(v)
                    if fv > 0:
                        return fv
                except (TypeError, ValueError):
                    continue
    except Exception as e:
        log.warning(f"레버리지 조회 오류: {type(e).__name__}: {e}")

    return max(float(ENTRY_LEVERAGE), 1.0)


def estimate_required_entry_balance(
    size_btc: float,
    reference_price: float,
    leverage: Optional[float] = None,
) -> float:
    notional = abs(float(size_btc) * float(reference_price))
    effective_leverage = max(float(leverage or get_effective_entry_leverage()), 1.0)
    initial_margin = notional / effective_leverage
    fee_buffer = notional * ENTRY_TAKER_FEE_RATE
    return (initial_margin + fee_buffer) * ENTRY_MARGIN_BUFFER_RATIO

def precheck_entry_balance(
    size_btc: float,
    reference_price: float,
    cached_positions: Optional[List[Dict]] = None,
) -> Tuple[bool, str]:
    free_balance = get_available_quote_balance("USDT")
    effective_leverage = get_effective_entry_leverage(cached_positions=cached_positions)
    required_balance = estimate_required_entry_balance(
        size_btc,
        reference_price,
        leverage=effective_leverage,
    )
    if free_balance is None:
        return True, (
            f"balance_check=unavailable "
            f"leverage_est={effective_leverage:.2f} "
            f"required_est={required_balance:.2f} USDT "
            f"ref_price={reference_price:.1f}"
        )
    return (
        free_balance + 1e-9 >= required_balance,
        (
            f"free_usdt={free_balance:.2f} "
            f"leverage_est={effective_leverage:.2f} "
            f"required_est={required_balance:.2f} "
            f"ref_price={reference_price:.1f}"
        ),
    )

def get_exchange_position() -> Optional[Dict]:
    try:
        positions = exchange.fetch_positions([SYMBOL])
        for p in positions:
            contracts = abs(float(p.get("contracts", 0) or 0))
            if contracts > 0:
                return p
    except Exception as e:
        log.error(f"포지션 조회 오류: {e}")
    return None

def get_exchange_position_size() -> float:
    p = get_exchange_position()
    if p is None:
        return 0.0
    return round_qty(abs(float(p.get("contracts", 0) or 0)))


def resolve_actual_entry_size(
    requested_size: float,
    resolved_fill_size: float,
    exchange_position: Optional[Dict],
) -> float:
    candidates = []
    if exchange_position is not None:
        candidates.append(exchange_position.get("contracts"))
    candidates.extend((resolved_fill_size, requested_size))
    for candidate in candidates:
        try:
            size = round_qty(abs(float(candidate)))
        except (TypeError, ValueError):
            continue
        if math.isfinite(size) and size > 0:
            return size
    raise ValueError("actual entry size could not be resolved")

def get_exchange_entry_price_fallback(default_price: float) -> float:
    try:
        p = get_exchange_position()
        if p:
            candidates = [
                p.get("entryPrice"),
                p.get("entry_price"),
                p.get("markPrice"),
                p.get("mark_price"),
            ]
            for v in candidates:
                if v is not None:
                    fv = float(v)
                    if fv > 0:
                        return fv
    except Exception:
        pass
    return default_price

def cancel_order_safe(order_id: str):
    if not order_id:
        return
    try:
        exchange.cancel_order(order_id, SYMBOL)
        log.info(f"주문 취소: {order_id}")
    except Exception as e:
        log.warning(f"주문 취소 스킵({order_id}): {e}")

def cancel_all_orders_safe():
    try:
        exchange.cancel_all_orders(SYMBOL)
        log.info("전체 주문 취소")
    except Exception as e:
        log.warning(f"전체 주문 취소 오류: {e}")

def place_entry_market(side: str, size: float, label: str = "") -> Optional[Dict]:
    try:
        order = exchange.create_order(
            SYMBOL,
            "market",
            "buy" if side == "long" else "sell",
            size
        )
        log.info(f"시장가 진입 [{label}] {side} {size}BTC -> id={order.get('id')}")
        return order
    except Exception as e:
        log.error(
            f"시장가 진입 오류 [{label}] side={side} size={size}BTC "
            f"type={type(e).__name__}: {e}"
        )
        return None

def place_reduce_market_close(side: str, size: float, label: str = "") -> Optional[Dict]:
    try:
        order = exchange.create_order(
            SYMBOL,
            "market",
            "sell" if side == "long" else "buy",
            size,
            None,
            {"reduceOnly": True}
        )
        log.info(f"시장가 청산 [{label}] {size}BTC -> id={order.get('id')}")
        return order
    except Exception as e:
        log.error(f"시장가 청산 오류: {e}")
        return None

def place_reduce_limit_tp(side: str, size: float, price: float, label: str = "", link_id: str = "TP") -> Optional[str]:
    try:
        order = exchange.create_order(
            SYMBOL,
            "limit",
            "sell" if side == "long" else "buy",
            size,
            round(price, 1),
            {
                "reduceOnly": True,
                "timeInForce": "GTC",
                "orderLinkId": make_order_link_id(link_id),
            }
        )
        log.info(f"지정가 TP [{label}] size={size} price={price:.1f} -> id={order.get('id')}")
        return str(order["id"])
    except Exception as e:
        log.error(f"지정가 TP 오류 [{label}]: {e}")
        return None

def place_hard_stop(side: str, size: float, trigger_price: float, label: str = "", link_id: str = "SL") -> Optional[str]:
    try:
        is_long = side == "long"
        order = exchange.create_order(
            SYMBOL,
            "market",
            "sell" if is_long else "buy",
            size,
            None,
            {
                "triggerPrice": round(trigger_price, 1),
                "triggerDirection": 2 if is_long else 1,
                "triggerBy": "LastPrice",
                "reduceOnly": True,
                "closeOnTrigger": True,
                "orderLinkId": make_order_link_id(link_id),
            }
        )
        log.info(
            f"하드 스탑 [{label}] size={size} trigger={trigger_price:.1f} "
            f"dir={2 if is_long else 1} -> id={order.get('id')}"
        )
        return str(order["id"])
    except Exception as e:
        log.error(f"하드 스탑 오류 [{label}]: {e}")
        return None

# =========================
# 주문 재설정
# =========================
def replace_hard_stop(
    pos: Position,
    new_stop: float,
    label: str,
    link_id: str = "MOVE_SL"
) -> Optional[Position]:
    cancel_order_safe(pos.sl_order_id)
    pos.stop_price = new_stop

    new_sl_id = place_hard_stop(pos.side, pos.size_remaining, pos.stop_price, label, link_id)

    # 1차 실패 → 1초 후 재시도
    if not new_sl_id:
        log.warning(f"하드스탑 재설정 1차 실패 [{label}] → 1초 후 재시도")
        time.sleep(1.0)
        new_sl_id = place_hard_stop(pos.side, pos.size_remaining, pos.stop_price, label, link_id)

    # 재시도도 실패 → 긴급 알림 + 포지션 강제청산
    if not new_sl_id:
        notify(
            f"🚨 하드스탑 재설정 최종 실패 → 긴급 청산\n"
            f"{SYMBOL}\n"
            f"side={pos.side}\n"
            f"remain={pos.size_remaining} BTC\n"
            f"stop={pos.stop_price:.1f}\n"
            f"reason={label}"
        )
        cancel_all_orders_safe()

        exch_pos = get_exchange_position()
        if exch_pos is not None:
            current_size = round_qty(abs(float(exch_pos.get("contracts", 0) or 0)))
            if current_size > 0:
                close_order = place_reduce_market_close(pos.side, current_size, "EMERGENCY_NO_SL")
                if close_order is not None:
                    close_order_id = str(close_order.get("id", ""))
                    fallback_close_price = estimate_order_fill_price(close_order, None)
                    close_price, close_size, close_fee, close_fee_currency = resolve_order_fill(close_order_id, fallback_close_price, current_size)
                    dashboard_record_close(
                        pos,
                        "emergency_no_sl",
                        close_price,
                        exit_size=close_size,
                        close_fee=close_fee,
                        fee_currency=close_fee_currency,
                        note="하드스탑 재설정 실패 후 긴급 시장가 청산",
                        order_id=close_order_id,
                    )
                    notify(
                        f"🆘 SL 재설정 실패로 긴급청산 완료\n"
                        f"{SYMBOL}\n"
                        f"side={pos.side}\n"
                        f"size={current_size} BTC"
                    )
                    return None  # 청산 성공 → 포지션 종료
                else:
                    # 시장가 청산 주문 실패 — 이벤트만 기록, 다음 루프에서 재시도
                    dashboard_append_event(pos, "emergency_no_sl_order_failed",
                                           note="하드스탑 재설정 실패 + 긴급청산 주문도 실패")
                    notify(
                        f"❗ 긴급청산 주문도 실패\n"
                        f"{SYMBOL}\n"
                        f"side={pos.side}\n"
                        f"즉시 수동 확인 필요"
                    )
                    return pos  # 청산 실패 → 포지션 살아있을 수 있음, state 유지
        else:
            # 거래소 포지션이 이미 사라진 경우 — fetch_closed_pnl로 PnL 확보
            pnl_records = fetch_closed_pnl(pos.entry_time)
            exit_price = None
            close_fee = 0.0
            fee_currency = ""
            if pnl_records:
                pnl_records.sort(key=lambda r: r.get("updatedTime", r.get("createdTime", "0")), reverse=True)
                last_rec = pnl_records[0]
                exit_price = float(last_rec.get("avgExitPrice") or 0) or None
                close_fee = (abs(float(last_rec.get("cumExitValue") or 0))) * ENTRY_TAKER_FEE_RATE
                fee_currency = "USDT"
            dashboard_record_close(pos, "emergency_no_sl_position_gone", exit_price,
                                   exit_size=pos.size_remaining, close_fee=close_fee,
                                   fee_currency=fee_currency,
                                   note="하드스탑 재설정 실패 시 포지션 이미 소멸")
            notify(f"❗ 하드스탑 재설정 실패 + 포지션 이미 소멸\n{SYMBOL}\nside={pos.side}")
        return None

    pos.sl_order_id = new_sl_id
    notify(
        f"🛡️ 하드스탑 재설정\n"
        f"{SYMBOL}\n"
        f"side={pos.side}\n"
        f"remain={pos.size_remaining} BTC\n"
        f"stop={pos.stop_price:.1f}\n"
        f"reason={label}"
    )
    return pos

# =========================
# TP1 즉시 감지 + SL 이동
# =========================
def handle_tp1_fill(pos: "Position") -> Optional["Position"]:
    """TP1 체결 감지 → 프로파일별 SL 락인 이동 + 대시보드 기록. 긴급청산 시 None 반환."""
    if pos.tp1_hit:
        return pos
    current_size = round_qty(get_exchange_position_size())
    if current_size < 0:
        return pos
    # 포지션이 완전히 사라진 경우 TP1이 아님 (SL 전량체결 등)
    # manage_position의 exch_pos is None 경로에서 정상 처리되도록 pos 반환
    if current_size <= 0.001:
        return pos
    tp1_target = position_size_after_tp1(pos)
    tp1_filled = position_size_reached_target(
        current_size,
        tp1_target,
        pos.tp1_order_id,
        pos.tp1_size,
    )
    if not tp1_filled:
        return pos
    tp1_price, tp1_size, tp1_fee, tp1_fee_currency = resolve_order_fill(pos.tp1_order_id, pos.tp1_price, pos.tp1_size)
    pos.tp1_hit = True
    pos.size_remaining = current_size
    pos.tp1_fill_price = tp1_price
    pos.tp1_fill_size = tp1_size
    pos.tp1_fee = tp1_fee
    if tp1_fee_currency and not pos.fees_currency:
        pos.fees_currency = tp1_fee_currency
    tp1_stop_dist = resolve_position_tp1_stop_dist(pos)
    tp1_sl = pos.entry_price + tp1_stop_dist if pos.side == "long" else pos.entry_price - tp1_stop_dist
    pos = replace_hard_stop(pos, tp1_sl, f"TP1 후 SL {tp1_stop_dist:+.0f} 이동", link_id="MOVE_SL")
    if pos is None:
        return None
    notify(
        f"✅ 1차 익절 체결 감지\n"
        f"{SYMBOL}\n"
        f"side={pos.side}\n"
        f"tp1={tp1_price:.1f}\n"
        f"remain={pos.size_remaining} BTC\n"
        f"reason={pos.entry_reason}\n"
        f"bucket={pos.leader_bucket}\n"
        f"{pos.leader_details}"
    )
    partial_gross = calc_leg_pnl(pos.side, pos.entry_price, tp1_price, tp1_size)
    partial_fees = estimate_total_fees(pos, 0.0)
    with open_dashboard_db() as conn:
        dashboard_upsert_trade(
            pos,
            status="partially_closed",
            gross_pnl=partial_gross,
            fees_total=partial_fees,
            fees_currency=pos.fees_currency,
            realized_pnl=partial_gross - partial_fees,
            notes="TP1 체결",
            conn=conn,
        )
        dashboard_append_event(
            pos,
            "tp1",
            price=tp1_price,
            size=tp1_size,
            pnl_delta=partial_gross - abs(tp1_fee or 0.0),
            fee_amount=abs(tp1_fee or 0.0),
            fee_currency=tp1_fee_currency or pos.fees_currency,
            note="1차 익절 실제 체결가 기록",
            order_id=pos.tp1_order_id,
            event_key=f"{pos.trade_id}:tp1",
            conn=conn,
        )
        conn.commit()
    save_state(pos)
    return pos

# =========================
# 포지션 동기화 / 관리
# =========================
def sync_tp_state_from_exchange(pos: Position, latest_bar: pd.Series) -> Optional[Position]:
    current_size = get_exchange_position_size()
    if current_size < 0:
        return pos

    current_size = round_qty(current_size)

    tp2_target = position_size_after_tp2(pos)

    if not pos.tp1_hit:
        pos = handle_tp1_fill(pos)
        if pos is None:
            return None

    tp2_filled = position_size_reached_target(
        current_size,
        tp2_target,
        pos.tp2_order_id,
        pos.tp2_size,
    )

    if pos.tp1_hit and (not pos.tp2_hit) and tp2_filled:
        tp1_price = pos.tp1_fill_price if pos.tp1_fill_price is not None else pos.tp1_price
        tp2_target_price = pos.tp2_price
        tp2_price, tp2_size, tp2_fee, tp2_fee_currency = resolve_order_fill(pos.tp2_order_id, tp2_target_price, pos.tp2_size)

        pos.tp2_hit = True
        pos.size_remaining = current_size
        pos.tp2_fill_price = tp2_price
        pos.tp2_fill_size = tp2_size
        pos.tp2_fee = tp2_fee
        if tp2_fee_currency and not pos.fees_currency:
            pos.fees_currency = tp2_fee_currency

        if current_size <= 0.001:
            cancel_all_orders_safe()
            dashboard_record_close(
                pos,
                "tp2_exit",
                tp2_price,
                exit_size=0.0,
                close_fee=0.0,
                fee_currency=tp2_fee_currency or pos.fees_currency,
                note="2차 익절로 전량 종료",
                order_id=pos.tp2_order_id,
            )
            notify(
                f"✅ 2차 익절 체결로 전량 종료\n"
                f"{SYMBOL}\n"
                f"side={pos.side}\n"
                f"tp2={tp2_price:.1f}\n"
                f"reason={pos.entry_reason}\n"
                f"bucket={pos.leader_bucket}\n"
                f"{pos.leader_details}"
            )
            return None

        pos = replace_hard_stop(pos, tp1_price, "TP2 후 1차익절가 이동", link_id="MOVE_SL")
        if pos is None:
            return None

        pos.trail_active = current_size > 0.001
        pos.trail_extreme = float(latest_bar["high"]) if pos.side == "long" else float(latest_bar["low"])

        notify(
            f"✅ 2차 익절 체결 감지\n"
            f"{SYMBOL}\n"
            f"side={pos.side}\n"
            f"tp2={tp2_price:.1f}\n"
            f"remain={pos.size_remaining} BTC\n"
            f"trail=ON\n"
            f"reason={pos.entry_reason}\n"
            f"bucket={pos.leader_bucket}\n"
            f"{pos.leader_details}"
        )
        partial_gross = 0.0
        if pos.tp1_fill_price is not None and pos.tp1_fill_size is not None:
            partial_gross += calc_leg_pnl(pos.side, pos.entry_price, pos.tp1_fill_price, pos.tp1_fill_size)
        partial_gross += calc_leg_pnl(pos.side, pos.entry_price, tp2_price, tp2_size)
        partial_fees = estimate_total_fees(pos, 0.0)
        with open_dashboard_db() as conn:
            dashboard_upsert_trade(
                pos,
                status="partially_closed",
                gross_pnl=partial_gross,
                fees_total=partial_fees,
                fees_currency=pos.fees_currency,
                realized_pnl=partial_gross - partial_fees,
                notes="TP2 체결, 트레일 시작",
                conn=conn,
            )
            dashboard_append_event(
                pos,
                "tp2",
                price=tp2_price,
                size=tp2_size,
                pnl_delta=calc_leg_pnl(pos.side, pos.entry_price, tp2_price, tp2_size) - abs(tp2_fee or 0.0),
                fee_amount=abs(tp2_fee or 0.0),
                fee_currency=tp2_fee_currency or pos.fees_currency,
                note="2차 익절 실제 체결가 기록 후 트레일링 활성화",
                order_id=pos.tp2_order_id,
                event_key=f"{pos.trade_id}:tp2",
                conn=conn,
            )
            conn.commit()

    pos.size_remaining = current_size
    return pos


def _tp2_fill_from_my_trades(order_id: str, min_filled: float):
    """Bybit fetch_my_trades로 실제 체결 이력만 확인. 체결 없으면 None 반환.
    fetch_order 500건 제한 우회 + 미체결/취소 주문을 체결로 오판하지 않는다.
    인덱싱 지연(<1s) 대비 빈 결과 시 1.0초 후 1회 재시도."""
    if not order_id or not exchange.has.get("fetchMyTrades"):
        return None

    def _try_once():
        """예외는 호출부로 전파(=API 에러는 retry 금지). 빈 결과/체결량 부족만 None."""
        trades = exchange.fetch_my_trades(SYMBOL, None, 100, {"orderId": order_id})
        total_qty = total_val = total_fee = 0.0
        fee_currency = ""
        for t in trades:
            try:
                q = float(t.get("amount") or t.get("info", {}).get("execQty") or 0)
                p = float(t.get("price") or t.get("info", {}).get("execPrice") or 0)
            except (TypeError, ValueError):
                continue
            if q <= 0 or p <= 0:
                continue
            total_qty += q
            total_val += q * p
            f = (t.get("fee") or {}).get("cost")
            if f is None:
                f = t.get("info", {}).get("execFee")
            try:
                if f is not None:
                    total_fee += abs(float(f))
            except (TypeError, ValueError):
                pass
            if not fee_currency:
                fee_currency = str((t.get("fee") or {}).get("currency") or t.get("info", {}).get("feeCurrency") or "")
        if total_qty < min_filled or total_val <= 0:
            return None
        return total_val / total_qty, total_qty, total_fee, fee_currency or "USDT"

    try:
        result = _try_once()
    except Exception as e:
        log.warning(f"TP2 체결 이력 조회 실패({order_id}): {e}")
        return None
    if result is not None:
        return result
    time.sleep(1.0)
    try:
        result = _try_once()
    except Exception as e:
        log.warning(f"TP2 체결 이력 retry 실패({order_id}): {e}")
        return None
    if result is not None:
        log.info(f"TP2 체결 이력 retry 성공 ({order_id})")
    return result


def finalize_missing_exchange_position(pos: Position) -> bool:
    if not (pos.tp1_hit and (not pos.tp2_hit) and pos.tp2_order_id):
        return False

    min_filled = max(0.001, pos.tp2_size - POSITION_SIZE_TOLERANCE)
    tp2_price: Optional[float] = None
    tp2_filled = 0.0
    tp2_fee = 0.0
    tp2_fee_currency = ""
    resolve_path = ""

    try:
        tp2_order = exchange.fetch_order(pos.tp2_order_id, SYMBOL)
        tp2_status = (tp2_order.get("status") or "").lower()
        filled = float(tp2_order.get("filled") or 0)
        if tp2_status == "closed" and filled >= min_filled:
            tp2_price = estimate_order_fill_price(tp2_order, pos.tp2_price)
            tp2_filled = filled
            tp2_fee, tp2_fee_currency = estimate_order_fee(tp2_order)
            resolve_path = "fetch_order"
    except Exception as e:
        log.warning(f"TP2 오더 상태 조회 실패({pos.tp2_order_id}): {e}")

    if tp2_price is None or tp2_filled < min_filled:
        fill = _tp2_fill_from_my_trades(pos.tp2_order_id, min_filled)
        if fill is not None:
            tp2_price, tp2_filled, tp2_fee, tp2_fee_currency = fill
            resolve_path = "fetch_my_trades"

    if tp2_price is None or tp2_filled < min_filled:
        return False

    log.info(f"TP2 확정 (path={resolve_path}) price={tp2_price} size={tp2_filled}")
    pos.tp2_hit = True
    pos.tp2_fill_price = tp2_price
    pos.tp2_fill_size = tp2_filled
    pos.tp2_fee = tp2_fee
    if tp2_fee_currency and not pos.fees_currency:
        pos.fees_currency = tp2_fee_currency
    cancel_all_orders_safe()
    dashboard_record_close(
        pos,
        "tp2_exit",
        tp2_price,
        exit_size=0.0,
        close_fee=0.0,
        fee_currency=tp2_fee_currency or pos.fees_currency,
        note="거래소 포지션 없음 상태에서 TP2 전량 종료 확인",
        order_id=pos.tp2_order_id,
    )
    notify(
        f"✅ 2차 익절 체결로 전량 종료\n"
        f"{SYMBOL}\n"
        f"side={pos.side}\n"
        f"tp2={tp2_price:.1f}\n"
        f"reason={pos.entry_reason}\n"
        f"bucket={pos.leader_bucket}\n"
        f"{pos.leader_details}"
    )
    return True

def poll_position_events(pos: "Position") -> Optional["Position"]:
    """5초 폴링용 경량 포지션 이벤트 감지: SL/TP1/TP2 체결 즉시 반영."""
    exch_pos = get_exchange_position()

    # 1. 포지션 소멸 → SL/청산 감지
    if exch_pos is None:
        if finalize_missing_exchange_position(pos):
            save_state(None)
            return None
        cp, cs, cf, cfc = resolve_order_fill(pos.sl_order_id, None, pos.size_remaining)
        dashboard_record_close(pos, "exchange_position_missing", cp, exit_size=cs, close_fee=cf,
                               fee_currency=cfc, note="폴링 중 포지션 없음 감지", order_id=pos.sl_order_id)
        notify(
            f"포지션 종료 감지(폴링)\n"
            f"{SYMBOL}\n"
            f"side={pos.side}\n"
            f"entry={pos.entry_price:.1f}\n"
            f"reason={pos.entry_reason}\n"
            f"bucket={pos.leader_bucket}\n"
            f"{pos.leader_details}"
        )
        cancel_all_orders_safe()
        save_state(None)
        return None

    current_size = round_qty(abs(float(exch_pos.get("contracts", 0) or 0)))

    # 2. TP1 미체결 → TP1 감지
    if not pos.tp1_hit:
        if current_size <= 0.001:
            return pos  # 다음 폴에서 exch_pos is None 경로로 처리
        tp1_target = position_size_after_tp1(pos)
        tp1_filled = position_size_reached_target(
            current_size,
            tp1_target,
            pos.tp1_order_id,
            pos.tp1_size,
        )
        if tp1_filled:
            pos = handle_tp1_fill(pos)
            if pos is None:
                save_state(None)
        return pos

    # 3. TP1 체결, TP2 미체결 → TP2 감지
    if not pos.tp2_hit:
        tp2_target = position_size_after_tp2(pos)
        tp2_filled = position_size_reached_target(
            current_size,
            tp2_target,
            pos.tp2_order_id,
            pos.tp2_size,
        )
        if tp2_filled:
            tp1_price = pos.tp1_fill_price if pos.tp1_fill_price is not None else pos.tp1_price
            tp2_price, tp2_size, tp2_fee, tp2_fc = resolve_order_fill(pos.tp2_order_id, pos.tp2_price, pos.tp2_size)
            pos.tp2_hit = True
            pos.size_remaining = current_size
            pos.tp2_fill_price = tp2_price
            pos.tp2_fill_size = tp2_size
            pos.tp2_fee = tp2_fee
            if tp2_fc and not pos.fees_currency:
                pos.fees_currency = tp2_fc

            if current_size <= 0.001:
                cancel_all_orders_safe()
                dashboard_record_close(pos, "tp2_exit", tp2_price, exit_size=0.0, close_fee=0.0,
                                       fee_currency=tp2_fc or pos.fees_currency,
                                       note="폴링 중 2차 익절 전량 종료", order_id=pos.tp2_order_id)
                notify(
                    f"2차 익절 전량 종료(폴링)\n"
                    f"{SYMBOL}\n"
                    f"side={pos.side}\n"
                    f"tp2={tp2_price:.1f}\n"
                    f"reason={pos.entry_reason}\n"
                    f"bucket={pos.leader_bucket}\n"
                    f"{pos.leader_details}"
                )
                save_state(None)
                return None

            pos = replace_hard_stop(pos, tp1_price, "TP2 후 1차익절가 이동(폴링)", link_id="MOVE_SL")
            if pos is None:
                save_state(None)
                return None
            pos.trail_active = current_size > 0.001
            # trail_extreme은 다음 봉 manage_position에서 초기화
            notify(
                f"2차 익절 체결(폴링)\n"
                f"{SYMBOL}\n"
                f"side={pos.side}\n"
                f"tp2={tp2_price:.1f}\n"
                f"remain={pos.size_remaining} BTC\n"
                f"trail=ON\n"
                f"reason={pos.entry_reason}\n"
                f"bucket={pos.leader_bucket}\n"
                f"{pos.leader_details}"
            )
            partial_gross = 0.0
            if pos.tp1_fill_price is not None and pos.tp1_fill_size is not None:
                partial_gross += calc_leg_pnl(pos.side, pos.entry_price, pos.tp1_fill_price, pos.tp1_fill_size)
            partial_gross += calc_leg_pnl(pos.side, pos.entry_price, tp2_price, tp2_size)
            partial_fees = estimate_total_fees(pos, 0.0)
            with open_dashboard_db() as conn:
                dashboard_upsert_trade(pos, status="partially_closed", gross_pnl=partial_gross,
                                       fees_total=partial_fees, fees_currency=pos.fees_currency,
                                       realized_pnl=partial_gross - partial_fees,
                                       notes="TP2 체결(폴링), 트레일 시작", conn=conn)
                dashboard_append_event(pos, "tp2", price=tp2_price, size=tp2_size,
                                       pnl_delta=calc_leg_pnl(pos.side, pos.entry_price, tp2_price, tp2_size) - abs(tp2_fee or 0.0),
                                       fee_amount=abs(tp2_fee or 0.0), fee_currency=tp2_fc or pos.fees_currency,
                                       note="폴링 중 2차 익절 후 트레일링", order_id=pos.tp2_order_id,
                                       event_key=f"{pos.trade_id}:tp2", conn=conn)
                conn.commit()
            save_state(pos)

    return pos


def position_max_hold_limit(pos: Position) -> int:
    return pos.max_hold_bars if pos.max_hold_bars > 0 else MAX_HOLD_BARS


def position_max_hold_due(pos: Position) -> bool:
    return (
        (not pos.tp1_hit or pos.max_hold_after_tp1)
        and pos.bars_held >= position_max_hold_limit(pos)
    )


def manage_position(pos: Position, df_15m: pd.DataFrame) -> Optional[Position]:
    global STRATEGY9_COOLDOWN_UNTIL
    if len(df_15m) < 2:
        return pos

    latest_bar = df_15m.iloc[-2]
    hi = float(latest_bar["high"])
    lo = float(latest_bar["low"])
    atr = float(latest_bar["atr"]) if not pd.isna(latest_bar.get("atr", np.nan)) else pos.entry_atr

    exch_pos = get_exchange_position()
    if exch_pos is None:
        if finalize_missing_exchange_position(pos):
            return None
        close_price, close_size, close_fee, close_fee_currency = resolve_order_fill(pos.sl_order_id, None, pos.size_remaining)
        dashboard_record_close(
            pos,
            "exchange_position_missing",
            close_price,
            exit_size=close_size,
            close_fee=close_fee,
            fee_currency=close_fee_currency,
            note="거래소 포지션 없음 → getClosedPnl 기준 PnL 기록",
            order_id=pos.sl_order_id,
        )
        notify(
            f"📭 거래소 포지션 없음 -> 상태 종료\n"
            f"{SYMBOL}\n"
            f"side={pos.side}\n"
            f"entry={pos.entry_price:.1f}\n"
            f"reason={pos.entry_reason}\n"
            f"bucket={pos.leader_bucket}\n"
            f"{pos.leader_details}"
        )
        cancel_all_orders_safe()
        return None

    pos.bars_held += 1
    max_hold_bars = position_max_hold_limit(pos)
    if position_max_hold_due(pos):
        exch_pos = get_exchange_position()
        if exch_pos is None:
            cancel_all_orders_safe()
            return None

        current_size = round_qty(abs(float(exch_pos.get("contracts", 0) or 0)))
        cancel_all_orders_safe()
        close_order = place_reduce_market_close(pos.side, current_size, f"MAX_HOLD_{max_hold_bars}")
        if close_order is not None:
            close_order_id = str(close_order.get("id", ""))
            close_price, close_size, close_fee, close_fee_currency = resolve_order_fill(close_order_id, float(latest_bar["close"]), current_size)
            dashboard_record_close(
                pos,
                f"max_hold_{max_hold_bars}",
                close_price,
                exit_size=close_size,
                close_fee=close_fee,
                fee_currency=close_fee_currency,
                note="최대 보유 바 수 도달로 강제청산",
                order_id=close_order_id,
            )
            notify(
                f"⏰ MAX_HOLD 강제청산\n"
                f"{SYMBOL}\n"
                f"side={pos.side}\n"
                f"size={current_size} BTC\n"
                f"bars={pos.bars_held}\n"
                f"reason={pos.entry_reason}\n"
                f"bucket={pos.leader_bucket}\n"
                f"{pos.leader_details}"
            )
            if pos.entry_reason == "4h_cloud_doji_short":
                latest_close_time = pd.Timestamp(latest_bar["timestamp"]) + pd.to_timedelta(timeframe_to_ms(ENTRY_TF), unit="ms")
                STRATEGY9_COOLDOWN_UNTIL = latest_close_time + pd.Timedelta(hours=4 * COOLDOWN_AFTER_MAXHOLD_4H)
                log.info(
                    f"strategy9 cooldown 설정 | until={STRATEGY9_COOLDOWN_UNTIL.isoformat()} "
                    f"after max_hold exit"
                )
            return None

        notify(
            f"⚠️ MAX_HOLD 강제청산 주문 실패\n"
            f"{SYMBOL}\n"
            f"side={pos.side}\n"
            f"size={current_size} BTC\n"
            f"bars={pos.bars_held}\n"
            f"다음 루프에서 재시도"
        )
        return pos

    pos = sync_tp_state_from_exchange(pos, latest_bar)
    if pos is None:
        return None

    exch_pos = get_exchange_position()
    if exch_pos is None:
        close_price, close_size, close_fee, close_fee_currency = resolve_order_fill(pos.sl_order_id, None, pos.size_remaining)
        dashboard_record_close(
            pos,
            "exchange_position_missing",
            close_price,
            exit_size=close_size,
            close_fee=close_fee,
            fee_currency=close_fee_currency,
            note="TP 동기화 후 포지션 없음 → getClosedPnl 기준 PnL 기록",
            order_id=pos.sl_order_id,
        )
        cancel_all_orders_safe()
        return None

    current_size = round_qty(abs(float(exch_pos.get("contracts", 0) or 0)))
    pos.size_remaining = current_size

    if pos.trail_active and current_size > 0:
        if pos.trail_extreme is None:
            pos.trail_extreme = hi if pos.side == "long" else lo

        if pos.side == "long":
            pos.trail_extreme = max(pos.trail_extreme, hi)
            trail_stop = pos.trail_extreme - atr * TRAIL_ATR_MULT

            if lo <= trail_stop:
                time.sleep(0.5)
                exch_pos = get_exchange_position()
                if exch_pos is None:
                    notify("ℹ️ 트레일 직전 재조회 결과: 이미 포지션 종료됨")
                    cancel_all_orders_safe()
                    return None

                current_size = round_qty(abs(float(exch_pos.get("contracts", 0) or 0)))
                cancel_all_orders_safe()
                close_order = place_reduce_market_close(pos.side, current_size, f"TRAIL_{trail_stop:.1f}")
                if close_order is not None:
                    pos.trail_close_fail_count = 0
                    close_order_id = str(close_order.get("id", ""))
                    close_price, close_size, close_fee, close_fee_currency = resolve_order_fill(close_order_id, trail_stop, current_size)
                    dashboard_record_close(
                        pos,
                        "trail_exit",
                        close_price,
                        exit_size=close_size,
                        close_fee=close_fee,
                        fee_currency=close_fee_currency,
                        note="롱 포지션 트레일링 청산",
                        order_id=close_order_id,
                    )
                    notify(
                        f"📉 트레일링 청산\n"
                        f"{SYMBOL}\n"
                        f"side=long\n"
                        f"size={current_size} BTC\n"
                        f"trail_stop={trail_stop:.1f}\n"
                        f"extreme={pos.trail_extreme:.1f}\n"
                        f"reason={pos.entry_reason}\n"
                        f"bucket={pos.leader_bucket}\n"
                        f"{pos.leader_details}"
                    )
                    return None
                else:
                    pos.trail_close_fail_count += 1
                    msg = (
                        f"⚠️ 트레일링 청산 주문 실패\n"
                        f"{SYMBOL}\n"
                        f"side=long\n"
                        f"trail_stop={trail_stop:.1f}\n"
                        f"연속 실패: {pos.trail_close_fail_count}회\n"
                        f"다음 루프에서 재시도"
                    )
                    if pos.trail_close_fail_count >= 3:
                        msg += f"\n🚨 트레일 청산 {pos.trail_close_fail_count}회 연속 실패. 즉시 수동 확인 필요"
                    notify(msg)
            else:
                # 트레일 조건 미달 → 연속 실패 카운트 리셋
                pos.trail_close_fail_count = 0

        else:
            pos.trail_extreme = min(pos.trail_extreme, lo)
            trail_stop = pos.trail_extreme + atr * TRAIL_ATR_MULT

            if hi >= trail_stop:
                time.sleep(0.5)
                exch_pos = get_exchange_position()
                if exch_pos is None:
                    notify("ℹ️ 트레일 직전 재조회 결과: 이미 포지션 종료됨")
                    cancel_all_orders_safe()
                    return None

                current_size = round_qty(abs(float(exch_pos.get("contracts", 0) or 0)))
                cancel_all_orders_safe()
                close_order = place_reduce_market_close(pos.side, current_size, f"TRAIL_{trail_stop:.1f}")
                if close_order is not None:
                    pos.trail_close_fail_count = 0
                    close_order_id = str(close_order.get("id", ""))
                    close_price, close_size, close_fee, close_fee_currency = resolve_order_fill(close_order_id, trail_stop, current_size)
                    dashboard_record_close(
                        pos,
                        "trail_exit",
                        close_price,
                        exit_size=close_size,
                        close_fee=close_fee,
                        fee_currency=close_fee_currency,
                        note="숏 포지션 트레일링 청산",
                        order_id=close_order_id,
                    )
                    notify(
                        f"📈 트레일링 청산\n"
                        f"{SYMBOL}\n"
                        f"side=short\n"
                        f"size={current_size} BTC\n"
                        f"trail_stop={trail_stop:.1f}\n"
                        f"extreme={pos.trail_extreme:.1f}\n"
                        f"reason={pos.entry_reason}\n"
                        f"bucket={pos.leader_bucket}\n"
                        f"{pos.leader_details}"
                    )
                    return None
                else:
                    pos.trail_close_fail_count += 1
                    msg = (
                        f"⚠️ 트레일링 청산 주문 실패\n"
                        f"{SYMBOL}\n"
                        f"side=short\n"
                        f"trail_stop={trail_stop:.1f}\n"
                        f"연속 실패: {pos.trail_close_fail_count}회\n"
                        f"다음 루프에서 재시도"
                    )
                    if pos.trail_close_fail_count >= 3:
                        msg += f"\n🚨 트레일 청산 {pos.trail_close_fail_count}회 연속 실패. 즉시 수동 확인 필요"
                    notify(msg)
            else:
                # 트레일 조건 미달 → 연속 실패 카운트 리셋
                pos.trail_close_fail_count = 0

    return pos

# =========================
# 신호 탐지
# =========================
def detect_signal(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    now_utc: Optional[pd.Timestamp] = None,
) -> Optional[tuple]:
    closed_idx = latest_closed_15m_index(df_15m, now_utc)
    if closed_idx is None:
        return None
    signal_ts = pd.Timestamp(df_15m.iloc[closed_idx]["timestamp"])
    df_live = prepare_live_signal_frame(df_15m)
    matches = df_live.index[pd.to_datetime(df_live["timestamp"]) == signal_ts]
    if len(matches) == 0:
        return None
    return detect_signal_from_frame(df_live, df_1h, df_4h, df_1d, int(matches[-1]))


def latest_closed_15m_index(
    df_15m: pd.DataFrame,
    now_utc: Optional[pd.Timestamp] = None,
) -> Optional[int]:
    if df_15m.empty:
        return None
    now = pd.Timestamp.now(tz="UTC") if now_utc is None else pd.Timestamp(now_utc)
    timestamps = pd.to_datetime(df_15m["timestamp"])
    if timestamps.dt.tz is None:
        timestamps = timestamps.dt.tz_localize("UTC")
    elif now.tzinfo is None:
        now = now.tz_localize("UTC")
    else:
        now = now.tz_convert("UTC")
    close_times = timestamps + pd.to_timedelta(timeframe_to_ms(ENTRY_TF), unit="ms")
    closed_positions = np.flatnonzero((close_times <= now).to_numpy())
    return int(closed_positions[-1]) if len(closed_positions) else None


def record_opposing_signal_conflict(
    signal_time,
    entry_price: float,
    signals: List[Tuple[str, str, str, str, float]],
) -> None:
    conflict_id = f"CONFLICT-{pd.Timestamp(signal_time).strftime('%Y%m%dT%H%M%S')}"
    shadow = BOT1_RUNTIME.setdefault("shadow_trades", [])
    if not isinstance(shadow, list):
        shadow = []
        BOT1_RUNTIME["shadow_trades"] = shadow
    if any(item.get("conflict_id") == conflict_id for item in shadow if isinstance(item, dict)):
        return
    for signal in signals:
        side, bucket, details, reason, _ = signal
        exit_profile = get_exit_profile(bucket, reason)
        stop_profile = get_stop_profile(bucket, reason)
        shadow.append({
            "conflict_id": conflict_id,
            "signal_time": str(pd.Timestamp(signal_time)),
            "side": side,
            "bucket": bucket,
            "reason": reason,
            "details": details,
            "entry_price": float(entry_price),
            "stop_price": float(entry_price - stop_profile["sl_dist"] if side == "long" else entry_price + stop_profile["sl_dist"]),
            "tp1_price": float(entry_price + exit_profile["tp1_dist"] if side == "long" else entry_price - exit_profile["tp1_dist"]),
            "tp2_price": float(entry_price + exit_profile["tp2_dist"] if side == "long" else entry_price - exit_profile["tp2_dist"]),
            "tp1_hit": False,
            "bars": 0,
            "mae": 0.0,
            "mfe": 0.0,
            "status": "open",
            "exit_reason": None,
            "exit_price": None,
        })
    log.warning(f"반대 방향 동시 신호 취소 및 shadow 등록 | conflict_id={conflict_id}")


def update_shadow_trades(latest_bar: pd.Series) -> None:
    shadow = BOT1_RUNTIME.get("shadow_trades")
    if not isinstance(shadow, list):
        return
    hi = float(latest_bar["high"])
    lo = float(latest_bar["low"])
    close = float(latest_bar["close"])
    for trade in shadow:
        if not isinstance(trade, dict) or trade.get("status") != "open":
            continue
        entry = float(trade["entry_price"])
        side = str(trade["side"])
        trade["bars"] = int(trade.get("bars", 0)) + 1
        if side == "long":
            trade["mae"] = max(float(trade.get("mae", 0.0)), entry - lo)
            trade["mfe"] = max(float(trade.get("mfe", 0.0)), hi - entry)
            stop_hit = lo <= float(trade["stop_price"])
            tp1_hit = hi >= float(trade["tp1_price"])
            tp2_hit = hi >= float(trade["tp2_price"])
        else:
            trade["mae"] = max(float(trade.get("mae", 0.0)), hi - entry)
            trade["mfe"] = max(float(trade.get("mfe", 0.0)), entry - lo)
            stop_hit = hi >= float(trade["stop_price"])
            tp1_hit = lo <= float(trade["tp1_price"])
            tp2_hit = lo <= float(trade["tp2_price"])
        if stop_hit:
            trade["status"] = "closed"
            trade["exit_reason"] = "stop"
            trade["exit_price"] = float(trade["stop_price"])
        elif tp2_hit:
            trade["tp1_hit"] = True
            trade["status"] = "closed"
            trade["exit_reason"] = "tp2"
            trade["exit_price"] = float(trade["tp2_price"])
        elif tp1_hit:
            trade["tp1_hit"] = True
        elif (not bool(trade.get("tp1_hit"))) and int(trade["bars"]) >= MAX_HOLD_BARS:
            trade["status"] = "closed"
            trade["exit_reason"] = "max_hold"
            trade["exit_price"] = close

# =========================
# 바 대기
# =========================
def wait_for_next_bar(tf_minutes: int = 15, position: Optional["Position"] = None) -> Optional["Position"]:
    now = datetime.now(timezone.utc)
    elapsed = (now.minute % tf_minutes) * 60 + now.second
    remain = tf_minutes * 60 - elapsed
    sleep_sec = remain + LOOP_SLEEP
    log.info(f"다음 {tf_minutes}m봉 마감까지 {sleep_sec}초 대기")
    # 포지션이 있으면 5초 간격 폴링으로 SL/TP/포지션종료 즉시 감지
    if position is not None:
        deadline = time.time() + sleep_sec
        while time.time() < deadline:
            time.sleep(min(TP_POLL_SEC, max(0.1, deadline - time.time())))
            if position is None:
                break
            try:
                position = poll_position_events(position)
            except Exception as e:
                log.warning(f"포지션 폴링 오류: {e}")
        # 폴링 중 포지션 종료 시 봉 마감까지 남은 시간 대기
        if position is None:
            remaining = deadline - time.time()
            if remaining > 1:
                log.info(f"포지션 종료 감지 완료, 다음 봉까지 {remaining:.0f}초 대기")
                time.sleep(remaining)
        return position
    time.sleep(sleep_sec)
    return position

# =========================
# 신규 진입 주문 세팅
# =========================
def create_entry_bundle(
    side: str,
    entry_price: float,
    bucket: str,
    reason_key: str,
    entry_size: Optional[float] = None,
    zone_role_decision: Optional[AttenuationDecision] = None,
) -> Dict[str, Union[str, float, int, bool]]:
    actual_entry_size = POSITION_SIZE_BTC if entry_size is None else round_qty(entry_size)
    split_profile = get_split_profile(bucket, reason_key, actual_entry_size)
    execution_profile = resolve_entry_execution_profile(bucket, reason_key, zone_role_decision)
    tp1_dist = float(execution_profile["tp1_dist"])
    tp2_dist = float(execution_profile["tp2_dist"])
    sl_dist = float(execution_profile["sl_dist"])
    tp1_stop_dist = float(execution_profile["tp1_stop_dist"])
    tp1_size = split_profile["tp1_size"]
    tp2_size = split_profile["tp2_size"]

    tp1_price = entry_price + tp1_dist if side == "long" else entry_price - tp1_dist
    tp2_price = entry_price + tp2_dist if side == "long" else entry_price - tp2_dist
    sl_price = entry_price - sl_dist if side == "long" else entry_price + sl_dist

    sl_id = place_hard_stop(side, actual_entry_size, sl_price, "ENTRY_SL", link_id="ENTRY")
    tp1_id = place_reduce_limit_tp(side, tp1_size, tp1_price, "TP1", link_id="TP1")
    tp2_id = place_reduce_limit_tp(side, tp2_size, tp2_price, "TP2", link_id="TP2")

    if not tp1_id:
        log.warning(f"TP1 주문 생성 실패 side={side} price={tp1_price:.1f}")
        notify(
            f"⚠️ TP1 주문 생성 실패\n"
            f"{SYMBOL}\n"
            f"side={side}\n"
            f"tp1={tp1_price:.1f}\n"
            f"수동 주문 필요"
        )
    if not tp2_id:
        log.warning(f"TP2 주문 생성 실패 side={side} price={tp2_price:.1f}")
        notify(
            f"⚠️ TP2 주문 생성 실패\n"
            f"{SYMBOL}\n"
            f"side={side}\n"
            f"tp2={tp2_price:.1f}\n"
            f"수동 주문 필요"
        )
    if not sl_id:
        log.warning(
            f"초기 SL 주문 생성 실패 side={side} price={sl_price:.1f} "
            f"(TP 주문은 유지, 상위 로직에서 긴급청산 처리)"
        )

    return {
        "tp1_id": tp1_id,
        "tp2_id": tp2_id,
        "sl_id": sl_id,
        "tp1_price": tp1_price,
        "tp2_price": tp2_price,
        "sl_price": sl_price,
        "tp1_size": tp1_size,
        "tp2_size": tp2_size,
        "tp1_dist": tp1_dist,
        "tp2_dist": tp2_dist,
        "sl_dist": sl_dist,
        "tp1_stop_dist": tp1_stop_dist,
        "max_hold_bars": int(execution_profile["max_hold_bars"]),
        "max_hold_after_tp1": bool(execution_profile["max_hold_after_tp1"]),
        "exit_profile_name": str(execution_profile["name"]),
    }

# =========================
# 메인
# =========================
def main():
    if F_LIVE_MODE:
        from f_dca_live import assert_legacy_rollback_safe, bot_session_lock
        with bot_session_lock(F_SESSION_LOCK_FILE):
            if DCA_LIVE_MODE:
                return f_dca_live_main()
            assert_legacy_rollback_safe(sys.modules[__name__], DCA_STATE_FILE)
            return f_live_main()
    notify("🚀 바이비트 자동매매 봇 시작")
    ensure_dashboard_db()

    # 총 물량 하나에서 파생된 TP 분할의 최종 일관성 검증
    derived_tp1, derived_tp2 = derive_position_split(POSITION_SIZE_BTC)
    if (TP1_SIZE, TP2_SIZE) != (derived_tp1, derived_tp2):
        raise ValueError(
            f"position split drift: configured=({TP1_SIZE}, {TP2_SIZE}) "
            f"derived=({derived_tp1}, {derived_tp2})"
        )
    TRAIL_SIZE = round_qty(POSITION_SIZE_BTC - TP1_SIZE - TP2_SIZE)
    if TRAIL_SIZE < -POSITION_SIZE_TOLERANCE:
        raise ValueError(
            f"TP 분할 물량 초과: POSITION_SIZE_BTC({POSITION_SIZE_BTC}) "
            f"- TP1({TP1_SIZE}) - TP2({TP2_SIZE}) = {TRAIL_SIZE}"
        )
    if abs(TRAIL_SIZE) <= POSITION_SIZE_TOLERANCE:
        TRAIL_SIZE = 0.0

    log.info("=" * 70)
    log.info("바이비트 자동매매 봇 시작")
    log.info(f"심볼: {SYMBOL}")
    log.info(
        f"SIZE:{POSITION_SIZE_BTC}BTC | split=TP1 {TP1_POSITION_RATIO:.0%} "
        f"/ TP2 {TP2_POSITION_RATIO:.0%} | qty_step={POSITION_MIN_LEG_SIZE_BTC}"
    )
    if TRAIL_SIZE > 0:
        log.info(f"SL:{FIXED_SL} | TP1:{TP1_DIST}({TP1_SIZE}BTC) | TP2:{TP2_DIST}({TP2_SIZE}BTC) | Trail:{TRAIL_SIZE}BTC ATR×{TRAIL_ATR_MULT}")
    else:
        log.info(f"SL:{FIXED_SL} | TP1:{TP1_DIST}({TP1_SIZE}BTC) | TP2:{TP2_DIST}({TP2_SIZE}BTC) | Trail:OFF")
    log.info(f"HUNT_BUFFER:{HUNT_BUFFER} | WAVE_RESET:{WAVE_RESET_MOVE} | MAX_HOLD:{MAX_HOLD_BARS}")
    log.info(f"제외 셋업: {EXCLUDED_SETUPS}")
    log.info("=" * 70)

    if AUTO_SET_LEVERAGE:
        set_leverage(ENTRY_LEVERAGE)
    else:
        log.info("레버리지 자동 설정 비활성화")
    next_heartbeat_at = time.time() + HEARTBEAT_INTERVAL_SEC

    position = load_state()
    if position:
        position = ensure_position_trade_id(position, "RECOVER")
        exch_pos = get_exchange_position()
        if exch_pos is None:
            # state는 있는데 거래소 포지션 없음 → 찌꺼기 state 삭제
            close_price, close_size, close_fee, close_fee_currency = resolve_order_fill(position.sl_order_id, None, position.size_remaining)
            dashboard_record_close(
                position,
                "startup_position_missing",
                close_price,
                exit_size=close_size,
                close_fee=close_fee,
                fee_currency=close_fee_currency,
                note="봇 재시작 시 state는 있었지만 거래소 포지션이 없어 주문 체결가 조회 기준으로 정리 시도",
                order_id=position.sl_order_id,
            )
            notify(
                f"⚠️ 상태 복원 실패: 거래소 포지션 없음\n"
                f"{SYMBOL}\n"
                f"side={position.side}\n"
                f"entry={position.entry_price:.1f}\n"
                f"state 초기화 후 시작"
            )
            save_state(None)
            position = None
        else:
            notify(
                f"📂 상태 복원\n"
                f"{SYMBOL}\n"
                f"side={position.side}\n"
                f"entry={position.entry_price:.1f}\n"
                f"remain={position.size_remaining} BTC\n"
                f"SL={position.stop_price:.1f}\n"
                f"TP1={position.tp1_hit} TP2={position.tp2_hit} Trail={position.trail_active}\n"
                f"reason={position.entry_reason}\n"
                f"bucket={position.leader_bucket}\n"
                f"{position.leader_details}"
            )
            open_fees = estimate_total_fees(position, 0.0)
            with open_dashboard_db() as conn:
                dashboard_upsert_trade(
                    position,
                    status="open",
                    gross_pnl=0.0,
                    fees_total=open_fees,
                    fees_currency=position.fees_currency,
                    realized_pnl=-open_fees,
                    notes="봇 재시작 후 상태 복원",
                    conn=conn,
                )
                dashboard_append_event(
                    position,
                    "state_recovered",
                    price=position.entry_price,
                    size=position.size_remaining,
                    fee_currency=position.fees_currency,
                    note="bot_state.json 기반 포지션 복원",
                    order_id=position.entry_order_id,
                    event_key=f"{position.trade_id}:state_recovered",
                    conn=conn,
                )
                conn.commit()
            save_state(position)
    else:
        exch_pos = get_exchange_position()
        if exch_pos is not None:
            contracts = abs(float(exch_pos.get("contracts", 0) or 0))
            if contracts > 0:
                # state 없는데 거래소 포지션 있음 → 수동 개입 포지션 경고
                notify(
                    f"⚠️ 감지되지 않은 거래소 포지션 존재\n"
                    f"{SYMBOL}\n"
                    f"size={round_qty(contracts)} BTC\n"
                    f"state 없음 → 수동 확인 필요"
                )

    first_cycle = True
    while True:
        try:
            _prev_reason_pre_poll = position.entry_reason if position else None
            if first_cycle:
                first_cycle = False
                log.info("시작 직후 최신 확정봉 확인")
            else:
                position = wait_for_next_bar(15, position)

            df_15m, df_1h, df_4h, df_1d = load_data()
            df_15m = add_atr(df_15m, ATR_LEN)
            now_utc = pd.Timestamp(exchange.milliseconds(), unit="ms", tz="UTC")
            closed_idx = latest_closed_15m_index(df_15m, now_utc)
            if closed_idx is None:
                log.warning("확정된 15분봉 없음")
                continue
            latest_bar = df_15m.iloc[closed_idx]
            latest_ts = str(pd.Timestamp(latest_bar["timestamp"]))
            update_shadow_trades(latest_bar)

            # 폴링 중 포지션 종료 시 도지숏 쿨다운 적용
            if _prev_reason_pre_poll and position is None:
                log.info("포지션 청산 완료 (폴링 감지)")
                if _prev_reason_pre_poll == "4h_cloud_doji_short" and STRATEGY9_COOLDOWN_UNTIL is None:
                    ensure_strategy9_cooldown_after_exit(
                        df_15m.iloc[-2]["timestamp"],
                        "poll_exit",
                    )
                    log.info(f"strategy9 cooldown 설정 | until={STRATEGY9_COOLDOWN_UNTIL.isoformat()} after poll exit")

            if position is not None:
                position = ensure_position_signal_consumed(position, df_15m)
                prev_entry_reason = position.entry_reason
                position = manage_position(position, df_15m)
                save_state(position)

                if position is None:
                    log.info("✅ 포지션 청산 완료")
                    # SL/TP 종료 시에도 도지숏 쿨다운 설정 (재진입 방지)
                    if prev_entry_reason == "4h_cloud_doji_short" and STRATEGY9_COOLDOWN_UNTIL is None:
                        ensure_strategy9_cooldown_after_exit(
                            df_15m.iloc[-2]["timestamp"],
                            "managed_exit",
                        )
                        log.info(f"strategy9 cooldown 설정 | until={STRATEGY9_COOLDOWN_UNTIL.isoformat()} after position exit")
                else:
                    log.info(
                        f"📊 유지 | side={position.side} entry={position.entry_price:.1f} "
                        f"remain={position.size_remaining} TP1={position.tp1_hit} TP2={position.tp2_hit} "
                        f"Trail={position.trail_active} SL={position.stop_price:.1f} "
                        f"profile={position.exit_profile_name} "
                        f"bars={position.bars_held}/{position.max_hold_bars}"
                    )
                continue

            next_heartbeat_at = maybe_send_heartbeat(next_heartbeat_at, has_position=position is not None)

            if BOT1_RUNTIME.get("last_processed_closed_ts") == latest_ts:
                log.info(f"이미 처리한 확정봉 | ts={latest_ts}")
                save_state(position)
                continue

            buffer_entries = update_buffer_zone_alerts(
                df_15m,
                df_4h,
                df_1d,
                float(latest_bar["close"]),
                latest_bar["timestamp"],
                df_1h=df_1h,
            )
            if buffer_entries:
                notify(
                    format_buffer_zone_alert(
                        buffer_entries,
                        float(latest_bar["close"]),
                        latest_bar["timestamp"],
                    )
                )

            pending_signal = evaluate_pending_setups(
                df_15m,
                df_4h,
                df_1d,
                latest_bar,
                df_1h=df_1h,
            )
            early_signal = pending_signal or detect_signal(df_15m, df_1h, df_4h, df_1d, now_utc)
            doji_signal = detect_4h_cloud_doji_signal(df_15m, df_4h, closed_idx)

            if early_signal is not None and doji_signal is not None and early_signal[0] != doji_signal[0]:
                record_opposing_signal_conflict(
                    latest_bar["timestamp"],
                    float(latest_bar["close"]),
                    [early_signal, doji_signal],
                )
                BOT1_RUNTIME["last_processed_closed_ts"] = latest_ts
                save_state(position)
                notify(
                    f"⚖️ 반대 방향 동시 신호 취소\n{SYMBOL}\n"
                    f"early={early_signal[0]}:{early_signal[1]}\n"
                    f"doji={doji_signal[0]}:{doji_signal[1]}\n"
                    f"shadow 추적 시작"
                )
                continue

            signal = early_signal or doji_signal
            BOT1_RUNTIME["last_processed_closed_ts"] = latest_ts
            save_state(position)
            if signal is None:
                log.info("🔍 신호 없음")
                continue

            side, bucket, details, reason, entry_atr = signal
            entry_zones = getattr(signal, "entry_zones", ())
            signal_entry_price = float(latest_bar["close"])
            signal_id = resolve_entry_signal_id(
                df_15m,
                latest_bar["timestamp"],
                details,
                reason,
            )
            if reason in EARLY_DIVERGENCE_SIGNAL_COLUMNS:
                if not signal_id:
                    log.error(
                        f"진입 스킵: 다이버전스 신호 ID 생성 실패 | "
                        f"reason={reason} signal_bar={latest_ts}"
                    )
                    continue
                if is_signal_consumed(signal_id):
                    log.info(
                        f"진입 스킵: 이미 사용한 다이버전스 신호 | "
                        f"signal_id={signal_id} signal_bar={latest_ts}"
                    )
                    continue

            # 진입 전 거래소 포지션 재확인 (중복 진입 hard-block)
            # 동일 snapshot을 precheck_entry_balance로 전달해 중복 fetch_positions 제거.
            # 예외 시 snapshot=None → precheck는 live fallback 경로로 복귀.
            try:
                _positions_snapshot = exchange.fetch_positions([SYMBOL])
            except Exception as e:
                log.error(f"포지션 조회 오류: {type(e).__name__}: {e}")
                _positions_snapshot = None
            _blocked_existing_position = False
            for _p in (_positions_snapshot or []):
                if abs(float(_p.get("contracts", 0) or 0)) > 0:
                    log.warning(f"진입 스킵: 거래소에 이미 포지션 존재 contracts={_p.get('contracts')}")
                    _blocked_existing_position = True
                    break
            if _blocked_existing_position:
                continue

            zone_role_decision: Optional[AttenuationDecision] = None
            target_entry_size = POSITION_SIZE_BTC
            signal_close_time = pd.Timestamp(latest_bar["timestamp"]) + pd.to_timedelta(
                timeframe_to_ms(ENTRY_TF), unit="ms"
            )
            try:
                target_entry_size, zone_role_decision = resolve_target_entry_size_for_signal(
                    reason,
                    side,
                    signal_close_time,
                    df_1h,
                    df_4h,
                    df_1d,
                    entry_zones=entry_zones,
                )
            except Exception as e:
                if reason in EARLY_DIVERGENCE_SIGNAL_COLUMNS:
                    persist_zone_role_blocked_signal(signal_id)
                policy_context = (
                    f"multiplier=0 timeframe=unknown epoch=0 "
                    f"base_size={POSITION_SIZE_BTC} target_size=0 "
                    f"policy_reason={type(e).__name__}:{e}"
                )
                log.error(f"진입 스킵: zone-role fail-closed | {policy_context}")
                notify(
                    f"⛔ 다이버전스 진입 차단: zone-role 확인 실패\n"
                    f"{SYMBOL}\nside={side}\n{policy_context}"
                )
                continue
            if zone_role_decision is not None and zone_role_decision.blocked:
                persist_zone_role_blocked_signal(signal_id)
                policy_context = zone_role_decision_context(zone_role_decision, 0.0)
                log.warning(f"진입 스킵: zone-role policy block | {policy_context}")
                notify(
                    f"⛔ 다이버전스 진입 차단: zone-role policy\n"
                    f"{SYMBOL}\nside={side}\n{policy_context}"
                )
                continue
            if zone_role_decision is not None and zone_role_decision.reversal_started:
                persist_bullish_reversal_reset(
                    zone_role_decision,
                    entry_zones,
                    signal_close_time,
                )
            if zone_role_decision is not None:
                log.info(
                    f"zone-role 진입 크기 결정 side={side} "
                    f"{zone_role_decision_context(zone_role_decision, target_entry_size)}"
                )

            balance_ok, balance_context = precheck_entry_balance(
                target_entry_size,
                signal_entry_price,
                cached_positions=_positions_snapshot,
            )
            if not balance_ok:
                log.warning(
                    f"진입 스킵: 가용 잔고 부족 추정 side={side} size={target_entry_size}BTC "
                    f"{balance_context}"
                )
                notify(
                    f"❌ 진입 스킵: 가용 잔고 부족 추정\n"
                    f"{SYMBOL}\n"
                    f"side={side}\n"
                    f"size={target_entry_size} BTC\n"
                    f"{balance_context}"
                )
                continue
            log.info(
                f"진입 전 잔고 확인 side={side} size={target_entry_size}BTC {balance_context}"
            )

            if zone_role_decision is not None and zone_role_decision.quota_key:
                persist_zone_role_reservation(
                    zone_role_decision,
                    signal_id,
                    signal_close_time,
                )

            entry_order = place_entry_market(side, target_entry_size, "ENTRY")
            if entry_order is None:
                notify(
                    f"❌ 진입 실패\n"
                    f"{SYMBOL}\n"
                    f"side={side}\n"
                    f"size={target_entry_size} BTC\n"
                    f"{balance_context}"
                )
                continue

            time.sleep(POSITION_SYNC_RETRY_SEC)
            entry_order_id = str(entry_order.get("id") or "")
            filled_entry = entry_order.get("average") or entry_order.get("price")
            if filled_entry is None:
                filled_entry = get_exchange_entry_price_fallback(signal_entry_price)
            entry_price, resolved_entry_size, entry_fee, entry_fee_currency = resolve_order_fill(
                entry_order_id,
                float(filled_entry),
                target_entry_size,
            )
            trade_id = str(entry_order_id or make_order_link_id("TRADE"))
            entry_exchange_position = get_exchange_position()
            actual_entry_size = resolve_actual_entry_size(
                target_entry_size,
                resolved_entry_size,
                entry_exchange_position,
            )
            if zone_role_decision is not None and zone_role_decision.quota_key:
                persist_zone_role_consumption(
                    zone_role_decision,
                    signal_id,
                    signal_close_time,
                )
            else:
                consume_signal_id(signal_id)
                save_state(None)

            bundle = create_entry_bundle(
                side,
                entry_price,
                bucket,
                reason,
                entry_size=actual_entry_size,
                zone_role_decision=zone_role_decision,
            )

            # 초기 SL 생성 실패 → 긴급 청산
            if not bundle["sl_id"]:
                notify(
                    f"🚨 진입 후 초기 하드스탑 생성 실패\n"
                    f"{SYMBOL}\n"
                    f"side={side}\n"
                    f"entry={entry_price:.1f}\n"
                    f"reason={reason}\n"
                    f"bucket={bucket}\n"
                    f"{details}\n"
                    f"긴급 시장가 청산 시도"
                )
                cancel_all_orders_safe()

                exch_pos = get_exchange_position()
                current_size = target_entry_size
                if exch_pos is not None:
                    current_size = round_qty(abs(float(exch_pos.get("contracts", 0) or 0)))

                if current_size > 0:
                    close_order = place_reduce_market_close(side, current_size, "EMERGENCY_NO_ENTRY_SL")
                    if close_order is not None:
                        close_order_id = str(close_order.get("id", ""))
                        fallback_close_price = estimate_order_fill_price(close_order, entry_price)
                        close_price, close_size, close_fee, close_fee_currency = resolve_order_fill(close_order_id, fallback_close_price, current_size)
                        temp_position = Position(
                            side=side,
                            entry_price=entry_price,
                            entry_time=str(latest_bar["timestamp"]),
                            stop_price=float(bundle["sl_price"]),
                            size_remaining=current_size,
                            entry_size=current_size,
                            tp1_price=float(bundle["tp1_price"]),
                            tp2_price=float(bundle["tp2_price"]),
                            tp1_size=float(bundle["tp1_size"]),
                            tp2_size=float(bundle["tp2_size"]),
                            sl_dist=float(bundle["sl_dist"]),
                            tp1_dist=float(bundle["tp1_dist"]),
                            tp2_dist=float(bundle["tp2_dist"]),
                            tp1_stop_dist=float(bundle["tp1_stop_dist"]),
                            max_hold_bars=int(bundle["max_hold_bars"]),
                            max_hold_after_tp1=bool(bundle["max_hold_after_tp1"]),
                            exit_profile_name=str(bundle["exit_profile_name"]),
                            tp1_hit=False,
                            tp2_hit=False,
                            trail_active=False,
                            trail_extreme=None,
                            entry_atr=entry_atr,
                            leader_bucket=bucket,
                            leader_details=details,
                            entry_reason=f"{reason}_NO_ENTRY_SL",
                            signal_id=signal_id,
                            bars_held=0,
                            entry_fee=entry_fee,
                            fees_currency=entry_fee_currency,
                            trade_id=trade_id,
                            entry_order_id=str(entry_order.get("id", "")),
                            tp1_order_id=bundle["tp1_id"],
                            tp2_order_id=bundle["tp2_id"],
                            sl_order_id="",
                        )
                        dashboard_record_entry(temp_position, note="진입 직후 초기 SL 생성 실패")
                        dashboard_record_close(
                            temp_position,
                            "initial_sl_emergency",
                            close_price,
                            exit_size=close_size,
                            close_fee=close_fee,
                            fee_currency=close_fee_currency,
                            note="초기 하드스탑 생성 실패로 즉시 긴급청산",
                            order_id=close_order_id,
                        )
                        notify(
                            f"🆘 초기 SL 실패로 긴급청산 완료\n"
                            f"{SYMBOL}\n"
                            f"side={side}\n"
                            f"size={current_size} BTC"
                        )
                        save_state(None)
                    else:
                        notify(
                            f"❗ 초기 SL 실패 + 긴급청산 주문도 실패\n"
                            f"{SYMBOL}\n"
                            f"side={side}\n"
                            f"즉시 수동 확인 필요"
                        )
                        # 청산 실패 → 거래소 포지션이 살아있을 수 있음
                        # position 객체를 생성해 다음 루프에서 추적 유지
                        position = Position(
                            side=side,
                            entry_price=entry_price,
                            entry_time=str(latest_bar["timestamp"]),
                            stop_price=float(bundle["sl_price"]),
                            size_remaining=current_size,
                            entry_size=current_size,
                            tp1_price=float(bundle["tp1_price"]),
                            tp2_price=float(bundle["tp2_price"]),
                            tp1_size=float(bundle["tp1_size"]),
                            tp2_size=float(bundle["tp2_size"]),
                            sl_dist=float(bundle["sl_dist"]),
                            tp1_dist=float(bundle["tp1_dist"]),
                            tp2_dist=float(bundle["tp2_dist"]),
                            tp1_stop_dist=float(bundle["tp1_stop_dist"]),
                            max_hold_bars=int(bundle["max_hold_bars"]),
                            max_hold_after_tp1=bool(bundle["max_hold_after_tp1"]),
                            exit_profile_name=str(bundle["exit_profile_name"]),
                            tp1_hit=False,
                            tp2_hit=False,
                            trail_active=False,
                            trail_extreme=None,
                            entry_atr=entry_atr,
                            leader_bucket=bucket,
                            leader_details=details,
                            entry_reason=f"{reason}_NO_ENTRY_SL",
                            signal_id=signal_id,
                            bars_held=0,
                            entry_fee=entry_fee,
                            fees_currency=entry_fee_currency,
                            trade_id=trade_id,
                            entry_order_id=entry_order_id,
                            tp1_order_id=bundle["tp1_id"],
                            tp2_order_id=bundle["tp2_id"],
                            sl_order_id="",  # SL 생성 실패
                        )
                        save_state(position)
                        dashboard_record_entry(position, note="초기 SL 생성 실패 상태로 추적 유지")
                else:
                    # 거래소 포지션이 이미 없음 → state 정리
                    save_state(None)
                continue

            position = Position(
                side=side,
                entry_price=entry_price,
                entry_time=str(latest_bar["timestamp"]),
                stop_price=float(bundle["sl_price"]),
                size_remaining=actual_entry_size,
                entry_size=actual_entry_size,
                tp1_price=float(bundle["tp1_price"]),
                tp2_price=float(bundle["tp2_price"]),
                tp1_size=float(bundle["tp1_size"]),
                tp2_size=float(bundle["tp2_size"]),
                sl_dist=float(bundle["sl_dist"]),
                tp1_dist=float(bundle["tp1_dist"]),
                tp2_dist=float(bundle["tp2_dist"]),
                tp1_stop_dist=float(bundle["tp1_stop_dist"]),
                max_hold_bars=int(bundle["max_hold_bars"]),
                max_hold_after_tp1=bool(bundle["max_hold_after_tp1"]),
                exit_profile_name=str(bundle["exit_profile_name"]),
                tp1_hit=False,
                tp2_hit=False,
                trail_active=False,
                trail_extreme=None,
                entry_atr=entry_atr,
                leader_bucket=bucket,
                leader_details=details,
                entry_reason=reason,
                signal_id=signal_id,
                bars_held=0,
                entry_fee=entry_fee,
                fees_currency=entry_fee_currency,
                trade_id=trade_id,
                entry_order_id=entry_order_id,
                tp1_order_id=bundle["tp1_id"],
                tp2_order_id=bundle["tp2_id"],
                sl_order_id=bundle["sl_id"],
            )
            save_state(position)
            dashboard_record_entry(position, note="신규 진입")

            notify(format_entry_alert(
                side=side,
                entry_price=entry_price,
                entry_size=actual_entry_size,
                stop_price=bundle["sl_price"],
                tp1_price=bundle["tp1_price"],
                tp1_size=bundle["tp1_size"],
                tp2_price=bundle["tp2_price"],
                tp2_size=bundle["tp2_size"],
                reason=reason,
                bucket=bucket,
                details=details,
                signal_bar_time=latest_bar["timestamp"],
                sl_id=bundle["sl_id"],
                tp1_id=bundle["tp1_id"],
                tp2_id=bundle["tp2_id"],
                zone_role_decision=zone_role_decision,
            ))

            tp_missing = []
            if not bundle["tp1_id"]:
                tp_missing.append("TP1")
            if not bundle["tp2_id"]:
                tp_missing.append("TP2")
            if tp_missing:
                notify(
                    f"⚠️ 보호구조 불완전 진입\n"
                    f"{SYMBOL}\n"
                    f"missing={','.join(tp_missing)}\n"
                    f"수동 확인 필요"
                )

        except KeyboardInterrupt:
            notify("🛑 봇 종료")
            break
        except Exception as e:
            err = f"루프 오류: {e}"
            log.error(err, exc_info=True)
            send_telegram_chunks(build_exception_report("봇 예외 발생", e))
            time.sleep(60)

# 보유 중 반대 방향 신호를 그림자로 평가하기 위한 현재 캠페인 방향.
F_CAMPAIGN_SIDE: Optional[str] = None
F_RUNTIME: Dict[str, Any] = {
    "used_zone_waves": [],
    "last_processed_closed_ts": "",
    "state_invalid": False,
    "state_mismatch": False,
}


def f_slot_sizes() -> List[float]:
    sizes = [round_qty(F_TOTAL_SIZE_BTC * weight) for weight in F_SLOT_WEIGHTS]
    if sum(sizes) != F_TOTAL_SIZE_BTC:
        sizes[-1] = round_qty(F_TOTAL_SIZE_BTC - sum(sizes[:-1]))
    if any(size < POSITION_MIN_LEG_SIZE_BTC for size in sizes):
        raise ValueError(f"F slot size below exchange minimum: {sizes}")
    return sizes


def f_save_state(positions: List[Position]) -> None:
    payload = {
        "version": 1,
        "mode": "F",
        "total_size_btc": F_TOTAL_SIZE_BTC,
        "slot_weights": list(F_SLOT_WEIGHTS),
        "positions": [asdict(pos) for pos in positions],
        "runtime": F_RUNTIME,
    }
    tmp_path = f"{F_STATE_FILE}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, default=str, ensure_ascii=False, indent=2)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, F_STATE_FILE)


def f_load_state() -> List[Position]:
    if not os.path.exists(F_STATE_FILE):
        return []
    try:
        with open(F_STATE_FILE, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        log.error(f"F state 읽기 오류: {type(exc).__name__}: {exc}")
        notify(f"⚠️ F state 읽기 실패\npath={F_STATE_FILE}\ndetail={exc}")
        return []
    try:
        payload_valid = (
            isinstance(payload, dict)
            and payload.get("mode") == "F"
            and int(payload.get("version") or 0) == 1
            and round_qty(float(payload.get("total_size_btc") or 0.0)) == F_TOTAL_SIZE_BTC
            and tuple(float(value) for value in payload.get("slot_weights") or ()) == F_SLOT_WEIGHTS
        )
    except (TypeError, ValueError):
        payload_valid = False
    if not payload_valid:
        log.error("F state 형식 불일치")
        notify(f"⚠️ F state 형식 불일치\npath={F_STATE_FILE}")
        F_RUNTIME["state_invalid"] = True
        return []
    runtime = payload.get("runtime")
    if isinstance(runtime, dict):
        F_RUNTIME.update(runtime)
    fields = Position.__dataclass_fields__
    positions: List[Position] = []
    raw_positions = payload.get("positions") or []
    if not isinstance(raw_positions, list) or len(raw_positions) > len(F_SLOT_WEIGHTS):
        F_RUNTIME["state_invalid"] = True
        notify(f"⚠️ F state 슬롯 수 불일치\npath={F_STATE_FILE}")
        return []
    seen_slots = set()
    expected_sizes = f_slot_sizes()
    for item in raw_positions:
        if not isinstance(item, dict):
            F_RUNTIME["state_invalid"] = True
            continue
        try:
            position = Position(**{key: value for key, value in item.items() if key in fields})
            if position.slot_index in seen_slots or not 0 <= position.slot_index < len(F_SLOT_WEIGHTS):
                raise ValueError("duplicate or invalid slot_index")
            if abs(float(position.slot_weight) - F_SLOT_WEIGHTS[position.slot_index]) > 1e-9:
                raise ValueError("slot_weight does not match slot_index")
            if abs(float(position.entry_size) - expected_sizes[position.slot_index]) > POSITION_SIZE_TOLERANCE:
                raise ValueError("slot entry_size does not match POSITION_SIZE_BTC allocation")
            if not all((position.tp1_order_id, position.tp2_order_id, position.sl_order_id)):
                raise ValueError("slot protection order id missing")
            if position.size_remaining <= 0 or position.entry_price <= 0:
                raise ValueError("slot position values invalid")
            seen_slots.add(position.slot_index)
            positions.append(position)
        except TypeError as exc:
            log.error(f"F slot state 무시: {exc}")
            F_RUNTIME["state_invalid"] = True
        except (ValueError, TypeError) as exc:
            log.error(f"F slot state invalid: {exc}")
            F_RUNTIME["state_invalid"] = True
    if F_RUNTIME.get("state_invalid"):
        return []
    return positions


def f_zone_wave_key(zone: Dict, side: str) -> str:
    role_epoch = zone.get("role_epoch")
    touch_count = zone.get("entry_wave_count", zone.get("touch_count"))
    return "|".join(
        (
            str(zone.get("zone_id") or zone.get("name") or ""),
            str(-1 if role_epoch is None else role_epoch),
            side,
            str(-1 if touch_count is None else touch_count),
        )
    )


def f_order_snapshot(order_id: str) -> Tuple[str, float, Optional[Dict]]:
    if not order_id or not exchange.has.get("fetchOrder"):
        return "", 0.0, None
    try:
        order = fetch_order_record(order_id)
    except Exception as exc:
        log.warning(f"F 주문 상태 조회 실패({order_id}): {exc}")
        return "", 0.0, None
    status = str(order.get("status") or "").lower()
    try:
        filled = float(order.get("filled") or (order.get("info") or {}).get("cumExecQty") or 0.0)
    except (TypeError, ValueError):
        filled = 0.0
    return status, filled, order


def f_fetch_slot_closed_pnl(entry_time: str) -> List[Dict]:
    """Fetch every settlement page in bounded seven-day API windows."""
    start = int(pd.Timestamp(entry_time).timestamp()*1000)
    now = int(datetime.now(timezone.utc).timestamp()*1000)
    result = []
    seen_rows = set()
    while start <= now:
        end = min(start+7*24*60*60*1000-1, now)
        params = {"category": "linear", "symbol": SYMBOL.split("/")[0]+"USDT",
                  "startTime": str(start), "endTime": str(end), "limit": "100"}
        cursors = set()
        while True:
            response = exchange.private_get_v5_position_closed_pnl(params)
            if str(response.get("retCode", 0)) != "0":
                raise AccountingPending("closed-PnL API error")
            payload = response.get("result")
            if not isinstance(payload, dict) or not isinstance(payload.get("list"), list):
                raise AccountingPending("invalid closed-PnL response")
            page = payload["list"]
            for row in page:
                key = tuple(str(row.get(k) or "") for k in (
                    "orderId", "createdTime", "updatedTime", "closedSize", "cumExitValue"))
                if key not in seen_rows:
                    seen_rows.add(key)
                    result.append(row)
            cursor = str(payload.get("nextPageCursor") or "")
            if not cursor or len(page) < 100:
                break
            if cursor in cursors:
                raise AccountingPending("closed-PnL cursor repeated")
            cursors.add(cursor)
            params["cursor"] = cursor
        start = end+1
    return result


def f_order_filled(order_id: str, expected: float) -> Tuple[bool, Optional[Dict]]:
    status, filled, order = f_order_snapshot(order_id)
    if status in {"closed", "filled"} and filled + POSITION_SIZE_TOLERANCE >= expected:
        return True, order
    if order_has_confirmed_fill(order_id, expected):
        return True, order
    return False, order


def f_slot_stop_filled(pos: Position) -> Tuple[bool, Optional[Dict]]:
    done, order = f_order_filled(pos.sl_order_id, pos.size_remaining)
    if done:
        return done, order
    # TP1 and the moved stop may both execute between two polls. State still
    # carries the pre-TP1 quantity; compare the actual combined executions.
    _, sl_qty, sl_order = f_order_snapshot(pos.sl_order_id)
    if sl_qty <= 0:
        return False, order
    _, tp1_qty, _ = f_order_snapshot(pos.tp1_order_id)
    _, tp2_qty, _ = f_order_snapshot(pos.tp2_order_id)
    return abs(sl_qty+tp1_qty+tp2_qty-pos.entry_size) < 1e-8, sl_order


def f_cancel_slot_orders(pos: Position) -> None:
    for order_id in (pos.tp1_order_id, pos.tp2_order_id, pos.sl_order_id):
        cancel_order_safe(order_id)


def f_place_entry_market(side: str, size: float, label: str) -> Optional[Dict]:
    try:
        return exchange.create_order(
            SYMBOL,
            "market",
            "buy" if side == "long" else "sell",
            size,
            None,
            {"positionIdx": 0},
        )
    except Exception as exc:
        log.error(f"F 시장가 진입 오류 [{label}]: {exc}")
        return None


def f_place_reduce_limit_tp(side: str, size: float, price: float, label: str, link_id: str) -> Optional[str]:
    try:
        order = exchange.create_order(
            SYMBOL,
            "limit",
            "sell" if side == "long" else "buy",
            size,
            round(price, 1),
            {
                "reduceOnly": True,
                "timeInForce": "GTC",
                "orderLinkId": make_order_link_id(link_id),
                "positionIdx": 0,
            },
        )
        return str(order.get("id") or "") or None
    except Exception as exc:
        log.error(f"F 지정가 TP 오류 [{label}]: {exc}")
        return None


def f_place_hard_stop(side: str, size: float, trigger_price: float, label: str, link_id: str) -> Optional[str]:
    try:
        is_long = side == "long"
        order = exchange.create_order(
            SYMBOL,
            "market",
            "sell" if is_long else "buy",
            size,
            None,
            {
                "triggerPrice": round(trigger_price, 1),
                "triggerDirection": 2 if is_long else 1,
                "triggerBy": "LastPrice",
                "reduceOnly": True,
                "closeOnTrigger": True,
                "orderLinkId": make_order_link_id(link_id),
                "positionIdx": 0,
            },
        )
        return str(order.get("id") or "") or None
    except Exception as exc:
        log.error(f"F 하드스탑 오류 [{label}]: {exc}")
        return None


def f_place_reduce_market_close(side: str, size: float, label: str) -> Optional[Dict]:
    try:
        return exchange.create_order(
            SYMBOL,
            "market",
            "sell" if side == "long" else "buy",
            size,
            None,
            {"reduceOnly": True, "positionIdx": 0},
        )
    except Exception as exc:
        log.error(f"F 시장가 청산 오류 [{label}]: {exc}")
        return None


def f_create_bundle(side: str, entry_price: float, entry_size: float, atr: float, slot_index: int) -> Dict[str, Any]:
    actual_size = round_qty(entry_size)
    sl_dist = F_ATR_SL_MULT * atr
    tp1_dist = F_ATR_TP1_MULT * atr
    tp2_dist = F_ATR_TP2_MULT * atr
    tp1_stop_dist = F_TP1_STOP_RATIO * tp1_dist
    tp1_size = round_qty(actual_size * F_TP1_RATIO)
    tp2_size = round_qty(actual_size - tp1_size)
    if tp1_size < POSITION_MIN_LEG_SIZE_BTC or tp2_size < POSITION_MIN_LEG_SIZE_BTC:
        raise ValueError(f"F TP leg below exchange minimum: size={actual_size} tp1={tp1_size} tp2={tp2_size}")
    tp1_price = entry_price + tp1_dist if side == "long" else entry_price - tp1_dist
    tp2_price = entry_price + tp2_dist if side == "long" else entry_price - tp2_dist
    sl_price = entry_price - sl_dist if side == "long" else entry_price + sl_dist
    prefix = f"F{slot_index + 1}"
    sl_id = f_place_hard_stop(side, actual_size, sl_price, f"{prefix}_SL", link_id=f"{prefix}_SL")
    tp1_id = f_place_reduce_limit_tp(side, tp1_size, tp1_price, f"{prefix}_TP1", link_id=f"{prefix}_TP1")
    tp2_id = f_place_reduce_limit_tp(side, tp2_size, tp2_price, f"{prefix}_TP2", link_id=f"{prefix}_TP2")
    if not sl_id or not tp1_id or not tp2_id:
        for order_id in (sl_id, tp1_id, tp2_id):
            cancel_order_safe(order_id or "")
        raise RuntimeError(f"F 보호주문 생성 실패 slot={slot_index + 1} sl={bool(sl_id)} tp1={bool(tp1_id)} tp2={bool(tp2_id)}")
    return {
        "sl_id": sl_id,
        "tp1_id": tp1_id,
        "tp2_id": tp2_id,
        "sl_price": sl_price,
        "tp1_price": tp1_price,
        "tp2_price": tp2_price,
        "sl_dist": sl_dist,
        "tp1_dist": tp1_dist,
        "tp2_dist": tp2_dist,
        "tp1_stop_dist": tp1_stop_dist,
        "tp1_size": tp1_size,
        "tp2_size": tp2_size,
    }


def f_record_slot_executions(pos: Position, reason: str = "", closing_orders=None, allocations=None) -> Dict:
    """Write this slot's confirmed fills exactly once, never foreign orphan PnL."""
    specs = {pos.tp1_order_id: "tp1", pos.tp2_order_id: "tp2_exit"}
    for oid in closing_orders or ():
        specs.setdefault(oid, reason)
    executions = []
    for oid, kind in specs.items():
        _, filled, order = f_order_snapshot(oid)
        if filled <= 0 and oid not in (closing_orders or ()):
            if (oid == pos.tp1_order_id and pos.tp1_hit) or (oid == pos.tp2_order_id and pos.tp2_hit):
                raise AccountingPending("previously confirmed leg unavailable")
            continue
        execution = confirmed_execution(order, oid, kind)
        ratio = (allocations or {}).get(oid, 1.0)
        execution["size"] *= ratio
        execution["fee_amount"] *= ratio
        executions.append(execution)
    records = f_fetch_slot_closed_pnl(pos.entry_time)
    for row_index, row in enumerate(records):
        ratio = (allocations or {}).get(str(row.get("orderId") or ""), 1.0)
        if ratio != 1.0:
            records[row_index] = dict(row, **{key: float(row[key])*ratio for key in (
                "closedSize", "closedPnl", "openFee", "closeFee") if row.get(key) is not None})
    summary = summarize_slot(pos, executions, records, bool(reason))
    if reason and summary["source"] != "exchange_closed_pnl":
        raise AccountingPending("waiting for this slot's complete closed-PnL quantities")
    latest = max(summary["events"], key=lambda e: e["event_time"])
    from dataclasses import replace
    journal_pos = replace(pos, size_remaining=summary["size_remaining"])
    note = f"{pos.slot_index + 1}차 진입분 {exit_reason_label(reason) if reason else '1차 익절 체결'}"
    with open_dashboard_db() as conn:
        dashboard_upsert_trade(
            journal_pos, status="closed" if reason else "partially_closed",
            exit_price=latest["price"] if reason else None,
            exit_time=latest["event_time"] if reason else None,
            gross_pnl=summary["gross_pnl"], fees_total=summary["fees_total"],
            fees_currency=pos.fees_currency or "USDT", realized_pnl=summary["realized_pnl"],
            close_reason=reason, notes=note, conn=conn,
        )
        for event in summary["events"]:
            write_execution_event(conn, pos.trade_id, event, iso_utc_now(), note)
        conn.commit()
    pos.size_remaining = summary["size_remaining"]
    return summary


def f_close_slot(pos: Position, reason: str, price: Optional[float], order_id: str = "", closing_orders=None, allocations=None) -> bool:
    oids = closing_orders or [order_id]
    try:
        summary = f_record_slot_executions(pos, reason, oids, allocations)
    except Exception as exc:
        F_RUNTIME.setdefault("accounting_pending", {})[pos.trade_id] = {
            "reason": reason, "closing_orders": oids, "allocations": allocations or {}}
        raise AccountingPending(str(exc)) from exc
    F_RUNTIME.setdefault("accounting_pending", {}).pop(pos.trade_id, None)
    close_price = max(summary["events"], key=lambda e: e["event_time"])["price"]
    notify(
        f"📤 포지션 종료 ({pos.slot_index + 1}번 분할)\n"
        f"종목: {SYMBOL}\nside={pos.side}\nclose_reason={exit_reason_label(reason)}\n"
        f"총 종료 수량: {pos.entry_size} BTC\n"
        f"종료가: {_format_alert_price(close_price if close_price else price)}"
    )
    return True


def f_emergency_flatten(
    positions: List[Position],
    side: str,
    extra_size: float,
    reason: str,
) -> List[Position]:
    """Flatten the aggregate one-way position after a protection failure."""
    cancel_all_orders_safe()
    try:
        exchange_position = f_exchange_position_strict()
    except Exception as exc:
        log.error(f"F 긴급 종료 포지션 조회 실패: {exc}")
        return positions
    if exchange_position is None:
        notify(
            f"🚨 긴급 종료할 포지션이 없습니다\n종목: {SYMBOL}\nreason={reason}\n"
            "거래소에 남은 포지션이 없어 추가 조치를 하지 않았습니다."
        )
        return []
    try:
        aggregate_size = round_qty(abs(float(exchange_position.get("contracts", 0) or 0)))
    except (TypeError, ValueError):
        aggregate_size = 0.0
    if aggregate_size <= 0:
        return []
    close_order = f_place_reduce_market_close(side, aggregate_size, reason)
    if close_order is None:
        notify(
            f"🚨 긴급 종료 주문이 실패했습니다\n종목: {SYMBOL}\n"
            f"남은 수량: {aggregate_size} BTC\n지금 거래소에서 직접 확인해 주세요."
        )
        return positions
    close_id = str(close_order.get("id") or "")
    close_price, close_size, close_fee, close_currency = resolve_order_fill(
        close_id,
        estimate_order_fill_price(close_order, None),
        aggregate_size,
    )
    pending_positions = []
    for pos in positions:
        share = float(pos.size_remaining) / aggregate_size
        try:
            f_close_slot(pos, reason, close_price, close_id, allocations={close_id: share})
        except AccountingPending as exc:
            log.warning(f"F 긴급 종료 원장 확정 대기 slot={pos.slot_index + 1}: {exc}")
            pending_positions.append(pos)
    notify(
        f"🚨 보호 주문 실패로 전체 포지션을 종료했습니다\n종목: {SYMBOL}\n"
        f"종료 수량: {close_size or aggregate_size} BTC\nreason={reason}\n"
        f"종료가: {_format_alert_price(close_price)}"
    )
    return pending_positions


def f_sync_slot(pos: Position) -> Optional[Position]:
    pending = F_RUNTIME.get("accounting_pending", {}).get(pos.trade_id)
    if pending:
        f_close_slot(pos, pending["reason"], None, closing_orders=pending["closing_orders"],
                     allocations=pending.get("allocations"))
        return None
    # Stop is checked first when an exchange reports several reduce-only orders
    # closed in the same polling interval; this matches the replay's stop-first policy.
    sl_done, sl_order = f_slot_stop_filled(pos)
    if sl_done:
        tp1_done, _ = f_order_filled(pos.tp1_order_id, pos.tp1_size)
        if tp1_done:
            pos.tp1_hit = True
        f_cancel_slot_orders(pos)
        price = estimate_order_fill_price(sl_order, pos.stop_price)
        f_close_slot(pos, "stop_after_tp1" if pos.tp1_hit else "stop_loss", price, pos.sl_order_id)
        return None

    tp1_done, tp1_order = f_order_filled(pos.tp1_order_id, pos.tp1_size)
    tp2_done, tp2_order = f_order_filled(pos.tp2_order_id, pos.tp2_size)
    if tp2_done and not pos.tp2_hit:
        if not pos.tp1_hit and not tp1_done:
            # A TP2 fill without TP1 leaves the TP1 leg in the aggregate
            # one-way position. Do not drop the slot or leave that remainder
            # under a canceled SL; flatten the residual explicitly.
            f_cancel_slot_orders(pos)
            residual_close = f_place_reduce_market_close(
                pos.side,
                pos.tp1_size,
                f"F{pos.slot_index + 1}_TP2_OUT_OF_ORDER_FLATTEN",
            )
            if residual_close is None:
                raise RuntimeError(f"F TP2-before-TP1 residual flatten failed slot={pos.slot_index + 1}")
            residual_id = str(residual_close.get("id") or "")
            residual_price = estimate_order_fill_price(residual_close, pos.entry_price)
            f_close_slot(pos, "tp2_out_of_order_flatten", residual_price, residual_id)
            return None
        f_cancel_slot_orders(pos)
        if not pos.tp1_hit and tp1_done:
            pos.tp1_hit = True
            pos.tp1_fill_price, pos.tp1_fill_size, pos.tp1_fee, _ = resolve_order_fill(
                pos.tp1_order_id, pos.tp1_price, pos.tp1_size
            )
        pos.tp2_hit = True
        pos.tp2_fill_price, pos.tp2_fill_size, pos.tp2_fee, _ = resolve_order_fill(
            pos.tp2_order_id, pos.tp2_price, pos.tp2_size
        )
        pos.tp2_fill_size = pos.tp2_size
        f_close_slot(pos, "tp2_exit", pos.tp2_fill_price, pos.tp2_order_id)
        return None

    if tp1_done and not pos.tp1_hit:
        pos.tp1_hit = True
        pos.tp1_fill_price, pos.tp1_fill_size, pos.tp1_fee, _ = resolve_order_fill(
            pos.tp1_order_id, pos.tp1_price, pos.tp1_size
        )
        pos.tp1_fill_size = pos.tp1_size
        pos.size_remaining = round_qty(pos.entry_size - pos.tp1_size)
        cancel_order_safe(pos.sl_order_id)
        pos.stop_price = (
            pos.entry_price + pos.tp1_stop_dist
            if pos.side == "long"
            else pos.entry_price - pos.tp1_stop_dist
        )
        pos.sl_order_id = ""
        for attempt in range(3):
            pos.sl_order_id = f_place_hard_stop(
                pos.side,
                pos.size_remaining,
                pos.stop_price,
                f"F{pos.slot_index + 1}_TP1_MOVE_SL",
                link_id=f"F{pos.slot_index + 1}_MOVE_SL",
            ) or ""
            if pos.sl_order_id:
                break
            time.sleep(1.0)
        if not pos.sl_order_id:
            raise RuntimeError(f"F TP1 후 SL 재설정 실패 slot={pos.slot_index + 1}")
        try:
            f_record_slot_executions(pos)
        except Exception as exc:
            F_RUNTIME.setdefault("tp1_accounting_pending", []).append(pos.trade_id)
            raise AccountingPending(str(exc)) from exc
        notify(
            f"✅ 1차 익절 체결 ({pos.slot_index + 1}번 분할)\n"
            f"종목: {SYMBOL}\nside={pos.side}\n"
            f"익절가: {_format_alert_price(pos.tp1_fill_price or pos.tp1_price)}\n"
            f"remain={pos.size_remaining} BTC\n"
            f"손절가를 {_format_alert_price(pos.stop_price)}로 올렸습니다"
        )
    if pos.trade_id in F_RUNTIME.get("tp1_accounting_pending", []):
        try:
            f_record_slot_executions(pos)
        except Exception as exc:
            raise AccountingPending(str(exc)) from exc
        F_RUNTIME["tp1_accounting_pending"].remove(pos.trade_id)
    return pos


def f_manage_slots(positions: List[Position], latest_bar: Optional[pd.Series] = None) -> List[Position]:
    active: List[Position] = []
    for index, pos in enumerate(positions):
        try:
            synced = f_sync_slot(pos)
        except AccountingPending as exc:
            log.warning(f"F 원장 확정 대기 slot={pos.slot_index + 1}: {exc}")
            return active + positions[index:]
        except Exception as exc:
            log.error(f"F 슬롯 관리 실패 slot={pos.slot_index + 1}: {exc}", exc_info=True)
            return f_emergency_flatten(positions, pos.side, 0.0, f"F_SLOT_{pos.slot_index + 1}_MANAGE_FAILURE")
        if synced is None:
            continue
        if latest_bar is not None:
            pos.bars_held += 1
        active.append(pos)
    return active


def _zone_identity(zone: Dict[str, Any]):
    return (zone.get("zone_id") or zone.get("name"), zone.get("side"))


def f_observe_opposite_signal(side: str, matched: List[Dict], live: pd.DataFrame,
                              df_1h: pd.DataFrame, zones: List[Dict], idx: int,
                              lifecycle, status: Dict[str, Any]) -> None:
    """보유 중 반대 방향 트리거가 났을 때의 존 게이트 결과를 그림자로 기록한다.

    주문은 내지 않는다. 존 방향을 일치시킨 기존 게이트(strict)와, 존 방향을 무시한
    게이트(relaxed)를 같은 봉에서 함께 계산해 F_RUNTIME["opp_shadow"]에 누적한다.
    """
    campaign_side = F_CAMPAIGN_SIDE
    if not campaign_side or side == campaign_side:
        return
    try:
        other_filter = "resistance" if side == "long" else "support"
        other = zones_touched_in_lookback(live, df_1h, zones, idx, other_filter, lifecycle=lifecycle)
        if side == "long":
            other = exclude_long_cloud_zones(other)
        seen = {_zone_identity(zone) for zone in matched}
        relaxed = list(matched) + [zone for zone in other if _zone_identity(zone) not in seen]
        used = set(str(item) for item in F_RUNTIME.get("used_zone_waves", []))
        strict_eligible = [zone for zone in matched if f_zone_wave_key(zone, side) not in used]
        relaxed_eligible = [zone for zone in relaxed if f_zone_wave_key(zone, side) not in used]
    except Exception:
        return
    trade_id = str(F_RUNTIME.get("current_trade_id") or "")
    shadow = F_RUNTIME.setdefault("opp_shadow", {})
    if shadow.get("trade_id") != trade_id:
        shadow.clear()
        shadow.update({"trade_id": trade_id, "triggers": 0, "hits": [], "notified": False})
    shadow["triggers"] = int(shadow.get("triggers", 0)) + 1
    if not relaxed_eligible and not strict_eligible:
        return
    entry = {"bar": str(status.get("bar_time")), "side": side,
             "strict": len(strict_eligible), "relaxed": len(relaxed_eligible),
             "volume_ratio": round(float(status.get("volume_ratio") or 0.0), 2)}
    hits = list(shadow.get("hits") or [])
    hits.append(entry)
    shadow["hits"] = hits[-40:]
    gate = "strict" if strict_eligible else "relaxed"
    try:
        atr_value = float(live["atr"].iloc[idx])
        close_price = float(live["close"].iloc[idx])
    except Exception:
        return
    f_open_opp_shadow_trade(side, gate, close_price, atr_value, status.get("bar_time"), trade_id)


OPP_SHADOW_SIZE_BTC = 0.48
OPP_SHADOW_STOP_ATR = 5.0
OPP_SHADOW_TP1_ATR = 5.0
OPP_SHADOW_TP2_ATR = 10.0
OPP_SHADOW_BE_ATR = 1.25
OPP_SHADOW_TP1_RATIO = 0.30
OPP_SHADOW_MAX_TRADES = 200


def _shadow_gate_label(gate: str) -> str:
    return "존 방향 일치" if gate == "strict" else "존 방향 무시"


def _shadow_side_label(side: str) -> str:
    return "롱" if side == "long" else "숏"


def _shadow_price(value: float) -> str:
    return f"{float(value):,.1f}"


def _opp_shadow_open_trades() -> list:
    trades = F_RUNTIME.setdefault("opp_shadow_trades", [])
    if not isinstance(trades, list):
        trades = []
        F_RUNTIME["opp_shadow_trades"] = trades
    return trades


def f_open_opp_shadow_trade(side: str, gate: str, price: float, atr_value: float,
                            bar_time, campaign_id: str) -> None:
    """보유 중 반대 방향 신호를 가상 포지션으로 기록하고 체결 알림을 보낸다.

    주문은 내지 않는다. 존 방향을 일치시킨 게이트(strict)로 잡힌 신호와
    존 방향을 무시한 게이트(relaxed)에서만 잡히는 신호를 구분해 남긴다.
    """
    try:
        entry = float(price)
        atr_value = float(atr_value)
    except (TypeError, ValueError):
        return
    if entry <= 0 or atr_value <= 0:
        return
    trade_id = f"OPP-{pd.Timestamp(bar_time).strftime('%Y%m%dT%H%M%S')}-{side}" if bar_time else ""
    if not trade_id:
        return
    trades = _opp_shadow_open_trades()
    if any(str(item.get("id")) == trade_id for item in trades if isinstance(item, dict)):
        return
    sgn = 1.0 if side == "long" else -1.0
    stop = entry - sgn * OPP_SHADOW_STOP_ATR * atr_value
    trade = {
        "id": trade_id,
        "gate": "strict" if gate == "strict" else "relaxed",
        "side": side,
        "entry": entry,
        "atr": atr_value,
        "stop": stop,
        "initial_stop": stop,
        "tp1": entry + sgn * OPP_SHADOW_TP1_ATR * atr_value,
        "tp2": entry + sgn * OPP_SHADOW_TP2_ATR * atr_value,
        "be_stop": entry + sgn * OPP_SHADOW_BE_ATR * atr_value,
        "opened_bar": str(bar_time),
        "opened_at": str(pd.Timestamp.now(tz="Asia/Seoul").strftime("%m-%d %H:%M")),
        "campaign_id": str(campaign_id or ""),
        "status": "open",
        "tp1_hit": False,
        "bars": 0,
        "mfe": 0.0,
        "mae": 0.0,
        "exit_reason": "",
        "exit_price": None,
        "r": 0.0,
    }
    trades.append(trade)
    del trades[:-OPP_SHADOW_MAX_TRADES]
    notify(
        "👻 그림자 체결\n"
        f"{_shadow_gate_label(trade['gate'])}\n"
        f"{_shadow_side_label(side)} {_shadow_price(entry)}\n"
        f"가상 {OPP_SHADOW_SIZE_BTC:.2f} BTC\n"
        f"손절 {_shadow_price(stop)}\n"
        f"1차 {_shadow_price(trade['tp1'])}\n"
        f"2차 {_shadow_price(trade['tp2'])}\n"
        f"ATR {atr_value:,.1f}"
    )


def _opp_shadow_close(trade: Dict[str, Any], reason: str, price: float, r_value: float) -> None:
    trade["status"] = "closed"
    trade["exit_reason"] = reason
    trade["exit_price"] = float(price)
    trade["r"] = round(float(r_value), 2)
    pnl_usd = float(r_value) * OPP_SHADOW_STOP_ATR * float(trade["atr"]) * OPP_SHADOW_SIZE_BTC
    reason_label = {"tp2": "2차 익절", "stop": "손절", "stop_after_tp1": "본절 손절"}.get(reason, reason)
    notify(
        "👻 그림자 종료\n"
        f"{_shadow_gate_label(trade['gate'])}\n"
        f"{_shadow_side_label(trade['side'])} {_shadow_price(trade['entry'])} → {_shadow_price(price)}\n"
        f"사유 {reason_label}\n"
        f"{trade['r']:+.2f}R (가상 {pnl_usd:+,.0f} USDT)"
    )


def f_track_opp_shadow_trades(latest_bar: pd.Series) -> None:
    """그림자 가상 포지션을 봉 단위로 갱신하고 1차 익절/종료를 알린다."""
    trades = F_RUNTIME.get("opp_shadow_trades")
    if not isinstance(trades, list) or not trades:
        return
    try:
        bar_ts = str(pd.Timestamp(latest_bar["timestamp"]))
        hi = float(latest_bar["high"])
        lo = float(latest_bar["low"])
    except Exception:
        return
    if F_RUNTIME.get("opp_shadow_last_bar") == bar_ts:
        return
    F_RUNTIME["opp_shadow_last_bar"] = bar_ts
    for trade in trades:
        if not isinstance(trade, dict) or trade.get("status") != "open":
            continue
        try:
            side = str(trade["side"])
            sgn = 1.0 if side == "long" else -1.0
            stop = float(trade["stop"])
            tp1 = float(trade["tp1"])
            tp2 = float(trade["tp2"])
            entry = float(trade["entry"])
            atr_value = float(trade["atr"])
        except (KeyError, TypeError, ValueError):
            continue
        trade["bars"] = int(trade.get("bars", 0)) + 1
        if side == "long":
            trade["mfe"] = max(float(trade.get("mfe", 0.0)), hi - entry)
            trade["mae"] = max(float(trade.get("mae", 0.0)), entry - lo)
            stop_hit = lo <= stop
            tp1_hit = hi >= tp1
            tp2_hit = hi >= tp2
        else:
            trade["mfe"] = max(float(trade.get("mfe", 0.0)), entry - lo)
            trade["mae"] = max(float(trade.get("mae", 0.0)), hi - entry)
            stop_hit = hi >= stop
            tp1_hit = lo <= tp1
            tp2_hit = lo <= tp2
        stop_r = OPP_SHADOW_STOP_ATR * atr_value
        if stop_hit:
            if trade.get("tp1_hit"):
                r_value = (OPP_SHADOW_TP1_RATIO * OPP_SHADOW_TP1_ATR
                           + (1 - OPP_SHADOW_TP1_RATIO) * OPP_SHADOW_BE_ATR)
                _opp_shadow_close(trade, "stop_after_tp1", stop, r_value)
            else:
                _opp_shadow_close(trade, "stop", stop, -OPP_SHADOW_STOP_ATR)
            continue
        if tp2_hit:
            trade["tp1_hit"] = True
            r_value = (OPP_SHADOW_TP1_RATIO * OPP_SHADOW_TP1_ATR
                       + (1 - OPP_SHADOW_TP1_RATIO) * OPP_SHADOW_TP2_ATR)
            _opp_shadow_close(trade, "tp2", tp2, r_value)
            continue
        if tp1_hit and not trade.get("tp1_hit"):
            trade["tp1_hit"] = True
            trade["stop"] = float(trade["be_stop"])
            notify(
                "👻 그림자 1차 익절\n"
                f"{_shadow_gate_label(trade['gate'])}\n"
                f"{_shadow_side_label(side)} {_shadow_price(entry)} → {_shadow_price(tp1)}\n"
                "30% 익절\n"
                f"손절 {_shadow_price(trade['stop'])}로 조임\n"
                f"+{OPP_SHADOW_TP1_RATIO * OPP_SHADOW_TP1_ATR:.2f}R"
            )


def f_opp_shadow_summary() -> Dict[str, Any]:
    """그림자 가상 포지션 성적을 게이트 종류별로 집계한다."""
    trades = [t for t in (F_RUNTIME.get("opp_shadow_trades") or []) if isinstance(t, dict)]
    summary: Dict[str, Any] = {}
    for gate in ("strict", "relaxed"):
        rows = [t for t in trades if t.get("gate") == gate]
        closed = [t for t in rows if t.get("status") == "closed"]
        summary[gate] = {
            "opened": len(rows),
            "closed": len(closed),
            "open": len(rows) - len(closed),
            "wins": sum(1 for t in closed if float(t.get("r") or 0) > 0),
            "losses": sum(1 for t in closed if float(t.get("r") or 0) < 0),
            "r_sum": round(sum(float(t.get("r") or 0) for t in closed), 2),
        }
    return summary


def f_detect_signal(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    closed_idx: int,
    diagnostics: Optional[Dict[str, Any]] = None,
    context_snapshot=None,
    indicator_only: bool = False,
) -> Optional[EntrySignal]:
    status = diagnostics if diagnostics is not None else {}
    status.clear()
    status["stage"] = "warming_up"
    if closed_idx < 55:
        return None
    live = df_15m.iloc[: closed_idx + 1].copy().reset_index(drop=True)
    idx = len(live) - 1
    current = live.iloc[idx]
    previous = live.iloc[idx - 1]
    atr = float(current.get("atr", 0.0) or 0.0)
    status.update({
        "bar_time": str(current.get("timestamp", "")),
        "close": float(current.get("close", 0.0) or 0.0),
        "atr": atr,
    })
    if not math.isfinite(atr) or atr <= 0:
        status["stage"] = "invalid_atr"
        return None
    volume_base = float(live["volume"].rolling(20).median().iloc[idx])
    if not math.isfinite(volume_base) or volume_base <= 0:
        status["stage"] = "invalid_volume"
        return None
    volume_ratio = float(current["volume"]) / volume_base
    bullish = float(current["close"]) >= float(current["open"])
    bearish = float(current["close"]) < float(current["open"])
    close = live["close"].astype(float)
    ema20 = close.ewm(span=20, adjust=False).mean()
    ema50 = close.ewm(span=50, adjust=False).mean()
    ema20_up = float(previous["close"]) <= float(ema20.iloc[idx - 1]) and float(current["close"]) > float(ema20.iloc[idx])
    ema50_up = float(previous["close"]) <= float(ema50.iloc[idx - 1]) and float(current["close"]) > float(ema50.iloc[idx])
    ema20_down = float(previous["close"]) >= float(ema20.iloc[idx - 1]) and float(current["close"]) < float(ema20.iloc[idx])
    ema50_down = float(previous["close"]) >= float(ema50.iloc[idx - 1]) and float(current["close"]) < float(ema50.iloc[idx])
    long_trigger = volume_ratio >= 1.5 and bullish and (ema20_up or ema50_up)
    short_trigger = volume_ratio >= 1.5 and bearish and (ema20_down or ema50_down)
    try:
        rsi_now = float(rsi(close).iloc[idx])
    except Exception:
        rsi_now = None
    status.update({
        "stage": "indicator_wait",
        "volume_ratio": volume_ratio,
        "volume_ready": volume_ratio >= 1.5,
        "ema20": float(ema20.iloc[idx]),
        "ema50": float(ema50.iloc[idx]),
        "ema_cross": bool(ema20_up or ema50_up or ema20_down or ema50_down),
        "ema20_up": bool(ema20_up),
        "ema50_up": bool(ema50_up),
        "ema20_down": bool(ema20_down),
        "ema50_down": bool(ema50_down),
        "rsi": rsi_now if rsi_now is not None and math.isfinite(rsi_now) else None,
        "direction": "long" if long_trigger else "short" if short_trigger else "",
    })
    if long_trigger == short_trigger:
        return None

    status["stage"] = "indicator_trigger"
    if indicator_only:
        return None

    close_time = pd.Timestamp(current["timestamp"]) + pd.to_timedelta(timeframe_to_ms(ENTRY_TF), unit="ms")
    lifecycle = None
    if context_snapshot is not None:
        frames = {"15m": df_15m, "1h": df_1h, "4h": df_4h, "1d": df_1d}
        if not context_snapshot.matches(frames, close_time, sys.modules[__name__]):
            raise ValueError("signal context revision mismatch")
        zones = context_snapshot.zones
        lifecycle = (context_snapshot.selected, context_snapshot.exhausted)
    else:
        role_context = reconstruct_zone_role_context_cached(df_1h, df_4h, df_1d, close_time)
        zones = get_active_zones(
            live, df_4h, df_1d, current["timestamp"], df_1h=df_1h,
            zone_role_context=role_context, role_visible_at=close_time,
        )
    side = "long" if long_trigger else "short"
    side_filter = "support" if side == "long" else "resistance"
    matched = (zones_touched_in_lookback(live, df_1h, zones, idx, side_filter)
               if lifecycle is None else
               zones_touched_in_lookback(live, df_1h, zones, idx, side_filter, lifecycle=lifecycle))
    if side == "long":
        matched = exclude_long_cloud_zones(matched)
    f_observe_opposite_signal(side, matched, live, df_1h, zones, idx, lifecycle, status)
    used = set(str(item) for item in F_RUNTIME.get("used_zone_waves", []))
    eligible = [zone for zone in matched if f_zone_wave_key(zone, side) not in used]
    status.update({
        "matched_zones": len(matched),
        "eligible_zones": len(eligible),
        "stage": "zone_ready" if eligible else "no_eligible_zone",
    })
    if not eligible:
        return None
    zone = eligible[0]
    bucket = "overlap" if len(matched) > 1 else (
        f"{zone['tf']}_ob" if zone.get("kind") == "ob" else f"{zone['tf']}_cloud"
    )
    zone_key = f_zone_wave_key(zone, side)
    details = (
        f"F volume_ema_or_ema50 | volume_ratio20={volume_ratio:.2f} | "
        f"zone={zone.get('name', '')} | wave={zone_key}"
    )
    signal = EntrySignal(side, bucket, details, "f_volume_ema_or_ema50", atr, entry_zones=[zone])
    signal.f_zone_key = zone_key
    signal.f_zone_origin = format_zone_origin_candle(zone)
    signal.f_zone_range = (
        f"{_format_alert_price(min(float(zone['low']), float(zone['high'])))}"
        f"~{_format_alert_price(max(float(zone['low']), float(zone['high'])))}"
    )
    return signal


def f_record_signal_diagnostics(diagnostics: Dict[str, Any]) -> None:
    counters = F_RUNTIME.setdefault("signal_status_counts", {})
    stage = str(diagnostics.get("stage") or "unknown")
    counters[stage] = int(counters.get(stage, 0)) + 1
    if diagnostics.get("volume_ready"):
        counters["volume_ready"] = int(counters.get("volume_ready", 0)) + 1
    if diagnostics.get("ema_cross"):
        counters["ema_cross"] = int(counters.get("ema_cross", 0)) + 1
    F_RUNTIME["last_signal_diagnostics"] = dict(diagnostics)


def format_f_indicator_reaction_alert(diagnostics: Dict[str, Any]) -> str:
    direction = "상승" if diagnostics.get("direction") == "long" else "하락"
    return format_telegram_bullets(localize_telegram_message(
        "🟠 지표 신호 · 진입 보류\n"
        f"{direction} · 거래량 {float(diagnostics.get('volume_ratio') or 0.0):.2f}배\n"
        "유효 존 없음"
    ))


def format_f_signal_status_alert(positions: List[Position], now=None) -> str:
    current = pd.Timestamp.now(tz="Asia/Seoul") if now is None else pd.Timestamp(now)
    if current.tzinfo is None:
        current = current.tz_localize("UTC").tz_convert("Asia/Seoul")
    else:
        current = current.tz_convert("Asia/Seoul")
    latest = F_RUNTIME.get("last_signal_diagnostics") or {}
    counts = F_RUNTIME.get("signal_status_counts") or {}
    stage_label = {
        "warming_up": "지표 준비 중",
        "snapshot_not_ready": "존 계산 준비 중 · 진입 차단",
        "invalid_atr": "ATR 계산 대기",
        "invalid_volume": "거래량 기준 계산 대기",
        "indicator_wait": "거래량·EMA 동시 신호 대기",
        "indicator_trigger": "지표 신호 발생, 존 확인 중",
        "no_eligible_zone": "지표 신호 발생, 맞는 존 없음",
        "zone_ready": "지표·존 충족 · 주문 여부 별도 확인",
    }.get(str(latest.get("stage")), str(latest.get("stage") or "확인 중"))
    entry_check = F_RUNTIME.get("last_entry_check") or {}
    if entry_check.get("bar_time") == latest.get("bar_time"):
        if entry_check.get("result") == "expired":
            stage_label = "신호 유효시간 초과 · 진입 차단"
        elif entry_check.get("result") == "not_closed":
            stage_label = "봉 마감 시각 불일치 · 진입 차단"
    return format_telegram_bullets(localize_telegram_message(
        "📡 신호 감시 중\n"
        f"상태: {stage_label}\n"
        f"1시간: 거래량 {int(counts.get('volume_ready', 0))} · "
        f"EMA {int(counts.get('ema_cross', 0))} · "
        f"완성 신호 {int(counts.get('indicator_trigger', 0)) + int(counts.get('no_eligible_zone', 0)) + int(counts.get('zone_ready', 0))} · "
        f"만료 차단 {int(counts.get('signal_expired', 0))}\n"
        f"슬롯: {len(positions)}/{len(F_SLOT_WEIGHTS)}"
    ))


F_STATUS_INTERVAL_SEC = int(os.getenv("BOT1_F_STATUS_INTERVAL_SEC", str(4 * 3600)))
F_STATUS_MIN_GAP_SEC = int(os.getenv("BOT1_F_STATUS_MIN_GAP_SEC", "600"))
F_ZONE_TF_ORDER = (("1h", "1h"), ("4h", "4h"), ("1d", "1d"))


def _f_dashboard_zones_path() -> str:
    return os.getenv(
        "BOT1_DASHBOARD_ZONES_FILE",
        os.path.join(os.path.dirname(F_STATE_FILE), "bot1_dashboard_zones.json"),
    )


def _f_zone_touch_by_tf(side: str, price: Optional[float]) -> Dict[str, int]:
    """현재가가 버퍼(HUNT_BUFFER) 안에 닿아 있는 존만 기준봉별로 센다."""
    wanted = "support" if side == "long" else "resistance"
    counts = {tf: 0 for tf, _ in F_ZONE_TF_ORDER}
    try:
        reference = float(price) if price else None
    except (TypeError, ValueError):
        reference = None
    if reference is None or reference <= 0:
        return counts
    try:
        with open(_f_dashboard_zones_path(), "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        for zone in payload.get("zones") or []:
            tf = str(zone.get("tf") or "")
            if tf not in counts or zone.get("side") != wanted:
                continue
            try:
                low = float(zone.get("low"))
                high = float(zone.get("high"))
            except (TypeError, ValueError):
                continue
            lower, upper = min(low, high), max(low, high)
            distance = 0.0 if lower <= reference <= upper else (
                lower - reference if reference < lower else reference - upper)
            if distance <= HUNT_BUFFER:
                counts[tf] += 1
    except Exception:
        pass
    return counts


def format_f_status_block(campaign: Optional[Dict], diagnostics: Optional[Dict] = None) -> str:
    """지표 온오프와 포지션 상태를 섹션 묶음 한 화면으로 정리한다."""
    from f_dca_live import PYR_ARM_ATR
    diag = diagnostics if diagnostics is not None else (F_RUNTIME.get("last_signal_diagnostics") or {})
    # 헤더는 "지금 들고 있는 방향"만 가리킨다. 미보유면 항상 방향 미정.
    # (과거엔 진단값 direction을 폴백으로 써서, 트리거만 뜬 봉에서 숏 기준으로 튀었다)
    raw_side = str((campaign or {}).get("side") or "")
    side = raw_side if raw_side in {"long", "short"} else ""
    undetermined = not side
    side_label = "롱" if side == "long" else "숏"
    on = lambda flag: "✅" if flag else "✗"

    position: List[str] = []
    if campaign:
        stage = max(0, int(campaign.get("dca_stage") or 0))
        remaining = float(campaign.get("remaining") or 0.0)
        stop = float(campaign.get("stop") or 0.0)
        avg = float(campaign.get("inventory_average") or campaign.get("average") or 0.0)
        atr = float(campaign.get("initial_atr") or 0.0)
        locked = ""
        if avg > 0 and stop > 0 and atr > 0:
            distance = ((stop - avg) if side == "long" else (avg - stop)) / atr
            if distance > 0:
                locked = f" (평단 +{distance:.2f} ATR 잠금)"
        title = f"📊 {side_label} {remaining:.3f} BTC 보유 중"
        position.append(f"평단   {avg:,.1f}" if avg > 0 else "평단   확인 필요")
        position.append(f"손절   {stop:,.1f}{locked}" if stop > 0 else "손절   확인 필요")
        position.append(f"물타기 {stage}/2")
    else:
        title = "📊 포지션 없음 · 진입 대기"
        trigger = str(diag.get("direction") or "")
        if trigger in {"long", "short"}:
            position.append(f"이번 봉 {('롱' if trigger == 'long' else '숏')} 트리거")
            if str(diag.get("stage") or "") == "no_eligible_zone":
                position.append("존 조건 미충족으로 미진입")

    indicators: List[str] = []
    volume_ratio = diag.get("volume_ratio")
    volume_text = f"거래량 {on(diag.get('volume_ready'))}"
    if isinstance(volume_ratio, (int, float)):
        volume_text += f" {float(volume_ratio):.1f}배"
    indicators.append(volume_text)
    if undetermined:
        indicators.append(f"EMA20  상승 {on(diag.get('ema20_up'))} · 하락 {on(diag.get('ema20_down'))}")
        indicators.append(f"EMA50  상승 {on(diag.get('ema50_up'))} · 하락 {on(diag.get('ema50_down'))}")
    else:
        indicators.append(f"EMA20  {on(diag.get('ema20_up' if side == 'long' else 'ema20_down'))}")
        indicators.append(f"EMA50  {on(diag.get('ema50_up' if side == 'long' else 'ema50_down'))}")
    rsi_value = diag.get("rsi")
    if isinstance(rsi_value, (int, float)):
        indicators.append(f"RSI    {float(rsi_value):.0f}")

    zones: List[str] = []
    labels = {"1h": "1시간", "4h": "4시간", "1d": "일봉 "}
    if undetermined:
        long_counts = _f_zone_touch_by_tf("long", diag.get("close"))
        short_counts = _f_zone_touch_by_tf("short", diag.get("close"))
        for tf, _ in F_ZONE_TF_ORDER:
            lc, sc = long_counts.get(tf, 0), short_counts.get(tf, 0)
            zones.append(f"{labels[tf]} 롱 {lc}개 · 숏 {sc}개" if (lc or sc) else f"{labels[tf]} ✗")
    else:
        counts = _f_zone_touch_by_tf(side, diag.get("close"))
        for tf, _ in F_ZONE_TF_ORDER:
            count = counts.get(tf, 0)
            zones.append(f"{labels[tf]} ✅ {count}개" if count else f"{labels[tf]} ✗")

    pyramid: List[str] = []
    track = F_RUNTIME.get("pyr_track") or {}
    if campaign and side:
        lots = campaign.get("pyr_lots") or []
        if campaign.get("pyr_done"):
            pyramid.append("체결 완료 · 신호 해제 대기")
        elif not campaign.get("tp1_started"):
            pyramid.append("1차 익절 대기")
        else:
            fe = track.get("fav_extreme") if track.get("trade_id") == campaign.get("trade_id") else None
            avg = float(campaign.get("inventory_average") or campaign.get("average") or 0.0)
            atr = float(campaign.get("initial_atr") or 0.0)
            if fe and avg > 0 and atr > 0:
                direction = 1 if side == "long" else -1
                excursion = direction * (float(fe) - avg) / atr
                if excursion >= PYR_ARM_ATR:
                    pyramid.append("무장 · 되돌림 대기")
                else:
                    pyramid.append(f"무장까지 {PYR_ARM_ATR - excursion:.1f} ATR")
        if lots:
            pyramid.append(f"전용 익절 {len(lots)}건")

    levels: List[Tuple[str, List[str]]] = []
    if campaign:
        try:
            from f_dca_live import trigger_levels
            raw = trigger_levels(campaign, track, F_TOTAL_SIZE_BTC, f_slot_sizes(),
                                 POSITION_MIN_LEG_SIZE_BTC)
            titles = (("stop", "손절"), ("profit", "익절 주문"), ("trail", "트레일링"),
                      ("pyramid", "불타기 발동"), ("dca", "물타기 발동"))
            levels = [(title, raw[key]) for key, title in titles if raw.get(key)]
        except Exception as exc:
            log.warning("trigger levels unavailable: %s", type(exc).__name__)
    sections = [("포지션", position), ("지표", indicators), ("존 터치", zones)]
    sections += levels if levels else [("불타기", pyramid)]
    return telegram_messages.card(title, sections, telegram_messages.kst_stamp())


def f_status_key(campaign: Optional[Dict], diagnostics: Optional[Dict]) -> str:
    diag = diagnostics or {}
    side = str((campaign or {}).get("side") or diag.get("direction") or "long")
    counts = _f_zone_touch_by_tf(side, diag.get("close"))
    return "|".join([
        side,
        f"{(campaign or {}).get('trade_id', '')[:8]}",
        str(len((campaign or {}).get("entries") or [])),
        "1" if diag.get("volume_ready") else "0",
        "1" if diag.get("ema20_up") or diag.get("ema20_down") else "0",
        "1" if diag.get("ema50_up") or diag.get("ema50_down") else "0",
        ", ".join(f"{tf}:{counts.get(tf, 0)}" for tf, _ in F_ZONE_TF_ORDER),
        f"pyr:{(campaign or {}).get('pyr_done', False)}",
    ])


def maybe_send_f_status(next_status_at: float, campaign: Optional[Dict],
                        diagnostics: Optional[Dict]) -> float:
    """4시간 주기 + 지표 온오프가 바뀌는 순간 1건(최소 간격 10분)."""
    now = time.time()
    key = f_status_key(campaign, diagnostics)
    changed = F_RUNTIME.get("status_key") != key
    last_sent = float(F_RUNTIME.get("status_sent_at") or 0.0)
    due = now >= next_status_at
    if not (due or (changed and now - last_sent >= F_STATUS_MIN_GAP_SEC)):
        return next_status_at
    notify(format_f_status_block(campaign, diagnostics))
    F_RUNTIME["status_key"] = key
    F_RUNTIME["status_sent_at"] = now
    return (now + F_STATUS_INTERVAL_SEC) if due else next_status_at


def maybe_send_f_signal_status(next_status_at: float, positions: List[Position]) -> float:
    """정기 감시 알림은 보내지 않는다. 상태는 로그와 대시보드에만 남긴다."""
    now = time.time()
    if now < next_status_at:
        return next_status_at
    try:
        summary = format_f_signal_status_alert(positions).replace("\n", " | ")
    except Exception:
        summary = "상태 요약 실패"
    log.info(f"신호 감시 상태(알림 생략) | {summary}")
    F_RUNTIME["signal_status_counts"] = {}
    return now + F_SIGNAL_STATUS_INTERVAL_SEC


def f_enter_slot(
    positions: List[Position],
    signal: EntrySignal,
    latest_bar: pd.Series,
    cached_positions: Optional[List[Dict]],
) -> List[Position]:
    used_slots = {int(pos.slot_index) for pos in positions}
    free_slots = [index for index in range(len(F_SLOT_WEIGHTS)) if index not in used_slots]
    if not free_slots:
        return positions
    if positions and any(pos.side != signal[0] for pos in positions):
        log.warning("F 반대 방향 동시 포지션 차단")
        return positions
    slot_index = free_slots[0]
    slot_size = f_slot_sizes()[slot_index]
    if round_qty(sum(pos.size_remaining for pos in positions) + slot_size) > F_TOTAL_SIZE_BTC:
        log.warning("F 총 노출 한도 초과로 진입 차단")
        return positions
    ok, context = precheck_entry_balance(slot_size, float(latest_bar["close"]), cached_positions=cached_positions)
    if not ok:
        notify(
            f"⏸️ 잔고가 부족해 진입하지 않았습니다\n종목: {SYMBOL}\n"
            f"slot={slot_index + 1}\n{context}"
        )
        return positions
    entry_order = f_place_entry_market(signal[0], slot_size, f"F_SLOT_{slot_index + 1}")
    if entry_order is None:
        return positions
    time.sleep(POSITION_SYNC_RETRY_SEC)
    order_id = str(entry_order.get("id") or "")
    fill_hint = entry_order.get("average") or entry_order.get("price") or float(latest_bar["close"])
    entry_price, filled_size, entry_fee, fee_currency = resolve_order_fill(order_id, float(fill_hint), slot_size)
    actual_size = round_qty(filled_size or slot_size)
    try:
        bundle = f_create_bundle(signal[0], entry_price, actual_size, float(signal[4]), slot_index)
    except Exception as exc:
        log.error(f"F 보호주문 생성 실패: {exc}", exc_info=True)
        return f_emergency_flatten(
            positions,
            signal[0],
            actual_size,
            f"F_SLOT_{slot_index + 1}_NO_PROTECTION",
        )
    trade_id = str(order_id or make_order_link_id(f"F{slot_index + 1}_TRADE"))
    zone_key = str(getattr(signal, "f_zone_key", ""))
    if zone_key:
        used = set(str(item) for item in F_RUNTIME.setdefault("used_zone_waves", []))
        used.add(zone_key)
        F_RUNTIME["used_zone_waves"] = sorted(used)[-5000:]
    pos = Position(
        side=signal[0],
        entry_price=entry_price,
        entry_time=str(latest_bar["timestamp"]),
        stop_price=float(bundle["sl_price"]),
        size_remaining=actual_size,
        entry_size=actual_size,
        tp1_price=float(bundle["tp1_price"]),
        tp2_price=float(bundle["tp2_price"]),
        tp1_size=float(bundle["tp1_size"]),
        tp2_size=float(bundle["tp2_size"]),
        sl_dist=float(bundle["sl_dist"]),
        tp1_dist=float(bundle["tp1_dist"]),
        tp2_dist=float(bundle["tp2_dist"]),
        tp1_stop_dist=float(bundle["tp1_stop_dist"]),
        max_hold_bars=MAX_HOLD_BARS,
        max_hold_after_tp1=False,
        exit_profile_name="F_ATR_5_5_10",
        entry_atr=float(signal[4]),
        leader_bucket=signal[1],
        leader_details=signal[2],
        entry_reason=signal[3],
        signal_id=make_order_link_id(f"F{slot_index + 1}_{latest_bar['timestamp']}") ,
        entry_fee=entry_fee,
        fees_currency=fee_currency,
        trade_id=trade_id,
        entry_order_id=order_id,
        tp1_order_id=str(bundle["tp1_id"]),
        tp2_order_id=str(bundle["tp2_id"]),
        sl_order_id=str(bundle["sl_id"]),
        slot_index=slot_index,
        slot_weight=F_SLOT_WEIGHTS[slot_index],
        zone_key=zone_key,
    )
    dashboard_record_entry(pos, note=f"F slot {slot_index + 1} entry")
    notify(
        f"📥 신규 진입 ({slot_index + 1}번 분할)\n"
        f"종목: {SYMBOL}\nside={pos.side}\n"
        f"진입가: {_format_alert_price(entry_price)}\n"
        f"수량: {_format_alert_qty(actual_size)} BTC\n"
        f"entry_reason={pos.entry_reason}\nbucket={pos.leader_bucket}\n"
        f"존 범위: {getattr(signal, 'f_zone_range', '-')}\n"
        f"존 원본봉: {getattr(signal, 'f_zone_origin', '-')}\n"
        f"손절가: {_format_alert_price(pos.stop_price)}\n"
        f"1차 익절: {_format_alert_price(pos.tp1_price)} · {_format_alert_qty(pos.tp1_size)} BTC\n"
        f"2차 익절: {_format_alert_price(pos.tp2_price)} · {_format_alert_qty(pos.tp2_size)} BTC\n"
        f"진입봉: {format_kst_candle_window(latest_bar['timestamp'], ENTRY_TF)}"
    )
    return positions + [pos]


def f_wait_for_next_bar(positions: List[Position]) -> List[Position]:
    now = datetime.now(timezone.utc)
    elapsed = (now.minute % 15) * 60 + now.second
    deadline = time.time() + (15 * 60 - elapsed + LOOP_SLEEP)
    while time.time() < deadline:
        positions = f_reconcile_slots(positions)
        if f_exchange_matches_state(positions):
            positions = f_manage_slots(positions)
        else:
            F_RUNTIME["state_mismatch"] = True
        f_save_state(positions)
        time.sleep(min(TP_POLL_SEC, max(0.1, deadline - time.time())))
    return positions


def f_exchange_position_strict() -> Optional[Dict]:
    rows = exchange.fetch_positions([SYMBOL])
    if not isinstance(rows, list):
        raise ValueError("invalid position response")
    active = []
    for row in rows:
        size = abs(float(row["contracts"]))
        if not math.isfinite(size):
            raise ValueError("invalid position quantity")
        if size > 0:
            active.append(row)
    if len(active) > 1:
        raise ValueError("multiple exchange positions in one-way mode")
    return active[0] if active else None


def f_reconcile_slots(positions: List[Position]) -> List[Position]:
    """Allow known fills through the mismatch guard, never infer fills from zero."""
    if F_RUNTIME.get("state_invalid"):
        return positions
    active = []
    for pos in positions:
        if pos.trade_id in F_RUNTIME.get("tp1_accounting_pending", []):
            try:
                f_record_slot_executions(pos)
                F_RUNTIME["tp1_accounting_pending"].remove(pos.trade_id)
            except Exception as exc:
                log.warning(f"F TP1 원장 재대조 대기: {type(exc).__name__}: {exc}")
        pending = F_RUNTIME.get("accounting_pending", {}).get(pos.trade_id)
        if pending:
            try:
                f_close_slot(pos, pending["reason"], None, closing_orders=pending["closing_orders"],
                             allocations=pending.get("allocations"))
                continue
            except AccountingPending as exc:
                log.warning(f"F 종료 원장 재대조 대기: {exc}")
        active.append(pos)
    if len(active) != len(positions):
        positions = active
        F_RUNTIME["state_mismatch"] = not f_exchange_matches_state(positions)
        f_save_state(positions)
    if not positions or f_exchange_matches_state(positions):
        return positions
    from dataclasses import replace

    projected = []
    for pos in positions:
        sl_done, _ = f_slot_stop_filled(pos)
        if sl_done:
            continue
        tp1_done, _ = f_order_filled(pos.tp1_order_id, pos.tp1_size)
        tp2_done, _ = f_order_filled(pos.tp2_order_id, pos.tp2_size)
        if tp2_done and (pos.tp1_hit or tp1_done):
            continue
        remaining = pos.size_remaining
        if tp1_done and not pos.tp1_hit:
            remaining = round_qty(pos.entry_size - pos.tp1_size)
        if tp2_done:
            remaining = round_qty(pos.entry_size-pos.tp2_size)
        projected.append(replace(pos, size_remaining=remaining))
    if not f_exchange_matches_state(projected):
        projected_ids = {pos.trade_id for pos in projected}
        remaining_positions = []
        for pos in positions:
            if pos.trade_id not in projected_ids:
                try:
                    if f_sync_slot(pos) is None:
                        continue
                except AccountingPending as exc:
                    log.warning(f"F 확정 종료 원장 대기: {exc}")
            remaining_positions.append(pos)
        if len(remaining_positions) != len(positions):
            positions = remaining_positions
            f_save_state(positions)
        recovered = f_recover_manual_close(positions, projected)
        if recovered is not None:
            return recovered
        return positions
    reconciled = f_manage_slots(positions)
    F_RUNTIME["state_mismatch"] = not f_exchange_matches_state(reconciled)
    f_save_state(reconciled)
    return reconciled


def f_recover_manual_close(positions: List[Position], projected: List[Position]) -> Optional[List[Position]]:
    """Recover a uniquely owned full residual from executions, never from zero alone."""
    if len(projected) != 1 or len(positions) != 1:
        return None
    try:
        if f_exchange_position_strict() is not None:
            return None
        pos = positions[0]
        expected = float(projected[0].size_remaining)
        with open_dashboard_db() as conn:
            known = {str(oid) for row in conn.execute(
                "SELECT entry_order_id,tp1_order_id,tp2_order_id,sl_order_id FROM trades"
            ) for oid in row if oid}
            known.update(str(row[0]) for row in conn.execute(
                "SELECT order_id FROM trade_events WHERE order_id IS NOT NULL"
            ) if row[0])
            owned_manual = {str(row[0]) for row in conn.execute(
                "SELECT order_id FROM trade_events WHERE trade_id=? AND event_type='manual_close'",
                (pos.trade_id,),
            ) if row[0]}
        known.update((pos.entry_order_id, pos.tp1_order_id, pos.tp2_order_id, pos.sl_order_id))
        start_ms = int(pd.Timestamp(pos.entry_time).timestamp()*1000)
        rows = f_fetch_slot_closed_pnl(pos.entry_time)
        close_side = "Sell" if pos.side == "long" else "Buy"
        candidates = {}
        for row in rows:
            oid = str(row.get("orderId") or "")
            if (oid and (oid not in known or oid in owned_manual) and row.get("side") == close_side
                    and int(row.get("createdTime") or 0) >= start_ms):
                candidates.setdefault(oid, []).append(row)
        quantity = sum(float(r.get("closedSize") or 0) for group in candidates.values() for r in group)
        if not candidates or abs(quantity-expected) > 1e-8:
            return None
        trades = exchange.fetch_my_trades(SYMBOL, since=start_ms, limit=100)
        if len(trades) >= 100 or any(
            str(t.get("side") or "").lower() == ("buy" if pos.side == "long" else "sell")
            and str(t.get("order") or "") not in known for t in trades
        ):
            return None
        for oid, group in candidates.items():
            status, filled, order = f_order_snapshot(oid)
            qty = sum(float(r.get("closedSize") or 0) for r in group)
            if (status not in {"closed", "filled"} or abs(filled-qty) > 1e-8
                    or not order or order.get("reduceOnly") is not True):
                return None
        tp1_done, _ = f_order_filled(pos.tp1_order_id, pos.tp1_size)
        if tp1_done and not pos.tp1_hit:
            pos.tp1_hit = True
            pos.tp1_fill_price, pos.tp1_fill_size, pos.tp1_fee, _ = resolve_order_fill(
                pos.tp1_order_id, pos.tp1_price, pos.tp1_size)
        pos.size_remaining = expected
        f_close_slot(pos, "manual_close", None, closing_orders=list(candidates))
        F_RUNTIME["state_mismatch"] = False
        f_save_state([])
        return []
    except Exception as exc:
        log.warning(f"F 수동 종료 대조 보류: {type(exc).__name__}: {exc}")
        return None


def f_exchange_matches_state(positions: List[Position]) -> bool:
    if (F_RUNTIME.get("state_invalid") or F_RUNTIME.get("accounting_pending")
            or F_RUNTIME.get("tp1_accounting_pending")):
        return False
    try:
        exchange_position = f_exchange_position_strict()
    except Exception as exc:
        log.warning(f"F 포지션 대조 실패: {type(exc).__name__}: {exc}")
        return False
    if not positions:
        return exchange_position is None
    if exchange_position is None:
        return False
    try:
        actual_size = round_qty(abs(float(exchange_position.get("contracts", 0) or 0)))
    except (TypeError, ValueError):
        return False
    expected_size = round_qty(sum(float(pos.size_remaining) for pos in positions))
    if abs(actual_size - expected_size) > POSITION_SIZE_TOLERANCE:
        return False
    exchange_side = str(exchange_position.get("side") or "").lower()
    expected_sides = {str(pos.side).lower() for pos in positions}
    if len(expected_sides) != 1:
        return False
    if exchange_side and exchange_side not in expected_sides:
        return False
    return True


def f_preflight_one_way_mode() -> bool:
    try:
        exchange_positions = exchange.fetch_positions([SYMBOL])
    except Exception as exc:
        log.error(f"F one-way preflight 실패: {exc}")
        notify(
            f"⚠️ 거래소 포지션 모드를 확인할 수 없습니다\n종목: {SYMBOL}\n"
            "안전을 위해 신규 진입을 멈췄습니다."
        )
        return False
    for item in exchange_positions or []:
        position_index = item.get("positionIdx")
        if position_index is None:
            position_index = (item.get("info") or {}).get("positionIdx")
        try:
            if position_index is not None and int(position_index) != 0:
                notify(
                    f"⚠️ 원웨이 모드에서만 동작합니다\n종목: {SYMBOL}\n"
                    f"positionIdx={position_index}\n"
                    "거래소 포지션 모드를 원웨이로 바꿔 주세요. 신규 진입을 멈췄습니다."
                )
                return False
        except (TypeError, ValueError):
            notify(
                f"⚠️ 거래소 포지션 정보를 해석할 수 없습니다\n종목: {SYMBOL}\n"
                "안전을 위해 신규 진입을 멈췄습니다."
            )
            return False
    return True


F_DASHBOARD_EXPORT_LOCK = Lock()


def f_export_dashboard_zones(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    closed_idx: int,
    context_snapshot=None,
) -> None:
    """Submit one observer without waiting for reconstruction or file I/O."""
    if not F_DASHBOARD_EXPORT_LOCK.acquire(blocking=False):
        return
    try:
        captured_at = iso_utc_now()
        frames = None if context_snapshot is not None else (
            df_15m.iloc[:closed_idx + 1].copy().reset_index(drop=True),
            df_1h.copy(), df_4h.copy(), df_1d.copy(),
        )
        def publish():
            try:
                if context_snapshot is not None:
                    f_write_dashboard_zones(None, None, None, None, closed_idx, captured_at,
                                            context_snapshot=context_snapshot)
                else:
                    f_write_dashboard_zones(*frames, len(frames[0]) - 1, captured_at)
            except Exception as exc:
                log.warning("F dashboard observer worker unavailable: %s", type(exc).__name__)
            finally:
                F_DASHBOARD_EXPORT_LOCK.release()
        Thread(target=publish, name="bot1-dashboard-zones", daemon=True).start()
    except Exception as exc:
        F_DASHBOARD_EXPORT_LOCK.release()
        log.warning("F dashboard observer submission unavailable: %s", type(exc).__name__)


def f_write_dashboard_zones(
    df_15m: pd.DataFrame,
    df_1h: pd.DataFrame,
    df_4h: pd.DataFrame,
    df_1d: pd.DataFrame,
    closed_idx: int,
    captured_at: str,
    context_snapshot=None,
) -> None:
    """Publish an isolated read-only snapshot; telemetry cannot abort trading."""
    try:
        if context_snapshot is not None:
            bar_time = context_snapshot.visible_at - pd.Timedelta(minutes=15)
            zones = context_snapshot.selected
        else:
            live = df_15m.iloc[:closed_idx + 1].copy().reset_index(drop=True)
            hourly, four_hourly, daily = df_1h.copy(), df_4h.copy(), df_1d.copy()
            bar_time = pd.Timestamp(live.iloc[-1]["timestamp"])
            close_time = bar_time + pd.to_timedelta(timeframe_to_ms(ENTRY_TF), unit="ms")
            context = reconstruct_zone_role_context(
                hourly, four_hourly, daily, close_time,
                extract_order_blocks, add_atr, ATR_LEN,
                ZONE_ROLE_BREAKOUT_RULE, ZONE_ROLE_POLICY_SETTING,
            )
            zones = get_active_zones(
                live, four_hourly, daily, bar_time, df_1h=hourly,
                zone_role_context=context, role_visible_at=close_time,
            )
            zones, _ = select_entry_lifecycle_zones(live, zones, len(live) - 1)
        fields = ("zone_id", "tf", "kind", "side", "low", "high", "role_epoch", "touch_count", "active_from")
        exported = []
        for zone in zones:
            item = {key: zone.get(key) for key in fields}
            side = "long" if zone["side"] == "support" else "short"
            item["wave_key"] = f_zone_wave_key(zone, side)
            exported.append(item)
        path = os.getenv("BOT1_DASHBOARD_ZONES_FILE", os.path.join(os.path.dirname(F_STATE_FILE), "bot1_dashboard_zones.json"))
        payload = {"version": 1, "as_of": captured_at, "bar_time": str(bar_time), "zones": exported}
        with open(f"{path}.tmp", "w", encoding="utf-8") as handle:
            json.dump(payload, handle, default=str, ensure_ascii=False, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(f"{path}.tmp", path)
    except Exception as exc:
        log.warning("F dashboard zone snapshot unavailable: %s", type(exc).__name__)


def f_live_main(legacy_drain: bool = False) -> None:
    sizes_preview = f_slot_sizes()
    notify(
        "🤖 자동매매 시작\n"
        f"종목: {SYMBOL}\n"
        f"총 운용 수량: {_format_alert_qty(F_TOTAL_SIZE_BTC)} BTC\n"
        f"분할 진입: {len(sizes_preview)}회 ("
        + " / ".join(f"{_format_alert_qty(size)}" for size in sizes_preview)
        + " BTC)"
    )
    ensure_dashboard_db()
    sizes = f_slot_sizes()
    log.info(
        f"F mode | total={F_TOTAL_SIZE_BTC} BTC | slots={sizes} | "
        f"TP={F_TP1_RATIO:.0%}/{F_TP2_RATIO:.0%} | ATR=SL5 TP1=5 TP2=10 | TP1_SL=1.25ATR"
    )
    if not f_preflight_one_way_mode():
        return
    positions = f_load_state()
    positions = f_reconcile_slots(positions)
    if not f_exchange_matches_state(positions):
        notify(
            f"⚠️ 기록과 거래소 포지션이 다릅니다\n종목: {SYMBOL}\n"
            f"state_slots={len(positions)}\n"
            "자동 진입과 자동 관리를 멈췄습니다. 거래소에서 확인해 주세요."
        )
        F_RUNTIME["state_mismatch"] = True
    next_heartbeat_at = time.time() + HEARTBEAT_INTERVAL_SEC
    next_signal_status_at = time.time() + F_SIGNAL_STATUS_INTERVAL_SEC
    first_cycle = True
    while True:
        try:
            if not first_cycle:
                positions = f_wait_for_next_bar(positions)
            first_cycle = False
            df_15m, df_1h, df_4h, df_1d = load_data()
            df_15m = add_atr(df_15m, ATR_LEN)
            now_utc = pd.Timestamp(exchange.milliseconds(), unit="ms", tz="UTC")
            closed_idx = latest_closed_15m_index(df_15m, now_utc)
            if closed_idx is None:
                continue
            latest_bar = df_15m.iloc[closed_idx]
            latest_ts = str(pd.Timestamp(latest_bar["timestamp"]))
            positions = f_reconcile_slots(positions)
            if not f_exchange_matches_state(positions):
                if not F_RUNTIME.get("state_mismatch"):
                    notify(
                        f"⚠️ 기록과 거래소 포지션이 다릅니다\n종목: {SYMBOL}\n"
                        "자동 관리를 멈췄습니다. 거래소에서 확인해 주세요."
                    )
                F_RUNTIME["state_mismatch"] = True
                f_save_state(positions)
                continue
            F_RUNTIME["state_mismatch"] = False
            positions = f_manage_slots(positions, latest_bar)
            if legacy_drain:
                f_save_state(positions)
                if not positions and f_exchange_matches_state([]):
                    return
                continue
            if F_RUNTIME.get("last_processed_closed_ts") == latest_ts:
                continue
            F_RUNTIME["last_processed_closed_ts"] = latest_ts
            f_export_dashboard_zones(df_15m, df_1h, df_4h, df_1d, closed_idx)
            next_heartbeat_at = maybe_send_heartbeat(next_heartbeat_at, bool(positions))
            next_signal_status_at = maybe_send_f_signal_status(next_signal_status_at, positions)
            if len(positions) >= len(F_SLOT_WEIGHTS):
                f_save_state(positions)
                continue
            diagnostics: Dict[str, Any] = {}
            signal = f_detect_signal(
                df_15m,
                df_1h,
                df_4h,
                df_1d,
                closed_idx,
                diagnostics=diagnostics,
            )
            f_record_signal_diagnostics(diagnostics)
            if signal is None:
                if diagnostics.get("stage") == "no_eligible_zone":
                    log.info(
                        "지표 신호 발생 · 유효 존 없음(알림 생략) | bar=%s direction=%s matched=%s",
                        diagnostics.get("bar_time"), diagnostics.get("direction"),
                        diagnostics.get("matched_zones"),
                    )
                f_save_state(positions)
                continue
            try:
                snapshot = exchange.fetch_positions([SYMBOL])
            except Exception as exc:
                log.error(f"F 포지션 조회 오류: {exc}")
                snapshot = None
            positions = f_enter_slot(positions, signal, latest_bar, snapshot)
            f_save_state(positions)
        except KeyboardInterrupt:
            notify("🛑 자동매매를 종료했습니다")
            break
        except Exception as exc:
            log.error(f"F 루프 오류: {exc}", exc_info=True)
            send_telegram_chunks(build_exception_report("자동매매 처리 중 문제 발생", exc))
            time.sleep(60)


def f_dca_live_main() -> None:
    global F_RUNTIME
    from f_dca_live import DcaEngine, campaign_view
    from bot1_signal_context import ContextCalculator
    from bot1_market_loader import MarketDataLoader

    if not f_preflight_one_way_mode():
        return
    ensure_dashboard_db()
    legacy_positions = f_load_state()
    if F_RUNTIME.get("state_invalid"):
        notify("🚨 기존 F 기록을 읽을 수 없어 물타기 모드 시작을 중단했습니다")
        return
    engine = DcaEngine(sys.modules[__name__], DCA_STATE_FILE)
    try:
        engine.load()
    except Exception as exc:
        log.error(f"DCA 기록 복구 실패: {type(exc).__name__}")
        notify("🚨 물타기 기록 복구 실패 · 기존 파일을 보존하고 시작을 중단했습니다")
        return
    if legacy_positions:
        if engine.campaign or engine.state.get("pending") or engine.state.get("paused"):
            notify("🚨 기존 슬롯과 물타기 기록이 동시에 존재합니다. 수동 대조가 필요합니다")
            return
        notify("ℹ️ 기존 슬롯은 시간 제한 없이 종료 관리하며 신규 슬롯 진입은 하지 않습니다")
        f_live_main(legacy_drain=True)
        if not f_exchange_matches_state([]):
            return
        engine.state["runtime"] = F_RUNTIME
        engine.save()
    F_RUNTIME = engine.state["runtime"]
    sys.modules[__name__].F_RUNTIME = F_RUNTIME
    try:
        engine.poll()
        engine.save()
    except Exception as exc:
        log.error(f"DCA 최초 상태 대조 실패: {type(exc).__name__}")
        notify("🚨 물타기 최초 상태 대조 실패 · 신규 진입 없이 시작을 중단했습니다")
        return
    import f_dca_live as _f_dca
    # Default when no /mode has been saved yet: the BOT1_PYR_MODE setting.
    commands = telegram_commands.TelegramCommands(
        TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID,
        _f_dca.PYR_MODE if _f_dca.PYR_MODE in _f_dca.PYR_MODES else "cyclic",
        open_mode=lambda: _f_dca.campaign_mode(engine.campaign) if engine.campaign else None,
        log=log.info)
    mode_now = _f_dca.current_pyr_mode()
    notify(f"🤖 봇 가동 물타기 모드 · 불타기 {telegram_commands.MODES.get(mode_now, mode_now)}"
           f" (텔레그램 /mode 로 변경)")
    next_heartbeat = time.time() + HEARTBEAT_INTERVAL_SEC
    next_status = time.time()
    calculator = ContextCalculator(sys.modules[__name__])
    market_loader = MarketDataLoader(create_market_client, load_data, TP_POLL_SEC)
    first_cycle = True
    while True:
        try:
            if not first_cycle:
                now = datetime.now(timezone.utc)
                elapsed = (now.minute % 15) * 60 + now.second
                deadline = time.time() + (15 * 60 - elapsed + LOOP_SLEEP)
                while time.time() < deadline:
                    engine.poll()
                    commands.poll()
                    time.sleep(min(TP_POLL_SEC, max(0.1, deadline - time.time())))
            first_cycle = False
            load_started = time.perf_counter()
            loaded = market_loader.load(engine)
            if loaded is None:
                log.warning("DCA market data pending; protection polling continues, no entry")
                continue
            df_15m, df_1h, df_4h, df_1d = loaded
            df_15m = add_atr(df_15m, ATR_LEN)
            df_15m["rsi14"] = rsi(df_15m["close"])
            load_ms = (time.perf_counter() - load_started) * 1000
            now_utc = pd.Timestamp(exchange.milliseconds(), unit="ms", tz="UTC")
            closed_idx = latest_closed_15m_index(df_15m, now_utc)
            if closed_idx is None:
                continue
            latest_bar = df_15m.iloc[closed_idx]
            f_track_opp_shadow_trades(latest_bar)
            close_ms = int(pd.Timestamp(latest_bar["timestamp"]).timestamp() * 1000) + 900_000
            frames = {"15m": df_15m, "1h": df_1h, "4h": df_4h, "1d": df_1d}
            signal_started = time.perf_counter()
            key = calculator.submit(frames, pd.Timestamp(close_ms, unit="ms", tz="UTC"))
            snapshot = None
            try:
                # Keep managing owned orders while bootstrap/revisions run off-thread.
                while time.perf_counter() - signal_started < 30:
                    snapshot = calculator.result(key)
                    if snapshot is not None or key is None:
                        break
                    engine.poll()
                    time.sleep(0.1)
            except Exception as exc:
                log.error("DCA context unavailable: %s", type(exc).__name__, exc_info=True)
            diagnostics: Dict[str, Any] = {}
            if snapshot is None:
                diagnostics.update(stage="snapshot_not_ready", bar_time=str(latest_bar["timestamp"]),
                                   load_ms=round(load_ms, 3),
                                   signal_compute_ms=round((time.perf_counter() - signal_started) * 1000, 3),
                                   signal_age_ms=exchange.milliseconds() - close_ms)
                f_record_signal_diagnostics(diagnostics)
                # Existing campaigns need only indicator data for DCA; initial entries
                # still require a validated zone snapshot.
                campaign = engine.campaign
                dca_signal = None
                if campaign:
                    indicator_status = {}
                    f_detect_signal(df_15m, df_1h, df_4h, df_1d, closed_idx,
                                    indicator_status, indicator_only=True)
                    if indicator_status.get("direction") == campaign.get("side"):
                        dca_signal = EntrySignal(campaign["side"], campaign["bucket"],
                                                 "DCA volume_ema_or_ema50 (zone independent)",
                                                 "f_volume_ema_or_ema50", float(indicator_status["atr"]))
                        dca_signal.dca_zone_independent = True
                engine.on_bar(dca_signal, latest_bar)
                engine.save()
                log.warning("DCA context not ready; initial entries blocked for bar=%s", latest_bar["timestamp"])
                continue
            __campaign = engine.campaign
            __side = (__campaign or {}).get("side")
            __mod = sys.modules[__name__]
            __mod.F_CAMPAIGN_SIDE = __side if __side in {"long", "short"} else None
            F_RUNTIME["current_trade_id"] = str((__campaign or {}).get("trade_id") or "")
            signal = f_detect_signal(df_15m, df_1h, df_4h, df_1d, closed_idx, diagnostics,
                                     context_snapshot=snapshot)
            if __campaign and diagnostics.get("direction") == __side:
                signal = EntrySignal(__side, __campaign["bucket"],
                                     "DCA volume_ema_or_ema50 (zone independent)",
                                     "f_volume_ema_or_ema50", float(diagnostics["atr"]))
                signal.dca_zone_independent = True
            diagnostics["context_stats"] = dict(snapshot.stats)
            f_export_dashboard_zones(df_15m, df_1h, df_4h, df_1d, closed_idx,
                                     context_snapshot=snapshot)
            diagnostics.update({
                "load_ms": round(load_ms, 3),
                "signal_compute_ms": round((time.perf_counter() - signal_started) * 1000, 3),
                "signal_age_ms": exchange.milliseconds() - close_ms,
            })
            f_record_signal_diagnostics(diagnostics)
            log.info(
                "DCA signal evaluated | bar=%s stage=%s load_ms=%s compute_ms=%s age_ms=%s",
                diagnostics.get("bar_time"), diagnostics.get("stage"), diagnostics["load_ms"],
                diagnostics["signal_compute_ms"], diagnostics["signal_age_ms"],
            )
            engine.on_bar(signal, latest_bar)
            next_heartbeat = maybe_send_heartbeat(next_heartbeat, bool(engine.campaign))
            F_RUNTIME["signal_status_counts"] = {}
            next_status = maybe_send_f_status(
                next_status, engine.campaign, F_RUNTIME.get("last_signal_diagnostics") or {}
            )
            engine.save()
        except KeyboardInterrupt:
            market_loader.close()
            calculator.close()
            notify("🛑 봇1 물타기 모드 종료 · 거래소 보호 주문은 유지합니다")
            break
        except Exception as exc:
            log.error(f"DCA 루프 오류: {type(exc).__name__}", exc_info=True)
            time.sleep(LOOP_SLEEP)


if __name__ == "__main__":
    main()
