"""Aggregate Bot1 DCA ownership, protection transitions and confirmed accounting."""
from contextlib import contextmanager
from copy import deepcopy
from decimal import Decimal, InvalidOperation
import json
import math
import os

from cyclic_position_rules import available_size, pyramid_target
from f_slot_accounting import confirmed_execution
from telegram_messages import card, kst_stamp

def dca_limit_price(side, average, tick):
    """Return the closest exchange tick strictly adverse to the current average."""
    try:
        average, tick = Decimal(str(average)), Decimal(str(tick))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError('invalid price or tick') from exc
    if side not in ('long', 'short') or not average.is_finite() or not tick.is_finite() or average <= 0 or tick <= 0:
        raise ValueError('invalid side, price, or tick')
    for value in (average, tick):
        representation = value.as_tuple()
        if len(representation.digits) > 64 or not -64 <= representation.exponent <= 64:
            raise ValueError('price precision outside bounds')
    avg_n, avg_d = average.as_integer_ratio()
    tick_n, tick_d = tick.as_integer_ratio()
    num, den = avg_n * tick_d, avg_d * tick_n
    units = (num - 1) // den if side == 'long' else num // den + 1
    if units <= 0:
        raise ValueError('no positive permissible price')
    t = tick.as_tuple()
    coefficient = int(''.join(map(str, t.digits))) * units
    result = Decimal((0, tuple(map(int, str(coefficient))), t.exponent))
    if not (result < average if side == 'long' else result > average):
        raise ValueError('strict price boundary failed')
    return format(result, 'f')


DCA_ATR_STEPS = (1.5, 3.0)
BREAKEVEN_BUFFER_RATE = 0.0002
SIGNAL_MAX_AGE_MS = 90_000
TERMINAL_STATUSES = {"closed", "filled", "canceled", "cancelled", "rejected", "expired"}
QTY_EPS = 1e-8

# --- 추가진입(피라미딩) 그림자 모드: 주문 없이 판정만 하고 알림으로 남긴다 ---
# 백테스트(~/bybit-bot/backtest) 결론 반영: 롱 전용, +2ATR 고점 후 0.5ATR 눌림,
# RSI 65 이상 → 60 이하 식힘, 수익권 유지, 캠페인당 1회.
# 트레일 청산: 8 ATR 도달 후 극단 대비 2 ATR 되돌림이 봉 종가로 확인되면 시장가 청산.
#   손절 주문을 계속 갈아끼우지 않는다(주문 교체 리스크 회피). 기존 손절은 그대로 백스톱.
TRAIL_ARM_ATR = 8.0
TRAIL_DIST_ATR = 2.0

# 일시정지 자동복구: 일시적 네트워크 오류로 멈춘 경우 거래소 대조를 다시 시도한다.
PAUSE_RETRY_COOLDOWN_MS = 60_000
# 재시도 상한: 넘으면 수동 확인으로 승격한다(무한 재시도로 조용히 멈춰 있는 일 방지).
PAUSE_MAX_RETRY_ATTEMPTS = int(os.getenv("BOT1_PAUSE_MAX_RETRY_ATTEMPTS", "20"))
# 무포지션 상태의 조회 실패는 n봉 연속이면 중단 알림을 띄운다(그 전엔 조용히 재시도).
FLAT_READ_FAIL_STREAK = int(os.getenv("BOT1_FLAT_READ_FAIL_STREAK", "4"))
# 수동 중단이 길어질 때 재알림: 1시간 뒤 첫 알림, 이후 6시간마다.
PAUSE_NAG_FIRST_MS = 3_600_000
PAUSE_NAG_INTERVAL_MS = 6 * 3_600_000
TRANSIENT_ERROR_NAMES = {
    "RequestTimeout", "NetworkError", "ExchangeNotAvailable", "RateLimitExceeded",
    "DDoSProtection", "Timeout", "TimeoutError", "ConnectionError", "ConnectionResetError",
}

SHADOW_ENABLED = False      # 그림자 로깅(주문 없음) — 실주문 도입으로 끔
PYR_ENABLED = True         # 롱/숏 순환 불타기(추가진입) 실주문
# 수량: 동시 보유 한도(F_TOTAL_SIZE_BTC) − 현재 보유량의 남은 여유 전부.
PYR_ARM_ATR = 4.0          # 고점이 평단 +4ATR 이상 (2ATR은 눌림목으로 보기엔 너무 짧음)
PYR_PULL_ATR = 2.0         # 고점 대비 2ATR 눌림 (전용 익절까지 최소 1.8ATR 확보)
PYR_MIN_CUSHION_ATR = 0.5  # 여전히 평단 +0.5ATR 위
PYR_RSI_PEAK = 65.0        # RSI 65 이상 찍고
PYR_RSI_COOL = 60.0        # 60 이하로 식을 때
PYR_LADDER = (5.0, 8.0, 11.0)  # 순환 이전에 불타기한 기존 캠페인의 3단 익절(이관 전용)
PYR_TP_OFFSET_ATR = 0.2   # 불타기 체결분 전용 익절: 신호 극점보다 최초 ATR×0.2 안쪽
# 불타기 운용 모드. 새 캠페인이 만들어질 때 캠페인에 기록되고, 그 캠페인 끝까지 유지된다.
#   cyclic     : 순환형. 남은 여유 전부 매수, 손절을 보유분 본절로 당김, 전용 익절에서 전량 회수.
#   accumulate : 적립형. 매수 크기를 캠페인 실현 이익 예산의 ACC_BUDGET_USE로 제한, 손절을 올리지 않음,
#                전용 익절에서 번 만큼(공식 × ACC_KEEP_RATIO)을 남겨 보유량을 키움,
#                TP2에서 보유량의 ACC_TP2_RATIO만 매도하고 이후 불타기 중단, 트레일 되돌림 ACC_TRAIL_DIST_ATR.
PYR_MODE = os.getenv("BOT1_PYR_MODE", "accumulate").strip().lower()
PYR_MODES = {"cyclic", "accumulate"}
ACC_BUDGET_USE = 0.7
ACC_KEEP_RATIO = 0.7
ACC_TP2_RATIO = 0.3
ACC_TRAIL_DIST_ATR = 3.0
PROFIT_KINDS = {"tp1", "tp2", "tp3", "risk_reduce", "pyr_tp"}
SHADOW_SIDE = "long"
SHADOW_ARM_ATR = 2.0
SHADOW_PULL_ATR = 0.5      # 0.5ATR 눌림부터 후보
SHADOW_MAX_PULL_ATR = 99.0  # 눌림 상한 없음(수익권 조건이 실질 안전장치)
SHADOW_MIN_PROFIT_ATR = 0.5
SHADOW_RSI_PEAK = 65.0
SHADOW_RSI_COOL = 60.0
SHADOW_ADD_SIZE = 0.12


class ReconciliationRequired(RuntimeError):
    """Ownership is uncertain; never repeat a potentially accepted submission."""


class OrderRejected(RuntimeError):
    """The exchange definitively rejected an order, without accepting it."""


def _safe_notify(bot_module, message):
    """알림 실패가 거래 로직을 막지 않게 감싼다."""
    try:
        bot_module.notify(message)
    except Exception:
        pass


def finite(value, positive=False):
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0):
        raise ValueError("invalid numeric state")
    return number


def order_quantity(order):
    direct = order.get("filled")
    raw = (order.get("info") or {}).get("cumExecQty")
    if direct is None and raw is None:
        raise ReconciliationRequired("주문의 누적 체결 수량이 누락됐습니다.")
    quantity = finite(direct if direct is not None else raw)
    if quantity < 0 or (direct is not None and raw is not None and abs(quantity - finite(raw)) > QTY_EPS):
        raise ReconciliationRequired("주문의 누적 체결 수량이 불일치합니다.")
    return quantity


def pyramid_signal(avg, atr, fav_extreme, rsi_peak, close, rsi, direction=1):
    """불타기 판정(순수 함수).

    방향별 유리한 극단이 평단에서 PYR_ARM_ATR 이상 이동한 뒤 되돌림과 수익 쿠션을 확인한다.
    RSI는 롱은 원값, 숏은 100-RSI로 정규화해 동일한 기준을 사용한다.
    조건을 모두 만족하면 계산 근거를 담은 dict, 아니면 None.
    """
    for value in (avg, atr, fav_extreme, close):
        if not (isinstance(value, (int, float)) and math.isfinite(value) and value > 0):
            return None
    excursion = direction * (fav_extreme - avg) / atr
    retrace = direction * (fav_extreme - close) / atr
    cushion = direction * (close - avg) / atr
    info = {"excursion_atr": excursion, "retrace_atr": retrace, "cushion_atr": cushion,
            "rsi": rsi if math.isfinite(rsi) else None, "rsi_peak": rsi_peak}
    if excursion < PYR_ARM_ATR or retrace < PYR_PULL_ATR or cushion < PYR_MIN_CUSHION_ATR:
        return None
    if not (math.isfinite(rsi) and rsi_peak >= PYR_RSI_PEAK and rsi <= PYR_RSI_COOL):
        return None
    return info


def breakeven_price(side, average, entry_fee, entered_size, exit_fee_rate,
                    buffer_rate=BREAKEVEN_BUFFER_RATE):
    average = finite(average, True)
    entered_size = finite(entered_size, True)
    entry_fee = finite(entry_fee)
    rate = finite(exit_fee_rate)
    buffer = finite(buffer_rate)
    if side not in {"long", "short"} or entry_fee < 0 or not 0 <= rate < 1 or buffer < 0:
        raise ValueError("invalid breakeven parameters")
    allocated_fee = entry_fee / entered_size
    if side == "long":
        return (average + allocated_fee) / (1 - rate) * (1 + buffer)
    return (average - allocated_fee) / (1 + rate) * (1 - buffer)


PROFIT_LABELS = {"tp1": "1차 익절", "tp2": "2차 익절", "tp3": "3차 익절",
                 "risk_reduce": "물타기분 본절", "pyr_tp": "불타기 전용 익절"}


def current_pyr_mode():
    """새 캠페인에 기록할 모드: 텔레그램 /mode로 저장한 값이 있으면 그것, 없으면 PYR_MODE."""
    default = PYR_MODE if PYR_MODE in PYR_MODES else "cyclic"
    try:
        import telegram_commands
    except ImportError:
        return default
    return telegram_commands.read_mode(default)


def campaign_mode(c):
    """캠페인에 기록된 불타기 모드. 기록이 없는 기존 캠페인은 순환형으로 본다."""
    mode = (c or {}).get("pyr_mode") or "cyclic"
    return mode if mode in PYR_MODES else "cyclic"


def trail_distance(c):
    return ACC_TRAIL_DIST_ATR if campaign_mode(c) == "accumulate" else TRAIL_DIST_ATR


def accumulate_keep(size, buy, sell, stop, ratio=ACC_KEEP_RATIO):
    """적립형 전용 익절에서 남길 수량(순수 함수).

    판 물량의 이익 = 남긴 물량이 손절가에서 잃는 돈이 되도록 하는 양에 ratio를 곱한다.
    k = size × |sell − buy| ÷ |sell − stop|. 방향과 무관하게 거리로 계산한다.
    """
    size, buy, sell, stop, ratio = (finite(v) for v in (size, buy, sell, stop, ratio))
    gain, risk = abs(sell - buy), abs(sell - stop)
    if size <= 0 or gain <= 0 or risk <= 0 or not 0 <= ratio <= 1:
        return 0.0
    return min(size, ratio * size * gain / risk)


def accumulate_add_size(budget, fill, stop, room, step, use=ACC_BUDGET_USE):
    """적립형 불타기 수량(순수 함수): use × 이익 예산 ÷ 손절까지 거리, 남은 여유와 수량 단위로 제한."""
    budget, fill, stop, room, step, use = (finite(v) for v in (budget, fill, stop, room, step, use))
    risk = abs(fill - stop)
    if budget <= 0 or risk <= 0 or room <= 0 or step <= 0:
        return 0.0
    q = math.floor(use * budget / risk / step + 1e-9) * step
    return round(max(0.0, min(room, q)), 8)


def trigger_levels(c, track, total_size, slot_sizes, min_leg):
    """알림 시점 기준 각 자동 동작의 발동 가격(순수 함수, 표시 전용).

    반환: {"stop": [...], "profit": [...], "trail": [...], "pyramid": [...], "dca": [...]}
    각 값은 사람이 읽는 줄 목록이다. 계산에 필요한 값이 없으면 그 항목은 비운다.
    """
    out = {"stop": [], "profit": [], "trail": [], "pyramid": [], "dca": []}
    if not c or finite(c.get("remaining")) <= QTY_EPS:
        return out
    d = 1 if c.get("side") == "long" else -1
    up = d == 1
    try:
        atr = finite(c["initial_atr"], True)
        entry = finite(c["initial_price"], True)
        avg = finite(c.get("inventory_average") or c.get("average"), True)
        stop = finite(c["stop"], True)
    except (KeyError, TypeError, ValueError):
        return out
    remaining = finite(c["remaining"])

    loss = d * (stop - avg) * remaining
    out["stop"].append(f"{stop:,.1f} · 체결 시 {'+' if loss >= 0 else ''}{loss:,.0f} USDT (수수료 전)")

    for o in sorted((o for o in (c.get("orders") or {}).values()
                     if o.get("status") == "open" and o.get("kind") in PROFIT_LABELS),
                    key=lambda o: d * finite(o.get("price") or 0)):  # nearest first
        try:
            price = finite(o["price"], True)
            qty = finite(o.get("approved_remaining", o["requested"] - o.get("filled", 0.0)))
        except (KeyError, TypeError, ValueError):
            continue
        out["profit"].append(f"{PROFIT_LABELS[o['kind']]} {price:,.1f} · {qty:.3f} BTC")

    # Trailing: arms at entry ± TRAIL_ARM_ATR, then a close TRAIL_DIST_ATR back from the extreme exits.
    if c.get("trail_armed"):
        try:
            ext = finite(c.get("trail_extreme"), True)
            exit_px = ext - d * trail_distance(c) * atr
            out["trail"].append(f"무장됨 · 극단 {ext:,.1f}")
            out["trail"].append(f"종가가 {exit_px:,.1f} {'이하' if up else '이상'}면 전량 청산")
        except (TypeError, ValueError):
            out["trail"].append("무장됨")
    else:
        arm_px = entry + d * TRAIL_ARM_ATR * atr
        out["trail"].append(f"{arm_px:,.1f} {'돌파' if up else '이탈'} 시 무장 "
                            f"(진입가 {'+' if up else '−'}{TRAIL_ARM_ATR:g} ATR)")
        out["trail"].append(f"무장 후 극단에서 {trail_distance(c):g} ATR 되돌린 종가에 청산")

    # Pyramid: extreme must reach avg ± PYR_ARM_ATR, then a close inside the pull-back window.
    capacity = max(0.0, total_size - remaining)
    if capacity < min_leg:
        out["pyramid"].append(f"한도 {total_size:g} BTC 소진 · 대기")
    elif c.get("side") == "short" and not c.get("short_pyramid_eligible", False):
        out["pyramid"].append("이 숏 캠페인은 불타기 대상 아님")
    elif c.get("pyr_done"):
        out["pyramid"].append("직전 신호 해제 대기 (해제 후 재무장)")
    else:
        arm_px = avg + d * PYR_ARM_ATR * atr
        fav = None
        if track and track.get("trade_id") == c.get("trade_id"):
            try:
                fav = finite(track.get("fav_extreme"), True)
            except (TypeError, ValueError):
                fav = None
        armed = fav is not None and d * (fav - avg) / atr >= PYR_ARM_ATR - 1e-9
        if not armed:
            out["pyramid"].append(f"{arm_px:,.1f} {'돌파' if up else '이탈'} 시 무장 "
                                  f"(평단 {'+' if up else '−'}{PYR_ARM_ATR:g} ATR)")
        else:
            pull = fav - d * PYR_PULL_ATR * atr
            cushion = avg + d * PYR_MIN_CUSHION_ATR * atr
            lo, hi = sorted((pull, cushion))
            # Valid closes: past the pull-back level yet still inside the profit cushion.
            if d * (pull - cushion) >= 0:
                out["pyramid"].append(f"무장됨 · 극단 {fav:,.1f} (더 가면 구간도 따라 이동)")
                out["pyramid"].append(f"종가 {lo:,.1f} ~ {hi:,.1f} 구간 + RSI 식으면 체결")
            else:
                out["pyramid"].append(f"무장됨 · 극단 {fav:,.1f}")
                out["pyramid"].append("되돌림 구간이 평단 쿠션과 겹치지 않아 대기")
        cool = PYR_RSI_COOL if up else 100 - PYR_RSI_COOL
        peak = PYR_RSI_PEAK if up else 100 - PYR_RSI_PEAK
        out["pyramid"].append(f"RSI {peak:.0f} {'이상' if up else '이하'} 찍은 뒤 "
                              f"{cool:.0f} {'이하' if up else '이상'} · 추가 {capacity:.3f} BTC")

    # DCA: the next stage arms at entry ∓ step ATR; then needs the same-direction 1h signal
    # with price still between the stop and the average.
    stage = int(c.get("dca_stage", c.get("dca_count", 0))) + 1
    if stage > len(DCA_ATR_STEPS):
        out["dca"].append("2/2 사용 · 물타기분 본절 정리 뒤 다시 1차부터")
    elif capacity < min_leg:
        out["dca"].append(f"한도 {total_size:g} BTC 소진 · 대기")
    else:
        step = DCA_ATR_STEPS[stage - 1]
        arm_px = entry - d * step * atr
        size = min(slot_sizes[stage] if stage < len(slot_sizes) else 0.0, capacity)
        armed = bool((c.get("armed") or [False, False])[stage - 1])
        head = f"{stage}차 {size:.3f} BTC"
        if armed:
            out["dca"].append(f"{head} · 무장됨")
        else:
            out["dca"].append(f"{head} · {arm_px:,.1f} {'이탈' if up else '돌파'} 시 무장 "
                              f"(진입가 {'−' if up else '+'}{step:g} ATR)")
        # DCA submits only while price sits between the stop and the average (losing side).
        if d * (avg - stop) > 0:
            lo, hi = sorted((stop, avg))
            out["dca"].append(f"무장 후 1시간 {'롱' if up else '숏'} 신호 + 가격 {lo:,.1f} ~ {hi:,.1f} 안")
        else:
            out["dca"].append("손절이 평단보다 유리하게 잠겨 있어 지금은 물타기 불가")
    return out


@contextmanager
def bot_session_lock(path):
    """Both F runtimes hold the same OS lock; a crash releases ownership."""
    with open(path, "a+b") as handle:
        handle.seek(0, os.SEEK_END)
        if handle.tell() == 0:
            handle.write(b"0")
            handle.flush()
        handle.seek(0)
        if os.name == "nt":
            import msvcrt
            msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        try:
            yield
        finally:
            handle.seek(0)
            if os.name == "nt":
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def campaign_view(bot, campaign):
    c = campaign
    return bot.Position(
        side=c["side"], entry_price=c["average"], entry_time=c["entry_time"],
        stop_price=c["stop"], size_remaining=c["remaining"], entry_size=c["entered_size"],
        entry_atr=c["initial_atr"], sl_dist=abs(c["average"] - c["stop"]),
        tp1_dist=c["initial_atr"] * 5, tp2_dist=c["initial_atr"] * 10,
        tp1_stop_dist=c["initial_atr"] * 1.25,
        tp1_price=c["tp_anchor"] + c["direction"] * c["initial_atr"] * 5,
        tp2_price=c["tp_anchor"] + c["direction"] * c["initial_atr"] * 10,
        tp1_size=c["tp1_goal"], tp2_size=c["tp2_goal"], tp1_hit=c["tp1_started"],
        max_hold_bars=0, max_hold_after_tp1=False, exit_profile_name="F_DCA_FIXED_INITIAL_SL",
        entry_fee=sum(e["fee_amount"] for e in c["entries"]), fees_currency="USDT",
        trade_id=c["trade_id"], entry_order_id=c["entries"][0]["order_id"],
        leader_bucket=c["bucket"], leader_details=c["details"], entry_reason="f_dca_volume_ema_or_ema50",
        sl_order_id=c.get("sl_id", ""), tp1_order_id=c.get("tp1_id", ""),
        tp2_order_id=c.get("tp2_id", ""),
    )


class DcaEngine:
    def __init__(self, bot, state_file):
        self.bot = bot
        self.path = state_file
        self.state = {
            "version": 2, "mode": "F_DCA", "total_size_btc": bot.F_TOTAL_SIZE_BTC,
            "weights": list(bot.F_SLOT_WEIGHTS), "campaign": None,
            "runtime": deepcopy(bot.F_RUNTIME), "pending": None, "paused": "",
            "dirty_journal": False, "retired_order_ids": [], "rebuilding": False,
        }

    @property
    def campaign(self):
        return self.state["campaign"]

    def save(self):
        temporary = self.path + ".tmp"
        with open(temporary, "w", encoding="utf-8") as handle:
            json.dump(self.state, handle, ensure_ascii=False, indent=2, allow_nan=False)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, self.path)

    def validate_state(self):
        s = self.state
        if (s.get("version") != 2 or s.get("mode") != "F_DCA"
                or finite(s["total_size_btc"], True) != self.bot.F_TOTAL_SIZE_BTC
                or tuple(s["weights"]) != self.bot.F_SLOT_WEIGHTS
                or not isinstance(s["runtime"], dict)):
            raise ValueError("DCA state/config mismatch")
        c = self.campaign
        if c is None:
            return
        if c["side"] not in {"long", "short"} or c["direction"] != (1 if c["side"] == "long" else -1):
            raise ValueError("invalid campaign direction")
        for key in ("initial_price", "initial_atr", "initial_stop", "average", "stop",
                    "base_size", "entered_size", "tp_anchor"):
            finite(c[key], True)
        for key in ("remaining", "tp1_goal", "tp2_goal"):
            if finite(c[key]) < 0:
                raise ValueError("negative campaign quantity")
        expected_stop = c["initial_price"] - c["direction"] * 5 * c["initial_atr"]
        if abs(c["initial_stop"] - expected_stop) > QTY_EPS:
            raise ValueError("initial stop anchor changed")
        if c["direction"] * (c["stop"] - c["initial_stop"]) < -QTY_EPS:
            raise ValueError("stop widened")
        entries = c["entries"]
        # Cyclic entries: the count is unbounded; the simultaneous-position cap is enforced below.
        if not entries or len({e["order_id"] for e in entries}) != len(entries):
            raise ValueError("invalid entry ownership")
        entered = sum(finite(e["size"], True) for e in entries)
        for event in entries:
            finite(event["price"], True)
            if finite(event["fee_amount"]) < 0:
                raise ValueError("invalid entry fee")
        average = sum(e["size"] * e["price"] for e in entries) / entered
        if (abs(entered - c["entered_size"]) > QTY_EPS
                or abs(average - c["average"]) > QTY_EPS
                or abs(c["base_size"] - entries[0]["size"]) > QTY_EPS):
            raise ValueError("entry ledger mismatch")
        if finite(c["remaining"]) > self.bot.F_TOTAL_SIZE_BTC + QTY_EPS:
            raise ValueError("simultaneous position cap exceeded")
        lot_ids = {e["order_id"] for e in entries if e.get("event_type") == "pyr_entry"}
        lots = c.get("pyr_lots", [])
        if len({lot["order_id"] for lot in lots}) != len(lots):
            raise ValueError("duplicate pyramid lot")
        for lot in lots:
            if lot["order_id"] not in lot_ids:
                raise ValueError("pyramid lot without entry")
            for key in ("size", "extreme", "atr", "target"):
                finite(lot[key], True)
        exits = 0.0
        for oid, order in c["orders"].items():
            if oid != order["order_id"] or order["kind"] not in PROFIT_KINDS | {"sl", "native_sl", "emergency"}:
                raise ValueError("invalid exit ownership")
            if order["kind"] == "pyr_tp" and order.get("lot") not in {lot["order_id"] for lot in lots}:
                raise ValueError("pyramid TP without lot")
            qty = finite(order.get("filled", 0))
            if qty < 0 or (order["kind"] != "native_sl" and qty > finite(order["requested"], True) + QTY_EPS):
                raise ValueError("invalid exit quantity")
            if qty:
                event = order["execution"]
                if abs(finite(event["size"], True) - qty) > QTY_EPS or event["order_id"] != oid:
                    raise ValueError("exit execution mismatch")
                finite(event["price"], True)
                if finite(event["fee_amount"]) < 0:
                    raise ValueError("invalid exit fee")
            exits += qty
        if abs(entered - exits - c["remaining"]) > QTY_EPS:
            raise ValueError("campaign quantity conservation failed")
        if exits > QTY_EPS and not c["exit_started"]:
            raise ValueError("DCA permitted after exit")
        if c["reduction_done"] and not c["exit_started"]:
            raise ValueError("invalid reduction state")
        if len(c["armed"]) != 2:
            raise ValueError("invalid DCA thresholds")
        inventory_average = finite(c["inventory_average"], True)
        exit_cost = sum(finite(o["execution"]["cost_basis"], True)
                        for o in c["orders"].values() if o.get("filled", 0) > 0)
        entry_cost = sum(e["size"] * e["price"] for e in entries)
        if not math.isclose(entry_cost, exit_cost + inventory_average * c["remaining"],
                            rel_tol=1e-10, abs_tol=1e-6):
            raise ValueError("inventory cost conservation failed")
        dca, lot_open, base = self._buckets()
        if dca < -QTY_EPS or base < -QTY_EPS or any(q < -QTY_EPS for _, q in lot_open):
            raise ValueError("exit budget exceeds position buckets")

    def _migrate_cyclic(self, campaign):
        """Adopt an open legacy campaign into the cyclic model without touching its orders.

        Legacy pyramid volume has no dedicated lot, so it stays in the base bucket and keeps
        its existing ladder or TP1/TP2 plan; new cyclic pyramids are recorded as lots.
        """
        campaign.setdefault("pyr_lots", [])
        if "dca_stage" not in campaign:
            dca_in = sum(e["size"] for e in campaign.get("entries", []) if e.get("event_type") == "dca_entry")
            reduced = sum(o.get("filled", 0) for o in campaign.get("orders", {}).values()
                          if o.get("kind") == "risk_reduce")
            cleared = campaign.get("exit_started") and dca_in - reduced <= QTY_EPS
            # A cleared legacy DCA cycle restarts at stage 0; an open one keeps its stage.
            campaign["dca_stage"] = 0 if cleared else int(campaign.get("dca_count", 0))
            if cleared:
                campaign["armed"] = [False, False]

    def _buckets(self):
        """Split the confirmed position into DCA, pyramid-lot and base quantities.

        Each profit order may only sell its own bucket, so the open profit orders can never
        sum to more than the confirmed remaining position.
        """
        c = self.campaign
        dca_in = sum(e["size"] for e in c["entries"] if e.get("event_type") == "dca_entry")
        dca = dca_in - self._filled("risk_reduce")
        lot_open = []
        for lot in c.get("pyr_lots", []):
            sold = sum(o.get("filled", 0) for o in c["orders"].values()
                       if o["kind"] == "pyr_tp" and o.get("lot") == lot["order_id"])
            lot_open.append((lot, lot["size"] - sold))
        if c.get("closing"):
            # Stop and emergency fills close every bucket at once; budgets no longer apply.
            return max(0.0, dca), [(lot, max(0.0, q)) for lot, q in lot_open], max(0.0, c["remaining"])
        base = c["remaining"] - dca - sum(q for _, q in lot_open)
        return dca, lot_open, base

    def _dca_open(self):
        return self._buckets()[0]

    def _profit_plan(self):
        """Desired profit orders as (key, kind, size, price, lot_id); pure, no exchange calls."""
        c = self.campaign
        minimum = self.bot.POSITION_MIN_LEG_SIZE_BTC
        dca, lots, base = self._buckets()
        accumulate = campaign_mode(c) == "accumulate"
        plan = []
        dca = self.bot.round_qty(max(0.0, dca))
        base = self.bot.round_qty(max(0.0, base))
        if dca > QTY_EPS:
            if dca >= minimum:
                fees = sum(e["fee_amount"] for e in c["entries"])
                price = breakeven_price(c["side"], c["inventory_average"], fees, c["entered_size"],
                                        self.bot.ENTRY_TAKER_FEE_RATE)
                plan.append(("dca", "risk_reduce", dca, price, None))
        elif base > QTY_EPS and int(c.get("ladder", 2)) >= 3:
            # Legacy campaigns that pyramided before the cyclic model keep their 3-leg ladder.
            anchor = finite(c["tp_anchor"], True)
            atr = finite(c["initial_atr"], True)
            guard = self._last_mark_price()
            valid = [mult for mult in PYR_LADDER if round(anchor + c["direction"] * mult * atr, 1) > 0]
            if not valid:
                raise ReconciliationRequired("유효한 불타기 익절 가격이 없습니다.")
            live = [mult for mult in valid
                    if guard is None or c["direction"] * (anchor + c["direction"] * mult * atr - guard) > 0]
            live = live or [valid[-1]]
            for index, (mult, size) in enumerate(zip(live, self._ladder_sizes(base, len(live)))):
                if size >= minimum:
                    plan.append(("base", f"tp{index + 1}", size, anchor + c["direction"] * mult * atr, None))
        elif base > QTY_EPS:
            tp1_size = self.bot.round_qty(min(base, max(0, c["tp1_goal"] - self._filled("tp1"))))
            tp2_size = self.bot.round_qty(max(0, base - tp1_size))
            if accumulate:
                # Accumulate: TP2 sells ACC_TP2_RATIO of the whole holding once; the rest rides the trail.
                tp2_size = (0.0 if self._filled("tp2") > QTY_EPS
                            else self.bot.round_qty(min(tp2_size, ACC_TP2_RATIO * finite(c["remaining"]))))
            if tp1_size >= minimum:
                plan.append(("base", "tp1", tp1_size, c["tp_anchor"] + c["direction"] * 5 * c["initial_atr"], None))
            if tp2_size >= minimum:
                plan.append(("base", "tp2", tp2_size, c["tp_anchor"] + c["direction"] * 10 * c["initial_atr"], None))
        for lot, quantity in lots:
            if accumulate and lot.get("keep") is not None:
                sold = sum(o.get("filled", 0) for o in c["orders"].values()
                           if o["kind"] == "pyr_tp" and o.get("lot") == lot["order_id"])
                # Only the profit-taking part is sold; the kept part stays with the position
                # and exits with the stop or trail.
                quantity = finite(lot["size"]) - finite(lot["keep"]) - sold
            quantity = self.bot.round_qty(max(0.0, quantity))
            if quantity >= minimum:
                plan.append((f"lot:{lot['order_id']}", "pyr_tp", quantity, lot["target"], lot["order_id"]))
        return plan

    def _upgrade_cost_state(self):
        if self.state.get("version") != 1:
            return
        c = self.campaign
        if c:
            exits = [o["execution"] for o in c["orders"].values() if o.get("filled", 0) > 0]
            if exits:
                # Cumulative snapshots cannot reconstruct fills interleaved with adds.
                last_entry = max(self.bot.pd.Timestamp(e["event_time"]) for e in c["entries"])
                first_exit = min(self.bot.pd.Timestamp(e["event_time"]) for e in exits)
                if len(c["entries"]) > 1 and last_entry >= first_exit:
                    raise ReconciliationRequired("구버전 교차 진입·청산 원가는 수동 대조가 필요합니다.")
                if any(e.get("event_type") == "pyr_entry" for e in c["entries"]):
                    raise ReconciliationRequired("구버전 불타기 원장의 체결 순서를 확정할 수 없습니다.")
            c["inventory_average"] = finite(c["average"], True)
            for event in exits:
                event["cost_basis"] = event["size"] * c["inventory_average"]
        self.state["version"] = 2

    def load(self):
        if os.path.exists(self.path):
            with open(self.path, "r", encoding="utf-8") as handle:
                self.state = json.load(handle)
            campaign = self.state.get("campaign")
            if isinstance(campaign, dict):
                campaign.setdefault("dca_count", max(0, len(campaign.get("entries", [])) - 1))
                campaign.setdefault("pyr_total", 0.0)
                campaign.setdefault("pyr_done", bool(campaign.get("pyr_total", 0.0) > 0))
                campaign.setdefault("ladder", 2)
                self._migrate_cyclic(campaign)
            self._upgrade_cost_state()
            self.validate_state()
        self.bot.F_RUNTIME = self.state["runtime"]
        if self.state.get("pending") or self.state.get("rebuilding"):
            self.pause("미확정 주문 작업이 남아 있습니다. 저장된 주문 식별자로 거래소와 수동 대조가 필요합니다.")
        return self.campaign

    def pause(self, reason, retryable=False):
        changed = self.state.get("paused") != reason
        self.state["paused"] = reason
        self.state["paused_retryable"] = bool(retryable)
        if changed:
            self.state["paused_attempts"] = 0
            self.state["paused_last_try_ms"] = 0
            self.state["paused_since_ms"] = int(self.bot.exchange.milliseconds())
            self.state["paused_nag_ms"] = 0
        self.save()
        if changed:
            if retryable:
                tail = "\n일시 오류로 보입니다. 자동으로 다시 대조합니다."
            else:
                tail = "\n거래소의 현재 수량과 손절 주문을 확인해 주세요."
            _safe_notify(self.bot, f"🚨 봇1 물타기 자동화 중단\n{reason}{tail}")

    def position_matches(self):
        live = self.bot.f_exchange_position_strict()
        c = self.campaign
        expected = c["remaining"] if c else 0.0
        if live is None:
            return expected <= QTY_EPS
        size = finite(abs(float(live["contracts"])))
        return bool(c and str(live.get("side", "")).lower() == c["side"]
                    and abs(size - expected) <= QTY_EPS)

    def _snapshot(self, oid):
        order = self.bot.fetch_order_record(oid)
        if not isinstance(order, dict) or str(order.get("id", "")) != oid:
            raise ReconciliationRequired("주문 조회 응답의 ID가 일치하지 않습니다.")
        if str(order.get("status", "")).lower() not in TERMINAL_STATUSES | {"open"}:
            raise ReconciliationRequired("주문 상태를 확인할 수 없습니다.")
        order_quantity(order)
        return order

    def _apply(self, oid, order):
        c = self.campaign
        owned = c["orders"][oid]
        filled = order_quantity(order)
        old = owned.get("filled", 0.0)
        if filled < old - QTY_EPS or filled < 0:
            raise ReconciliationRequired("누적 체결 수량이 감소했습니다.")
        pending = self.state.get("pending") or {}
        if (pending.get("kind") == "pyr" and owned["kind"] == "native_sl"
                and filled > old + QTY_EPS):
            pending["native_stop_id"] = oid
            pending["native_stop_observed_filled"] = filled
            self.save()
            raise ReconciliationRequired("불타기와 첨부 손절의 체결 순서를 수동 대조해야 합니다.")
        owned["status"] = str(order["status"]).lower()
        if owned["kind"] != "native_sl":
            info = order.get("info") or {}
            amount = order.get("amount") if order.get("amount") is not None else info.get("qty")
            if amount is None:
                raise ReconciliationRequired("주문의 승인 수량이 누락됐습니다.")
            amount = finite(amount)
            if amount < filled - QTY_EPS:
                raise ReconciliationRequired("승인 수량보다 체결 수량이 큽니다.")
            owned["approved_remaining"] = max(0.0, amount - filled)
        if filled > 0:
            event = confirmed_execution(order, oid, owned["kind"])
            previous = owned.get("execution") or {}
            event["cost_basis"] = (finite(previous["cost_basis"]) if old else 0.0) + (
                filled - old) * finite(c["inventory_average"], True)
            if owned.get("execution") != event:
                self.state["dirty_journal"] = True
            owned["execution"] = event
            owned["filled"] = filled
            c["exit_started"] = True
            if owned["kind"] in {"sl", "native_sl"}:
                c["closing"] = True
                c["close_reason"] = "stop_after_tp1" if c["tp1_started"] else "stop_loss"
            elif owned["kind"] == "tp2":
                c["close_reason"] = "tp2_exit"
            elif owned["kind"] == "tp3":
                c["close_reason"] = "tp3_exit"
            if owned["kind"] == "tp1":
                c["tp1_started"] = True
                proposed = c["tp_anchor"] + c["direction"] * c["initial_atr"] * 1.25
                c["stop"] = max(c["stop"], proposed) if c["side"] == "long" else min(c["stop"], proposed)
        exits = sum(item.get("filled", 0) for item in c["orders"].values())
        if exits > c["entered_size"] + QTY_EPS:
            raise ReconciliationRequired("청산 수량이 총 진입 수량을 초과합니다.")
        c["remaining"] = self.bot.round_qty(c["entered_size"] - exits)
        # Cyclic: true while no DCA volume is waiting for its risk_reduce exit.
        c["reduction_done"] = bool(c["exit_started"] and self._dca_open() <= QTY_EPS)
        if (owned["kind"] == "risk_reduce" and filled > old + QTY_EPS
                and self._dca_open() <= QTY_EPS and not c.get("closing")):
            # The DCA cycle is fully cleared: restart the two DCA stages from scratch.
            c["dca_stage"] = 0
            c["armed"] = [False, False]
        if filled != old:
            self.state["dirty_journal"] = True
        return filled > old + QTY_EPS

    def _filled(self, kind):
        return sum(o.get("filled", 0) for o in self.campaign["orders"].values() if o["kind"] == kind)

    def _submit(self, kind, size, price=None, attached_stop=None, lot=None):
        c = self.campaign
        finite(size, True)
        pending = self.state.get("pending")
        entry_like = kind in {"entry", "pyr"}
        allowed_intent = pending and (pending.get("kind") == "rebuild"
                                      or entry_like and pending.get("kind") == kind
                                      and not pending.get("order_id"))
        if self.state.get("paused") or (pending and not allowed_intent):
            raise ReconciliationRequired("미확정 작업 이후 추가 주문을 제출할 수 없습니다.")
        if c and c.get("closing") and kind != "emergency":
            raise ReconciliationRequired("청산 진행 중에는 신규 보호·이익·진입 주문을 만들 수 없습니다.")
        if c and kind not in {"entry", "pyr"} and size > c["remaining"] + QTY_EPS:
            raise ReconciliationRequired("주문 수량이 확인된 잔량보다 큽니다.")
        side = c["side"] if c else self.state["pending"]["side"]
        link = self.bot.make_order_link_id("DCA_" + kind.upper())
        prior_intent = self.state.get("pending")
        intent = {"kind": kind, "link_id": link, "size": size, "price": price,
                  "side": side, "submitted_ms": self.bot.exchange.milliseconds()}
        if entry_like:
            intent.update(prior_intent or {})
            intent["link_id"] = link
        if kind == "emergency":
            c["closing"] = True
            c["close_reason"] = c.get("close_reason") or "emergency_protection_failure"
        self.state["pending"] = intent
        self.save()
        params = {"positionIdx": 0, "orderLinkId": link}
        entry = entry_like
        action = ("buy" if side == "long" else "sell") if entry_like else ("sell" if side == "long" else "buy")
        bounded_dca = kind == "entry" and c is not None and price is not None
        market = kind in {"entry", "pyr", "sl", "emergency"} and not bounded_dca
        if entry_like:
            params.update(stopLoss=str(round(finite(attached_stop, True), 1)), slTriggerBy="LastPrice",
                          tpslMode="Full", slOrderType="Market")
        else:
            params["reduceOnly"] = True
        if kind == "sl":
            params.update(triggerPrice=round(price, 1), triggerDirection=2 if side == "long" else 1,
                          triggerBy="LastPrice", closeOnTrigger=True)
        if not market:
            params["timeInForce"] = "FOK" if bounded_dca else "GTC"
        if entry_like and not 0 <= self.bot.exchange.milliseconds() - intent["signal_close_ms"] <= SIGNAL_MAX_AGE_MS:
            self.state["pending"] = None
            self.save()
            raise OrderRejected("stale entry signal")
        try:
            order = self.bot.exchange.create_order(self.bot.SYMBOL, "market" if market else "limit",
                                                  action, size, None if market else (price if bounded_dca else round(price, 1)), params)
        except Exception as exc:
            if type(exc).__name__ in {"InvalidOrder", "InsufficientFunds", "BadRequest", "AuthenticationError", "PermissionDenied"}:
                self.state["pending"] = None
                self.save()
                raise OrderRejected(type(exc).__name__) from exc
            self.pause("주문 제출 응답이 불명확합니다. 같은 주문을 자동 재전송하지 않습니다.")
            raise ReconciliationRequired("ambiguous order submission") from exc
        oid = str((order or {}).get("id") or "")
        if not oid:
            self.pause("접수된 주문의 ID가 없습니다. 주문 식별자로 수동 대조가 필요합니다.")
            raise ReconciliationRequired("missing submitted order ID")
        self.state["pending"]["order_id"] = oid
        if not entry:
            c["orders"][oid] = {"order_id": oid, "kind": kind, "requested": size,
                                 "price": price, "status": "open", "filled": 0.0, "approved_remaining": size}
            if lot:
                c["orders"][oid]["lot"] = lot
            self.state["pending"] = None
        self.save()
        if kind in PROFIT_KINDS:
            snapshot = self._snapshot(oid)
            self._apply(oid, snapshot)
            approved_total = self.campaign["orders"][oid]["approved_remaining"] + order_quantity(snapshot)
            if abs(approved_total - size) > QTY_EPS:
                raise ReconciliationRequired("거래소 승인 이익 주문 수량이 요청한 청산 예산과 다릅니다.")
            self.save()
        return oid

    def _cancel(self, oid):
        self.state["pending"] = {"kind": "cancel", "order_id": oid}
        self.save()
        try:
            self.bot.exchange.cancel_order(oid, self.bot.SYMBOL)
        except Exception:
            # A terminal exact-ID snapshot resolves an already-filled/canceled order.
            pass
        snapshot = self._snapshot(oid)
        self._apply(oid, snapshot)
        if str(snapshot["status"]).lower() not in TERMINAL_STATUSES:
            raise ReconciliationRequired("주문 취소가 확인되지 않았습니다.")
        self.state["pending"] = None
        self.save()

    def _cancel_profits(self):
        for oid, owned in list(self.campaign["orders"].items()):
            if owned["kind"] in PROFIT_KINDS and owned["status"] not in TERMINAL_STATUSES:
                self._cancel(oid)

    def _native_census(self, since=None):
        exchange = self.bot.exchange
        params = {"orderFilter": "StopOrder"}
        rows = list(exchange.fetch_open_orders(self.bot.SYMBOL, params=params))
        if since is not None:
            for method in (exchange.fetch_closed_orders, exchange.fetch_canceled_orders):
                page = method(self.bot.SYMBOL, since=since, limit=100, params=params)
                if len(page) >= 100:
                    raise ReconciliationRequired("임시 손절 주문 이력 조회 범위를 초과했습니다.")
                rows.extend(page)
        return {str(row["id"]): row for row in rows if row.get("id")}

    def _discover_native(self, intent):
        symbol_id = self.bot.SYMBOL.split(":")[0].replace("/", "")
        candidates = []
        for oid, order in self._native_census(intent["submitted_ms"]).items():
            info = order.get("info") or {}
            if str(info.get("stopOrderType")) != "StopLoss":
                continue
            trigger = finite(order.get("triggerPrice") or info.get("triggerPrice") or 0)
            close_side = "sell" if intent["side"] == "long" else "buy"
            created = int(order.get("timestamp") or info.get("createdTime") or 0)
            index = order.get("positionIdx", info.get("positionIdx"))
            owned_before = (oid in intent["native_before"] and self.campaign is not None
                            and self.campaign["orders"].get(oid, {}).get("kind") == "native_sl")
            if oid in intent["native_before"] and not owned_before:
                continue
            if (str(order.get("side") or info.get("side") or "").lower() == close_side
                    and order.get("symbol", info.get("symbol")) in {self.bot.SYMBOL, symbol_id}
                    and index is not None and int(index) == 0
                    and abs(trigger - round(intent["attached_stop"], 1)) <= 0.11
                    and (created >= intent["submitted_ms"] or owned_before)):
                candidates.append((oid, order))
        if len(candidates) != 1:
            raise ReconciliationRequired("진입에 붙인 임시 손절 주문을 유일하게 식별할 수 없습니다.")
        oid, snapshot = candidates[0]
        c = self.campaign
        c["orders"].setdefault(oid, {"order_id": oid, "kind": "native_sl", "requested": c["remaining"],
                                    "price": intent["attached_stop"], "status": "open", "filled": 0.0})
        if c["orders"][oid]["kind"] != "native_sl":
            raise ReconciliationRequired("native stop ownership conflict")
        self._apply(oid, snapshot)
        self.save()
        return oid

    def _clear_native(self):
        c = self.campaign
        ids = [oid for oid, item in c["orders"].items()
               if item["kind"] == "native_sl" and item["status"] not in TERMINAL_STATUSES]
        if not ids:
            return
        self.state["pending"] = {"kind": "native_clear", "order_ids": ids}
        self.save()
        symbol_id = self.bot.SYMBOL.split(":")[0].replace("/", "")
        response = self.bot.exchange.private_post_v5_position_trading_stop({
            "category": "linear", "symbol": symbol_id, "tpslMode": "Full",
            "positionIdx": 0, "stopLoss": "0", "slTriggerBy": "LastPrice",
        })
        if not isinstance(response, dict) or response.get("retCode") not in (0, "0"):
            raise ReconciliationRequired("임시 손절 해제 성공 응답을 확인할 수 없습니다.")
        for oid in ids:
            snapshot = self._snapshot(oid)
            self._apply(oid, snapshot)
            if str(snapshot["status"]).lower() not in TERMINAL_STATUSES:
                raise ReconciliationRequired("임시 손절 해제가 확인되지 않았습니다.")
        live = self.bot.f_exchange_position_strict()
        if live and float((live.get("info") or {}).get("stopLoss") or 0) != 0:
            raise ReconciliationRequired("포지션의 임시 손절 설정이 남아 있습니다.")
        if any(str((row.get("info") or {}).get("stopOrderType")) == "StopLoss"
               for row in self._native_census().values()):
            raise ReconciliationRequired("미확인 임시 손절 주문이 남아 있습니다.")
        if not self.position_matches():
            raise ReconciliationRequired("임시 손절 해제 중 포지션 수량이 변경됐습니다.")
        self.state["pending"] = None
        self.save()

    def _trail_exit_check(self, bar):
        """8 ATR 도달 뒤 극단 대비 2 ATR 되돌림이 종가로 확인되면 True(시장가 청산)."""
        c = self.campaign
        if not c or c.get("closing") or c["remaining"] <= QTY_EPS:
            return False
        try:
            atr = finite(c.get("initial_atr"), True)
            entry = finite(c.get("initial_price"), True)
            close_px = finite(bar["close"], True)
            hi = finite(bar["high"], True)
            lo = finite(bar["low"], True)
        except (KeyError, TypeError, ValueError):
            return False
        if not (0 < lo <= hi):
            return False
        d = c["direction"]
        fav = hi if d > 0 else lo
        try:
            old_ext = finite(c.get("trail_extreme"), True)
        except (TypeError, ValueError):
            old_ext = fav
        ext = max(old_ext, fav) if d > 0 else min(old_ext, fav)
        c["trail_extreme"] = ext
        run_up = d * (ext - entry) / atr
        if not c.get("trail_armed"):
            if run_up < TRAIL_ARM_ATR - 1e-9:
                # 극단이 갱신됐으면 조기 반환 전에 남긴다(피라미딩 조기 return 대비).
                if ext != old_ext:
                    self.save()
                return False
            c["trail_armed"] = True
            c["trail_armed_bar"] = str(self.bot.pd.Timestamp(bar["timestamp"]))
            self.save()
            side_label = "롱" if d > 0 else "숏"
            _safe_notify(self.bot, card(
                f"🎯 {side_label} 트레일 무장 · 최대 +{run_up:.2f} ATR",
                [("포지션", [f"수량   {finite(c['remaining']):.3f} BTC",
                            f"기준가 {entry:,.1f}",
                            f"극단   {ext:,.1f}"]),
                 ("청산 조건", [f"다음 봉부터 종가가 {ext - d * trail_distance(c) * atr:,.1f} "
                               f"{'이하' if d > 0 else '이상'}면 전량 청산 ({trail_distance(c):g} ATR 되돌림)",
                               "극단이 더 가면 청산 가격도 같이 따라갑니다"]),
                 *self.level_sections(("stop", "profit"))],
                kst_stamp()))
            # 한 봉 유예: 8 ATR을 찍은 봉의 종가로는 청산하지 않는다. 그 봉의 극단값이
            # 기준이 되고, 다음 확정봉 종가부터 되돌림을 판정한다.
            return False
        # 재시작으로 무장 봉이 다시 평가되는 경우에도 유예를 지킨다.
        if c.get("trail_armed_bar") == str(self.bot.pd.Timestamp(bar["timestamp"])):
            return False
        trail_px = ext - d * trail_distance(c) * atr
        if d * (close_px - trail_px) <= 0:
            c["close_reason"] = c.get("close_reason") or "trail_exit"
            self.save()
            return True
        return False

    def _profit_plan_missing(self):
        """True when open profit orders differ from the per-bucket plan in any bucket."""
        c = self.campaign

        def bucket(kind, lot):
            return f"lot:{lot}" if kind == "pyr_tp" else "dca" if kind == "risk_reduce" else "base"

        have, want = {}, {}
        for o in c["orders"].values():
            if o["kind"] in PROFIT_KINDS and o["status"] == "open":
                key = bucket(o["kind"], o.get("lot"))
                have[key] = have.get(key, 0.0) + o.get("approved_remaining", o["requested"] - o.get("filled", 0))
        for key, _, size, _, _ in self._profit_plan():
            want[key] = want.get(key, 0.0) + size
        return any(abs(have.get(k, 0.0) - want.get(k, 0.0)) > QTY_EPS for k in set(have) | set(want))

    def _refresh(self):
        before = deepcopy(self.campaign)
        changed = False
        stops = []
        for oid, owned in list(self.campaign["orders"].items()):
            # Terminal cumulative executions are immutable; retain them for accounting.
            if owned["status"] in TERMINAL_STATUSES:
                continue
            snapshot = self._snapshot(oid)
            changed = self._apply(oid, snapshot) or changed
            if owned["kind"] == "sl" and owned["status"] == "open":
                stops.append((owned, snapshot))
        c = self.campaign
        if not c.get("closing"):
            for owned, snapshot in stops:
                expected = owned["requested"] - owned.get("filled", 0)
                # Apply all exits first: exchanges can resize reduce-only stops on TP fills.
                if (before["remaining"] > c["remaining"] + QTY_EPS
                        and abs(owned["approved_remaining"] - c["remaining"]) <= QTY_EPS):
                    expected = c["remaining"]
                self._verify_stop(snapshot, expected, expected_price=owned["price"])
        if self.campaign != before:
            self.save()
        return changed

    def _verify_stop(self, order, expected, expected_price=None):
        c = self.campaign
        expected_price = c["stop"] if expected_price is None else expected_price
        info, params = order.get("info") or {}, order.get("params") or {}
        index = order.get("positionIdx", info.get("positionIdx"))
        reduce_only = order.get("reduceOnly", info.get("reduceOnly", params.get("reduceOnly")))
        trigger = finite(order.get("triggerPrice") or info.get("triggerPrice"), True)
        remaining = finite(order.get("amount") if order.get("amount") is not None else info.get("qty")) - order_quantity(order)
        if (str(order.get("status")).lower() != "open"
                or str(order.get("side") or info.get("side")).lower() != ("sell" if c["side"] == "long" else "buy")
                or index is None or int(index) != 0
                or reduce_only not in (True, "true", "True")
                or abs(remaining - expected) > QTY_EPS
                or abs(trigger - round(expected_price, 1)) > 0.11):
            raise ReconciliationRequired("교체 손절의 전량·방향·트리거·reduceOnly 증거가 불일치합니다.")

    def _finish_stop_exit(self):
        c = self.campaign
        if not c.get("closing"):
            return False
        pending = self.state.get("pending")
        if self.state.get("paused") or (pending and pending.get("kind") != "rebuild"):
            return True
        if pending:
            self.state["pending"] = None
        self.state["rebuilding"] = False
        self.save()
        if c["remaining"] > QTY_EPS:
            self.emergency_close()
        return True

    def _replace_stop(self):
        c = self.campaign
        old_stops = [oid for oid, item in c["orders"].items()
                     if item["kind"] == "sl" and item["status"] not in TERMINAL_STATUSES]
        c["sl_id"] = self._submit("sl", c["remaining"], c["stop"])
        self.save()
        replacement = self._snapshot(c["sl_id"])
        self._apply(c["sl_id"], replacement)
        if c.get("closing"):
            return
        self._verify_stop(replacement, c["remaining"])
        for oid in old_stops:
            self._cancel(oid)
            if c.get("closing"):
                return
        if not self.position_matches():
            raise ReconciliationRequired("손절 주문 교체 중 체결 발생")
        self._clear_native()

    def rebuild_orders(self):
        c = self.campaign
        if self._finish_stop_exit():
            return
        self.state["rebuilding"] = True
        self.state["pending"] = {"kind": "rebuild"}
        self.save()
        self._cancel_profits()
        if not self.position_matches():
            raise ReconciliationRequired("보호 주문 교체 전 포지션 수량 불일치")
        if c["remaining"] <= QTY_EPS:
            self.state["pending"] = None
            self.state["rebuilding"] = False
            self.save()
            return
        self._replace_stop()
        if self._finish_stop_exit():
            return
        if c["remaining"] <= QTY_EPS:
            self.state["rebuilding"] = False
            self.save()
            return
        c["tp1_id"] = c["tp2_id"] = c["tp3_id"] = ""
        # Each bucket gets its own profit orders: DCA → breakeven risk_reduce (the base keeps
        # no TP until it clears), each pyramid lot → its dedicated TP, base → TP1/TP2 or ladder.
        for _, kind, size, price, lot in self._profit_plan():
            oid = self._submit(kind, size, price, lot=lot)
            if kind == "risk_reduce":
                c["risk_id"], c["risk_price"] = oid, price
            elif kind in {"tp1", "tp2", "tp3"}:
                c[f"{kind}_id"] = oid
            if c.get("closing"):
                break
        self.state["pending"] = None
        self.state["rebuilding"] = False
        self.state["dirty_journal"] = True
        self.save()

    def record_journal(self):
        c = self.campaign
        if not c:
            return True
        view = campaign_view(self.bot, c)
        exits = [o["execution"] for o in c["orders"].values() if o.get("filled", 0) > 0]
        entry_fee = sum(e["fee_amount"] for e in c["entries"])
        fees = entry_fee + sum(e["fee_amount"] for e in exits)
        gross = sum(c["direction"] * (e["price"] * e["size"] - e["cost_basis"]) for e in exits)
        net = gross - fees
        final = c["remaining"] <= QTY_EPS
        source = "confirmed_fills"
        settled = False
        if final:
            records = self.bot.f_fetch_slot_closed_pnl(c["entry_time"])
            matched = []
            for execution in exits:
                rows = [r for r in records if str(r.get("orderId")) == execution["order_id"]]
                if not rows or abs(sum(float(r.get("closedSize") or 0) for r in rows) - execution["size"]) > QTY_EPS:
                    break
                if any(r.get("closedPnl") is None or r.get("openFee") is None or r.get("closeFee") is None for r in rows):
                    break
                matched.extend(rows)
            else:
                settled = bool(exits)
            if settled:
                net = sum(finite(r["closedPnl"]) for r in matched)
                fees = sum(abs(finite(r["openFee"])) + abs(finite(r["closeFee"])) for r in matched)
                gross = net + fees
                source = "exchange_closed_pnl"
                self._last_settlement = {"net": net, "fees": fees, "side": c["side"],
                                         "size": c.get("entered_size"), "average": c.get("average"),
                                         "reason": c.get("close_reason", "")}
        status = "closed" if settled else "partially_closed" if exits else "open"
        latest = max(exits, key=lambda e: e["event_time"]) if exits else None
        note = f"F aggregate DCA | source={source} | settlement_pending={final and not settled}"
        with self.bot.open_dashboard_db() as conn:
            self.bot.dashboard_upsert_trade(view, status=status,
                exit_price=latest["price"] if settled else None,
                exit_time=latest["event_time"] if settled else None,
                gross_pnl=gross, fees_total=fees, fees_currency="USDT", realized_pnl=net,
                close_reason=c.get("close_reason", "") if settled else "", notes=note, conn=conn)
            for index, event in enumerate(c["entries"] + exits):
                kind = ("entry" if index == 0 else str(event.get("event_type") or "dca_entry")) \
                    if index < len(c["entries"]) else event["event_type"]
                key = f"{c['trade_id']}:{kind}:{event['order_id']}"
                delta = -event["fee_amount"] if index < len(c["entries"]) else (
                    c["direction"] * (event["price"] * event["size"] - event["cost_basis"]) - event["fee_amount"])
                self.bot.dashboard_append_event(view, kind, event_time=event["event_time"], price=event["price"],
                    size=event["size"], pnl_delta=delta, fee_amount=event["fee_amount"], fee_currency="USDT",
                    note=note, order_id=event["order_id"], event_key=key, conn=conn)
                conn.execute("UPDATE trade_events SET event_time=?,price=?,size=?,pnl_delta=?,fee_amount=?,note=? WHERE event_id=?",
                             (event["event_time"], event["price"], event["size"], delta, event["fee_amount"], note, key))
            conn.commit()
        self.state["dirty_journal"] = final and not settled
        self.save()
        return settled if final else True

    def _campaign_pnl_at(self, price):
        """캠페인 전체 순손익(수수료 포함)을 보유분을 `price`에 모두 정리한다고 가정해 계산한다."""
        c = self.campaign
        d = c["direction"]
        price = finite(price, True)
        exits = [o["execution"] for o in c["orders"].values() if o.get("filled", 0) > 0]
        realized = sum(d * (e["price"] * e["size"] - e["cost_basis"]) - e["fee_amount"] for e in exits)
        entry_fees = sum(e["fee_amount"] for e in c["entries"])
        held = finite(c["remaining"])
        open_pnl = d * (price - finite(c["inventory_average"], True)) * held
        exit_fee = self.bot.ENTRY_TAKER_FEE_RATE * price * held
        return realized - entry_fees + open_pnl - exit_fee

    def _pyramid_size(self, fill):
        """이번 불타기 수량. 순환형은 남은 여유 전부, 적립형은 이익 예산으로 제한."""
        c = self.campaign
        step = self.bot.POSITION_MIN_LEG_SIZE_BTC
        room = float(available_size(self.bot.F_TOTAL_SIZE_BTC, c["remaining"], step))
        if campaign_mode(c) != "accumulate":
            return room
        budget = self._campaign_pnl_at(c["stop"])
        return accumulate_add_size(budget, fill, c["stop"], room, step)

    def _pyramid_breakeven_stop(self):
        """불타기 후 손절 하한: 현재 보유분 본절(수수료 포함). 불타기 전이거나 적립형이면 None."""
        c = self.campaign
        if not c or not any(e.get("event_type") == "pyr_entry" for e in c["entries"]):
            return None
        if campaign_mode(c) == "accumulate":
            # 적립형은 손절을 올리지 않고 매수 크기(이익 예산)로 위험을 막는다.
            return None
        fees = sum(e["fee_amount"] for e in c["entries"])
        return breakeven_price(c["side"], c["inventory_average"], fees, c["entered_size"],
                               self.bot.ENTRY_TAKER_FEE_RATE)

    def _tighten_to_breakeven(self):
        """불타기한 캠페인의 손절이 본절보다 느슨하면 당기고 거래소 손절을 교체한다."""
        c = self.campaign
        if (not c or c.get("closing") or c["remaining"] <= QTY_EPS
                or self.state.get("pending") or self.state.get("paused")):
            return False
        target = self._pyramid_breakeven_stop()
        if target is None:
            return False
        tighter = (max if c["direction"] == 1 else min)(finite(c["stop"]), target)
        if abs(tighter - finite(c["stop"])) < 0.05:
            return False
        quote = finite(self.bot.exchange.fetch_ticker(self.bot.SYMBOL).get("last"), True)
        if c["direction"] * (quote - tighter) <= 0:
            # 현재가가 이미 본절을 넘어섰다: 즉시 체결될 손절은 걸지 않고 기존 손절을 유지한다.
            return False
        old = finite(c["stop"])
        c["stop"] = tighter
        self.state["dirty_journal"] = True
        self.save()
        try:
            self.rebuild_orders()
        except Exception as exc:
            self.pause(f"본절 손절 교체 확인 필요: {type(exc).__name__}")
            return True
        side_label = "롱" if c["direction"] == 1 else "숏"
        _safe_notify(self.bot, card(
            f"🛡️ {side_label} 손절 본절로 이동 · {tighter:,.1f}",
            [("포지션", [f"수량 {c['remaining']:.3f} BTC",
                        f"평단 {c['inventory_average']:,.1f}",
                        f"손절 {old:,.1f} → {tighter:,.1f}"]),
             *self.level_sections(("profit", "trail", "pyramid", "dca"))],
            kst_stamp()))
        return True

    def level_sections(self, keys=("stop", "profit", "trail", "pyramid", "dca")):
        """알림 끝에 붙일 '발동 가격' 섹션들(지금 저장 상태 기준)."""
        try:
            levels = trigger_levels(self.campaign, self.state["runtime"].get("pyr_track") or {},
                                    self.bot.F_TOTAL_SIZE_BTC, self.bot.f_slot_sizes(),
                                    self.bot.POSITION_MIN_LEG_SIZE_BTC)
        except Exception:
            return []
        titles = {"stop": "손절", "profit": "익절 주문", "trail": "트레일링",
                  "pyramid": "불타기 발동", "dca": "물타기 발동"}
        return [(titles[k], levels[k]) for k in keys if levels.get(k)]

    def _close_alert(self):
        info = getattr(self, "_last_settlement", None) or {}
        self._last_settlement = None
        side = info.get("side")
        side_label = "롱 " if side == "long" else "숏 " if side == "short" else ""
        title = f"✅ {side_label}거래 종료"
        net = info.get("net")
        if isinstance(net, (int, float)) and math.isfinite(net):
            title += f" · {'+' if net >= 0 else ''}{net:,.2f} USDT"
        result = []
        if info.get("reason"):
            result.append(f"사유   {info['reason']}")
        if isinstance(info.get("fees"), (int, float)):
            result.append(f"수수료 {info['fees']:,.2f} USDT")
        position = []
        if isinstance(info.get("size"), (int, float)):
            position.append(f"누적 진입 {info['size']:.3f} BTC")
        if isinstance(info.get("average"), (int, float)) and info["average"] > 0:
            position.append(f"평단     {info['average']:,.1f}")
        return card(title, [("결과", result), ("포지션", position)],
                    f"거래소 손익 대조 완료 · {kst_stamp()}")

    def _shadow_pyramid(self, bar):
        """추가진입 판정만 수행한다(주문 없음). 캠페인당 1회, 결과는 텔레그램 1건."""
        if not SHADOW_ENABLED:
            return
        c = self.campaign
        if (c is None or c["side"] != SHADOW_SIDE or c.get("closing")
                or c["remaining"] <= QTY_EPS):
            return
        shadow = self.state["runtime"].setdefault("pyr_shadow", {})
        if shadow.get("fired_trade_id") == c["trade_id"]:
            return
        atr = finite(c["initial_atr"], True)
        avg = finite(c["average"], True)
        close = finite(bar["close"], True)
        high = finite(bar["high"], True)
        rsi = float(bar.get("rsi14")) if bar.get("rsi14") is not None else float("nan")

        if shadow.get("trade_id") != c["trade_id"]:
            shadow.clear()
            shadow.update({"trade_id": c["trade_id"], "fav_extreme": high,
                           "rsi_peak": rsi if math.isfinite(rsi) else 0.0})
        shadow["fav_extreme"] = max(finite(shadow.get("fav_extreme") or high), high)
        if math.isfinite(rsi):
            shadow["rsi_peak"] = max(finite(shadow.get("rsi_peak") or 0.0), rsi)

        fe = finite(shadow["fav_extreme"], True)
        peak = finite(shadow.get("rsi_peak") or 0.0)
        excursion = (fe - avg) / atr
        retrace = (fe - close) / atr
        cushion = (close - avg) / atr
        shadow["last"] = {"bar": str(bar["timestamp"]), "excursion_atr": round(excursion, 2),
                          "retrace_atr": round(retrace, 2), "cushion_atr": round(cushion, 2),
                          "rsi": round(rsi, 1) if math.isfinite(rsi) else None,
                          "rsi_peak": round(peak, 1)}
        if (excursion < SHADOW_ARM_ATR or retrace < SHADOW_PULL_ATR
                or retrace > SHADOW_MAX_PULL_ATR or cushion < SHADOW_MIN_PROFIT_ATR):
            return
        if not (peak >= SHADOW_RSI_PEAK and math.isfinite(rsi) and rsi <= SHADOW_RSI_COOL):
            return

        add = SHADOW_ADD_SIZE
        new_size = self.bot.round_qty(c["remaining"] + add)
        new_avg = (avg * c["remaining"] + close * add) / new_size
        new_stop = max(finite(c["stop"]), new_avg - self.bot.F_ATR_SL_MULT * atr)
        shadow.update({"fired_trade_id": c["trade_id"], "fired_time": str(bar["timestamp"]),
                       "fired_price": close, "add_size": add, "new_avg": new_avg,
                       "new_stop": new_stop})
        self.bot.notify(
            "🎯 추가진입 신호 그림자 (주문 안 함)\n"
            f"롱 {c['remaining']:.3f} BTC\n"
            f"현재 {close:,.1f}\n"
            f"평단 {avg:,.1f}\n"
            f"고점 {fe:,.1f}\n"
            f"{add:.3f} BTC 추가 시 평단 {new_avg:,.1f}\n"
            f"손절 {new_stop:,.1f}\n"
            f"고점 +{excursion:.1f}ATR\n"
            f"눌림 {retrace:.1f}ATR\n"
            f"RSI {rsi:.0f} 최고 {peak:.0f}"
        )

    def _last_mark_price(self):
        """마지막으로 알려진 현재가. 없으면 None(가드 없이 배치)."""
        value = self.state["runtime"].get("pyr_mark")
        if value is None:
            value = (self.bot.F_RUNTIME.get("last_signal_diagnostics") or {}).get("close")
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        return value if math.isfinite(value) and value > 0 else None

    def _ladder_sizes(self, remaining, levels):
        each = self.bot.round_qty(remaining / levels)
        sizes = [each] * (levels - 1)
        sizes.append(self.bot.round_qty(remaining - sum(sizes)))
        return sizes

    def _pyramid_add(self, bar):
        """방향 대칭 불타기. 캠페인당 1회, 체결 후 익절을 3단으로 재분할한다."""
        c = self.campaign
        if not PYR_ENABLED or c is None or c["side"] not in ("long", "short"):
            return False
        # Existing short campaigns were opened before short pyramiding was enabled.
        if c["side"] == "short" and not c.get("short_pyramid_eligible", False):
            return False
        if (c.get("closing") or c["remaining"] <= QTY_EPS
                or self.state.get("pending") or self.state.get("paused")):
            return False
        close_ms = int(self.bot.pd.Timestamp(bar["timestamp"]).timestamp() * 1000) + 900_000
        if not self._signal_fresh(close_ms, "pyr_after_signal"):
            return False
        atr = finite(c["initial_atr"], True)
        # Current inventory cost; the all-entries average would include volume already sold.
        avg = finite(c["inventory_average"], True)
        high = finite(bar["high"] if c["direction"] == 1 else bar["low"], True)
        close = finite(bar["close"], True)
        rsi = float(bar.get("rsi14")) if bar.get("rsi14") is not None else float("nan")
        if c["direction"] == -1:
            rsi = 100.0 - rsi
        track = self.state["runtime"].setdefault("pyr_track", {})
        if (track.get("trade_id") != c["trade_id"]
                or (c["direction"] == -1 and track.get("side") != "short")):
            track.clear()
            track.update({"trade_id": c["trade_id"], "side": c["side"], "fav_extreme": high,
                          "rsi_peak": rsi if math.isfinite(rsi) else 0.0})
        track["fav_extreme"] = (max if c["direction"] == 1 else min)(finite(track.get("fav_extreme") or high), high)
        if math.isfinite(rsi):
            track["rsi_peak"] = max(finite(track.get("rsi_peak") or 0.0), rsi)
        fav = finite(track["fav_extreme"], True)
        peak = finite(track.get("rsi_peak") or 0.0)
        info = pyramid_signal(avg, atr, fav, peak, close, rsi, c["direction"])
        track["last"] = {"bar": str(bar["timestamp"]), "fav_extreme": fav, "rsi_peak": peak,
                         "signal": info is not None}
        if info is None:
            # Re-arm: after a pyramid the signal must clear once before the next one.
            if c.get("pyr_done"):
                c["pyr_done"] = False
                self.save()
            return False
        # Pyramiding starts only after the first take-profit (TP1) has filled.
        # The DCA breakeven exit (risk_reduce) does not count as TP1.
        if not c.get("tp1_started"):
            return False
        if c.get("pyr_done") or track.get("last_pyr_bar") == str(bar["timestamp"]):
            return False
        accumulate = campaign_mode(c) == "accumulate"
        if accumulate and self._filled("tp2") > QTY_EPS:
            # Accumulate stops adding once TP2 has taken its share.
            return False
        # Cyclic size: all of the remaining simultaneous-position capacity.
        # Accumulate size: ACC_BUDGET_USE of the campaign profit budget at the stop.
        add = self._pyramid_size(close)
        if add < self.bot.POSITION_MIN_LEG_SIZE_BTC:
            return False
        target = float(pyramid_target(c["side"], fav, atr, PYR_TP_OFFSET_ATR))
        added_this_bar = False
        try:
            self._cancel_profits()
            if not self.position_matches():
                self.rebuild_orders()
                return True
            if c.get("closing") or c["remaining"] <= QTY_EPS:
                return True
            native_before = list(self._native_census())
            quote = finite(self.bot.exchange.fetch_ticker(self.bot.SYMBOL).get("last"), True)
            info = pyramid_signal(avg, atr, fav, peak, quote, rsi, c["direction"])
            if info is None or c["direction"] * (target - quote) <= 0:
                self.rebuild_orders()
                return True
            if not self.position_matches():
                raise ReconciliationRequired("불타기 제출 직전 포지션 수량이 변경됐습니다.")
            # Re-check capacity (and the accumulate budget) against the confirmed position
            # and live quote right before submission.
            add = min(add, self._pyramid_size(quote))
            if add < self.bot.POSITION_MIN_LEG_SIZE_BTC:
                self.rebuild_orders()
                return True
            old_size = finite(c["remaining"], True)
            old_average = finite(c["inventory_average"], True)
            old_stop = finite(c["stop"])
            old_exits = {oid: o.get("filled", 0) for oid, o in c["orders"].items()
                         if o["status"] not in TERMINAL_STATUSES}
            c["pyr_done"] = True
            track["last_pyr_bar"] = str(bar["timestamp"])
            self.state["runtime"]["pyr_mark"] = quote
            self.state["pending"] = {"kind": "pyr", "side": c["side"],
                                     "native_before": native_before, "attached_stop": c["stop"],
                                     "signal_close_ms": close_ms}
            self.save()
            oid = self._submit("pyr", add, attached_stop=c["stop"])
            intent = deepcopy(self.state["pending"])
            self.bot.time.sleep(self.bot.POSITION_SYNC_RETRY_SEC)
            snapshot = self._snapshot(oid)
            if str(snapshot["status"]).lower() not in TERMINAL_STATUSES:
                raise ReconciliationRequired("불타기 주문 체결 상태가 확정되지 않았습니다.")
            if order_quantity(snapshot) <= QTY_EPS:
                if not self.position_matches():
                    raise ReconciliationRequired("불타기 미체결 후 포지션 수량이 변경됐습니다.")
                c["pyr_done"] = False
                self.state["pending"] = None
                self.save()
                self.rebuild_orders()
                return True
            execution = confirmed_execution(snapshot, oid, "pyr_entry")
            filled = finite(execution["size"], True)
            for old_id, previous_fill in old_exits.items():
                if abs(order_quantity(self._snapshot(old_id)) - previous_fill) > QTY_EPS:
                    raise ReconciliationRequired("불타기와 기존 청산이 경합하여 체결 순서 대조가 필요합니다.")
            if filled > add + QTY_EPS:
                raise ReconciliationRequired("불타기 체결 수량이 요청보다 큽니다.")
            c["inventory_average"] = (old_average * old_size + execution["price"] * filled) / (old_size + filled)
            c["entries"].append(execution)
            c["pyr_total"] = self.bot.round_qty(c.get("pyr_total", 0.0) + filled)
            c["entered_size"] = self.bot.round_qty(sum(e["size"] for e in c["entries"]))
            c["average"] = sum(e["size"] * e["price"] for e in c["entries"]) / c["entered_size"]
            c["remaining"] = self.bot.round_qty(
                c["entered_size"] - sum(o.get("filled", 0) for o in c["orders"].values()))
            # Cyclic: the existing position keeps its TP plan; only this fill gets a dedicated TP.
            lot = {"order_id": oid, "size": filled, "price": execution["price"],
                   "extreme": fav, "atr": atr, "target": target, "signal_bar": str(bar["timestamp"])}
            if accumulate:
                # Keep what the dedicated TP pays for: its gain covers the kept part's loss at the stop.
                lot["keep"] = self.bot.round_qty(accumulate_keep(filled, execution["price"], target, old_stop))
            c.setdefault("pyr_lots", []).append(lot)
            added_this_bar = True
            # H: anchor the post-add stop to the ADD FILL, not the blended average.
            # The blended average is dragged toward the original entry, so anchoring to it
            # barely moves the stop; anchoring to the fill tightens it by the add cushion.
            proposed = finite(execution["price"], True) - c["direction"] * self.bot.F_ATR_SL_MULT * atr
            tighter = max if c["direction"] == 1 else min
            # Never let the enlarged position stop out below breakeven (cyclic only;
            # accumulate keeps the stop and bounds risk through the add size instead).
            floor = self._pyramid_breakeven_stop()
            c["stop"] = tighter(old_stop, proposed) if floor is None else tighter(old_stop, proposed, floor)
            if c["direction"] * (quote - c["stop"]) <= 0:
                # Price already sits past breakeven: keep a stop that will not fill at once.
                c["stop"] = tighter(old_stop, proposed)
            self.state["dirty_journal"] = True
            self.save()
            self._discover_native(intent)
            self._refresh()
            if not self.position_matches():
                raise ReconciliationRequired("불타기·임시 손절 체결 후 수량 불일치")
            self.state["pending"] = None
            self.save()
            if self._finish_stop_exit():
                self.record_journal()
                return True
            self.rebuild_orders()
            self.record_journal()
            up = c["direction"] == 1
            side_label = "롱" if up else "숏"
            rsi_now = rsi if up else 100 - rsi
            rsi_ext = peak if up else 100 - peak
            self.bot.notify(card(
                f"🔥 {side_label} 불타기 체결 · {filled:.3f} BTC @ {execution['price']:,.1f}",
                [("포지션", [f"수량      {c['remaining']:.3f} BTC",
                            f"평단      {c['inventory_average']:,.1f}",
                            f"손절      {c['stop']:,.1f}",
                            f"전용 익절 {target:,.1f} ({filled:.3f} BTC)"]),
                 ("근거", [f"{'고점' if up else '저점'} +{info['excursion_atr']:.1f} ATR · "
                           f"되돌림 {info['retrace_atr']:.1f} ATR",
                           f"RSI {rsi_now:.0f} ({'최고' if up else '최저'} {rsi_ext:.0f})"]),
                 *self.level_sections(("stop", "profit", "trail", "pyramid", "dca"))],
                kst_stamp()))
            return True
        except OrderRejected:
            if added_this_bar:
                self.emergency_close()
                return True
            c["pyr_done"] = False
            try:
                self.rebuild_orders()
            except Exception as exc:
                self.pause(f"불타기 거절 후 보호 복원 확인 필요: {type(exc).__name__}")
            return True
        except Exception as exc:
            self.pause(f"불타기 확인 필요: {type(exc).__name__}")
            return True

    def _protection_cover_ok(self):
        """잔여 포지션을 덮는 열린 손절이 있는지 확인한다(자동복구 게이트)."""
        c = self.campaign
        if not c or c["remaining"] <= QTY_EPS:
            return True
        for owned in c["orders"].values():
            if owned["kind"] in {"sl", "native_sl"} and owned["status"] == "open":
                try:
                    covered = finite(owned.get("approved_remaining",
                                               owned["requested"] - owned.get("filled", 0.0)))
                except (TypeError, ValueError):
                    continue
                if covered + QTY_EPS >= c["remaining"]:
                    return True
        return False

    def _retry_paused(self):
        """일시 오류로 멈춘 상태를 거래소 대조로 되살린다. 대조가 통과할 때만 해제한다."""
        state = self.state
        if not state.get("paused_retryable") or state.get("pending") or state.get("rebuilding"):
            return False
        now_ms = int(self.bot.exchange.milliseconds())
        if now_ms - int(state.get("paused_last_try_ms") or 0) < PAUSE_RETRY_COOLDOWN_MS:
            return False
        reason = str(state.get("paused") or "")
        state["paused_last_try_ms"] = now_ms
        state["paused_attempts"] = int(state.get("paused_attempts") or 0) + 1
        if state["paused_attempts"] > PAUSE_MAX_RETRY_ATTEMPTS:
            # 상한 초과: 무한 재시도로 조용히 멈춰 있는 대신 수동 확인으로 승격한다.
            self.pause(f"{reason} (자동 복구 {PAUSE_MAX_RETRY_ATTEMPTS}회 실패)", retryable=False)
            _safe_notify(self.bot, f"🚨 봇1 물타기 자동 복구 실패\n수동 확인 필요\n{reason}")
            return False
        try:
            self._refresh()
            matched = self.position_matches()
            covered = self._protection_cover_ok()
        except Exception as exc:
            name = type(exc).__name__
            if name not in TRANSIENT_ERROR_NAMES:
                self.pause(f"복구 중 오류: {name}", retryable=False)
            else:
                self.save()
            return False
        # 대조 중 새 중단/미확정 작업이 생겼거나, 수동 사유로 바뀌었으면 해제하지 않는다.
        if (not matched or not covered
                or str(self.state.get("paused") or "") != reason
                or not self.state.get("paused_retryable")
                or self.state.get("pending") or self.state.get("rebuilding")):
            self.save()
            return False
        state["paused"] = ""
        state["paused_retryable"] = False
        state["paused_attempts"] = 0
        state["paused_since_ms"] = 0
        state["paused_nag_ms"] = 0
        self.save()
        _safe_notify(self.bot, "✅ 봇1 물타기 자동 복구 · 거래소 대조 통과")
        return True

    def _stuck_nag(self):
        """수동 중단이 길어지면 주기적으로 다시 알린다. 한 번 알리고 잊히는 걸 막는다."""
        if self.state.get("paused_retryable") or not self.state.get("paused"):
            return
        now_ms = int(self.bot.exchange.milliseconds())
        since = int(self.state.get("paused_since_ms") or 0)
        if not since:
            self.state["paused_since_ms"] = now_ms
            self.save()
            return
        if now_ms - since < PAUSE_NAG_FIRST_MS:
            return
        last = int(self.state.get("paused_nag_ms") or 0)
        if last and now_ms - last < PAUSE_NAG_INTERVAL_MS:
            return
        self.state["paused_nag_ms"] = now_ms
        self.save()
        hours = (now_ms - since) / 3_600_000
        _safe_notify(self.bot, f"⚠️ 봇1 물타기 {hours:.0f}시간째 중단\n"
                               f"{self.state.get('paused')}\n"
                               f"거래소 확인 후 재시작해 주세요.")

    def poll(self):
        if self.state.get("paused"):
            if not self._retry_paused():
                self._stuck_nag()
                return False
        if self.state.get("pending") or self.state.get("rebuilding"):
            return False
        c = self.campaign
        if c is None:
            try:
                matched = self.position_matches()
            except Exception as exc:
                # 무포지션 상태의 조회 실패는 지킬 포지션이 없으므로 봉마다 조용히 재시도한다.
                # 다만 계속 실패하면 조용히 죽은 봇이 되므로 n회 연속에서 알림을 띄운다.
                detail = f"{type(exc).__name__}: {str(exc).strip().splitlines()[0][:80]}" if str(exc).strip() else type(exc).__name__
                streak = int(self.state.get("read_fail_streak") or 0) + 1
                self.state["read_fail_streak"] = streak
                self.state["read_fail_last"] = detail
                self.save()
                self.bot.log.warning("무포지션 포지션 조회 실패 %s회 연속 | %s", streak, detail)
                if streak >= FLAT_READ_FAIL_STREAK:
                    self.pause(f"포지션 조회 반복 실패({streak}회): {detail}", retryable=True)
                return False
            if self.state.get("read_fail_streak"):
                self.state["read_fail_streak"] = 0
                self.state["read_fail_last"] = ""
                self.save()
            if not matched:
                self.pause("거래소에 소유권 기록이 없는 포지션이 있어 신규 진입을 중단합니다.")
                return False
            return True
        try:
            self._refresh()
            if not self.position_matches():
                # 주문 조회와 포지션 조회 사이에 손절·익절이 체결되면 일시적으로 어긋난다.
                # 주문 상태를 한 번 더 반영한 뒤에도 다를 때만 수동 대조로 멈춘다.
                self._refresh()
                c = self.campaign
                if not self.position_matches():
                    self.pause("저장된 체결과 거래소 포지션이 다릅니다. 수동 대조가 필요합니다.")
                    return False
            if c["remaining"] <= QTY_EPS:
                for oid, owned in list(c["orders"].items()):
                    if owned["status"] not in TERMINAL_STATUSES:
                        self._cancel(oid)
                if not self.record_journal():
                    return False
                self.state["retired_order_ids"].extend(c["orders"])
                self.state["runtime"]["last_closed_campaign_ms"] = self.bot.exchange.milliseconds()
                self.state["campaign"] = None
                self.state["dirty_journal"] = False
                self.save()
                self.bot.notify(self._close_alert())
                return False
            if self._filled("sl") + self._filled("native_sl") > QTY_EPS:
                c["closing"] = True
                self.save()
            if c.get("closing"):
                active_close = any(o["kind"] == "emergency" and o["status"] == "open" for o in c["orders"].values())
                if not active_close:
                    self.emergency_close()
                return False
            if self._profit_plan_missing():
                self.rebuild_orders()
                if (c.get("closing") or c["remaining"] <= QTY_EPS
                        or self.state.get("paused") or self.state.get("pending")):
                    return False
            active_sl = c["orders"].get(c.get("sl_id"), {})
            stop_needed = (active_sl.get("status") != "open"
                           or abs(active_sl.get("requested", 0) - active_sl.get("filled", 0) - c["remaining"]) > QTY_EPS
                           or abs((active_sl.get("price") or 0) - c["stop"]) > 0.11)
            if stop_needed:
                self.state["rebuilding"] = True
                self.save()
                self._replace_stop()
                if self._finish_stop_exit():
                    return False
                self.state["rebuilding"] = False
                self.state["dirty_journal"] = True
                self.save()
            if self.state["dirty_journal"]:
                self.record_journal()
            return not self.state.get("paused") and not self.state.get("pending")
        except OrderRejected:
            self.emergency_close()
            return False
        except Exception as exc:
            name = type(exc).__name__
            self.pause(f"체결·보호 주문 대조 실패: {name}", retryable=name in TRANSIENT_ERROR_NAMES)
            return False

    def emergency_close(self):
        if self.state.get("pending"):
            self.pause("미확정 주문이 있어 긴급 종료를 재전송하지 않습니다. 거래소에서 즉시 확인해 주세요.")
            return
        try:
            c = self.campaign
            live = self.bot.f_exchange_position_strict()
            if not c or not live or str(live.get("side", "")).lower() != c["side"]:
                raise ReconciliationRequired("긴급 종료 대상 소유권 불명확")
            try:
                self._cancel_profits()
            except Exception:
                # 취소 실패는 청산을 막지 않는다. 잔여 주문은 포지션 종료 후 무효가 된다.
                self.save()
            live = self.bot.f_exchange_position_strict()
            if not live or not self.position_matches():
                raise ReconciliationRequired("긴급 종료 직전 수량 대조 실패")
            size = finite(abs(float(live["contracts"])), True)
            oid = self._submit("emergency", size)
            self._apply(oid, self._snapshot(oid))
            self.state["rebuilding"] = False
            self.save()
            self.bot.notify("🚨 보호 주문 실패로 긴급 종료 주문을 접수했습니다. 체결을 대조합니다.")
        except Exception as exc:
            self.pause(f"긴급 종료 확인 필요: {type(exc).__name__}")

    def _signal_fresh(self, close_ms, checkpoint):
        age_ms = self.bot.exchange.milliseconds() - close_ms
        fresh = 0 <= age_ms <= SIGNAL_MAX_AGE_MS
        runtime = self.state["runtime"]
        runtime["last_entry_check"] = {
            "bar_time": str(self.bot.pd.Timestamp(close_ms - 900_000, unit="ms", tz="UTC")),
            "age_ms": age_ms, "max_age_ms": SIGNAL_MAX_AGE_MS,
            "checkpoint": checkpoint,
            "result": "fresh" if fresh else "expired" if age_ms > SIGNAL_MAX_AGE_MS else "not_closed",
        }
        if not fresh:
            if age_ms > SIGNAL_MAX_AGE_MS and runtime.get("last_expired_signal_close_ms") != close_ms:
                runtime["last_expired_signal_close_ms"] = close_ms
                runtime["signal_expired_total"] = int(runtime.get("signal_expired_total", 0)) + 1
                counts = runtime.setdefault("signal_status_counts", {})
                counts["signal_expired"] = int(counts.get("signal_expired", 0)) + 1
            self.save()
            self.bot.log.warning(
                "DCA signal freshness | bar=%s checkpoint=%s age_ms=%s limit_ms=%s result=%s",
                runtime["last_entry_check"]["bar_time"], checkpoint, age_ms,
                SIGNAL_MAX_AGE_MS, runtime["last_entry_check"]["result"],
            )
        return fresh

    def on_bar(self, signal, bar, cached_positions=None):
        if not self.poll():
            return
        timestamp = str(bar["timestamp"])
        runtime = self.state["runtime"]
        if runtime.get("last_processed_closed_ts") == timestamp:
            return
        runtime["last_processed_closed_ts"] = timestamp
        close_ms = int(self.bot.pd.Timestamp(bar["timestamp"]).timestamp() * 1000) + 900_000
        c = self.campaign
        if c:
            if close_ms > c["last_entry_close_ms"]:
                extreme = float(bar["low"] if c["side"] == "long" else bar["high"])
                adverse = c["direction"] * (c["initial_price"] - extreme) / c["initial_atr"]
                for index, threshold in enumerate(DCA_ATR_STEPS):
                    c["armed"][index] = c["armed"][index] or adverse >= threshold
            if self._trail_exit_check(bar):
                self.save()
                self.emergency_close()
                return
            if self._pyramid_add(bar):
                return
            if self._tighten_to_breakeven():
                return
        self.save()
        if not signal or not self._signal_fresh(close_ms, "after_signal"):
            return
        if runtime.get("last_closed_campaign_ms", 0) >= close_ms:
            return
        zone_key = str(getattr(signal, "f_zone_key", ""))
        zone_independent_dca = bool(c and getattr(signal, "dca_zone_independent", False))
        if not zone_independent_dca and (not zone_key or zone_key in runtime.get("used_zone_waves", [])):
            return
        atr = finite(signal[4], True)
        # Cyclic DCA: the stage counts adds in the current open cycle and restarts once the
        # cycle's DCA volume has been cleared by risk_reduce (see _reset_dca_cycle).
        stage = (int(c.get("dca_stage", c.get("dca_count", 0))) + 1) if c else 0
        if c and (stage >= 3 or c.get("closing") or signal[0] != c["side"]
                  or close_ms <= c["last_entry_close_ms"] or not c["armed"][stage - 1]):
            return
        quote = finite(self.bot.exchange.fetch_ticker(self.bot.SYMBOL).get("last"), True)
        if c and c["direction"] * (quote - c["stop"]) <= 0:
            return
        size = self.bot.f_slot_sizes()[stage]
        if c:
            # Partial add allowed: never more than the simultaneous-position capacity left.
            size = min(size, float(available_size(self.bot.F_TOTAL_SIZE_BTC, c["remaining"],
                                                  self.bot.POSITION_MIN_LEG_SIZE_BTC)))
            if size < self.bot.POSITION_MIN_LEG_SIZE_BTC:
                return
        ok, context = self.bot.precheck_entry_balance(size, quote, cached_positions=cached_positions)
        if not ok:
            self.bot.notify(f"⏸️ 물타기 진입 잔고 부족\n{context}")
            return
        entries_before = len(c["entries"]) if c else 0
        try:
            # Bybit instrument metadata supplies the actual price tick; never guess it.
            limit = None
            if c:
                market_info = self.bot.exchange.market(self.bot.SYMBOL)
                tick = market_info['info']['priceFilter']['tickSize']
                live = self.bot.f_exchange_position_strict()
                if (not live or str(live.get('side', '')).lower() != c['side']
                        or abs(finite(live['contracts'], True) - c['remaining']) > QTY_EPS):
                    raise ReconciliationRequired('물타기 전 포지션이 일치하지 않습니다.')
                # Use Bybit's original decimal position average, not the float ledger.
                average = (live.get('info') or {}).get('avgPrice')
                try:
                    if not isinstance(average, str) or not average:
                        raise ValueError('missing average')
                    limit = dca_limit_price(c['side'], average, tick)
                    avg_decimal = Decimal(average)
                except (ValueError, InvalidOperation):
                    return  # Leave existing protection untouched; skip this DCA signal.
                if (c['direction'] * (avg_decimal - Decimal(str(bar['close']))) <= 0
                        or c['direction'] * (avg_decimal - Decimal(str(quote))) <= 0):
                    return
            if c:
                self._cancel_profits()
                if c.get("closing") or not self.position_matches():
                    self.rebuild_orders()
                    return
                if size > float(available_size(self.bot.F_TOTAL_SIZE_BTC, c["remaining"],
                                               self.bot.POSITION_MIN_LEG_SIZE_BTC)) + QTY_EPS:
                    self.rebuild_orders()
                    return
            else:
                # This account/symbol must have exclusive ownership before initial entry.
                if self.bot.exchange.fetch_open_orders(self.bot.SYMBOL):
                    raise ReconciliationRequired("기존 주문이 있어 신규 거래 소유권을 확정할 수 없습니다.")
            native_before = list(self._native_census())
            quote = finite(self.bot.exchange.fetch_ticker(self.bot.SYMBOL).get("last"), True)
            price_safe = not c or (c["direction"] * (quote - c["stop"]) > 0
                                   and c["direction"] * (avg_decimal - Decimal(str(quote))) > 0)
            if not self.position_matches():
                raise ReconciliationRequired("제출 직전 포지션 수량이 변경됐습니다.")
            fresh = self._signal_fresh(close_ms, "before_submit")
            if not fresh or not price_safe:
                if c:
                    self.rebuild_orders()
                return
            stop = c["stop"] if c else quote - (1 if signal[0] == "long" else -1) * 5 * atr
            if stop <= 0:
                raise ValueError("nonpositive initial stop")
            self.state["pending"] = {"kind": "entry", "side": signal[0], "native_before": native_before,
                                      "attached_stop": stop, "signal_close_ms": close_ms}
            oid = self._submit("entry", size, price=limit, attached_stop=stop)
            intent = deepcopy(self.state["pending"])
            self.bot.time.sleep(self.bot.POSITION_SYNC_RETRY_SEC)
            snapshot = self._snapshot(oid)
            if str(snapshot["status"]).lower() not in TERMINAL_STATUSES:
                raise ReconciliationRequired("진입 주문의 최종 체결 상태가 아직 확정되지 않았습니다.")
            filled = order_quantity(snapshot)
            if c and filled <= QTY_EPS:
                self.state["pending"] = None
                self.save()
                self.rebuild_orders()
                return
            if c and abs(filled - size) > QTY_EPS:
                raise ReconciliationRequired("FOK 물타기의 비정상 부분 체결을 확인해야 합니다.")
            execution = confirmed_execution(snapshot, oid, "entry" if stage == 0 else "dca_entry")
            actual = finite(execution["size"], True)
            if actual > size + QTY_EPS or (stage == 0 and actual < self.bot.POSITION_MIN_LEG_SIZE_BTC * 4):
                raise ReconciliationRequired("진입 체결 수량을 안전하게 분할할 수 없습니다.")
            if c is None:
                direction = 1 if signal[0] == "long" else -1
                c = {"trade_id": oid, "side": signal[0], "direction": direction,
                     "initial_price": execution["price"], "initial_atr": atr,
                     "initial_stop": execution["price"] - direction * 5 * atr,
                     "stop": execution["price"] - direction * 5 * atr,
                     "entry_time": execution["event_time"], "signal_time": timestamp,
                     "base_size": actual, "entries": [], "orders": {}, "armed": [False, False],
                     "exit_started": False, "tp1_started": False, "reduction_done": False, "closing": False,
                     "dca_count": 0, "dca_stage": 0, "pyr_total": 0.0, "pyr_done": False, "ladder": 2,
                     "pyr_lots": [], "pyr_mode": current_pyr_mode(),
                     "short_pyramid_eligible": direction == -1,
                     "trail_armed": False, "trail_extreme": None, "trail_armed_bar": None,
                     "bucket": signal[1], "details": signal[2], "close_reason": ""}
                self.state["campaign"] = c
            held, held_average = (c["remaining"], c["inventory_average"]) if stage >= 1 else (0.0, 0.0)
            c["entries"].append(execution)
            if stage >= 1:
                c["dca_stage"] = stage
                c["dca_count"] = int(c.get("dca_count", 0)) + 1
            c["entered_size"] = sum(e["size"] for e in c["entries"])
            c["average"] = sum(e["size"] * e["price"] for e in c["entries"]) / c["entered_size"]
            c["remaining"] = self.bot.round_qty(c["entered_size"] - sum(o.get("filled", 0) for o in c["orders"].values()))
            # Cyclic DCA can follow exits, so the inventory cost is updated incrementally.
            c["inventory_average"] = (held * held_average + actual * execution["price"]) / (held + actual)
            c["tp_anchor"] = c["inventory_average"]
            c["tp1_goal"] = self.bot.round_qty(c["base_size"] * self.bot.F_TP1_RATIO)
            c["tp2_goal"] = self.bot.round_qty(c["base_size"] - c["tp1_goal"])
            c["last_entry_close_ms"] = close_ms
            self.state["dirty_journal"] = True
            if not zone_independent_dca:
                runtime.setdefault("used_zone_waves", []).append(zone_key)
            self.save()
            self._discover_native(intent)
            if not self.position_matches():
                raise ReconciliationRequired("진입 또는 임시 손절 체결 후 수량 불일치")
            self.state["pending"] = None
            self.save()
            if not self._finish_stop_exit():
                self.rebuild_orders()
            self.record_journal()
            side_label = "롱" if c["side"] == "long" else "숏"
            reason_key = str(signal[3])
            reason_label = self.bot.ENTRY_REASON_LABELS.get(reason_key, reason_key)
            zone_label = self.bot.ENTRY_BUCKET_LABELS.get(str(signal[1]), str(signal[1]))
            head = "최초 진입" if stage == 0 else f"{stage}차 물타기"
            fill_price = finite(execution["price"], True)
            position = [f"수량 {c['remaining']:.3f} BTC",
                        f"평단 {c['inventory_average']:,.1f}",
                        f"손절 {finite(c['stop']):,.1f}"]
            if stage >= 1:
                position.insert(1, f"물타기 {stage}/2")
            self.bot.notify(card(
                f"📥 {side_label} {head} · {actual:.3f} BTC @ {fill_price:,.1f}",
                [("포지션", position),
                 ("근거", [f"신호 {reason_label}", f"존   {zone_label}"]),
                 *self.level_sections()],
                kst_stamp()))
        except OrderRejected:
            if self.campaign:
                if c is not None and len(self.campaign["entries"]) == entries_before:
                    try:
                        self.rebuild_orders()
                    except Exception as exc:
                        self.pause(f"진입 거절 후 보호 복원 확인 필요: {type(exc).__name__}")
                else:
                    self.emergency_close()
        except Exception as exc:
            self.pause(f"진입·주문 교체 확인 필요: {type(exc).__name__}")


def assert_legacy_rollback_safe(bot, state_file):
    if not os.path.exists(state_file):
        return
    engine = DcaEngine(bot, state_file)
    engine.load()
    if (engine.campaign or engine.state.get("pending") or engine.state.get("paused")
            or engine.state.get("dirty_journal") or bot.f_exchange_position_strict()
            or bot.exchange.fetch_open_orders(bot.SYMBOL)):
        raise ReconciliationRequired("물타기 거래·미확정 작업·잔존 주문이 있어 기존 슬롯 모드로 전환할 수 없습니다.")
