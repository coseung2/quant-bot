"""사용자용 텔레그램 알림 문구 변환기.

Bot1과 Bot2가 함께 쓴다. 내부 상태 키와 영어 사유 코드를 한국어 문장으로
바꾸고, 하나의 알림을 제목 + 불릿 형태로 정리한다. 거래 판단과 주문 값은
바꾸지 않고 표현만 담당한다.
"""

from __future__ import annotations

import re
from typing import Dict, List, Tuple

# 내부 사유 코드는 패턴이 슬롯 번호를 포함하므로 정규식으로 먼저 바꾼다.
REASON_CODE_PATTERNS: Tuple[Tuple[str, str], ...] = (
    (r"\bF_SLOT_(\d+)_NO_PROTECTION\b", r"\1번 분할의 보호 주문 생성 실패"),
    (r"\bF_SLOT_(\d+)_MANAGE_FAILURE\b", r"\1번 분할 자동 관리 실패"),
    (r"\bF(\d+)_TP2_OUT_OF_ORDER_FLATTEN\b", r"\1번 분할 2차 익절 선체결 정리"),
    (r"\bF(\d+)_TP1_MOVE_SL\b", r"\1번 분할 1차 익절 후 손절 이동"),
    (r"\bF(\d+)_MAX_HOLD\b", r"\1번 분할 보유 기간 초과"),
    (r"\bF_SLOT_(\d+)\b", r"\1번 분할"),
    (r"\bEMERGENCY_NO_ENTRY_SL\b", "진입 직후 손절 주문 실패"),
    (r"\bEMERGENCY_NO_SL\b", "손절 주문 실패"),
    (r"\bMAX_HOLD_(\d+)\b", r"보유 기간 \1봉 초과"),
    (r"\bMAX_HOLD\b", "보유 기간 초과"),
    (r"\bTRAIL_([\d.]+)\b", r"트레일 손절가 \1"),
    (r"\bTP2_RECOVER\b", "2차 익절 주문 재설정"),
    (r"\bRECOVER_SL\b", "손절 주문 재설정"),
    (r"\bENTRY_SL\b", "진입 손절 주문"),
    (r"\bMOVE_SL\b", "손절가 이동"),
    (r"_NO_ENTRY_SL\b", " / 진입 손절 주문 실패"),
    (r"\bmax_hold_(\d+)\b", r"보유 기간 \1봉 초과"),
    (r"\binitial_sl_emergency\b", "진입 손절 주문 실패로 긴급 종료"),
    (r"\bstate_recovered\b", "기존 포지션 이어받음"),
    (r"TP1 후 (\d+)% 락인 이동", r"1차 익절 후 손절가 \1% 확보"),
    (r"TP2 후 1차익절가 이동", "2차 익절 후 손절가를 1차 익절가로 이동"),
    (r"복원 후 SL 재배치", "재시작 후 손절 주문 재설정"),
)

# 진입 사유, 존, 청산 사유 등 값 자체를 한국어로 바꾼다.
VALUE_LABELS: Dict[str, str] = {
    "early_bull_div_candidate": "상승 다이버전스",
    "early_bear_div_candidate": "하락 다이버전스",
    "4h_cloud_doji_short": "4시간 구름대 도지",
    "f_volume_ema_or_ema50": "거래량 급증 + 이평 돌파",
    "bullish_reversal_started": "긴 아래꼬리 상승 반전 시작",
    "fvg_retracement": "FVG 되돌림",
    "squeeze_momentum": "스퀴즈 모멘텀",
    "flipped_4h_ob": "전환 4시간 OB",
    "flipped_1d_ob": "전환 일봉 OB",
    "flipped_1h_ob": "전환 1시간 OB",
    "1h_ob": "1시간 OB",
    "4h_ob": "4시간 OB",
    "1d_ob": "일봉 OB",
    "4h_cloud": "4시간 구름대",
    "1d_cloud": "일봉 구름대",
    "4h_bull_ob": "4시간 상승 OB",
    "4h_bear_ob": "4시간 하락 OB",
    "1h_bull_ob": "1시간 상승 OB",
    "1h_bear_ob": "1시간 하락 OB",
    "1d_bull_ob": "일봉 상승 OB",
    "1d_bear_ob": "일봉 하락 OB",
    "4h_flipped_bull_ob": "전환 4시간 상승 OB",
    "4h_flipped_bear_ob": "전환 4시간 하락 OB",
    "1d_flipped_bull_ob": "전환 일봉 상승 OB",
    "1d_flipped_bear_ob": "전환 일봉 하락 OB",
    "overlap": "중첩 존",
    "4h_cloud_senkou_a": "4시간 구름대 선행스팬A",
    "4h_cloud_senkou_b": "4시간 구름대 선행스팬B",
    "1d_cloud_senkou_a": "일봉 구름대 선행스팬A",
    "1d_cloud_senkou_b": "일봉 구름대 선행스팬B",
    "senkou_a": "선행스팬A",
    "senkou_b": "선행스팬B",
    "attenuated_countertrend": "추세 반대 감쇠",
    "epoch_quota_exhausted": "이번 전환 허용 횟수 소진",
    "zero_multiplier": "강한 전환으로 차단",
    "no_active_countertrend_regime": "활성 역추세 없음",
    "countertrend": "추세 반대",
    "tp1_exit": "1차 익절",
    "tp2_exit": "2차 익절 종료",
    "tp2_out_of_order_flatten": "2차 익절 선체결 정리",
    "stop_loss": "손절",
    "stop_after_tp1": "1차 익절 후 보호 손절",
    "trail_exit": "트레일링 종료",
    "exchange_position_missing": "거래소 포지션 없음",
    "startup_position_missing": "재시작 시 포지션 없음",
    "emergency_no_sl": "손절 주문 실패로 긴급 종료",
    "emergency_no_sl_position_gone": "손절 재설정 중 포지션 소멸",
    "emergency_no_sl_order_failed": "긴급 종료 주문 실패",
    "open": "보유",
    "none": "없음",
    "ON": "켜짐",
    "OFF": "꺼짐",
    "True": "완료",
    "False": "미완료",
    "alive": "정상",
    "bull": "상승",
    "bear": "하락",
    "flipped_bull": "전환 상승",
    "flipped_bear": "전환 하락",
    "long": "롱",
    "short": "숏",
    "1h": "1시간",
    "4h": "4시간",
    "1d": "일봉",
}

# 내부 라벨(key=)을 사용자 표현으로 바꾼다. 순서가 중요하다.
LABEL_PATTERNS: Tuple[Tuple[str, str], ...] = (
    (r"\bstatus=alive\b", "상태: 정상"),
    (r"\bstatus=buffer_entered\b", "상태: 관찰 구간 진입"),
    (r"\bstatus=partially_closed\b", "상태: 일부 청산"),
    (r"\bstatus=closed\b", "상태: 종료"),
    (r"\bstatus=open\b", "상태: 보유"),
    (r"\bposition=none\b", "포지션: 없음"),
    (r"\bposition=open\b", "포지션: 보유"),
    (r"\bposition=", "포지션: "),
    (r"\bstatus=", "상태: "),
    (r"\bstrategy=", "전략: "),
    (r"\bbot=", "봇: "),
    (r"\bsymbol=", "종목: "),
    (r"\blast_closed=", "최근봉: "),
    (r"\bbuffer_zones=", "관찰 존: "),
    (r"\bprice=", "현재가: "),
    (r"\bbuffer=", "관찰 범위: "),
    (r"\bsignal=", "신호: "),
    (r"\bside=", "방향: "),
    (r"\bbucket=", "존: "),
    (r"\bclose_reason=", "청산 사유: "),
    (r"\bentry_reason=", "진입 신호: "),
    (r"\bpolicy_reason=", "사유: "),
    (r"\breason=", "사유: "),
    (r"\bentry=", "진입가: "),
    (r"\bremain=", "남은 수량: "),
    (r"\bstop=", "손절가: "),
    (r"\bnew_sl=", "새 손절가: "),
    (r"(?i)\bSL=", "손절가: "),
    (r"(?i)\bTP1=", "1차 익절: "),
    (r"(?i)\bTP2=", "2차 익절: "),
    (r"(?i)\bTrail=", "트레일링: "),
    (r"\btrail_stop=", "트레일 손절가: "),
    (r"\bextreme=", "최고·최저가: "),
    (r"\bsize=", "수량: "),
    (r"\bslot=", "슬롯: "),
    (r"\bstate_slots=", "기록된 슬롯 수: "),
    (r"\bbars=", "경과 봉: "),
    (r"\bmissing=", "누락: "),
    (r"(누락: [^\n]*?)\bTP1\b", r"\g<1>1차 익절"),
    (r"(누락: [^\n]*?)\bTP2\b", r"\g<1>2차 익절"),
    (r"\bmultiplier=", "수량 배율: "),
    (r"\btimeframe=", "기준봉: "),
    (r"\bepoch=", "전환 번호: "),
    (r"\bstrength_atr=", "돌파 강도: "),
    (r"\bbase_size=", "기준 수량: "),
    (r"\btarget_size=", "주문 수량: "),
    (r"\bleader_bucket=", "대표 존: "),
    (r"\bATR=", "ATR: "),
    (r"\bpositionIdx=", "포지션 번호: "),
    (r"\bpath=", "파일: "),
    (r"\bdetail=", "상세: "),
    (r"\btype=", "유형: "),
    (r"\bmessage=", "내용: "),
    (r"\brole_epoch=", "전환 번호="),
    (r"\btransition_tf=", "전환 기준봉="),
    (r"\btransition_strength=", "전환 강도="),
    (r"\bscore=", "점수="),
    (r"\bob_type=", "유형="),
    (r"\bsource_tf=", "기준봉="),
    (r"\bzone_top=", "상단="),
    (r"\bzone_bottom=", "하단="),
    (r"\banchor_time=", "원본시각="),
    (r"\banchor_open=", "원본시가="),
    (r"\banchor_high=", "원본고가="),
    (r"\banchor_low=", "원본저가="),
    (r"\banchor_close=", "원본종가="),
    (r"\bimpulse_time=", "돌파시각="),
    (r"\btouch_time=", "터치시각="),
    (r"\bvolume_ratio20=", "거래량 배수="),
    (r"\bzone=", "존="),
    (r"\bwave=", "파동="),
    (r"\bearly=", "다이버전스 신호="),
    (r"\bdoji=", "도지 신호="),
    (r"\bshadow=", "기록만 남긴 거래="),
    (r"\bbackup=", "백업 파일="),
    (r"\[traceback\]", "[오류 추적]"),
    (r"\[recent bot\.log\]", "[최근 로그]"),
    (r"\[recent bot2\.log\]", "[최근 로그]"),
    (r"\[INFO\]", "[정보]"),
    (r"\[WARNING\]", "[경고]"),
    (r"\[ERROR\]", "[오류]"),
)

# 접두사와 기계 용어를 사용자 문장으로 바꾼다.
PHRASE_REPLACEMENTS: Tuple[Tuple[str, str], ...] = (
    ("[order-id-redacted]", "[주문번호 숨김]"),
    ("[telegram-token-redacted]", "[알림 토큰 숨김]"),
    ("[id-redacted]", "[식별자 숨김]"),
    ("[redacted]", "[숨김]"),
    ("[v2] ", ""),
    ("FVG+Squeeze (one-way)", "FVG 되돌림 + 스퀴즈 모멘텀"),
    ("MAX_HOLD 강제청산", "보유 기간 초과 종료"),
    ("MAX_HOLD 청산 주문 실패", "보유 기간 초과 종료 주문 실패"),
    ("aggregate 긴급 flatten 완료", "전체 포지션 긴급 종료 완료"),
    ("aggregate 긴급 flatten", "전체 포지션 긴급 종료"),
    ("긴급 flatten 대상 포지션 없음", "긴급 종료할 포지션 없음"),
    ("긴급 flatten 실패", "긴급 종료 실패"),
    ("긴급 flatten", "긴급 종료"),
    ("하드스탑", "손절 주문"),
    ("하드 스탑", "손절 주문"),
    ("state/거래소 포지션 불일치", "기록과 거래소 포지션 불일치"),
    ("state 파일 읽기 오류", "저장 기록 읽기 오류"),
    ("state 초기화", "저장 기록 초기화"),
    ("state 없이 계속 진행", "저장 기록 없이 계속 진행"),
    ("bot_state 무효 감지", "저장 기록 손상 감지"),
    ("one-way preflight 실패", "원웨이 모드 확인 실패"),
    ("원웨이 모드만 지원", "원웨이 모드에서만 동작"),
    ("트레일 직전 재조회", "트레일링 직전 재확인"),
    ("zone-role 확인 실패", "존 역할 확인 실패"),
    ("zone-role policy", "존 역할 정책"),
    ("zone-role", "존 역할"),
    ("F state 읽기 실패", "저장 기록 읽기 실패"),
    ("F state 형식 불일치", "저장 기록 형식 불일치"),
    ("F state 슬롯 수 불일치", "저장 기록 분할 수 불일치"),
    ("F state", "저장 기록"),
    ("보호구조 불완전", "보호 주문 일부 누락"),
    ("긴급청산", "긴급 종료"),
    ("SL 재설정", "손절 주문 재설정"),
    ("초기 SL", "진입 손절 주문"),
    ("상태 복원", "기존 포지션 이어받기"),
    ("-> 상태 종료", "→ 기록 정리"),
    ("(폴링)", "(실시간 감지)"),
    ("rate limit", "전송 제한"),
)


def localize(text: str) -> str:
    """내부 알림 문자열을 한국어 사용자 문구로 바꾼다."""
    if not text:
        return text

    localized = str(text)
    for source, replacement in PHRASE_REPLACEMENTS:
        localized = localized.replace(source, replacement)
    for pattern, replacement in REASON_CODE_PATTERNS:
        localized = re.sub(pattern, replacement, localized)
    for source, replacement in VALUE_LABELS.items():
        localized = re.sub(
            rf"(?<![A-Za-z0-9_]){re.escape(source)}(?![A-Za-z0-9_])",
            replacement,
            localized,
        )
    for pattern, replacement in LABEL_PATTERNS:
        localized = re.sub(pattern, replacement, localized)
    return localized


# 섹션 카드로 이미 정리된 알림 표시. bulletize가 이 표시가 있는 글을 다시 불릿으로
# 바꾸지 않게 하고, 실제 전송 직전에 strip_card_mark로 지운다(보이지 않는 문자).
CARD_MARK = "⁣"


def card(title: str, sections, footer: str = "") -> str:
    """제목 한 줄 + 빈 줄로 나눈 섹션 묶음 알림을 만든다.

    sections: (소제목, [줄, ...]) 목록. 소제목이 비면 줄만 쓴다. 빈 섹션은 건너뛴다.
    """
    blocks = [title.strip()]
    for heading, lines in sections:
        body = [str(line).strip() for line in lines if str(line or "").strip()]
        if not body:
            continue
        rendered = [f"  {line}" for line in body] if heading else body
        blocks.append("\n".join(([heading] if heading else []) + rendered))
    if footer:
        blocks.append(footer.strip())
    return CARD_MARK + "\n\n".join(blocks)


def kst_stamp(now=None) -> str:
    from datetime import datetime, timedelta, timezone
    kst = timezone(timedelta(hours=9))
    current = datetime.now(kst) if now is None else now
    if current.tzinfo is None:
        current = current.replace(tzinfo=timezone.utc)
    return current.astimezone(kst).strftime("%m-%d %H:%M KST")


def strip_card_mark(text: str) -> str:
    return str(text).replace(CARD_MARK, "") if text else text


def bulletize(text: str) -> str:
    """알림 하나를 제목 한 줄과 불릿 목록으로 정리한다."""
    if not text:
        return text
    if CARD_MARK in str(text):
        return str(text)

    output: List[str] = []
    title_written = False
    for raw_line in str(text).splitlines():
        line = raw_line.strip()
        if not line:
            if output and output[-1] != "":
                output.append("")
            continue

        parts = [part.strip() for part in line.split(" | ") if part.strip()]
        for part in parts:
            if not title_written:
                output.append(part)
                title_written = True
            elif part.startswith(("• ", "[")):
                output.append(part)
            else:
                output.append(f"• {part}")

    while output and output[-1] == "":
        output.pop()
    return "\n".join(output)


def render(text: str) -> str:
    """전송 직전 최종 사용자 문구를 만든다."""
    return bulletize(localize(text))
