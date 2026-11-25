"""읽기 전용 설치 점검. 비밀값은 절대 출력하지 않는다.

사용법:  python preflight.py
"""
import os
import sys

from dotenv import load_dotenv

REQUIRED = ("BYBIT_API_KEY", "BYBIT_API_SECRET", "POSITION_SIZE_BTC")
MIN_SIZE = 0.002


def main() -> int:
    load_dotenv()
    problems = []

    for name in REQUIRED:
        if not os.getenv(name, "").strip():
            problems.append(f"{name} is not set in .env")

    raw_size = os.getenv("POSITION_SIZE_BTC", "").strip()
    try:
        size = float(raw_size) if raw_size else 0.0
    except ValueError:
        size = 0.0
        problems.append("POSITION_SIZE_BTC is not a number")
    if 0 < size < MIN_SIZE:
        problems.append(
            f"POSITION_SIZE_BTC {size:g} is below the minimum {MIN_SIZE:g}"
        )

    testnet = os.getenv("BYBIT_TESTNET", "true").strip().lower() == "true"
    symbol = os.getenv("SYMBOL", "BTC/USDT:USDT")
    print(f"testnet={testnet} symbol={symbol} position_size={size:g}")

    if problems:
        for item in problems:
            print(f"[FAIL] {item}")
        return 1

    import ccxt

    exchange = ccxt.bybit({
        "apiKey": os.environ["BYBIT_API_KEY"],
        "secret": os.environ["BYBIT_API_SECRET"],
        "enableRateLimit": True,
        "options": {"defaultType": "linear"},
    })
    if testnet:
        exchange.set_sandbox_mode(True)

    try:
        balance = exchange.fetch_balance()
    except Exception as exc:  # noqa: BLE001 - report the exchange error to the user
        print(f"[FAIL] fetch_balance: {type(exc).__name__}: {exc}")
        return 1
    usdt = balance.get("USDT") or {}
    print(f"USDT free={usdt.get('free', 0)} total={usdt.get('total', 0)}")

    try:
        positions = exchange.fetch_positions([symbol])
    except Exception as exc:  # noqa: BLE001 - report the exchange error to the user
        print(f"[FAIL] fetch_positions: {type(exc).__name__}: {exc}")
        return 1

    indexes = set()
    for item in positions or []:
        value = item.get("positionIdx")
        if value is None:
            value = (item.get("info") or {}).get("positionIdx")
        if value is not None:
            indexes.add(str(value))

    if not indexes:
        print("[WARN] 열린 포지션이 없어 포지션 모드를 확인할 수 없습니다.")
        print("       Bybit UI에서 포지션 모드가 '원웨이'인지 직접 확인하세요.")
    else:
        print(f"positionIdx={sorted(indexes)}")
        if any(value != "0" for value in indexes):
            print("[FAIL] 헤지/양방향 모드입니다. Bybit에서 '원웨이'로 바꾸세요.")
            return 1

    print("[OK] read-only preflight passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
