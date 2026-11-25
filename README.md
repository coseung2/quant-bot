# Bybit F_DCA Bot

Bybit **BTCUSDT 무기한 선물**을 15분봉 기준으로 자동매매하는 단일 봇입니다.
F 존과 거래량/EMA 신호로 진입하고, 세 슬롯(0.60 / 0.25 / 0.15)으로 나누어
물타기(DCA)와 불타기(Pyramid)를 수행한 뒤 TP1/TP2 분할 익절과 ATR 손절로 청산합니다.

> ⚠️ **실제 돈이 거래되는 프로그램입니다.**
> 이 코드는 어떠한 수익도 보장하지 않으며, 사용으로 발생하는 모든 손실은 사용자 책임입니다.
> 반드시 **Testnet → 최소 수량 → 소액** 순서로 검증한 뒤 확대하세요.

---

## 1. 요구사항

- Python **3.11 이상** (검증: 3.12)
- Bybit 계정 (USDT 선물 거래 가능)
- Windows / macOS / Linux

의존성은 `requirements.txt`에 고정되어 있습니다.

---

## 2. Bybit 준비 (가장 중요)

### 2-1. API 키

- 권한: **읽기 + 선물 거래**만. **출금 권한은 절대 켜지 마세요.**
- IP 화이트리스트를 켜는 것을 권장합니다.
- 키는 코드가 아니라 `.env`에만 넣습니다.

### 2-2. 포지션 모드 = 원웨이(One-Way)

이 봇은 **원웨이 모드에서만** 동작합니다. Bybit에서
`포지션 모드 → 원웨이`로 설정하세요. 헤지 모드면 봇이 시작 직후
신규 진입을 멈추고 텔레그램으로 경고합니다.

### 2-3. 레버리지와 증거금

- 봇은 수량을 **BTC로** 고정하므로 레버리지는 필요 증거금에만 영향을 줍니다.
- 계정에서 직접 레버리지를 설정하거나, `.env`에서 `AUTO_SET_LEVERAGE=true`로
  `ENTRY_LEVERAGE` 값을 자동 적용할 수 있습니다.
- `POSITION_SIZE_BTC`에 해당하는 증거금이 계정에 있어야 합니다.

### 2-4. Testnet

처음에는 Bybit **Testnet** API 키를 발급해 `BYBIT_TESTNET=true`로 검증하세요.

---

## 3. 설치

```bash
git clone <이 저장소 URL>
cd quant-bot

python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
# macOS / Linux
source .venv/bin/activate

pip install -r requirements.txt
```

`.env` 파일을 만듭니다.

```bash
# macOS / Linux
cp .env.example .env
```

```powershell
# Windows PowerShell
Copy-Item .env.example .env
```

---

## 4. 설정

`.env`를 열어 최소 두 항목을 채웁니다.

| 변수 | 필수 | 설명 |
|---|---|---|
| `BYBIT_API_KEY` | ✅ | 본인 API 키 |
| `BYBIT_API_SECRET` | ✅ | 본인 API 시크릿 |
| `POSITION_SIZE_BTC` | ✅ | 총 캠페인 수량(BTC). **최소 0.002**, 처음엔 아주 작게 |
| `BYBIT_TESTNET` | | 기본 `true`. 실거래 시 `false` |
| `SYMBOL` | | 기본 `BTC/USDT:USDT` |
| `ENTRY_LEVERAGE` | | 레버리지 폴백 값 (기본 100) |
| `AUTO_SET_LEVERAGE` | | 기본 `false`. `true`면 시작 시 레버리지 설정 |
| `BOT1_PYR_MODE` | | `accumulate`(기본) 또는 `cyclic` |
| `TELEGRAM_BOT_TOKEN` | | 선택. 없으면 알림만 생략 |
| `TELEGRAM_CHAT_ID` | | 선택 |

> `POSITION_SIZE_BTC`를 나중에 바꾸면 기존 상태 파일과 불일치로 봇이 멈춥니다.
> **보유 포지션이 없을 때** `bot1_dca_state.json`을 지우고 다시 시작하세요.

---

## 5. 실행

```bash
python bot.py
```

- 첫 실행 시 `bot1_dca_state.json`, `bot1_f_state.json`, `bot1_f_session.lock`이 생성됩니다.
- 중지: `Ctrl+C`. 거래소에 걸어 둔 보호 주문(손절/익절)은 유지됩니다.
- 상주 실행은 Windows **작업 스케줄러**, Linux **systemd**, 또는 `tmux`/`screen`을 사용하세요.

---

## 6. 동작 요약

| 단계 | 내용 |
|---|---|
| 진입 | F 존 + 거래량/EMA 조건 충족 시 세 슬롯으로 분할 진입 |
| DCA | 포지션이 역행하면 조건에 따라 추가 진입(물타기) |
| Pyramid | `accumulate`(적립형) / `cyclic`(순환형) 중 선택 |
| 청산 | TP1/TP2 분할 익절, ATR 기반 손절·트레일링 |
| 알림 | Telegram (설정한 경우) |

---

## 7. 파일 구조

```
bot.py                     # 진입점 (설정·거래소 연결·메인 루프)
f_dca_live.py              # F_DCA 엔진 (슬롯·물타기·불타기·청산)
cyclic_position_rules.py   # 순환형 사이징 규칙
f_slot_accounting.py       # 체결·손익 회계
zone_role_attenuation.py   # F 존 역할 판정
zone_cloud_policy.py       # F 존 클라우드 정책
bot1_signal_context.py     # 신호 컨텍스트 계산
bot1_wave_state.py         # 파동 상태
bot1_market_loader.py      # 공개 시세 로더
telegram_messages.py       # 메시지 포맷
telegram_commands.py       # 텔레그램 명령(/mode 등)
requirements.txt           # 고정 의존성
.env.example               # 환경변수 예시
```

---

## 8. Telegram 알림 (선택)

1. BotFather에서 봇을 만들고 토큰을 받습니다.
2. 본인 chat id를 확인합니다.
3. `.env`에 `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID`를 넣습니다.

비워 두면 알림만 생략되고 매매는 정상 동작합니다.
`/mode` 명령으로 불타기 모드를 전환할 수 있습니다.

---

## 9. 대시보드 (별도)

봇은 체결/손익을 `bot_dashboard/bot_dashboard.db`에 기록합니다.
웹 대시보드는 이 저장소에 포함되어 있지 않으며, 필요하면 별도로 추가할 수 있습니다.

---

## 10. 문제 해결

| 증상 | 확인 |
|---|---|
| `POSITION_SIZE_BTC is not set` | `.env`에 수량을 넣었는지 |
| `derived TP leg is below minimum` | 총 수량이 0.002 BTC 이상인지 |
| `BYBIT_API_KEY ... are not set` | `.env` 위치가 실행 디렉터리인지 |
| 시작 직후 진입 중단 + 원웨이 경고 | Bybit 포지션 모드를 원웨이로 |
| `DCA state/config mismatch` | 수량 변경 후 상태 파일이 남아 있음. 포지션 없을 때 삭제 |
| API 401/403 | 키 권한·IP 화이트리스트·Testnet 여부 |
| 텔레그램 알림 없음 | 토큰/chat id, 봇에게 먼저 말을 건 뒤 chat id 확인 |
| 주문 거부(수량) | 최소 주문 수량, 증거금, 레버리지 |

---

## 11. 면책

이 소프트웨어는 **있는 그대로(as-is)** 제공되며, 명시적이든 묵시적이든 어떠한 보증도 하지 않습니다.
자동매매로 인한 손실, 거래소 장애, API 변경, 네트워크 문제 등 어떤 손해에도 작성자는 책임지지 않습니다.
사용 전 Testnet에서 충분히 검증하고, 감당 가능한 금액만 사용하세요.
