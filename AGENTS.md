# AGENTS.md — 에이전트용 설치 가이드

이 문서는 **Claude Code, Codex, Cursor 등 코딩 에이전트**가 이 저장소를 받은 사람 대신
설치·검증까지 수행하기 위한 절차입니다. 사람이 읽어도 됩니다.

사용자가 "이 봇 세팅해줘"라고 하면 아래 순서를 따른다.

---

## 0. 절대 규칙 (안전)

1. **실거래 금지 기본값**: `BYBIT_TESTNET=false`는 사용자가 "실거래"라고 **명시적으로 확인한 뒤에만** 설정한다.
2. **비밀값 취급**: API 키·시크릿·텔레그램 토큰을 채팅, 로그, 커밋, 에코, 보고서에 **절대 출력하지 않는다.**
   사용자가 채팅에 붙여넣으려 하면 `.env` 파일에 직접 입력하도록 안내한다.
3. `.env`를 Git에 추가·커밋하지 않는다.
4. `POSITION_SIZE_BTC`는 **사용자에게 직접 물어** 정한다. 불명확하면 최소값 `0.002`로 두고
   실거래 전에 다시 확인한다.
5. 전략 파라미터(진입·청산·불타기 규칙)를 임의로 바꾸지 않는다.
6. 상태 파일(`bot1_f_state.json`, `bot1_dca_state.json`)은 **열린 포지션이 없음을 확인한 뒤에만** 삭제한다.
7. **설치는 자동, 실거래 시작은 사람 승인.** 실행 직전에 체크리스트를 보여주고 승인받는다.
8. 사용자가 요청하지 않으면 `git commit`/`push`를 하지 않는다.

---

## 1. 환경 확인

```bash
python --version
```

- Python **3.11 이상** 필요. 없으면 설치 방법만 안내하고 임의로 시스템을 바꾸지 않는다.

---

## 2. 의존성 설치

```bash
python -m venv .venv
```

```powershell
# Windows PowerShell
.venv\Scripts\Activate.ps1
```

```bash
# macOS / Linux
source .venv/bin/activate
```

```bash
pip install -r requirements.txt
```

---

## 3. 설정 파일 만들기

```powershell
# Windows PowerShell
Copy-Item .env.example .env
```

```bash
# macOS / Linux
cp .env.example .env
```

그다음 사용자에게 **세 가지**를 요청한다.

| 항목 | 안내 |
|---|---|
| Bybit API 키 | 읽기 + 선물 거래 권한만. **출금 권한 금지**, IP 화이트리스트 권장 |
| `POSITION_SIZE_BTC` | 총 캠페인 수량(BTC). 최소 `0.002`, 처음엔 작게 |
| Testnet 여부 | 기본 `true`. 실거래는 명시적 확인 후에만 |

키는 사용자가 `.env`에 직접 입력하게 한다. 에이전트는 값을 읽거나 출력하지 않는다.

---

## 4. 읽기 전용 점검

```bash
python preflight.py
```

이 스크립트는 비밀값을 출력하지 않고 아래를 확인한다.

- 필수 환경변수 존재 여부(이름만)
- `POSITION_SIZE_BTC` 최소값
- Testnet 연결
- USDT 잔고
- 포지션 모드(원웨이 여부)

실패하면 원인을 설명하고 `README.md`의 "문제 해결"을 참고하게 한다.
**이 단계에서 봇을 실행하지 않는다.**

---

## 5. 실행 전 체크리스트 (사용자 승인)

아래를 표로 보여주고 승인을 받는다.

- [ ] Testnet인가? (`BYBIT_TESTNET`)
- [ ] 종목 (`SYMBOL`, 기본 `BTC/USDT:USDT`)
- [ ] 수량 (`POSITION_SIZE_BTC`) — 손실 가능 금액인가?
- [ ] Bybit 포지션 모드 = **원웨이**
- [ ] 레버리지 설정 방식 (`AUTO_SET_LEVERAGE`)
- [ ] Telegram 알림 사용 여부

---

## 6. 실행

```bash
python bot.py
```

- 처음에는 **포그라운드**로 띄워 로그를 확인한다.
- 정상 기동이 확인되면 사용자가 원할 때만 백그라운드/서비스로 전환한다.
- 첫 실행 시 `bot1_f_state.json`, `bot1_dca_state.json`, `bot1_f_session.lock`이 생성된다.
- 중지: `Ctrl+C`. 거래소의 보호 주문은 유지된다.

---

## 7. 완료 보고

아래만 보고한다.

- 실행 여부와 프로세스 상태
- Testnet 여부, 종목, 수량
- `preflight.py` 결과 요약
- 로그 파일 위치
- 남은 수동 작업(예: Bybit UI에서 원웨이 확인)

비밀값은 보고서에 포함하지 않는다.

---

## 8. 하지 말 것

- API 키를 코드에 하드코딩
- `AUTO_SET_LEVERAGE=true`를 임의로 켜기
- 대시보드·백테스트 데이터를 이 저장소에 추가
- 사용자 요청 없는 `git commit`/`push`
- `requirements.txt` 버전을 근거 없이 올리기
