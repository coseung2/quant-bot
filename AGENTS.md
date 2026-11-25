# AGENTS.md — 에이전트용 설치 가이드

이 문서는 **Claude Code, Codex, Cursor 등 코딩 에이전트**가 이 저장소를 받은 사람 대신
설치·검증까지 수행하기 위한 절차입니다. 사람이 읽어도 됩니다.

사용자가 "이 봇 세팅해줘"라고 하면 아래 순서를 따른다.

---

## 0. 절대 규칙 (안전)

1. **실거래 금지 기본값**: `BYBIT_TESTNET=false`는 사용자가 "실거래"라고 **명시적으로 확인한 뒤에만** 설정한다.
2. **비밀값 취급**: API 키·시크릿·텔레그램 토큰을 채팅, 로그, 커밋, 에코, 보고서에 **절대 출력하지 않는다.**
   브라우저 보조 중에도 시크릿이 표시되는 화면을 스크린샷·DOM 읽기·클립보드 읽기로 가져오지 않는다.
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

## 3-B. API 키 발급 (브라우저 보조 · 선택)

에이전트에 브라우저 도구(Claude in Chrome, 내장 브라우저 등)가 있으면 **사용자가 이미 로그인한
세션에서** 발급 화면을 열어 폼만 채워 줄 수 있다. 브라우저 도구가 없으면 이 절을 건너뛰고
사용자가 직접 발급한다.

> 이 봇은 **Bybit 전용**이다. Binance API로는 동작하지 않는다.
> Testnet은 `testnet.bybit.com`에서 **별도 계정·별도 키**를 발급한다.

### 에이전트가 해도 되는 것

- Bybit의 API 관리 화면을 연다. **이미 로그인된 세션에서만.**
- 키 이름(라벨)을 제안하고 사용자와 확인한다.
- 권한을 **읽기 + 선물 거래**로 맞춘다.
- IP 화이트리스트를 켤지 묻고, 사용자가 알려 준 IP를 입력한다.
- **출금 권한이 꺼져 있는지** 확인한다.

### 에이전트가 절대 하지 않는 것

- 비밀번호·2FA·이메일 인증코드 입력 (사용자가 직접)
- 출금(Withdraw) 권한 켜기
- 시크릿이 표시된 화면의 스크린샷·DOM 텍스트·클립보드 읽기
- 시크릿을 채팅·로그·커밋·보고서에 남기기
- 사용자 확인 없이 최종 "Create / 확인" 클릭

### 절차

1. 사용자에게 Bybit **로그인 상태**와 **Testnet/실거래** 여부를 확인한다.
2. API 관리 화면을 연다.
3. 폼을 채우기 전에 아래를 표로 보여주고 승인받는다.

| 화면 항목 | 값 |
|---|---|
| Key name | `f-dca-bot` 등 식별 가능한 이름 |
| Permissions | Read-Write |
| Contract / Unified Trading | 켜기 |
| Withdraw | **끄기** |
| IP restriction | 켜기 권장 (사용자 서버 IP) |

> IP 화이트리스트를 켜면 **봇이 도는 서버의 IP만** 허용된다. IP를 모르거나 유동 IP면 켜지 말고,
> 켤 경우 고정 IP를 넣는다. 잘못 넣으면 봇이 401/403으로 멈춘다.

4. 폼을 채운 뒤 **최종 생성 직전에 멈추고** 사용자에게 넘긴다.
5. 사용자가 2FA·생성을 마치고, 표시된 값을 **직접** `.env`에 넣는다.

| 화면 값 | `.env` 변수 |
|---|---|
| API Key | `BYBIT_API_KEY` |
| API Secret | `BYBIT_API_SECRET` |

6. 에이전트는 `python preflight.py`로 **존재 여부만** 확인한다.

### 권한 항목 이름이 다를 때

Bybit UI 문구는 버전에 따라 다르다. "Read-Write", "Contract Trading", "Unified Trading",
"Withdraw" 같은 항목을 기준으로 판단하고, 애매하면 **사용자에게 확인**한다.
출금 권한이 이미 켜진 키를 발견하면 사용을 멈추고 새 키를 만들도록 안내한다.

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
