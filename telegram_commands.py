"""Telegram commands for bot1: change the pyramid mode from chat.

Polls getUpdates (no webhook, no open port) and only accepts messages from the
configured TELEGRAM_CHAT_ID. The chosen mode is written to a small JSON file so it
survives restarts; f_dca_live reads it when a new campaign is created, so an open
campaign keeps the mode it started with.

Commands
  /mode               show the current mode
  /mode accumulate    적립형 (also: /mode 적립, /적립형)
  /mode cyclic        순환형 (also: /mode 순환, /순환형)
"""
import json
import os
import time
import urllib.parse
import urllib.request

MODE_FILE = os.getenv("BOT1_PYR_MODE_FILE", "bot1_pyr_mode.json")
POLL_INTERVAL_SEC = 5.0
STARTUP_GRACE_SEC = 5
MODES = {"accumulate": "적립형", "cyclic": "순환형"}
ALIASES = {"accumulate": "accumulate", "acc": "accumulate", "적립": "accumulate", "적립형": "accumulate",
           "cyclic": "cyclic", "cycle": "cyclic", "순환": "cyclic", "순환형": "cyclic"}


def read_mode(default):
    """Mode saved by a Telegram command, else `default`. Never raises."""
    try:
        with open(MODE_FILE, encoding="utf-8") as handle:
            mode = json.load(handle).get("mode")
    except (OSError, ValueError, AttributeError):
        return default
    return mode if mode in MODES else default


def write_mode(mode, who):
    if mode not in MODES:
        raise ValueError("unknown mode")
    temporary = f"{MODE_FILE}.tmp"
    with open(temporary, "w", encoding="utf-8") as handle:
        json.dump({"mode": mode, "by": who, "at": int(time.time())}, handle)
    os.replace(temporary, MODE_FILE)


def parse(text):
    """Return ('show', None), ('set', mode), ('bad', None) or None for unrelated text."""
    words = (text or "").strip().split()
    if not words:
        return None
    head = words[0].lower().split("@", 1)[0]
    if head in ("/적립형", "/순환형"):
        return ("set", ALIASES[head[1:]])
    if head != "/mode":
        return None
    if len(words) == 1:
        return ("show", None)
    mode = ALIASES.get(words[1].lower())
    return ("set", mode) if mode else ("bad", None)


class TelegramCommands:
    def __init__(self, token, chat_id, default_mode, open_mode=None, http=None, now=time.time, log=None):
        self.token = token
        self.chat_id = str(chat_id or "")
        self.default_mode = default_mode
        self.open_mode = open_mode or (lambda: None)
        self.http = http or self._http
        self.now = now
        self.log = log or (lambda text: None)
        self.offset = None
        self.next_poll = 0.0
        self.started = self.now()
        self.enabled = bool(token and self.chat_id)

    def _http(self, method, params):
        url = f"https://api.telegram.org/bot{self.token}/{method}"
        data = urllib.parse.urlencode(params).encode("utf-8")
        with urllib.request.urlopen(urllib.request.Request(url, data=data, method="POST"), timeout=10) as resp:
            return json.loads(resp.read().decode("utf-8"))

    def reply(self, text):
        try:
            self.http("sendMessage", {"chat_id": self.chat_id, "text": text})
        except Exception:
            pass

    def status_text(self):
        mode = read_mode(self.default_mode)
        lines = [f"불타기 모드: {MODES[mode]} ({mode})", "새 캠페인부터 적용됩니다."]
        current = self.open_mode()
        if current:
            lines.append(f"지금 열린 캠페인: {MODES.get(current, current)} (끝날 때까지 유지)")
        lines.append("바꾸기: /mode 적립 · /mode 순환")
        return "\n".join(lines)

    def handle(self, message):
        chat = str((message.get("chat") or {}).get("id", ""))
        if chat != self.chat_id:
            if parse(message.get("text")) is not None:
                self.log("텔레그램 명령 무시: 설정된 채팅이 아님")
            return  # only the configured chat may change the bot
        command = parse(message.get("text"))
        if command is None:
            return
        kind, mode = command
        self.log(f"텔레그램 명령 수신: {kind} {mode or ''}".strip())
        if kind == "bad":
            self.reply("모르는 모드입니다. /mode 적립 또는 /mode 순환")
            return
        if kind == "set":
            before = read_mode(self.default_mode)
            write_mode(mode, chat)
            head = "그대로입니다" if before == mode else f"{MODES[before]} → {MODES[mode]}"
            self.reply(f"✅ 불타기 모드 {head}\n" + self.status_text())
            return
        self.reply(self.status_text())

    def poll(self):
        """Fetch and handle new messages at most every POLL_INTERVAL_SEC. Never raises."""
        if not self.enabled or self.now() < self.next_poll:
            return
        self.next_poll = self.now() + POLL_INTERVAL_SEC
        try:
            params = {"timeout": 0, "allowed_updates": json.dumps(["message"])}
            if self.offset is not None:
                params["offset"] = self.offset
            result = self.http("getUpdates", params)
            updates = result.get("result") or []
            for update in updates:
                self.offset = max(self.offset or 0, update["update_id"] + 1)
                message = update.get("message")
                if not message:
                    continue
                # Skip messages sent before this bot process started (sent while it was down).
                if int(message.get("date", 0)) < self.started - STARTUP_GRACE_SEC:
                    continue
                self.handle(message)
        except Exception as exc:
            self.log(f"텔레그램 명령 확인 실패: {type(exc).__name__}")
            return
