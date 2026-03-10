import json
import logging
import queue
import re
import threading
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional

from ..config import Config
from ..db import DB


class SlackAPI:
    def __init__(
        self,
        bot_token: str,
        api_base: str,
        connect_timeout_sec: int,
        read_timeout_sec: int,
    ):
        self.bot_token = bot_token.strip()
        self.api_base = self._normalize_api_base(api_base)
        self.connect_timeout_sec = max(1, int(connect_timeout_sec))
        self.read_timeout_sec = max(1, int(read_timeout_sec))
        self.request_timeout_sec = self.connect_timeout_sec + self.read_timeout_sec
        self.opener = urllib.request.build_opener()

    @staticmethod
    def _normalize_api_base(value: str) -> str:
        raw = value.strip()
        if not raw:
            return "https://slack.com/api"
        parsed = urllib.parse.urlsplit(raw)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
        logging.warning("invalid slack.api_base=%r; fallback to https://slack.com/api", value)
        return "https://slack.com/api"

    def _call(self, method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.api_base}/{method}",
            data=body,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "Authorization": f"Bearer {self.bot_token}",
            },
            method="POST",
        )
        with self.opener.open(req, timeout=self.request_timeout_sec) as resp:
            raw = resp.read()
        parsed = json.loads(raw.decode("utf-8"))
        if not parsed.get("ok"):
            raise RuntimeError(f"slack api error method={method}: {parsed}")
        return parsed

    def post_message(self, channel: str, text: str) -> None:
        payload = {
            "channel": channel,
            "text": text,
            "mrkdwn": True,
            "unfurl_links": False,
            "unfurl_media": False,
        }
        self._call("chat.postMessage", payload)


class SlackService(threading.Thread):
    CHUNK_LIMIT = 3500

    def __init__(self, cfg: Config, db: DB, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.db = db
        self.stop_event = stop_event
        self.events_q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.api = SlackAPI(
            bot_token=cfg.slack_bot_token,
            api_base=cfg.slack_api_base,
            connect_timeout_sec=cfg.slack_connect_timeout_sec,
            read_timeout_sec=cfg.slack_read_timeout_sec,
        )

    def publish_event(self, event: Dict[str, Any]) -> None:
        self.events_q.put(event)

    def run(self) -> None:
        logging.info("slack service started")
        while not self.stop_event.is_set():
            self._flush_events(limit=50)
            self.stop_event.wait(self.cfg.poll_interval_sec)
        self._flush_events(limit=200)
        logging.info("slack service stopped")

    def _flush_events(self, limit: int) -> None:
        for _ in range(limit):
            try:
                event = self.events_q.get_nowait()
            except queue.Empty:
                break
            try:
                self._send_event(event)
            except urllib.error.HTTPError as e:
                logging.warning("slack post http error status=%s reason=%s", e.code, e.reason)
            except urllib.error.URLError as e:
                logging.warning("slack post network error: %s", e.reason)
            except Exception:
                logging.exception("slack post event failed")

    def _resolve_channel(self, event: Dict[str, Any]) -> Optional[str]:
        session_id = str(event.get("session_id") or "").strip()
        if session_id:
            row = self.db.get_managed_by_session_id(session_id)
            if row:
                alias = str(row["alias"] or "").strip()
                mapped = self.cfg.slack_channel_map.get(alias)
                if mapped:
                    return mapped
        return self.cfg.slack_default_channel or None

    @staticmethod
    def _escape(text: Any) -> str:
        s = str(text)
        s = s.replace("&", "&amp;")
        s = s.replace("<", "&lt;")
        s = s.replace(">", "&gt;")
        return s

    def _split_chunks(self, text: str) -> List[str]:
        normalized = text.replace("\r\n", "\n")
        if len(normalized) <= self.CHUNK_LIMIT:
            return [normalized]
        chunks: List[str] = []
        left = normalized
        while left:
            if len(left) <= self.CHUNK_LIMIT:
                chunks.append(left)
                break
            cut = self.CHUNK_LIMIT
            nl = left.rfind("\n", 0, cut + 1)
            if nl >= self.CHUNK_LIMIT // 2:
                cut = nl + 1
            else:
                sp = left.rfind(" ", 0, cut + 1)
                if sp >= self.CHUNK_LIMIT // 2:
                    cut = sp + 1
            chunks.append(left[:cut])
            left = left[cut:]
        return [c for c in chunks if c]

    def _format_event(self, event: Dict[str, Any]) -> str:
        etype = str(event.get("type") or "")
        session_id = str(event.get("session_id") or "")
        turn = str(event.get("turn_id") or "unknown")

        row = self.db.get_managed_by_session_id(session_id) if session_id else None
        if row:
            session_label = f"{row['alias']} ({session_id})"
        else:
            session_label = session_id or "unknown"

        if etype == "task_complete":
            lines = [
                "*Codex task complete*",
                f"*session:* `{self._escape(session_label)}`",
                f"*turn:* `{self._escape(turn)}`",
            ]
            primary = str(event.get("assistant_text_primary") or event.get("assistant_text") or "").strip()
            cont = str(event.get("assistant_text_continued") or "").strip()
            if primary:
                lines.append("")
                lines.append("*assistant summary:*")
                lines.append(self._escape(primary))
            if cont:
                lines.append("")
                lines.append("*continued available in next message chunk*" if len(cont) > 100 else self._escape(cont))
            return "\n".join(lines)

        if etype == "proposed_plan_ready":
            lines = [
                "*Codex plan ready for execution*",
                f"*session:* `{self._escape(session_label)}`",
                f"*turn:* `{self._escape(turn)}`",
                "Use Telegram to `/select` then `/approve`, or use one-tap approve there.",
            ]
            primary = str(event.get("assistant_text_primary") or event.get("assistant_text") or "").strip()
            if primary:
                lines.append("")
                lines.append("*assistant summary:*")
                lines.append(self._escape(primary))
            return "\n".join(lines)

        if etype == "request_user_input":
            return "\n".join(
                [
                    "*Codex request_user_input*",
                    f"*session:* `{self._escape(session_label)}`",
                    f"*turn:* `{self._escape(turn)}`",
                    "Please handle options from Telegram for now.",
                ]
            )

        compact = re.sub(r"\s+", " ", json.dumps(event, ensure_ascii=True))
        if len(compact) > 900:
            compact = compact[:900] + "..."
        return f"*Codex event:* `{self._escape(etype)}`\n{self._escape(compact)}"

    def _send_event(self, event: Dict[str, Any]) -> None:
        channel = self._resolve_channel(event)
        if not channel:
            logging.debug("skip slack event without target channel type=%s", event.get("type"))
            return

        text = self._format_event(event)
        chunks = self._split_chunks(text)
        for idx, chunk in enumerate(chunks, start=1):
            if idx == 1:
                payload = chunk
            else:
                payload = f"(continued {idx}/{len(chunks)})\n{chunk}"
            self.api.post_message(channel, payload)
