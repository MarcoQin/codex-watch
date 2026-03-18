import dataclasses
import datetime as dt
import json
import logging
import os
import re
import sqlite3
import threading
import collections
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .bus import NotificationBus
from .config import Config
from .db import DB
from .common import utc_ts

@dataclasses.dataclass
class SessionState:
    current_turn_id: Optional[str] = None
    proposed_plan_turns: set = dataclasses.field(default_factory=set)
    assistant_text_by_turn: Dict[str, str] = dataclasses.field(default_factory=dict)

class SessionMonitor(threading.Thread):
    def __init__(self, cfg: Config, db: DB, bus: NotificationBus, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.db = db
        self.bus = bus
        self.stop_event = stop_event
        self.states: Dict[str, SessionState] = {}

    def run(self) -> None:
        logging.info("session monitor started")
        while not self.stop_event.is_set():
            try:
                self.scan_once()
            except Exception:
                logging.exception("monitor scan failed")
            self.stop_event.wait(self.cfg.scan_interval_sec)
        logging.info("session monitor stopped")

    def scan_once(self) -> None:
        root = Path(self.cfg.sessions_root)
        if not root.exists():
            return

        files = sorted(root.rglob("rollout-*.jsonl"))
        now = utc_ts()
        for file_path in files:
            f = str(file_path)
            try:
                stat = file_path.stat()
            except FileNotFoundError:
                continue

            rec = self.db.get_session_file(f)
            if rec is None:
                is_old = int(stat.st_mtime) < (now - 120)
                skip_history = 1 if is_old else 0
                offset = int(stat.st_size) if is_old else 0
                should_backfill = is_old and self.cfg.backfill_lines > 0
                self.db.upsert_session_file(
                    file_path=f,
                    last_offset=offset,
                    skip_history=skip_history,
                    backfill_done=0 if should_backfill else 1,
                    last_mtime=int(stat.st_mtime),
                )
                self._parse_session_meta_if_needed(f)
                rec = self.db.get_session_file(f)
                if rec is None:
                    continue
            else:
                self._parse_session_meta_if_needed(f)

            if int(rec["backfill_done"]) == 0:
                if self.cfg.backfill_lines > 0:
                    self._backfill_file(f, self.cfg.backfill_lines)
                self.db.mark_session_file_backfill_done(f)
                rec = self.db.get_session_file(f)
                if rec is None:
                    continue

            offset = int(rec["last_offset"])
            if stat.st_size < offset:
                offset = 0

            with file_path.open("r", encoding="utf-8", errors="replace") as fh:
                fh.seek(offset)
                while True:
                    line = fh.readline()
                    if not line:
                        break
                    offset = fh.tell()
                    self._handle_line(f, line, mode="live")

            self.db.update_session_file_offset(f, offset, int(stat.st_mtime))

    def _parse_session_meta_if_needed(self, file_path: str) -> None:
        rec = self.db.get_session_file(file_path)
        if rec and rec["session_id"]:
            self._try_attach_existing_session(rec)
            return
        p = Path(file_path)
        if not p.exists():
            return
        try:
            with p.open("r", encoding="utf-8", errors="replace") as fh:
                first = fh.readline()
        except OSError:
            return
        if not first.strip():
            return
        try:
            data = json.loads(first)
        except json.JSONDecodeError:
            return
        if data.get("type") != "session_meta":
            return
        payload = data.get("payload", {}) if isinstance(data.get("payload"), dict) else {}
        session_id = payload.get("id")
        cwd = payload.get("cwd")
        ts_raw = payload.get("timestamp")
        session_ts: Optional[int] = None
        if isinstance(ts_raw, str):
            try:
                session_ts = int(dt.datetime.fromisoformat(ts_raw.replace("Z", "+00:00")).timestamp())
            except ValueError:
                session_ts = None
        self.db.update_session_file_meta(file_path, str(session_id) if session_id else None, str(cwd) if cwd else None)
        if session_id:
            launch_nonce, nonce_conflict = self._resolve_launch_nonce(file_path, str(session_id))
            if nonce_conflict:
                logging.warning("session %s nonce conflict detected; refusing auto-attach", session_id)
                return
            alias, attach_reason = self.db.maybe_attach_session_id(
                str(session_id),
                str(cwd) if cwd else None,
                session_ts,
                launch_nonce,
            )
            if alias:
                if attach_reason == "nonce_rotate":
                    logging.info("reattached session_id %s -> alias %s strategy=%s", session_id, alias, attach_reason)
                else:
                    logging.info("attached session_id %s -> alias %s strategy=%s", session_id, alias, attach_reason)
            else:
                if attach_reason in ("nonce_ambiguous_unbound", "nonce_ambiguous_existing"):
                    logging.info(
                        "session %s not auto-attached in strict nonce mode (launch_nonce=%s, reason=%s)",
                        session_id,
                        launch_nonce,
                        attach_reason,
                    )
                elif launch_nonce:
                    logging.info(
                        "session %s not auto-attached in strict nonce mode (launch_nonce=%s, reason=%s)",
                        session_id,
                        launch_nonce,
                        attach_reason,
                    )
                else:
                    logging.info(
                        "session %s not auto-attached in strict nonce mode (missing launch_nonce, reason=%s), use manual attach if needed",
                        session_id,
                        attach_reason,
                    )

    def _try_attach_existing_session(self, rec: sqlite3.Row) -> None:
        session_id = rec["session_id"]
        if not session_id:
            return
        session_id_str = str(session_id)
        if self.db.get_managed_by_session_id(session_id_str):
            return
        cwd_val = str(rec["cwd"]) if rec["cwd"] else None
        file_path = str(rec["file_path"])
        launch_nonce, nonce_conflict = self._resolve_launch_nonce(file_path, session_id_str)
        if nonce_conflict:
            logging.warning("session %s nonce conflict detected on existing attach path", session_id_str)
            return
        alias, attach_reason = self.db.maybe_attach_session_id(session_id_str, cwd_val, None, launch_nonce)
        if alias:
            if attach_reason == "nonce_rotate":
                logging.info("reattached existing session_id %s -> alias %s strategy=%s", session_id_str, alias, attach_reason)
            else:
                logging.info("attached existing session_id %s -> alias %s strategy=%s", session_id_str, alias, attach_reason)
        else:
            if launch_nonce:
                logging.info(
                    "existing session %s not auto-attached (launch_nonce=%s, reason=%s)",
                    session_id_str,
                    launch_nonce,
                    attach_reason,
                )
            else:
                logging.info(
                    "existing session %s not auto-attached (missing launch_nonce, reason=%s)",
                    session_id_str,
                    attach_reason,
                )

    def _resolve_launch_nonce(self, file_path: str, session_id: str) -> Tuple[Optional[str], bool]:
        content_nonce = self._extract_launch_nonce_from_session_content(file_path)
        snapshot_nonce = self._extract_launch_nonce_from_snapshot(session_id)
        if content_nonce and snapshot_nonce and content_nonce != snapshot_nonce:
            rows = self.db.mark_managed_awaiting_manual_attach_for_nonces([content_nonce, snapshot_nonce])
            logging.warning(
                "launch_nonce conflict for session_id=%s content_nonce=%s snapshot_nonce=%s affected_rows=%s",
                session_id,
                content_nonce,
                snapshot_nonce,
                rows,
            )
            return None, True
        return (content_nonce or snapshot_nonce), False

    def _extract_launch_nonce_from_session_content(self, file_path: str, max_lines: int = 240) -> Optional[str]:
        path = Path(file_path)
        if not path.exists():
            return None
        try:
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                for i, line in enumerate(fh):
                    if i >= max_lines:
                        break
                    nonce = self._extract_nonce_from_text(line)
                    if nonce:
                        return nonce
        except OSError:
            return None
        return None

    def _extract_nonce_from_text(self, text: str) -> Optional[str]:
        if not text:
            return None
        patterns = [
            r'CODEX_WATCH_NONCE=("[^"]+"|\'[^\']+\'|[A-Za-z0-9._:-]+)',
            r'"launch_nonce"\s*:\s*"([^"]+)"',
            r'"CODEX_WATCH_NONCE"\s*:\s*"([^"]+)"',
        ]
        for pattern in patterns:
            match = re.search(pattern, text)
            if not match:
                continue
            raw = match.group(1).strip()
            if not raw:
                continue
            if (raw.startswith("'") and raw.endswith("'")) or (raw.startswith('"') and raw.endswith('"')):
                raw = raw[1:-1]
            nonce = raw.split()[0].strip()
            if not nonce:
                continue
            if re.fullmatch(r"[A-Za-z0-9._:-]+", nonce):
                return nonce
        return None

    def _extract_launch_nonce_from_snapshot(self, session_id: str) -> Optional[str]:
        snapshot_path = Path(os.path.expanduser(f"~/.codex/shell_snapshots/{session_id}.sh"))
        if not snapshot_path.exists():
            return None
        try:
            with snapshot_path.open("r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    if "CODEX_WATCH_NONCE=" not in line:
                        continue
                    match = re.search(r"CODEX_WATCH_NONCE=(.+)", line)
                    if not match:
                        continue
                    raw = match.group(1).strip()
                    if not raw:
                        continue
                    if (raw.startswith("'") and "'" in raw[1:]) or (raw.startswith('"') and '"' in raw[1:]):
                        quote = raw[0]
                        end = raw.find(quote, 1)
                        if end > 1:
                            return raw[1:end]
                    return raw.split()[0]
        except OSError:
            return None
        return None

    def _backfill_file(self, file_path: str, backfill_lines: int) -> None:
        path = Path(file_path)
        if not path.exists():
            return
        tail: collections.deque[str] = collections.deque(maxlen=backfill_lines)
        try:
            with path.open("r", encoding="utf-8", errors="replace") as fh:
                for line in fh:
                    tail.append(line)
        except OSError:
            return
        if not tail:
            return
        for line in tail:
            self._handle_line(file_path, line, mode="backfill")

    def _state_for(self, session_key: str) -> SessionState:
        if session_key not in self.states:
            self.states[session_key] = SessionState()
        return self.states[session_key]

    def _extract_session_key(self, file_path: str) -> str:
        sid = self.db.resolve_session_id_for_path(file_path)
        return sid if sid else file_path

    @staticmethod
    def _resolve_turn_id(payload: Dict[str, Any], fallback: Optional[str]) -> str:
        turn_id = payload.get("turn_id")
        if isinstance(turn_id, str) and turn_id:
            return turn_id
        return fallback or "unknown"

    @staticmethod
    def _clean_text(value: Any) -> str:
        if not isinstance(value, str):
            return ""
        return value.strip()

    def _remember_assistant_text(self, state: SessionState, turn_id: str, text: str) -> None:
        cleaned = self._clean_text(text)
        if not cleaned:
            return
        state.assistant_text_by_turn[turn_id] = cleaned
        if len(state.assistant_text_by_turn) > 80:
            oldest = next(iter(state.assistant_text_by_turn))
            del state.assistant_text_by_turn[oldest]

    def _emit_once(self, dedup_key: str, event: Dict[str, Any]) -> None:
        if self.db.add_dedup(dedup_key):
            self.bus.publish(event)

    def _format_assistant_text_parts(
        self,
        text: str,
    ) -> Tuple[str, str]:
        cleaned = text.replace("<proposed_plan>", "").replace("</proposed_plan>", "")
        cleaned = cleaned.replace("\r\n", "\n").replace("\r", "\n")
        raw_lines = [line.rstrip() for line in cleaned.split("\n")]
        lines: List[str] = []
        prev_blank = False
        for line in raw_lines:
            stripped = line.strip()
            if not stripped:
                if not prev_blank and lines:
                    lines.append("")
                prev_blank = True
                continue
            lines.append(stripped)
            prev_blank = False
        normalized = "\n".join(lines).strip()
        if not normalized:
            return "", ""
        # Keep full assistant text; Telegram side handles chunk splitting.
        return normalized, ""

    def _handle_line(self, file_path: str, line: str, mode: str = "live") -> None:
        text = line.strip()
        if not text:
            return
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return

        session_key = self._extract_session_key(file_path)
        state = self._state_for(session_key)
        top_type = data.get("type")
        payload = data.get("payload", {}) if isinstance(data.get("payload"), dict) else {}

        if top_type == "turn_context":
            turn_id = payload.get("turn_id")
            if isinstance(turn_id, str) and turn_id:
                state.current_turn_id = turn_id
            return

        if top_type == "event_msg":
            ev_type = payload.get("type")
            if ev_type == "task_started":
                turn = payload.get("turn_id")
                if isinstance(turn, str) and turn:
                    state.current_turn_id = turn
                return

            if ev_type == "item_completed":
                turn = self._resolve_turn_id(payload, state.current_turn_id)
                item = payload.get("item", {}) if isinstance(payload.get("item"), dict) else {}
                item_type = self._clean_text(item.get("type")).lower()
                item_text = self._clean_text(item.get("text"))
                if item_type in ("plan", "message") and item_text:
                    self._remember_assistant_text(state, turn, item_text)
                if item_type == "plan":
                    state.proposed_plan_turns.add(turn)
                return

            if ev_type == "task_complete":
                turn = self._resolve_turn_id(payload, state.current_turn_id)

                if mode == "live" and "task_complete" in self.cfg.enabled_triggers:
                    dedup = f"{session_key}:{turn}:task_complete"
                    assistant_text = self._clean_text(state.assistant_text_by_turn.get(turn))
                    source = "state"
                    if not assistant_text:
                        assistant_text = self._clean_text(payload.get("last_agent_message"))
                        source = "last_agent_message" if assistant_text else "none"
                    logging.debug("task_complete notify text source session=%s turn=%s source=%s", session_key, turn, source)
                    event: Dict[str, Any] = {
                        "type": "task_complete",
                        "session_id": session_key,
                        "turn_id": turn,
                    }
                    if assistant_text:
                        primary, continued = self._format_assistant_text_parts(assistant_text)
                        if primary:
                            event["assistant_text_primary"] = primary
                            event["assistant_text"] = primary
                        if continued:
                            event["assistant_text_continued"] = continued
                    self._emit_once(
                        dedup,
                        event,
                    )

                if "proposed_plan_ready" in self.cfg.enabled_triggers and turn in state.proposed_plan_turns:
                    dedup = f"{session_key}:{turn}:proposed_plan_ready"
                    if self.db.add_dedup(dedup):
                        pending_id, payload_hash = self.db.create_pending_input(
                            session_id=session_key,
                            turn_id=turn,
                            kind="proposed_plan",
                            payload={"turn_id": turn},
                        )
                        event = {
                            "type": "proposed_plan_ready",
                            "session_id": session_key,
                            "turn_id": turn,
                            "pending_id": pending_id,
                            "payload_hash": payload_hash,
                        }
                        assistant_text = state.assistant_text_by_turn.get(turn)
                        if assistant_text:
                            primary, continued = self._format_assistant_text_parts(assistant_text)
                            if primary:
                                event["assistant_text_primary"] = primary
                                event["assistant_text"] = primary
                            if continued:
                                event["assistant_text_continued"] = continued
                        self.bus.publish(event)
                    state.proposed_plan_turns.discard(turn)
                return

            return

        if top_type == "response_item":
            ptype = payload.get("type")

            if ptype == "function_call" and payload.get("name") == "request_user_input":
                if "request_user_input" not in self.cfg.enabled_triggers:
                    return
                args_raw = payload.get("arguments", "{}")
                try:
                    args = json.loads(args_raw) if isinstance(args_raw, str) else {}
                except json.JSONDecodeError:
                    args = {}
                turn = state.current_turn_id or "unknown"
                call_id = str(payload.get("call_id", ""))
                dedup = f"{session_key}:{turn}:request_user_input:{call_id}"
                if self.db.add_dedup(dedup):
                    pending_id, payload_hash = self.db.create_pending_input(
                        session_id=session_key,
                        turn_id=state.current_turn_id,
                        kind="request_user_input",
                        payload={"arguments": args, "call_id": call_id},
                    )
                    self.bus.publish(
                        {
                            "type": "request_user_input",
                            "session_id": session_key,
                            "turn_id": state.current_turn_id,
                            "pending_id": pending_id,
                            "payload_hash": payload_hash,
                            "arguments": args,
                        }
                    )
                return

            if ptype == "message" and payload.get("role") == "assistant":
                content = payload.get("content")
                if not isinstance(content, list):
                    return
                found = False
                texts: List[str] = []
                for item in content:
                    if not isinstance(item, dict):
                        continue
                    itype = item.get("type")
                    if itype in ("output_text", "text"):
                        body = item.get("text", "")
                        if isinstance(body, str):
                            cleaned = body.strip()
                            if cleaned:
                                texts.append(cleaned)
                            if "<proposed_plan>" in body:
                                found = True
                if texts:
                    turn = self._resolve_turn_id(payload, state.current_turn_id)
                    self._remember_assistant_text(state, turn, "\n\n".join(texts))
                if found:
                    turn = self._resolve_turn_id(payload, state.current_turn_id)
                    state.proposed_plan_turns.add(turn)
                return
