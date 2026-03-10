import html
import hashlib
import json
import logging
import queue
import re
import socket
import sqlite3
import subprocess
import threading
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Optional, Tuple

from ..common import hash_binding_id, normalize_binding_id, is_valid_binding_id, utc_ts
from ..config import Config
from ..db import DB
from ..tmux_client import TmuxController

class TelegramAPI:
    def __init__(
        self,
        token: str,
        api_base: str,
        proxy_url: str,
        connect_timeout_sec: int,
        read_timeout_sec: int,
    ):
        normalized_api_base = self._normalize_api_base(api_base)
        self.api_base_root = normalized_api_base
        self.base = f"{normalized_api_base}/bot{token}"
        self.connect_timeout_sec = max(1, int(connect_timeout_sec))
        self.read_timeout_sec = max(1, int(read_timeout_sec))
        self.request_timeout_sec = self.connect_timeout_sec + self.read_timeout_sec

        self.proxy_for_log = "env/default"
        proxy = proxy_url.strip()
        if proxy:
            parsed_proxy = urllib.parse.urlsplit(proxy)
            if parsed_proxy.scheme and parsed_proxy.netloc:
                host = parsed_proxy.hostname or parsed_proxy.netloc.rsplit("@", 1)[-1]
                if parsed_proxy.port:
                    host = f"{host}:{parsed_proxy.port}"
                self.proxy_for_log = f"{parsed_proxy.scheme}://{host}"
                handler = urllib.request.ProxyHandler({"http": proxy, "https": proxy})
                self.opener = urllib.request.build_opener(handler)
            else:
                logging.warning("invalid telegram.proxy_url=%r; fallback to direct connection", proxy)
                self.proxy_for_log = "direct (invalid proxy_url)"
                self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        else:
            self.opener = urllib.request.build_opener()

    def _normalize_api_base(self, value: str) -> str:
        raw = value.strip()
        if not raw:
            return "https://api.telegram.org"
        parsed = urllib.parse.urlsplit(raw)
        if parsed.scheme in ("http", "https") and parsed.netloc:
            base = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            return base.rstrip("/")
        logging.warning("invalid telegram.api_base=%r; fallback to https://api.telegram.org", value)
        return "https://api.telegram.org"

    def _call(self, method: str, payload: Dict[str, Any], request_timeout_sec: Optional[int] = None) -> Dict[str, Any]:
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            f"{self.base}/{method}",
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        timeout_sec = self.request_timeout_sec if request_timeout_sec is None else max(1, int(request_timeout_sec))
        with self.opener.open(req, timeout=timeout_sec) as resp:
            body = resp.read()
        parsed = json.loads(body.decode("utf-8"))
        if not parsed.get("ok"):
            raise RuntimeError(f"telegram api error method={method}: {parsed}")
        return parsed

    def get_updates(self, offset: Optional[int], timeout: int) -> List[Dict[str, Any]]:
        payload: Dict[str, Any] = {"timeout": timeout}
        if offset is not None:
            payload["offset"] = offset
        timeout_budget = self.connect_timeout_sec + max(0, int(timeout)) + self.read_timeout_sec
        res = self._call("getUpdates", payload, request_timeout_sec=timeout_budget)
        return list(res.get("result", []))

    def send_message(
        self,
        chat_id: int,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
        parse_mode: Optional[str] = None,
        disable_web_page_preview: Optional[bool] = None,
    ) -> None:
        payload: Dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if disable_web_page_preview is not None:
            payload["disable_web_page_preview"] = disable_web_page_preview
        self._call("sendMessage", payload)

    def answer_callback_query(self, callback_query_id: str, text: str = "") -> None:
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        self._call("answerCallbackQuery", payload)

    def set_my_commands(self, commands: List[Dict[str, str]]) -> None:
        payload: Dict[str, Any] = {"commands": commands}
        self._call("setMyCommands", payload)

class TelegramService(threading.Thread):
    SESSIONS_PAGE_SIZE = 6
    TELEGRAM_TEXT_LIMIT = 4096
    TELEGRAM_CHUNK_SOFT_LIMIT = 3800
    SUPPORTED_HTML_TAGS = ("b", "i", "u", "code", "pre")

    def __init__(self, cfg: Config, db: DB, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.db = db
        self.stop_event = stop_event
        self.tmux = TmuxController()
        self.events_q: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.api = TelegramAPI(
            token=cfg.telegram_bot_token,
            api_base=cfg.telegram_api_base,
            proxy_url=cfg.telegram_proxy_url,
            connect_timeout_sec=cfg.telegram_connect_timeout_sec,
            read_timeout_sec=cfg.telegram_read_timeout_sec,
        )
        self.update_offset: Optional[int] = None

    def publish_event(self, event: Dict[str, Any]) -> None:
        self.events_q.put(event)

    def run(self) -> None:
        logging.info("telegram service started")
        logging.info(
            "telegram network api_base=%s proxy=%s connect_timeout=%ss read_timeout=%ss request_timeout=%ss",
            self.api.api_base_root,
            self.api.proxy_for_log,
            self.api.connect_timeout_sec,
            self.api.read_timeout_sec,
            self.api.request_timeout_sec,
        )
        self._register_bot_commands()
        try:
            self._initialize_update_offset()
        except Exception:
            logging.exception("failed to initialize telegram update offset; continuing without persisted offset")
        while not self.stop_event.is_set():
            try:
                self._flush_events(limit=50)
                updates = self.api.get_updates(self.update_offset, timeout=15)
                for upd in updates:
                    update_id = int(upd["update_id"])
                    next_offset = update_id + 1
                    # Advance offset first to avoid duplicate command execution after crashes.
                    self.update_offset = next_offset
                    self.db.set_kv("telegram.update_offset", str(next_offset))
                    try:
                        self._handle_update(upd)
                    except Exception:
                        logging.exception("failed to handle telegram update_id=%s after offset advance", update_id)
                self._flush_events(limit=50)
            except urllib.error.HTTPError as e:
                logging.warning(
                    "telegram loop http error status=%s reason=%s; retry in %ss",
                    e.code,
                    e.reason,
                    self.cfg.poll_interval_sec,
                )
                self.stop_event.wait(self.cfg.poll_interval_sec)
            except urllib.error.URLError as e:
                reason = str(e.reason)
                if "timed out" in reason.lower():
                    logging.warning("telegram loop network timeout: %s; retry in %ss", reason, self.cfg.poll_interval_sec)
                else:
                    logging.warning("telegram loop network error: %s; retry in %ss", reason, self.cfg.poll_interval_sec)
                self.stop_event.wait(self.cfg.poll_interval_sec)
            except socket.timeout as e:
                logging.warning("telegram loop timeout: %s; retry in %ss", e, self.cfg.poll_interval_sec)
                self.stop_event.wait(self.cfg.poll_interval_sec)
            except TimeoutError as e:
                logging.warning("telegram loop timeout: %s; retry in %ss", e, self.cfg.poll_interval_sec)
                self.stop_event.wait(self.cfg.poll_interval_sec)
            except Exception:
                logging.exception("telegram loop error")
                self.stop_event.wait(self.cfg.poll_interval_sec)
        logging.info("telegram service stopped")

    def _initialize_update_offset(self) -> None:
        stored = self.db.get_kv("telegram.update_offset")
        if stored:
            try:
                self.update_offset = int(stored)
                return
            except ValueError:
                pass

        # First-time bootstrapping: skip historical backlog to avoid replaying old commands.
        next_offset: Optional[int] = None
        while True:
            updates = self.api.get_updates(next_offset, timeout=0)
            if not updates:
                break
            next_offset = max(int(u["update_id"]) for u in updates) + 1
            if len(updates) < 100:
                break
        if next_offset is not None:
            self.update_offset = next_offset
            self.db.set_kv("telegram.update_offset", str(next_offset))

    def _register_bot_commands(self) -> None:
        commands = [
            {"command": "help", "description": "Show help"},
            {"command": "menu", "description": "Show quick action keyboard"},
            {"command": "sessions", "description": "List/select managed sessions"},
            {"command": "select", "description": "Select a managed session"},
            {"command": "status", "description": "Show selected session status"},
            {"command": "mode", "description": "Switch mode: /mode plan"},
            {"command": "approve", "description": "Approve pending action"},
            {"command": "reject", "description": "Reject pending action"},
            {"command": "send", "description": "Send text to selected session"},
            {"command": "bind", "description": "Bind chat: /bind <binding_id>"},
            {"command": "unbind", "description": "Unbind this chat"},
        ]
        try:
            self.api.set_my_commands(commands)
            logging.info("telegram commands registered: %s", len(commands))
        except Exception:
            logging.exception("failed to register telegram commands")

    def _main_menu_keyboard(self) -> Dict[str, Any]:
        return {
            "keyboard": [
                [{"text": "Sessions"}, {"text": "Select"}, {"text": "Status"}, {"text": "Help"}],
                [{"text": "Plan"}, {"text": "Approve"}, {"text": "Reject"}],
            ],
            "resize_keyboard": True,
            "is_persistent": True,
            "one_time_keyboard": False,
        }

    def _send_main_menu(self, chat_id: int, text: str, rich: bool = False) -> None:
        self._reply(chat_id, text, reply_markup=self._main_menu_keyboard(), rich=rich)

    def _normalize_message_text(self, text: str) -> str:
        mapping = {
            "Sessions": "/sessions",
            "Select": "/select",
            "Status": "/status",
            "Help": "/help",
            "Menu": "/menu",
            "Plan": "/mode plan",
            # Keep compatibility with stale keyboards that still show "Default".
            "Default": "/mode default",
            "Approve": "/approve",
            "Reject": "/reject",
        }
        normalized = text.strip()
        return mapping.get(normalized, normalized)

    def _managed_sessions_sorted(self) -> List[sqlite3.Row]:
        managed = self.db.list_managed_sessions()
        return sorted(managed, key=lambda row: str(row["alias"]).lower())

    def _selected_alias(self, chat_id: int) -> Optional[str]:
        ref = self.db.get_selected_session(chat_id)
        if not ref:
            return None
        if ref.startswith("alias:"):
            return ref.split(":", 1)[1]
        if ref.startswith("session:"):
            sid = ref.split(":", 1)[1]
            row = self.db.get_managed_by_session_id(sid)
            if row:
                return str(row["alias"])
        return None

    def _render_sessions_page(self, chat_id: int, page: int) -> Tuple[str, Optional[Dict[str, Any]], int]:
        managed = self._managed_sessions_sorted()
        lines = ["<b>[Codex] managed sessions</b>"]
        if not managed:
            lines.append("<i>- none</i>")
            return "\n".join(lines), None, 0

        total_pages = (len(managed) + self.SESSIONS_PAGE_SIZE - 1) // self.SESSIONS_PAGE_SIZE
        page = max(0, min(page, total_pages - 1))
        start = page * self.SESSIONS_PAGE_SIZE
        end = min(start + self.SESSIONS_PAGE_SIZE, len(managed))
        selected_alias = self._selected_alias(chat_id)

        buttons: List[List[Dict[str, str]]] = []
        for idx, row in enumerate(managed[start:end]):
            alias = str(row["alias"])
            status = str(row["status"])
            sid = str(row["codex_session_id"] or "-")
            active = alias == selected_alias
            marker = "*" if active else " "
            lines.append(
                f"<b>{start + idx + 1}.</b> {self._h(marker)} {self._h(alias)} | {self._h(status)} | sid=<code>{self._h(sid)}</code>"
            )
            button_text = f"{'[*] ' if active else ''}{alias} ({status})"
            callback = f"sl|s|{page}|{idx}"
            buttons.append([{"text": button_text[:64], "callback_data": callback[:64]}])

        lines.append(self._kv_html("page", f"{page + 1}/{total_pages}"))
        nav: List[Dict[str, str]] = []
        if page > 0:
            nav.append({"text": "Prev", "callback_data": f"sl|p|{page - 1}"[:64]})
        if page < total_pages - 1:
            nav.append({"text": "Next", "callback_data": f"sl|p|{page + 1}"[:64]})
        if nav:
            buttons.append(nav)

        return "\n".join(lines), {"inline_keyboard": buttons}, page

    def _handle_sessions_callback(self, cb_id: str, chat_id: int, parts: List[str]) -> None:
        if len(parts) < 3:
            self.api.answer_callback_query(cb_id, "Invalid callback data")
            return
        action = parts[1]
        if action == "p":
            if len(parts) != 3:
                self.api.answer_callback_query(cb_id, "Invalid callback data")
                return
            try:
                page = int(parts[2])
            except ValueError:
                self.api.answer_callback_query(cb_id, "Invalid callback data")
                return
            text, keyboard, _ = self._render_sessions_page(chat_id, page)
            self._reply(chat_id, text, reply_markup=keyboard, rich=True)
            self.api.answer_callback_query(cb_id, "Updated")
            return

        if action == "s":
            if len(parts) != 4:
                self.api.answer_callback_query(cb_id, "Invalid callback data")
                return
            try:
                page = int(parts[2])
                index_in_page = int(parts[3])
            except ValueError:
                self.api.answer_callback_query(cb_id, "Invalid callback data")
                return
            if page < 0 or index_in_page < 0:
                self.api.answer_callback_query(cb_id, "Invalid callback data")
                return
            managed = self._managed_sessions_sorted()
            target = (page * self.SESSIONS_PAGE_SIZE) + index_in_page
            if target < 0 or target >= len(managed):
                self.api.answer_callback_query(cb_id, "Stale list, send /sessions again")
                return
            alias = str(managed[target]["alias"])
            ref = f"alias:{alias}"
            self.db.set_selected_session(chat_id, ref)
            self.api.answer_callback_query(cb_id, f"Selected: {alias}")
            self._reply(
                chat_id,
                f"<b>Selected:</b> <code>{self._h(ref)}</code>\n{self._format_selected_status_html(chat_id)}",
                reply_markup=self._main_menu_keyboard(),
                rich=True,
            )
            return

        self.api.answer_callback_query(cb_id, "Unsupported callback")

    def _flush_events(self, limit: int) -> None:
        for _ in range(limit):
            try:
                msg = self.events_q.get_nowait()
            except queue.Empty:
                break
            self._broadcast_event(msg)

    def _broadcast_event(self, event: Dict[str, Any]) -> None:
        chats = self.db.list_bound_chats()
        if not chats:
            return

        session_id = str(event.get("session_id", ""))
        managed = self.db.get_managed_by_session_id(session_id)
        session_label = f"{managed['alias']} ({session_id})" if managed else session_id

        etype = event.get("type")
        text = ""
        continued_text = ""
        keyboard = None
        rich = False

        if etype == "task_complete":
            lines = [
                "<b>[Codex] task complete</b>",
                self._kv_html("session", session_label),
                self._kv_html("turn", event.get("turn_id", "unknown")),
            ]
            primary = str(event.get("assistant_text_primary") or event.get("assistant_text") or "").strip()
            continued_text = str(event.get("assistant_text_continued") or "").strip()
            if primary:
                lines.append("")
                lines.append("<b>assistant summary:</b>")
                lines.append(self._render_assistant_text_html(primary))
            text = "\n".join(lines)
            rich = True
        elif etype == "proposed_plan_ready":
            lines = [
                "<b>[Codex] plan ready for execution</b>",
                self._kv_html("session", session_label),
                self._kv_html("turn", event.get("turn_id", "unknown")),
                "Tap <b>Approve Plan</b> to auto-select this session and execute.",
            ]
            primary = str(event.get("assistant_text_primary") or event.get("assistant_text") or "").strip()
            continued_text = str(event.get("assistant_text_continued") or "").strip()
            if primary:
                lines.append("")
                lines.append("<b>assistant summary:</b>")
                lines.append(self._render_assistant_text_html(primary))
            text = "\n".join(lines)
            rich = True
            try:
                pending_id = int(event.get("pending_id"))
            except (TypeError, ValueError):
                pending_id = 0
            payload_hash = str(event.get("payload_hash") or "")
            callback_token = self._build_pending_callback_token(pending_id, payload_hash)
            if pending_id > 0 and callback_token:
                cb = f"appr|{pending_id}|{callback_token}"
                keyboard = {"inline_keyboard": [[{"text": "Approve Plan", "callback_data": cb[:64]}]]}
        elif etype == "request_user_input":
            pending: Optional[sqlite3.Row] = None
            try:
                pending_id = int(event.get("pending_id"))
                pending = self.db.get_pending_by_id(pending_id)
            except (TypeError, ValueError):
                pending = None
            if pending and str(pending["status"]) == "pending" and str(pending["kind"]) == "request_user_input":
                text, keyboard = self._render_pending_question_message(session_label, pending)
            else:
                text = (
                    "<b>[Codex] request_user_input</b>\n"
                    f"{self._kv_html('session', session_label)}\n"
                    "<i>(pending payload unavailable)</i>"
                )
            rich = True
        else:
            text = f"[Codex] event: {etype}\n{json.dumps(event, ensure_ascii=True)}"

        for chat_id in chats:
            try:
                self._reply(chat_id, text, reply_markup=keyboard, rich=rich)
                if continued_text:
                    self._reply(
                        chat_id,
                        f"<b>[Codex] assistant (continued)</b>\n\n{self._render_assistant_text_html(continued_text)}",
                        rich=True,
                    )
            except Exception:
                logging.exception("failed to send telegram event")

    def _render_pending_question_message(
        self,
        session_label: str,
        pending: sqlite3.Row,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        pending_id = int(pending["id"])
        payload_hash = str(pending["payload_hash"] or "")
        lines = ["<b>[Codex] request_user_input</b>", self._kv_html("session", session_label)]
        keyboard: Optional[Dict[str, Any]] = None
        try:
            payload = json.loads(str(pending["payload_json"]))
        except json.JSONDecodeError:
            lines.append("error: invalid pending payload")
            return "\n".join(lines), None
        args = payload.get("arguments", {}) if isinstance(payload, dict) else {}
        questions = args.get("questions", []) if isinstance(args, dict) else []
        if not isinstance(questions, list) or not questions:
            lines.append("error: no questions found")
            return "\n".join(lines), None

        question_index = int(pending["question_index"] if pending["question_index"] is not None else 0)
        if question_index < 0 or question_index >= len(questions):
            lines.append("error: question index out of range")
            return "\n".join(lines), None

        question = questions[question_index]
        if not isinstance(question, dict):
            lines.append("error: invalid question payload")
            return "\n".join(lines), None

        lines.append(self._kv_html("question", f"{question_index + 1}/{len(questions)}"))
        prompt = str(question.get("question", "")).strip()
        if prompt:
            lines.append(self._kv_html("prompt", prompt))

        options = question.get("options", [])
        if not isinstance(options, list) or not options:
            lines.append("error: no options found")
            return "\n".join(lines), None

        callback_token = self._build_pending_callback_token(pending_id, payload_hash)
        buttons: List[List[Dict[str, str]]] = []
        for idx, option in enumerate(options[:6]):
            if not isinstance(option, dict):
                continue
            label = str(option.get("label", f"option-{idx + 1}"))
            lines.append(f"<b>{idx + 1}.</b> {self._h(label)}")
            description = str(option.get("description", "")).strip()
            if description:
                description = re.sub(r"\s+", " ", description)
                lines.append(f"<i>{self._h(description)}</i>")
            if callback_token:
                cb = f"pick|{pending_id}|{question_index}|{idx}|{callback_token}"
                buttons.append([{"text": label[:64], "callback_data": cb[:64]}])
        if buttons:
            keyboard = {"inline_keyboard": buttons}
        return "\n".join(lines), keyboard

    def _handle_update(self, update: Dict[str, Any]) -> None:
        if "message" in update:
            self._handle_message(update["message"])
        elif "callback_query" in update:
            self._handle_callback(update["callback_query"])

    def _handle_callback(self, cb: Dict[str, Any]) -> None:
        cb_id = str(cb.get("id", ""))
        data = str(cb.get("data", ""))
        message = cb.get("message", {}) if isinstance(cb.get("message"), dict) else {}
        chat = message.get("chat", {}) if isinstance(message.get("chat"), dict) else {}
        chat_id = int(chat.get("id", 0))

        if not self.db.is_chat_bound(chat_id):
            self.api.answer_callback_query(cb_id, "Bind first with /bind <binding_id>")
            return

        parts = data.split("|")
        if parts and parts[0] == "appr":
            if len(parts) != 3:
                self.api.answer_callback_query(cb_id, "Invalid callback data")
                return
            try:
                pending_id = int(parts[1])
            except ValueError:
                self.api.answer_callback_query(cb_id, "Invalid callback data")
                return
            callback_token = str(parts[2]).strip().lower()
            self._handle_approve_plan_callback(cb_id, chat_id, pending_id, callback_token)
            return

        if parts and parts[0] == "sl":
            self._handle_sessions_callback(cb_id, chat_id, parts)
            return

        if len(parts) != 5 or parts[0] != "pick":
            self.api.answer_callback_query(cb_id, "Unsupported callback")
            return

        try:
            pending_id = int(parts[1])
            question_idx = int(parts[2])
            idx = int(parts[3])
            callback_token = str(parts[4]).strip().lower()
        except ValueError:
            self.api.answer_callback_query(cb_id, "Invalid callback data")
            return

        pending = self.db.get_pending_by_id(pending_id)
        if not pending or pending["status"] != "pending":
            self.api.answer_callback_query(cb_id, "Already handled")
            return
        payload_hash = str(pending["payload_hash"] or "")
        if not payload_hash:
            self.api.answer_callback_query(cb_id, "Stale option, use /approve or /reject")
            return
        expected_token = self._build_pending_callback_token(pending_id, payload_hash)
        if callback_token != expected_token:
            self.api.answer_callback_query(cb_id, "Stale options, refresh from latest prompt")
            return

        session_id = str(pending["session_id"])
        try:
            payload = json.loads(str(pending["payload_json"]))
        except json.JSONDecodeError:
            self.api.answer_callback_query(cb_id, "Invalid pending payload")
            return
        args = payload.get("arguments", {})
        questions = args.get("questions", []) if isinstance(args, dict) else []
        if not questions or not isinstance(questions, list):
            self.api.answer_callback_query(cb_id, "No options found")
            return
        current_qidx = int(pending["question_index"] if pending["question_index"] is not None else 0)
        if question_idx != current_qidx:
            self.api.answer_callback_query(cb_id, "Stale options, refresh from latest prompt")
            return
        if question_idx < 0 or question_idx >= len(questions):
            self.api.answer_callback_query(cb_id, "Question out of range")
            return
        question = questions[question_idx]
        if not isinstance(question, dict):
            self.api.answer_callback_query(cb_id, "Invalid question")
            return

        options = question.get("options", [])
        if not isinstance(options, list) or idx < 0 or idx >= len(options):
            self.api.answer_callback_query(cb_id, "Option out of range")
            return

        option = options[idx]
        if not isinstance(option, dict):
            self.api.answer_callback_query(cb_id, "Invalid option")
            return

        label = str(option.get("label", ""))
        label_send = self._clean_option_label(label)
        ok, msg = self._send_to_session(session_id, label_send)
        if not ok:
            self.api.answer_callback_query(cb_id, f"Failed: {msg[:80]}")
            return

        answer_entry = {
            "question_index": question_idx,
            "option_index": idx,
            "label": label,
            "sent_text": label_send,
            "answered_at": utc_ts(),
        }
        progressed, done, next_idx, progress_msg = self.db.advance_pending_request_user_input(
            pending_id,
            question_idx,
            answer_entry,
            len(questions),
        )
        if not progressed:
            if progress_msg == "stale question":
                self.api.answer_callback_query(cb_id, "Stale options, refresh from latest prompt")
            elif progress_msg == "already handled":
                self.api.answer_callback_query(cb_id, "Already handled")
            else:
                self.api.answer_callback_query(cb_id, "State update failed")
            return

        self.api.answer_callback_query(cb_id, "Sent")
        if done:
            return

        pending_next = self.db.get_pending_by_id(pending_id)
        if not pending_next or str(pending_next["status"]) != "pending":
            return
        managed = self.db.get_managed_by_session_id(session_id)
        session_label = f"{managed['alias']} ({session_id})" if managed else session_id
        text_next, keyboard_next = self._render_pending_question_message(session_label, pending_next)
        try:
            self._reply(chat_id, text_next, reply_markup=keyboard_next, rich=True)
        except Exception:
            logging.exception("failed to send next request_user_input question pending_id=%s next_idx=%s", pending_id, next_idx)

    def _handle_approve_plan_callback(self, cb_id: str, chat_id: int, pending_id: int, callback_token: str) -> None:
        pending = self.db.get_pending_by_id(pending_id)
        if not pending or str(pending["status"]) != "pending":
            self.api.answer_callback_query(cb_id, "Already handled")
            return
        if str(pending["kind"]) != "proposed_plan":
            self.api.answer_callback_query(cb_id, "Not a plan action")
            return

        payload_hash = str(pending["payload_hash"] or "")
        if not payload_hash:
            self.api.answer_callback_query(cb_id, "Stale action")
            return
        expected_token = self._build_pending_callback_token(pending_id, payload_hash)
        if callback_token != expected_token:
            self.api.answer_callback_query(cb_id, "Stale action")
            return

        session_id = str(pending["session_id"] or "")
        if not session_id:
            self.api.answer_callback_query(cb_id, "Missing session id")
            return

        managed = self.db.get_managed_by_session_id(session_id)
        if managed:
            self.db.set_selected_session(chat_id, f"alias:{managed['alias']}")
        else:
            self.db.set_selected_session(chat_id, f"session:{session_id}")

        ok, msg = self._send_to_session(session_id, self.cfg.approve_plan_template)
        if ok:
            self.db.mark_pending_responded(pending_id)
            self.api.answer_callback_query(cb_id, "Approved")
            return
        self.api.answer_callback_query(cb_id, f"Failed: {msg[:80]}")

    def _build_pending_callback_token(self, pending_id: int, payload_hash: str) -> str:
        if pending_id <= 0 or not payload_hash:
            return ""
        raw = f"{pending_id}:{payload_hash}".encode("utf-8")
        return hashlib.sha256(raw).hexdigest()[:12]

    def _handle_message(self, message: Dict[str, Any]) -> None:
        chat = message.get("chat", {}) if isinstance(message.get("chat"), dict) else {}
        from_user = message.get("from", {}) if isinstance(message.get("from"), dict) else {}
        chat_id = int(chat.get("id", 0))
        raw_text = str(message.get("text", "")).strip()
        username = str(from_user.get("username") or from_user.get("first_name") or "unknown")
        has_non_text_payload = any(
            key in message
            for key in ("photo", "sticker", "document", "video", "voice", "audio", "animation", "video_note")
        )

        if not raw_text:
            if not self.db.is_chat_bound(chat_id):
                self._reply(chat_id, "Not bound. Use <code>/bind &lt;binding_id&gt;</code>.", rich=True)
                return
            if has_non_text_payload:
                self._reply(
                    chat_id,
                    "Image/file messages are not supported yet. Please send text, or send an image link with context.",
                )
            return

        text = self._normalize_message_text(raw_text)
        cmd, arg = self._split_command(text)

        if cmd == "/start":
            self._send_main_menu(chat_id, "Use <code>/bind &lt;binding_id&gt;</code> to bind this chat.", rich=True)
            return

        if cmd == "/help":
            self._reply(
                chat_id,
                "<b>Commands</b>\n"
                "<code>/bind &lt;binding_id&gt;</code>\n"
                "<code>/unbind</code>\n"
                "<code>/menu</code>\n"
                "<code>/sessions</code>\n"
                "<code>/select &lt;alias|session_id&gt;</code>\n"
                "<code>/status</code>\n"
                "<code>/send &lt;text&gt;</code>\n"
                "<code>/mode &lt;plan&gt;</code>\n"
                "<code>/approve</code>\n"
                "<code>/reject</code>\n\n"
                "<i>Tip: plain text (without leading /) is sent to the selected managed session.</i>",
                reply_markup=self._main_menu_keyboard(),
                rich=True,
            )
            return

        if cmd == "/bind":
            if self.db.is_chat_bound(chat_id):
                self._reply(chat_id, "<b>Already bound.</b>", rich=True)
                return
            token = normalize_binding_id(arg)
            if not token or not is_valid_binding_id(token):
                self._reply(chat_id, "Usage: <code>/bind &lt;binding_id&gt;</code>", rich=True)
                return
            ok, msg = self.db.consume_bind_token(hash_binding_id(token), chat_id)
            if not ok:
                self._reply(chat_id, f"<b>Bind failed:</b> {self._h(msg)}", rich=True)
                return
            self.db.bind_chat(chat_id, username)
            self._send_main_menu(chat_id, "Bind success. Use buttons or commands to control sessions.")
            return

        if cmd == "/unbind":
            self.db.unbind_chat(chat_id)
            self._reply(chat_id, "<b>Unbound.</b>", rich=True)
            return

        if cmd == "/menu":
            if not self.db.is_chat_bound(chat_id):
                self._reply(chat_id, "Not bound. Use <code>/bind &lt;binding_id&gt;</code>.", rich=True)
                return
            self._send_main_menu(chat_id, "Main menu ready.")
            return

        if not self.db.is_chat_bound(chat_id):
            self._reply(chat_id, "Not bound. Use <code>/bind &lt;binding_id&gt;</code>.", rich=True)
            return

        if cmd == "/sessions":
            text_page, keyboard, _ = self._render_sessions_page(chat_id, page=0)
            self._reply(chat_id, text_page, reply_markup=keyboard, rich=True)
            return

        if cmd == "/select":
            if not arg:
                text_page, keyboard, _ = self._render_sessions_page(chat_id, page=0)
                self._reply(chat_id, text_page, reply_markup=keyboard, rich=True)
                return
            ref = self._resolve_session_ref(arg)
            if not ref:
                self._reply(chat_id, "<b>Session not found.</b>", rich=True)
                return
            self.db.set_selected_session(chat_id, ref)
            self._reply(
                chat_id,
                f"<b>Selected:</b> <code>{self._h(ref)}</code>\n{self._format_selected_status_html(chat_id)}",
                reply_markup=self._main_menu_keyboard(),
                rich=True,
            )
            return

        if cmd == "/status":
            self._reply(chat_id, self._format_selected_status_html(chat_id), rich=True)
            return

        if cmd == "/send":
            if not arg:
                self._reply(chat_id, "Usage: <code>/send &lt;text&gt;</code>", rich=True)
                return
            session_id = self._selected_session_id(chat_id)
            if not session_id:
                self._reply(chat_id, "No selected managed session. Use /sessions.")
                return
            ok, msg = self._send_to_session(session_id, arg)
            self._reply(chat_id, "Sent." if ok else f"Send failed: {msg}")
            return

        if cmd == "/mode":
            if arg == "plan":
                text_to_send = self.cfg.mode_plan_template
            elif arg == "default":
                self._reply(
                    chat_id,
                    "Default mode cannot be switched via chat in this Codex environment.",
                )
                return
            else:
                self._reply(chat_id, "Usage: <code>/mode &lt;plan&gt;</code>", rich=True)
                return
            session_id = self._selected_session_id(chat_id)
            if not session_id:
                self._reply(chat_id, "No selected managed session. Use /sessions.")
                return
            ok, msg = self._send_to_session(session_id, text_to_send)
            self._reply(chat_id, "Mode command sent." if ok else f"Failed: {msg}")
            return

        if cmd in ("/approve", "/reject"):
            session_id = self._selected_session_id(chat_id)
            if not session_id:
                self._reply(chat_id, "No selected managed session. Use /sessions.")
                return
            ok, msg = self._handle_approval_action(session_id, approve=(cmd == "/approve"))
            self._reply(chat_id, "Done." if ok else f"Failed: {msg}")
            return

        if text.startswith("/"):
            self._reply(chat_id, "Unknown command. Use /help.")
            return

        session_id = self._selected_session_id(chat_id)
        if not session_id:
            self._reply(chat_id, "No selected managed session. Use /sessions.")
            return
        ok, msg = self._send_to_session(session_id, text)
        self._reply(chat_id, "Sent." if ok else f"Send failed: {msg}")

    def _handle_approval_action(self, session_id: str, approve: bool) -> Tuple[bool, str]:
        pendings = self.db.get_pending_for_session(session_id)
        if pendings:
            first = pendings[0]
            kind = str(first["kind"])
            try:
                payload = json.loads(str(first["payload_json"]))
            except json.JSONDecodeError:
                return False, "invalid pending payload"

            if kind == "request_user_input":
                args = payload.get("arguments", {})
                questions = args.get("questions", []) if isinstance(args, dict) else []
                if not isinstance(questions, list) or not questions:
                    return False, "invalid pending question"
                start_idx = int(first["question_index"] if first["question_index"] is not None else 0)
                if start_idx < 0:
                    start_idx = 0
                if start_idx >= len(questions):
                    self.db.mark_pending_responded(int(first["id"]))
                    return True, "already completed"

                answers_raw = str(first["answers_json"] or "[]")
                try:
                    answers = json.loads(answers_raw)
                    if not isinstance(answers, list):
                        answers = []
                except json.JSONDecodeError:
                    answers = []

                for qidx in range(start_idx, len(questions)):
                    question = questions[qidx]
                    if not isinstance(question, dict):
                        return False, "invalid question payload"
                    options = question.get("options", [])
                    if not isinstance(options, list) or not options:
                        return False, "no options"

                    pick_idx = self._pick_option_index(options, approve=approve)
                    option = options[pick_idx]
                    if not isinstance(option, dict):
                        return False, "invalid option"
                    label = str(option.get("label", ""))
                    send_text = self._clean_option_label(label)
                    ok, msg = self._send_to_session(session_id, send_text)
                    if not ok:
                        return False, msg
                    answers.append(
                        {
                            "question_index": qidx,
                            "option_index": pick_idx,
                            "label": label,
                            "sent_text": send_text,
                            "answered_at": utc_ts(),
                            "source": "approve" if approve else "reject",
                        }
                    )

                self.db.mark_pending_responded_with_answers(int(first["id"]), answers, len(questions))
                return True, "ok"

            if kind == "proposed_plan":
                send_text = self.cfg.approve_plan_template if approve else self.cfg.reject_plan_template
                ok, msg = self._send_to_session(session_id, send_text)
                if ok:
                    self.db.mark_pending_responded(int(first["id"]))
                return ok, msg

        fallback = self.cfg.approve_plan_template if approve else self.cfg.reject_plan_template
        return self._send_to_session(session_id, fallback)

    def _split_command(self, text: str) -> Tuple[str, str]:
        parts = text.split(maxsplit=1)
        cmd = parts[0]
        cmd = cmd.split("@", maxsplit=1)[0]
        arg = parts[1].strip() if len(parts) > 1 else ""
        return cmd, arg

    def _clean_option_label(self, label: str) -> str:
        cleaned = re.sub(r"\(\s*recommended\s*\)", "", label, flags=re.IGNORECASE)
        return cleaned.strip()

    def _is_recommended_label(self, label: str) -> bool:
        return "(recommended)" in label.lower()

    def _pick_option_index(self, options: List[Any], approve: bool) -> int:
        normalized: List[Tuple[int, str]] = []
        for i, opt in enumerate(options):
            if isinstance(opt, dict):
                normalized.append((i, str(opt.get("label", ""))))
            else:
                normalized.append((i, ""))
        if not normalized:
            return 0

        recommended = [i for i, label in normalized if self._is_recommended_label(label)]
        if approve:
            if recommended:
                return recommended[0]
            return normalized[0][0]

        for i, label in normalized:
            if not self._is_recommended_label(label):
                return i
        if len(normalized) > 1:
            return normalized[1][0]
        return normalized[0][0]

    def _render_assistant_text_html(self, text: str) -> str:
        if not text:
            return ""
        if not self.cfg.telegram_markdown_render_enabled:
            return self._h(text)
        return self._markdown_to_html(text)

    def _markdown_to_html(self, text: str) -> str:
        normalized = text.replace("\r\n", "\n")
        lines = normalized.split("\n")
        out: List[str] = []
        in_code_block = False
        code_lines: List[str] = []

        for line in lines:
            if line.strip().startswith("```"):
                if in_code_block:
                    code_block_text = "\n".join(code_lines)
                    out.append(f"<pre><code>{self._h(code_block_text)}</code></pre>")
                    code_lines = []
                    in_code_block = False
                else:
                    if code_lines:
                        out.append(self._h("\n".join(code_lines)))
                        code_lines = []
                    in_code_block = True
                continue

            if in_code_block:
                code_lines.append(line)
                continue

            out.append(self._markdown_line_to_html(line))

        if in_code_block:
            code_block_text = "\n".join(code_lines)
            out.append(f"<pre><code>{self._h(code_block_text)}</code></pre>")

        return "\n".join(out)

    def _markdown_line_to_html(self, line: str) -> str:
        if not line:
            return ""

        stripped = line.lstrip()
        if not stripped:
            return ""

        heading = re.match(r"^(#{1,6})\s+(.*)$", stripped)
        if heading:
            return f"<b>{self._markdown_inline_to_html(heading.group(2).strip())}</b>"

        if stripped.startswith(">"):
            content = stripped[1:].lstrip()
            return f"<i>{self._markdown_inline_to_html(content)}</i>"

        ordered = re.match(r"^(\d+)\.\s+(.*)$", stripped)
        if ordered:
            return f"{ordered.group(1)}. {self._markdown_inline_to_html(ordered.group(2))}"

        unordered = re.match(r"^[-*]\s+(.*)$", stripped)
        if unordered:
            return f"• {self._markdown_inline_to_html(unordered.group(1))}"

        return self._markdown_inline_to_html(line)

    def _markdown_inline_to_html(self, text: str) -> str:
        if not text:
            return ""

        placeholders: List[str] = []

        def _save(fragment: str) -> str:
            placeholders.append(fragment)
            return f"@@CWMD{len(placeholders) - 1}@@"

        def _replace_code(match: re.Match[str]) -> str:
            return _save(f"<code>{self._h(match.group(1))}</code>")

        def _replace_link(match: re.Match[str]) -> str:
            label = self._h(match.group(1).strip())
            url = match.group(2).strip()
            if re.fullmatch(r"https?://\\S+", url):
                return _save(f"{label} ({self._h(url)})")
            return _save(f"{label} ({self._h(url)})")

        working = re.sub(r"`([^`\n]+)`", _replace_code, text)
        working = re.sub(r"\[([^\]\n]+)\]\(([^)\n]+)\)", _replace_link, working)
        working = self._h(working)

        # Bold first, then italic.
        working = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", working)
        working = re.sub(r"(?<!\*)\*(?!\s)(.+?)(?<!\s)\*(?!\*)", r"<i>\1</i>", working)
        working = re.sub(r"(?<!\w)_(?!\s)(.+?)(?<!\s)_(?!\w)", r"<i>\1</i>", working)

        for idx, fragment in enumerate(placeholders):
            working = working.replace(f"@@CWMD{idx}@@", fragment)
        return working

    @staticmethod
    def _h(value: Any) -> str:
        return html.escape(str(value), quote=False)

    @staticmethod
    def _html_to_plain(text: str) -> str:
        no_tags = re.sub(r"</?(?:b|i|u|code|pre)>", "", text)
        no_tags = re.sub(r"<br\s*/?>", "\n", no_tags)
        return html.unescape(no_tags)

    def _kv_html(self, label: str, value: Any, code: bool = False) -> str:
        escaped = self._h(value)
        if code:
            escaped = f"<code>{escaped}</code>"
        return f"<b>{self._h(label)}:</b> {escaped}"

    def _sanitize_html(self, text: str) -> str:
        def _replace_tag(match: re.Match[str]) -> str:
            token = match.group(0)
            kind, _ = self._parse_supported_tag(token)
            return token if kind in ("open", "close") else self._h(token)

        return re.sub(r"<[^>]+>", _replace_tag, text)

    @classmethod
    def _parse_supported_tag(cls, token: str) -> Tuple[Optional[str], Optional[str]]:
        m = re.fullmatch(r"<(/?)([a-zA-Z0-9]+)>", token.strip())
        if not m:
            return None, None
        name = m.group(2).lower()
        if name not in cls.SUPPORTED_HTML_TAGS:
            return None, None
        return ("close" if m.group(1) else "open"), name

    @staticmethod
    def _pick_plain_cut(text: str, max_len: int) -> int:
        if len(text) <= max_len:
            return len(text)
        cut = max_len
        newline_pos = text.rfind("\n", 0, cut + 1)
        if newline_pos >= max_len // 2:
            return newline_pos + 1
        space_pos = text.rfind(" ", 0, cut + 1)
        if space_pos >= max_len // 2:
            return space_pos + 1
        return cut

    @staticmethod
    def _pick_html_text_cut(text: str, max_len: int) -> int:
        if len(text) <= max_len:
            return len(text)
        cut = TelegramService._pick_plain_cut(text, max_len)
        amp = text.rfind("&", 0, cut)
        semicolon = text.rfind(";", 0, cut)
        if amp != -1 and semicolon < amp:
            candidate = text[amp:cut]
            if re.fullmatch(r"&(?:#\d{1,8}|#x[0-9A-Fa-f]{1,8}|[A-Za-z][A-Za-z0-9]{1,31})?", candidate):
                if amp > 0:
                    cut = amp
        return max(1, cut)

    def _split_plain_chunks(self, text: str, max_len: Optional[int] = None) -> List[str]:
        normalized = text.replace("\r\n", "\n")
        limit = max(32, min(max_len or self.TELEGRAM_CHUNK_SOFT_LIMIT, self.TELEGRAM_TEXT_LIMIT))
        if len(normalized) <= limit:
            return [normalized]

        chunks: List[str] = []
        remaining = normalized
        while remaining:
            if len(remaining) <= limit:
                chunks.append(remaining)
                break
            cut = self._pick_plain_cut(remaining, limit)
            if cut <= 0:
                cut = limit
            chunk = remaining[:cut]
            if not chunk:
                chunk = remaining[:limit]
                cut = len(chunk)
            chunks.append(chunk)
            remaining = remaining[cut:]
        return [c for c in chunks if c] or [normalized]

    def _split_html_chunks(self, text: str, max_len: Optional[int] = None) -> List[str]:
        normalized = text.replace("\r\n", "\n")
        limit = max(32, min(max_len or self.TELEGRAM_CHUNK_SOFT_LIMIT, self.TELEGRAM_TEXT_LIMIT))
        if len(normalized) <= limit:
            return [normalized]

        tokens = [tok for tok in re.split(r"(<[^>]+>)", normalized) if tok]
        chunks: List[str] = []
        open_tags: List[str] = []
        chunk_prefix_tags: List[str] = []
        chunk_parts: List[str] = []
        chunk_body_len = 0

        def _open_prefix(tags: List[str]) -> str:
            return "".join(f"<{tag}>" for tag in tags)

        def _close_suffix(tags: List[str]) -> str:
            return "".join(f"</{tag}>" for tag in reversed(tags))

        def _render_chunk() -> str:
            return f"{_open_prefix(chunk_prefix_tags)}{''.join(chunk_parts)}{_close_suffix(open_tags)}"

        def _start_new_chunk() -> None:
            nonlocal chunk_prefix_tags, chunk_parts, chunk_body_len
            chunk_prefix_tags = list(open_tags)
            chunk_parts = []
            chunk_body_len = 0

        def _append_part(part: str) -> None:
            nonlocal chunk_body_len
            if not part:
                return
            chunk_parts.append(part)
            chunk_body_len += len(part)

        _start_new_chunk()

        for token in tokens:
            token_kind, token_name = self._parse_supported_tag(token)
            if token.startswith("<") and token.endswith(">") and token_kind is None:
                # Escape unknown tags to keep parse_mode=HTML stable.
                token = self._h(token)
                token_kind, token_name = None, None

            if token_kind in ("open", "close"):
                while True:
                    prefix_len = len(_open_prefix(chunk_prefix_tags))
                    suffix_len = len(_close_suffix(open_tags))
                    available = limit - (prefix_len + chunk_body_len + suffix_len)
                    if available < len(token) and chunk_parts:
                        rendered = _render_chunk()
                        if rendered:
                            chunks.append(rendered)
                        _start_new_chunk()
                        continue
                    break

                _append_part(token)
                if token_kind == "open" and token_name is not None:
                    open_tags.append(token_name)
                elif token_kind == "close" and token_name is not None:
                    if open_tags and open_tags[-1] == token_name:
                        open_tags.pop()
                    elif token_name in open_tags:
                        idx = len(open_tags) - 1 - open_tags[::-1].index(token_name)
                        del open_tags[idx]
                continue

            remaining_text = token
            while remaining_text:
                prefix_len = len(_open_prefix(chunk_prefix_tags))
                suffix_len = len(_close_suffix(open_tags))
                available = limit - (prefix_len + chunk_body_len + suffix_len)
                if available <= 0:
                    rendered = _render_chunk()
                    if rendered:
                        chunks.append(rendered)
                    _start_new_chunk()
                    continue
                if len(remaining_text) <= available:
                    _append_part(remaining_text)
                    remaining_text = ""
                    continue
                cut = self._pick_html_text_cut(remaining_text, available)
                if cut <= 0:
                    cut = min(len(remaining_text), max(1, available))
                _append_part(remaining_text[:cut])
                remaining_text = remaining_text[cut:]
                rendered = _render_chunk()
                if rendered:
                    chunks.append(rendered)
                _start_new_chunk()

        final_chunk = _render_chunk()
        if final_chunk:
            chunks.append(final_chunk)
        return [c for c in chunks if c] or [normalized]

    def _reply(
        self,
        chat_id: int,
        text: str,
        reply_markup: Optional[Dict[str, Any]] = None,
        rich: bool = False,
        disable_preview: Optional[bool] = None,
    ) -> None:
        html_text = self._sanitize_html(text) if rich else self._h(text)
        chunks = self._split_html_chunks(html_text)
        total_chunks = len(chunks)

        for idx, chunk in enumerate(chunks, start=1):
            chunk_markup = reply_markup if idx == 1 else None
            payload_text = chunk if chunk else " "
            try:
                self.api.send_message(
                    chat_id,
                    payload_text,
                    reply_markup=chunk_markup,
                    parse_mode="HTML",
                    disable_web_page_preview=disable_preview,
                )
                continue
            except Exception:
                logging.exception(
                    "telegram html send failed chat_id=%s rich=%s len=%s chunk=%s/%s; fallback to plain",
                    chat_id,
                    rich,
                    len(html_text),
                    idx,
                    total_chunks,
                )
            plain_chunk = self._html_to_plain(chunk)
            plain_chunks = self._split_plain_chunks(plain_chunk)
            for pidx, plain_text in enumerate(plain_chunks, start=1):
                plain_markup = chunk_markup if pidx == 1 else None
                self.api.send_message(
                    chat_id,
                    plain_text if plain_text else " ",
                    reply_markup=plain_markup,
                    disable_web_page_preview=disable_preview,
                )

    def _resolve_session_ref(self, value: str) -> Optional[str]:
        m = self.db.get_managed_by_alias(value)
        if m:
            return f"alias:{value}"
        m2 = self.db.get_managed_by_session_id(value)
        if m2:
            return f"session:{value}"
        discovered = self.db.list_discovered_sessions()
        for row in discovered:
            sid = row["session_id"]
            if sid and str(sid) == value:
                return f"session:{value}"
        return None

    def _selected_session_id(self, chat_id: int) -> Optional[str]:
        ref = self.db.get_selected_session(chat_id)
        if not ref:
            return None
        if ref.startswith("alias:"):
            alias = ref.split(":", 1)[1]
            row = self.db.get_managed_by_alias(alias)
            if row and row["codex_session_id"]:
                return str(row["codex_session_id"])
            if row:
                return f"alias::{alias}"
            return None
        if ref.startswith("session:"):
            return ref.split(":", 1)[1]
        return None

    def _send_to_session(self, session_id: str, text: str) -> Tuple[bool, str]:
        alias_hint = None
        if session_id.startswith("alias::"):
            alias_hint = session_id.split("::", 1)[1]

        row = self.db.get_managed_by_session_id(session_id) if alias_hint is None else self.db.get_managed_by_alias(alias_hint)
        if not row:
            return False, "session is not managed (legacy session is notify-only)"

        pane = str(row["tmux_pane"] or "")
        if not pane:
            return False, "missing tmux pane"

        if not self.tmux.pane_exists(pane):
            self.db.update_managed_status_by_alias(str(row["alias"]), "stopped")
            return False, "tmux pane not running"

        try:
            self.tmux.send_text(pane, text, enter=True)
        except subprocess.CalledProcessError as e:
            return False, e.stderr.strip() or "tmux send failed"

        return True, "ok"

    def _format_sessions(self) -> str:
        lines = ["Managed sessions:"]
        managed = self.db.list_managed_sessions()
        if managed:
            for row in managed:
                lines.append(
                    f"- {row['alias']} | status={row['status']} | session_id={row['codex_session_id'] or '-'} | cwd={row['cwd']} | nonce={row['launch_nonce'] or '-'}"
                )
        else:
            lines.append("- none")
        return "\n".join(lines)

    def _format_selected_status(self, chat_id: int) -> str:
        ref = self.db.get_selected_session(chat_id)
        if not ref:
            return "No selected session. Use /sessions."

        session_id = self._selected_session_id(chat_id)
        lines = [f"selected: {ref}"]

        if not session_id:
            lines.append("resolved session: none")
            return "\n".join(lines)

        if session_id.startswith("alias::"):
            alias = session_id.split("::", 1)[1]
            row = self.db.get_managed_by_alias(alias)
            if row:
                lines.append(f"managed alias: {alias}")
                lines.append(f"codex_session_id: {row['codex_session_id'] or '(waiting for session id)'}")
                lines.append(f"status: {row['status']}")
                pendings = self.db.get_pending_for_session(str(row["codex_session_id"])) if row["codex_session_id"] else []
                lines.append(f"pending items: {len(pendings)}")
                return "\n".join(lines)
            lines.append("managed alias not found")
            return "\n".join(lines)

        row = self.db.get_managed_by_session_id(session_id)
        if row:
            lines.append(f"managed alias: {row['alias']}")
            lines.append(f"status: {row['status']}")
        else:
            lines.append("session mode: legacy (notify-only)")

        pendings = self.db.get_pending_for_session(session_id)
        lines.append(f"pending items: {len(pendings)}")
        return "\n".join(lines)

    def _format_selected_status_html(self, chat_id: int) -> str:
        ref = self.db.get_selected_session(chat_id)
        if not ref:
            return "No selected session. Use <code>/sessions</code>."

        session_id = self._selected_session_id(chat_id)
        lines = [self._kv_html("selected", ref, code=True)]

        if not session_id:
            lines.append(self._kv_html("resolved session", "none"))
            return "\n".join(lines)

        if session_id.startswith("alias::"):
            alias = session_id.split("::", 1)[1]
            row = self.db.get_managed_by_alias(alias)
            if row:
                lines.append(self._kv_html("managed alias", alias))
                lines.append(self._kv_html("codex_session_id", row["codex_session_id"] or "(waiting for session id)", code=True))
                lines.append(self._kv_html("status", row["status"]))
                pendings = self.db.get_pending_for_session(str(row["codex_session_id"])) if row["codex_session_id"] else []
                lines.append(self._kv_html("pending items", len(pendings)))
                return "\n".join(lines)
            lines.append(self._kv_html("managed alias", "not found"))
            return "\n".join(lines)

        row = self.db.get_managed_by_session_id(session_id)
        if row:
            lines.append(self._kv_html("managed alias", row["alias"]))
            lines.append(self._kv_html("status", row["status"]))
        else:
            lines.append(self._kv_html("session mode", "legacy (notify-only)"))

        pendings = self.db.get_pending_for_session(session_id)
        lines.append(self._kv_html("pending items", len(pendings)))
        return "\n".join(lines)
