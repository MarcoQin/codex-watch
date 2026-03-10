#!/usr/bin/env python3
import argparse
import collections
import dataclasses
import datetime as dt
import hashlib
import json
import logging
import os
import queue
import random
import shlex
import signal
import sqlite3
import string
import subprocess
import sys
import threading
import time
import re
import socket
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

APP_NAME = "codex-watch"
DEFAULT_CONFIG_PATH = os.path.expanduser("~/.config/codex-watch/config.toml")
DEFAULT_RUNTIME_DIR = os.path.expanduser("~/.local/state/codex-watch")
DEFAULT_SQLITE_PATH = os.path.join(DEFAULT_RUNTIME_DIR, "state.sqlite3")
DEFAULT_LOG_PATH = os.path.join(DEFAULT_RUNTIME_DIR, "codex-watch.log")
DEFAULT_PID_PATH = os.path.join(DEFAULT_RUNTIME_DIR, "codex-watch.pid")
DEFAULT_SESSIONS_ROOT = os.path.expanduser("~/.codex/sessions")


def utc_ts() -> int:
    return int(time.time())


def utc_iso(ts: Optional[int] = None) -> str:
    value = ts if ts is not None else utc_ts()
    return dt.datetime.utcfromtimestamp(value).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_binding_id(raw: str) -> str:
    compact = re.sub(r"[\s-]+", "", raw.strip().upper())
    return compact


def is_valid_binding_id(token: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9]{8,64}", token))


def hash_binding_id(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate_binding_id(length: int = 20) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choices(alphabet, k=length))


def format_binding_id(token: str, group_size: int = 4) -> str:
    return "-".join(token[i : i + group_size] for i in range(0, len(token), group_size))


@dataclasses.dataclass
class Config:
    bot_token: str
    poll_interval_sec: int
    telegram_proxy_url: str
    telegram_connect_timeout_sec: int
    telegram_read_timeout_sec: int
    telegram_api_base: str
    binding_id_ttl_sec: int
    sessions_root: str
    scan_interval_sec: int
    backfill_lines: int
    enabled_triggers: List[str]
    sqlite_path: str
    log_path: str
    pid_path: str
    mode_plan_template: str
    mode_default_template: str
    approve_plan_template: str
    reject_plan_template: str


def deep_get(d: Dict[str, Any], path: List[str], default: Any) -> Any:
    cur: Any = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def load_config(path: str) -> Config:
    raw: Dict[str, Any] = {}
    p = Path(path)
    if p.exists():
        with p.open("rb") as f:
            raw = tomllib.load(f)

    bot_token = str(deep_get(raw, ["telegram", "bot_token"], "")).strip()
    poll_interval_sec = int(deep_get(raw, ["telegram", "poll_interval_sec"], 2))
    telegram_proxy_url = str(deep_get(raw, ["telegram", "proxy_url"], "")).strip()
    telegram_connect_timeout_sec = int(deep_get(raw, ["telegram", "connect_timeout_sec"], 10))
    telegram_read_timeout_sec = int(deep_get(raw, ["telegram", "read_timeout_sec"], 30))
    telegram_api_base = str(deep_get(raw, ["telegram", "api_base"], "https://api.telegram.org")).strip()
    binding_id_ttl_sec = int(deep_get(raw, ["auth", "binding_id_ttl_sec"], 600))
    sessions_root = os.path.expanduser(str(deep_get(raw, ["monitor", "sessions_root"], DEFAULT_SESSIONS_ROOT)))
    scan_interval_sec = int(deep_get(raw, ["monitor", "scan_interval_sec"], 2))
    backfill_lines = int(deep_get(raw, ["monitor", "backfill_lines"], 3000))
    enabled_triggers = list(
        deep_get(
            raw,
            ["notify", "enabled_triggers"],
            [
                "task_complete",
                "request_user_input",
                "proposed_plan_ready",
            ],
        )
    )
    sqlite_path = os.path.expanduser(str(deep_get(raw, ["runtime", "sqlite_path"], DEFAULT_SQLITE_PATH)))
    log_path = os.path.expanduser(str(deep_get(raw, ["runtime", "log_path"], DEFAULT_LOG_PATH)))
    pid_path = os.path.expanduser(str(deep_get(raw, ["runtime", "pid_path"], DEFAULT_PID_PATH)))

    mode_plan_template = str(deep_get(raw, ["commands", "mode_plan_template"], "/plan"))
    mode_default_template = str(deep_get(raw, ["commands", "mode_default_template"], "/default"))
    approve_plan_template = str(deep_get(raw, ["commands", "approve_plan_template"], "Implement the plan."))
    reject_plan_template = str(deep_get(raw, ["commands", "reject_plan_template"], "Revise the plan with more detail, then resend it."))

    return Config(
        bot_token=bot_token,
        poll_interval_sec=max(1, poll_interval_sec),
        telegram_proxy_url=telegram_proxy_url,
        telegram_connect_timeout_sec=max(1, telegram_connect_timeout_sec),
        telegram_read_timeout_sec=max(1, telegram_read_timeout_sec),
        telegram_api_base=telegram_api_base or "https://api.telegram.org",
        binding_id_ttl_sec=max(60, binding_id_ttl_sec),
        sessions_root=sessions_root,
        scan_interval_sec=max(1, scan_interval_sec),
        backfill_lines=max(0, backfill_lines),
        enabled_triggers=enabled_triggers,
        sqlite_path=sqlite_path,
        log_path=log_path,
        pid_path=pid_path,
        mode_plan_template=mode_plan_template,
        mode_default_template=mode_default_template,
        approve_plan_template=approve_plan_template,
        reject_plan_template=reject_plan_template,
    )


def ensure_parent_dir(path: str) -> None:
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def setup_logging(log_path: str, verbose: bool = False) -> None:
    ensure_parent_dir(log_path)
    level = logging.DEBUG if verbose else logging.INFO
    handlers: List[logging.Handler] = [
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ]
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )


class DB:
    def __init__(self, path: str):
        ensure_parent_dir(path)
        self.conn = sqlite3.connect(path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.lock = threading.Lock()
        self._init_schema()

    def _init_schema(self) -> None:
        with self.lock:
            cur = self.conn.cursor()
            cur.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS tg_bindings (
                    chat_id INTEGER PRIMARY KEY,
                    user_label TEXT,
                    bound_at INTEGER NOT NULL,
                    status TEXT NOT NULL DEFAULT 'bound'
                );

                CREATE TABLE IF NOT EXISTS chat_state (
                    chat_id INTEGER PRIMARY KEY,
                    selected_session_ref TEXT,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS managed_sessions (
                    alias TEXT PRIMARY KEY,
                    tmux_session TEXT NOT NULL,
                    tmux_window TEXT,
                    tmux_pane TEXT,
                    codex_session_id TEXT,
                    cwd TEXT NOT NULL,
                    launch_nonce TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    last_seen_at INTEGER
                );

                CREATE UNIQUE INDEX IF NOT EXISTS idx_managed_codex_session_id
                ON managed_sessions(codex_session_id)
                WHERE codex_session_id IS NOT NULL;

                CREATE TABLE IF NOT EXISTS session_files (
                    file_path TEXT PRIMARY KEY,
                    session_id TEXT,
                    cwd TEXT,
                    first_seen INTEGER NOT NULL,
                    last_offset INTEGER NOT NULL DEFAULT 0,
                    skip_history INTEGER NOT NULL DEFAULT 0,
                    backfill_done INTEGER NOT NULL DEFAULT 0,
                    last_mtime INTEGER NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_session_files_session_id
                ON session_files(session_id);

                CREATE TABLE IF NOT EXISTS dedup_events (
                    dedup_key TEXT PRIMARY KEY,
                    created_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS pending_inputs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT NOT NULL,
                    turn_id TEXT,
                    kind TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    payload_hash TEXT NOT NULL DEFAULT '',
                    question_index INTEGER NOT NULL DEFAULT 0,
                    answers_json TEXT NOT NULL DEFAULT '[]',
                    status TEXT NOT NULL DEFAULT 'pending',
                    created_at INTEGER NOT NULL,
                    responded_at INTEGER
                );

                CREATE INDEX IF NOT EXISTS idx_pending_inputs_session_status
                ON pending_inputs(session_id, status);

                CREATE TABLE IF NOT EXISTS kv_store (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL,
                    updated_at INTEGER NOT NULL
                );

                CREATE TABLE IF NOT EXISTS bind_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    token_hash TEXT NOT NULL UNIQUE,
                    expires_at INTEGER NOT NULL,
                    created_at INTEGER NOT NULL,
                    used_by_chat_id INTEGER,
                    used_at INTEGER
                );
                """
            )
            self.conn.commit()
            self._migrate_schema(cur)
            self.conn.commit()

    def _migrate_schema(self, cur: sqlite3.Cursor) -> None:
        cur.execute("PRAGMA table_info(managed_sessions)")
        cols = {str(r[1]) for r in cur.fetchall()}
        if "launch_nonce" not in cols:
            cur.execute("ALTER TABLE managed_sessions ADD COLUMN launch_nonce TEXT NOT NULL DEFAULT ''")

        cur.execute("PRAGMA table_info(session_files)")
        session_file_cols = {str(r[1]) for r in cur.fetchall()}
        if "backfill_done" not in session_file_cols:
            cur.execute("ALTER TABLE session_files ADD COLUMN backfill_done INTEGER NOT NULL DEFAULT 0")

        cur.execute("PRAGMA table_info(pending_inputs)")
        pending_cols = {str(r[1]) for r in cur.fetchall()}
        if "payload_hash" not in pending_cols:
            cur.execute("ALTER TABLE pending_inputs ADD COLUMN payload_hash TEXT NOT NULL DEFAULT ''")
        if "question_index" not in pending_cols:
            cur.execute("ALTER TABLE pending_inputs ADD COLUMN question_index INTEGER NOT NULL DEFAULT 0")
        if "answers_json" not in pending_cols:
            cur.execute("ALTER TABLE pending_inputs ADD COLUMN answers_json TEXT NOT NULL DEFAULT '[]'")
        cur.execute("UPDATE pending_inputs SET question_index=0 WHERE question_index IS NULL")
        cur.execute("UPDATE pending_inputs SET answers_json='[]' WHERE answers_json IS NULL OR answers_json=''")
        cur.execute("SELECT id, payload_json FROM pending_inputs WHERE payload_hash='' OR payload_hash IS NULL")
        rows = cur.fetchall()
        for row in rows:
            pid = int(row[0])
            payload_json = str(row[1] or "{}")
            payload_hash = self.compute_payload_hash(payload_json)
            cur.execute("UPDATE pending_inputs SET payload_hash=? WHERE id=?", (payload_hash, pid))

    @staticmethod
    def compute_payload_hash(payload_json: str) -> str:
        return hashlib.sha256(payload_json.encode("utf-8")).hexdigest()

    def close(self) -> None:
        with self.lock:
            self.conn.close()

    def bind_chat(self, chat_id: int, user_label: str) -> None:
        now = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO tg_bindings(chat_id, user_label, bound_at, status)
                VALUES(?, ?, ?, 'bound')
                ON CONFLICT(chat_id) DO UPDATE SET
                    user_label=excluded.user_label,
                    bound_at=excluded.bound_at,
                    status='bound'
                """,
                (chat_id, user_label, now),
            )
            self.conn.commit()

    def unbind_chat(self, chat_id: int) -> None:
        with self.lock:
            self.conn.execute("DELETE FROM tg_bindings WHERE chat_id=?", (chat_id,))
            self.conn.execute("DELETE FROM chat_state WHERE chat_id=?", (chat_id,))
            self.conn.commit()

    def is_chat_bound(self, chat_id: int) -> bool:
        with self.lock:
            cur = self.conn.execute(
                "SELECT 1 FROM tg_bindings WHERE chat_id=? AND status='bound'",
                (chat_id,),
            )
            return cur.fetchone() is not None

    def list_bound_chats(self) -> List[int]:
        with self.lock:
            cur = self.conn.execute("SELECT chat_id FROM tg_bindings WHERE status='bound'")
            return [int(row[0]) for row in cur.fetchall()]

    def set_selected_session(self, chat_id: int, session_ref: str) -> None:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO chat_state(chat_id, selected_session_ref, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    selected_session_ref=excluded.selected_session_ref,
                    updated_at=excluded.updated_at
                """,
                (chat_id, session_ref, utc_ts()),
            )
            self.conn.commit()

    def get_selected_session(self, chat_id: int) -> Optional[str]:
        with self.lock:
            cur = self.conn.execute(
                "SELECT selected_session_ref FROM chat_state WHERE chat_id=?",
                (chat_id,),
            )
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else None

    def create_managed_session(self, alias: str, tmux_session: str, tmux_pane: str, cwd: str, launch_nonce: str) -> None:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO managed_sessions(alias, tmux_session, tmux_window, tmux_pane, codex_session_id, cwd, launch_nonce, status, created_at)
                VALUES(?, ?, '0', ?, NULL, ?, ?, 'running', ?)
                """,
                (alias, tmux_session, tmux_pane, cwd, launch_nonce, utc_ts()),
            )
            self.conn.commit()

    def attach_managed_session(self, alias: str, session_id: str) -> bool:
        with self.lock:
            cur = self.conn.execute(
                "UPDATE managed_sessions SET codex_session_id=?, last_seen_at=? WHERE alias=?",
                (session_id, utc_ts(), alias),
            )
            self.conn.commit()
            return cur.rowcount > 0

    def update_managed_status_by_alias(self, alias: str, status: str) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE managed_sessions SET status=?, last_seen_at=? WHERE alias=?",
                (status, utc_ts(), alias),
            )
            self.conn.commit()

    def update_managed_status_by_session_id(self, session_id: str, status: str) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE managed_sessions SET status=?, last_seen_at=? WHERE codex_session_id=?",
                (status, utc_ts(), session_id),
            )
            self.conn.commit()

    def maybe_attach_session_id(
        self,
        session_id: str,
        cwd: Optional[str],
        session_ts: Optional[int],
        launch_nonce: Optional[str],
    ) -> Optional[str]:
        now = session_ts or utc_ts()
        with self.lock:
            if launch_nonce:
                cur = self.conn.execute(
                    """
                    SELECT alias FROM managed_sessions
                    WHERE codex_session_id IS NULL
                      AND launch_nonce=?
                    ORDER BY created_at DESC
                    LIMIT 2
                    """,
                    (launch_nonce,),
                )
                rows = cur.fetchall()
                if len(rows) == 1:
                    alias = str(rows[0][0])
                    self.conn.execute(
                        "UPDATE managed_sessions SET codex_session_id=?, last_seen_at=? WHERE alias=?",
                        (session_id, utc_ts(), alias),
                    )
                    self.conn.commit()
                    return alias
                if len(rows) > 1:
                    for row in rows:
                        self.conn.execute(
                            "UPDATE managed_sessions SET status='awaiting_manual_attach', last_seen_at=? WHERE alias=?",
                            (utc_ts(), str(row[0])),
                        )
                    self.conn.commit()
                    return None

            if cwd is None:
                return None
            cur = self.conn.execute(
                """
                SELECT alias FROM managed_sessions
                WHERE codex_session_id IS NULL
                  AND status='running'
                  AND cwd=?
                  AND created_at >= ?
                ORDER BY created_at DESC
                LIMIT 2
                """,
                (cwd, now - 900),
            )
            rows = cur.fetchall()
            if len(rows) == 0:
                return None
            if len(rows) > 1:
                for row in rows:
                    self.conn.execute(
                        "UPDATE managed_sessions SET status='awaiting_manual_attach', last_seen_at=? WHERE alias=?",
                        (utc_ts(), str(row[0])),
                    )
                self.conn.commit()
                return None
            alias = str(rows[0][0])
            self.conn.execute(
                "UPDATE managed_sessions SET codex_session_id=?, last_seen_at=? WHERE alias=?",
                (session_id, utc_ts(), alias),
            )
            self.conn.commit()
            return alias

    def get_managed_by_alias(self, alias: str) -> Optional[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute("SELECT * FROM managed_sessions WHERE alias=?", (alias,))
            return cur.fetchone()

    def get_managed_by_session_id(self, session_id: str) -> Optional[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute("SELECT * FROM managed_sessions WHERE codex_session_id=?", (session_id,))
            return cur.fetchone()

    def list_managed_sessions(self) -> List[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute("SELECT * FROM managed_sessions ORDER BY created_at DESC")
            return cur.fetchall()

    def upsert_session_file(
        self,
        file_path: str,
        last_offset: int,
        skip_history: int,
        backfill_done: int,
        last_mtime: int,
    ) -> None:
        now = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO session_files(file_path, session_id, cwd, first_seen, last_offset, skip_history, backfill_done, last_mtime)
                VALUES(?, NULL, NULL, ?, ?, ?, ?, ?)
                ON CONFLICT(file_path) DO UPDATE SET
                    last_offset=excluded.last_offset,
                    skip_history=excluded.skip_history,
                    last_mtime=excluded.last_mtime
                """,
                (file_path, now, last_offset, skip_history, backfill_done, last_mtime),
            )
            self.conn.commit()

    def get_session_file(self, file_path: str) -> Optional[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute("SELECT * FROM session_files WHERE file_path=?", (file_path,))
            return cur.fetchone()

    def update_session_file_offset(self, file_path: str, last_offset: int, last_mtime: int) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE session_files SET last_offset=?, last_mtime=? WHERE file_path=?",
                (last_offset, last_mtime, file_path),
            )
            self.conn.commit()

    def mark_session_file_backfill_done(self, file_path: str) -> None:
        with self.lock:
            self.conn.execute("UPDATE session_files SET backfill_done=1 WHERE file_path=?", (file_path,))
            self.conn.commit()

    def update_session_file_meta(self, file_path: str, session_id: Optional[str], cwd: Optional[str]) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE session_files SET session_id=COALESCE(?, session_id), cwd=COALESCE(?, cwd) WHERE file_path=?",
                (session_id, cwd, file_path),
            )
            self.conn.commit()

    def resolve_session_id_for_path(self, file_path: str) -> Optional[str]:
        with self.lock:
            cur = self.conn.execute("SELECT session_id FROM session_files WHERE file_path=?", (file_path,))
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else None

    def list_discovered_sessions(self) -> List[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute(
                """
                SELECT session_id, MAX(cwd) AS cwd, MAX(last_mtime) AS last_mtime
                FROM session_files
                WHERE session_id IS NOT NULL
                GROUP BY session_id
                ORDER BY MAX(last_mtime) DESC
                """
            )
            return cur.fetchall()

    def add_dedup(self, key: str) -> bool:
        with self.lock:
            try:
                self.conn.execute(
                    "INSERT INTO dedup_events(dedup_key, created_at) VALUES(?, ?)",
                    (key, utc_ts()),
                )
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def get_kv(self, key: str) -> Optional[str]:
        with self.lock:
            cur = self.conn.execute("SELECT value FROM kv_store WHERE key=?", (key,))
            row = cur.fetchone()
            return str(row[0]) if row and row[0] is not None else None

    def set_kv(self, key: str, value: str) -> None:
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO kv_store(key, value, updated_at)
                VALUES(?, ?, ?)
                ON CONFLICT(key) DO UPDATE SET
                    value=excluded.value,
                    updated_at=excluded.updated_at
                """,
                (key, value, utc_ts()),
            )
            self.conn.commit()

    def create_bind_token(self, token_hash: str, expires_at: int) -> bool:
        if not token_hash:
            return False
        with self.lock:
            try:
                self.conn.execute(
                    """
                    INSERT INTO bind_tokens(token_hash, expires_at, created_at, used_by_chat_id, used_at)
                    VALUES(?, ?, ?, NULL, NULL)
                    """,
                    (token_hash, expires_at, utc_ts()),
                )
                self.conn.commit()
                return True
            except sqlite3.IntegrityError:
                return False

    def consume_bind_token(self, token_hash: str, used_by_chat_id: int) -> Tuple[bool, str]:
        now = utc_ts()
        with self.lock:
            cur = self.conn.execute(
                """
                UPDATE bind_tokens
                SET used_by_chat_id=?, used_at=?
                WHERE token_hash=?
                  AND used_by_chat_id IS NULL
                  AND expires_at >= ?
                """,
                (used_by_chat_id, now, token_hash, now),
            )
            if cur.rowcount > 0:
                self.conn.commit()
                return True, "ok"

            cur2 = self.conn.execute(
                "SELECT expires_at, used_by_chat_id FROM bind_tokens WHERE token_hash=?",
                (token_hash,),
            )
            row = cur2.fetchone()
            if not row:
                return False, "binding id not found"
            if row["used_by_chat_id"] is not None:
                return False, "binding id already used"
            if int(row["expires_at"]) < now:
                return False, "binding id expired"
            return False, "binding id unavailable"

    def create_pending_input(
        self,
        session_id: str,
        turn_id: Optional[str],
        kind: str,
        payload: Dict[str, Any],
    ) -> Tuple[int, str]:
        payload_json = json.dumps(payload, ensure_ascii=True, sort_keys=True)
        payload_hash = self.compute_payload_hash(payload_json)
        with self.lock:
            cur = self.conn.execute(
                """
                INSERT INTO pending_inputs(session_id, turn_id, kind, payload_json, payload_hash, question_index, answers_json, status, created_at)
                VALUES(?, ?, ?, ?, ?, 0, '[]', 'pending', ?)
                """,
                (session_id, turn_id, kind, payload_json, payload_hash, utc_ts()),
            )
            self.conn.commit()
            return int(cur.lastrowid), payload_hash

    def advance_pending_request_user_input(
        self,
        pending_id: int,
        expected_question_index: int,
        answer_entry: Dict[str, Any],
        total_questions: int,
    ) -> Tuple[bool, bool, int, str]:
        now = utc_ts()
        with self.lock:
            cur = self.conn.execute(
                "SELECT status, question_index, answers_json FROM pending_inputs WHERE id=?",
                (pending_id,),
            )
            row = cur.fetchone()
            if not row:
                return False, False, expected_question_index, "pending not found"
            if str(row["status"]) != "pending":
                return False, False, expected_question_index, "already handled"
            current_idx = int(row["question_index"] if row["question_index"] is not None else 0)
            if current_idx != expected_question_index:
                return False, False, current_idx, "stale question"

            answers_raw = str(row["answers_json"] or "[]")
            try:
                answers = json.loads(answers_raw)
                if not isinstance(answers, list):
                    answers = []
            except json.JSONDecodeError:
                answers = []
            answers.append(answer_entry)
            answers_json = json.dumps(answers, ensure_ascii=True, sort_keys=True)

            next_idx = expected_question_index + 1
            if next_idx >= total_questions:
                cur2 = self.conn.execute(
                    """
                    UPDATE pending_inputs
                    SET question_index=?, answers_json=?, status='responded', responded_at=?
                    WHERE id=? AND status='pending' AND question_index=?
                    """,
                    (next_idx, answers_json, now, pending_id, expected_question_index),
                )
                self.conn.commit()
                if cur2.rowcount <= 0:
                    return False, False, expected_question_index, "stale question"
                return True, True, next_idx, "ok"

            cur2 = self.conn.execute(
                """
                UPDATE pending_inputs
                SET question_index=?, answers_json=?
                WHERE id=? AND status='pending' AND question_index=?
                """,
                (next_idx, answers_json, pending_id, expected_question_index),
            )
            self.conn.commit()
            if cur2.rowcount <= 0:
                return False, False, expected_question_index, "stale question"
            return True, False, next_idx, "ok"

    def mark_pending_responded_with_answers(self, pending_id: int, answers: List[Dict[str, Any]], final_question_index: int) -> None:
        with self.lock:
            answers_json = json.dumps(answers, ensure_ascii=True, sort_keys=True)
            self.conn.execute(
                """
                UPDATE pending_inputs
                SET status='responded', responded_at=?, answers_json=?, question_index=?
                WHERE id=?
                """,
                (utc_ts(), answers_json, final_question_index, pending_id),
            )
            self.conn.commit()

    def mark_managed_awaiting_manual_attach_for_nonces(self, nonces: List[str]) -> int:
        uniq = sorted({str(v).strip() for v in nonces if str(v).strip()})
        if not uniq:
            return 0
        placeholders = ",".join("?" for _ in uniq)
        with self.lock:
            cur = self.conn.execute(
                f"""
                UPDATE managed_sessions
                SET status='awaiting_manual_attach', last_seen_at=?
                WHERE codex_session_id IS NULL
                  AND launch_nonce IN ({placeholders})
                """,
                (utc_ts(), *uniq),
            )
            self.conn.commit()
            return int(cur.rowcount)

    def get_pending_for_session(self, session_id: str) -> List[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute(
                """
                SELECT * FROM pending_inputs
                WHERE session_id=? AND status='pending'
                ORDER BY created_at ASC
                """,
                (session_id,),
            )
            return cur.fetchall()

    def get_pending_by_id(self, pending_id: int) -> Optional[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute("SELECT * FROM pending_inputs WHERE id=?", (pending_id,))
            return cur.fetchone()

    def mark_pending_responded(self, pending_id: int) -> None:
        with self.lock:
            self.conn.execute(
                "UPDATE pending_inputs SET status='responded', responded_at=? WHERE id=?",
                (utc_ts(), pending_id),
            )
            self.conn.commit()


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

    def send_message(self, chat_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> None:
        payload: Dict[str, Any] = {"chat_id": chat_id, "text": text}
        if reply_markup is not None:
            payload["reply_markup"] = reply_markup
        self._call("sendMessage", payload)

    def answer_callback_query(self, callback_query_id: str, text: str = "") -> None:
        payload = {"callback_query_id": callback_query_id}
        if text:
            payload["text"] = text
        self._call("answerCallbackQuery", payload)

    def set_my_commands(self, commands: List[Dict[str, str]]) -> None:
        payload: Dict[str, Any] = {"commands": commands}
        self._call("setMyCommands", payload)


class TmuxController:
    @staticmethod
    def _run_tmux(args: List[str], check: bool = True) -> subprocess.CompletedProcess:
        cmd = ["tmux"] + args
        return subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=check)

    def create_session(self, session_name: str, cwd: str, command: str) -> str:
        self._run_tmux(["new-session", "-d", "-s", session_name, "-c", cwd, command])
        result = self._run_tmux(["list-panes", "-t", session_name, "-F", "#{pane_id}"])
        pane = result.stdout.strip().splitlines()[0]
        return pane

    def pane_exists(self, pane_id: str) -> bool:
        try:
            result = self._run_tmux(["list-panes", "-a", "-F", "#{pane_id}"], check=True)
            panes = {line.strip() for line in result.stdout.splitlines() if line.strip()}
            return pane_id in panes
        except subprocess.CalledProcessError:
            return False

    def send_text(self, pane_id: str, text: str, enter: bool = True) -> None:
        self._run_tmux(["set-buffer", "--", text])
        self._run_tmux(["paste-buffer", "-t", pane_id])
        if enter:
            self._run_tmux(["send-keys", "-t", pane_id, "C-m"])


@dataclasses.dataclass
class SessionState:
    current_turn_id: Optional[str] = None
    proposed_plan_turns: set = dataclasses.field(default_factory=set)
    assistant_text_by_turn: Dict[str, str] = dataclasses.field(default_factory=dict)


class NotificationBus:
    def __init__(self):
        self.q: "queue.Queue[Dict[str, Any]]" = queue.Queue()

    def publish(self, message: Dict[str, Any]) -> None:
        self.q.put(message)

    def poll(self) -> Optional[Dict[str, Any]]:
        try:
            return self.q.get_nowait()
        except queue.Empty:
            return None


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
            alias = self.db.maybe_attach_session_id(
                str(session_id),
                str(cwd) if cwd else None,
                session_ts,
                launch_nonce,
            )
            if alias:
                logging.info("attached session_id %s -> alias %s", session_id, alias)

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
        alias = self.db.maybe_attach_session_id(session_id_str, cwd_val, None, launch_nonce)
        if alias:
            logging.info("attached existing session_id %s -> alias %s", session_id_str, alias)

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

    def _emit_once(self, dedup_key: str, event: Dict[str, Any]) -> None:
        if self.db.add_dedup(dedup_key):
            self.bus.publish(event)

    def _split_for_limit(self, text: str, limit: int) -> Tuple[str, str]:
        if len(text) <= limit:
            return text, ""
        cut = text.rfind("\n", 0, limit + 1)
        if cut < int(limit * 0.6):
            cut = text.rfind(" ", 0, limit + 1)
        if cut < int(limit * 0.6):
            cut = limit
        head = text[:cut].rstrip()
        tail = text[cut:].lstrip()
        return head, tail

    def _format_assistant_text_parts(
        self,
        text: str,
        primary_chars: int = 700,
        continued_chars: int = 1200,
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

        primary, rest = self._split_for_limit(normalized, primary_chars)
        if not rest:
            return primary, ""
        continued, remainder = self._split_for_limit(rest, continued_chars)
        if remainder:
            continued = f"{continued.rstrip()} ..."
        return primary, continued

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

            if ev_type == "task_complete":
                turn = payload.get("turn_id")
                if not isinstance(turn, str) or not turn:
                    turn = state.current_turn_id or "unknown"

                if mode == "live" and "task_complete" in self.cfg.enabled_triggers:
                    dedup = f"{session_key}:{turn}:task_complete"
                    assistant_text = state.assistant_text_by_turn.get(turn)
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
                    turn = state.current_turn_id or "unknown"
                    state.assistant_text_by_turn[turn] = "\n\n".join(texts)
                    if len(state.assistant_text_by_turn) > 80:
                        oldest = next(iter(state.assistant_text_by_turn))
                        del state.assistant_text_by_turn[oldest]
                if found:
                    turn = state.current_turn_id or "unknown"
                    state.proposed_plan_turns.add(turn)
                return


class TelegramService(threading.Thread):
    SESSIONS_PAGE_SIZE = 6

    def __init__(self, cfg: Config, db: DB, bus: NotificationBus, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.cfg = cfg
        self.db = db
        self.bus = bus
        self.stop_event = stop_event
        self.tmux = TmuxController()
        self.api = TelegramAPI(
            token=cfg.bot_token,
            api_base=cfg.telegram_api_base,
            proxy_url=cfg.telegram_proxy_url,
            connect_timeout_sec=cfg.telegram_connect_timeout_sec,
            read_timeout_sec=cfg.telegram_read_timeout_sec,
        )
        self.update_offset: Optional[int] = None

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
                self._flush_notifications(limit=20)
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
                self._flush_notifications(limit=20)
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

    def _send_main_menu(self, chat_id: int, text: str) -> None:
        self._reply(chat_id, text, reply_markup=self._main_menu_keyboard())

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
        lines = ["[Codex] managed sessions"]
        if not managed:
            lines.append("- none")
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
            lines.append(f"{start + idx + 1}. {marker} {alias} | {status} | sid={sid}")
            button_text = f"{'[*] ' if active else ''}{alias} ({status})"
            callback = f"sl|s|{page}|{idx}"
            buttons.append([{"text": button_text[:64], "callback_data": callback[:64]}])

        lines.append(f"page: {page + 1}/{total_pages}")
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
            self.api.send_message(chat_id, text, reply_markup=keyboard)
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
            self._send_main_menu(chat_id, f"Selected: {ref}\n{self._format_selected_status(chat_id)}")
            return

        self.api.answer_callback_query(cb_id, "Unsupported callback")

    def _flush_notifications(self, limit: int) -> None:
        for _ in range(limit):
            msg = self.bus.poll()
            if msg is None:
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

        if etype == "task_complete":
            lines = [
                "[Codex] task complete",
                f"session: {session_label}",
                f"turn: {event.get('turn_id', 'unknown')}",
            ]
            primary = str(event.get("assistant_text_primary") or event.get("assistant_text") or "").strip()
            continued_text = str(event.get("assistant_text_continued") or "").strip()
            if primary:
                lines.append("")
                lines.append("assistant summary:")
                lines.append(primary)
            text = "\n".join(lines)
        elif etype == "proposed_plan_ready":
            lines = [
                "[Codex] plan ready for execution",
                f"session: {session_label}",
                f"turn: {event.get('turn_id', 'unknown')}",
                "Tap Approve Plan to auto-select this session and execute.",
            ]
            primary = str(event.get("assistant_text_primary") or event.get("assistant_text") or "").strip()
            continued_text = str(event.get("assistant_text_continued") or "").strip()
            if primary:
                lines.append("")
                lines.append("assistant summary:")
                lines.append(primary)
            text = "\n".join(lines)
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
                text = f"[Codex] request_user_input\nsession: {session_label}\n(pending payload unavailable)"
        else:
            text = f"[Codex] event: {etype}\n{json.dumps(event, ensure_ascii=True)}"

        for chat_id in chats:
            try:
                self.api.send_message(chat_id, text, reply_markup=keyboard)
                if continued_text:
                    self.api.send_message(chat_id, f"[Codex] assistant (continued)\n\n{continued_text}")
            except Exception:
                logging.exception("failed to send telegram event")

    def _render_pending_question_message(
        self,
        session_label: str,
        pending: sqlite3.Row,
    ) -> Tuple[str, Optional[Dict[str, Any]]]:
        pending_id = int(pending["id"])
        payload_hash = str(pending["payload_hash"] or "")
        lines = [f"[Codex] request_user_input", f"session: {session_label}"]
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

        lines.append(f"question {question_index + 1}/{len(questions)}")
        prompt = str(question.get("question", "")).strip()
        if prompt:
            lines.append(f"prompt: {prompt}")

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
            lines.append(f"{idx + 1}. {label}")
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
            self.api.send_message(chat_id, text_next, reply_markup=keyboard_next)
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

        if not raw_text:
            return

        text = self._normalize_message_text(raw_text)
        cmd, arg = self._split_command(text)

        if cmd == "/start":
            self._send_main_menu(chat_id, "Use /bind <binding_id> to bind this chat.")
            return

        if cmd == "/help":
            self._send_main_menu(
                chat_id,
                "Commands:\n"
                "/bind <binding_id>\n"
                "/unbind\n"
                "/menu\n"
                "/sessions\n"
                "/select <alias|session_id>\n"
                "/status\n"
                "/send <text>\n"
                "/mode <plan>\n"
                "/approve\n"
                "/reject\n\n"
                "Tip: plain text (without leading /) is sent to the selected managed session.",
            )
            return

        if cmd == "/bind":
            if self.db.is_chat_bound(chat_id):
                self._reply(chat_id, "Already bound.")
                return
            token = normalize_binding_id(arg)
            if not token or not is_valid_binding_id(token):
                self._reply(chat_id, "Usage: /bind <binding_id>")
                return
            ok, msg = self.db.consume_bind_token(hash_binding_id(token), chat_id)
            if not ok:
                self._reply(chat_id, f"Bind failed: {msg}")
                return
            self.db.bind_chat(chat_id, username)
            self._send_main_menu(chat_id, "Bind success. Use buttons or commands to control sessions.")
            return

        if cmd == "/unbind":
            self.db.unbind_chat(chat_id)
            self._reply(chat_id, "Unbound.")
            return

        if cmd == "/menu":
            if not self.db.is_chat_bound(chat_id):
                self._reply(chat_id, "Not bound. Use /bind <binding_id>.")
                return
            self._send_main_menu(chat_id, "Main menu ready.")
            return

        if not self.db.is_chat_bound(chat_id):
            self._reply(chat_id, "Not bound. Use /bind <binding_id>.")
            return

        if cmd == "/sessions":
            text_page, keyboard, _ = self._render_sessions_page(chat_id, page=0)
            self._reply(chat_id, text_page, reply_markup=keyboard)
            return

        if cmd == "/select":
            if not arg:
                text_page, keyboard, _ = self._render_sessions_page(chat_id, page=0)
                self._reply(chat_id, text_page, reply_markup=keyboard)
                return
            ref = self._resolve_session_ref(arg)
            if not ref:
                self._reply(chat_id, "Session not found.")
                return
            self.db.set_selected_session(chat_id, ref)
            self._reply(chat_id, f"Selected: {ref}")
            return

        if cmd == "/status":
            self._reply(chat_id, self._format_selected_status(chat_id))
            return

        if cmd == "/send":
            if not arg:
                self._reply(chat_id, "Usage: /send <text>")
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
                self._reply(chat_id, "Usage: /mode <plan>")
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

    def _reply(self, chat_id: int, text: str, reply_markup: Optional[Dict[str, Any]] = None) -> None:
        self.api.send_message(chat_id, text, reply_markup=reply_markup)

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


class Daemon:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.stop_event = threading.Event()
        self.db = DB(cfg.sqlite_path)
        self.bus = NotificationBus()
        self.monitor = SessionMonitor(cfg, self.db, self.bus, self.stop_event)
        self.telegram: Optional[TelegramService] = None
        if cfg.bot_token:
            self.telegram = TelegramService(cfg, self.db, self.bus, self.stop_event)

    def run(self) -> int:
        if not self.cfg.bot_token:
            logging.warning("telegram.bot_token is empty; running monitor only")

        def _handle_signal(signum: int, _frame: Any) -> None:
            logging.info("signal %s received, stopping", signum)
            self.stop_event.set()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        self.monitor.start()
        if self.telegram:
            self.telegram.start()

        while not self.stop_event.is_set():
            time.sleep(0.5)

        self.monitor.join(timeout=5)
        if self.telegram:
            self.telegram.join(timeout=5)
        self.db.close()
        return 0


def is_pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except OSError:
        return False


def read_pid(pid_path: str) -> Optional[int]:
    p = Path(pid_path)
    if not p.exists():
        return None
    try:
        return int(p.read_text(encoding="utf-8").strip())
    except Exception:
        return None


def write_pid(pid_path: str, pid: int) -> None:
    ensure_parent_dir(pid_path)
    Path(pid_path).write_text(str(pid), encoding="utf-8")


def remove_pid(pid_path: str) -> None:
    p = Path(pid_path)
    if p.exists():
        p.unlink()


def cmd_init_config(args: argparse.Namespace) -> int:
    path = os.path.expanduser(args.config)
    p = Path(path)
    if p.exists() and not args.force:
        print(f"Config exists: {path}")
        return 1
    ensure_parent_dir(path)
    p.write_text(
        """[telegram]
bot_token = ""
poll_interval_sec = 2
proxy_url = ""
connect_timeout_sec = 10
read_timeout_sec = 30
api_base = "https://api.telegram.org"

[auth]
binding_id_ttl_sec = 600

[monitor]
sessions_root = "~/.codex/sessions"
scan_interval_sec = 2
backfill_lines = 3000

[notify]
enabled_triggers = ["task_complete", "request_user_input", "proposed_plan_ready"]

[runtime]
sqlite_path = "~/.local/state/codex-watch/state.sqlite3"
log_path = "~/.local/state/codex-watch/codex-watch.log"
pid_path = "~/.local/state/codex-watch/codex-watch.pid"

[commands]
mode_plan_template = "/plan"
mode_default_template = "/default"
approve_plan_template = "Implement the plan."
reject_plan_template = "Revise the plan with more detail, then resend it."
""",
        encoding="utf-8",
    )
    print(f"Wrote config: {path}")
    return 0


def build_tmux_session_name(alias: str) -> str:
    safe = "".join(ch if (ch.isalnum() or ch in "-_") else "-" for ch in alias)
    return f"cw_{safe}"


def build_launch_nonce(alias: str) -> str:
    safe = "".join(ch if (ch.isalnum() or ch in "-_") else "-" for ch in alias)[:24]
    rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{safe}-{utc_ts()}-{rand}"


def cmd_run_session(args: argparse.Namespace, cfg: Config, db: DB) -> int:
    alias = args.name.strip()
    if not alias:
        print("--name is required", file=sys.stderr)
        return 1
    cwd = os.path.abspath(os.path.expanduser(args.cwd))
    if not os.path.isdir(cwd):
        print(f"cwd does not exist: {cwd}", file=sys.stderr)
        return 1

    if db.get_managed_by_alias(alias):
        print(f"alias exists: {alias}", file=sys.stderr)
        return 1

    codex_args = list(args.codex_args) if args.codex_args else ["--dangerously-bypass-approvals-and-sandbox"]
    if codex_args and codex_args[0] == "--":
        codex_args = codex_args[1:]
    if not codex_args:
        codex_args = ["--dangerously-bypass-approvals-and-sandbox"]
    cmd = ["codex"] + codex_args
    launch_nonce = build_launch_nonce(alias)
    shell_cmd = f"CODEX_WATCH_NONCE={shlex.quote(launch_nonce)} {shlex.join(cmd)}"

    tmux_session = build_tmux_session_name(alias)
    tmux = TmuxController()

    try:
        pane = tmux.create_session(tmux_session, cwd, shell_cmd)
    except subprocess.CalledProcessError as e:
        print(e.stderr.strip() or "tmux create session failed", file=sys.stderr)
        return 1

    db.create_managed_session(alias, tmux_session, pane, cwd, launch_nonce)
    print(f"started alias={alias} tmux_session={tmux_session} pane={pane}")
    print(f"launch_nonce={launch_nonce}")
    print("session_id will auto-attach after codex writes session_meta")
    return 0


def cmd_sessions_list(db: DB) -> int:
    managed = db.list_managed_sessions()
    print("Managed sessions:")
    if not managed:
        print("- none")
    else:
        for row in managed:
            print(
                f"- alias={row['alias']} status={row['status']} "
                f"session_id={row['codex_session_id'] or '-'} pane={row['tmux_pane']} cwd={row['cwd']} nonce={row['launch_nonce'] or '-'}"
            )

    managed_sids = {str(r["codex_session_id"]) for r in managed if r["codex_session_id"]}
    discovered = db.list_discovered_sessions()
    legacy = [row for row in discovered if str(row["session_id"]) not in managed_sids]

    print("\nLegacy sessions (notify-only):")
    if not legacy:
        print("- none")
    else:
        for row in legacy[:50]:
            print(f"- session_id={row['session_id']} cwd={row['cwd'] or '-'}")

    return 0


def cmd_auth_issue_bind_id(args: argparse.Namespace, cfg: Config, db: DB) -> int:
    ttl = int(args.ttl) if int(args.ttl) > 0 else cfg.binding_id_ttl_sec
    ttl = max(60, ttl)
    for _ in range(8):
        token = generate_binding_id()
        token_hash = hash_binding_id(token)
        expires_at = utc_ts() + ttl
        if db.create_bind_token(token_hash, expires_at):
            print(f"binding_id={format_binding_id(token)}")
            print(f"expires_at={utc_iso(expires_at)}")
            print("Use it in Telegram: /bind <binding_id>")
            return 0
    print("failed to issue binding id; retry", file=sys.stderr)
    return 1


def cmd_sessions_attach(args: argparse.Namespace, db: DB) -> int:
    ok = db.attach_managed_session(args.name, args.session_id)
    if not ok:
        print("attach failed: alias not found", file=sys.stderr)
        return 1
    print(f"attached alias={args.name} -> session_id={args.session_id}")
    return 0


def cmd_daemon_start(args: argparse.Namespace, cfg: Config) -> int:
    pid = read_pid(cfg.pid_path)
    if pid and is_pid_alive(pid):
        print(f"daemon already running pid={pid}")
        return 1

    if args.foreground:
        write_pid(cfg.pid_path, os.getpid())
        try:
            daemon = Daemon(cfg)
            return daemon.run()
        finally:
            remove_pid(cfg.pid_path)

    ensure_parent_dir(cfg.log_path)
    logf = open(cfg.log_path, "a", encoding="utf-8")
    cmd = [sys.executable, os.path.abspath(__file__), "--config", args.config, "daemon", "run"]
    proc = subprocess.Popen(cmd, stdout=logf, stderr=logf, start_new_session=True)
    write_pid(cfg.pid_path, proc.pid)
    print(f"daemon started pid={proc.pid}")
    return 0


def cmd_daemon_run(args: argparse.Namespace, cfg: Config) -> int:
    write_pid(cfg.pid_path, os.getpid())
    try:
        daemon = Daemon(cfg)
        return daemon.run()
    finally:
        remove_pid(cfg.pid_path)


def cmd_daemon_stop(cfg: Config) -> int:
    pid = read_pid(cfg.pid_path)
    if not pid:
        print("daemon not running")
        return 1
    if not is_pid_alive(pid):
        remove_pid(cfg.pid_path)
        print("stale pid removed")
        return 1
    os.kill(pid, signal.SIGTERM)
    print(f"sent SIGTERM to pid={pid}")
    return 0


def cmd_daemon_status(cfg: Config) -> int:
    pid = read_pid(cfg.pid_path)
    if pid and is_pid_alive(pid):
        print(f"running pid={pid}")
        return 0
    print("not running")
    return 1


def cmd_daemon_restart(args: argparse.Namespace, cfg: Config) -> int:
    pid = read_pid(cfg.pid_path)
    if pid and is_pid_alive(pid):
        os.kill(pid, signal.SIGTERM)
        print(f"sent SIGTERM to pid={pid}")
        deadline = time.time() + 10
        while time.time() < deadline:
            if not is_pid_alive(pid):
                break
            time.sleep(0.2)
        if is_pid_alive(pid):
            print(f"failed to stop daemon pid={pid} within 10s", file=sys.stderr)
            return 1
        remove_pid(cfg.pid_path)
        print("daemon stopped")
    elif pid:
        remove_pid(cfg.pid_path)
        print("stale pid removed")
    else:
        print("daemon not running, starting new daemon")
    return cmd_daemon_start(args, cfg)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog=APP_NAME)
    parser.add_argument("--config", default=DEFAULT_CONFIG_PATH)
    parser.add_argument("--verbose", action="store_true")

    sub = parser.add_subparsers(dest="command", required=True)

    init_p = sub.add_parser("init-config")
    init_p.add_argument("--force", action="store_true")

    run_p = sub.add_parser("run")
    run_p.add_argument("--name", required=True)
    run_p.add_argument("--cwd", required=True)
    run_p.add_argument("codex_args", nargs=argparse.REMAINDER)

    sess_p = sub.add_parser("sessions")
    sess_sub = sess_p.add_subparsers(dest="sessions_cmd", required=True)
    sess_sub.add_parser("list")
    att = sess_sub.add_parser("attach")
    att.add_argument("--name", required=True)
    att.add_argument("--session-id", required=True)

    auth = sub.add_parser("auth")
    auth_sub = auth.add_subparsers(dest="auth_cmd", required=True)
    issue = auth_sub.add_parser("issue-bind-id")
    issue.add_argument("--ttl", type=int, default=0, help="binding id ttl seconds (default from config)")

    daemon = sub.add_parser("daemon")
    dsub = daemon.add_subparsers(dest="daemon_cmd", required=True)
    dstart = dsub.add_parser("start")
    dstart.add_argument("--foreground", action="store_true")
    dsub.add_parser("run")
    dsub.add_parser("stop")
    dsub.add_parser("status")
    drestart = dsub.add_parser("restart")
    drestart.add_argument("--foreground", action="store_true")

    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)

    cfg = load_config(args.config)
    setup_logging(cfg.log_path, verbose=args.verbose)

    if args.command == "init-config":
        return cmd_init_config(args)

    if args.command == "daemon":
        if args.daemon_cmd == "start":
            return cmd_daemon_start(args, cfg)
        if args.daemon_cmd == "run":
            return cmd_daemon_run(args, cfg)
        if args.daemon_cmd == "stop":
            return cmd_daemon_stop(cfg)
        if args.daemon_cmd == "status":
            return cmd_daemon_status(cfg)
        if args.daemon_cmd == "restart":
            return cmd_daemon_restart(args, cfg)
        return 1

    db = DB(cfg.sqlite_path)
    try:
        if args.command == "run":
            return cmd_run_session(args, cfg, db)
        if args.command == "auth":
            if args.auth_cmd == "issue-bind-id":
                return cmd_auth_issue_bind_id(args, cfg, db)
            return 1
        if args.command == "sessions":
            if args.sessions_cmd == "list":
                return cmd_sessions_list(db)
            if args.sessions_cmd == "attach":
                return cmd_sessions_attach(args, db)
        return 1
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
