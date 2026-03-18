import hashlib
import json
import logging
import sqlite3
import threading
from typing import Any, Dict, List, Optional, Tuple

from .common import ensure_parent_dir, utc_ts

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

                CREATE TABLE IF NOT EXISTS tg_session_routes (
                    alias TEXT PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    bound_at INTEGER NOT NULL,
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
            self.conn.execute("DELETE FROM tg_session_routes WHERE chat_id=?", (chat_id,))
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
    ) -> Tuple[Optional[str], str]:
        with self.lock:
            sid = str(session_id).strip()
            nonce = str(launch_nonce).strip() if launch_nonce else ""
            if not sid:
                return None, "missing_session_id"
            if not nonce:
                return None, "missing_launch_nonce"

            now = utc_ts()

            # 1) First try strict attach to a unique unbound managed row by nonce.
            cur_unbound = self.conn.execute(
                """
                SELECT alias
                FROM managed_sessions
                WHERE codex_session_id IS NULL
                  AND launch_nonce=?
                ORDER BY created_at DESC
                """,
                (nonce,),
            )
            unbound_rows = cur_unbound.fetchall()
            if len(unbound_rows) == 1:
                alias = str(unbound_rows[0][0])
                self.conn.execute(
                    "UPDATE managed_sessions SET codex_session_id=?, last_seen_at=? WHERE alias=?",
                    (sid, now, alias),
                )
                self.conn.commit()
                return alias, "nonce_unbound"
            if len(unbound_rows) > 1:
                for row in unbound_rows:
                    self.conn.execute(
                        "UPDATE managed_sessions SET status='awaiting_manual_attach', last_seen_at=? WHERE alias=?",
                        (now, str(row[0])),
                    )
                self.conn.commit()
                return None, "nonce_ambiguous_unbound"

            # 2) Rotate attach: same nonce uniquely identifies an existing managed alias.
            cur_nonce_all = self.conn.execute(
                """
                SELECT alias, codex_session_id
                FROM managed_sessions
                WHERE launch_nonce=?
                ORDER BY created_at DESC
                """,
                (nonce,),
            )
            nonce_rows = cur_nonce_all.fetchall()
            if len(nonce_rows) == 1:
                alias = str(nonce_rows[0][0])
                current_sid = str(nonce_rows[0][1] or "").strip()
                if current_sid == sid:
                    # already attached to the same session id
                    return alias, "already_attached"

                # If sid is already attached elsewhere, do not rebind automatically.
                cur_sid_owner = self.conn.execute(
                    "SELECT alias FROM managed_sessions WHERE codex_session_id=?",
                    (sid,),
                )
                sid_owner = cur_sid_owner.fetchone()
                if sid_owner and str(sid_owner[0]) != alias:
                    logging.warning(
                        "skip nonce rotate attach sid=%s nonce=%s alias=%s reason=sid_owned_by_other owner=%s",
                        sid,
                        nonce,
                        alias,
                        str(sid_owner[0]),
                    )
                    return None, "sid_owned_by_other"

                self.conn.execute(
                    "UPDATE managed_sessions SET codex_session_id=?, last_seen_at=? WHERE alias=?",
                    (sid, now, alias),
                )
                self.conn.commit()
                return alias, "nonce_rotate"

            if len(nonce_rows) > 1:
                # Ambiguous nonce across multiple managed aliases; do not auto-attach.
                return None, "nonce_ambiguous_existing"

            return None, "nonce_not_found"

    def get_managed_by_alias(self, alias: str) -> Optional[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute("SELECT * FROM managed_sessions WHERE alias=?", (alias,))
            return cur.fetchone()

    def get_managed_by_session_id(self, session_id: str) -> Optional[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute("SELECT * FROM managed_sessions WHERE codex_session_id=?", (session_id,))
            return cur.fetchone()

    def has_managed_session_id(self, session_id: str) -> bool:
        sid = str(session_id or "").strip()
        if not sid:
            return False
        with self.lock:
            cur = self.conn.execute(
                "SELECT 1 FROM managed_sessions WHERE codex_session_id=? LIMIT 1",
                (sid,),
            )
            return cur.fetchone() is not None

    def has_managed_launch_nonce(self, launch_nonce: str) -> bool:
        nonce = str(launch_nonce or "").strip()
        if not nonce:
            return False
        with self.lock:
            cur = self.conn.execute(
                "SELECT 1 FROM managed_sessions WHERE launch_nonce=? LIMIT 1",
                (nonce,),
            )
            return cur.fetchone() is not None

    def list_managed_sessions(self) -> List[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute("SELECT * FROM managed_sessions ORDER BY created_at DESC")
            return cur.fetchall()

    def delete_managed_session(self, alias: str) -> bool:
        with self.lock:
            cur = self.conn.execute("DELETE FROM managed_sessions WHERE alias=?", (alias,))
            self.conn.execute("DELETE FROM tg_session_routes WHERE alias=?", (alias,))
            self.conn.commit()
            return cur.rowcount > 0

    def set_session_route(self, alias: str, chat_id: int) -> None:
        now = utc_ts()
        with self.lock:
            self.conn.execute(
                """
                INSERT INTO tg_session_routes(alias, chat_id, bound_at, updated_at)
                VALUES(?, ?, ?, ?)
                ON CONFLICT(alias) DO UPDATE SET
                    chat_id=excluded.chat_id,
                    updated_at=excluded.updated_at
                """,
                (alias, chat_id, now, now),
            )
            self.conn.commit()

    def remove_session_route(self, alias: str) -> bool:
        with self.lock:
            cur = self.conn.execute("DELETE FROM tg_session_routes WHERE alias=?", (alias,))
            self.conn.commit()
            return cur.rowcount > 0

    def get_session_route_chat(self, alias: str) -> Optional[int]:
        with self.lock:
            cur = self.conn.execute("SELECT chat_id FROM tg_session_routes WHERE alias=?", (alias,))
            row = cur.fetchone()
            if not row or row[0] is None:
                return None
            return int(row[0])

    def list_session_routes(self) -> List[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute(
                """
                SELECT alias, chat_id, bound_at, updated_at
                FROM tg_session_routes
                ORDER BY updated_at DESC, alias ASC
                """
            )
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

    def get_discovered_session(self, session_id: str) -> Optional[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute(
                """
                SELECT session_id, MAX(cwd) AS cwd, MAX(last_mtime) AS last_mtime
                FROM session_files
                WHERE session_id=?
                GROUP BY session_id
                """,
                (session_id,),
            )
            return cur.fetchone()

    def list_running_managed_by_cwd(self, cwd: str) -> List[sqlite3.Row]:
        with self.lock:
            cur = self.conn.execute(
                """
                SELECT *
                FROM managed_sessions
                WHERE cwd=?
                  AND status='running'
                  AND codex_session_id IS NOT NULL
                ORDER BY created_at DESC
                """,
                (cwd,),
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

    def cleanup_legacy_records(self, dry_run: bool = False) -> Dict[str, int]:
        managed_sid_subquery = (
            "SELECT codex_session_id FROM managed_sessions "
            "WHERE codex_session_id IS NOT NULL AND codex_session_id!=''"
        )
        legacy_session_predicate = (
            "session_id IS NULL OR session_id='' OR "
            f"session_id NOT IN ({managed_sid_subquery})"
        )
        dedup_prefix = (
            "CASE "
            "WHEN instr(dedup_key, ':') > 0 THEN substr(dedup_key, 1, instr(dedup_key, ':') - 1) "
            "ELSE dedup_key END"
        )
        legacy_dedup_predicate = (
            f"{dedup_prefix} IS NULL OR {dedup_prefix}='' OR "
            f"{dedup_prefix} NOT IN ({managed_sid_subquery})"
        )
        legacy_chat_session_predicate = (
            "selected_session_ref LIKE 'session:%' AND "
            "substr(selected_session_ref, 9) NOT IN "
            f"({managed_sid_subquery})"
        )

        with self.lock:
            cur = self.conn.cursor()

            cur.execute(f"SELECT COUNT(*) FROM session_files WHERE {legacy_session_predicate}")
            session_files = int(cur.fetchone()[0])

            cur.execute(f"SELECT COUNT(*) FROM pending_inputs WHERE {legacy_session_predicate}")
            pending_inputs = int(cur.fetchone()[0])

            cur.execute(f"SELECT COUNT(*) FROM dedup_events WHERE {legacy_dedup_predicate}")
            dedup_events = int(cur.fetchone()[0])

            cur.execute(f"SELECT COUNT(*) FROM chat_state WHERE {legacy_chat_session_predicate}")
            chat_state = int(cur.fetchone()[0])

            result = {
                "session_files": session_files,
                "pending_inputs": pending_inputs,
                "dedup_events": dedup_events,
                "chat_state": chat_state,
                "total": session_files + pending_inputs + dedup_events + chat_state,
            }

            if dry_run:
                return result

            cur.execute(f"DELETE FROM session_files WHERE {legacy_session_predicate}")
            cur.execute(f"DELETE FROM pending_inputs WHERE {legacy_session_predicate}")
            cur.execute(f"DELETE FROM dedup_events WHERE {legacy_dedup_predicate}")
            cur.execute(f"DELETE FROM chat_state WHERE {legacy_chat_session_predicate}")
            self.conn.commit()
            return result
