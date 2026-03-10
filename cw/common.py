import datetime as dt
import hashlib
import logging
import os
import random
import re
import signal
import time
from pathlib import Path
from typing import Any, List, Optional

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
    return re.sub(r"[\s-]+", "", raw.strip().upper())


def is_valid_binding_id(token: str) -> bool:
    return bool(re.fullmatch(r"[A-Z0-9]{8,64}", token))


def hash_binding_id(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def generate_binding_id(length: int = 20) -> str:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
    return "".join(random.choices(alphabet, k=length))


def format_binding_id(token: str, group_size: int = 4) -> str:
    return "-".join(token[i : i + group_size] for i in range(0, len(token), group_size))


def ensure_parent_dir(path: str) -> None:
    Path(path).expanduser().resolve().parent.mkdir(parents=True, exist_ok=True)


def setup_logging(log_path: str, verbose: bool = False) -> None:
    ensure_parent_dir(log_path)
    level = logging.DEBUG if verbose else logging.INFO
    handlers: List[logging.Handler] = [
        logging.FileHandler(log_path, encoding="utf-8"),
        logging.StreamHandler(),
    ]
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=handlers,
    )


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
