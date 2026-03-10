import dataclasses
import os
from typing import Any, Dict, List

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:
    import tomli as tomllib  # type: ignore[no-redef]

from .common import (
    DEFAULT_LOG_PATH,
    DEFAULT_PID_PATH,
    DEFAULT_SESSIONS_ROOT,
    DEFAULT_SQLITE_PATH,
)


@dataclasses.dataclass
class Config:
    poll_interval_sec: int
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

    tmux_send_strategy: str
    tmux_enter_delay_ms: int
    tmux_retry_enter_enabled: bool
    tmux_retry_enter_delay_ms: int
    tmux_retry_enter_count: int
    tmux_view_lines: int

    enabled_channels: List[str]

    telegram_bot_token: str
    telegram_proxy_url: str
    telegram_connect_timeout_sec: int
    telegram_read_timeout_sec: int
    telegram_api_base: str
    telegram_markdown_render_enabled: bool

    slack_enabled: bool
    slack_bot_token: str
    slack_app_token: str
    slack_default_channel: str
    slack_channel_map: Dict[str, str]
    slack_api_base: str
    slack_connect_timeout_sec: int
    slack_read_timeout_sec: int



def deep_get(d: Dict[str, Any], path: List[str], default: Any) -> Any:
    cur: Any = d
    for key in path:
        if not isinstance(cur, dict) or key not in cur:
            return default
        cur = cur[key]
    return cur


def _normalize_channel_map(raw_map: Any) -> Dict[str, str]:
    if not isinstance(raw_map, dict):
        return {}
    out: Dict[str, str] = {}
    for k, v in raw_map.items():
        key = str(k).strip()
        val = str(v).strip()
        if key and val:
            out[key] = val
    return out


def load_config(path: str) -> Config:
    raw: Dict[str, Any] = {}
    p = os.path.expanduser(path)
    if os.path.exists(p):
        with open(p, "rb") as f:
            raw = tomllib.load(f)

    poll_interval_sec = int(deep_get(raw, ["telegram", "poll_interval_sec"], 2))

    telegram_bot_token = str(deep_get(raw, ["telegram", "bot_token"], "")).strip()
    telegram_proxy_url = str(deep_get(raw, ["telegram", "proxy_url"], "")).strip()
    telegram_connect_timeout_sec = int(deep_get(raw, ["telegram", "connect_timeout_sec"], 10))
    telegram_read_timeout_sec = int(deep_get(raw, ["telegram", "read_timeout_sec"], 30))
    telegram_api_base = str(deep_get(raw, ["telegram", "api_base"], "https://api.telegram.org")).strip()
    telegram_markdown_render_enabled = bool(deep_get(raw, ["telegram", "markdown_render_enabled"], True))

    slack_enabled = bool(deep_get(raw, ["slack", "enabled"], False))
    slack_bot_token = str(deep_get(raw, ["slack", "bot_token"], "")).strip()
    slack_app_token = str(deep_get(raw, ["slack", "app_token"], "")).strip()
    slack_default_channel = str(deep_get(raw, ["slack", "default_channel"], "")).strip()
    slack_channel_map = _normalize_channel_map(deep_get(raw, ["slack", "channel_map"], {}))
    slack_api_base = str(deep_get(raw, ["slack", "api_base"], "https://slack.com/api")).strip()
    slack_connect_timeout_sec = int(deep_get(raw, ["slack", "connect_timeout_sec"], 10))
    slack_read_timeout_sec = int(deep_get(raw, ["slack", "read_timeout_sec"], 30))

    configured_channels = deep_get(raw, ["channels", "enabled"], None)
    enabled_channels: List[str]
    if isinstance(configured_channels, list) and configured_channels:
        enabled_channels = [str(v).strip().lower() for v in configured_channels if str(v).strip()]
    else:
        enabled_channels = []
        if telegram_bot_token:
            enabled_channels.append("telegram")
        if slack_enabled or slack_bot_token:
            enabled_channels.append("slack")

    # Telegram remains usable when listed in channels and token exists.
    if "telegram" in enabled_channels and not telegram_bot_token:
        enabled_channels = [c for c in enabled_channels if c != "telegram"]

    # Slack requires token + target channel to actually send notifications.
    if "slack" in enabled_channels:
        if not slack_bot_token or (not slack_default_channel and not slack_channel_map):
            enabled_channels = [c for c in enabled_channels if c != "slack"]

    binding_id_ttl_sec = int(deep_get(raw, ["auth", "binding_id_ttl_sec"], 600))
    sessions_root = os.path.expanduser(str(deep_get(raw, ["monitor", "sessions_root"], DEFAULT_SESSIONS_ROOT)))
    scan_interval_sec = int(deep_get(raw, ["monitor", "scan_interval_sec"], 2))
    backfill_lines = int(deep_get(raw, ["monitor", "backfill_lines"], 3000))
    enabled_triggers = list(
        deep_get(
            raw,
            ["notify", "enabled_triggers"],
            ["task_complete", "request_user_input", "proposed_plan_ready"],
        )
    )
    sqlite_path = os.path.expanduser(str(deep_get(raw, ["runtime", "sqlite_path"], DEFAULT_SQLITE_PATH)))
    log_path = os.path.expanduser(str(deep_get(raw, ["runtime", "log_path"], DEFAULT_LOG_PATH)))
    pid_path = os.path.expanduser(str(deep_get(raw, ["runtime", "pid_path"], DEFAULT_PID_PATH)))

    mode_plan_template = str(deep_get(raw, ["commands", "mode_plan_template"], "/plan"))
    mode_default_template = str(deep_get(raw, ["commands", "mode_default_template"], "/default"))
    approve_plan_template = str(deep_get(raw, ["commands", "approve_plan_template"], "Implement the plan."))
    reject_plan_template = str(
        deep_get(raw, ["commands", "reject_plan_template"], "Revise the plan with more detail, then resend it.")
    )
    tmux_send_strategy = str(deep_get(raw, ["tmux", "send_strategy"], "keys")).strip().lower()
    if tmux_send_strategy not in ("keys", "paste", "auto"):
        tmux_send_strategy = "keys"
    tmux_enter_delay_ms = int(deep_get(raw, ["tmux", "enter_delay_ms"], 100))
    tmux_retry_enter_enabled = bool(deep_get(raw, ["tmux", "retry_enter_enabled"], True))
    tmux_retry_enter_delay_ms = int(deep_get(raw, ["tmux", "retry_enter_delay_ms"], 250))
    tmux_retry_enter_count = int(deep_get(raw, ["tmux", "retry_enter_count"], 1))
    tmux_retry_enter_count = 1 if tmux_retry_enter_count > 0 else 0
    tmux_view_lines = int(deep_get(raw, ["tmux", "view_lines"], 80))

    return Config(
        poll_interval_sec=max(1, poll_interval_sec),
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
        tmux_send_strategy=tmux_send_strategy,
        tmux_enter_delay_ms=max(0, tmux_enter_delay_ms),
        tmux_retry_enter_enabled=tmux_retry_enter_enabled,
        tmux_retry_enter_delay_ms=max(0, tmux_retry_enter_delay_ms),
        tmux_retry_enter_count=tmux_retry_enter_count,
        tmux_view_lines=max(20, min(400, tmux_view_lines)),
        enabled_channels=enabled_channels,
        telegram_bot_token=telegram_bot_token,
        telegram_proxy_url=telegram_proxy_url,
        telegram_connect_timeout_sec=max(1, telegram_connect_timeout_sec),
        telegram_read_timeout_sec=max(1, telegram_read_timeout_sec),
        telegram_api_base=telegram_api_base or "https://api.telegram.org",
        telegram_markdown_render_enabled=telegram_markdown_render_enabled,
        slack_enabled=slack_enabled,
        slack_bot_token=slack_bot_token,
        slack_app_token=slack_app_token,
        slack_default_channel=slack_default_channel,
        slack_channel_map=slack_channel_map,
        slack_api_base=slack_api_base or "https://slack.com/api",
        slack_connect_timeout_sec=max(1, slack_connect_timeout_sec),
        slack_read_timeout_sec=max(1, slack_read_timeout_sec),
    )


def default_config_toml() -> str:
    return """[channels]
enabled = ["telegram"]

[telegram]
bot_token = ""
poll_interval_sec = 2
proxy_url = ""
connect_timeout_sec = 10
read_timeout_sec = 30
api_base = "https://api.telegram.org"
markdown_render_enabled = true

[slack]
enabled = false
bot_token = ""
app_token = ""
default_channel = ""
channel_map = {}
api_base = "https://slack.com/api"
connect_timeout_sec = 10
read_timeout_sec = 30

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

[tmux]
send_strategy = "keys" # keys | paste | auto
enter_delay_ms = 100
retry_enter_enabled = true
retry_enter_delay_ms = 250
retry_enter_count = 1
view_lines = 80
"""
