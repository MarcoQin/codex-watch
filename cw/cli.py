import argparse
import os
import shlex
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

from .common import (
    APP_NAME,
    DEFAULT_CONFIG_PATH,
    ensure_parent_dir,
    format_binding_id,
    generate_binding_id,
    hash_binding_id,
    is_pid_alive,
    read_pid,
    remove_pid,
    utc_iso,
    utc_ts,
    write_pid,
)
from .config import Config, default_config_toml
from .daemon import Daemon
from .db import DB
from .tmux_client import TmuxController, build_launch_nonce, build_tmux_session_name


def cmd_init_config(args: argparse.Namespace) -> int:
    path = os.path.expanduser(args.config)
    p = Path(path)
    if p.exists() and not args.force:
        print(f"Config exists: {path}")
        return 1
    ensure_parent_dir(path)
    p.write_text(default_config_toml(), encoding="utf-8")
    print(f"Wrote config: {path}")
    return 0


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


def _managed_tmux_health(row, tmux: TmuxController) -> Tuple[bool, Optional[str]]:
    tmux_session = str(row["tmux_session"] or "").strip()
    pane = str(row["tmux_pane"] or "").strip()
    if not tmux_session:
        return False, "missing tmux session"
    if not pane:
        return False, "missing tmux pane"
    if not tmux.session_exists(tmux_session):
        return False, "tmux session not found"
    if not tmux.pane_belongs_to_session(tmux_session, pane):
        return False, "tmux pane mismatch"
    return True, None


def cmd_sessions_list(db: DB) -> int:
    tmux = TmuxController()
    managed = db.list_managed_sessions()
    print("Managed sessions:")
    if not managed:
        print("- none")
    else:
        for row in managed:
            healthy, orphan_reason = _managed_tmux_health(row, tmux)
            orphan_flag = "no" if healthy else "yes"
            reason = f" orphan_reason={orphan_reason}" if orphan_reason else ""
            print(
                f"- alias={row['alias']} status={row['status']} "
                f"session_id={row['codex_session_id'] or '-'} tmux={row['tmux_session']} pane={row['tmux_pane']} "
                f"orphan={orphan_flag} cwd={row['cwd']} nonce={row['launch_nonce'] or '-'}{reason}"
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


def cmd_sessions_rm(args: argparse.Namespace, db: DB) -> int:
    alias = args.name.strip()
    row = db.get_managed_by_alias(alias)
    if not row:
        print(f"remove failed: alias not found: {alias}", file=sys.stderr)
        return 1

    tmux = TmuxController()
    healthy, orphan_reason = _managed_tmux_health(row, tmux)
    if healthy and not args.force:
        print(
            f"refusing to remove active session alias={alias}; use --force to override",
            file=sys.stderr,
        )
        return 1

    ok = db.delete_managed_session(alias)
    if not ok:
        print(f"remove failed: alias not found: {alias}", file=sys.stderr)
        return 1

    reason = "forced" if healthy else (orphan_reason or "orphan")
    print(f"removed alias={alias} reason={reason}")
    return 0


def cmd_sessions_prune(args: argparse.Namespace, db: DB) -> int:
    managed = db.list_managed_sessions()
    tmux = TmuxController()

    remove_candidates: List[Tuple[str, str]] = []
    skipped_running: List[Tuple[str, str]] = []
    for row in managed:
        alias = str(row["alias"])
        healthy, orphan_reason = _managed_tmux_health(row, tmux)
        if healthy:
            continue
        reason = orphan_reason or "orphan"
        if str(row["status"]) == "running" and not args.force:
            skipped_running.append((alias, reason))
            continue
        remove_candidates.append((alias, reason))

    if not remove_candidates and not skipped_running:
        print("no orphan managed sessions found")
        return 0

    if remove_candidates:
        print("prune candidates:")
        for alias, reason in remove_candidates:
            print(f"- alias={alias} reason={reason}")
    if skipped_running:
        print("skipped running orphans (use --force):")
        for alias, reason in skipped_running:
            print(f"- alias={alias} reason={reason}")

    if args.dry_run:
        print("dry-run only; no records removed")
        return 0

    removed = 0
    for alias, _reason in remove_candidates:
        if db.delete_managed_session(alias):
            removed += 1
    print(f"removed {removed} managed sessions")
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
    entry = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "codex_watch.py"))
    cmd = [sys.executable, entry, "--config", args.config, "daemon", "run"]
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


def cmd_channels_status(cfg: Config) -> int:
    print("Enabled channels:")
    if not cfg.enabled_channels:
        print("- none")
    for name in cfg.enabled_channels:
        print(f"- {name}")

    print("\nTelegram:")
    print(f"- configured: {'yes' if bool(cfg.telegram_bot_token) else 'no'}")
    print(f"- enabled: {'yes' if 'telegram' in cfg.enabled_channels else 'no'}")

    print("\nSlack:")
    print(f"- configured token: {'yes' if bool(cfg.slack_bot_token) else 'no'}")
    print(f"- default_channel: {cfg.slack_default_channel or '-'}")
    print(f"- channel_map entries: {len(cfg.slack_channel_map)}")
    print(f"- enabled: {'yes' if 'slack' in cfg.enabled_channels else 'no'}")

    print("\nTmux:")
    print(f"- send_strategy: {cfg.tmux_send_strategy}")
    print(f"- enter_delay_ms: {cfg.tmux_enter_delay_ms}")
    print(f"- retry_enter_enabled: {'yes' if cfg.tmux_retry_enter_enabled else 'no'}")
    print(f"- retry_enter_delay_ms: {cfg.tmux_retry_enter_delay_ms}")
    print(f"- retry_enter_count: {cfg.tmux_retry_enter_count}")
    print(f"- view_lines: {cfg.tmux_view_lines}")
    return 0


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
    rm = sess_sub.add_parser("rm")
    rm.add_argument("name")
    rm.add_argument("--force", action="store_true")
    prune = sess_sub.add_parser("prune")
    prune.add_argument("--dry-run", action="store_true")
    prune.add_argument("--force", action="store_true")

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

    channels = sub.add_parser("channels")
    channels_sub = channels.add_subparsers(dest="channels_cmd", required=True)
    channels_sub.add_parser("status")

    return parser.parse_args(argv)
