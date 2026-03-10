import sys

from .cli import (
    cmd_auth_issue_bind_id,
    cmd_channels_status,
    cmd_daemon_restart,
    cmd_daemon_run,
    cmd_daemon_start,
    cmd_daemon_status,
    cmd_daemon_stop,
    cmd_init_config,
    cmd_run_session,
    cmd_sessions_attach,
    cmd_sessions_list,
    parse_args,
)
from .common import setup_logging
from .config import load_config
from .db import DB


def main(argv):
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

    if args.command == "channels":
        if args.channels_cmd == "status":
            return cmd_channels_status(cfg)
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
