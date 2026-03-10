import random
import string
import subprocess
from typing import List

from .common import utc_ts


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


def build_tmux_session_name(alias: str) -> str:
    safe = "".join(ch if (ch.isalnum() or ch in "-_") else "-" for ch in alias)
    return f"cw_{safe}"


def build_launch_nonce(alias: str) -> str:
    safe = "".join(ch if (ch.isalnum() or ch in "-_") else "-" for ch in alias)[:24]
    rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{safe}-{utc_ts()}-{rand}"
