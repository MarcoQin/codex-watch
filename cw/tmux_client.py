import dataclasses
import random
import string
import subprocess
import time
from typing import List

from .common import utc_ts


@dataclasses.dataclass(frozen=True)
class TmuxSendPolicy:
    strategy: str = "keys"
    enter_delay_ms: int = 100
    retry_enter_enabled: bool = True
    retry_enter_delay_ms: int = 250
    retry_enter_count: int = 1


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

    def session_exists(self, session_name: str) -> bool:
        if not session_name:
            return False
        try:
            self._run_tmux(["has-session", "-t", session_name], check=True)
            return True
        except subprocess.CalledProcessError:
            return False

    def kill_session(self, session_name: str) -> None:
        self._run_tmux(["kill-session", "-t", session_name], check=True)

    def pane_belongs_to_session(self, session_name: str, pane_id: str) -> bool:
        if not session_name or not pane_id:
            return False
        try:
            result = self._run_tmux(["list-panes", "-t", session_name, "-F", "#{pane_id}"], check=True)
        except subprocess.CalledProcessError:
            return False
        panes = {line.strip() for line in result.stdout.splitlines() if line.strip()}
        return pane_id in panes

    def capture_pane_text(self, pane_id: str, lines: int = 80) -> str:
        tail_lines = max(1, int(lines))
        result = self._run_tmux(["capture-pane", "-p", "-J", "-t", pane_id, "-S", f"-{tail_lines}", "-E", "-"])
        return result.stdout

    def send_special_key(self, pane_id: str, key: str) -> None:
        self._run_tmux(["send-keys", "-t", pane_id, key])

    def _send_via_paste(self, pane_id: str, text: str) -> None:
        self._run_tmux(["set-buffer", "--", text])
        self._run_tmux(["paste-buffer", "-t", pane_id])

    def _send_via_keys(self, pane_id: str, text: str) -> None:
        if not text:
            return
        self._run_tmux(["send-keys", "-l", "-t", pane_id, text])

    def _send_enter(self, pane_id: str) -> None:
        self._run_tmux(["send-keys", "-t", pane_id, "C-m"])

    @staticmethod
    def _sleep_ms(delay_ms: int) -> None:
        if delay_ms <= 0:
            return
        time.sleep(delay_ms / 1000.0)

    def send_text_with_policy(self, pane_id: str, text: str, policy: TmuxSendPolicy, enter: bool = True) -> None:
        strategy = (policy.strategy or "keys").strip().lower()
        if strategy not in ("keys", "paste", "auto"):
            strategy = "keys"

        chosen = strategy
        if chosen == "auto":
            many_lines = text.count("\n") > 30
            chosen = "paste" if (len(text) > 2000 or many_lines or "\n" in text) else "keys"
        elif chosen == "keys" and "\n" in text:
            # Multiline text is more reliably preserved via paste.
            chosen = "paste"

        if chosen == "keys":
            self._send_via_keys(pane_id, text)
        else:
            self._send_via_paste(pane_id, text)

        if not enter:
            return

        self._sleep_ms(max(0, int(policy.enter_delay_ms)))
        self._send_enter(pane_id)

        if policy.retry_enter_enabled:
            retries = 1 if int(policy.retry_enter_count) > 0 else 0
            for _ in range(retries):
                self._sleep_ms(max(0, int(policy.retry_enter_delay_ms)))
                self._send_enter(pane_id)

    def send_text(self, pane_id: str, text: str, enter: bool = True) -> None:
        # Backward-compatible behavior (paste + single enter, no delay/retry).
        self.send_text_with_policy(
            pane_id,
            text,
            TmuxSendPolicy(
                strategy="paste",
                enter_delay_ms=0,
                retry_enter_enabled=False,
                retry_enter_delay_ms=0,
                retry_enter_count=0,
            ),
            enter=enter,
        )


def build_tmux_session_name(alias: str) -> str:
    safe = "".join(ch if (ch.isalnum() or ch in "-_") else "-" for ch in alias)
    return f"cw_{safe}"


def build_launch_nonce(alias: str) -> str:
    safe = "".join(ch if (ch.isalnum() or ch in "-_") else "-" for ch in alias)[:24]
    rand = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
    return f"{safe}-{utc_ts()}-{rand}"
