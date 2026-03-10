import logging
import signal
import threading
import time
from typing import Any, List

from .bus import NotificationBus
from .channels import SlackService, TelegramService
from .config import Config
from .db import DB
from .monitor import SessionMonitor


class EventDispatcher(threading.Thread):
    def __init__(self, bus: NotificationBus, channels: List[Any], stop_event: threading.Event):
        super().__init__(daemon=True)
        self.bus = bus
        self.channels = channels
        self.stop_event = stop_event

    def run(self) -> None:
        logging.info("event dispatcher started channels=%s", [type(c).__name__ for c in self.channels])
        while not self.stop_event.is_set():
            drained = self._flush(limit=100)
            if drained == 0:
                self.stop_event.wait(0.2)
        self._flush(limit=500)
        logging.info("event dispatcher stopped")

    def _flush(self, limit: int) -> int:
        count = 0
        for _ in range(limit):
            msg = self.bus.poll()
            if msg is None:
                break
            count += 1
            for ch in self.channels:
                try:
                    ch.publish_event(msg)
                except Exception:
                    logging.exception("channel publish failed channel=%s", type(ch).__name__)
        return count


class Daemon:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.stop_event = threading.Event()
        self.db = DB(cfg.sqlite_path)
        self.bus = NotificationBus()
        self.monitor = SessionMonitor(cfg, self.db, self.bus, self.stop_event)
        self.channels: List[Any] = []

        if "telegram" in cfg.enabled_channels:
            self.channels.append(TelegramService(cfg, self.db, self.stop_event))
        if "slack" in cfg.enabled_channels:
            self.channels.append(SlackService(cfg, self.db, self.stop_event))

        self.dispatcher = EventDispatcher(self.bus, self.channels, self.stop_event)

    def run(self) -> int:
        if not self.channels:
            logging.warning("no channel enabled; running monitor only")

        def _handle_signal(signum: int, _frame: Any) -> None:
            logging.info("signal %s received, stopping", signum)
            self.stop_event.set()

        signal.signal(signal.SIGINT, _handle_signal)
        signal.signal(signal.SIGTERM, _handle_signal)

        self.monitor.start()
        self.dispatcher.start()
        for ch in self.channels:
            ch.start()

        while not self.stop_event.is_set():
            time.sleep(0.5)

        self.monitor.join(timeout=5)
        self.dispatcher.join(timeout=5)
        for ch in self.channels:
            ch.join(timeout=5)

        self.db.close()
        return 0
