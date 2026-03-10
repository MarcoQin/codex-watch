import queue
from typing import Any, Dict, Optional


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
