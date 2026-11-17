"""Workers and fencing tokens.

Event versions decide the ORDER in which business events may be applied.
Fencing tokens decide WHICH WORKER is currently allowed to write.

A worker can believe it still owns an order after it has lost ownership
(a long GC pause, a network partition, an expired lease). When it wakes up it
may try to write stale data. To stop this, a central service hands out
monotonically increasing tokens, and the order rejects any write carrying a
token lower than the highest one it has already accepted.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from order_processor import OrderEvent, OrderProcessor, Outcome


class FencingTokenGenerator:
    """Central, monotonically increasing token issuer (like a lock service).

    Workers never choose their own tokens; they must ask this generator, so
    every new owner is guaranteed a strictly larger token than the previous.
    """

    def __init__(self) -> None:
        self._last_token = 0
        self._lock = threading.Lock()

    def grant(self, worker_id: str) -> Worker:
        with self._lock:
            self._last_token += 1
            return Worker(worker_id=worker_id, fencing_token=self._last_token)


@dataclass(frozen=True)
class Worker:
    worker_id: str
    fencing_token: int

    def update(self, processor: OrderProcessor, event: OrderEvent) -> Outcome:
        """Send a write through the processor's centralized, fenced update path."""
        if processor.verbose:
            print(f"> {self.worker_id} (token {self.fencing_token}) attempts update: {event}")
        return processor.process_worker_update(self.worker_id, self.fencing_token, event)
