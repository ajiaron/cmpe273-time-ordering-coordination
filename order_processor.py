"""Versioned order-event processing.

In a distributed system, events can be delivered out of order, late, or more
than once (e.g. a message broker with at-least-once delivery). Instead of
trusting arrival order or wall-clock timestamps, every event carries a version
number. An order only ever moves forward one version at a time:

    v1 CREATED -> v2 PAID -> v3 SHIPPED

Events that arrive "too early" are buffered until the gap is filled, and events
that were already applied are ignored, which makes processing idempotent.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from enum import Enum

# The only legal status for each version. Because each version maps to exactly
# one status, applying versions strictly in sequence also guarantees valid
# status transitions (CREATED -> PAID -> SHIPPED).
VERSION_TO_STATUS: dict[int, str] = {1: "CREATED", 2: "PAID", 3: "SHIPPED"}


class Outcome(str, Enum):
    """What the processor did with an incoming event or worker update."""

    APPLIED = "APPLIED"
    BUFFERED = "BUFFERED"
    IGNORED = "IGNORED"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class OrderEvent:
    order_id: str
    version: int
    status: str

    def __str__(self) -> str:
        return f"{self.order_id} v{self.version} {self.status}"


@dataclass
class Order:
    order_id: str
    current_version: int = 0
    status: str = "NONE"
    pending_events: dict[int, OrderEvent] = field(default_factory=dict)
    highest_fencing_token: int = 0

    def pending_versions(self) -> list[int]:
        return sorted(self.pending_events)

    def missing_versions(self) -> list[int]:
        """Versions that must arrive before the buffered events can be applied."""
        if not self.pending_events:
            return []
        highest_pending = max(self.pending_events)
        return [
            v
            for v in range(self.current_version + 1, highest_pending)
            if v not in self.pending_events
        ]


def _fmt(versions: list[int]) -> str:
    return str(versions) if versions else "[]"


class OrderProcessor:
    """Keeps all orders in memory and applies events according to their version.

    A single lock protects all order state, so the "check" (version or fencing
    token comparison) and the "update" happen atomically. Without it, two
    threads could both pass a check and then both write.
    """

    def __init__(self, verbose: bool = True) -> None:
        self.orders: dict[str, Order] = {}
        self.verbose = verbose
        self._lock = threading.Lock()

    def _log(self, message: str) -> None:
        if self.verbose:
            print(message)

    def get_or_create_order(self, order_id: str) -> Order:
        if order_id not in self.orders:
            self.orders[order_id] = Order(order_id=order_id)
        return self.orders[order_id]

    # ------------------------------------------------------------------
    # Event-delivery pipeline (versions)
    # ------------------------------------------------------------------
    def process_event(self, event: OrderEvent) -> Outcome:
        with self._lock:
            return self._process_event_locked(event)

    def _process_event_locked(self, event: OrderEvent) -> Outcome:
        order = self.get_or_create_order(event.order_id)
        expected_version = order.current_version + 1

        if event.version not in VERSION_TO_STATUS:
            self._log(f"  [REJECTED] {event}: unknown version {event.version}")
            return Outcome.REJECTED

        # Rule C: already applied -> duplicate or outdated delivery.
        if event.version <= order.current_version:
            self._log(
                f"  [IGNORED]  {event}: version {event.version} already processed "
                f"(current version is {order.current_version})"
            )
            return Outcome.IGNORED

        # Rule D: this version is already waiting in the buffer.
        if event.version in order.pending_events:
            buffered = order.pending_events[event.version]
            if buffered.status == event.status:
                self._log(f"  [IGNORED]  {event}: version {event.version} is already pending")
                return Outcome.IGNORED
            self._log(
                f"  [REJECTED] {event}: CONFLICT with buffered v{event.version} "
                f"{buffered.status}; keeping the buffered event"
            )
            return Outcome.REJECTED

        # Rule E: the status must match the version.
        error = self.validate_event(event)
        if error:
            self._log(f"  [REJECTED] {event}: {error}")
            return Outcome.REJECTED

        # Rule A: exactly the next version -> apply, then drain the buffer.
        if event.version == expected_version:
            self.apply_event(order, event)
            self._log(f"  [APPLIED]  {event}: was the expected next version")
            self.process_pending_events(order)
            return Outcome.APPLIED

        # Rule B: a future version -> buffer it and report the gap.
        order.pending_events[event.version] = event
        self._log(
            f"  [BUFFERED] {event}: expected v{expected_version}; "
            f"missing versions {_fmt(order.missing_versions())}"
        )
        return Outcome.BUFFERED

    @staticmethod
    def validate_event(event: OrderEvent) -> str | None:
        """Return an error message if the status is invalid for the version."""
        required = VERSION_TO_STATUS.get(event.version)
        if required is None:
            return f"unknown version {event.version}"
        if event.status != required:
            return f"invalid status for version {event.version} (must be {required})"
        return None

    def apply_event(self, order: Order, event: OrderEvent) -> None:
        # Guard against ever skipping a version, even if called directly.
        if event.version != order.current_version + 1:
            raise ValueError(
                f"cannot apply v{event.version} to {order.order_id} at v{order.current_version}"
            )
        order.current_version = event.version
        order.status = event.status

    def process_pending_events(self, order: Order) -> list[int]:
        """Apply buffered events for as long as the next version is available."""
        applied: list[int] = []
        while order.current_version + 1 in order.pending_events:
            event = order.pending_events.pop(order.current_version + 1)
            self.apply_event(order, event)
            applied.append(event.version)
            self._log(f"  [APPLIED]  {event}: drained from pending buffer")
        return applied

    # ------------------------------------------------------------------
    # Worker update path (fencing tokens)
    # ------------------------------------------------------------------
    def process_worker_update(
        self, worker_id: str, fencing_token: int, event: OrderEvent
    ) -> Outcome:
        """Centralized entry point for worker writes.

        The order remembers the highest fencing token it has accepted. A worker
        presenting a lower token is a stale owner (e.g. it was paused, lost its
        lease, and a newer worker took over), so its write is rejected before
        any state changes. The token check and the update run under one lock.
        """
        with self._lock:
            order = self.get_or_create_order(event.order_id)
            if fencing_token < order.highest_fencing_token:
                self._log(
                    f"  [REJECTED] {worker_id} (token {fencing_token}) -> {event}: "
                    f"stale fencing token, order already accepted token "
                    f"{order.highest_fencing_token}"
                )
                return Outcome.REJECTED
            if fencing_token > order.highest_fencing_token:
                self._log(
                    f"  [TOKEN]    {worker_id} token {fencing_token} supersedes "
                    f"token {order.highest_fencing_token}"
                )
                order.highest_fencing_token = fencing_token
            # The worker is authorized; the normal version rules still apply.
            return self._process_event_locked(event)

    # ------------------------------------------------------------------
    # Display
    # ------------------------------------------------------------------
    def display_order(self, order_id: str) -> str:
        with self._lock:
            order = self.get_or_create_order(order_id)
            line = (
                f"  State: order={order.order_id} version={order.current_version} "
                f"status={order.status} pending={_fmt(order.pending_versions())} "
                f"missing={_fmt(order.missing_versions())} "
                f"highest_fencing_token={order.highest_fencing_token}"
            )
        self._log(line)
        return line
