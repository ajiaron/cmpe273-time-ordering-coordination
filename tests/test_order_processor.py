"""Tests for versioned event processing and fencing-token validation.

Every test builds its own OrderProcessor through the fixture, so no state is
shared between tests and they can run in any order.
"""

import threading

import pytest

from order_processor import OrderEvent, OrderProcessor, Outcome
from worker import FencingTokenGenerator


@pytest.fixture
def processor() -> OrderProcessor:
    return OrderProcessor(verbose=False)


def created(order_id: str = "ORDER-1") -> OrderEvent:
    return OrderEvent(order_id, 1, "CREATED")


def paid(order_id: str = "ORDER-1") -> OrderEvent:
    return OrderEvent(order_id, 2, "PAID")


def shipped(order_id: str = "ORDER-1") -> OrderEvent:
    return OrderEvent(order_id, 3, "SHIPPED")


def test_new_order_starts_empty(processor):
    order = processor.get_or_create_order("ORDER-1")
    assert (order.current_version, order.status) == (0, "NONE")
    assert order.pending_events == {}
    assert order.highest_fencing_token == 0


# 1
def test_version_1_is_applied(processor):
    assert processor.process_event(created()) == Outcome.APPLIED
    order = processor.orders["ORDER-1"]
    assert (order.current_version, order.status) == (1, "CREATED")


# 2
def test_version_2_before_version_1_is_buffered(processor):
    assert processor.process_event(paid()) == Outcome.BUFFERED
    order = processor.orders["ORDER-1"]
    assert (order.current_version, order.status) == (0, "NONE")
    assert order.pending_versions() == [2]


# 3
def test_late_version_1_applies_version_1_then_buffered_version_2(processor):
    processor.process_event(paid())
    applied_statuses: list[str] = []
    original_apply = processor.apply_event

    def recording_apply(order, event):
        applied_statuses.append(event.status)
        original_apply(order, event)

    processor.apply_event = recording_apply
    assert processor.process_event(created()) == Outcome.APPLIED

    order = processor.orders["ORDER-1"]
    assert applied_statuses == ["CREATED", "PAID"]
    assert (order.current_version, order.status) == (2, "PAID")
    assert order.pending_events == {}


# 4
def test_duplicate_version_2_is_ignored(processor):
    processor.process_event(created())
    processor.process_event(paid())
    assert processor.process_event(paid()) == Outcome.IGNORED
    order = processor.orders["ORDER-1"]
    assert (order.current_version, order.status) == (2, "PAID")
    assert order.pending_events == {}


def test_duplicate_pending_event_is_not_stored_twice(processor):
    processor.process_event(shipped())
    assert processor.process_event(shipped()) == Outcome.IGNORED
    order = processor.orders["ORDER-1"]
    assert order.pending_events == {3: shipped()}


# 5
def test_version_3_without_earlier_versions_stays_pending(processor):
    assert processor.process_event(shipped()) == Outcome.BUFFERED
    order = processor.orders["ORDER-1"]
    assert (order.current_version, order.status) == (0, "NONE")
    assert order.pending_versions() == [3]


# 6
def test_missing_versions_are_identified(processor):
    processor.process_event(shipped())
    assert processor.orders["ORDER-1"].missing_versions() == [1, 2]

    processor.process_event(created())
    assert processor.orders["ORDER-1"].missing_versions() == [2]


# 7
def test_buffer_drains_when_missing_versions_arrive(processor):
    processor.process_event(shipped())
    processor.process_event(paid())
    order = processor.orders["ORDER-1"]
    assert order.pending_versions() == [2, 3]
    assert order.current_version == 0

    processor.process_event(created())
    assert (order.current_version, order.status) == (3, "SHIPPED")
    assert order.pending_events == {}
    assert order.missing_versions() == []


# 8
@pytest.mark.parametrize(
    "event",
    [
        OrderEvent("ORDER-1", 1, "PAID"),
        OrderEvent("ORDER-1", 2, "SHIPPED"),
        OrderEvent("ORDER-1", 3, "CREATED"),
        OrderEvent("ORDER-1", 4, "DELIVERED"),
        OrderEvent("ORDER-1", 0, "NONE"),
    ],
    ids=["v1-PAID", "v2-SHIPPED", "v3-CREATED", "unknown-v4", "v0"],
)
def test_invalid_status_version_combination_is_rejected(processor, event):
    assert processor.process_event(event) == Outcome.REJECTED
    order = processor.orders["ORDER-1"]
    assert (order.current_version, order.status) == (0, "NONE")
    assert order.pending_events == {}


def test_invalid_event_does_not_change_existing_state_or_buffer(processor):
    processor.process_event(created())
    processor.process_event(shipped())
    assert processor.process_event(OrderEvent("ORDER-1", 2, "CREATED")) == Outcome.REJECTED
    order = processor.orders["ORDER-1"]
    assert (order.current_version, order.status) == (1, "CREATED")
    assert order.pending_events == {3: shipped()}


# 9
def test_conflicting_pending_event_does_not_overwrite_buffer(processor):
    processor.process_event(paid())
    conflicting = OrderEvent("ORDER-1", 2, "SHIPPED")
    assert processor.process_event(conflicting) == Outcome.REJECTED
    order = processor.orders["ORDER-1"]
    assert order.pending_events[2] == paid()

    processor.process_event(created())
    assert (order.current_version, order.status) == (2, "PAID")


# 10
def test_worker_with_newer_token_is_accepted(processor):
    tokens = FencingTokenGenerator()
    worker_a = tokens.grant("A")
    worker_b = tokens.grant("B")
    assert worker_a.update(processor, created()) == Outcome.APPLIED
    assert worker_b.update(processor, paid()) == Outcome.APPLIED
    order = processor.orders["ORDER-1"]
    assert order.highest_fencing_token == worker_b.fencing_token == 2
    assert (order.current_version, order.status) == (2, "PAID")


# 11
def test_worker_with_older_token_is_rejected(processor):
    tokens = FencingTokenGenerator()
    worker_a = tokens.grant("A")
    worker_b = tokens.grant("B")
    worker_b.update(processor, created())
    assert worker_a.update(processor, paid()) == Outcome.REJECTED


# 12
def test_rejected_stale_worker_does_not_modify_order(processor):
    tokens = FencingTokenGenerator()
    worker_a = tokens.grant("A")
    worker_a.update(processor, created())
    worker_b = tokens.grant("B")
    worker_b.update(processor, paid())
    order = processor.orders["ORDER-1"]
    before = (order.current_version, order.status, dict(order.pending_events), order.highest_fencing_token)

    # v3 would be a valid next event, but the stale token must block it.
    assert worker_a.update(processor, shipped()) == Outcome.REJECTED
    assert (order.current_version, order.status, dict(order.pending_events), order.highest_fencing_token) == before
    assert before == (2, "PAID", {}, 2)


# 13
def test_current_worker_can_repeat_valid_operations(processor):
    worker = FencingTokenGenerator().grant("A")
    assert worker.update(processor, created()) == Outcome.APPLIED
    assert worker.update(processor, paid()) == Outcome.APPLIED
    assert worker.update(processor, paid()) == Outcome.IGNORED  # idempotent retry
    assert worker.update(processor, shipped()) == Outcome.APPLIED
    order = processor.orders["ORDER-1"]
    assert (order.current_version, order.status) == (3, "SHIPPED")
    assert order.highest_fencing_token == 1


def test_worker_updates_still_respect_version_order(processor):
    worker = FencingTokenGenerator().grant("A")
    assert worker.update(processor, shipped()) == Outcome.BUFFERED
    order = processor.orders["ORDER-1"]
    assert (order.current_version, order.status) == (0, "NONE")
    assert order.missing_versions() == [1, 2]


def test_token_generator_is_monotonic_under_concurrency():
    tokens = FencingTokenGenerator()
    issued: list[int] = []
    lock = threading.Lock()

    def grab() -> None:
        token = tokens.grant("w").fencing_token
        with lock:
            issued.append(token)

    threads = [threading.Thread(target=grab) for _ in range(50)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(issued) == list(range(1, 51))


# 14
def test_different_orders_have_independent_state(processor):
    processor.process_event(created("ORDER-A"))
    processor.process_event(paid("ORDER-A"))
    processor.process_event(shipped("ORDER-B"))

    a = processor.orders["ORDER-A"]
    b = processor.orders["ORDER-B"]
    assert (a.current_version, a.status, a.pending_events) == (2, "PAID", {})
    assert (b.current_version, b.status) == (0, "NONE")
    assert b.pending_versions() == [3]


def test_fencing_tokens_are_tracked_per_order(processor):
    tokens = FencingTokenGenerator()
    worker_a = tokens.grant("A")
    worker_b = tokens.grant("B")
    worker_b.update(processor, created("ORDER-A"))
    assert worker_a.update(processor, created("ORDER-B")) == Outcome.APPLIED
    assert processor.orders["ORDER-A"].highest_fencing_token == 2
    assert processor.orders["ORDER-B"].highest_fencing_token == 1


# 15
def test_full_lifecycle_created_paid_shipped(processor):
    order = processor.get_or_create_order("ORDER-1")
    statuses = []
    for event in (created(), paid(), shipped()):
        assert processor.process_event(event) == Outcome.APPLIED
        statuses.append(order.status)
    assert statuses == ["CREATED", "PAID", "SHIPPED"]
    assert order.current_version == 3
    assert order.pending_events == {}


def test_apply_event_refuses_to_skip_a_version(processor):
    order = processor.get_or_create_order("ORDER-1")
    with pytest.raises(ValueError):
        processor.apply_event(order, paid())
    assert order.current_version == 0
