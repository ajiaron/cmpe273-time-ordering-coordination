"""Runs the three Module 7 demonstrations.

Usage:
    python main.py                  # run all demos
    python main.py --save-evidence  # run demos + pytest, write output to evidence/
"""

from __future__ import annotations

import contextlib
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import TextIO

from order_processor import OrderEvent, OrderProcessor, Outcome
from worker import FencingTokenGenerator

EVIDENCE_DIR = Path(__file__).parent / "evidence"


class DemoChecker:
    """Records programmatic checks so each demo can report PASS/FAIL."""

    def __init__(self) -> None:
        self.failures: list[str] = []

    def check(self, condition: bool, description: str) -> None:
        print(f"  [{'OK' if condition else 'FAIL'}] {description}")
        if not condition:
            self.failures.append(description)

    def report(self, name: str) -> bool:
        passed = not self.failures
        print(f"\nRESULT: {name} {'PASS' if passed else 'FAIL'}")
        return passed


def heading(title: str) -> None:
    print("=" * 40)
    print(title)
    print("=" * 40)


def deliver(processor: OrderProcessor, event: OrderEvent, label: str) -> Outcome:
    print(f"\n> Incoming event ({label}): {event}")
    outcome = processor.process_event(event)
    print(f"  Outcome: {outcome.value}")
    processor.display_order(event.order_id)
    return outcome


def demo_out_of_order() -> bool:
    heading("DEMO 1: OUT-OF-ORDER EVENT DELIVERY")
    print("Delivery order: v2 PAID -> v1 CREATED -> v2 PAID (duplicate) -> v3 SHIPPED")
    processor = OrderProcessor()
    checker = DemoChecker()
    order = processor.get_or_create_order("ORDER-001")

    outcome = deliver(processor, OrderEvent("ORDER-001", 2, "PAID"), "step 1")
    checker.check(outcome == Outcome.BUFFERED, "v2 was buffered")
    checker.check(order.current_version == 0 and order.status == "NONE", "state still v0 NONE")
    checker.check(order.pending_versions() == [2], "pending versions are [2]")
    checker.check(order.missing_versions() == [1], "missing version 1 identified")

    outcome = deliver(processor, OrderEvent("ORDER-001", 1, "CREATED"), "step 2")
    checker.check(outcome == Outcome.APPLIED, "v1 was applied")
    checker.check(order.current_version == 2 and order.status == "PAID", "buffered v2 auto-applied -> v2 PAID")
    checker.check(order.pending_events == {}, "pending buffer is empty")

    outcome = deliver(processor, OrderEvent("ORDER-001", 2, "PAID"), "step 3, duplicate")
    checker.check(outcome == Outcome.IGNORED, "duplicate v2 was ignored")
    checker.check(order.current_version == 2 and order.status == "PAID", "state still v2 PAID")

    outcome = deliver(processor, OrderEvent("ORDER-001", 3, "SHIPPED"), "step 4")
    checker.check(outcome == Outcome.APPLIED, "v3 was applied")
    checker.check(order.current_version == 3 and order.status == "SHIPPED", "lifecycle complete -> v3 SHIPPED")

    return checker.report("DEMO 1")


def demo_missing_event() -> bool:
    heading("DEMO 2: MISSING EVENT DETECTION")
    print("Only v3 SHIPPED is delivered; v1 and v2 never arrive.")
    processor = OrderProcessor()
    checker = DemoChecker()
    order = processor.get_or_create_order("ORDER-002")

    outcome = deliver(processor, OrderEvent("ORDER-002", 3, "SHIPPED"), "only event")

    print("\nFinal order state:")
    print(f"  Current version:  {order.current_version}")
    print(f"  Status:           {order.status}")
    print(f"  Pending versions: {order.pending_versions()}")
    print(f"  Missing versions: {order.missing_versions()}")
    print()
    checker.check(outcome == Outcome.BUFFERED, "v3 was buffered, not applied")
    checker.check(order.current_version == 0, "current version remains 0")
    checker.check(order.status == "NONE", "status remains NONE (not SHIPPED)")
    checker.check(order.pending_versions() == [3], "v3 stored in pending events")
    checker.check(order.missing_versions() == [1, 2], "missing versions 1 and 2 reported")

    return checker.report("DEMO 2")


def demo_stale_worker() -> bool:
    heading("DEMO 3: STALE WORKER REJECTION")
    print("Event versions decide the ORDER of business events.")
    print("Fencing tokens decide WHICH WORKER may write to the order.")
    processor = OrderProcessor()
    tokens = FencingTokenGenerator()
    checker = DemoChecker()
    order = processor.get_or_create_order("ORDER-003")

    print("\n> Token service grants ownership of ORDER-003 to Worker-A")
    worker_a = tokens.grant("Worker-A")
    print(f"  Worker-A received fencing token {worker_a.fencing_token}")
    outcome = worker_a.update(processor, OrderEvent("ORDER-003", 1, "CREATED"))
    print(f"  Outcome: {outcome.value}")
    processor.display_order("ORDER-003")
    checker.check(outcome == Outcome.APPLIED, "Worker-A (token 1) created the order")

    print("\n> Worker-A stalls (e.g. GC pause); token service grants ownership to Worker-B")
    worker_b = tokens.grant("Worker-B")
    print(f"  Worker-B received fencing token {worker_b.fencing_token}")
    outcome = worker_b.update(processor, OrderEvent("ORDER-003", 2, "PAID"))
    print(f"  Outcome: {outcome.value}")
    processor.display_order("ORDER-003")
    checker.check(outcome == Outcome.APPLIED, "Worker-B (token 2) update accepted")
    checker.check(order.highest_fencing_token == 2, "order now records highest token 2")

    print("\n> Worker-A wakes up, still believes it owns the order, and tries to write")
    outcome = worker_a.update(processor, OrderEvent("ORDER-003", 3, "SHIPPED"))
    print(f"  Outcome: {outcome.value}")
    processor.display_order("ORDER-003")
    checker.check(outcome == Outcome.REJECTED, "stale Worker-A (token 1) rejected")
    checker.check(
        order.current_version == 2 and order.status == "PAID" and order.pending_events == {},
        "order state unchanged after rejection (v2 PAID, nothing pending)",
    )
    checker.check(order.highest_fencing_token == 2, "highest token still 2")

    print("\n> Worker-B (current owner) continues with the next update")
    outcome = worker_b.update(processor, OrderEvent("ORDER-003", 3, "SHIPPED"))
    print(f"  Outcome: {outcome.value}")
    processor.display_order("ORDER-003")
    checker.check(outcome == Outcome.APPLIED, "Worker-B (token 2) can keep writing")
    checker.check(order.current_version == 3 and order.status == "SHIPPED", "order reached v3 SHIPPED")

    return checker.report("DEMO 3")


DEMOS: list[tuple[str, Callable[[], bool]]] = [
    ("ordering_demo.txt", demo_out_of_order),
    ("missing_event_demo.txt", demo_missing_event),
    ("stale_worker_demo.txt", demo_stale_worker),
]


class Tee:
    """Writes to several streams at once (console + evidence file)."""

    def __init__(self, *streams: TextIO) -> None:
        self.streams = streams

    def write(self, text: str) -> int:
        for stream in self.streams:
            stream.write(text)
        return len(text)

    def flush(self) -> None:
        for stream in self.streams:
            stream.flush()


def run_demos(save_evidence: bool) -> bool:
    all_passed = True
    for filename, demo in DEMOS:
        if save_evidence:
            with open(EVIDENCE_DIR / filename, "w", encoding="utf-8") as f:
                with contextlib.redirect_stdout(Tee(sys.stdout, f)):
                    passed = demo()
        else:
            passed = demo()
        all_passed = all_passed and passed
        print()
    return all_passed


def save_test_results() -> bool:
    result = subprocess.run(
        [sys.executable, "-m", "pytest", "-v"],
        cwd=Path(__file__).parent,
        capture_output=True,
        text=True,
    )
    output = result.stdout + result.stderr
    (EVIDENCE_DIR / "test_results.txt").write_text(output, encoding="utf-8")
    print(output)
    return result.returncode == 0


def main() -> int:
    save_evidence = "--save-evidence" in sys.argv[1:]
    if save_evidence:
        EVIDENCE_DIR.mkdir(exist_ok=True)

    demos_passed = run_demos(save_evidence)
    tests_passed = save_test_results() if save_evidence else True

    print("ALL DEMOS PASSED" if demos_passed else "ONE OR MORE DEMOS FAILED")
    return 0 if demos_passed and tests_passed else 1


if __name__ == "__main__":
    sys.exit(main())
