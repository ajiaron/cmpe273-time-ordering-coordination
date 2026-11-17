# CMPE 273 Module 7: Time, Ordering & Coordination

## Project Overview

In a distributed system, messages don't always arrive the way they were sent. They can show up out of order, arrive late, be delivered twice (at-least-once delivery), or come from a worker that no longer owns the data it's trying to change.

This project is a small in-memory order-processing simulation that handles each of those cases. Every order moves through versioned states:

```text
v1 CREATED -> v2 PAID -> v3 SHIPPED
```

The simulation demonstrates:

1. Applying out-of-order events in the correct version sequence.
2. Detecting missing events and buffering later events until the gap is filled.
3. Ignoring duplicate or already-applied events.
4. Rejecting updates from stale workers using fencing tokens.

## Technologies Used

- Python 3.11+ (standard library only: `dataclasses`, `threading`, `enum`)
- pytest (the only external dependency, used for tests)

No database, web server, Docker, or message broker is needed. All state is kept in memory.

## Project Structure

```text
cmpe273-module7/
├── README.md
├── requirements.txt          # pytest
├── .gitignore
├── main.py                   # Runs the 3 demos; --save-evidence regenerates evidence/
├── order_processor.py        # OrderEvent, Order, OrderProcessor (versioning + fencing check)
├── worker.py                 # Worker and the central FencingTokenGenerator
├── tests/
│   ├── __init__.py
│   └── test_order_processor.py
└── evidence/                 # Real output captured from running the code
    ├── ordering_demo.txt
    ├── missing_event_demo.txt
    ├── stale_worker_demo.txt
    └── test_results.txt
```

- **`order_processor.py`**: the core logic. `OrderProcessor.process_event()` applies, buffers, ignores, or rejects events by version. `process_worker_update()` is the single, centralized path for worker writes, and it checks fencing tokens first.
- **`worker.py`**: `FencingTokenGenerator` hands out increasing tokens, and `Worker` sends its updates through the processor.
- **`main.py`**: runs the three demos and prints PASS/FAIL based on programmatic checks. It exits with a nonzero status if any check fails.

## Installation and Running

```bash
git clone <YOUR-GITHUB-REPOSITORY-URL>   # placeholder: replace with this repo's URL
cd cmpe273-module7
python -m pip install -r requirements.txt
python main.py
```

To regenerate every file in `evidence/` from a fresh run (the three demos plus `pytest -v`):

```bash
python main.py --save-evidence
```

## Running Automated Tests

```bash
python -m pytest -v
```

## Demo 1: Event Ordering and Duplicate Handling

Events for `ORDER-001` are delivered as **v2 → v1 → v2 again**, followed by a normal v3.

1. **v2 PAID** arrives first. The order is still at v0, so v2 can't be applied yet. It is buffered, and v1 is reported as missing.
2. **v1 CREATED** arrives. It is the expected next version, so it is applied, and the buffered v2 is then drained from the buffer and applied automatically.
3. **v2 PAID** arrives again. Version 2 has already been applied, so the duplicate is ignored.
4. **v3 SHIPPED** arrives normally and completes the lifecycle.

Actual output (from `evidence/ordering_demo.txt`):

```text
> Incoming event (step 1): ORDER-001 v2 PAID
  [BUFFERED] ORDER-001 v2 PAID: expected v1; missing versions [1]
  Outcome: BUFFERED
  State: order=ORDER-001 version=0 status=NONE pending=[2] missing=[1] highest_fencing_token=0
  ...
> Incoming event (step 2): ORDER-001 v1 CREATED
  [APPLIED]  ORDER-001 v1 CREATED: was the expected next version
  [APPLIED]  ORDER-001 v2 PAID: drained from pending buffer
  Outcome: APPLIED
  State: order=ORDER-001 version=2 status=PAID pending=[] missing=[] highest_fencing_token=0
  ...
> Incoming event (step 3, duplicate): ORDER-001 v2 PAID
  [IGNORED]  ORDER-001 v2 PAID: version 2 already processed (current version is 2)
  Outcome: IGNORED
  State: order=ORDER-001 version=2 status=PAID pending=[] missing=[] highest_fencing_token=0
  ...
> Incoming event (step 4): ORDER-001 v3 SHIPPED
  [APPLIED]  ORDER-001 v3 SHIPPED: was the expected next version
  Outcome: APPLIED
  State: order=ORDER-001 version=3 status=SHIPPED pending=[] missing=[] highest_fencing_token=0
  ...
RESULT: DEMO 1 PASS
```

**Final state:** after the required three events, the order is at version 2 with status `PAID` and an empty buffer. The duplicate didn't change anything. After v3, the order is at version 3 with status `SHIPPED`.

## Demo 2: Missing Event

For `ORDER-002`, only **v3 SHIPPED** is delivered. Applying it would skip CREATED and PAID, so the processor stores it in `pending_events` and reports that versions 1 and 2 are missing. The order stays at version 0 with status `NONE`, and it does not jump to `SHIPPED`.

Actual output (from `evidence/missing_event_demo.txt`):

```text
> Incoming event (only event): ORDER-002 v3 SHIPPED
  [BUFFERED] ORDER-002 v3 SHIPPED: expected v1; missing versions [1, 2]
  Outcome: BUFFERED
  State: order=ORDER-002 version=0 status=NONE pending=[3] missing=[1, 2] highest_fencing_token=0

Final order state:
  Current version:  0
  Status:           NONE
  Pending versions: [3]
  Missing versions: [1, 2]

  [OK] v3 was buffered, not applied
  [OK] current version remains 0
  [OK] status remains NONE (not SHIPPED)
  [OK] v3 stored in pending events
  [OK] missing versions 1 and 2 reported

RESULT: DEMO 2 PASS
```

## Demo 3: Stale Worker

Two workers operate on `ORDER-003`. Both get their tokens from the central `FencingTokenGenerator`; workers never choose their own tokens.

1. **Worker-A** is granted **token 1** and creates the order (v1). The order records token 1 as the highest token it has accepted.
2. Worker-A stalls (for example, during a long GC pause), and **Worker-B** is granted **token 2**. Worker-B's update to v2 PAID is accepted, and the order's highest token becomes 2.
3. **Worker-A** wakes up, still believes it owns the order, and tries to write v3 SHIPPED with token 1. Since 1 < 2, the write is **rejected before any state changes**. Worker-A is now stale.
4. Worker-B, which holds the current token, continues normally and ships the order.

Actual output (from `evidence/stale_worker_demo.txt`):

```text
> Worker-A stalls (e.g. GC pause); token service grants ownership to Worker-B
  Worker-B received fencing token 2
> Worker-B (token 2) attempts update: ORDER-003 v2 PAID
  [TOKEN]    Worker-B token 2 supersedes token 1
  [APPLIED]  ORDER-003 v2 PAID: was the expected next version
  Outcome: APPLIED
  State: order=ORDER-003 version=2 status=PAID pending=[] missing=[] highest_fencing_token=2
  [OK] Worker-B (token 2) update accepted
  [OK] order now records highest token 2

> Worker-A wakes up, still believes it owns the order, and tries to write
> Worker-A (token 1) attempts update: ORDER-003 v3 SHIPPED
  [REJECTED] Worker-A (token 1) -> ORDER-003 v3 SHIPPED: stale fencing token, order already accepted token 2
  Outcome: REJECTED
  State: order=ORDER-003 version=2 status=PAID pending=[] missing=[] highest_fencing_token=2
  [OK] stale Worker-A (token 1) rejected
  [OK] order state unchanged after rejection (v2 PAID, nothing pending)
  [OK] highest token still 2
```

After the rejection, the state is still `version=2 status=PAID` with nothing pending, exactly as it was before Worker-A's attempt.

**Event versions vs. fencing tokens:** event versions decide the *order* in which business events can be applied (CREATED before PAID before SHIPPED). Fencing tokens decide *which worker* is allowed to write at all. A worker update has to pass both checks: first the token check, then the normal version rules.

## Implementation Explanation

- **Version-based ordering:** an event is applied only if `event.version == current_version + 1`. `apply_event()` also refuses to skip a version, so an order can never jump ahead.
- **Pending-event buffering:** a future version (`version > current_version + 1`) is stored in `pending_events` (a dict keyed by version), and the missing versions are reported. After each successful apply, `process_pending_events()` keeps applying buffered events as long as the next version is available.
- **Duplicate detection:** an event with `version <= current_version` has already been applied and is ignored, which makes redelivery harmless (idempotent). If an event's version is already buffered, it isn't stored twice. If the buffered copy has a different status, the event is rejected as a conflict and the original buffered event is kept.
- **Validation:** each version must carry its matching status (1=CREATED, 2=PAID, 3=SHIPPED). Invalid or unknown combinations are rejected without touching the order or its buffer.
- **Fencing-token validation:** each order stores `highest_fencing_token`. A worker update with a lower token is rejected. An equal token is allowed, and a higher token replaces the stored one. Tokens come from a central, monotonically increasing `FencingTokenGenerator`.
- **Atomic updates:** a single `threading.Lock` in `OrderProcessor` wraps both the check (token and version) and the state update. This means no other thread can change the order between the moment a write is approved and the moment it is applied.

## Test Results

The suite has 26 tests (including parametrized cases) covering every required scenario: applying v1, buffering, draining the buffer in order, duplicates, missing-version detection, invalid and conflicting events, newer/older/repeated fencing tokens, unchanged state after a stale rejection, independent orders, the full lifecycle, and the token generator staying monotonic under concurrency. Each test creates a fresh `OrderProcessor`.

Actual summary (full output in [`evidence/test_results.txt`](evidence/test_results.txt)):

```text
collecting ... collected 26 items
...
tests/test_order_processor.py::test_worker_with_newer_token_is_accepted PASSED [ 65%]
tests/test_order_processor.py::test_worker_with_older_token_is_rejected PASSED [ 69%]
tests/test_order_processor.py::test_rejected_stale_worker_does_not_modify_order PASSED [ 73%]
...
============================= 26 passed in 0.05s ==============================
```

## Reflection

**Why are timestamps alone not enough to guarantee correct event ordering in a distributed system?**

Timestamps can't guarantee correct ordering because each machine has its own clock, and those clocks are never perfectly synchronized. They drift, and NTP corrections can even move them backward, so a later event can carry an earlier timestamp. Network delays and retries also mean events often arrive in a different order than they were created. A system that trusts timestamps alone could apply an outdated event over a newer one or skip a required step without noticing. Version numbers provide a logical sequence that doesn't depend on any clock, which lets the system detect gaps and duplicates. Fencing tokens solve the related ownership problem by preventing a stale worker from overwriting shared state after a newer worker has taken over.
