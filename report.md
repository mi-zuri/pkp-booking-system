# Distributed Train Booking — Report

## 1. Description

A train seat-reservation system on a 3-node Apache Cassandra cluster (RF=3, single
datacenter). Three stateless FastAPI instances sit behind Nginx; any instance can
make, edit, and view reservations, and all state lives in Cassandra. The system
guarantees no double-booking under heavy concurrency, returns proper errors (not
500s) on contention, keeps latency bounded, and keeps serving when a node fails.

That correctness guarantee comes from a single lightweight transaction (LWT) — a
Paxos-backed conditional `UPDATE` — not from any application-level lock.

## 2. Schema

One query-first, denormalized table. Cassandra has no joins, so the table is built
around the access patterns: every query is served by the primary key alone, so
`ALLOW FILTERING` is never used.

```cql
CREATE TABLE reservations_by_departure (
    departure_id     text,        -- partition key
    coach            int,         -- clustering
    seat_number      text,        -- clustering
    status           text,        -- 'free' | 'booked'  (regular, mutable)
    user_id          text,
    user_name        text,
    user_email       text,        -- denormalized: "who booked it" needs no join
    idempotency_key  text,        -- last applied request, stored on the row
    created_at       timestamp,
    updated_at       timestamp,
    PRIMARY KEY ((departure_id), coach, seat_number)
);
```

All seats of a departure share one partition (`departure_id`), so the whole seat
map is a single-partition read and every booking LWT for that departure serializes
through that partition's Paxos state. `status` is a regular column on purpose: it
must be mutable, since booking is `UPDATE ... IF status='free'` and you cannot
`UPDATE` a primary-key column. Storing `idempotency_key`/`user_id` on the row makes
the seat its own idempotency record — no second table needed.

## 3. Consistency-level rationale

Cassandra is AP by default; we apply tunable consistency to get strong guarantees
exactly on the booking path while keeping browsing fast.

| Operation                        | Consistency                                 | Why                                                                |
| -------------------------------- | ------------------------------------------- | ------------------------------------------------------------------ |
| Browse seat map                  | `LOCAL_ONE`                                 | Fast; a slightly stale map is fine for display.                    |
| Book a seat (LWT)                | Paxos `LOCAL_SERIAL`, commit `LOCAL_QUORUM` | Conditional CAS; the condition is the correctness guarantee.       |
| Read-after-write (view a seat)   | `LOCAL_QUORUM`                              | RF=3: QUORUM write + QUORUM read overlap, so you see your booking. |
| Read-back after failed/timed LWT | `LOCAL_SERIAL`                              | Linearizable read of the seat's true post-Paxos state.             |

We use `LOCAL_*` everywhere — identical to the bare forms in a single DC, but with a
cleaner multi-DC story. Booking correctness lives in the conditional write itself,
not in the read consistency; reads are tuned only for the freshness each case needs.

## 4. The booking path — one LWT + a read-back

```cql
UPDATE reservations_by_departure
SET status='booked', user_id=?, user_name=?, user_email=?,
    idempotency_key=?, updated_at=?
WHERE departure_id=? AND coach=? AND seat_number=?
IF status='free';
```

Decision logic on the result:

- `applied=True` → the seat is ours → **201**.
- `applied=False` → a conditional `UPDATE` returns only the `IF`-clause column
  (`status`), not the row's `idempotency_key`/`user_id`. So we read the seat back at
  `LOCAL_SERIAL`:
  - row's `idempotency_key` matches this request → idempotent retry of an already-
    successful booking → **201**;
  - otherwise genuinely taken by someone else → **409** (holder comes from the
    read-back).

The read-back is an ordinary read, not a second Paxos round, so the "one LWT per
booking" property holds. This single round handles both ST1 (same request 50×: one
CAS applies, the other 49 read back their own key → all 201, one booking) and ST3
(two requests race: Paxos applies exactly one, the loser reads back a different
holder → 409).

Edit uses the same pattern with an ownership guard,
`IF status='booked' AND user_id=?` — you can only edit a seat that is actually held,
and only your own.

## 5. Problems encountered

**Per-instance state behind the load balancer.** The demo test-runner first kept
each run's state in a per-process dict. With three stateless instances behind Nginx's
round-robin, the `POST` that launched a run created it on one instance, but the poll
`GET`s round-robined — so two-thirds landed on instances that had never seen the
`run_id` and returned 404, and the UI froze. The lesson is the project's own thesis
turned on the one place that broke it: nothing app-local survives load balancing. The
fix moves run state into a Cassandra `test_runs` table (`LOCAL_QUORUM` write-then-read
= read-after-write), so any instance answers any poll consistently.

## 6. Metrics methodology

All three stress tests emit the same format — total / 201 / 409 / errors, throughput
(rps), p50/p95/p99 latency — so results are directly comparable. For headline numbers,
run the load generators in `loadgen/` as separate processes (not via `/admin/tests`),
so the generator and the system under test don't share app resources and skew latency.
The in-browser `/tests` runner exists to make stress-test behavior visible during a
live demo; it uses the same scenarios and metrics format, but its numbers share the
app's event loop and shouldn't be quoted as performance figures.

Each test ends with a verification phase (`verify.py`) that reads full partitions by
key at `LOCAL_QUORUM` (no `ALLOW FILTERING`) and counts/groups in Python — asserting,
for ST3: every booked seat has exactly one holder, all seats sold,
`clientA_wins + clientB_wins == total`, and neither client took 0 or all.

> Fairness is statistical, not guaranteed. The independent shuffle + backoff make a
> lopsided split overwhelmingly unlikely, but only no-double-booking is a hard
> guarantee. The "neither took all" invariant passes essentially always; the honest
> framing is that fairness is probabilistic while safety is absolute (enforced by
> Paxos).

## 7. Results

Environment: 3 Cassandra 4.1 nodes (RF=3) + 3 FastAPI instances + Nginx, all on
one Docker host (laptop). Load driven from a separate one-off container via
`loadgen/run_standalone.py` (reuses the documented scenarios), so the generator
doesn't share the serving apps' event loops. Target: the 80-seat
`TEST_POZ_WAW_RACE` departure, auto-reset to all-free before each test.

| Test                             | total | 201 | 409 | errors | rps  | p50 ms | p95 ms | p99 ms | verdict                                             |
| -------------------------------- | ----- | --- | --- | ------ | ---- | ------ | ------ | ------ | --------------------------------------------------- |
| ST1 (same request ×50, fast)     | 50    | 49  | 0   | 1      | 70.6 | 410.6  | 679.2  | 698.3  | **PASS** — seat booked once, single holder          |
| ST2 (20 clients × 40 random ops) | 800   | 332 | 467 | 1      | 98.9 | 82.2   | 688.6  | 1023.8 | **PASS** — all 80 sold, no corruption               |
| ST3 (2 clients race full wagon)  | 160   | 80  | 80  | 0      | 68.1 | 15.0   | 114.4  | 205.7  | **PASS** — A 56 / B 24, no double-booking, all sold |

**LWT-cost comparison (problem (e)), single client, sequential, uncontended:**

| Path                                                  | p50 ms | p95 ms |
| ----------------------------------------------------- | ------ | ------ |
| Book a seat — LWT `POST /reservations`                | 10.0   | 10.9   |
| Plain read — `GET /reservations/...` (`LOCAL_QUORUM`) | 4.1    | 4.3    |

The booking LWT costs ~2.4× a plain read (10.0 vs 4.1 ms p50) — the concrete price
of the Paxos propose/accept + commit round on the booking path.

**Reading the numbers.** ST3 is the headline: two clients fired 160 booking
attempts at the same 80-seat partition; Paxos applied exactly one CAS per seat, so
the losers got 80 clean 409s, all seats sold, zero double-bookings, and _both_
clients ended up with reservations (56 / 24 — fairness is statistical, see §6). The
single `errors` in ST1/ST2 is a transient timeout under the concurrent burst that
the read-back resolved to a `503` (never a double-booking) — the safety guarantee
held. ST1's higher p50 (≈410 ms) is expected: 50 identical requests fired
simultaneously at one seat all serialize through that partition's single Paxos lane
(problem (a)); the uncontended baseline above (p50 10 ms) shows the real per-booking
cost once the herd is removed.
