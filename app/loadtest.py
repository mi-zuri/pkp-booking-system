"""Async HTTP scenario runners + uniform metrics for the /admin/tests runner.
Load goes over the real HTTP path (Nginx -> FastAPI -> driver -> Cassandra);
loadgen/ holds standalone runners with the same scenarios."""
import time
import uuid
import random
import asyncio

import httpx


def percentile(sorted_vals, q):
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    frac = k - lo
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * frac


def summarize(records, wall_seconds):
    """records: list of (status_code_or_'error', latency_ms)."""
    total = len(records)
    n201 = sum(1 for c, _ in records if c == 201 or c == 200)
    n409 = sum(1 for c, _ in records if c == 409)
    errors = sum(1 for c, _ in records
                 if c == "error" or (isinstance(c, int) and c >= 500))
    lats = sorted(l for _, l in records)
    return {
        "total": total,
        "201": n201,
        "409": n409,
        "errors": errors,
        "throughput_rps": round(total / wall_seconds, 1) if wall_seconds else 0,
        "wall_seconds": round(wall_seconds, 3),
        "p50_ms": round(percentile(lats, 0.50), 1),
        "p95_ms": round(percentile(lats, 0.95), 1),
        "p99_ms": round(percentile(lats, 0.99), 1),
    }


async def _book(client, departure_id, coach, seat, user_id, name, email, idem):
    t0 = time.perf_counter()
    try:
        r = await client.post("/reservations", json={
            "departure_id": departure_id, "coach": coach, "seat_number": seat,
            "user_id": user_id, "user_name": name, "user_email": email,
            "idempotency_key": idem,
        })
        return r.status_code, (time.perf_counter() - t0) * 1000
    except Exception:  # noqa: BLE001
        return "error", (time.perf_counter() - t0) * 1000


async def _free_seats(client, departure_id):
    r = await client.get(f"/departures/{departure_id}/seats")
    r.raise_for_status()
    data = r.json()
    return [(s["coach"], s["seat_number"]) for s in data["seats"]
            if s["status"] == "free"], data


async def run_st1(base_url, departure_id, coach, seat, n=50):
    """Fire the identical booking request (one idempotency key) n times."""
    idem = "st1-" + uuid.uuid4().hex
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as c:
        t0 = time.perf_counter()
        results = await asyncio.gather(*[
            _book(c, departure_id, coach, seat, "st1-user",
                  "ST1 User", "st1@example.com", idem)
            for _ in range(n)
        ])
        wall = time.perf_counter() - t0
    return summarize(results, wall)


async def run_st2(base_url, departure_id, users=5, requests_per_user=40):
    """Multiple random clients doing book / view / edit."""
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as c:
        free, _ = await _free_seats(c, departure_id)
        random.shuffle(free)

        async def worker(uid):
            recs = []
            for _ in range(requests_per_user):
                if random.random() < 0.7 and free:
                    coach, seat = random.choice(free)
                    recs.append(await _book(
                        c, departure_id, coach, seat, f"u{uid}",
                        f"User {uid}", f"u{uid}@example.com",
                        "st2-" + uuid.uuid4().hex))
                else:
                    coach, seat = random.choice(free) if free else (1, "1A")
                    t0 = time.perf_counter()
                    try:
                        r = await c.get(
                            f"/reservations/{departure_id}/{coach}/{seat}")
                        recs.append((r.status_code, (time.perf_counter()-t0)*1000))
                    except Exception:  # noqa: BLE001
                        recs.append(("error", (time.perf_counter()-t0)*1000))
            return recs

        t0 = time.perf_counter()
        per = await asyncio.gather(*[worker(i) for i in range(users)])
        wall = time.perf_counter() - t0
    flat = [r for recs in per for r in recs]
    return summarize(flat, wall)


async def run_st3(base_url, departure_id, transient_retry_cap=5):
    """Two clients race to fill the train (the headline test)."""
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as c:
        free, data = await _free_seats(c, departure_id)
        total_seats = len(free)
        start = asyncio.Event()

        async def client_run(label):
            order = list(free)
            random.shuffle(order)            # independent shuffle per client
            recs, wins = [], []
            await start.wait()               # shared barrier — start together
            for coach, seat in order:
                attempts = 0
                while True:
                    code, lat = await _book(
                        c, departure_id, coach, seat, f"client-{label}",
                        f"Client {label}", f"client{label}@example.com",
                        f"st3-{label}-" + uuid.uuid4().hex)
                    recs.append((code, lat))
                    if code in (200, 201):
                        wins.append((coach, seat))
                        break
                    if code == 409:
                        break               # taken — move on, never retry a 409
                    attempts += 1            # transient: bounded backoff, retry
                    if attempts > transient_retry_cap:
                        break
                    await asyncio.sleep(min(1.0, 0.05 * (2 ** attempts)))
            return recs, wins

        t0 = time.perf_counter()
        task_a = asyncio.create_task(client_run("A"))
        task_b = asyncio.create_task(client_run("B"))
        await asyncio.sleep(0.05)            # let both reach the barrier
        start.set()
        (recs_a, wins_a), (recs_b, wins_b) = await asyncio.gather(task_a, task_b)
        wall = time.perf_counter() - t0

    metrics = summarize(recs_a + recs_b, wall)
    metrics.update({
        "total_seats": total_seats,
        "clientA_wins": len(wins_a),
        "clientB_wins": len(wins_b),
    })
    return metrics
