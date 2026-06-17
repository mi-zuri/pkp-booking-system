"""ST3 — two independent clients race to claim all seats, each in its own
shuffled order, released on a shared barrier. Backoff on transient errors only;
a 409 means "taken, move on" and is never retried."""
import time
import uuid
import json
import random
import asyncio
import argparse

import httpx

import verify as verifier


def percentile(vals, q):
    if not vals:
        return 0.0
    k = (len(vals) - 1) * q
    lo = int(k); hi = min(lo + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (k - lo)


def summarize(records, wall):
    total = len(records)
    n201 = sum(1 for c, _ in records if c in (200, 201))
    n409 = sum(1 for c, _ in records if c == 409)
    errors = sum(1 for c, _ in records if c == "error" or (isinstance(c, int) and c >= 500))
    lats = sorted(l for _, l in records)
    return {
        "total": total, "201": n201, "409": n409, "errors": errors,
        "throughput_rps": round(total / wall, 1) if wall else 0,
        "wall_seconds": round(wall, 3),
        "p50_ms": round(percentile(lats, 0.5), 1),
        "p95_ms": round(percentile(lats, 0.95), 1),
        "p99_ms": round(percentile(lats, 0.99), 1),
    }


async def book(client, dep, coach, seat, label):
    t0 = time.perf_counter()
    try:
        r = await client.post("/reservations", json={
            "departure_id": dep, "coach": coach, "seat_number": seat,
            "user_id": f"client-{label}", "user_name": f"Client {label}",
            "user_email": f"client{label}@example.com",
            "idempotency_key": f"st3-{label}-" + uuid.uuid4().hex,
        })
        return r.status_code, (time.perf_counter() - t0) * 1000
    except Exception:  # noqa: BLE001
        return "error", (time.perf_counter() - t0) * 1000


async def run(base_url, dep, retry_cap=5):
    async with httpx.AsyncClient(base_url=base_url, timeout=30.0) as c:
        r = await c.get(f"/departures/{dep}/seats")
        r.raise_for_status()
        seats = [(s["coach"], s["seat_number"]) for s in r.json()["seats"]
                 if s["status"] == "free"]
        total = len(seats)
        start = asyncio.Event()

        async def client_run(label):
            order = list(seats)
            random.shuffle(order)
            recs, wins = [], 0
            await start.wait()
            for coach, seat in order:
                attempts = 0
                while True:
                    code, lat = await book(c, dep, coach, seat, label)
                    recs.append((code, lat))
                    if code in (200, 201):
                        wins += 1; break
                    if code == 409:
                        break
                    attempts += 1
                    if attempts > retry_cap:
                        break
                    await asyncio.sleep(min(1.0, 0.05 * (2 ** attempts)))
            return recs, wins

        t0 = time.perf_counter()
        ta = asyncio.create_task(client_run("A"))
        tb = asyncio.create_task(client_run("B"))
        await asyncio.sleep(0.05)
        start.set()
        (recs_a, wins_a), (recs_b, wins_b) = await asyncio.gather(ta, tb)
        wall = time.perf_counter() - t0

    metrics = summarize(recs_a + recs_b, wall)
    metrics.update({"total_seats": total, "clientA_wins": wins_a, "clientB_wins": wins_b})
    return metrics


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8000")
    ap.add_argument("--dep", required=True)
    ap.add_argument("--contact", default="127.0.0.1")
    ap.add_argument("--dc", default="datacenter1")
    args = ap.parse_args()

    metrics = asyncio.run(run(args.base, args.dep))
    print("=== ST3 metrics ===")
    print(json.dumps(metrics, indent=2))

    cluster, session = verifier.connect(args.contact.split(","), args.dc)
    try:
        verdict = verifier.verify_st3(session, args.dep,
                                      metrics["clientA_wins"], metrics["clientB_wins"])
    finally:
        cluster.shutdown()
    print("=== ST3 verdict ===")
    print(json.dumps(verdict, indent=2))
    raise SystemExit(0 if verdict["passed"] else 1)


if __name__ == "__main__":
    main()
