"""Standalone driver: runs ST1/ST2/ST3 + a plain-read latency baseline over the
real HTTP path and verifies each against Cassandra directly. Reuses the app's
scenario runners (loadtest.py) and the loadgen invariant checks (verify.py), so
the numbers match the documented scenarios but run in their own process.

Designed to run inside a one-off container built from the app image (which has
httpx + cassandra-driver). Example:

  docker compose run --rm --no-deps -e PYTHONPATH=/app \
    -v "$PWD/loadgen:/loadgen" -w /loadgen app-1 \
    python run_standalone.py --base http://nginx:8000 \
      --contact cassandra-1,cassandra-2,cassandra-3 --dep TEST_POZ_WAW_RACE
"""
import time
import json
import argparse
import asyncio

import httpx

import loadtest          # from /app (PYTHONPATH=/app)
import verify as vr      # loadgen/verify.py


async def reset(base, dep):
    async with httpx.AsyncClient(base_url=base, timeout=30.0) as c:
        await c.post(f"/admin/departures/{dep}/reset")


async def latency_baseline(base, dep, n=50):
    """Single client, SEQUENTIAL — isolates per-request latency with no
    self-contention. Books n distinct free seats (LWT) then reads them back
    (plain LOCAL_QUORUM), so POST p50/p95 vs GET p50/p95 is the clean 'price of
    the LWT' comparison the report asks for."""
    async with httpx.AsyncClient(base_url=base, timeout=30.0) as c:
        r = await c.get(f"/departures/{dep}/seats")
        free = [(s["coach"], s["seat_number"]) for s in r.json()["seats"]
                if s["status"] == "free"][:n]
        post, get = [], []
        for i, (coach, seat) in enumerate(free):
            t0 = time.perf_counter()
            rp = await c.post("/reservations", json={
                "departure_id": dep, "coach": coach, "seat_number": seat,
                "user_id": "base", "user_name": "Base", "user_email": "b@x.com",
                "idempotency_key": f"baseline-{i}"})
            post.append((rp.status_code, (time.perf_counter() - t0) * 1000))
            t0 = time.perf_counter()
            rg = await c.get(f"/reservations/{dep}/{coach}/{seat}")
            get.append((rg.status_code, (time.perf_counter() - t0) * 1000))
    return {"book_POST": loadtest.summarize(post, 1),
            "read_GET": loadtest.summarize(get, 1)}


def verify_st2(session, dep):
    """Inline no-corruption check (loadgen/verify.py has no ST2 helper)."""
    rows = vr.read_partition(session, dep)
    bad = [r for r in rows if r.status not in ("free", "booked")]
    no_holder = [r for r in rows if r.status == "booked" and not r.user_id]
    booked = sum(1 for r in rows if r.status == "booked")
    return {
        "passed": not bad and not no_holder,
        "total": len(rows), "booked": booked, "free": len(rows) - booked,
        "bad_status": len(bad), "booked_without_holder": len(no_holder),
    }


async def main_async(args):
    cluster, session = vr.connect(args.contact.split(","), args.dc)
    out = {}
    try:
        # ST1 — same request, very fast (idempotent storm).
        await reset(args.base, args.dep)
        m = await loadtest.run_st1(args.base, args.dep, args.coach, args.seat, n=args.st1_n)
        v = vr.verify_st1(session, args.dep, args.coach, args.seat)
        out["ST1"] = {"metrics": m, "verdict": v}

        # ST2 — many random clients (book/view/edit).
        await reset(args.base, args.dep)
        m = await loadtest.run_st2(args.base, args.dep, users=args.st2_users,
                                   requests_per_user=args.st2_reqs)
        v = verify_st2(session, args.dep)
        out["ST2"] = {"metrics": m, "verdict": v}

        # ST3 — two clients race to fill the train (headline).
        await reset(args.base, args.dep)
        m = await loadtest.run_st3(args.base, args.dep)
        v = vr.verify_st3(session, args.dep, m["clientA_wins"], m["clientB_wins"])
        out["ST3"] = {"metrics": m, "verdict": v}

        # Latency baseline: single client, sequential, uncontended.
        await reset(args.base, args.dep)
        out["LATENCY_BASELINE"] = await latency_baseline(
            args.base, args.dep, n=args.read_n)
    finally:
        cluster.shutdown()
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://nginx:8000")
    ap.add_argument("--dep", default="TEST_POZ_WAW_RACE")
    ap.add_argument("--contact", default="cassandra-1,cassandra-2,cassandra-3")
    ap.add_argument("--dc", default="datacenter1")
    ap.add_argument("--coach", type=int, default=1)
    ap.add_argument("--seat", default="1A")
    ap.add_argument("--st1-n", type=int, default=200)
    ap.add_argument("--st2-users", type=int, default=20)
    ap.add_argument("--st2-reqs", type=int, default=40)
    ap.add_argument("--read-n", type=int, default=200)
    args = ap.parse_args()

    out = asyncio.run(main_async(args))
    print("=== RESULTS ===")
    print(json.dumps(out, indent=2))
    all_passed = all(s["verdict"]["passed"] for k, s in out.items()
                     if isinstance(s, dict) and "verdict" in s)
    print("ALL_PASSED:", all_passed)
    raise SystemExit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
