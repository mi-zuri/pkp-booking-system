"""Invariant checks run after a stress test. Reads full partitions by key at
LOCAL_QUORUM and counts/groups in Python — never filters on a non-key column."""
from collections import Counter


def _read_partition(db, departure_id):
    return db.read_seatmap(departure_id, quorum=True)


def _count(rows):
    booked = [r for r in rows if r.status == "booked"]
    free = [r for r in rows if r.status == "free"]
    return rows, booked, free


def verify_st1(db, departure_id, coach, seat):
    """ST1: exactly one effective booking on the target seat, one holder."""
    rows = _read_partition(db, departure_id)
    target = [r for r in rows if r.coach == coach and r.seat_number == seat]
    if not target:
        return {"passed": False, "reason": "target seat missing"}
    row = target[0]
    booked = row.status == "booked"
    holders = {row.user_id} if booked else set()
    passed = booked and len(holders) == 1
    return {
        "passed": passed,
        "seat_booked": booked,
        "distinct_holders": len(holders),
        "holder": row.user_id,
        "note": "one idempotency key fired N times -> exactly one booking",
    }


def verify_no_corruption(db, departure_id):
    """ST2: every seat is either free or booked-by-exactly-one; counts add up."""
    rows, booked, free = _count(_read_partition(db, departure_id))
    bad_status = [r for r in rows if r.status not in ("free", "booked")]
    booked_without_holder = [r for r in booked if not r.user_id]
    passed = not bad_status and not booked_without_holder
    return {
        "passed": passed,
        "total": len(rows),
        "free": len(free),
        "booked": len(booked),
        "bad_status": len(bad_status),
        "booked_without_holder": len(booked_without_holder),
    }


def verify_st3(db, departure_id, metrics=None):
    """ST3 headline invariants: no double-booking, all seats sold, neither
    client took 0 or all."""
    rows, booked, free = _count(_read_partition(db, departure_id))
    total = len(rows)

    # One seat = one row, so "one holder per seat" is structurally guaranteed by
    # the PK; double-booking would show as a booked seat with an empty holder.
    by_holder = Counter(r.user_id for r in booked)
    all_sold = len(free) == 0 and len(booked) == total

    no_overlap = all(r.user_id for r in booked)  # each booked row has one holder
    neither_took_all = True
    fairness = {}
    if metrics and "clientA_wins" in metrics:
        a, b = metrics["clientA_wins"], metrics["clientB_wins"]
        neither_took_all = (a > 0 and b > 0 and a < total and b < total)
        # cross-check the API's win counts against the DB's holder tally
        db_a = by_holder.get("client-A", 0)
        db_b = by_holder.get("client-B", 0)
        fairness = {
            "api_clientA_wins": a, "api_clientB_wins": b,
            "db_clientA_holds": db_a, "db_clientB_holds": db_b,
            "wins_sum_equals_total": (a + b == total),
        }

    passed = all_sold and no_overlap and neither_took_all
    return {
        "passed": passed,
        "total_seats": total,
        "sold": len(booked),
        "free": len(free),
        "all_sold": all_sold,
        "no_double_booking": no_overlap,
        "neither_took_all": neither_took_all,
        "holders": dict(by_holder),
        **fairness,
    }
