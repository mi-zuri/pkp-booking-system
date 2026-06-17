"""Standalone invariant checker — a separate process that talks to Cassandra
directly, so it sees committed ground truth rather than an app-cached view."""
import os
import sys
import json
import argparse
from collections import Counter

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy

KEYSPACE = "train_booking"


def connect(contact_points, local_dc):
    profile = ExecutionProfile(
        load_balancing_policy=TokenAwarePolicy(
            DCAwareRoundRobinPolicy(local_dc=local_dc)),
        consistency_level=ConsistencyLevel.LOCAL_QUORUM,
        request_timeout=15.0,
    )
    cluster = Cluster(contact_points=contact_points,
                      execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                      protocol_version=4)
    return cluster, cluster.connect(KEYSPACE)


def read_partition(session, departure_id):
    rows = session.execute(
        "SELECT coach, seat_number, status, user_id "
        "FROM reservations_by_departure WHERE departure_id=%s",
        (departure_id,))
    return list(rows)


def verify_st3(session, departure_id, clientA_wins=None, clientB_wins=None):
    rows = read_partition(session, departure_id)
    total = len(rows)
    booked = [r for r in rows if r.status == "booked"]
    free = [r for r in rows if r.status == "free"]
    by_holder = Counter(r.user_id for r in booked)

    all_sold = len(free) == 0 and len(booked) == total
    no_double_booking = all(r.user_id for r in booked)  # one holder per seat row
    neither_took_all = len(by_holder) >= 2 and all(0 < c < total for c in by_holder.values())

    result = {
        "departure_id": departure_id,
        "total_seats": total,
        "sold": len(booked),
        "free": len(free),
        "all_sold": all_sold,
        "no_double_booking": no_double_booking,
        "neither_took_all": neither_took_all,
        "holders": dict(by_holder),
    }
    if clientA_wins is not None:
        result["wins_sum_equals_total"] = (clientA_wins + clientB_wins == total)
    result["passed"] = all_sold and no_double_booking and neither_took_all
    return result


def verify_st1(session, departure_id, coach, seat):
    rows = read_partition(session, departure_id)
    target = [r for r in rows if r.coach == coach and r.seat_number == seat]
    if not target:
        return {"passed": False, "reason": "target seat missing"}
    row = target[0]
    return {
        "passed": row.status == "booked" and bool(row.user_id),
        "seat_booked": row.status == "booked",
        "holder": row.user_id,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("departure_id")
    ap.add_argument("--contact", default=os.environ.get("CASSANDRA_CONTACT_POINTS", "127.0.0.1"))
    ap.add_argument("--dc", default=os.environ.get("CASSANDRA_LOCAL_DC", "datacenter1"))
    args = ap.parse_args()

    cluster, session = connect(args.contact.split(","), args.dc)
    try:
        result = verify_st3(session, args.departure_id)
        print(json.dumps(result, indent=2))
        sys.exit(0 if result["passed"] else 1)
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    main()
