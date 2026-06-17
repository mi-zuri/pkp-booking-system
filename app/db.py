"""Cassandra driver setup: prepared statements, per-statement consistency
levels, token/DC-aware routing, and bounded backoff for transient errors only."""
import os
import time
import random
import logging
from datetime import datetime, timezone

from cassandra import ConsistencyLevel, Unavailable, OperationTimedOut, ReadTimeout
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy

log = logging.getLogger("db")

CONTACT_POINTS = os.environ.get("CASSANDRA_CONTACT_POINTS", "127.0.0.1").split(",")
LOCAL_DC = os.environ.get("CASSANDRA_LOCAL_DC", "datacenter1")
KEYSPACE = os.environ.get("CASSANDRA_KEYSPACE", "train_booking")

MAX_RETRIES = 5
BASE_BACKOFF = 0.05   # seconds
MAX_BACKOFF = 1.0


class TransientError(Exception):
    """Raised when a retryable Cassandra error exhausts its retry cap."""


def _now():
    return datetime.now(timezone.utc)


class DB:
    def __init__(self):
        self.cluster = None
        self.session = None
        self.stmts = {}

    def connect(self):
        # Token-aware routing to a replica that owns the key; DC-aware with an
        # explicit local_dc — recent driver versions no longer infer it and will
        # otherwise route to no local replicas.
        profile = ExecutionProfile(
            load_balancing_policy=TokenAwarePolicy(
                DCAwareRoundRobinPolicy(local_dc=LOCAL_DC)
            ),
            request_timeout=15.0,
        )
        self.cluster = Cluster(
            contact_points=CONTACT_POINTS,
            execution_profiles={EXEC_PROFILE_DEFAULT: profile},
            protocol_version=4,
        )
        self.session = self.cluster.connect(KEYSPACE)
        self._prepare()
        log.info("Connected to %s (local_dc=%s, keyspace=%s)",
                 CONTACT_POINTS, LOCAL_DC, KEYSPACE)

    def _prepare(self):
        s = self.session

        # Booking: one conditional write.
        book = s.prepare("""
            UPDATE reservations_by_departure
            SET status='booked', user_id=?, user_name=?, user_email=?,
                idempotency_key=?, updated_at=?
            WHERE departure_id=? AND coach=? AND seat_number=?
            IF status='free'
        """)
        book.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        book.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        self.stmts["book"] = book

        # Edit: ownership-guarded conditional write.
        edit = s.prepare("""
            UPDATE reservations_by_departure
            SET user_name=?, user_email=?, updated_at=?
            WHERE departure_id=? AND coach=? AND seat_number=?
            IF status='booked' AND user_id=?
        """)
        edit.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        edit.serial_consistency_level = ConsistencyLevel.LOCAL_SERIAL
        self.stmts["edit"] = edit

        # Single-seat read for read-after-write / "view who booked it".
        read_q = s.prepare("""
            SELECT departure_id, coach, seat_number, status, user_id,
                   user_name, user_email, idempotency_key, created_at, updated_at
            FROM reservations_by_departure
            WHERE departure_id=? AND coach=? AND seat_number=?
        """)
        read_q.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        self.stmts["read_seat"] = read_q

        # Linearizable read-back after a failed CAS or a WriteTimeout.
        read_s = s.prepare("""
            SELECT departure_id, coach, seat_number, status, user_id,
                   user_name, user_email, idempotency_key, created_at, updated_at
            FROM reservations_by_departure
            WHERE departure_id=? AND coach=? AND seat_number=?
        """)
        read_s.consistency_level = ConsistencyLevel.LOCAL_SERIAL
        self.stmts["read_seat_serial"] = read_s

        # Seat-map browse: LOCAL_ONE, fast, staleness acceptable.
        seatmap = s.prepare("""
            SELECT departure_id, coach, seat_number, status, user_id,
                   user_name, user_email, idempotency_key, created_at, updated_at
            FROM reservations_by_departure
            WHERE departure_id=?
        """)
        seatmap.consistency_level = ConsistencyLevel.LOCAL_ONE
        self.stmts["seatmap"] = seatmap

        # Same full-partition read at QUORUM, for verification / counting.
        seatmap_q = s.prepare("""
            SELECT departure_id, coach, seat_number, status, user_id,
                   user_name, user_email, idempotency_key, created_at, updated_at
            FROM reservations_by_departure
            WHERE departure_id=?
        """)
        seatmap_q.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        self.stmts["seatmap_quorum"] = seatmap_q

        # Seeding / reset: plain writes at LOCAL_QUORUM.
        seed = s.prepare("""
            INSERT INTO reservations_by_departure
                (departure_id, coach, seat_number, status, created_at, updated_at)
            VALUES (?, ?, ?, 'free', ?, ?)
        """)
        seed.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        self.stmts["seed"] = seed

        # Reset clears the booking fields back to free (overwrite, no LWT).
        reset = s.prepare("""
            UPDATE reservations_by_departure
            SET status='free', user_id=null, user_name=null, user_email=null,
                idempotency_key=null, updated_at=?
            WHERE departure_id=? AND coach=? AND seat_number=?
        """)
        reset.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        self.stmts["reset"] = reset

        # Demo test-run state shared across instances (not a per-process dict).
        put_run = s.prepare(
            "INSERT INTO test_runs (run_id, test, status, result) "
            "VALUES (?, ?, ?, ?)")
        put_run.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        self.stmts["put_run"] = put_run

        get_run = s.prepare(
            "SELECT run_id, test, status, result FROM test_runs WHERE run_id=?")
        get_run.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        self.stmts["get_run"] = get_run

    def shutdown(self):
        if self.cluster:
            self.cluster.shutdown()

    def _execute_retry(self, stmt, params):
        """Retry transient errors only with bounded jittered backoff. Never
        wraps an LWT WriteTimeout (caller does read-back) or a 409."""
        attempt = 0
        while True:
            try:
                return self.session.execute(stmt, params)
            except (Unavailable, OperationTimedOut, ReadTimeout) as e:
                attempt += 1
                if attempt > MAX_RETRIES:
                    log.warning("Transient error, retry cap hit: %r", e)
                    raise TransientError(str(e)) from e
                backoff = min(MAX_BACKOFF, BASE_BACKOFF * (2 ** (attempt - 1)))
                backoff += random.uniform(0, backoff)  # full jitter
                log.info("Transient error %s; retry %d after %.3fs", type(e).__name__, attempt, backoff)
                time.sleep(backoff)

    def book_seat(self, departure_id, coach, seat_number, user_id, user_name,
                  user_email, idempotency_key):
        """Run the booking LWT. Returns the ResultSet (row[0].applied tells us
        whether the CAS won). WriteTimeout is intentionally NOT caught here —
        only _execute_retry's safe (Unavailable) cases are."""
        params = (user_id, user_name, user_email, idempotency_key, _now(),
                  departure_id, coach, seat_number)
        return self._execute_retry(self.stmts["book"], params)

    def edit_seat(self, departure_id, coach, seat_number, user_id,
                  user_name, user_email):
        params = (user_name, user_email, _now(),
                  departure_id, coach, seat_number, user_id)
        return self._execute_retry(self.stmts["edit"], params)

    def read_seat(self, departure_id, coach, seat_number, serial=False):
        stmt = self.stmts["read_seat_serial"] if serial else self.stmts["read_seat"]
        rs = self._execute_retry(stmt, (departure_id, coach, seat_number))
        return rs.one()

    def read_seatmap(self, departure_id, quorum=False):
        stmt = self.stmts["seatmap_quorum"] if quorum else self.stmts["seatmap"]
        rs = self._execute_retry(stmt, (departure_id,))
        return list(rs)

    def seed_seat(self, departure_id, coach, seat_number):
        now = _now()
        self.session.execute(self.stmts["seed"],
                             (departure_id, coach, seat_number, now, now))

    def reset_seat(self, departure_id, coach, seat_number):
        self.session.execute(self.stmts["reset"],
                             (_now(), departure_id, coach, seat_number))

    def save_run(self, run_id, test, status, result=None):
        """Persist a stress-test run so any instance can serve the poll."""
        self._execute_retry(self.stmts["put_run"],
                            (run_id, test, status, result))

    def load_run(self, run_id):
        return self._execute_retry(self.stmts["get_run"], (run_id,)).one()


# Module-level singleton used by the FastAPI app.
db = DB()
