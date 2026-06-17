-- Keyspace + tables. RF=3, NetworkTopologyStrategy, single DC 'datacenter1'.
-- This DC name MUST match the snitch's DC (CASSANDRA_DC) and the driver's
-- local_dc (CASSANDRA_LOCAL_DC), or queries route to "no local replicas".

CREATE KEYSPACE IF NOT EXISTS train_booking
WITH replication = {
    'class': 'NetworkTopologyStrategy',
    'datacenter1': 3
};

USE train_booking;

-- One partition per departure: the seat map is a single-partition read and
-- every booking LWT serializes through that partition's Paxos state. `status`
-- is a non-key column so the booking path can be UPDATE ... IF status='free';
-- the row's idempotency_key + user_id make it its own idempotency record.
CREATE TABLE IF NOT EXISTS reservations_by_departure (
    departure_id     text,        -- e.g. 'IC3501_2026-06-20'  (partition key)
    coach            int,         -- (clustering)
    seat_number      text,        -- (clustering)
    status           text,        -- 'free' | 'booked'
    user_id          text,
    user_name        text,
    user_email       text,
    idempotency_key  text,
    created_at       timestamp,
    updated_at       timestamp,
    PRIMARY KEY ((departure_id), coach, seat_number)
);

-- Demo stress-test run state. Shared in Cassandra (not a per-instance dict) so a
-- poll landing on ANY app instance behind the load balancer sees the same run —
-- LOCAL_QUORUM write-then-read gives read-after-write. `result` is a JSON blob
-- ({metrics, verdict} on success, {error} on failure).
CREATE TABLE IF NOT EXISTS test_runs (
    run_id   text PRIMARY KEY,
    test     text,
    status   text,        -- 'running' | 'done' | 'error'
    result   text
);
