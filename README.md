# Distributed Train Booking System

Book a specific seat (coach + seat number) on a specific train departure, held for
the whole run. The app is stateless and runs as three identical instances behind
Nginx — hit any one and get the same answer, and the system keeps serving if a node
dies. No double-booking is guaranteed by Cassandra lightweight transactions (LWT —
Paxos compare-and-set), not by application locks.

**Stack:** Python · FastAPI · `cassandra-driver` (CQL, prepared statements) ·
Apache Cassandra (3 nodes, RF=3) · Docker Compose · Nginx · Locust + asyncio
load generators · minimal HTML/JS seat-map UI.

## Architecture

```text
              [ Nginx LB :8000 ]
              /       |        \
          app-1     app-2     app-3      ← FastAPI (stateless), :8001–:8003
            |         |          |
       cassandra-1 cassandra-2 cassandra-3   ← masterless, RF=3, DC 'datacenter1'
```

All state lives in Cassandra. Each app uses all three nodes as contact points with
a token-aware, DC-aware policy and an explicit `local_dc`.

## Quick start

```bash
# Start everything (3 Cassandra + 3 apps + Nginx). The schema-loader service
# applies the schema automatically once the cluster is healthy.
docker compose up -d --build

# Wait until all three nodes show UN (Up/Normal):
docker exec cassandra-1 nodetool status

# Open the UI (round-robined across the three app instances):
open http://localhost:8000          # seat map
open http://localhost:8000/tests    # stress-test runner
```

First boot takes a couple of minutes — Cassandra must gossip and stabilize before
it's writable. Compose healthchecks gate the apps on this, so they won't serve
until the schema is loaded.

### Using the UI

A few default departures from Poznań (to Warszawa, Kraków, Gdańsk, Wrocław,
Szczecin) are auto-seeded on startup. Pick a departure → its wagon appears → tap
free (green) seats to select → enter passenger name/email → **Accept** to book.
Tap a booked (red) seat to see its holder; if it's yours, edit it inline (one seat
at a time).

### Same flow from the CLI

```bash
BASE=http://localhost:8000
DEP=IC3501_POZ_WAW_2026-06-20    # one of the auto-seeded departures

curl -s $BASE/departures | jq '.departures[].departure_id'   # list routes
curl -s $BASE/departures/$DEP/seats | jq '.free,.booked,.total'

# Book seat 1A in coach 1. The idempotency key makes retries safe.
curl -s -XPOST $BASE/reservations -H 'content-type: application/json' -d "{
  \"departure_id\":\"$DEP\",\"coach\":1,\"seat_number\":\"1A\",
  \"user_id\":\"alice\",\"user_name\":\"Alice\",\"user_email\":\"a@x.com\",
  \"idempotency_key\":\"demo-1\"}"

curl -s $BASE/reservations/$DEP/1/1A | jq    # who booked it
curl -s $BASE/health                          # which instance served you
```

## API

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/departures` | List the auto-seeded Poznań departures |
| `POST` | `/departures` | Create a departure + pre-seed free seats |
| `GET` | `/departures/{id}/seats` | Seat map (`LOCAL_ONE`) |
| `GET` | `/reservations/{dep}/{coach}/{seat}` | View a seat + its holder (`LOCAL_QUORUM`) |
| `POST` | `/reservations` | Book a seat (single update-if-free LWT) |
| `PATCH` | `/reservations/{dep}/{coach}/{seat}` | Edit passenger details (ownership-guarded LWT) |
| `GET` | `/health` | Which instance is serving |
| `POST` | `/admin/tests/{st}` | _(demo)_ Launch ST1/ST2/ST3, returns `run_id` |
| `GET` | `/admin/tests/{run_id}` | _(demo)_ Fetch metrics + verdict |
| `POST` | `/admin/departures/{id}/reset` | _(demo)_ Re-seed all seats to free |

## Stress tests

The in-browser runner (`/tests`) is for the live demo. For the report's headline
numbers, run the load generators as separate host processes so the generator
doesn't share resources with the system under test.

```bash
cd loadgen
pip install -r requirements.txt   # locust, httpx, cassandra-driver

DEP=TEST_POZ_WAW_RACE             # the dedicated load-test departure
curl -s -XPOST http://localhost:8000/admin/departures/$DEP/reset   # start from all-free

# ST1 — idempotent storm: one fixed key fired as fast as possible.
#        Expect ~all 201, exactly one booking in the DB.
DEP=$DEP COACH=1 SEAT=1A IDEM=fixed-key-123 \
  locust -f st1_locust.py --host http://localhost:8000 --headless -u 1 -r 1 -t 10s
python verify.py $DEP

# ST2 — random clients book/view/edit; assert no corruption afterward.
DEP=$DEP \
  locust -f st2_locust.py --host http://localhost:8000 --headless -u 20 -r 5 -t 30s
python verify.py $DEP

# ST3 — headline: two clients race to fill the train. Asserts zero overlap,
#        all seats sold, neither took all. Prints metrics + verdict.
python st3_race.py --base http://localhost:8000 --dep $DEP
```

`verify.py` and `st3_race.py` connect to Cassandra on `127.0.0.1:9042`
(cassandra-1's mapped port) with `local_dc=datacenter1`. Override via
`--contact` / `--dc`.

## Node-failure tolerance

```bash
# While ST2 runs, kill one Cassandra node. RF=3 + LOCAL_QUORUM (2 of 3) means
# bookings continue and no data is lost. Bring it back when done.
docker stop cassandra-2
docker start cassandra-2

# Killing an app instance is invisible to clients — Nginx routes around it,
# and the survivors talk to all three Cassandra nodes anyway.
docker stop app-1
```

## Repository layout

```text
train-booking/
 ├── docker-compose.yml     # 3 Cassandra + schema-loader + 3 apps + Nginx
 ├── nginx.conf
 ├── schema.cql             # keyspace (RF=3) + reservations_by_departure
 ├── app/
 │   ├── main.py            # FastAPI routes (booking LWT + read-back)
 │   ├── db.py              # driver setup, prepared stmts, consistency, retry/backoff
 │   ├── seeder.py          # create departure + pre-seed free seats
 │   ├── loadtest.py        # async scenarios + metrics (admin runner)
 │   ├── verify.py          # invariant checks (admin runner)
 │   └── static/            # seat-map UI + test-runner page
 ├── loadgen/
 │   ├── st1_locust.py  st2_locust.py  st3_race.py  verify.py
 │   └── requirements.txt
 ├── README.md
 └── report.md
```

See [report.md](report.md) for the consistency-level rationale and the
distributed-systems problems encountered.
