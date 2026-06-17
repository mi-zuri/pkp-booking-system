"""FastAPI routes. Booking is an update-if-free LWT plus a read-back resolving
idempotent-retry vs. conflict vs. ambiguous-timeout; everything else is a
key-only query."""
import os
import json
import uuid
import asyncio
import logging
from typing import Optional

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from cassandra import WriteTimeout

from db import db, TransientError
import seeder
import loadtest
import verify

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("main")

INSTANCE_NAME = os.environ.get("INSTANCE_NAME", "app")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "static")

# Re-attempts after an ambiguous write timeout that read-back shows still-free.
# Small cap: contention is resolved by Paxos, not by spinning.
MAX_LWT_ATTEMPTS = 3

app = FastAPI(title="Distributed Train Booking")


@app.on_event("startup")
def startup():
    db.connect()
    try:
        seeder.seed_defaults(db)  # idempotent — skips already-seeded departures
    except Exception as e:  # noqa: BLE001
        log.warning("default seeding skipped: %s", e)


@app.on_event("shutdown")
def shutdown():
    db.shutdown()


class CreateDeparture(BaseModel):
    departure_id: str
    coaches: int = 2
    rows_per_coach: int = 10
    seats_per_row: int = 6


class BookRequest(BaseModel):
    departure_id: str
    coach: int
    seat_number: str
    user_id: str
    user_name: str
    user_email: str
    idempotency_key: str = Field(..., description="Client-generated; makes retries safe")


class EditRequest(BaseModel):
    user_id: str
    user_name: str
    user_email: str


def _seat_dict(row):
    if row is None:
        return None
    return {
        "departure_id": row.departure_id,
        "coach": row.coach,
        "seat_number": row.seat_number,
        "status": row.status,
        "user_id": row.user_id,
        "user_name": row.user_name,
        "user_email": row.user_email,
        "updated_at": row.updated_at.isoformat() if row.updated_at else None,
    }


@app.get("/health")
def health():
    """Report the serving instance and whether the DB is reachable."""
    try:
        db.session.execute("SELECT now() FROM system.local")
        ok = True
    except Exception as e:  # noqa: BLE001
        log.warning("health DB check failed: %s", e)
        ok = False
    return {"instance": INSTANCE_NAME, "db_reachable": ok}


@app.get("/departures")
def list_departures():
    return {"departures": seeder.public_departures()}


@app.post("/departures", status_code=201)
def create_departure(req: CreateDeparture):
    n = seeder.seed_departure(db, req.departure_id, req.coaches,
                              req.rows_per_coach, req.seats_per_row)
    return {"departure_id": req.departure_id, "seats_seeded": n}


@app.get("/departures/{departure_id}/seats")
def seat_map(departure_id: str):
    rows = db.read_seatmap(departure_id)  # LOCAL_ONE — fast browse
    if not rows:
        raise HTTPException(404, f"No departure '{departure_id}'")
    seats = sorted((_seat_dict(r) for r in rows),
                   key=lambda s: (s["coach"], s["seat_number"]))
    free = sum(1 for s in seats if s["status"] == "free")
    return {
        "departure_id": departure_id,
        "total": len(seats),
        "free": free,
        "booked": len(seats) - free,
        "seats": seats,
    }


@app.get("/reservations/{departure_id}/{coach}/{seat}")
def view_reservation(departure_id: str, coach: int, seat: str):
    row = db.read_seat(departure_id, coach, seat)  # LOCAL_QUORUM
    if row is None:
        raise HTTPException(404, "No such seat")
    return _seat_dict(row)


@app.post("/reservations", status_code=201)
def book(req: BookRequest):
    for attempt in range(MAX_LWT_ATTEMPTS):
        try:
            rs = db.book_seat(req.departure_id, req.coach, req.seat_number,
                              req.user_id, req.user_name, req.user_email,
                              req.idempotency_key)
            row = rs.one()
            if row.applied:
                return {"status": "booked", "instance": INSTANCE_NAME,
                        "departure_id": req.departure_id, "coach": req.coach,
                        "seat_number": req.seat_number}

            # CAS failed — read the row back at LOCAL_SERIAL for the true state.
            seat = db.read_seat(req.departure_id, req.coach, req.seat_number,
                                serial=True)
            if seat is None:
                raise HTTPException(404, "No such seat")
            if seat.idempotency_key == req.idempotency_key:
                # This exact request already succeeded — idempotent retry.
                return {"status": "booked", "idempotent": True,
                        "instance": INSTANCE_NAME,
                        "departure_id": req.departure_id, "coach": req.coach,
                        "seat_number": req.seat_number}
            raise HTTPException(409, {
                "error": "seat_taken",
                "holder": {"user_id": seat.user_id, "user_name": seat.user_name},
            })

        except WriteTimeout:
            # Ambiguous: Paxos may or may not have committed. Read back the true
            # state — never blind-retry an LWT timeout.
            log.warning("WriteTimeout on book %s/%s/%s attempt %d",
                        req.departure_id, req.coach, req.seat_number, attempt)
            seat = db.read_seat(req.departure_id, req.coach, req.seat_number,
                                serial=True)
            if seat is not None and seat.idempotency_key == req.idempotency_key:
                return {"status": "booked", "recovered": True,
                        "instance": INSTANCE_NAME,
                        "departure_id": req.departure_id, "coach": req.coach,
                        "seat_number": req.seat_number}
            if seat is not None and seat.status == "booked":
                raise HTTPException(409, {
                    "error": "seat_taken",
                    "holder": {"user_id": seat.user_id, "user_name": seat.user_name},
                })
            # Still free and not ours: the round didn't commit — safe to retry.
            continue

        except TransientError as e:
            raise HTTPException(503, {"error": "unavailable", "detail": str(e)})

    raise HTTPException(503, {"error": "lwt_unresolved",
                             "detail": "write timeout did not resolve"})


@app.patch("/reservations/{departure_id}/{coach}/{seat}")
def edit(departure_id: str, coach: int, seat: str, req: EditRequest):
    """Edit a held seat — ownership-guarded LWT."""
    try:
        rs = db.edit_seat(departure_id, coach, seat, req.user_id,
                          req.user_name, req.user_email)
        row = rs.one()
        if row.applied:
            return {"status": "updated", "instance": INSTANCE_NAME}

        # CAS failed — read back to explain why (not booked vs. not yours).
        cur = db.read_seat(departure_id, coach, seat, serial=True)
        if cur is None:
            raise HTTPException(404, "No such seat")
        if cur.status != "booked":
            raise HTTPException(409, {"error": "not_booked"})
        if cur.user_id == req.user_id:
            # Condition failed but it's ours — idempotent success.
            return {"status": "updated", "idempotent": True}
        raise HTTPException(403, {"error": "not_your_reservation",
                                 "holder": {"user_id": cur.user_id}})
    except WriteTimeout:
        cur = db.read_seat(departure_id, coach, seat, serial=True)
        if cur is not None and cur.user_id == req.user_id \
                and cur.user_name == req.user_name \
                and cur.user_email == req.user_email:
            return {"status": "updated", "recovered": True}
        raise HTTPException(503, {"error": "write_timeout"})
    except TransientError as e:
        raise HTTPException(503, {"error": "unavailable", "detail": str(e)})


SUT_BASE_URL = os.environ.get("SUT_BASE_URL", "http://nginx:8000")


class TestParams(BaseModel):
    departure_id: str
    n: int = 50                       # ST1: number of identical fires
    coach: int = 1
    seat_number: str = "1A"
    users: int = 5                    # ST2: concurrent random users
    requests_per_user: int = 40       # ST2
    base_url: Optional[str] = None    # override SUT target


async def _run_test(run_id: str, st: str, p: TestParams):
    base = p.base_url or SUT_BASE_URL
    try:
        if st == "st1":
            metrics = await loadtest.run_st1(base, p.departure_id, p.coach,
                                             p.seat_number, p.n)
            verdict = verify.verify_st1(db, p.departure_id, p.coach, p.seat_number)
        elif st == "st2":
            metrics = await loadtest.run_st2(base, p.departure_id, p.users,
                                             p.requests_per_user)
            verdict = verify.verify_no_corruption(db, p.departure_id)
        elif st == "st3":
            metrics = await loadtest.run_st3(base, p.departure_id)
            verdict = verify.verify_st3(db, p.departure_id, metrics)
        else:
            db.save_run(run_id, st, "error",
                        json.dumps({"error": f"unknown test {st}"}))
            return
        db.save_run(run_id, st, "done",
                    json.dumps({"metrics": metrics, "verdict": verdict}))
    except Exception as e:  # noqa: BLE001
        log.exception("test run failed")
        db.save_run(run_id, st, "error", json.dumps({"error": str(e)}))


@app.post("/admin/tests/{st}")
async def launch_test(st: str, p: TestParams):
    if st not in ("st1", "st2", "st3"):
        raise HTTPException(400, "st must be st1, st2 or st3")
    run_id = uuid.uuid4().hex
    # Persist 'running' before returning so a poll on any instance finds it.
    db.save_run(run_id, st, "running", None)
    asyncio.create_task(_run_test(run_id, st, p))
    return {"run_id": run_id, "status": "running"}


@app.get("/admin/tests/{run_id}")
def get_test(run_id: str):
    row = db.load_run(run_id)
    if row is None:
        raise HTTPException(404, "no such run")
    out = {"status": row.status, "test": row.test}
    if row.result:
        out.update(json.loads(row.result))  # {metrics, verdict} or {error}
    return out


@app.post("/admin/departures/{departure_id}/reset")
def reset_departure(departure_id: str):
    n = seeder.reset_departure(db, departure_id)
    if n == 0:
        raise HTTPException(404, f"No departure '{departure_id}'")
    return {"departure_id": departure_id, "seats_reset": n}


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))


@app.get("/tests")
def tests_page():
    return FileResponse(os.path.join(STATIC_DIR, "tests.html"))


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
