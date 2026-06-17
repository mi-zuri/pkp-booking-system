"""ST1 — same request, very fast. One client fires the identical booking
(same idempotency key) repeatedly: ~all 201s, exactly one booking in the DB."""
import os
import uuid

from locust import HttpUser, task, constant

DEP = os.environ.get("DEP", "TEST_POZ_WAW_RACE")
COACH = int(os.environ.get("COACH", "1"))
SEAT = os.environ.get("SEAT", "1A")
# One fixed key for the whole run — makes this an idempotency test, not a contention test.
IDEM = os.environ.get("IDEM", "st1-" + uuid.uuid4().hex)


class IdempotentStorm(HttpUser):
    wait_time = constant(0)

    @task
    def book_same_seat(self):
        with self.client.post("/reservations", json={
            "departure_id": DEP, "coach": COACH, "seat_number": SEAT,
            "user_id": "st1-user", "user_name": "ST1 User",
            "user_email": "st1@example.com", "idempotency_key": IDEM,
        }, catch_response=True) as resp:
            # 201 (won or idempotent retry) is success; anything else is a fail.
            if resp.status_code == 201:
                resp.success()
            else:
                resp.failure(f"unexpected {resp.status_code}: {resp.text[:120]}")
