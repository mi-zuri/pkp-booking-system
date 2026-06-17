"""ST2 — multiple random clients doing book/view/edit over the HTTP path.
No corruption afterward; contention surfaces as 409s, never as a hang."""
import os
import uuid
import random

from locust import HttpUser, task, between

DEP = os.environ.get("DEP", "TEST_POZ_WAW_RACE")


class RandomPassenger(HttpUser):
    wait_time = between(0.0, 0.2)

    def on_start(self):
        self.user_id = "u-" + uuid.uuid4().hex[:8]
        # Cache the seat list once.
        r = self.client.get(f"/departures/{DEP}/seats")
        self.seats = [(s["coach"], s["seat_number"]) for s in r.json().get("seats", [])]
        self.mine = []

    @task(7)
    def book(self):
        if not self.seats:
            return
        coach, seat = random.choice(self.seats)
        with self.client.post("/reservations", json={
            "departure_id": DEP, "coach": coach, "seat_number": seat,
            "user_id": self.user_id, "user_name": "Random",
            "user_email": f"{self.user_id}@example.com",
            "idempotency_key": "st2-" + uuid.uuid4().hex,
        }, name="POST /reservations", catch_response=True) as resp:
            if resp.status_code in (201, 409):  # both are expected outcomes
                resp.success()
                if resp.status_code == 201:
                    self.mine.append((coach, seat))
            else:
                resp.failure(f"unexpected {resp.status_code}")

    @task(2)
    def view(self):
        if not self.seats:
            return
        coach, seat = random.choice(self.seats)
        self.client.get(f"/reservations/{DEP}/{coach}/{seat}",
                        name="GET /reservations/{seat}")

    @task(1)
    def edit(self):
        if not self.mine:
            return
        coach, seat = random.choice(self.mine)
        self.client.patch(f"/reservations/{DEP}/{coach}/{seat}", json={
            "user_id": self.user_id, "user_name": "Random Edited",
            "user_email": f"{self.user_id}@example.com",
        }, name="PATCH /reservations/{seat}")
