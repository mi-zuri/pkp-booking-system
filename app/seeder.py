"""Create departures by pre-seeding one 'free' row per seat, so the booking
path stays a clean UPDATE ... IF status='free'. Also defines the fixed demo
departures auto-seeded on startup."""
import string
import logging

log = logging.getLogger("seeder")

# Each demo departure is a single wagon (coaches=1). Route/time is static
# reference data and lives here; only the mutable seat rows live in Cassandra.
DEFAULT_DEPARTURES = [
    {
        "departure_id": "IC3501_POZ_WAW_2026-06-20",
        "train": "IC 3501", "origin": "Poznań Główny",
        "destination": "Warszawa Centralna", "departs": "2026-06-20 08:15",
        "rows": 20, "seats_per_row": 4, "for_tests": False,
    },
    {
        "departure_id": "IC5402_POZ_KRK_2026-06-20",
        "train": "IC 5402", "origin": "Poznań Główny",
        "destination": "Kraków Główny", "departs": "2026-06-20 09:40",
        "rows": 20, "seats_per_row": 4, "for_tests": False,
    },
    {
        "departure_id": "TLK8100_POZ_GDA_2026-06-20",
        "train": "TLK 8100", "origin": "Poznań Główny",
        "destination": "Gdańsk Główny", "departs": "2026-06-20 11:05",
        "rows": 20, "seats_per_row": 4, "for_tests": False,
    },
    {
        "departure_id": "IC2200_POZ_WRO_2026-06-20",
        "train": "IC 2200", "origin": "Poznań Główny",
        "destination": "Wrocław Główny", "departs": "2026-06-20 13:20",
        "rows": 20, "seats_per_row": 4, "for_tests": False,
    },
    {
        "departure_id": "IC1700_POZ_SZC_2026-06-20",
        "train": "IC 1700", "origin": "Poznań Główny",
        "destination": "Szczecin Główny", "departs": "2026-06-20 15:50",
        "rows": 20, "seats_per_row": 4, "for_tests": False,
    },
    # Dedicated to the /tests page so load runs never clobber demo bookings.
    {
        "departure_id": "TEST_POZ_WAW_RACE",
        "train": "IC 9999", "origin": "Poznań Główny",
        "destination": "Warszawa (load-test)", "departs": "2026-06-20 23:59",
        "rows": 20, "seats_per_row": 4, "for_tests": True,
    },
]


def find_departure(departure_id):
    return next((d for d in DEFAULT_DEPARTURES
                 if d["departure_id"] == departure_id), None)


def public_departures():
    """Departures shown in the booking UI (excludes the load-test one)."""
    return [d for d in DEFAULT_DEPARTURES if not d["for_tests"]]


def seat_labels(rows_per_coach, seats_per_row):
    """Yield ('1A', '1B', ...) seat labels: <row><letter>."""
    letters = string.ascii_uppercase[:seats_per_row]
    for row in range(1, rows_per_coach + 1):
        for letter in letters:
            yield f"{row}{letter}"


def plan_seats(coaches, rows_per_coach, seats_per_row):
    """Return the full list of (coach, seat_number) for a departure."""
    seats = []
    for coach in range(1, coaches + 1):
        for label in seat_labels(rows_per_coach, seats_per_row):
            seats.append((coach, label))
    return seats


def seed_departure(db, departure_id, coaches=1, rows_per_coach=20, seats_per_row=4):
    """Pre-seed every seat of a departure as free. Returns the seat count."""
    seats = plan_seats(coaches, rows_per_coach, seats_per_row)
    for coach, seat_number in seats:
        db.seed_seat(departure_id, coach, seat_number)
    return len(seats)


def seed_defaults(db):
    """Seed the fixed demo departures on startup, skipping any that already
    have seats (idempotent across instances and restarts)."""
    for d in DEFAULT_DEPARTURES:
        if db.read_seatmap(d["departure_id"], quorum=True):
            continue
        n = seed_departure(db, d["departure_id"], coaches=1,
                           rows_per_coach=d["rows"], seats_per_row=d["seats_per_row"])
        log.info("Seeded departure %s (%d seats)", d["departure_id"], n)


def reset_departure(db, departure_id):
    """Re-seed all existing seats of a departure back to free."""
    rows = db.read_seatmap(departure_id, quorum=True)
    for r in rows:
        db.reset_seat(departure_id, r.coach, r.seat_number)
    return len(rows)
