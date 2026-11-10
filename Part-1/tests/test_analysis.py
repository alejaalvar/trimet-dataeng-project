"""Tests for the speed transformation in analysis.py.

The tests drive the pipeline the same way Pub/Sub does: each breadcrumb is
handed to analysis.callback() as a message, analysis.finish_batch() marks the
end of the batch, and whatever reached the database is inspected. PostgreSQL
and Pub/Sub are replaced with in-memory fakes, so nothing external is touched.

Rows captured from the fake database are tuples in the INSERT column order:
    (trip_id, vehicle_id, timestamp, latitude, longitude, speed, meters)

Run from the Part-1 directory:
    python3 -m unittest discover -s tests -v

The replay test reads a backup.py log (breadcrumbs_*.log.gz). By default it
uses the newest one in /home/alejalva/ (where backup.py writes them on the
backup VM), or, if there are none there, the newest one in Part-1/.

Running on the backup VM: nothing to do; the logs are found automatically.
Run this from the Part-1/
    cd <path-to-repo>/Part-1
    python3 -m unittest discover -s tests -v

Running anywhere else (the analysis VM, a laptop, ...): the logs only exist
on the backup VM, so first copy one finished .log.gz file into Part-1/, e.g.
    download the file from the VM directly or some other method

Copy a .log.gz, not a .log: a plain .log is a batch still being written.

To pick a specific file instead, set BREADCRUMB_LOG for that one command
(nothing sets it by default):
    BREADCRUMB_LOG=/path/to/file.log.gz python3 -m unittest discover -s tests -v

If no log is found, the replay test is skipped (not failed); the unit tests
still run.
"""

import glob
import gzip
import json
import logging
import os
import random
import sys
import tempfile
import types
import unittest
import warnings
from collections import defaultdict
from datetime import datetime, timedelta

# Same filter as analysis.py. It must run before _stub_missing_modules()
# below, because that is where google.cloud is first imported (and warns).
warnings.filterwarnings("ignore", message="You are using a Python version", category=FutureWarning)

PART1_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PART1_DIR)


def _stub_missing_modules():
    """Install stand-ins for psycopg2 / google-cloud-pubsub if not installed.

    The tests never use them; analysis.py only needs them to import.
    """
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        psycopg2 = types.ModuleType("psycopg2")
        extras = types.ModuleType("psycopg2.extras")
        extras.execute_values = None
        psycopg2.extras = extras
        sys.modules["psycopg2"] = psycopg2
        sys.modules["psycopg2.extras"] = extras

    try:
        from google.cloud import pubsub_v1  # noqa: F401
    except ImportError:
        google = sys.modules.get("google") or types.ModuleType("google")
        cloud = types.ModuleType("google.cloud")
        pubsub_v1 = types.ModuleType("google.cloud.pubsub_v1")
        cloud.pubsub_v1 = pubsub_v1
        google.cloud = cloud
        sys.modules["google"] = google
        sys.modules["google.cloud"] = cloud
        sys.modules["google.cloud.pubsub_v1"] = pubsub_v1


_stub_missing_modules()
import analysis  # noqa: E402

# --------------------------------------------------------------------------
# Fakes for Pub/Sub and PostgreSQL
# --------------------------------------------------------------------------


class FakeMessage:
    def __init__(self, data):
        self.data = data
        self.acked = False
        self.nacked = False

    def ack(self):
        self.acked = True

    def nack(self):
        self.nacked = True


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, values=None):
        self.rows.append(tuple(values))


class FakeConnection:
    def __init__(self):
        self.rows = []

    def cursor(self):
        return FakeCursor(self.rows)

    def close(self):
        pass


def _fake_execute_values(cur, sql, argslist, *args, **kwargs):
    cur.rows.extend(tuple(row) for row in argslist)


TRIP, VEHICLE, TIMESTAMP, LAT, LON, SPEED, METERS = range(7)


class PipelineTestCase(unittest.TestCase):
    """Sets up fakes and fresh pipeline state for every test."""

    def setUp(self):
        logging.disable(logging.CRITICAL)
        self.tmp = tempfile.TemporaryDirectory()
        self._saved = {
            "INVALID_DIR": analysis.INVALID_DIR,
            "conn": analysis.conn,
            "execute_values": getattr(analysis, "execute_values", None),
        }
        analysis.INVALID_DIR = self.tmp.name
        analysis.conn = FakeConnection()
        if hasattr(analysis, "execute_values"):
            analysis.execute_values = _fake_execute_values
        analysis.finish_batch()  # start from empty per-batch state
        self.messages = []

    def tearDown(self):
        analysis.finish_batch()
        analysis.INVALID_DIR = self._saved["INVALID_DIR"]
        analysis.conn = self._saved["conn"]
        if hasattr(analysis, "execute_values"):
            analysis.execute_values = self._saved["execute_values"]
        self.tmp.cleanup()
        logging.disable(logging.NOTSET)

    # -- helpers ------------------------------------------------------------

    def send(self, data):
        if isinstance(data, dict):
            data = json.dumps(data).encode("utf-8")
        message = FakeMessage(data)
        self.messages.append(message)
        analysis.callback(message)

    def run_batch(self, records):
        """Deliver records in the given order, end the batch, return DB rows."""
        for record in records:
            self.send(record)
        analysis.finish_batch()
        return list(analysis.conn.rows)

    def invalid_entries(self):
        entries = []
        for path in glob.glob(os.path.join(self.tmp.name, "invalid_data_*.json")):
            with open(path, encoding="utf-8") as file:
                entries.extend(json.load(file))
        return entries


BASE_DATE = "10NOV2026:00:00:00"
BASE_TIME = datetime(2026, 11, 10)
START = 30000  # ACT_TIME of the first breadcrumb in synthetic trips


def bc(trip, seconds, meters, vehicle=4208, lat=45.52, lon=-122.67):
    """A breadcrumb `seconds` after START, with the same fields as the API."""
    return {
        "EVENT_NO_TRIP": trip,
        "EVENT_NO_STOP": trip + 11,
        "OPD_DATE": BASE_DATE,
        "VEHICLE_ID": vehicle,
        "METERS": meters,
        "ACT_TIME": START + seconds,
        "GPS_LONGITUDE": lon,
        "GPS_LATITUDE": lat,
        "GPS_SATELLITES": 11.0,
        "GPS_HDOP": 0.9,
    }


def at(seconds):
    return BASE_TIME + timedelta(seconds=START + seconds)


def speeds_by_time(rows, trip=None):
    return {
        row[TIMESTAMP]: row[SPEED] for row in rows if trip is None or row[TRIP] == trip
    }


# --------------------------------------------------------------------------
# Layer 1: small synthetic trips with known answers
# --------------------------------------------------------------------------


class SpeedTransformationTests(PipelineTestCase):

    def test_in_order_basic_math(self):
        rows = self.run_batch([bc(1, 0, 0), bc(1, 10, 100), bc(1, 20, 250)])
        self.assertEqual(speeds_by_time(rows), {at(0): 0.0, at(10): 10.0, at(20): 15.0})

    def test_reversed_delivery_gives_same_speeds(self):
        rows = self.run_batch([bc(1, 20, 250), bc(1, 10, 100), bc(1, 0, 0)])
        self.assertEqual(speeds_by_time(rows), {at(0): 0.0, at(10): 10.0, at(20): 15.0})

    def test_shuffled_delivery_gives_same_speeds(self):
        rng = random.Random(436)
        trip, meters = [], 0
        for i in range(50):
            trip.append(bc(1, 5 * i, meters))
            meters += rng.randint(0, 80)  # 0-16 m/s, including stops

        analysis.conn = FakeConnection()
        expected = speeds_by_time(self.run_batch(trip))

        shuffles, wrong = 20, 0
        for _ in range(shuffles):
            shuffled = trip[:]
            rng.shuffle(shuffled)
            analysis.conn = FakeConnection()
            got = speeds_by_time(self.run_batch(shuffled))
            wrong += sum(got.get(ts) != speed for ts, speed in expected.items())
        total = shuffles * len(expected)
        self.assertEqual(
            wrong,
            0,
            f"{wrong}/{total} speeds differ from in-order "
            f"delivery across {shuffles} shuffles",
        )

    def test_first_breadcrumb_gets_zero_speed(self):
        rows = self.run_batch([bc(1, 10, 100), bc(1, 20, 250), bc(1, 0, 0)])
        self.assertEqual(speeds_by_time(rows)[at(0)], 0.0)

    def test_trips_are_computed_independently(self):
        # Trip 2's METERS reset to 0; it must never be diffed against trip 1.
        records = [
            bc(1, 0, 5000),
            bc(2, 0, 0),
            bc(1, 10, 5100),
            bc(2, 10, 50),
            bc(1, 20, 5250),
            bc(2, 20, 150),
        ]
        rows = self.run_batch(records)
        self.assertEqual(
            speeds_by_time(rows, trip=1), {at(0): 0.0, at(10): 10.0, at(20): 15.0}
        )
        self.assertEqual(
            speeds_by_time(rows, trip=2), {at(0): 0.0, at(10): 5.0, at(20): 10.0}
        )

    def test_duplicate_delivery_is_inserted_once(self):
        rows = self.run_batch(
            [bc(1, 0, 0), bc(1, 10, 100), bc(1, 10, 100), bc(1, 20, 250)]
        )
        self.assertEqual(len(rows), 3)
        self.assertEqual(speeds_by_time(rows), {at(0): 0.0, at(10): 10.0, at(20): 15.0})

    def test_decreasing_distance_is_rejected_not_stored_as_zero(self):
        rows = self.run_batch(
            [bc(1, 0, 0), bc(1, 10, 100), bc(1, 20, 90), bc(1, 30, 200)]
        )
        speeds = speeds_by_time(rows)
        self.assertNotIn(at(20), speeds)
        self.assertEqual(speeds[at(30)], 5.0)  # (200 - 100) / 20, vs last kept
        self.assertEqual(len(self.invalid_entries()), 1)

    def test_overspeed_is_rejected_and_next_uses_last_kept(self):
        # 1000 m in 10 s = 360 km/h -> A14; next record compares to t=0.
        rows = self.run_batch([bc(1, 0, 0), bc(1, 10, 1000), bc(1, 20, 100)])
        self.assertEqual(speeds_by_time(rows), {at(0): 0.0, at(20): 5.0})
        self.assertTrue(
            any("A14" in v for e in self.invalid_entries() for v in e["violations"])
        )

    def test_vehicle_change_within_trip_is_rejected(self):
        rows = self.run_batch([bc(1, 0, 0, vehicle=4208), bc(1, 10, 100, vehicle=9999)])
        self.assertEqual(speeds_by_time(rows), {at(0): 0.0})
        self.assertTrue(
            any("A10" in v for e in self.invalid_entries() for v in e["violations"])
        )

    def test_invalid_coordinates_go_to_invalid_file(self):
        rows = self.run_batch([bc(1, 0, 0), bc(1, 10, 100, lat=None)])
        self.assertEqual(len(rows), 1)
        self.assertEqual(len(self.invalid_entries()), 1)

    def test_every_message_is_acked(self):
        self.run_batch(
            [bc(1, 20, 250), bc(1, 0, 0), bc(1, 10, 100, lat=None), bc(1, 10, 100)]
        )
        self.assertTrue(all(m.acked and not m.nacked for m in self.messages))


# --------------------------------------------------------------------------
# Layer 2: replay a real day in the order Pub/Sub delivered it
# --------------------------------------------------------------------------

# Where backup.py writes its logs on the backup VM.
BACKUP_LOG_DIR = "/home/alejalva"


def _find_replay_log():
    """BREADCRUMB_LOG if set; otherwise the newest log in BACKUP_LOG_DIR,
    falling back to Part-1/ (e.g., a log copied to another machine)."""
    path = os.environ.get("BREADCRUMB_LOG")
    if path:
        return path
    for directory in (BACKUP_LOG_DIR, PART1_DIR):
        logs = sorted(glob.glob(os.path.join(directory, "breadcrumbs_*.log.gz")))
        if logs:
            return logs[-1]
    return None


REPLAY_LOG = _find_replay_log()


@unittest.skipUnless(
    REPLAY_LOG and os.path.exists(REPLAY_LOG),
    "no breadcrumbs_*.log.gz found (set BREADCRUMB_LOG)",
)
class RecordedDeliveryReplayTest(PipelineTestCase):

    def test_replay_recorded_pubsub_order(self):
        delivered = 0
        with gzip.open(REPLAY_LOG, "rb") as log:
            for line in log:
                line = line.rstrip(b"\n")
                if line:
                    self.send(line)
                    delivered += 1
        analysis.finish_batch()
        rows = analysis.conn.rows

        # Independent check, same as the SQL LAG() query: sort each trip's
        # stored rows by time and recompute speed from consecutive rows.
        by_trip = defaultdict(list)
        for row in rows:
            by_trip[row[TRIP]].append(row)

        mismatches = zeros = duplicates = 0
        total_distance = total_seconds = 0.0
        for trip_rows in by_trip.values():
            trip_rows.sort(key=lambda r: r[TIMESTAMP])
            previous = None
            for row in trip_rows:
                if previous is None:
                    expected = 0.0
                else:
                    dt = (row[TIMESTAMP] - previous[TIMESTAMP]).total_seconds()
                    if dt == 0:
                        duplicates += 1
                        continue
                    expected = (row[METERS] - previous[METERS]) / dt
                if abs(row[SPEED] - expected) > 1e-6:
                    mismatches += 1
                if row[SPEED] == 0:
                    zeros += 1
                previous = row
            if len(trip_rows) > 1:
                total_distance += trip_rows[-1][METERS] - trip_rows[0][METERS]
                total_seconds += (
                    trip_rows[-1][TIMESTAMP] - trip_rows[0][TIMESTAMP]
                ).total_seconds()

        stored = len(rows) or 1
        avg_stored = sum(r[SPEED] for r in rows) / stored
        avg_time_weighted = total_distance / total_seconds if total_seconds else 0.0
        invalid = len(self.invalid_entries())

        summary = (
            f"\n  replay log           : {os.path.basename(REPLAY_LOG)}"
            f"\n  messages delivered   : {delivered:,}"
            f"\n  rows stored          : {len(rows):,}"
            f"\n  rows invalid         : {invalid:,}"
            f"\n  speed == 0           : {zeros / stored:.1%}"
            f"\n  avg stored speed     : {avg_stored:.2f} m/s"
            f"\n  true avg (dist/time) : {avg_time_weighted:.2f} m/s"
            f"\n  wrong speeds         : {mismatches:,} ({mismatches / stored:.1%})"
            f"\n  duplicate rows       : {duplicates:,}"
        )
        print(summary, file=sys.stderr)

        self.assertTrue(
            all(m.acked and not m.nacked for m in self.messages),
            "some messages were not acked",
        )
        self.assertEqual(
            len(rows) + invalid,
            delivered,
            "every message should be stored or logged as invalid",
        )
        self.assertEqual(duplicates, 0, summary)
        self.assertEqual(mismatches, 0, summary)
        self.assertLess(zeros / stored, 0.10, summary)


if __name__ == "__main__":
    unittest.main()
