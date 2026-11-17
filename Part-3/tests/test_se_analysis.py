"""Tests for se_analysis.py.

PostgreSQL and Pub/Sub are replaced with fakes, so nothing external is
touched. Records are shaped like the ones se_publisher.py sends: every page
column as a string, trip_number holding the PDX_TRIP id, and service_date.

Run from the Part-3 directory:
    python3 -m unittest discover -s tests -v
"""

import glob
import json
import os
import signal
import sys
import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest import mock

import psycopg2

PART3_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PART3_DIR)

import se_analysis  # noqa: E402
import se_publisher  # noqa: E402

FIXTURE_PATH = os.path.join(PART3_DIR, "tests", "fixtures", "stop_events_4001.html")


def valid_record(**changes):
    """Return the first StopEvent of the real fixture page, with `changes` applied."""
    record = {
        "vehicle_number": "4001",
        "leave_time": "24902",
        "train": "5467",
        "route_number": "54",
        "direction": "1",
        "service_key": "S",
        "trip_number": "242538843",
        "stop_time": "24840",
        "arrive_time": "24902",
        "dwell": "0",
        "location_id": "9654",
        "door": "2",
        "lift": "0",
        "ons": "1",
        "offs": "0",
        "estimated_load": "1",
        "maximum_speed": "0",
        "train_mileage": "3.1",
        "pattern_distance": "0",
        "location_distance": "0",
        "x_coordinate": "7611970.33",
        "y_coordinate": "673458.61",
        "data_source": "2",
        "schedule_status": "5",
        "service_date": "2023-01-21",
    }
    record.update(changes)
    return record


def failed_assertions(record):
    """Return the set of assertion codes (e.g. {"A1"}) a record violates."""
    return {v["assertion"] for v in se_analysis.validate_record(record)}


class ValidateRecordTests(unittest.TestCase):
    """validate_record() enforces assertions A1-A10 from spec section 6.2."""

    def assert_only_violates(self, assertion, **changes):
        """Check that applying `changes` to a valid record breaks exactly `assertion`."""
        self.assertEqual(failed_assertions(valid_record(**changes)), {assertion})

    def test_valid_record_has_no_violations(self):
        self.assertEqual(se_analysis.validate_record(valid_record()), [])

    def test_every_record_on_the_real_page_is_valid(self):
        # Checks the publisher and this subscriber agree on the record shape.
        with open(FIXTURE_PATH, encoding="utf-8") as file:
            records, _ = se_publisher.parse_stop_events(file.read())

        self.assertEqual(len(records), 1039)
        failures = [
            (record["trip_number"], record["arrive_time"], failed_assertions(record))
            for record in records
            if failed_assertions(record)
        ]
        self.assertEqual(failures, [])

    def test_a1_arrive_time_must_be_an_integer_in_range(self):
        for value in (None, "", "abc", "12.5", "-1", "108001"):
            with self.subTest(arrive_time=value):
                self.assert_only_violates("A1", arrive_time=value)

    def test_a1_accepts_the_range_limits(self):
        for value in ("0", "108000"):
            with self.subTest(arrive_time=value):
                self.assertEqual(
                    failed_assertions(valid_record(arrive_time=value, leave_time=value)),
                    set(),
                )

    def test_a2_leave_time_before_arrive_time_is_invalid(self):
        self.assert_only_violates("A2", arrive_time="24902", leave_time="24901")

    def test_a2_missing_leave_time_is_allowed(self):
        self.assertEqual(failed_assertions(valid_record(leave_time=None)), set())

    def test_a3_vehicle_number_must_be_a_positive_integer(self):
        for value in (None, "0", "-4001", "abc"):
            with self.subTest(vehicle_number=value):
                self.assert_only_violates("A3", vehicle_number=value)

    def test_a4_trip_number_must_be_a_positive_integer(self):
        for value in (None, "0", "abc"):
            with self.subTest(trip_number=value):
                self.assert_only_violates("A4", trip_number=value)

    def test_a5_service_date_must_be_a_real_yyyy_mm_dd_date(self):
        for value in (None, "", "2023-02-30", "01/21/2023"):
            with self.subTest(service_date=value):
                self.assert_only_violates("A5", service_date=value)

    def test_a6_stop_time_must_be_null_or_in_range(self):
        for value in ("abc", "-1", "108001"):
            with self.subTest(stop_time=value):
                self.assert_only_violates("A6", stop_time=value)

    def test_a6_accepts_null_and_the_range_limits(self):
        for value in (None, "0", "108000"):
            with self.subTest(stop_time=value):
                self.assertEqual(failed_assertions(valid_record(stop_time=value)), set())

    def test_a7_leave_time_must_be_null_or_in_range(self):
        for value in ("abc", "108001"):
            with self.subTest(leave_time=value):
                self.assert_only_violates("A7", leave_time=value)

    def test_a7_accepts_the_range_limits(self):
        # arrive_time 0 so that neither limit can trip A2 (leave >= arrive).
        for value in ("0", "108000"):
            with self.subTest(leave_time=value):
                self.assertEqual(
                    failed_assertions(valid_record(arrive_time="0", leave_time=value)),
                    set(),
                )

    def test_a8_coordinates_must_exist_and_convert_to_gps(self):
        for changes in (
            {"x_coordinate": None},
            {"y_coordinate": ""},
            {"x_coordinate": "abc"},
            {"y_coordinate": "inf"},
        ):
            with self.subTest(**changes):
                self.assert_only_violates("A8", **changes)

    def test_a8_coordinates_that_fail_to_convert_are_invalid(self):
        # pyproj reports a failed conversion as inf instead of raising.
        for lat, lon in ((float("inf"), -122.6), (45.5, float("inf"))):
            with self.subTest(lat=lat, lon=lon):
                with mock.patch.object(
                    se_analysis, "state_plane_to_latlon", return_value=(lat, lon)
                ):
                    self.assert_only_violates("A8")

    def test_a9_passenger_counts_must_be_null_or_non_negative(self):
        for field in ("ons", "offs", "estimated_load"):
            with self.subTest(field=field):
                self.assert_only_violates("A9", **{field: "-1"})
                self.assertEqual(failed_assertions(valid_record(**{field: None})), set())

    def test_a10_dwell_must_be_null_or_non_negative(self):
        self.assert_only_violates("A10", dwell="-5")
        self.assertEqual(failed_assertions(valid_record(dwell=None)), set())


class TransformRecordTests(unittest.TestCase):
    """transform_record() applies Transformations 1 and 2 from spec section 6.2."""

    def test_times_become_datetimes_on_the_service_date(self):
        row = se_analysis.transform_record(valid_record())

        self.assertEqual(row["arrive_time"], datetime(2023, 1, 21, 6, 55, 2))
        self.assertEqual(row["stop_time"], datetime(2023, 1, 21, 6, 54, 0))
        self.assertEqual(row["leave_time"], datetime(2023, 1, 21, 6, 55, 2))

    def test_times_use_the_records_own_service_date(self):
        row = se_analysis.transform_record(valid_record(service_date="2024-03-05"))

        self.assertEqual(row["arrive_time"], datetime(2024, 3, 5, 6, 55, 2))

    def test_time_past_midnight_rolls_over_to_the_next_day(self):
        # 89,978 s (24:59:38) is the last leave_time on the real page; on
        # New Year's Eve the rollover also changes the month and year.
        row = se_analysis.transform_record(
            valid_record(
                service_date="2023-12-31",
                arrive_time="89978",
                stop_time="89978",
                leave_time="89978",
            )
        )

        for column in ("arrive_time", "stop_time", "leave_time"):
            with self.subTest(column=column):
                self.assertEqual(row[column], datetime(2024, 1, 1, 0, 59, 38))

    def test_missing_stop_and_leave_times_become_none(self):
        row = se_analysis.transform_record(valid_record(stop_time=None, leave_time=""))

        self.assertIsNone(row["stop_time"])
        self.assertIsNone(row["leave_time"])

    def test_state_plane_coordinates_become_gps(self):
        # The professor's example point, "expected output near lat=45.494,
        # lon=-122.591". His scaffold gives 45.505 with pyproj 3.8, so "near"
        # is checked with a tolerance; a swapped axis or wrong projection is
        # off by degrees.
        row = se_analysis.transform_record(
            valid_record(x_coordinate="7666298.96", y_coordinate="677239.73")
        )

        self.assertAlmostEqual(row["gps_latitude"], 45.494, delta=0.02)
        self.assertAlmostEqual(row["gps_longitude"], -122.591, delta=0.02)

    def test_real_stop_converts_back_to_its_original_gps(self):
        # The page's x/y for this stop convert to 45.4908600018, -122.8015400000:
        # TriMet evidently derived x/y from these 5-decimal GPS values, which
        # makes them an independent check of the projection.
        row = se_analysis.transform_record(valid_record())

        self.assertAlmostEqual(row["gps_latitude"], 45.49086, delta=1e-6)
        self.assertAlmostEqual(row["gps_longitude"], -122.80154, delta=1e-6)

    def test_result_has_exactly_the_table_columns(self):
        # x/y are replaced by GPS and service_date only feeds the timestamps,
        # so none of them may reach the INSERT.
        row = se_analysis.transform_record(valid_record())

        self.assertEqual(set(row), set(se_analysis.DB_COLUMNS))

    def test_fields_become_the_column_types(self):
        row = se_analysis.transform_record(valid_record())

        # assertIs on the type, because 4001 == 4001.0 would hide a column
        # parsed as the wrong type.
        self.assertEqual(row["vehicle_number"], 4001)
        self.assertIs(type(row["vehicle_number"]), int)
        self.assertEqual(row["trip_number"], 242538843)
        self.assertIs(type(row["trip_number"]), int)
        self.assertEqual(row["train_mileage"], 3.1)
        self.assertIs(type(row["train_mileage"]), float)
        self.assertEqual(row["service_key"], "S")

    def test_empty_optional_fields_become_none(self):
        row = se_analysis.transform_record(
            valid_record(route_number="", train_mileage=None, service_key="")
        )

        self.assertIsNone(row["route_number"])
        self.assertIsNone(row["train_mileage"])
        self.assertIsNone(row["service_key"])


class FakeCursor:
    """Records INSERTs in its connection; raises the connection's `error` if set."""

    def __init__(self, connection):
        self.connection = connection
        self.rowcount = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, values):
        if self.connection.error is not None:
            raise self.connection.error
        self.connection.inserts.append((sql, values))
        self.rowcount = self.connection.rowcount


class FakeConnection:
    """Stands in for a psycopg2 connection.

    rowcount is what each INSERT reports: 1 stored, 0 skipped by ON CONFLICT.
    """

    def __init__(self):
        self.inserts = []
        self.rowcount = 1
        self.error = None
        self.closed = 0

    def cursor(self):
        return FakeCursor(self)

    def close(self):
        self.closed = 1


class AnalysisTestCase(unittest.TestCase):
    """Gives each test a fake database, an empty invalid-records dir and fresh stats."""

    def setUp(self):
        """Replace the database connection, INVALID_DIR and stats for one test."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.invalid_dir = Path(tmp.name)

        dir_patcher = mock.patch.object(se_analysis, "INVALID_DIR", self.invalid_dir)
        dir_patcher.start()
        self.addCleanup(dir_patcher.stop)

        self.db = FakeConnection()
        conn_patcher = mock.patch.object(se_analysis, "conn", self.db)
        conn_patcher.start()
        self.addCleanup(conn_patcher.stop)

        # stats is module state that callback() fills, so each test starts
        # from (and leaves behind) a clean copy.
        original_stats = se_analysis.stats
        se_analysis.stats = se_analysis.reset_stats()
        self.addCleanup(setattr, se_analysis, "stats", original_stats)

    def send(self, data):
        """Deliver one message to callback() and return it; a dict is sent as JSON."""
        if isinstance(data, dict):
            data = json.dumps(data).encode("utf-8")
        msg = mock.Mock(data=data)
        se_analysis.callback(msg)
        return msg

    def invalid_entries(self):
        """Return every entry written to the invalid-records files."""
        entries = []
        for path in glob.glob(str(self.invalid_dir / "se_invalid_data_*.jsonl")):
            with open(path, encoding="utf-8") as file:
                entries.extend(json.loads(line) for line in file)
        return entries

    def messages(self, logs):
        """Return the formatted messages captured by an assertLogs block."""
        return [record.getMessage() for record in logs.records]

    def inserted_value(self, column, insert=0):
        """Return the value sent for `column` in the given INSERT."""
        _, values = self.db.inserts[insert]
        return values[se_analysis.DB_COLUMNS.index(column)]


class CallbackTests(AnalysisTestCase):
    """callback() loads valid StopEvents and sets the rest aside."""

    def test_valid_record_is_inserted_and_acked(self):
        msg = self.send(valid_record())

        self.assertEqual(len(self.db.inserts), 1)
        self.assertEqual(self.db.inserts[0][0], se_analysis.INSERT_SQL)
        self.assertEqual(self.inserted_value("trip_number"), 242538843)
        self.assertEqual(self.inserted_value("arrive_time"), datetime(2023, 1, 21, 6, 55, 2))
        self.assertAlmostEqual(self.inserted_value("gps_latitude"), 45.49086, delta=1e-6)
        msg.ack.assert_called_once()
        msg.nack.assert_not_called()
        self.assertEqual(se_analysis.stats["inserted_count"], 1)

    def test_invalid_record_is_saved_with_its_violations_and_not_inserted(self):
        record = valid_record(arrive_time="abc")

        with self.assertLogs(level="WARNING"):
            msg = self.send(record)

        self.assertEqual(self.db.inserts, [])
        [entry] = self.invalid_entries()
        self.assertEqual(entry["record"], record)
        self.assertEqual([v["assertion"] for v in entry["violations"]], ["A1"])
        msg.ack.assert_called_once()
        msg.nack.assert_not_called()
        self.assertEqual(se_analysis.stats["invalid_count"], 1)
        self.assertEqual(se_analysis.stats["received_count"], 1)

    def test_malformed_messages_are_saved_and_acked(self):
        # Not JSON, JSON that is not an object, and bytes that are not UTF-8.
        for count, data in enumerate((b"not json", b"[1, 2]", b"\xff\xfe"), start=1):
            with self.subTest(data=data):
                with self.assertLogs(level="WARNING"):
                    msg = self.send(data)

                entries = self.invalid_entries()
                self.assertEqual(len(entries), count)
                self.assertEqual(entries[-1]["violations"][0]["assertion"], "FORMAT")
                msg.ack.assert_called_once()
                msg.nack.assert_not_called()

        self.assertEqual(self.db.inserts, [])
        self.assertEqual(se_analysis.stats["invalid_count"], 3)

    def test_duplicate_is_counted_and_acked(self):
        # ON CONFLICT ... DO NOTHING reports 0 rows for an already-stored stop.
        self.db.rowcount = 0

        msg = self.send(valid_record())

        self.assertEqual(len(self.db.inserts), 1)
        msg.ack.assert_called_once()
        msg.nack.assert_not_called()
        self.assertEqual(self.invalid_entries(), [])
        self.assertEqual(se_analysis.stats["inserted_count"], 0)
        self.assertEqual(se_analysis.stats["duplicate_count"], 1)

    def test_value_rejected_by_the_table_is_saved_and_acked(self):
        # Retrying cannot fix a value the column rejects, so it must not be
        # nacked into an endless redelivery loop.
        self.db.error = psycopg2.DataError("smallint out of range")

        with self.assertLogs(level="WARNING"):
            msg = self.send(valid_record())

        [entry] = self.invalid_entries()
        self.assertEqual(entry["violations"][0]["assertion"], "DATABASE")
        msg.ack.assert_called_once()
        msg.nack.assert_not_called()
        self.assertEqual(se_analysis.stats["invalid_count"], 1)
        self.assertEqual(se_analysis.stats["received_count"], 1)
        self.assertEqual(se_analysis.stats["inserted_count"], 0)
        self.assertEqual(se_analysis.stats["duplicate_count"], 0)

    def test_constraint_violation_is_saved_and_acked(self):
        self.db.error = psycopg2.IntegrityError("null value in column violates not-null")

        with self.assertLogs(level="WARNING"):
            msg = self.send(valid_record())

        [entry] = self.invalid_entries()
        self.assertEqual(entry["violations"][0]["assertion"], "DATABASE")
        msg.ack.assert_called_once()
        msg.nack.assert_not_called()

    def test_lost_connection_is_nacked_and_the_next_message_reconnects(self):
        self.db.error = psycopg2.OperationalError("server closed the connection")

        with self.assertLogs(level="ERROR"):
            first = self.send(valid_record())

        first.nack.assert_called_once()
        first.ack.assert_not_called()
        self.assertEqual(self.db.closed, 1)
        # Not counted yet: Pub/Sub redelivers it, and it is counted then.
        self.assertEqual(se_analysis.stats["received_count"], 0)

        new_db = FakeConnection()
        credentials = {"DB_USER": "test", "DB_PASSWORD": "test"}
        with mock.patch.dict(os.environ, credentials), mock.patch.object(
            se_analysis.psycopg2, "connect", return_value=new_db
        ):
            second = self.send(valid_record())

        self.assertEqual(len(new_db.inserts), 1)
        second.ack.assert_called_once()
        self.assertEqual(se_analysis.stats["received_count"], 1)

    def test_unexpected_validation_error_is_saved_and_acked(self):
        # An error that depends only on the record would repeat on every
        # redelivery, so the record is set aside instead of nacked.
        with mock.patch.object(
            se_analysis, "state_plane_to_latlon", side_effect=RuntimeError("PROJ failure")
        ):
            with self.assertLogs(level="WARNING"):
                msg = self.send(valid_record())

        [entry] = self.invalid_entries()
        self.assertEqual(entry["violations"][0]["assertion"], "TRANSFORM")
        msg.ack.assert_called_once()
        msg.nack.assert_not_called()

    def test_unexpected_transformation_error_is_saved_and_acked(self):
        with mock.patch.object(
            se_analysis, "transform_record", side_effect=RuntimeError("unexpected")
        ):
            with self.assertLogs(level="WARNING"):
                msg = self.send(valid_record())

        [entry] = self.invalid_entries()
        self.assertEqual(entry["violations"][0]["assertion"], "TRANSFORM")
        msg.ack.assert_called_once()
        msg.nack.assert_not_called()

    def test_failure_to_save_an_invalid_record_is_nacked(self):
        # INVALID_DIR points at a regular file, so the invalid file cannot be
        # written; the record must be redelivered rather than lost.
        blocker = self.invalid_dir / "not_a_dir"
        blocker.write_text("")

        with mock.patch.object(se_analysis, "INVALID_DIR", blocker):
            with self.assertLogs(level="ERROR"):
                msg = self.send(valid_record(arrive_time="abc"))

        msg.nack.assert_called_once()
        msg.ack.assert_not_called()
        # Not counted yet: Pub/Sub redelivers it, and it is counted then.
        self.assertEqual(se_analysis.stats["invalid_count"], 0)
        self.assertEqual(se_analysis.stats["received_count"], 0)
        self.assertEqual(se_analysis.stats["vehicle_ids"], set())

    def test_vehicles_and_trips_are_counted_for_valid_and_invalid_records(self):
        # The spec expects the subscriber's vehicle count to match the
        # publisher's, which counts every record it sent.
        self.send(valid_record(vehicle_number="4001", trip_number="111"))
        with self.assertLogs(level="WARNING"):
            self.send(valid_record(vehicle_number="4002", trip_number="222", arrive_time="abc"))

        self.assertEqual(se_analysis.stats["vehicle_ids"], {"4001", "4002"})
        self.assertEqual(se_analysis.stats["trip_ids"], {"111", "222"})
        self.assertEqual(se_analysis.stats["received_count"], 2)


class StatisticsTests(AnalysisTestCase):
    """The batch statistics spec section 8 asks for, logged when a batch ends."""

    def finish_and_capture_logs(self):
        """Run finish_batch() and return the messages it logged."""
        with self.assertLogs(level="INFO") as logs:
            se_analysis.finish_batch()
        return self.messages(logs)

    def test_logs_counts_vehicles_trips_and_throughput(self):
        # Messages arrive at 0, 4, 8 and 12 s: two stored, one duplicate and
        # one invalid, from three vehicles on three trips.
        with mock.patch.object(se_analysis.time, "monotonic", side_effect=[0.0, 4.0, 8.0, 12.0]):
            self.send(valid_record(vehicle_number="4001", trip_number="111"))
            self.send(valid_record(vehicle_number="4002", trip_number="222"))
            self.db.rowcount = 0
            self.send(valid_record(vehicle_number="4001", trip_number="111"))
            with self.assertLogs(level="WARNING"):
                self.send(valid_record(vehicle_number="4003", trip_number="333", arrive_time="abc"))

        logged = self.finish_and_capture_logs()

        expected = [
            "Total messages: 4",
            "Inserted records: 2",
            "Duplicate records skipped: 1",
            "Invalid records: 1",
            "Unique vehicles: 3",
            "Unique trips: 3",
            "Elapsed receive time: 12.000 seconds",
            "Throughput: 0.33 messages/second",
        ]
        for line in expected:
            with self.subTest(line=line):
                self.assertIn(line, logged)

    def test_logs_first_and_last_receive_times(self):
        first = datetime(2026, 11, 17, 9, 10, 0)
        last = datetime(2026, 11, 17, 9, 40, 0)
        se_analysis.record_receive(first, 0.0, valid_record())
        se_analysis.record_receive(last, 1800.0, valid_record())

        logged = self.finish_and_capture_logs()

        self.assertIn("First message received: 2026-11-17T09:10:00", logged)
        self.assertIn("Last message received: 2026-11-17T09:40:00", logged)

    def test_single_message_gives_zero_throughput(self):
        # One message means zero elapsed time, which must not divide by zero.
        self.send(valid_record())

        logged = self.finish_and_capture_logs()

        self.assertIn("Throughput: 0.00 messages/second", logged)

    def test_finishing_resets_the_statistics(self):
        self.send(valid_record())

        self.finish_and_capture_logs()

        self.assertEqual(se_analysis.stats, se_analysis.reset_stats())

    def test_finishing_without_messages_logs_nothing(self):
        with self.assertNoLogs(level="INFO"):
            se_analysis.finish_batch()


class FinishIfIdleTests(AnalysisTestCase):
    """finish_if_idle() ends the batch only after IDLE_SECONDS without a message."""

    def receive_at(self, monotonic_seconds):
        """Deliver one valid StopEvent as if it arrived at the given monotonic time."""
        with mock.patch.object(se_analysis.time, "monotonic", return_value=monotonic_seconds):
            self.send(valid_record())

    def check_idle_at(self, monotonic_seconds):
        """Run finish_if_idle() as if the clock read the given monotonic time."""
        with mock.patch.object(se_analysis.time, "monotonic", return_value=monotonic_seconds):
            se_analysis.finish_if_idle()

    def test_batch_is_not_finished_before_idle_seconds(self):
        self.receive_at(0.0)

        with self.assertNoLogs(level="INFO"):
            self.check_idle_at(se_analysis.IDLE_SECONDS - 1)

        self.assertEqual(se_analysis.stats["received_count"], 1)

    def test_batch_is_finished_once_idle_seconds_have_passed(self):
        self.receive_at(0.0)

        with self.assertLogs(level="INFO") as logs:
            self.check_idle_at(se_analysis.IDLE_SECONDS)

        self.assertIn("Total messages: 1", self.messages(logs))
        self.assertEqual(se_analysis.stats["received_count"], 0)

    def test_idle_check_without_messages_does_nothing(self):
        with self.assertNoLogs(level="INFO"):
            self.check_idle_at(se_analysis.IDLE_SECONDS * 10)


class ShutdownTests(AnalysisTestCase):
    """Stopping main(), by Ctrl+C or systemd's SIGTERM, logs the batch and closes the database."""

    def setUp(self):
        """Replace Pub/Sub, signal registration, the checker thread and log setup with mocks."""
        super().setUp()

        # The pull's first result() call is interrupted, as a stop signal
        # would; the second (waiting for running callbacks) returns.
        client_patcher = mock.patch.object(se_analysis.pubsub_v1, "SubscriberClient")
        self.client_class = client_patcher.start()
        self.addCleanup(client_patcher.stop)
        self.client = self.client_class.return_value
        self.streaming_pull = self.client.subscribe.return_value
        self.streaming_pull.result.side_effect = [KeyboardInterrupt, None]

        # Registering real handlers would change how this test process
        # itself responds to Ctrl+C and SIGTERM.
        signal_patcher = mock.patch.object(se_analysis.signal, "signal")
        self.register_signal = signal_patcher.start()
        self.addCleanup(signal_patcher.stop)

        # A real checker thread would outlive the test and keep calling
        # finish_if_idle() against later tests' state.
        thread_patcher = mock.patch.object(se_analysis, "Thread")
        thread_patcher.start()
        self.addCleanup(thread_patcher.stop)

        # basicConfig would attach a handler to the root logger for every
        # later test.
        logging_patcher = mock.patch.object(se_analysis.logging, "basicConfig")
        logging_patcher.start()
        self.addCleanup(logging_patcher.stop)

        # main() sets stop_event on the way out; later tests need it cleared.
        self.addCleanup(se_analysis.stop_event.clear)

    def run_main(self):
        """Run main() until its simulated stop and return the messages it logged."""
        with self.assertLogs(level="INFO") as logs:
            se_analysis.main()
        return self.messages(logs)

    def test_stop_signal_raises_keyboard_interrupt_and_ignores_repeats(self):
        with self.assertRaises(KeyboardInterrupt):
            se_analysis.handle_stop_signal(signal.SIGTERM, None)

        self.register_signal.assert_any_call(signal.SIGTERM, signal.SIG_IGN)
        self.register_signal.assert_any_call(signal.SIGINT, signal.SIG_IGN)

    def test_main_registers_the_handler_for_sigterm_and_ctrl_c(self):
        self.run_main()

        self.register_signal.assert_any_call(signal.SIGTERM, se_analysis.handle_stop_signal)
        self.register_signal.assert_any_call(signal.SIGINT, se_analysis.handle_stop_signal)

    def test_stopping_main_logs_the_batch_and_closes_the_database(self):
        self.send(valid_record())

        logged = self.run_main()

        self.assertIn("Total messages: 1", logged)
        self.assertEqual(se_analysis.stats, se_analysis.reset_stats())
        self.assertEqual(self.db.closed, 1)

    def test_client_is_closed_only_after_running_callbacks_finish(self):
        # Closing first would drop the acks of callbacks still running.
        self.run_main()

        # The pull is cancelled, then waited on (result() returns only once
        # running callbacks finish), and only then is the client closed. Log
        # formatting also records unrelated calls, so only these are kept.
        call_names = [
            name for name, args, kwargs in self.client.mock_calls
            if name in ("subscribe().result", "subscribe().cancel", "close")
        ]
        self.assertEqual(
            call_names,
            ["subscribe().result", "subscribe().cancel", "subscribe().result", "close"],
        )
        self.assertIs(
            self.client.subscribe.call_args.kwargs["await_callbacks_on_shutdown"], True
        )

    def test_message_finishing_during_shutdown_is_in_the_final_statistics(self):
        # A callback still running at the stop finishes inside the second
        # result(); the statistics must be logged after it, not before.
        def result():
            if self.streaming_pull.result.call_count == 1:
                raise KeyboardInterrupt
            self.send(valid_record(trip_number="222"))

        self.streaming_pull.result.side_effect = result
        self.send(valid_record(trip_number="111"))

        logged = self.run_main()

        self.assertIn("Total messages: 2", logged)

    def test_stop_during_reconnect_wait_still_logs_the_batch(self):
        # The connection drops, and the stop signal lands in the 5 s sleep
        # before reconnecting, outside the try around result().
        self.streaming_pull.result.side_effect = [RuntimeError("connection lost"), None]
        self.send(valid_record())

        with mock.patch.object(se_analysis.time, "sleep", side_effect=KeyboardInterrupt):
            logged = self.run_main()

        self.assertEqual(self.streaming_pull.result.call_count, 2)
        self.assertIn("Total messages: 1", logged)
        self.assertEqual(self.db.closed, 1)

    def test_failure_closing_the_client_still_logs_the_batch(self):
        self.client.close.side_effect = RuntimeError("channel already closed")
        self.send(valid_record())

        logged = self.run_main()

        self.assertIn("Error shutting down the Pub/Sub subscriber", logged)
        self.assertIn("Total messages: 1", logged)
        self.assertEqual(self.db.closed, 1)

    def test_missing_database_credentials_fail_before_subscribing(self):
        # Without this check every message would be nacked until someone
        # noticed; failing at startup makes systemd report the problem.
        # connect is patched so a future change cannot reach a real database.
        with mock.patch.object(se_analysis, "conn", None), mock.patch.dict(
            os.environ, clear=True
        ), mock.patch.object(se_analysis.psycopg2, "connect") as connect:
            with self.assertRaises(KeyError):
                se_analysis.main()

        connect.assert_not_called()
        self.client_class.assert_not_called()


if __name__ == "__main__":
    unittest.main()
