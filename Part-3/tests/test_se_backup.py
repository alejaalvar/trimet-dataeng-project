"""Tests for se_backup.py.

Pub/Sub messages are unittest.mock objects and the archive directory is a
temporary directory, so nothing external is touched. The clock
(se_backup.datetime) is replaced so daily file names are predictable.

Run from the Part-3 directory:
    python3 -m unittest discover -s tests -v
"""

import json
import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from unittest import mock

PART3_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PART3_DIR)

import se_backup  # noqa: E402

NOON = datetime(2026, 11, 17, 12, 0, 0, tzinfo=timezone.utc)


def stop_event(vehicle="4001", trip="111"):
    """Return a StopEvent record shaped like the ones se_publisher.py sends."""
    return {
        "trip_id": trip,
        "vehicle_number": vehicle,
        "arrive_time": "2023-01-21T06:55:02",
        "stop_time": "2023-01-21T06:54:00",
        "leave_time": "2023-01-21T06:55:02",
    }


def message(data):
    """Return a fake Pub/Sub message; a dict is sent as JSON bytes."""
    if isinstance(data, dict):
        data = json.dumps(data).encode("utf-8")
    return mock.Mock(data=data)


class BackupTestCase(unittest.TestCase):
    """Points the backup at a temp directory, a fixed clock and fresh stats."""

    def setUp(self):
        """Give each test an empty archive directory, a clock at NOON and empty stats."""
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.archive_dir = tmp.name

        dir_patcher = mock.patch.object(se_backup, "ARCHIVE_DIR", self.archive_dir)
        dir_patcher.start()
        self.addCleanup(dir_patcher.stop)

        # Only now() is replaced; the code under test uses nothing else
        # from datetime.
        clock_patcher = mock.patch.object(se_backup, "datetime")
        self.clock = clock_patcher.start()
        self.addCleanup(clock_patcher.stop)
        self.clock.now.return_value = NOON

        # stats is module state that callback() fills and finish_batch()
        # replaces, so each test starts from (and leaves behind) a clean copy.
        original_stats = se_backup.stats
        se_backup.stats = se_backup.reset_stats()
        self.addCleanup(setattr, se_backup, "stats", original_stats)

    def archived_lines(self, date="2026-11-17"):
        """Return the records in the day's working file, in order."""
        path = os.path.join(self.archive_dir, f"se_{date}.jsonl")
        with open(path, encoding="utf-8") as file:
            return [json.loads(line) for line in file]


class CallbackTests(BackupTestCase):
    """callback() writes each StopEvent to the day's file and acks it."""

    def test_valid_message_is_written_to_daily_file_and_acked(self):
        msg = message(stop_event())

        se_backup.callback(msg)

        self.assertEqual(self.archived_lines(), [stop_event()])
        msg.ack.assert_called_once()
        msg.nack.assert_not_called()

    def test_messages_are_written_in_arrival_order(self):
        se_backup.callback(message(stop_event(trip="111")))
        se_backup.callback(message(stop_event(trip="222")))

        self.assertEqual(
            [record["trip_id"] for record in self.archived_lines()], ["111", "222"]
        )
        self.assertEqual(se_backup.stats["record_count"], 2)

    def test_bad_json_is_acked_and_not_written(self):
        msg = message(b"not json")

        with self.assertLogs(level="WARNING"):
            se_backup.callback(msg)

        msg.ack.assert_called_once()
        msg.nack.assert_not_called()
        self.assertEqual(os.listdir(self.archive_dir), [])
        self.assertEqual(se_backup.stats["record_count"], 0)

    def test_json_that_is_not_an_object_is_acked_and_not_written(self):
        msg = message(b"[1, 2, 3]")

        with self.assertLogs(level="WARNING"):
            se_backup.callback(msg)

        msg.ack.assert_called_once()
        msg.nack.assert_not_called()
        self.assertEqual(os.listdir(self.archive_dir), [])
        self.assertEqual(se_backup.stats["record_count"], 0)

    def test_bytes_that_are_not_utf8_are_acked_and_not_written(self):
        msg = message(b"\xff\xfe")

        with self.assertLogs(level="WARNING"):
            se_backup.callback(msg)

        msg.ack.assert_called_once()
        msg.nack.assert_not_called()
        self.assertEqual(os.listdir(self.archive_dir), [])

    def test_unique_vehicles_are_counted_by_vehicle_number(self):
        for vehicle in ("4001", "4001", "4002"):
            se_backup.callback(message(stop_event(vehicle=vehicle)))

        self.assertEqual(se_backup.stats["vehicle_ids"], {"4001", "4002"})

    def test_record_without_vehicle_number_is_kept_but_not_counted_as_a_vehicle(self):
        record = stop_event()
        del record["vehicle_number"]
        msg = message(record)

        se_backup.callback(msg)

        self.assertEqual(self.archived_lines(), [record])
        msg.ack.assert_called_once()
        self.assertEqual(se_backup.stats["vehicle_ids"], set())

    def test_first_and_last_receive_times_are_recorded(self):
        first = datetime(2026, 11, 17, 12, 0, 0, tzinfo=timezone.utc)
        last = datetime(2026, 11, 17, 12, 5, 0, tzinfo=timezone.utc)
        self.clock.now.side_effect = [first, last]

        # time.monotonic is what elapsed time and throughput are computed from.
        with mock.patch.object(se_backup.time, "monotonic", side_effect=[10.0, 310.0]):
            se_backup.callback(message(stop_event(trip="111")))
            se_backup.callback(message(stop_event(trip="222")))

        self.assertEqual(se_backup.stats["first_received"], first)
        self.assertEqual(se_backup.stats["last_received"], last)
        self.assertEqual(se_backup.stats["first_monotonic"], 10.0)
        self.assertEqual(se_backup.stats["last_monotonic"], 310.0)

    def test_bytes_before_compression_match_the_working_file_size(self):
        se_backup.callback(message(stop_event(trip="111")))
        se_backup.callback(message(stop_event(trip="222")))

        working_file = os.path.join(self.archive_dir, "se_2026-11-17.jsonl")
        self.assertEqual(
            se_backup.stats["bytes_before_compression"], os.path.getsize(working_file)
        )

    def test_batch_running_past_midnight_stays_in_its_first_day_file(self):
        self.clock.now.return_value = datetime(2026, 11, 17, 23, 59, 59, tzinfo=timezone.utc)
        se_backup.callback(message(stop_event(trip="111")))
        self.clock.now.return_value = datetime(2026, 11, 18, 0, 0, 1, tzinfo=timezone.utc)
        se_backup.callback(message(stop_event(trip="222")))

        self.assertEqual(
            [record["trip_id"] for record in self.archived_lines("2026-11-17")],
            ["111", "222"],
        )
        self.assertEqual(os.listdir(self.archive_dir), ["se_2026-11-17.jsonl"])

    def test_write_failure_is_nacked_so_pubsub_redelivers(self):
        se_backup.ARCHIVE_DIR = os.path.join(self.archive_dir, "missing")
        msg = message(stop_event())

        with self.assertLogs(level="ERROR"):
            se_backup.callback(msg)

        msg.nack.assert_called_once()
        msg.ack.assert_not_called()
        self.assertEqual(se_backup.stats["record_count"], 0)
        # A batch date kept from a failed write would send every later
        # record to this day's file.
        self.assertIsNone(se_backup.stats["date"])


if __name__ == "__main__":
    unittest.main()
