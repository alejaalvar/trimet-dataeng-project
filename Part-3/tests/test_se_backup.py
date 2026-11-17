"""Tests for se_backup.py.

Pub/Sub messages are unittest.mock objects and the archive directory is a
temporary directory, so nothing external is touched. The clock
(se_backup.datetime) is replaced so daily file names are predictable.

Run from the Part-3 directory:
    python3 -m unittest discover -s tests -v
"""

import gzip
import json
import os
import signal
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

    def receive(self, records):
        """Deliver each record to callback() as a Pub/Sub message."""
        for record in records:
            se_backup.callback(message(record))

    def read_archive(self, date="2026-11-17"):
        """Return the parsed contents of the day's compressed archive."""
        path = os.path.join(self.archive_dir, f"se_{date}.json.gz")
        with gzip.open(path, "rt", encoding="utf-8") as file:
            return json.load(file)


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


class CompressRecordsTests(BackupTestCase):
    """compress_records() turns the day's working file into se_<date>.json.gz."""

    def test_archive_is_a_json_array_of_every_received_record(self):
        records = [stop_event(trip="111"), stop_event(trip="222"), stop_event(trip="333")]
        self.receive(records)

        se_backup.compress_records("2026-11-17")

        self.assertEqual(self.read_archive(), records)

    def test_single_record_archive_is_still_valid_json(self):
        self.receive([stop_event()])

        se_backup.compress_records("2026-11-17")

        self.assertEqual(self.read_archive(), [stop_event()])

    def test_returns_archive_size_and_finish_time(self):
        self.receive([stop_event()])

        compressed_bytes, finish_time = se_backup.compress_records("2026-11-17")

        archive = os.path.join(self.archive_dir, "se_2026-11-17.json.gz")
        self.assertEqual(compressed_bytes, os.path.getsize(archive))
        self.assertEqual(finish_time, NOON)

    def test_no_temporary_file_is_left_behind(self):
        self.receive([stop_event()])

        se_backup.compress_records("2026-11-17")

        self.assertEqual(
            sorted(os.listdir(self.archive_dir)),
            ["se_2026-11-17.json.gz", "se_2026-11-17.jsonl"],
        )

    def test_failed_compression_keeps_the_previous_archive(self):
        # Writing to a .tmp file and swapping it in is what protects the
        # existing archive if compression dies part way through.
        self.receive([stop_event(trip="111")])
        se_backup.compress_records("2026-11-17")
        self.receive([stop_event(trip="222")])

        with mock.patch.object(se_backup.os, "replace", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                se_backup.compress_records("2026-11-17")

        self.assertEqual(self.read_archive(), [stop_event(trip="111")])

    def test_only_the_requested_days_records_are_archived(self):
        self.receive([stop_event(trip="111")])
        se_backup.stats = se_backup.reset_stats()
        self.clock.now.return_value = datetime(2026, 11, 18, 12, 0, 0, tzinfo=timezone.utc)
        self.receive([stop_event(trip="222")])

        se_backup.compress_records("2026-11-17")

        self.assertEqual(self.read_archive("2026-11-17"), [stop_event(trip="111")])
        self.assertNotIn("se_2026-11-18.json.gz", os.listdir(self.archive_dir))

    def test_missing_working_file_creates_no_archive(self):
        self.assertEqual(se_backup.compress_records("2026-11-17"), (None, None))
        self.assertEqual(os.listdir(self.archive_dir), [])

    def test_second_batch_on_the_same_day_is_added_to_the_days_archive(self):
        first_batch = [stop_event(trip="111"), stop_event(trip="222")]
        second_batch = [stop_event(trip="333")]
        self.receive(first_batch)
        se_backup.compress_records("2026-11-17")
        se_backup.stats = se_backup.reset_stats()

        self.receive(second_batch)
        se_backup.compress_records("2026-11-17")

        self.assertEqual(self.read_archive(), first_batch + second_batch)


class FinishBatchTests(BackupTestCase):
    """finish_batch() archives the batch, logs its statistics and starts a new one."""

    def finish_and_capture_logs(self):
        """Run finish_batch() and return the messages it logged."""
        with self.assertLogs(level="INFO") as logs:
            se_backup.finish_batch()
        return [record.getMessage() for record in logs.records]

    def test_archives_the_batch_and_resets_stats(self):
        records = [stop_event(trip="111"), stop_event(trip="222")]
        self.receive(records)

        self.finish_and_capture_logs()

        self.assertEqual(self.read_archive(), records)
        self.assertEqual(se_backup.stats, se_backup.reset_stats())

    def test_does_nothing_without_records(self):
        with self.assertNoLogs(level="INFO"):
            se_backup.finish_batch()

        self.assertEqual(os.listdir(self.archive_dir), [])

    def test_logs_record_vehicle_byte_and_archive_statistics(self):
        # Distinct clock readings for the two arrivals and the end of
        # compression, so a line logging the wrong time is caught.
        self.clock.now.side_effect = [
            datetime(2026, 11, 17, 12, 0, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 17, 12, 5, 0, tzinfo=timezone.utc),
            datetime(2026, 11, 17, 12, 10, 0, tzinfo=timezone.utc),
        ]
        self.receive([stop_event(vehicle="4001"), stop_event(vehicle="4002")])
        working_file = os.path.join(self.archive_dir, "se_2026-11-17.jsonl")
        bytes_received = os.path.getsize(working_file)

        logged = self.finish_and_capture_logs()

        archive = os.path.join(self.archive_dir, "se_2026-11-17.json.gz")
        expected = [
            "First record received: 2026-11-17T12:00:00+00:00",
            "Last record received: 2026-11-17T12:05:00+00:00",
            "Compression finished: 2026-11-17T12:10:00+00:00",
            "Total StopEvent records: 2",
            "Unique vehicles: 2",
            f"Bytes before compression: {bytes_received}",
            f"Compressed file size: {os.path.getsize(archive)} bytes",
            "Archive file: se_2026-11-17.json.gz",
        ]
        for line in expected:
            with self.subTest(line=line):
                self.assertIn(line, logged)

    def test_elapsed_time_is_receive_time_plus_compression_time(self):
        # Messages arrive at 0 s and 8 s; compression runs from 100 s to 102 s.
        # The idle wait in between is not part of the batch, as in backup.py.
        with mock.patch.object(
            se_backup.time, "monotonic", side_effect=[0.0, 8.0, 100.0, 102.0]
        ):
            self.receive([stop_event(trip="111"), stop_event(trip="222")])
            logged = self.finish_and_capture_logs()

        self.assertIn("Elapsed wall-clock time: 10.000 seconds", logged)
        self.assertIn("Throughput: 0.20 records/second", logged)

    def test_zero_elapsed_time_gives_zero_throughput(self):
        # One record and an instant compression: elapsed is 0, which must
        # not raise ZeroDivisionError.
        with mock.patch.object(se_backup.time, "monotonic", return_value=5.0):
            self.receive([stop_event()])
            logged = self.finish_and_capture_logs()

        self.assertIn("Throughput: 0.00 records/second", logged)

    def test_missing_working_file_is_logged_and_the_batch_is_dropped(self):
        self.receive([stop_event()])
        os.remove(os.path.join(self.archive_dir, "se_2026-11-17.jsonl"))

        with self.assertLogs(level="ERROR"):
            se_backup.finish_batch()

        self.assertEqual(se_backup.stats, se_backup.reset_stats())
        self.assertEqual(os.listdir(self.archive_dir), [])


class FinishIfIdleTests(BackupTestCase):
    """finish_if_idle() ends the batch only after IDLE_SECONDS without a message."""

    def receive_at(self, monotonic_seconds):
        """Deliver one StopEvent as if it arrived at the given monotonic time."""
        with mock.patch.object(se_backup.time, "monotonic", return_value=monotonic_seconds):
            self.receive([stop_event()])

    def check_idle_at(self, monotonic_seconds):
        """Run finish_if_idle() as if the clock read the given monotonic time."""
        with mock.patch.object(se_backup.time, "monotonic", return_value=monotonic_seconds):
            se_backup.finish_if_idle()

    def test_batch_is_not_finished_before_idle_seconds(self):
        self.receive_at(0.0)

        with self.assertNoLogs(level="INFO"):
            self.check_idle_at(se_backup.IDLE_SECONDS - 1)

        self.assertNotIn("se_2026-11-17.json.gz", os.listdir(self.archive_dir))
        self.assertEqual(se_backup.stats["record_count"], 1)

    def test_batch_is_finished_once_idle_seconds_have_passed(self):
        self.receive_at(0.0)

        with self.assertLogs(level="INFO"):
            self.check_idle_at(se_backup.IDLE_SECONDS)

        self.assertEqual(self.read_archive(), [stop_event()])
        self.assertEqual(se_backup.stats["record_count"], 0)


class ShutdownTests(BackupTestCase):
    """Stopping main(), by Ctrl+C or systemd's SIGTERM, archives the current batch."""

    def setUp(self):
        """Replace Pub/Sub, signal registration, the checker thread and log setup with mocks."""
        super().setUp()

        # The pull's first result() call is interrupted, as a stop signal
        # would; the second (waiting for running callbacks) returns.
        client_patcher = mock.patch.object(se_backup.pubsub_v1, "SubscriberClient")
        self.client = client_patcher.start().return_value
        self.addCleanup(client_patcher.stop)
        self.streaming_pull = self.client.subscribe.return_value
        self.streaming_pull.result.side_effect = [KeyboardInterrupt, None]

        # Registering real handlers would change how this test process
        # itself responds to Ctrl+C and SIGTERM.
        signal_patcher = mock.patch.object(se_backup.signal, "signal")
        self.register_signal = signal_patcher.start()
        self.addCleanup(signal_patcher.stop)

        # A real checker thread would outlive the test and keep calling
        # finish_if_idle() against later tests' state.
        thread_patcher = mock.patch.object(se_backup, "Thread")
        thread_patcher.start()
        self.addCleanup(thread_patcher.stop)

        # basicConfig would attach a handler to the root logger for every
        # later test.
        logging_patcher = mock.patch.object(se_backup.logging, "basicConfig")
        logging_patcher.start()
        self.addCleanup(logging_patcher.stop)

        # main() sets stop_event on the way out; later tests need it cleared.
        self.addCleanup(se_backup.stop_event.clear)

    def run_main(self):
        """Run main() until its simulated stop, capturing its logs."""
        with self.assertLogs(level="INFO"):
            se_backup.main()

    def test_stop_signal_raises_keyboard_interrupt_and_ignores_repeats(self):
        with self.assertRaises(KeyboardInterrupt):
            se_backup.handle_stop_signal(signal.SIGTERM, None)

        self.register_signal.assert_any_call(signal.SIGTERM, signal.SIG_IGN)
        self.register_signal.assert_any_call(signal.SIGINT, signal.SIG_IGN)

    def test_main_registers_the_handler_for_sigterm_and_ctrl_c(self):
        self.run_main()

        self.register_signal.assert_any_call(signal.SIGTERM, se_backup.handle_stop_signal)
        self.register_signal.assert_any_call(signal.SIGINT, se_backup.handle_stop_signal)

    def test_stopping_main_archives_the_current_batch(self):
        records = [stop_event(trip="111"), stop_event(trip="222")]
        self.receive(records)

        self.run_main()

        self.assertEqual(self.read_archive(), records)
        self.assertEqual(se_backup.stats, se_backup.reset_stats())

    def test_stop_during_reconnect_wait_still_waits_for_callbacks_and_archives(self):
        # The connection drops, and the stop signal lands in the 5 s sleep
        # before reconnecting, outside the try around result().
        self.streaming_pull.result.side_effect = [RuntimeError("connection lost"), None]
        self.receive([stop_event()])

        with mock.patch.object(se_backup.time, "sleep", side_effect=KeyboardInterrupt):
            self.run_main()

        self.assertEqual(self.streaming_pull.result.call_count, 2)
        self.client.close.assert_called_once()
        self.assertEqual(self.read_archive(), [stop_event()])

    def test_client_is_closed_only_after_running_callbacks_finish(self):
        # Closing first would drop the acks of callbacks still running.
        self.run_main()

        # mock_calls records every call on the client and the objects it
        # returned, in order, e.g. "subscribe().result" then "close".
        call_names = [name for name, args, kwargs in self.client.mock_calls]
        self.assertEqual(call_names[-2:], ["subscribe().result", "close"])

    def test_failure_closing_the_client_still_archives_the_batch(self):
        self.client.close.side_effect = RuntimeError("channel already closed")
        self.receive([stop_event()])

        self.run_main()

        self.assertEqual(self.read_archive(), [stop_event()])


if __name__ == "__main__":
    unittest.main()
