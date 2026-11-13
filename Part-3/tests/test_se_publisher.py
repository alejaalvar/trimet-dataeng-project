"""Tests for se_publisher.py.

The BusData API and Pub/Sub are replaced with unittest.mock objects, so
nothing external is touched. The real page in fixtures/ was saved from
getStopEvents for vehicle 4001.

Run from the Part-3 directory:
    python3 -m unittest discover -s tests -v
"""

import contextlib
import io
import json
import os
import sys
import unittest
from unittest import mock

import requests

PART3_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PART3_DIR)

import se_publisher  # noqa: E402

FIXTURE_PATH = os.path.join(PART3_DIR, "tests", "fixtures", "stop_events_4001.html")

# A small subset of the real columns, keeping trip_number so its removal can
# be checked.
HEADER_ROW = "<tr><th>vehicle_number</th><th>trip_number</th><th>leave_time</th></tr>"


def trip_table(trip_id, rows):
    """Return one trip's <h2> heading and <table>, shaped like the real page.

    trip_id=None leaves out the heading. Each row is a tuple of cell texts.
    """
    heading = f"<h2>Stop events for PDX_TRIP {trip_id}</h2>" if trip_id else ""
    body = "".join(
        "<tr>" + "".join(f"<td>{cell}</td>" for cell in row) + "</tr>"
        for row in rows
    )
    return f"{heading}<table>{HEADER_ROW}{body}</table>"


def page(*parts):
    """Wrap HTML fragments in a page body."""
    return "<html><body>" + "".join(parts) + "</body></html>"


def fake_response(status_code=200, text="<html></html>"):
    """Return a stand-in for requests.Response with the given status and body.

    Like the real one, raise_for_status() raises HTTPError for a 4xx/5xx.
    """
    response = mock.Mock()
    response.status_code = status_code
    response.text = text
    if status_code >= 400:
        response.raise_for_status.side_effect = requests.HTTPError(f"{status_code} error")
    return response


def fake_api(responses):
    """Return a fake session whose get() answers each vehicle_num from responses."""
    session = mock.Mock()
    session.get.side_effect = lambda url, params, timeout: responses[params["vehicle_num"]]
    return session


def stop_events_page(trip_id, row_count):
    """Return a 200 response holding one trip with row_count valid StopEvents."""
    rows = [("4001", "1010", str(i)) for i in range(row_count)]
    return fake_response(text=page(trip_table(trip_id, rows)))


class ParseStopEventsTests(unittest.TestCase):
    """parse_stop_events() turns a getStopEvents page into StopEvent records."""

    @classmethod
    def setUpClass(cls):
        """Parse the real page once; it is large and every test only reads it."""
        with open(FIXTURE_PATH, encoding="utf-8") as file:
            cls.sample_records, cls.sample_bad_rows = se_publisher.parse_stop_events(
                file.read()
            )

    def test_real_page_yields_one_record_per_data_row(self):
        # Counted with grep: 24,936 <td> cells / 24 columns = 1,039 rows.
        self.assertEqual(len(self.sample_records), 1039)
        self.assertEqual(self.sample_bad_rows, 0)

    def test_real_page_first_record_has_its_trip_and_values(self):
        first = self.sample_records[0]
        self.assertEqual(first["trip_id"], "242538843")
        self.assertEqual(first["vehicle_number"], "4001")
        self.assertEqual(first["leave_time"], "24902")

    def test_record_has_trip_id_and_no_trip_number(self):
        records, _ = se_publisher.parse_stop_events(
            page(trip_table("111", [("4001", "1010", "100")]))
        )
        self.assertEqual(
            records, [{"vehicle_number": "4001", "leave_time": "100", "trip_id": "111"}]
        )

    def test_each_record_gets_its_own_tables_trip_id(self):
        records, _ = se_publisher.parse_stop_events(
            page(
                trip_table("111", [("4001", "1010", "100")]),
                trip_table("222", [("4001", "1020", "200")]),
            )
        )
        self.assertEqual([r["trip_id"] for r in records], ["111", "222"])

    def test_empty_cell_becomes_none(self):
        records, _ = se_publisher.parse_stop_events(
            page(trip_table("111", [("4001", "1010", "")]))
        )
        self.assertIsNone(records[0]["leave_time"])

    def test_row_with_wrong_cell_count_is_skipped_and_counted(self):
        records, bad_rows = se_publisher.parse_stop_events(
            page(
                trip_table(
                    "111",
                    [("4001", "1010", "100"), ("4001", "1010"), ("4001", "1010", "300")],
                )
            )
        )
        self.assertEqual([r["leave_time"] for r in records], ["100", "300"])
        self.assertEqual(bad_rows, 1)

    def test_table_without_trip_heading_is_skipped_and_counted(self):
        # The headless table follows trip 111's table, so this also checks
        # that its rows are not credited to trip 111.
        records, bad_rows = se_publisher.parse_stop_events(
            page(
                trip_table("111", [("4001", "1010", "100")]),
                trip_table(None, [("4001", "1020", "200"), ("4001", "1020", "300")]),
            )
        )
        self.assertEqual([r["leave_time"] for r in records], ["100"])
        self.assertEqual(bad_rows, 2)

    def test_page_without_tables_has_no_records_or_errors(self):
        self.assertEqual(
            se_publisher.parse_stop_events(page("<h1>No stop events</h1>")), ([], 0)
        )


class FetchStopEventsTests(unittest.TestCase):
    """fetch_stop_events_html() queries getStopEvents for one vehicle."""

    def setUp(self):
        """Give each test a fake session and a sleep that returns immediately."""
        # Without this, the retry backoff would really sleep for 2 + 4 seconds.
        sleep_patcher = mock.patch.object(se_publisher.time, "sleep")
        sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

        self.session = mock.Mock()

    def test_queries_the_vehicle_and_returns_the_html(self):
        self.session.get.return_value = fake_response(text="<html>4001</html>")

        html = se_publisher.fetch_stop_events_html(self.session, 4001)

        self.assertEqual(html, "<html>4001</html>")
        self.session.get.assert_called_once_with(
            se_publisher.API_URL, params={"vehicle_num": 4001}, timeout=10
        )

    def test_connection_error_is_retried(self):
        self.session.get.side_effect = [
            requests.ConnectionError(),
            fake_response(text="<html>4001</html>"),
        ]

        html = se_publisher.fetch_stop_events_html(self.session, 4001)

        self.assertEqual(html, "<html>4001</html>")
        self.assertEqual(self.session.get.call_count, 2)

    def test_gives_up_after_three_timeouts(self):
        self.session.get.side_effect = requests.Timeout()

        with self.assertRaises(requests.Timeout):
            se_publisher.fetch_stop_events_html(self.session, 4001)

        self.assertEqual(self.session.get.call_count, 3)

    def test_404_means_no_data_and_is_not_retried(self):
        self.session.get.return_value = fake_response(404, '{"error":"Not Found"}')

        self.assertIsNone(se_publisher.fetch_stop_events_html(self.session, 4001))
        self.assertEqual(self.session.get.call_count, 1)

    def test_server_error_is_raised_without_retry(self):
        self.session.get.return_value = fake_response(500)

        with self.assertRaises(requests.HTTPError):
            se_publisher.fetch_stop_events_html(self.session, 4001)

        self.assertEqual(self.session.get.call_count, 1)


TOPIC_PATH = "projects/test-project/topics/se_topic"


class PublishRecordsTests(unittest.TestCase):
    """publish_records() sends each StopEvent to Pub/Sub as its own message."""

    def setUp(self):
        """Give each test a fake Pub/Sub publisher client.

        By default, publish() returns a Mock future whose result() succeeds.
        """
        self.publisher = mock.Mock()
        self.records = [
            {"trip_id": "111", "leave_time": "100"},
            {"trip_id": "111", "leave_time": "200"},
        ]

    def test_each_record_is_published_as_its_own_json_message(self):
        published, errors = se_publisher.publish_records(
            self.publisher, TOPIC_PATH, self.records
        )

        calls = self.publisher.publish.call_args_list
        self.assertEqual([call.args[0] for call in calls], [TOPIC_PATH, TOPIC_PATH])
        self.assertEqual([json.loads(call.args[1]) for call in calls], self.records)
        self.assertEqual((published, errors), (2, 0))

    def test_failed_future_counts_as_error_not_published(self):
        failed_future = mock.Mock()
        failed_future.result.side_effect = RuntimeError("publish failed")
        self.publisher.publish.side_effect = [mock.Mock(), failed_future]

        result = se_publisher.publish_records(self.publisher, TOPIC_PATH, self.records)

        self.assertEqual(result, (1, 1))

    def test_publish_call_failure_counts_as_error_and_next_record_is_published(self):
        self.publisher.publish.side_effect = [RuntimeError("client error"), mock.Mock()]

        result = se_publisher.publish_records(self.publisher, TOPIC_PATH, self.records)

        self.assertEqual(result, (1, 1))
        self.assertEqual(self.publisher.publish.call_count, 2)


class RunTests(unittest.TestCase):
    """run() processes every vehicle and collects the run statistics."""

    def setUp(self):
        """Give each test a fake publisher and a sleep that returns immediately."""
        # run() sleeps 0.1 s per vehicle to be polite to the real server.
        sleep_patcher = mock.patch.object(se_publisher.time, "sleep")
        sleep_patcher.start()
        self.addCleanup(sleep_patcher.stop)

        self.publisher = mock.Mock()

    def run_vehicles(self, responses):
        """Run the publisher over every vehicle in responses and return its stats."""
        return se_publisher.run(
            fake_api(responses), self.publisher, TOPIC_PATH, list(responses)
        )

    def test_api_error_is_counted_and_next_vehicle_is_published(self):
        stats = self.run_vehicles({1: fake_response(500), 2: stop_events_page("111", 1)})

        self.assertEqual(stats["api_errors"], 1)
        self.assertEqual(stats["total_stop_events"], 1)

    def test_bad_row_is_counted_and_good_rows_are_published(self):
        rows = [("4001", "1010", "100"), ("4001", "1010"), ("4001", "1010", "300")]
        stats = self.run_vehicles(
            {1: fake_response(text=page(trip_table("111", rows)))}
        )

        self.assertEqual(stats["parsing_errors"], 1)
        self.assertEqual(stats["total_stop_events"], 2)

    def test_parser_crash_is_counted_and_next_vehicle_is_published(self):
        crash_then_one_record = [RuntimeError("unexpected page"), ([{"trip_id": "111"}], 0)]
        with mock.patch.object(
            se_publisher, "parse_stop_events", side_effect=crash_then_one_record
        ):
            stats = self.run_vehicles(
                {1: stop_events_page("111", 1), 2: stop_events_page("222", 1)}
            )

        self.assertEqual(stats["parsing_errors"], 1)
        self.assertEqual(stats["total_stop_events"], 1)

    def test_publishing_error_is_counted(self):
        self.publisher.publish.side_effect = RuntimeError("client error")

        stats = self.run_vehicles({1: stop_events_page("111", 1)})

        self.assertEqual(stats["publishing_errors"], 1)
        self.assertEqual(stats["total_stop_events"], 0)

    def test_vehicles_with_and_without_data_are_counted(self):
        stats = self.run_vehicles(
            {
                1: stop_events_page("111", 2),
                2: fake_response(404, '{"error":"Not Found"}'),
                3: fake_response(text=page("<h1>No stop events</h1>")),
                4: fake_response(500),
            }
        )

        self.assertEqual(stats["vehicles_with_data"], 1)
        self.assertEqual(stats["vehicles_without_data"], 2)
        self.assertEqual(stats["api_errors"], 1)

    def test_throughput_is_stop_events_per_second(self):
        with mock.patch.object(se_publisher.time, "perf_counter", side_effect=[100.0, 110.0]):
            stats = self.run_vehicles({1: stop_events_page("111", 20)})

        self.assertEqual(stats["elapsed_time"], 10.0)
        self.assertEqual(stats["throughput"], 2.0)

    def test_zero_elapsed_time_gives_zero_throughput(self):
        with mock.patch.object(se_publisher.time, "perf_counter", side_effect=[5.0, 5.0]):
            stats = self.run_vehicles({1: stop_events_page("111", 1)})

        self.assertEqual(stats["throughput"], 0)


class PrintStatisticsTests(unittest.TestCase):
    """print_statistics() logs the numbers the spec asks for."""

    def test_prints_counts_time_throughput_and_errors(self):
        # Every count is different so a line printing the wrong field fails.
        stats = {
            "run_id": "test-run",
            "api_start_timestamp": "start",
            "publish_end_timestamp": "end",
            "elapsed_time": 10.0,
            "total_stop_events": 20,
            "throughput": 2.0,
            "vehicles_with_data": 3,
            "vehicles_without_data": 7,
            "api_errors": 4,
            "parsing_errors": 5,
            "publishing_errors": 6,
        }
        output = io.StringIO()
        with contextlib.redirect_stdout(output):
            se_publisher.print_statistics(stats)

        expected_lines = [
            "Elapsed Wall-Clock Time: 10.00 seconds",
            "Total StopEvents Published: 20",
            "Publishing Throughput: 2.00 StopEvents/second",
            "Unique Vehicles With Data: 3",
            "Unique Vehicles Without Data: 7",
            "API Errors: 4",
            "Parsing Errors: 5",
            "Publishing Errors: 6",
        ]
        for line in expected_lines:
            with self.subTest(line=line):
                self.assertIn(line, output.getvalue().splitlines())


if __name__ == "__main__":
    unittest.main()
