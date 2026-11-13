"""Tests for se_publisher.py.

The BusData API and Pub/Sub are replaced with unittest.mock objects, so
nothing external is touched. The real page in fixtures/ was saved from
getStopEvents for vehicle 4001.

Run from the Part-3 directory:
    python3 -m unittest discover -s tests -v
"""

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


if __name__ == "__main__":
    unittest.main()
