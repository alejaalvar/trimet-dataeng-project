"""Tests for se_analysis.py.

PostgreSQL and Pub/Sub are replaced with fakes, so nothing external is
touched. Records are shaped like the ones se_publisher.py sends: every page
column as a string, trip_number holding the PDX_TRIP id, and service_date.

Run from the Part-3 directory:
    python3 -m unittest discover -s tests -v
"""

import os
import sys
import unittest
from datetime import datetime
from unittest import mock

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


if __name__ == "__main__":
    unittest.main()
