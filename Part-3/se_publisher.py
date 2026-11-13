import json
import re
import time
import uuid
from datetime import datetime, timezone

import requests
from bs4 import BeautifulSoup
from google.cloud import pubsub_v1

# GCP setup
PUBLISHER_VM_IP = "8.229.127.245"
PUBLISHER_VM_REGION = "us-west1 (Oregon)"
PROJECT_ID = "dataeng-project-511504"
TOPIC_ID = "se_topic"


# Vehicle IDs to fetch
VEHICLE_IDS = [
    4208,
    2925,
    4228,
    3649,
    3629,
    4522,
    3914,
    4016,
    3328,
    3564,
    3002,
    3250,
    3031,
    3253,
    3556,
    3801,
    4029,
    3232,
    3756,
    3902,
    2931,
    3406,
    3960,
    4209,
    3421,
    3304,
    4070,
    3239,
    4505,
    4058,
    3134,
    4020,
    3954,
    3008,
    3637,
    3612,
    3216,
    3114,
    3568,
    3222,
    4234,
    4519,
    3035,
    3538,
    4516,
    3732,
    3943,
    3913,
    3543,
    3725,
    3263,
    2916,
    3713,
    4215,
    3053,
    3207,
    3941,
    4064,
    3204,
    3211,
    3308,
    2915,
    3719,
    3011,
    4230,
    3736,
    4517,
    3422,
    3634,
    3754,
    3126,
    3302,
    3162,
    3303,
    3531,
    3165,
    3734,
    3547,
    3235,
    3905,
    3315,
    4531,
    3416,
    3016,
    3911,
    3015,
    4212,
    3005,
    3241,
    3948,
    4028,
    4304,
    4005,
    2934,
    3958,
    3220,
    3613,
    3323,
    3573,
    3961,
    4021,
    3621,
    3571,
    3518,
    3153,
    3313,
    3262,
    3419,
    3319,
    4237,
    3548,
    3615,
    3529,
    3018,
    3009,
    3945,
    3907,
    3127,
    3605,
    3004,
    3927,
    3118,
    4067,
    3906,
    4026,
    3109,
    3507,
    3160,
    3950,
    3408,
    3243,
    2912,
    3534,
    4515,
    3569,
    3639,
    2932,
    3757,
    3228,
    3708,
    3113,
    4061,
    3716,
    4043,
    4050,
    3052,
    3233,
    3321,
    3745,
    3036,
    3715,
    3722,
    3505,
    3120,
    3322,
    4048,
    4031,
    3509,
    3411,
    3102,
    3647,
    3626,
    3137,
    3312,
    3116,
    3245,
    3632,
    2902,
    3023,
    3608,
    4018,
    4004,
    3712,
    2938,
    4526,
    4053,
    4504,
    3112,
    2920,
    3937,
    3726,
    3206,
    4037,
    3230,
    3566,
    3537,
    4014,
    3115,
    4051,
    3413,
    4015,
    4010,
    4305,
    3146,
    3723,
    3130,
    3542,
    4059,
    3012,
    3208,
    3227,
    2901,
    3054,
    4211,
    3214,
    4219,
    3609,
    3741,
    3549,
    3029,
    3156,
    3740,
    3519,
    3750,
    3403,
    3414,
    3201,
    3933,
    3928,
    3577,
    3219,
    4227,
    3735,
    3254,
    3922,
    3919,
    4055,
    3103,
    3059,
    2927,
    4502,
    4206,
    3148,
    3407,
    3729,
    3320,
    3523,
    3947,
    3959,
    3748,
    4013,
    3912,
    3264,
    3648,
    4012,
    3257,
    3217,
    4040,
    3915,
    4001,
]

API_URL = "https://busdata.cs.pdx.edu/api/getStopEvents"

# Each trip's table is introduced by a heading like
# "Stop events for PDX_TRIP 242538843"; the trip ID appears nowhere else.
TRIP_ID_PATTERN = re.compile(r"PDX_TRIP\s+(\d+)")


def find_trip_id(table):
    """Return the trip ID from the <h2> heading directly before table, or None.

    Only the immediately preceding sibling is checked, so a table missing its
    own heading is not credited to the previous trip's heading.
    """
    heading = table.find_previous_sibling()
    if heading is None or heading.name != "h2":
        return None
    match = TRIP_ID_PATTERN.search(heading.get_text())
    return match.group(1) if match else None


def parse_stop_events(html):
    """Extract StopEvent records from a getStopEvents HTML page.

    Every record gets the trip ID of its table and loses the trip_number
    column. Returns (records, bad_rows): a malformed row, or a row whose trip
    is unknown, is skipped and counted rather than failing the whole page, so
    one bad row does not discard the vehicle's good StopEvents.
    """
    soup = BeautifulSoup(html, "html.parser")

    records = []
    bad_rows = 0

    for table in soup.find_all("table"):
        rows = table.find_all("tr")

        if not rows:
            continue

        header_index = None

        for i, row in enumerate(rows):
            if row.find_all("th", recursive=False):
                header_index = i
                break

        if header_index is None:
            header_index = 0

        header_cells = rows[header_index].find_all(
            ["th", "td"], recursive=False
        )

        headers = [
            re.sub(r"\s+", "_", cell.get_text(" ", strip=True).lower())
            for cell in header_cells
        ]

        if not headers:
            continue

        trip_id = find_trip_id(table)

        for row in rows[header_index + 1:]:
            cells = row.find_all("td", recursive=False)

            if not cells:
                continue

            # A StopEvent without a trip ID cannot be joined to its trip
            # downstream, so it is treated like a malformed row.
            if trip_id is None or len(cells) != len(headers):
                bad_rows += 1
                continue

            values = [
                cell.get_text(" ", strip=True) or None
                for cell in cells
            ]

            record = dict(zip(headers, values))
            # Dropped at the professor's request.
            record.pop("trip_number", None)
            record["trip_id"] = trip_id
            records.append(record)

    return records, bad_rows


def fetch_stop_events_html(session, vehicle):
    """Return the getStopEvents HTML page for one vehicle, or None if it has no data.

    Connection errors and timeouts are retried, up to 3 attempts in total;
    any other HTTP error status is raised immediately without retrying.
    Raises a requests.RequestException when the page cannot be fetched.
    """
    for attempt in range(3):
        try:
            response = session.get(
                API_URL,
                params={"vehicle_num": vehicle},
                timeout=10
            )

            # BusData answers 404 {"error":"Not Found"} for a vehicle with no
            # stop events that day, which is normal and not an API failure.
            if response.status_code == 404:
                return None

            response.raise_for_status()
            return response.text

        except (requests.ConnectionError, requests.Timeout):
            if attempt == 2:
                raise

            time.sleep(2 * (attempt + 1))


def publish_records(publisher, topic_path, records):
    """Publish each record as its own JSON message and wait for confirmation.

    Returns (published, errors). Only confirmed publishes count as published,
    so the statistics reflect what actually reached Pub/Sub.
    """
    futures = []
    errors = 0

    # Everything is handed to the client before any result is awaited so the
    # client can batch messages instead of making one round trip per record.
    for record in records:
        try:
            message = json.dumps(record).encode("utf-8")
            futures.append(publisher.publish(topic_path, message))

        except Exception:
            errors += 1

    published = 0

    for future in futures:
        try:
            future.result()
            published += 1

        except Exception:
            errors += 1

    return published, errors


def run(session, publisher, topic_path, vehicle_ids):
    """Fetch, parse and publish the StopEvents of every vehicle.

    A failure for one vehicle is counted and the run moves on to the next
    vehicle. Returns a dict of the run's statistics.
    """
    stats = {
        "run_id": str(uuid.uuid4()),
        "api_start_timestamp": datetime.now(timezone.utc),
        "total_stop_events": 0,
        "api_errors": 0,
        "parsing_errors": 0,
        "publishing_errors": 0,
    }
    vehicles_with_data = set()
    vehicles_without_data = set()
    start_time = time.perf_counter()

    for vehicle in vehicle_ids:
        try:
            html = fetch_stop_events_html(session, vehicle)

        except requests.RequestException:
            stats["api_errors"] += 1
            continue

        if html is None:
            vehicles_without_data.add(vehicle)
            continue

        try:
            records, bad_rows = parse_stop_events(html)

        # An unexpected page should cost one vehicle, not the whole
        # scheduled run.
        except Exception:
            stats["parsing_errors"] += 1
            continue

        stats["parsing_errors"] += bad_rows

        if records:
            vehicles_with_data.add(vehicle)
        else:
            vehicles_without_data.add(vehicle)

        published, errors = publish_records(publisher, topic_path, records)
        stats["total_stop_events"] += published
        stats["publishing_errors"] += errors

        # Be polite to the BusData server.
        time.sleep(0.1)

    elapsed_time = time.perf_counter() - start_time

    stats["publish_end_timestamp"] = datetime.now(timezone.utc)
    stats["elapsed_time"] = elapsed_time
    stats["throughput"] = (
        stats["total_stop_events"] / elapsed_time
        if elapsed_time > 0 else 0
    )
    stats["vehicles_with_data"] = len(vehicles_with_data)
    stats["vehicles_without_data"] = len(vehicles_without_data)

    return stats


def print_statistics(stats):
    """Print the publishing statistics of one run."""
    print("\nStopEvent Publisher Statistics:")
    print("-------------------------------")
    print(f"RunID: {stats['run_id']}")
    print(f"API Start Timestamp: {stats['api_start_timestamp']}")
    print(f"Publishing End Timestamp: {stats['publish_end_timestamp']}")
    print(f"Elapsed Wall-Clock Time: {stats['elapsed_time']:.2f} seconds")
    print(f"Total StopEvents Published: {stats['total_stop_events']}")
    print(f"Publishing Throughput: {stats['throughput']:.2f} StopEvents/second")
    print(f"Unique Vehicles With Data: {stats['vehicles_with_data']}")
    print(f"Unique Vehicles Without Data: {stats['vehicles_without_data']}")
    print(f"API Errors: {stats['api_errors']}")
    print(f"Parsing Errors: {stats['parsing_errors']}")
    print(f"Publishing Errors: {stats['publishing_errors']}")


def main():
    """Publish the StopEvents of every vehicle in our group and print statistics."""
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    with requests.Session() as session:
        stats = run(session, publisher, topic_path, VEHICLE_IDS)

    print_statistics(stats)


if __name__ == "__main__":
    main()
