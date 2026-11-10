#analysis.py

import json
import time
import logging
import os
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from threading import Lock, Event, Thread

import psycopg2
from psycopg2.extras import execute_values


import warnings

# Google warns that future library releases will drop Python 3.11; current versions work fine.
# ideally, we'd be using a venv for this
warnings.filterwarnings("ignore", message="You are using a Python version", category=FutureWarning)

from google.cloud import pubsub_v1

# Configuration
PROJECT_ID = 'dataeng-project-511504'
SUBSCRIPTION_ID = 'analysis_sub'
IDLE_SECONDS = 300
SPEED_LIMIT_KMH = 120
INVALID_DIR = '.'
INSERT_CHUNK = 5000  # rows per INSERT statement at batch end

# PostgreSQL configuration (credentials are read from the environment in main())
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'breadcrumbs'

# The database connection is opened in main(), so this module can be imported
# (e.g., by tests) without connecting to PostgreSQL or Pub/Sub.
conn = None

db_lock = Lock()
lock = Lock()
invalid_file_lock = Lock()
stop_event = Event()


def reset_stats():
    return {
        'first_received': None, 'last_received': None,
        'first_monotonic': None, 'last_monotonic': None,
        'earliest_breadcrumb': None, 'latest_breadcrumb': None,
        'vehicle_ids': set(), 'trip_ids': set(),
        'breadcrumb_count': 0, 'invalid_count': 0, 'duplicate_count': 0
    }


stats = reset_stats()

# Why speed is computed at batch end:
# Pub/Sub does not deliver messages in order (in a recorded day, 76% of
# breadcrumbs arrived after a later breadcrumb of the same trip). Speed is the
# difference between consecutive breadcrumbs of a trip, so it can only be
# computed once all of the trip's breadcrumbs are present and sorted by time.
# Each valid breadcrumb is therefore buffered here, keyed by trip, and speed is
# computed and the rows loaded when the batch ends (see finish_batch()).
#
# trip_id -> [(timestamp, meters, vehicle_id, latitude, longitude), ...]
trip_buffers = defaultdict(list)
last_message_monotonic = None


def parse_breadcrumb_time(record):
    date = datetime.strptime(record['OPD_DATE'], '%d%b%Y:%H:%M:%S')
    return date + timedelta(seconds=int(record['ACT_TIME']))


def require_number(value, field):
    if isinstance(value, bool) or value is None:
        raise ValueError(f'{field} must be numeric')
    result = float(value)
    if not math.isfinite(result):
        raise ValueError(f'{field} must be finite')
    return result


def validate_record(record):
    """Run the checks that need only this one breadcrumb (A1-A5) and transform it.

    Returns (violations, transformed). `transformed` is (trip_id, entry) as
    produced by transform_record(), or None if there are violations.

    Checks that compare a breadcrumb with others of its trip (A10, A14, and
    decreasing distance) run at batch end in finalize_trip().
    """
    violations = []
    vehicle_id = record.get('VEHICLE_ID')
    trip_id = record.get('EVENT_NO_TRIP')
    act_time = record.get('ACT_TIME')

    # A1/A2: GPS coordinates must be non-null and within valid ranges
    for code, field, low, high in (
        ("A1", "GPS_LATITUDE", -90, 90),
        ("A2", "GPS_LONGITUDE", -180, 180),
    ):
        message = f"{code}: {field} must be non-null and in the range [{low}, {high}]"
        try:
            value = require_number(record.get(field), field)
        except (ValueError, TypeError, OverflowError):
            violations.append(message)
            continue
        if not low <= value <= high:
            violations.append(message)

    if vehicle_id is None or str(vehicle_id).strip() == '':
        violations.append('A3: VEHICLE_ID must exist')
    if trip_id is None or str(trip_id).strip() == '':
        violations.append('A4: EVENT_NO_TRIP must exist')

    valid_time = (
        not isinstance(act_time, bool) and
        (isinstance(act_time, int) or
         (isinstance(act_time, str) and act_time.isascii() and act_time.isdecimal()))
    )
    if not valid_time or int(act_time) < 0:
        violations.append('A5: ACT_TIME must be a non-negative integer')

    if violations:
        return violations, None

    try:
        breadcrumb_time = parse_breadcrumb_time(record)
        transformed = transform_record(record, breadcrumb_time)
    except (ValueError, TypeError, KeyError, OverflowError) as error:
        violations.append(f'Invalid field for transformation: {error}')
        return violations, None

    return violations, transformed


def transform_record(record, breadcrumb_time):
    """Sections 5.2, 5.3 and 5.5: keep only the fields loaded into PostgreSQL.

    5.2: EVENT_NO_STOP, GPS_SATELLITES and GPS_HDOP (and any VELOCITY) are
         dropped by not being copied.
    5.3: `breadcrumb_time` (OPD_DATE + ACT_TIME) replaces its source fields.
    5.5: EVENT_NO_TRIP, VEHICLE_ID, GPS_LATITUDE and GPS_LONGITUDE become
         trip_id, vehicle_id, latitude and longitude (tuple positions below).
    5.4: speed is added at batch end by finalize_trip(), once all of the
         trip's breadcrumbs are present.

    Returns (trip_id, (timestamp, meters, vehicle_id, latitude, longitude)).
    A tuple takes ~240 bytes versus ~1.2 KB for the parsed dict, so a full day
    (~1M breadcrumbs) fits comfortably in memory until the batch ends.
    """
    trip_id = int(record['EVENT_NO_TRIP'])
    entry = (
        breadcrumb_time,
        require_number(record.get('METERS'), 'METERS'),
        int(record['VEHICLE_ID']),
        require_number(record['GPS_LATITUDE'], 'GPS_LATITUDE'),
        require_number(record['GPS_LONGITUDE'], 'GPS_LONGITUDE'),
    )
    return trip_id, entry


def entry_as_record(trip_id, entry):
    """Rebuild a readable record from a buffered entry, for the invalid-records file."""
    timestamp, meters, vehicle_id, latitude, longitude = entry
    return {
        'EVENT_NO_TRIP': trip_id, 'VEHICLE_ID': vehicle_id,
        'timestamp': timestamp.isoformat(), 'METERS': meters,
        'GPS_LATITUDE': latitude, 'GPS_LONGITUDE': longitude,
    }


def finalize_trip(trip_id, entries):
    """Section 5.4: compute speed (m/s) for one trip's breadcrumbs.

    Same calculation as the Data Transformation lab (groupby EVENT_NO_TRIP,
    then diff of METERS / diff of timestamp), plus a sort by timestamp first,
    because Pub/Sub delivers breadcrumbs out of order.

    - The first breadcrumb of the trip gets speed 0.0 (per the assignment).
    - A breadcrumb with the same timestamp as the one before it is a duplicate
      delivery and is skipped.
    - A10 (vehicle changes), A14 (over 120 km/h) and decreasing distance reject
      the breadcrumb. A rejected breadcrumb is not used as the "previous" one,
      so the next breadcrumb is compared with the last accepted breadcrumb.

    Returns (rows, invalid, duplicates):
      rows:    [(trip_id, vehicle_id, timestamp, latitude, longitude, speed, meters)]
      invalid: [(record_dict, [violation, ...])]
    """
    entries.sort(key=lambda entry: entry[0])
    rows, invalid, duplicates = [], [], 0
    previous = None   # last accepted entry
    last_time = None  # timestamp of the last entry seen, accepted or not

    for entry in entries:
        timestamp, meters, vehicle_id, latitude, longitude = entry
        if timestamp == last_time:
            duplicates += 1
            continue
        last_time = timestamp

        if previous is None:
            speed = 0.0
        else:
            prev_time, prev_meters, prev_vehicle = previous[0], previous[1], previous[2]
            if vehicle_id != prev_vehicle:
                invalid.append((entry_as_record(trip_id, entry),
                                ['A10: Records in the same trip must have the same VEHICLE_ID']))
                continue
            delta_meters = meters - prev_meters
            delta_seconds = (timestamp - prev_time).total_seconds()
            if delta_meters < 0:
                invalid.append((entry_as_record(trip_id, entry),
                                ['Distance decreased within a trip (negative calculated speed)']))
                continue
            speed = delta_meters / delta_seconds
            if speed > SPEED_LIMIT_KMH / 3.6:
                invalid.append((entry_as_record(trip_id, entry),
                                [f'A14: Calculated speed {speed * 3.6:.2f} km/h exceeds 120 km/h']))
                continue

        rows.append((trip_id, vehicle_id, timestamp, latitude, longitude, speed, meters))
        previous = entry

    return rows, invalid, duplicates


def save_invalid_record(record, violations, received_at):
    """Append to a valid daily JSON array.

    Called from Pub/Sub callbacks and from the batch-end load, so writes to
    the file are serialized with their own lock.
    """
    filename = os.path.join(INVALID_DIR, f'invalid_data_{received_at:%Y-%m-%d}.json')
    entry = {'record': record, 'violations': violations,
             'received_at': received_at.isoformat()}
    with invalid_file_lock:
        if not os.path.exists(filename):
            with open(filename, 'w', encoding='utf-8') as file:
                file.write('[]')

        with open(filename, 'r+', encoding='utf-8') as file:
            file.seek(0, os.SEEK_END)
            size = file.tell()
            if size < 2:
                raise ValueError('Invalid daily JSON file')
            file.seek(size - 1)
            if file.read(1) != ']':
                raise ValueError('Daily JSON file is not an array')
            file.seek(size - 1)
            file.write(',\n' if size > 2 else '\n')
            json.dump(entry, file, default=str)
            file.write('\n]')
            file.truncate()

    for violation in violations:
        logging.warning('VALIDATION VIOLATION [%s] | record: %s', violation, record)


INSERT_SQL = '''
    INSERT INTO breadcrumb
    (trip_id, vehicle_id, timestamp, latitude, longitude, speed, meters)
    VALUES %s
'''


def insert_rows(rows):
    """Insert rows in a single INSERT statement (atomic with autocommit)."""
    with db_lock:
        with conn.cursor() as cur:
            execute_values(cur, INSERT_SQL, rows, page_size=len(rows))


def load_rows(rows, batch_stats, rejected_at):
    try:
        insert_rows(rows)
    except Exception as error:
        # The messages are already acked, so keep the rows recoverable.
        logging.exception('Database insert failed for %d rows', len(rows))
        for row in rows:
            trip_id, vehicle_id, timestamp, latitude, longitude, speed, meters = row
            record = entry_as_record(trip_id, (timestamp, meters, vehicle_id, latitude, longitude))
            record['speed'] = speed
            save_invalid_record(record, [f'Database insert failed: {error}'], rejected_at)
        batch_stats['invalid_count'] += len(rows)
        return

    batch_stats['breadcrumb_count'] += len(rows)
    for trip_id, vehicle_id, timestamp, *_ in rows:
        batch_stats['vehicle_ids'].add(vehicle_id)
        batch_stats['trip_ids'].add(trip_id)
        if batch_stats['earliest_breadcrumb'] is None or timestamp < batch_stats['earliest_breadcrumb']:
            batch_stats['earliest_breadcrumb'] = timestamp
        if batch_stats['latest_breadcrumb'] is None or timestamp > batch_stats['latest_breadcrumb']:
            batch_stats['latest_breadcrumb'] = timestamp


def load_batch(batch, batch_stats):
    """Compute speeds trip by trip and insert the rows in chunks.

    Trips are removed from `batch` as they are processed, so memory is freed
    as the load progresses instead of holding a second copy of the batch.
    """
    # Records rejected here are filed under the time the batch's last message arrived.
    rejected_at = batch_stats['last_received'] or datetime.now(timezone.utc)
    pending = []
    while batch:
        trip_id, entries = batch.popitem()
        rows, invalid, duplicates = finalize_trip(trip_id, entries)
        batch_stats['duplicate_count'] += duplicates
        for record, violations in invalid:
            save_invalid_record(record, violations, rejected_at)
        batch_stats['invalid_count'] += len(invalid)

        pending.extend(rows)
        if len(pending) >= INSERT_CHUNK:
            load_rows(pending, batch_stats, rejected_at)
            pending = []
    if pending:
        load_rows(pending, batch_stats, rejected_at)


def log_statistics(batch_stats):
    count = batch_stats['breadcrumb_count']
    if count == 0 and batch_stats['invalid_count'] == 0:
        return
    start = batch_stats['first_monotonic']
    end = batch_stats['last_monotonic']
    elapsed = end - start if start is not None and end is not None else 0
    throughput = count / elapsed if elapsed > 0 else 0
    logging.info('========== ANALYSIS STATISTICS ==========')
    logging.info('First breadcrumb received: %s', batch_stats['first_received'])
    logging.info('Unique vehicles: %d', len(batch_stats['vehicle_ids']))
    logging.info('Earliest breadcrumb: %s', batch_stats['earliest_breadcrumb'])
    logging.info('Latest breadcrumb: %s', batch_stats['latest_breadcrumb'])
    logging.info('Unique trips: %d', len(batch_stats['trip_ids']))
    logging.info('Total breadcrumbs loaded: %d', count)
    logging.info('Last breadcrumb received: %s', batch_stats['last_received'])
    logging.info('Active receive time: %.3f seconds', elapsed)
    logging.info('Throughput: %.2f breadcrumbs/second', throughput)
    logging.info('Invalid records: %d', batch_stats['invalid_count'])
    logging.info('Duplicate deliveries skipped: %d', batch_stats['duplicate_count'])
    logging.info('=========================================')


def callback(message):
    global last_message_monotonic
    received_at = datetime.now(timezone.utc)
    received_mono = time.monotonic()

    try:
        try:
            payload = json.loads(message.data.decode('utf-8'))
        except (ValueError, UnicodeError) as error:
            payload = message.data.decode('utf-8', errors='replace')
            parse_error = f'Malformed JSON or encoding: {error}'
        else:
            parse_error = None if isinstance(payload, dict) else 'Expected JSON object'

        with lock:
            if parse_error:
                violations, transformed = [parse_error], None
            else:
                violations, transformed = validate_record(payload)

            if violations:
                save_invalid_record(payload, violations, received_at)
                stats['invalid_count'] += 1
            else:
                # Buffer until batch end; speed is computed and the row
                # loaded in finish_batch().
                trip_id, entry = transformed
                trip_buffers[trip_id].append(entry)
                if stats['first_received'] is None:
                    stats['first_received'] = received_at
                    stats['first_monotonic'] = received_mono
                stats['last_received'] = received_at
                stats['last_monotonic'] = received_mono
            last_message_monotonic = received_mono

        # Acked once buffered. Pub/Sub's ack deadline (10 min max) is far
        # shorter than a batch, so acks cannot wait until the batch is loaded.
        message.ack()
    except Exception:
        logging.exception('Error processing message; retrying')
        message.nack()


def finish_batch(idle_time=None):
    """End the current batch: compute speeds, load the rows, log statistics.

    Swaps out the buffered batch under the main lock, then loads it outside
    the lock so incoming messages (the next batch) are not blocked. Called by
    the inactivity checker, on shutdown, and by tests.
    """
    global stats, trip_buffers, last_message_monotonic
    with lock:
        batch, trip_buffers = trip_buffers, defaultdict(list)
        batch_stats, stats = stats, reset_stats()
        last_message_monotonic = None

    load_batch(batch, batch_stats)
    log_statistics(batch_stats)
    if idle_time is not None:
        logging.info('Batch complete after %.0f seconds idle.', idle_time)


def check_batch_completion():
    while not stop_event.wait(5):
        with lock:
            if last_message_monotonic is None:
                continue
            idle_time = time.monotonic() - last_message_monotonic
        if idle_time >= IDLE_SECONDS:
            finish_batch(idle_time)


def main():
    global conn
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s - %(levelname)s - %(message)s')

    conn = psycopg2.connect(host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
                            user=os.environ['DB_USER'],
                            password=os.environ['DB_PASSWORD'])
    conn.autocommit = True

    subscriber = pubsub_v1.SubscriberClient()
    sub_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    checker = Thread(target=check_batch_completion, daemon=True)
    checker.start()
    logging.info('Analysis subscriber listening on %s', sub_path)
    logging.info('Batch inactivity threshold: %d seconds', IDLE_SECONDS)

    try:
        with subscriber:
            while not stop_event.is_set():
                streaming_pull = subscriber.subscribe(sub_path, callback=callback)
                try:
                    streaming_pull.result()
                except KeyboardInterrupt:
                    logging.info('Stopping subscriber...')
                    stop_event.set()
                    streaming_pull.cancel()
                    try:
                        streaming_pull.result()  # wait for in-flight callbacks
                    except Exception:
                        pass
                    break
                except Exception:
                    logging.exception('Connection interrupted. Reconnecting...')
                    streaming_pull.cancel()
                    time.sleep(5)
    finally:
        stop_event.set()
        checker.join()
        # Load whatever is still buffered, so stopping the subscriber does
        # not discard breadcrumbs that were already acked.
        logging.info('Loading buffered breadcrumbs before exit...')
        finish_batch()
        conn.close()


if __name__ == '__main__':
    main()
