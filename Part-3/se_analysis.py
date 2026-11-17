#!/usr/bin/env python3
"""TriMet StopEvent analysis subscriber: validate, transform, load, report."""

import json
import logging
import math
import os
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Event, Lock, Thread

import psycopg2
from google.cloud import pubsub_v1
from pyproj import Transformer

# Configuration
PROJECT_ID = 'dataeng-project-511504'
TOPIC_ID = 'se_topic'
SUBSCRIPTION_ID = 'se_analysis_sub'
IDLE_SECONDS = 300
INVALID_DIR = Path('.')

# PostgreSQL configuration (credentials are read from the environment when
# connecting)
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'breadcrumbs'

# Times are seconds after midnight of the service date; 30 hours leaves room
# for trips that run well past midnight.
MAX_SECONDS = 108000

# Coordinate transformation: Oregon State Plane North (EPSG:2913, feet) to
# WGS 84 (spec section 6.2, Transformation 2)
_transformer = Transformer.from_crs(
    'EPSG:2913', 'EPSG:4326', always_xy=True
)

# Database columns
DB_COLUMNS = (
    'vehicle_number', 'leave_time', 'train', 'route_number',
    'direction', 'service_key', 'trip_number', 'stop_time',
    'arrive_time', 'dwell', 'location_id', 'door', 'lift',
    'ons', 'offs', 'estimated_load', 'maximum_speed',
    'train_mileage', 'pattern_distance', 'location_distance',
    'gps_latitude', 'gps_longitude', 'data_source',
    'schedule_status'
)

INSERT_SQL = (
    f"INSERT INTO stopevent ({', '.join(DB_COLUMNS)}) "
    f"VALUES ({', '.join(['%s'] * len(DB_COLUMNS))}) "
    'ON CONFLICT (vehicle_number, trip_number, arrive_time) DO NOTHING'
)

# Opened in main() and reopened by ensure_connection(), so this module can be
# imported (e.g. by tests) without a database.
conn = None

state_lock = Lock()
stop_event = Event()


def reset_stats():
    """Return empty statistics for a new batch."""
    return {
        'first_received': None,
        'last_received': None,
        'first_monotonic': None,
        'last_monotonic': None,
        'received_count': 0,
        'inserted_count': 0,
        'invalid_count': 0,
        'duplicate_count': 0,
        'vehicle_ids': set(),
        'trip_ids': set()
    }


stats = reset_stats()


def integer_value(value):
    """Return value as an int, or raise ValueError if it is not a whole number.

    Accepts ints, whole floats and digit strings (the publisher sends every
    field as a string); bool is rejected even though it is an int subclass.
    """
    if isinstance(value, bool) or value is None:
        raise ValueError('Missing or invalid integer')

    if isinstance(value, int):
        return value

    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError('Not an integer')
        return int(value)

    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.lstrip('+-').isdigit():
            return int(text)

    raise ValueError('Not an integer')


def optional_int(value):
    """Return None for a missing or empty value, otherwise integer_value(value)."""
    if value is None or value == '':
        return None
    return integer_value(value)


def optional_float(value):
    """Return None for a missing or empty value, otherwise a finite float."""
    if value is None or value == '':
        return None

    result = float(value)

    if not math.isfinite(result):
        raise ValueError('Non-finite number')

    return result


def parse_service_date(value):
    """Return the service date ("YYYY-MM-DD", as se_publisher.py sends it) as a date."""
    if not isinstance(value, str):
        raise ValueError('Missing service_date')

    return datetime.strptime(value.strip(), '%Y-%m-%d').date()


def state_plane_to_latlon(x_coordinate, y_coordinate):
    """Convert Oregon State Plane (EPSG:2913) x/y in feet to (latitude, longitude)."""
    longitude, latitude = _transformer.transform(
        float(x_coordinate),
        float(y_coordinate)
    )

    return latitude, longitude


def validate_record(record):
    """Check a StopEvent against assertions A1-A10 and return the violations.

    An empty list means the record is valid. A1 and A2 are defined by the
    spec; A3-A10 are ours. Every violation drops the record.
    """
    violations = []

    def fail(number, description):
        violations.append({
            'assertion': f'A{number}',
            'reason': description
        })

    # A1: arrive_time must be in [0, MAX_SECONDS]
    try:
        arrival = integer_value(record.get('arrive_time'))

        if not 0 <= arrival <= MAX_SECONDS:
            raise ValueError('Out of range')

    except (ValueError, TypeError):
        arrival = None
        fail(1, f'arrive_time must be in [0, {MAX_SECONDS}]')

    # A2: leave_time >= arrive_time when leave_time is present; a missing or
    # malformed leave_time is A7's to report.
    try:
        leave = integer_value(record.get('leave_time'))

        if arrival is not None and leave < arrival:
            fail(2, 'leave_time must be >= arrive_time')

    except (ValueError, TypeError):
        pass

    # A3: vehicle_number must exist and be positive
    try:
        if integer_value(record.get('vehicle_number')) <= 0:
            raise ValueError('Invalid vehicle number')

    except (ValueError, TypeError):
        fail(3, 'vehicle_number must exist and be positive')

    # A4: trip_number must exist and be positive
    try:
        if integer_value(record.get('trip_number')) <= 0:
            raise ValueError('Invalid trip number')

    except (ValueError, TypeError):
        fail(4, 'trip_number must exist and be positive')

    # A5: service_date must be valid
    try:
        parse_service_date(record.get('service_date'))

    except (ValueError, TypeError):
        fail(5, 'service_date must be a valid date')

    # A6: stop_time must be null or in [0, MAX_SECONDS]
    stop = record.get('stop_time')

    if stop is not None and stop != '':
        try:
            value = integer_value(stop)

            if not 0 <= value <= MAX_SECONDS:
                raise ValueError('Out of range')

        except (ValueError, TypeError):
            fail(6, f'stop_time must be null or in [0, {MAX_SECONDS}]')

    # A7: leave_time must be null or in [0, MAX_SECONDS]
    leave = record.get('leave_time')

    if leave is not None and leave != '':
        try:
            value = integer_value(leave)

            if not 0 <= value <= MAX_SECONDS:
                raise ValueError('Out of range')

        except (ValueError, TypeError):
            fail(7, f'leave_time must be null or in [0, {MAX_SECONDS}]')

    # A8: Coordinates must exist, be finite numbers and convert to GPS.
    # pyproj signals a failed conversion by returning inf rather than
    # raising, so the result is checked; it is always within +-90/+-180 when
    # finite, so no range check is needed.
    try:
        x = optional_float(record.get('x_coordinate'))
        y = optional_float(record.get('y_coordinate'))

        if x is None or y is None:
            raise ValueError('Missing coordinates')

        lat, lon = state_plane_to_latlon(x, y)

        if not (math.isfinite(lat) and math.isfinite(lon)):
            raise ValueError('Coordinates did not convert to GPS')

    except (ValueError, TypeError, OverflowError):
        fail(8, 'x_coordinate and y_coordinate must be finite numbers that convert to GPS')

    # A9: Passenger counts must be non-negative
    for field in ('ons', 'offs', 'estimated_load'):
        value = record.get(field)

        if value is not None and value != '':
            try:
                if integer_value(value) < 0:
                    raise ValueError('Negative count')

            except (ValueError, TypeError):
                fail(9, f'{field} must be non-negative')

    # A10: dwell must be non-negative
    dwell = record.get('dwell')

    if dwell is not None and dwell != '':
        try:
            if integer_value(dwell) < 0:
                raise ValueError('Negative dwell')

        except (ValueError, TypeError):
            fail(10, 'dwell must be non-negative')

    return violations


def transform_record(record):
    """Return a validated StopEvent as a dict of DB_COLUMNS values.

    Transformation 1: arrive/stop/leave seconds become datetimes relative to
    midnight of the service date (past 86,400 s they roll into the next day).
    Transformation 2: x/y State Plane coordinates become GPS latitude and
    longitude, replacing them.
    """
    service_date = parse_service_date(record.get('service_date'))

    midnight = datetime.combine(
        service_date,
        datetime.min.time()
    )

    transformed = {}

    for column in DB_COLUMNS:

        if column in ('arrive_time', 'leave_time', 'stop_time'):
            seconds = optional_int(record.get(column))

            transformed[column] = (
                midnight + timedelta(seconds=seconds)
                if seconds is not None else None
            )

        # Computed from x/y below
        elif column in ('gps_latitude', 'gps_longitude'):
            continue

        elif column == 'service_key':
            value = record.get(column)
            transformed[column] = (
                str(value) if value not in (None, '') else None
            )

        elif column in (
            'train_mileage',
            'pattern_distance',
            'location_distance'
        ):
            transformed[column] = optional_float(record.get(column))

        else:
            transformed[column] = optional_int(record.get(column))

    lat, lon = state_plane_to_latlon(
        record.get('x_coordinate'),
        record.get('y_coordinate')
    )

    transformed['gps_latitude'] = lat
    transformed['gps_longitude'] = lon

    return transformed


def save_invalid(record, violations, received_at):
    """Append a dropped record and its violations to the day's invalid file."""
    INVALID_DIR.mkdir(parents=True, exist_ok=True)

    filename = INVALID_DIR / (
        f"se_invalid_data_{received_at:%Y-%m-%d}.jsonl"
    )

    entry = {
        'received_at': received_at.isoformat(),
        'record': record,
        'violations': violations
    }

    with filename.open('a', encoding='utf-8') as file:
        file.write(json.dumps(entry, default=str) + '\n')

    for violation in violations:
        logging.warning(
            'VALIDATION VIOLATION [%s: %s] | record: %s',
            violation['assertion'],
            violation['reason'],
            record
        )


def record_receive(received_at, received_mono, record):
    """Count one processed message, its vehicle and trip, and update the receive times.

    Vehicles and trips are counted for every received record, valid or not,
    because the spec (section 8) expects the subscriber's vehicle count to
    agree with the publisher's.
    """
    # str() so 4001 and "4001" count once, and so an odd value such as a
    # list cannot raise TypeError and get the message nacked forever.
    if isinstance(record, dict):
        if record.get('vehicle_number') is not None:
            stats['vehicle_ids'].add(str(record['vehicle_number']))
        if record.get('trip_number') is not None:
            stats['trip_ids'].add(str(record['trip_number']))

    if stats['first_received'] is None:
        stats['first_received'] = received_at
        stats['first_monotonic'] = received_mono

    stats['last_received'] = received_at
    stats['last_monotonic'] = received_mono

    stats['received_count'] += 1


def log_stats():
    """Log the current batch's statistics (spec section 8)."""
    elapsed = (
        stats['last_monotonic']
        - stats['first_monotonic']
    )

    logging.info('========== STOPEVENT ANALYSIS STATISTICS ==========')

    logging.info(
        'First message received: %s',
        stats['first_received'].isoformat()
    )

    logging.info(
        'Last message received: %s',
        stats['last_received'].isoformat()
    )

    logging.info(
        'Unique vehicles: %d',
        len(stats['vehicle_ids'])
    )

    logging.info(
        'Unique trips: %d',
        len(stats['trip_ids'])
    )

    logging.info(
        'Total messages: %d',
        stats['received_count']
    )

    logging.info(
        'Inserted records: %d',
        stats['inserted_count']
    )

    logging.info(
        'Invalid records: %d',
        stats['invalid_count']
    )

    logging.info(
        'Duplicate records skipped: %d',
        stats['duplicate_count']
    )

    logging.info(
        'Elapsed receive time: %.3f seconds',
        elapsed
    )

    logging.info(
        'Throughput: %.2f messages/second',
        stats['received_count'] / elapsed if elapsed > 0 else 0
    )

    logging.info('==================================================')


def ensure_connection():
    """Return the database connection, opening a new one if there is none or it was closed."""
    global conn

    if conn is None or conn.closed:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=os.environ['DB_USER'],
            password=os.environ['DB_PASSWORD'],
            # Reconnecting happens while holding state_lock; a server that
            # never answers must not block every callback forever.
            connect_timeout=10
        )
        conn.autocommit = True

    return conn


def close_connection():
    """Close and forget the database connection so the next insert reconnects."""
    global conn

    old, conn = conn, None

    if old is not None:
        old.close()


def insert_record(transformed):
    """Insert one transformed StopEvent; return 1 if stored, 0 if it was already there."""
    try:
        with ensure_connection().cursor() as cursor:
            cursor.execute(
                INSERT_SQL,
                tuple(
                    transformed[column]
                    for column in DB_COLUMNS
                )
            )

            return cursor.rowcount

    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        # The connection is unusable (e.g. PostgreSQL restarted); without
        # dropping it, every later insert would fail on the dead connection.
        close_connection()
        raise


def reject(message, record, violations, received_at, received_mono):
    """Save a record that can never be loaded to the invalid file, count it and ack it.

    Acking, rather than nacking, keeps Pub/Sub from redelivering a record
    that would fail the same way every time.
    """
    try:
        with state_lock:
            save_invalid(record, violations, received_at)
            record_receive(received_at, received_mono, record)
            stats['invalid_count'] += 1

        message.ack()

    except Exception:
        logging.exception('Failed to save invalid StopEvent')
        message.nack()


def callback(message):
    """Validate, transform and load one StopEvent message.

    Malformed, invalid and table-rejected records go to the invalid file and
    are acked; any other failure (e.g. a lost database connection) is nacked
    so Pub/Sub redelivers the message.
    """
    received_at = datetime.now(timezone.utc)
    received_mono = time.monotonic()

    # UnicodeDecodeError and JSONDecodeError are both ValueErrors.
    try:
        record = json.loads(message.data.decode('utf-8'))

        if not isinstance(record, dict):
            raise ValueError('Expected JSON object')

    except ValueError as error:
        reject(
            message,
            message.data.decode('utf-8', errors='replace'),
            [{'assertion': 'FORMAT', 'reason': str(error)}],
            received_at,
            received_mono
        )
        return

    # Validation and transformation depend only on the record, so an
    # unexpected error (e.g. from pyproj) would repeat on every redelivery.
    try:
        violations = validate_record(record)
        transformed = None if violations else transform_record(record)

    except Exception as error:
        violations = [{'assertion': 'TRANSFORM', 'reason': str(error)}]

    if violations:
        reject(message, record, violations, received_at, received_mono)
        return

    try:
        with state_lock:
            inserted = insert_record(transformed)

            record_receive(received_at, received_mono, record)

            stats['inserted_count'] += inserted
            stats['duplicate_count'] += (1 - inserted)

        message.ack()

    # The table rejected this record's values (e.g. a number too large for a
    # SMALLINT column), which no retry can fix.
    except (psycopg2.DataError, psycopg2.IntegrityError) as error:
        reject(
            message,
            record,
            [{'assertion': 'DATABASE', 'reason': str(error)}],
            received_at,
            received_mono
        )

    except Exception:
        logging.exception('StopEvent processing failed; retrying message')
        message.nack()


def finish_batch_locked():
    """Log the current batch's statistics and start a new batch.

    The caller must hold `state_lock`. Does nothing if no messages were
    received.
    """
    global stats

    if stats['received_count'] == 0:
        return

    log_stats()
    stats = reset_stats()


def finish_batch():
    """Finish the current batch now (see finish_batch_locked)."""
    with state_lock:
        finish_batch_locked()


def finish_if_idle():
    """Finish the batch once no message has arrived for IDLE_SECONDS.

    Pub/Sub has no end-of-batch signal, so a long silence is taken to mean
    the publisher is done for the day. The check and the finish happen under
    one hold of the lock so a message arriving in between cannot be counted
    in the batch being closed.
    """
    with state_lock:
        if stats['received_count'] == 0:
            return

        idle = time.monotonic() - stats['last_monotonic']

        if idle >= IDLE_SECONDS:
            logging.info('StopEvent batch idle for %.0f seconds.', idle)
            finish_batch_locked()


def check_batch_completion():
    """Check every 5 seconds whether the batch has gone idle, until shutdown."""
    while not stop_event.wait(5):
        try:
            finish_if_idle()

        except Exception:
            logging.exception('Error finishing StopEvent batch')


def handle_stop_signal(signum, frame):
    """Turn Ctrl+C or systemd's SIGTERM into one KeyboardInterrupt so the batch is reported.

    The VM shuts down at the end of its daily window and systemd stops this
    service with SIGTERM, whose Python default exits at once without the
    final statistics. Later stop signals are ignored so a second Ctrl+C or
    SIGTERM cannot interrupt the shutdown itself.
    """
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    raise KeyboardInterrupt


def main():
    """Validate and load StopEvents from se_analysis_sub until stopped."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    signal.signal(signal.SIGTERM, handle_stop_signal)
    signal.signal(signal.SIGINT, handle_stop_signal)

    # Connecting before subscribing makes bad credentials fail at startup
    # instead of nacking every message.
    ensure_connection()

    subscriber = pubsub_v1.SubscriberClient()

    sub_path = subscriber.subscription_path(
        PROJECT_ID,
        SUBSCRIPTION_ID
    )

    checker = Thread(
        target=check_batch_completion,
        daemon=True
    )

    checker.start()

    logging.info(
        'StopEvent subscriber listening on %s (topic %s)',
        sub_path,
        TOPIC_ID
    )

    streaming_pull = None

    try:
        while not stop_event.is_set():
            streaming_pull = subscriber.subscribe(
                sub_path,
                callback=callback,
                await_callbacks_on_shutdown=True
            )

            try:
                streaming_pull.result()

            except Exception:
                logging.exception(
                    'Subscriber interrupted; reconnecting'
                )

                streaming_pull.cancel()
                time.sleep(5)

    # Caught here rather than around result() alone, because the stop signal
    # can arrive anywhere in the loop, including the reconnect sleep.
    # KeyboardInterrupt is not an Exception, so the handler above skips it.
    except KeyboardInterrupt:
        logging.info('Stopping StopEvent subscriber...')

    finally:
        stop_event.set()

        # cancel() does not wait for running callbacks; with
        # await_callbacks_on_shutdown=True this result() does, so the final
        # statistics include every message and no insert races the close.
        # A failure here must not stop the statistics from being logged.
        try:
            if streaming_pull is not None:
                streaming_pull.cancel()

                try:
                    streaming_pull.result()
                except Exception:
                    pass

            subscriber.close()

        except Exception:
            logging.exception('Error shutting down the Pub/Sub subscriber')

        try:
            finish_batch()

        except Exception:
            logging.exception('Failed to log final StopEvent statistics')

        close_connection()


if __name__ == '__main__':
    main()
