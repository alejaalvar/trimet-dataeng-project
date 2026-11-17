
#!/usr/bin/env python3
"""TriMet StopEvent analysis subscriber: validate, transform, load, report."""

import json
import logging
import math
import os
import time
from datetime import date, datetime, timedelta, timezone
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

# PostgreSQL configuration
DB_HOST = 'localhost'
DB_PORT = 5432
DB_NAME = 'breadcrumbs'

# Coordinate transformation: Oregon State Plane to WGS 84
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

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

state_lock = Lock()
stop_event = Event()


# Initialize statistics
def reset_stats():
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


# Get a field regardless of capitalization
def get_field(record, field):
    if field in record:
        return record[field]
    return record.get(field.upper())


# Convert value to integer
def integer_value(value):
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
    if value is None or value == '':
        return None
    return integer_value(value)


def optional_float(value):
    if value is None or value == '':
        return None

    result = float(value)

    if not math.isfinite(result):
        raise ValueError('Non-finite number')

    return result


# Parse service date
def parse_service_date(value):
    if isinstance(value, datetime):
        return value.date()

    if isinstance(value, date):
        return value

    if value is None:
        raise ValueError('Missing service_date')

    text = str(value).strip()

    formats = (
        '%Y-%m-%d',
        '%Y%m%d',
        '%d%b%Y:%H:%M:%S',
        '%d%b%Y',
        '%m/%d/%Y',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d %H:%M:%S'
    )

    for fmt in formats:
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            pass

    raise ValueError(f'Invalid service_date: {value}')


# Convert State Plane coordinates to GPS
def state_plane_to_latlon(x_coordinate, y_coordinate):
    longitude, latitude = _transformer.transform(
        float(x_coordinate),
        float(y_coordinate)
    )

    return latitude, longitude


# Validate StopEvent record
def validate_record(record):
    violations = []

    def fail(number, description):
        violations.append({
            'assertion': f'A{number}',
            'reason': description
        })

    # A1: arrive_time must be in [0, 108000]
    try:
        arrival = integer_value(get_field(record, 'arrive_time'))

        if not 0 <= arrival <= 108000:
            raise ValueError('Out of range')

    except (ValueError, TypeError):
        arrival = None
        fail(1, 'arrive_time must be in [0, 108000]')

    # A2: leave_time >= arrive_time
    try:
        leave = integer_value(get_field(record, 'leave_time'))

        if arrival is not None and leave < arrival:
            fail(2, 'leave_time must be >= arrive_time')

    except (ValueError, TypeError):
        pass

    # A3: vehicle_number must exist and be positive
    try:
        if integer_value(get_field(record, 'vehicle_number')) <= 0:
            raise ValueError('Invalid vehicle number')

    except (ValueError, TypeError):
        fail(3, 'vehicle_number must exist and be positive')

    # A4: trip_number must exist and be positive
    try:
        if integer_value(get_field(record, 'trip_number')) <= 0:
            raise ValueError('Invalid trip number')

    except (ValueError, TypeError):
        fail(4, 'trip_number must exist and be positive')

    # A5: service_date must be valid
    try:
        parse_service_date(get_field(record, 'service_date'))

    except (ValueError, TypeError):
        fail(5, 'service_date must be a valid date')

    # A6: stop_time must be null or in [0, 108000]
    stop = get_field(record, 'stop_time')

    if stop is not None and stop != '':
        try:
            value = integer_value(stop)

            if not 0 <= value <= 108000:
                raise ValueError('Out of range')

        except (ValueError, TypeError):
            fail(6, 'stop_time must be null or in [0, 108000]')

    # A7: leave_time must be null or in [0, 108000]
    leave = get_field(record, 'leave_time')

    if leave is not None and leave != '':
        try:
            value = integer_value(leave)

            if not 0 <= value <= 108000:
                raise ValueError('Out of range')

        except (ValueError, TypeError):
            fail(7, 'leave_time must be null or in [0, 108000]')

    # A8: Coordinates must convert to valid GPS values
    try:
        x = optional_float(get_field(record, 'x_coordinate'))
        y = optional_float(get_field(record, 'y_coordinate'))

        if x is None or y is None:
            raise ValueError('Missing coordinates')

        lat, lon = state_plane_to_latlon(x, y)

        if not (
            math.isfinite(lat)
            and math.isfinite(lon)
            and -90 <= lat <= 90
            and -180 <= lon <= 180
        ):
            raise ValueError('Invalid GPS coordinates')

    except (ValueError, TypeError, OverflowError):
        fail(8, 'Invalid State Plane coordinates')

    # A9: Passenger counts must be non-negative
    for field in ('ons', 'offs', 'estimated_load'):
        value = get_field(record, field)

        if value is not None and value != '':
            try:
                if integer_value(value) < 0:
                    raise ValueError('Negative count')

            except (ValueError, TypeError):
                fail(9, f'{field} must be non-negative')

    # A10: dwell must be non-negative
    dwell = get_field(record, 'dwell')

    if dwell is not None and dwell != '':
        try:
            if integer_value(dwell) < 0:
                raise ValueError('Negative dwell')

        except (ValueError, TypeError):
            fail(10, 'dwell must be non-negative')

    return violations


# Transform validated record
def transform_record(record):
    service_date = parse_service_date(
        get_field(record, 'service_date')
    )

    midnight = datetime.combine(
        service_date,
        datetime.min.time()
    )

    transformed = {}

    for column in DB_COLUMNS:

        # Convert seconds since midnight into timestamps
        if column in ('arrive_time', 'leave_time', 'stop_time'):
            seconds = optional_int(get_field(record, column))

            transformed[column] = (
                midnight + timedelta(seconds=seconds)
                if seconds is not None else None
            )

        # GPS coordinates are generated separately
        elif column in ('gps_latitude', 'gps_longitude'):
            continue

        elif column == 'service_key':
            value = get_field(record, column)
            transformed[column] = (
                str(value) if value is not None else None
            )

        elif column in (
            'train_mileage',
            'pattern_distance',
            'location_distance'
        ):
            transformed[column] = optional_float(
                get_field(record, column)
            )

        else:
            transformed[column] = optional_int(
                get_field(record, column)
            )

    # Convert State Plane coordinates to GPS
    lat, lon = state_plane_to_latlon(
        get_field(record, 'x_coordinate'),
        get_field(record, 'y_coordinate')
    )

    transformed['gps_latitude'] = lat
    transformed['gps_longitude'] = lon

    return transformed


# Save invalid records to daily file
def save_invalid(record, violations, received_at):
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


# Track received messages
def record_receive(received_at, received_mono):
    if stats['first_received'] is None:
        stats['first_received'] = received_at
        stats['first_monotonic'] = received_mono

    stats['last_received'] = received_at
    stats['last_monotonic'] = received_mono

    stats['received_count'] += 1


# Log batch statistics
def log_stats():
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


# Connect to PostgreSQL
db_user = os.environ['DB_USER']
db_password = os.environ['DB_PASSWORD']

conn = psycopg2.connect(
    host=DB_HOST,
    port=DB_PORT,
    dbname=DB_NAME,
    user=db_user,
    password=db_password
)

conn.autocommit = True


# Initialize Pub/Sub subscriber
subscriber = pubsub_v1.SubscriberClient()

sub_path = subscriber.subscription_path(
    PROJECT_ID,
    SUBSCRIPTION_ID
)


# Process incoming StopEvent messages
def callback(message):
    received_at = datetime.now(timezone.utc)
    received_mono = time.monotonic()

    try:
        raw = message.data.decode('utf-8')
        record = json.loads(raw)

        if not isinstance(record, dict):
            raise ValueError('Expected JSON object')

    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        try:
            with state_lock:
                save_invalid(
                    message.data.decode(
                        'utf-8',
                        errors='replace'
                    ),
                    [{
                        'assertion': 'FORMAT',
                        'reason': str(error)
                    }],
                    received_at
                )

                record_receive(received_at, received_mono)
                stats['invalid_count'] += 1

            message.ack()

        except Exception:
            logging.exception(
                'Failed to save malformed StopEvent'
            )
            message.nack()

        return

    try:
        # Validate record before transformation
        violations = validate_record(record)

        transformed = None

        if not violations:
            try:
                transformed = transform_record(record)

            except (ValueError, TypeError, OverflowError) as error:
                violations.append({
                    'assertion': 'TRANSFORM',
                    'reason': str(error)
                })

        with state_lock:

            # Drop invalid records and save original JSON
            if violations:
                save_invalid(record, violations, received_at)
                record_receive(received_at, received_mono)

                stats['invalid_count'] += 1

            else:
                # Insert transformed record into PostgreSQL
                with conn.cursor() as cursor:
                    cursor.execute(
                        INSERT_SQL,
                        tuple(
                            transformed[column]
                            for column in DB_COLUMNS
                        )
                    )

                    inserted = cursor.rowcount

                record_receive(received_at, received_mono)

                stats['inserted_count'] += inserted
                stats['duplicate_count'] += (1 - inserted)

                stats['vehicle_ids'].add(
                    transformed['vehicle_number']
                )

                stats['trip_ids'].add(
                    transformed['trip_number']
                )

        message.ack()

    except Exception:
        logging.exception(
            'StopEvent processing failed; retrying message'
        )
        message.nack()


# Detect batch completion after inactivity
def check_batch_completion():
    global stats

    while not stop_event.wait(5):
        with state_lock:

            if stats['received_count'] == 0:
                continue

            idle = (
                time.monotonic()
                - stats['last_monotonic']
            )

            if idle >= IDLE_SECONDS:
                log_stats()

                logging.info(
                    'StopEvent batch complete after %.0f seconds idle',
                    idle
                )

                stats = reset_stats()


# Start background checker
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


# Run subscriber automatically
try:
    with subscriber:

        while not stop_event.is_set():
            streaming_pull = subscriber.subscribe(
                sub_path,
                callback=callback
            )

            try:
                streaming_pull.result()

            except KeyboardInterrupt:
                logging.info('Stopping StopEvent subscriber...')
                stop_event.set()
                streaming_pull.cancel()
                break

            except Exception:
                logging.exception(
                    'Subscriber interrupted; reconnecting'
                )

                streaming_pull.cancel()
                stop_event.wait(5)

finally:
    stop_event.set()
    checker.join(timeout=2)

    with state_lock:
        if stats['received_count'] > 0:
            log_stats()

    conn.close()
